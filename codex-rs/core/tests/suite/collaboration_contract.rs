use anyhow::Result;
use codex_features::Feature;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call_with_namespace;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_once;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::test_codex::test_codex;
use pretty_assertions::assert_eq;
use serde_json::Value;

const COLLABORATION_NAMESPACE: &str = "collaboration";

fn collaboration_tools(tools: &Value) -> Vec<Value> {
    tools
        .as_array()
        .into_iter()
        .flatten()
        .filter(|tool| tool.get("name").and_then(Value::as_str) == Some("collaboration"))
        .cloned()
        .collect()
}

#[test]
fn responses_payload_keeps_pinned_collaboration_contract() -> Result<()> {
    let test_thread = std::thread::Builder::new()
        .name("responses_payload_keeps_pinned_collaboration_contract".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()?;
            runtime.block_on(responses_payload_keeps_pinned_collaboration_contract_impl())
        })?;
    match test_thread.join() {
        Ok(result) => result,
        Err(err) => std::panic::resume_unwind(err),
    }
}

async fn responses_payload_keeps_pinned_collaboration_contract_impl() -> Result<()> {
    let server = start_mock_server().await;
    let response = mount_sse_once(
        &server,
        sse(vec![
            ev_response_created("response-contract"),
            ev_completed("response-contract"),
        ]),
    )
    .await;
    let mut builder = test_codex().with_config(|config| {
        config
            .features
            .enable(Feature::MultiAgentV2)
            .expect("MultiAgentV2 should be configurable");
    });
    let test = builder.build_with_auto_env(&server).await?;
    assert_eq!(test.config.model.as_deref(), Some("gpt-5.5"));
    assert_eq!(test.config.service_tier, None);

    test.submit_turn("capture the collaboration contract")
        .await?;

    let body = response.single_request().body_json();
    let path = codex_utils_cargo_bin::find_resource!(
        "tests/fixtures/collaboration_responses_v2_root_d9511fb7.json"
    )?;
    let expected = std::fs::read_to_string(path)?;
    let expected: Value = serde_json::from_str(&expected)?;
    let expected = expected
        .as_array()
        .expect("pinned Responses API tools")
        .clone();
    let actual = collaboration_tools(&body["tools"]);
    assert_eq!(actual, expected);
    let [namespace] = actual.as_slice() else {
        anyhow::bail!("expected one collaboration namespace, got {}", actual.len());
    };
    let children = namespace["tools"]
        .as_array()
        .expect("collaboration namespace tools");
    assert!(children.iter().all(|tool| {
        matches!(
            tool.get("name").and_then(Value::as_str),
            Some(
                "followup_task"
                    | "interrupt_agent"
                    | "list_agents"
                    | "send_message"
                    | "spawn_agent"
                    | "wait_agent"
            )
        )
    }));

    Ok(())
}

#[test]
fn responses_error_wire_keeps_pinned_collaboration_contract() -> Result<()> {
    let test_thread = std::thread::Builder::new()
        .name("responses_error_wire_keeps_pinned_collaboration_contract".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()?;
            runtime.block_on(responses_error_wire_keeps_pinned_collaboration_contract_impl())
        })?;
    match test_thread.join() {
        Ok(result) => result,
        Err(err) => std::panic::resume_unwind(err),
    }
}

async fn responses_error_wire_keeps_pinned_collaboration_contract_impl() -> Result<()> {
    let server = start_mock_server().await;
    let calls = [
        ("list-success", "list_agents", r#"{}"#),
        ("list-error", "list_agents", r#"{"cursor":"x"}"#),
        (
            "spawn-error",
            "spawn_agent",
            r#"{"message":"x","task_name":"x","unexpected":true}"#,
        ),
        (
            "send-error",
            "send_message",
            r#"{"target":"x","message":"x","unexpected":true}"#,
        ),
        (
            "followup-error",
            "followup_task",
            r#"{"target":"x","message":"x","unexpected":true}"#,
        ),
        (
            "wait-error",
            "wait_agent",
            r#"{"timeout_ms":1,"unexpected":true}"#,
        ),
        (
            "interrupt-error",
            "interrupt_agent",
            r#"{"target":"x","unexpected":true}"#,
        ),
    ];
    let mut events = vec![ev_response_created("response-wire-1")];
    events.extend(calls.iter().map(|(call_id, tool_name, arguments)| {
        ev_function_call_with_namespace(call_id, COLLABORATION_NAMESPACE, tool_name, arguments)
    }));
    events.push(ev_completed("response-wire-1"));
    let responses = mount_sse_sequence(
        &server,
        vec![
            sse(events),
            sse(vec![
                ev_response_created("response-wire-2"),
                ev_completed("response-wire-2"),
            ]),
        ],
    )
    .await;
    let mut builder = test_codex().with_config(|config| {
        config
            .features
            .enable(Feature::MultiAgentV2)
            .expect("MultiAgentV2 should be configurable");
    });
    let test = builder.build_with_auto_env(&server).await?;
    assert_eq!(test.config.model.as_deref(), Some("gpt-5.5"));
    assert_eq!(test.config.service_tier, None);

    test.submit_turn("capture collaboration error serialization")
        .await?;

    let requests = responses.requests();
    let output_request = requests
        .get(1)
        .expect("tool outputs should be submitted in a second request");
    let actual = calls
        .iter()
        .map(|(call_id, tool_name, arguments)| {
            let (output, success) = output_request
                .function_call_output_content_and_success(call_id)
                .unwrap_or_else(|| panic!("missing output for {call_id}"));
            serde_json::json!({
                "call_id": call_id,
                "tool": tool_name,
                "arguments": serde_json::from_str::<Value>(arguments)
                    .expect("representative arguments should be JSON"),
                "output": output,
                "success": success,
            })
        })
        .collect::<Vec<_>>();
    let actual = serde_json::to_value(actual)?;
    let path = codex_utils_cargo_bin::find_resource!(
        "tests/fixtures/collaboration_error_wire_d9511fb7.json"
    )?;
    let expected = std::fs::read_to_string(path)?;
    let expected: Value = serde_json::from_str(&expected)?;
    assert_eq!(actual, expected);

    Ok(())
}
