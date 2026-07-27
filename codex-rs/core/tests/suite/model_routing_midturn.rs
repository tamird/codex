use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use codex_config::McpServerConfig;
use codex_features::Feature;
use codex_models_manager::CustomModelConfig;
use codex_models_manager::ModelRoutingCandidate;
use codex_models_manager::ModelRoutingProfile;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::turn_input::TurnInputRequest;
use codex_protocol::turn_input::TurnInputSubmission;
use codex_protocol::user_input::UserInput;
use core_test_support::responses::ResponseMock;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call;
use core_test_support::responses::ev_function_call_with_namespace;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_response_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::sse_completed;
use core_test_support::responses::sse_response;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::stdio_server_bin;
use core_test_support::submit_thread_settings;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use core_test_support::wait_for_mcp_server;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;

const PROFILE: &str = "test-midturn-route";
const PRIMARY: &str = "test-midturn-primary";
const FALLBACK: &str = "test-midturn-fallback";

fn routing_models() -> HashMap<String, CustomModelConfig> {
    HashMap::from([(
        PROFILE.to_string(),
        CustomModelConfig {
            model: PRIMARY.to_string(),
            routing_profile: Some(ModelRoutingProfile {
                candidates: vec![
                    ModelRoutingCandidate {
                        model: PRIMARY.to_string(),
                        reasoning_effort: None,
                        service_tier: None,
                    },
                    ModelRoutingCandidate {
                        model: FALLBACK.to_string(),
                        reasoning_effort: None,
                        service_tier: None,
                    },
                ],
            }),
            model_context_window: None,
            model_auto_compact_token_limit: None,
        },
    )])
}

async fn build_routed_test(
    server: &wiremock::MockServer,
    multi_agent_v2: bool,
) -> Result<TestCodex> {
    test_codex()
        .with_config(move |config| {
            config.model = Some(PROFILE.to_string());
            config.custom_models = routing_models();
            if multi_agent_v2 {
                config
                    .features
                    .enable(Feature::MultiAgentV2)
                    .expect("test config should allow multi-agent v2");
            }
        })
        .build(server)
        .await
}

async fn submit_prompt(test: &TestCodex, text: &str) -> Result<()> {
    test.codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: text.to_string(),
            text_elements: Vec::new(),
        }]))
        .await?;
    Ok(())
}

async fn events_until_complete(test: &TestCodex) -> Vec<EventMsg> {
    let mut events = Vec::new();
    loop {
        let event = wait_for_event(&test.codex, |_| true).await;
        let complete = matches!(event, EventMsg::TurnComplete(_));
        events.push(event);
        if complete {
            return events;
        }
    }
}

fn overload_response(response_id: &str) -> wiremock::ResponseTemplate {
    sse_response(sse(vec![json!({
        "type": "response.failed",
        "response": {
            "id": response_id,
            "status": "failed",
            "error": {
                "code": "server_is_overloaded",
                "message": "temporary test diagnostic"
            }
        }
    })]))
}

fn assert_request_models(mock: &ResponseMock, expected: &[&str]) {
    let models = mock
        .requests()
        .into_iter()
        .map(|request| {
            request.body_json()["model"]
                .as_str()
                .expect("request model")
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(models, expected);
}

fn user_input_texts(request: &core_test_support::responses::ResponsesRequest) -> Vec<String> {
    request
        .input()
        .into_iter()
        .filter(|item| item["type"] == "message" && item["role"] == "user")
        .filter_map(|item| item["content"].as_array().cloned())
        .flatten()
        .filter(|item| item["type"] == "input_text")
        .filter_map(|item| item["text"].as_str().map(str::to_string))
        .collect()
}

fn assert_one_output(request: &core_test_support::responses::ResponsesRequest, call_id: &str) {
    assert_eq!(
        request
            .inputs_of_type("function_call_output")
            .into_iter()
            .filter(|item| item["call_id"] == call_id)
            .count(),
        1,
        "expected one output for {call_id}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_tool_outputs_are_complete_before_midturn_reroute() -> Result<()> {
    skip_if_no_network!(Ok(()));

    const FIRST_CALL: &str = "parallel-call-1";
    const SECOND_CALL: &str = "parallel-call-2";
    let server = start_mock_server().await;
    let args = json!({
        "barrier": {
            "id": "routing-parallel-tools",
            "participants": 2,
            "timeout_ms": 2_000
        }
    })
    .to_string();
    let mock = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("parallel-response"),
                ev_function_call(FIRST_CALL, "test_sync_tool", &args),
                ev_function_call(SECOND_CALL, "test_sync_tool", &args),
                ev_completed("parallel-response"),
            ])),
            overload_response("primary-continuation-failed"),
            sse_response(sse(vec![
                ev_assistant_message("fallback-message", "completed after fallback"),
                ev_completed("fallback-response"),
            ])),
        ],
    )
    .await;
    let test = build_routed_test(&server, /*multi_agent_v2*/ false).await?;

    submit_prompt(&test, "run two tools").await?;
    let events = events_until_complete(&test).await;

    assert_request_models(&mock, &[PRIMARY, PRIMARY, FALLBACK]);
    let requests = mock.requests();
    let failed_continuation = &requests[1];
    let fallback_request = &requests[2];
    for request in [failed_continuation, fallback_request] {
        assert_one_output(request, FIRST_CALL);
        assert_one_output(request, SECOND_CALL);
    }
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_))),
        "transparent reroute should not emit a turn error"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn steering_during_tool_wait_is_recorded_once_before_midturn_reroute() -> Result<()> {
    skip_if_no_network!(Ok(()));

    const CALL_ID: &str = "routing-wait-call";
    const INITIAL_PROMPT: &str = "wait before continuing";
    const STEER_PROMPT: &str = "use this additional direction";
    let server = start_mock_server().await;
    let mock = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("wait-response"),
                ev_function_call_with_namespace(
                    CALL_ID,
                    "collaboration",
                    "wait_agent",
                    r#"{"timeout_ms":10000}"#,
                ),
                ev_completed("wait-response"),
            ])),
            overload_response("steered-continuation-failed"),
            sse_response(sse_completed("fallback-response")),
        ],
    )
    .await;
    let test = build_routed_test(&server, /*multi_agent_v2*/ true).await?;

    submit_prompt(&test, INITIAL_PROMPT).await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::CollabWaitingBegin(_))
    })
    .await;
    let submission = test
        .codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: STEER_PROMPT.to_string(),
            text_elements: Vec::new(),
        }]))
        .await
        .expect("steering should be accepted");
    assert!(matches!(submission, TurnInputSubmission::Steered { .. }));
    let events = events_until_complete(&test).await;

    assert_request_models(&mock, &[PRIMARY, PRIMARY, FALLBACK]);
    let requests = mock.requests();
    for request in [&requests[1], &requests[2]] {
        assert_one_output(request, CALL_ID);
        let relevant = user_input_texts(request)
            .into_iter()
            .filter(|text| text == INITIAL_PROMPT || text == STEER_PROMPT)
            .collect::<Vec<_>>();
        assert_eq!(
            relevant,
            vec![INITIAL_PROMPT.to_string(), STEER_PROMPT.to_string()]
        );
    }
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_))),
        "transparent reroute should not emit a turn error"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn steering_required_mcp_survives_precaptured_step_and_midturn_reroute() -> Result<()> {
    skip_if_no_network!(Ok(()));

    const CALL_ID: &str = "routing-required-mcp-wait";
    const MCP_SERVER: &str = "sample";
    let command = match stdio_server_bin() {
        Ok(command) => command,
        Err(err) => {
            eprintln!("test_stdio_server binary unavailable, skipping: {err}");
            return Ok(());
        }
    };
    let server = start_mock_server().await;
    let mock = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("required-mcp-wait-response"),
                ev_function_call_with_namespace(
                    CALL_ID,
                    "collaboration",
                    "wait_agent",
                    r#"{"timeout_ms":10000}"#,
                ),
                ev_completed("required-mcp-wait-response"),
            ])),
            overload_response("required-mcp-continuation-failed"),
            sse_response(sse_completed("required-mcp-fallback-response")),
        ],
    )
    .await;
    let codex_home = Arc::new(TempDir::new()?);
    let initialize_barrier = codex_home.path().join("allow-required-mcp-initialize");
    let mcp_server = serde_json::from_value::<McpServerConfig>(json!({
        "command": command,
        "env": {
            "MCP_TEST_INITIALIZE_BARRIER_FILE": initialize_barrier.to_string_lossy(),
        },
        "enabled_tools": ["echo"],
        "startup_timeout_sec": 10,
    }))?;
    let test = test_codex()
        .with_home(codex_home)
        .with_config(move |config| {
            config.model = Some(PROFILE.to_string());
            config.custom_models = routing_models();
            config
                .features
                .enable(Feature::MultiAgentV2)
                .expect("test config should allow multi-agent v2");
            let mut servers = config.mcp_servers.get().clone();
            servers.insert(MCP_SERVER.to_string(), mcp_server);
            config
                .mcp_servers
                .set(servers)
                .expect("test config should accept its MCP server");
        })
        .build(&server)
        .await?;

    submit_prompt(&test, "wait before using a required server").await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::CollabWaitingBegin(_))
    })
    .await;
    submit_thread_settings(
        &test.codex,
        ThreadSettingsOverrides {
            effort: Some(Some(ReasoningEffort::High)),
            ..Default::default()
        },
    )
    .await?;
    let submission = test
        .codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Mention {
            name: MCP_SERVER.to_string(),
            path: format!("mcp://{MCP_SERVER}"),
        }]))
        .await
        .expect("steering should be accepted");
    assert!(matches!(submission, TurnInputSubmission::Steered { .. }));

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        mock.requests().len(),
        1,
        "the continuation should wait for the steering-required MCP server"
    );
    std::fs::write(&initialize_barrier, "ready")?;
    wait_for_mcp_server(&test.codex, MCP_SERVER).await?;
    let events = events_until_complete(&test).await;

    assert_request_models(&mock, &[PRIMARY, PRIMARY, FALLBACK]);
    let requests = mock.requests();
    for request in [&requests[1], &requests[2]] {
        assert!(
            request.tool_by_name("mcp__sample", "echo").is_some(),
            "steering-required MCP tool should survive the route change: {}",
            request.body_json()
        );
    }
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_))),
        "transparent reroute should not emit a turn error"
    );

    Ok(())
}
