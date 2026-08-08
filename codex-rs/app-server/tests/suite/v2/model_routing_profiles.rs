use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use codex_app_server_protocol::AgentMessageDeltaNotification;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ItemCompletedNotification;
use codex_app_server_protocol::ItemStartedNotification;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::UserInput;
use codex_features::Feature;
use core_test_support::responses;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::time::timeout;
use wiremock::ResponseTemplate;

const READ_TIMEOUT: Duration = Duration::from_secs(10);
const PROFILE: &str = "test-route";
const RENAMED_PROFILE: &str = "test-route-renamed";
const PRIMARY: &str = "test-primary";
const FALLBACK: &str = "test-fallback";
const ADDED: &str = "test-added";
const PRIMARY_TIER: &str = "tier-priority";
const TOOL_CALL_ID: &str = "route-plan-call";
const PARTIAL_MESSAGE_ID: &str = "partial-route-message";
const PARTIAL_TEXT: &str = "partial answer";

#[tokio::test]
async fn routed_profile_falls_back_and_reloads_config_between_turns() -> Result<()> {
    let server = responses::start_mock_server().await;
    let overloaded = ResponseTemplate::new(503).set_body_json(serde_json::json!({
        "error": {
            "code": "server_is_overloaded",
            "param": "model",
            "message": "temporary provider condition"
        }
    }));
    let fallback_success = responses::sse_response(responses::sse(vec![
        responses::ev_response_created("resp-fallback"),
        responses::ev_assistant_message("msg-fallback", "fallback complete"),
        responses::ev_completed("resp-fallback"),
    ]));
    let added_success = responses::sse_response(responses::sse(vec![
        responses::ev_response_created("resp-added"),
        responses::ev_assistant_message("msg-added", "updated profile complete"),
        responses::ev_completed("resp-added"),
    ]));
    let retained_success = responses::sse_response(responses::sse(vec![
        responses::ev_response_created("resp-retained"),
        responses::ev_assistant_message("msg-retained", "retained profile complete"),
        responses::ev_completed("resp-retained"),
    ]));
    let detached_success = responses::sse_response(responses::sse(vec![
        responses::ev_response_created("resp-detached"),
        responses::ev_assistant_message("msg-detached", "detached profile complete"),
        responses::ev_completed("resp-detached"),
    ]));
    let response_mock = responses::mount_response_sequence(
        &server,
        vec![
            overloaded,
            fallback_success,
            added_success,
            retained_success,
            detached_success,
        ],
    )
    .await;

    let codex_home = TempDir::new()?;
    write_profile_config(
        codex_home.path(),
        &server.uri(),
        PROFILE,
        /*include_added*/ false,
    )?;

    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let ThreadStartResponse { thread, .. } = app
        .start_thread(ThreadStartParams {
            model: Some(PROFILE.to_string()),
            ..Default::default()
        })
        .await?;

    app.request::<TurnStartResponse>(|request_id| ClientRequest::TurnStart {
        request_id,
        params: turn_start_params(&thread.id, "use the configured route"),
    })
    .await?;
    assert_no_reroute_until_turn_completed(&mut app).await?;

    write_profile_config(
        codex_home.path(),
        &server.uri(),
        PROFILE,
        /*include_added*/ true,
    )?;
    app.request::<TurnStartResponse>(|request_id| ClientRequest::TurnStart {
        request_id,
        params: turn_start_params(&thread.id, "use the updated route"),
    })
    .await?;
    assert_no_reroute_until_turn_completed(&mut app).await?;

    write_profile_config(
        codex_home.path(),
        &server.uri(),
        RENAMED_PROFILE,
        /*include_added*/ true,
    )?;
    app.request::<TurnStartResponse>(|request_id| ClientRequest::TurnStart {
        request_id,
        params: turn_start_params(&thread.id, "use the renamed route"),
    })
    .await?;
    assert_no_reroute_until_turn_completed(&mut app).await?;

    write_config_without_profile(codex_home.path(), &server.uri())?;
    app.request::<TurnStartResponse>(|request_id| ClientRequest::TurnStart {
        request_id,
        params: turn_start_params(&thread.id, "retain the active route"),
    })
    .await?;
    assert_no_reroute_until_turn_completed(&mut app).await?;

    let request_models = response_mock
        .requests()
        .into_iter()
        .map(|request| {
            request.body_json()["model"]
                .as_str()
                .expect("request model")
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(request_models, vec![PRIMARY, FALLBACK, ADDED, ADDED, ADDED]);

    Ok(())
}

#[tokio::test]
async fn routed_profile_falls_back_after_tool_continuation_failure() -> Result<()> {
    let server = responses::start_mock_server().await;
    let plan_args = serde_json::json!({
        "explanation": "Record the routing checkpoint",
        "plan": [{"step": "Continue the routed turn", "status": "in_progress"}],
    })
    .to_string();
    let tool_response = responses::sse_response(responses::sse(vec![
        responses::ev_response_created("resp-primary-tool"),
        responses::ev_assistant_message("msg-primary-tool", "checking the next step"),
        responses::ev_function_call(TOOL_CALL_ID, "update_plan", &plan_args),
        responses::ev_completed("resp-primary-tool"),
    ]));
    let overloaded = ResponseTemplate::new(503).set_body_json(serde_json::json!({
        "error": {
            "code": "server_is_overloaded",
            "param": "model",
            "message": "temporary provider condition"
        }
    }));
    let fallback_success = responses::sse_response(responses::sse(vec![
        responses::ev_response_created("resp-fallback-final"),
        responses::ev_assistant_message("msg-fallback-final", "route completed"),
        responses::ev_completed("resp-fallback-final"),
    ]));
    let response_mock = responses::mount_response_sequence(
        &server,
        vec![tool_response, overloaded, fallback_success],
    )
    .await;

    let codex_home = TempDir::new()?;
    write_profile_config(
        codex_home.path(),
        &server.uri(),
        PROFILE,
        /*include_added*/ false,
    )?;
    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let ThreadStartResponse { thread, .. } = app
        .start_thread(ThreadStartParams {
            model: Some(PROFILE.to_string()),
            ..Default::default()
        })
        .await?;
    app.request::<TurnStartResponse>(|request_id| ClientRequest::TurnStart {
        request_id,
        params: turn_start_params(&thread.id, "use a tool and finish this turn"),
    })
    .await?;

    let mut plan_updates = 0;
    let mut errors = 0;
    let mut completed = 0;
    while completed == 0 {
        let message = timeout(READ_TIMEOUT, app.read_next_message()).await??;
        let JSONRPCMessage::Notification(notification) = message else {
            continue;
        };
        match notification.method.as_str() {
            "model/rerouted" => anyhow::bail!(
                "routing profiles must not emit the safety-specific model/rerouted notification"
            ),
            "turn/plan/updated" => plan_updates += 1,
            "error" => errors += 1,
            "turn/completed" => completed += 1,
            _ => {}
        }
    }

    assert_eq!(plan_updates, 1, "the tool should execute exactly once");
    assert_eq!(errors, 0, "transparent rerouting must not emit an error");
    assert_eq!(completed, 1);

    let requests = response_mock.requests();
    assert_eq!(
        requests
            .iter()
            .map(|request| {
                request.body_json()["model"]
                    .as_str()
                    .expect("request model")
                    .to_string()
            })
            .collect::<Vec<_>>(),
        vec![
            PRIMARY.to_string(),
            PRIMARY.to_string(),
            FALLBACK.to_string(),
        ]
    );
    assert_eq!(
        requests[0].inputs_of_type("function_call_output"),
        Vec::<serde_json::Value>::new()
    );
    assert_eq!(
        requests[1].inputs_of_type("function_call_output"),
        vec![requests[1].function_call_output(TOOL_CALL_ID)]
    );
    assert_eq!(
        requests[2].inputs_of_type("function_call_output"),
        vec![requests[2].function_call_output(TOOL_CALL_ID)]
    );

    Ok(())
}

#[tokio::test]
async fn routed_profile_balances_partial_message_before_falling_back() -> Result<()> {
    let server = responses::start_mock_server().await;
    let partial_failure = responses::sse_response(responses::sse(vec![
        responses::ev_response_created("resp-primary-partial"),
        responses::ev_message_item_added(PARTIAL_MESSAGE_ID, ""),
        responses::ev_output_text_delta("partial "),
        responses::ev_output_text_delta("answer"),
        serde_json::json!({
            "type": "response.failed",
            "response": {
                "id": "resp-primary-partial",
                "status": "failed",
                "error": {
                    "code": "server_is_overloaded",
                    "message": "temporary provider condition"
                }
            }
        }),
    ]));
    let fallback_success = responses::sse_response(responses::sse(vec![
        responses::ev_response_created("resp-fallback-after-partial"),
        responses::ev_assistant_message("msg-fallback-after-partial", "route completed"),
        responses::ev_completed("resp-fallback-after-partial"),
    ]));
    let response_mock =
        responses::mount_response_sequence(&server, vec![partial_failure, fallback_success]).await;

    let codex_home = TempDir::new()?;
    write_profile_config(
        codex_home.path(),
        &server.uri(),
        PROFILE,
        /*include_added*/ false,
    )?;
    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let ThreadStartResponse { thread, .. } = app
        .start_thread(ThreadStartParams {
            model: Some(PROFILE.to_string()),
            ..Default::default()
        })
        .await?;
    app.request::<TurnStartResponse>(|request_id| ClientRequest::TurnStart {
        request_id,
        params: turn_start_params(&thread.id, "continue after a partial response"),
    })
    .await?;

    let mut partial_started = Vec::new();
    let mut partial_deltas = Vec::new();
    let mut partial_completed = Vec::new();
    let mut relevant_order = Vec::new();
    let mut errors = 0;
    let mut completed = 0;
    while completed == 0 {
        let message = timeout(READ_TIMEOUT, app.read_next_message()).await??;
        let JSONRPCMessage::Notification(notification) = message else {
            continue;
        };
        match notification.method.as_str() {
            "item/started" => {
                let payload: ItemStartedNotification = serde_json::from_value(
                    notification
                        .params
                        .ok_or_else(|| anyhow::anyhow!("item/started is missing params"))?,
                )?;
                if matches!(&payload.item, ThreadItem::AgentMessage { id, .. } if id == PARTIAL_MESSAGE_ID)
                {
                    relevant_order.push("partial-started");
                    partial_started.push(payload.item);
                }
            }
            "item/agentMessage/delta" => {
                let payload: AgentMessageDeltaNotification =
                    serde_json::from_value(notification.params.ok_or_else(|| {
                        anyhow::anyhow!("agent message delta is missing params")
                    })?)?;
                if payload.item_id == PARTIAL_MESSAGE_ID {
                    relevant_order.push("partial-delta");
                    partial_deltas.push(payload.delta);
                }
            }
            "item/completed" => {
                let payload: ItemCompletedNotification = serde_json::from_value(
                    notification
                        .params
                        .ok_or_else(|| anyhow::anyhow!("item/completed is missing params"))?,
                )?;
                if matches!(&payload.item, ThreadItem::AgentMessage { id, .. } if id == PARTIAL_MESSAGE_ID)
                {
                    relevant_order.push("partial-completed");
                    partial_completed.push(payload.item);
                }
            }
            "model/rerouted" => anyhow::bail!(
                "routing profiles must not emit the safety-specific model/rerouted notification"
            ),
            "error" => errors += 1,
            "turn/completed" => completed += 1,
            _ => {}
        }
    }

    assert_eq!(
        partial_started,
        vec![ThreadItem::AgentMessage {
            id: PARTIAL_MESSAGE_ID.to_string(),
            text: String::new(),
            phase: None,
            delivery: None,
            memory_citation: None,
        }]
    );
    assert_eq!(partial_deltas, vec!["partial ", "answer"]);
    assert_eq!(
        partial_completed,
        vec![ThreadItem::AgentMessage {
            id: PARTIAL_MESSAGE_ID.to_string(),
            text: PARTIAL_TEXT.to_string(),
            phase: None,
            delivery: None,
            memory_citation: None,
        }]
    );
    assert_eq!(
        relevant_order,
        vec![
            "partial-started",
            "partial-delta",
            "partial-delta",
            "partial-completed",
        ]
    );
    assert_eq!(errors, 0);
    assert_eq!(completed, 1);

    let requests = response_mock.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[1]
            .inputs_of_type("message")
            .into_iter()
            .filter(|item| item["role"] == "assistant")
            .flat_map(|item| item["content"].as_array().cloned().unwrap_or_default())
            .filter(|content| {
                content["type"] == "output_text" && content["text"] == PARTIAL_TEXT
            })
            .count(),
        1
    );

    Ok(())
}

fn turn_start_params(thread_id: &str, text: &str) -> TurnStartParams {
    TurnStartParams {
        thread_id: thread_id.to_string(),
        client_user_message_id: None,
        input: vec![UserInput::Text {
            text: text.to_string(),
            text_elements: Vec::new(),
        }],
        ..Default::default()
    }
}

async fn assert_no_reroute_until_turn_completed(app: &mut TestAppServer) -> Result<()> {
    loop {
        let message = timeout(READ_TIMEOUT, app.read_next_message()).await??;
        let JSONRPCMessage::Notification(notification) = message else {
            continue;
        };
        match notification.method.as_str() {
            "model/rerouted" => anyhow::bail!(
                "routing profiles must not emit the safety-specific model/rerouted notification"
            ),
            "turn/completed" => return Ok(()),
            _ => {}
        }
    }
}

fn write_profile_config(
    codex_home: &Path,
    server_uri: &str,
    profile: &str,
    include_added: bool,
) -> Result<()> {
    let candidates = if include_added {
        format!("  {{ model = \"{ADDED}\", reasoning_effort = \"low\" }},")
    } else {
        format!(
            "  {{ model = \"{PRIMARY}\", reasoning_effort = \"medium\", service_tier = \"{PRIMARY_TIER}\" }},\n  {{ model = \"{FALLBACK}\", reasoning_effort = \"high\" }},"
        )
    };
    MockResponsesConfig::new(server_uri)
        .with_model(profile)
        .disable_feature(Feature::RemoteModels)
        .enable_feature(Feature::FastMode)
        .with_extra_config(&format!(
            r#"
[[custom_models]]
name = "{profile}"
candidates = [
{candidates}
]
"#
        ))
        .write(codex_home)?;
    Ok(())
}

fn write_config_without_profile(codex_home: &Path, server_uri: &str) -> Result<()> {
    MockResponsesConfig::new(server_uri)
        .with_model(PROFILE)
        .disable_feature(Feature::RemoteModels)
        .enable_feature(Feature::FastMode)
        .write(codex_home)?;
    Ok(())
}
