use std::collections::HashMap;

use anyhow::Result;
use codex_features::Feature;
use codex_models_manager::CustomModelConfig;
use codex_models_manager::ModelRoutingCandidate;
use codex_models_manager::ModelRoutingProfile;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::Settings;
use codex_protocol::items::AgentMessageContent;
use codex_protocol::items::TurnItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::user_input::UserInput;
use core_test_support::PathBufExt;
use core_test_support::responses::ResponseMock;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_response_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::sse_response;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::StreamingSseServer;
use core_test_support::streaming_sse::start_streaming_sse_server;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use tokio::sync::oneshot;

const PROFILE: &str = "test-stream-route";
const PRIMARY: &str = "test-stream-primary";
const FALLBACK: &str = "test-stream-fallback";

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

async fn build_routed_test(server: &wiremock::MockServer) -> Result<TestCodex> {
    test_codex()
        .with_config(|config| {
            config.model = Some(PROFILE.to_string());
            config.custom_models = routing_models();
        })
        .build(server)
        .await
}

async fn build_routed_streaming_test(server: &StreamingSseServer) -> Result<TestCodex> {
    test_codex()
        .with_config(|config| {
            config.model = Some(PROFILE.to_string());
            config.custom_models = routing_models();
        })
        .build_with_streaming_server(server)
        .await
}

async fn submit_prompt(test: &TestCodex) -> Result<()> {
    submit_prompt_with_settings(test, ThreadSettingsOverrides::default()).await
}

async fn submit_prompt_with_settings(
    test: &TestCodex,
    thread_settings: ThreadSettingsOverrides,
) -> Result<()> {
    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "stream this request".to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings,
        })
        .await?;
    Ok(())
}

fn chunk(event: Value) -> StreamingSseChunk {
    StreamingSseChunk {
        gate: None,
        body: sse(vec![event]),
    }
}

fn gated_chunk(gate: oneshot::Receiver<()>, event: Value) -> StreamingSseChunk {
    StreamingSseChunk {
        gate: Some(gate),
        body: sse(vec![event]),
    }
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

fn response_failed(response_id: &str) -> Value {
    json!({
        "type": "response.failed",
        "response": {
            "id": response_id,
            "status": "failed",
            "error": {
                "code": "server_is_overloaded",
                "message": "temporary stream diagnostic"
            }
        }
    })
}

fn assistant_item_added(item_id: &str) -> Value {
    json!({
        "type": "response.output_item.added",
        "item": {
            "type": "message",
            "role": "assistant",
            "id": item_id,
            "phase": "commentary",
            "content": [{"type": "output_text", "text": ""}]
        }
    })
}

fn untagged_assistant_item_added(item_id: &str) -> Value {
    json!({
        "type": "response.output_item.added",
        "item": {
            "type": "message",
            "role": "assistant",
            "id": item_id,
            "content": [{"type": "output_text", "text": ""}]
        }
    })
}

fn output_text_delta(delta: &str) -> Value {
    json!({
        "type": "response.output_text.delta",
        "delta": delta,
    })
}

fn assistant_item_done(item_id: &str, text: &str) -> Value {
    json!({
        "type": "response.output_item.done",
        "item": {
            "type": "message",
            "role": "assistant",
            "id": item_id,
            "phase": "commentary",
            "content": [{"type": "output_text", "text": text}]
        }
    })
}

fn request_models(mock: &ResponseMock) -> Vec<String> {
    mock.requests()
        .into_iter()
        .map(|request| {
            request.body_json()["model"]
                .as_str()
                .expect("request model")
                .to_string()
        })
        .collect()
}

fn assistant_texts(request: &core_test_support::responses::ResponsesRequest) -> Vec<String> {
    request
        .input()
        .into_iter()
        .filter(|item| item["type"] == "message" && item["role"] == "assistant")
        .filter_map(|item| item["content"].as_array().cloned())
        .flatten()
        .filter(|content| content["type"] == "output_text")
        .filter_map(|content| content["text"].as_str().map(str::to_string))
        .collect()
}

fn user_texts_from_body(body: &Value) -> Vec<String> {
    body["input"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|item| item["type"] == "message" && item["role"] == "user")
        .filter_map(|item| item["content"].as_array())
        .flatten()
        .filter(|content| content["type"] == "input_text")
        .filter_map(|content| content["text"].as_str().map(str::to_string))
        .collect()
}

fn agent_message_text(item: &TurnItem) -> Option<String> {
    let TurnItem::AgentMessage(message) = item else {
        return None;
    };
    Some(
        message
            .content
            .iter()
            .map(|content| match content {
                AgentMessageContent::Text { text } => text.as_str(),
            })
            .collect(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_commentary_is_checkpointed_before_streaming_reroute() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let item_id = "streamed-commentary";
    let first_delta = "checking the ";
    let second_delta = "request";
    let prefix = format!("{first_delta}{second_delta}");
    let mock = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("response-primary"),
                assistant_item_added(item_id),
                output_text_delta(first_delta),
                output_text_delta(second_delta),
                response_failed("response-primary"),
            ])),
            sse_response(sse(vec![
                ev_response_created("response-fallback"),
                ev_assistant_message("fallback-message", "finished"),
                ev_completed("response-fallback"),
            ])),
        ],
    )
    .await;
    let test = build_routed_test(&server).await?;

    submit_prompt(&test).await?;
    let events = events_until_complete(&test).await;

    assert_eq!(request_models(&mock), vec![PRIMARY, FALLBACK]);
    assert_eq!(assistant_texts(&mock.requests()[1]), vec![prefix.clone()]);

    let started = events
        .iter()
        .filter_map(|event| match event {
            EventMsg::ItemStarted(event) if event.item.id() == item_id => Some(&event.item),
            _ => None,
        })
        .collect::<Vec<_>>();
    let completed = events
        .iter()
        .filter_map(|event| match event {
            EventMsg::ItemCompleted(event) if event.item.id() == item_id => Some(&event.item),
            _ => None,
        })
        .collect::<Vec<_>>();
    let deltas = events
        .iter()
        .filter_map(|event| match event {
            EventMsg::AgentMessageContentDelta(event) if event.item_id == item_id => {
                Some(event.delta.as_str())
            }
            _ => None,
        })
        .collect::<String>();
    assert_eq!(started.len(), 1);
    assert_eq!(agent_message_text(started[0]).as_deref(), Some(""));
    assert_eq!(deltas, prefix);
    assert_eq!(completed.len(), 1);
    assert_eq!(agent_message_text(completed[0]), Some(prefix));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::ModelReroute(_)))
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_)))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_commentary_before_failure_is_not_duplicated() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let item_id = "completed-commentary";
    let text = "completed before the failure";
    let mock = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("response-primary"),
                assistant_item_added(item_id),
                output_text_delta(text),
                assistant_item_done(item_id, text),
                response_failed("response-primary"),
            ])),
            sse_response(sse(vec![
                ev_response_created("response-fallback"),
                ev_assistant_message("fallback-message", "finished"),
                ev_completed("response-fallback"),
            ])),
        ],
    )
    .await;
    let test = build_routed_test(&server).await?;

    submit_prompt(&test).await?;
    let events = events_until_complete(&test).await;

    assert_eq!(request_models(&mock), vec![PRIMARY, FALLBACK]);
    assert_eq!(assistant_texts(&mock.requests()[1]), vec![text.to_string()]);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_)))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incomplete_tool_arguments_are_recorded_as_not_executed_before_reroute() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mock = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("response-primary"),
                json!({
                    "type": "response.output_item.added",
                    "item": {
                        "type": "function_call",
                        "id": "incomplete-tool-item",
                        "call_id": "incomplete-tool-call",
                        "name": "test_sync_tool",
                        "arguments": ""
                    }
                }),
                json!({
                    "type": "response.function_call_arguments.delta",
                    "item_id": "incomplete-tool-item",
                    "call_id": "incomplete-tool-call",
                    "delta": "{\"value\":"
                }),
                response_failed("response-primary"),
            ])),
            sse_response(sse(vec![
                ev_response_created("response-fallback"),
                ev_assistant_message("fallback-message", "finished"),
                ev_completed("response-fallback"),
            ])),
        ],
    )
    .await;
    let test = build_routed_test(&server).await?;

    submit_prompt(&test).await?;
    let events = events_until_complete(&test).await;

    assert_eq!(request_models(&mock), vec![PRIMARY, FALLBACK]);
    let fallback_input = mock.requests()[1].input();
    let call = fallback_input
        .iter()
        .find(|item| item["type"] == "function_call")
        .expect("partial function call should be retained");
    assert_eq!(call["call_id"], "incomplete-tool-call");
    assert_eq!(call["arguments"], "{\"value\":");
    let output = fallback_input
        .iter()
        .find(|item| item["type"] == "function_call_output")
        .expect("partial function call should have a terminal output");
    assert_eq!(output["call_id"], "incomplete-tool-call");
    assert_eq!(
        output["output"],
        "Tool call was not executed because the model request ended before the call completed."
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::DynamicToolCallRequest(_)))
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::ModelReroute(_)))
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_)))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incomplete_apply_patch_is_recorded_without_execution_before_reroute() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let workspace = tempfile::tempdir()?;
    let workspace_path = workspace.path().to_path_buf();
    let partial_patch = "*** Begin Patch\n*** Add File: should-not-exist.txt\n+partial";
    let mock = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("response-primary"),
                json!({
                    "type": "response.output_item.added",
                    "item": {
                        "type": "custom_tool_call",
                        "id": "incomplete-patch-item",
                        "call_id": "incomplete-patch-call",
                        "name": "apply_patch",
                        "input": ""
                    }
                }),
                json!({
                    "type": "response.custom_tool_call_input.delta",
                    "item_id": "incomplete-patch-item",
                    "call_id": "incomplete-patch-call",
                    "delta": partial_patch
                }),
                response_failed("response-primary"),
            ])),
            sse_response(sse(vec![
                ev_response_created("response-fallback"),
                ev_assistant_message("fallback-message", "finished"),
                ev_completed("response-fallback"),
            ])),
        ],
    )
    .await;
    let test = test_codex()
        .with_config(move |config| {
            config.model = Some(PROFILE.to_string());
            config.custom_models = routing_models();
            config.cwd = workspace_path.abs();
            config
                .features
                .enable(Feature::ApplyPatchStreamingEvents)
                .expect("enable apply_patch streaming events");
        })
        .build(&server)
        .await?;

    submit_prompt(&test).await?;
    let events = events_until_complete(&test).await;

    assert_eq!(request_models(&mock), vec![PRIMARY, FALLBACK]);
    assert!(!workspace.path().join("should-not-exist.txt").exists());
    let fallback_input = mock.requests()[1].input();
    let call = fallback_input
        .iter()
        .find(|item| item["type"] == "custom_tool_call")
        .expect("partial custom tool call should be retained");
    assert_eq!(call["call_id"], "incomplete-patch-call");
    assert_eq!(call["input"], partial_patch);
    let output = fallback_input
        .iter()
        .find(|item| item["type"] == "custom_tool_call_output")
        .expect("partial custom tool call should have a terminal output");
    assert_eq!(output["call_id"], "incomplete-patch-call");
    assert_eq!(
        output["output"],
        "Tool call was not executed because the model request ended before the call completed."
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::DynamicToolCallRequest(_)))
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_)))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incomplete_provider_managed_item_uses_bounded_continuation_before_reroute() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mock = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("response-primary"),
                json!({
                    "type": "response.output_item.added",
                    "item": {
                        "type": "web_search_call",
                        "id": "incomplete-web-search",
                        "status": "in_progress",
                        "action": {"type": "search", "query": "public test query"}
                    }
                }),
                response_failed("response-primary"),
            ])),
            sse_response(sse(vec![
                ev_response_created("response-fallback"),
                ev_assistant_message("fallback-message", "finished"),
                ev_completed("response-fallback"),
            ])),
        ],
    )
    .await;
    let test = build_routed_test(&server).await?;

    submit_prompt(&test).await?;
    let events = events_until_complete(&test).await;

    assert_eq!(request_models(&mock), vec![PRIMARY, FALLBACK]);
    let fallback_body = mock.requests()[1].body_json().to_string();
    assert!(!fallback_body.contains("web_search_call"));
    assert!(fallback_body.contains("<interrupted_response>"));
    assert!(
        fallback_body
            .contains("No unfinished provider-managed operation should be assumed successful.")
    );
    assert!(!fallback_body.contains("temporary stream diagnostic"));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                EventMsg::ItemStarted(event) if event.item.id() == "incomplete-web-search"
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                EventMsg::ItemCompleted(event) if event.item.id() == "incomplete-web-search"
            ))
            .count(),
        1
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_)))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_plan_mode_message_reroutes_with_balanced_lifecycle() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let item_id = "partial-plan-message";
    let prefix = "considering the plan";
    let mock = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("response-primary"),
                assistant_item_added(item_id),
                output_text_delta(prefix),
                response_failed("response-primary"),
            ])),
            sse_response(sse(vec![
                ev_response_created("response-fallback"),
                ev_assistant_message("fallback-message", "finished"),
                ev_completed("response-fallback"),
            ])),
        ],
    )
    .await;
    let test = build_routed_test(&server).await?;

    submit_prompt_with_settings(
        &test,
        ThreadSettingsOverrides {
            collaboration_mode: Some(CollaborationMode {
                mode: ModeKind::Plan,
                settings: Settings {
                    model: PROFILE.to_string(),
                    reasoning_effort: None,
                    developer_instructions: None,
                },
            }),
            ..Default::default()
        },
    )
    .await?;
    let events = events_until_complete(&test).await;

    assert_eq!(request_models(&mock), vec![PRIMARY, FALLBACK]);
    assert_eq!(
        assistant_texts(&mock.requests()[1]),
        vec![prefix.to_string()]
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                EventMsg::ItemStarted(event) if event.item.id() == item_id
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                EventMsg::ItemCompleted(event) if event.item.id() == item_id
            ))
            .count(),
        1
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_)))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_steer_follows_untagged_partial_message_into_reroute() -> Result<()> {
    skip_if_no_network!(Ok(()));

    const INITIAL_PROMPT: &str = "start the streamed request";
    const STEER_PROMPT: &str = "include this steer";
    const PREFIX: &str = "partial untagged answer";

    let (release_failure, wait_for_release) = oneshot::channel();
    let first_response = vec![
        chunk(ev_response_created("response-primary")),
        chunk(untagged_assistant_item_added("untagged-message")),
        chunk(output_text_delta(PREFIX)),
        gated_chunk(wait_for_release, response_failed("response-primary")),
    ];
    let second_response = vec![
        chunk(ev_response_created("response-fallback")),
        chunk(ev_assistant_message("fallback-message", "finished")),
        chunk(ev_completed("response-fallback")),
    ];
    let (server, _completions) =
        start_streaming_sse_server(vec![first_response, second_response]).await;
    let test = build_routed_streaming_test(&server).await?;

    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: INITIAL_PROMPT.to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await?;
    wait_for_event(
        &test.codex,
        |event| matches!(event, EventMsg::AgentMessageContentDelta(event) if event.delta == PREFIX),
    )
    .await;
    test.codex
        .steer_input(
            vec![UserInput::Text {
                text: STEER_PROMPT.to_string(),
                text_elements: Vec::new(),
            }],
            Default::default(),
            /*expected_turn_id*/ None,
            /*client_user_message_id*/ None,
            /*responsesapi_client_metadata*/ None,
        )
        .await
        .map_err(|err| anyhow::anyhow!("steer input failed: {err:?}"))?;
    release_failure
        .send(())
        .expect("streaming failure gate should still be waiting");
    let events = events_until_complete(&test).await;

    let requests = server.requests().await;
    let request_bodies = requests
        .iter()
        .map(|request| serde_json::from_slice::<Value>(request).expect("request JSON"))
        .collect::<Vec<_>>();
    assert_eq!(
        request_bodies
            .iter()
            .map(|body| body["model"].as_str().expect("request model"))
            .collect::<Vec<_>>(),
        vec![PRIMARY, FALLBACK]
    );
    assert_eq!(
        user_texts_from_body(&request_bodies[1])
            .into_iter()
            .filter(|text| text == INITIAL_PROMPT || text == STEER_PROMPT)
            .collect::<Vec<_>>(),
        vec![INITIAL_PROMPT.to_string(), STEER_PROMPT.to_string()]
    );
    assert_eq!(
        request_bodies[1]["input"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|item| item["type"] == "message" && item["role"] == "assistant")
            .filter_map(|item| item["content"].as_array())
            .flatten()
            .filter_map(|content| content["text"].as_str())
            .collect::<Vec<_>>(),
        vec![PREFIX]
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_)))
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupting_partial_output_does_not_leave_continuation_guidance() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let (release_stream, wait_for_release) = oneshot::channel();
    let interrupted_chunks = vec![
        chunk(ev_response_created("response-interrupted")),
        chunk(assistant_item_added("interrupted-message")),
        chunk(output_text_delta("visible before interrupt")),
        gated_chunk(wait_for_release, ev_completed("response-interrupted")),
    ];
    let completed_chunks = vec![
        chunk(ev_response_created("response-next-turn")),
        chunk(ev_assistant_message("next-turn-message", "finished")),
        chunk(ev_completed("response-next-turn")),
    ];
    let (server, _) = start_streaming_sse_server(vec![interrupted_chunks, completed_chunks]).await;
    let test = build_routed_streaming_test(&server).await?;

    submit_prompt(&test).await?;
    wait_for_event(&test.codex, |event| {
        matches!(
            event,
            EventMsg::AgentMessageContentDelta(event)
                if event.delta == "visible before interrupt"
        )
    })
    .await;
    test.codex.submit(Op::Interrupt).await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnAborted(_))
    })
    .await;
    let _ = release_stream.send(());

    submit_prompt(&test).await?;
    let _events = events_until_complete(&test).await;

    let requests = server.requests().await;
    assert_eq!(requests.len(), 2);
    let second_request: Value = serde_json::from_slice(&requests[1])?;
    assert!(!second_request.to_string().contains("interrupted_response"));

    Ok(())
}
