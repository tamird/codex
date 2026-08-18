use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use codex_core::SleepFuture;
use codex_core::TimeFuture;
use codex_core::TimeProvider;
use codex_core::config::CurrentTimeReminderConfig;
use codex_features::CurrentTimeSource;
use codex_features::Feature;
use codex_history::RolloutItem;
use codex_history::RolloutLine;
use codex_models_manager::CustomModelConfig;
use codex_models_manager::ModelRoutingCandidate;
use codex_models_manager::ModelRoutingProfile;
use codex_models_manager::model_info::model_info_from_slug;
use codex_protocol::ThreadId;
use codex_protocol::items::TurnItem;
use codex_protocol::openai_models::ModelInfo;
use codex_protocol::openai_models::ModelServiceTier;
use codex_protocol::openai_models::ModelsResponse;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::openai_models::ReasoningEffortPreset;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HookEventName;
use codex_protocol::protocol::Op;
use codex_protocol::turn_input::TurnInputRequest;
use codex_protocol::user_input::UserInput;
use core_test_support::hooks::trust_discovered_hooks;
use core_test_support::responses::ResponseMock;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_completed_with_tokens;
use core_test_support::responses::ev_function_call;
use core_test_support::responses::ev_reasoning_item_added;
use core_test_support::responses::ev_reasoning_summary_text_delta;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_response_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::sse_completed;
use core_test_support::responses::sse_response;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use core_test_support::wait_for_event_match;
use pretty_assertions::assert_eq;
use serde_json::json;
use tokio::sync::Notify;
use wiremock::ResponseTemplate;

const PROFILE: &str = "test-route";
const PRIMARY: &str = "test-primary";
const FALLBACK: &str = "test-fallback";
const SECOND_FALLBACK: &str = "test-second-fallback";
const ROUTING_TIME_UNIX_SECONDS: i64 = 1_800_000_000;

struct RoutingTimeProvider {
    current_time: AtomicI64,
    sleeps: Mutex<Vec<Duration>>,
}

impl RoutingTimeProvider {
    fn new() -> Self {
        Self {
            current_time: AtomicI64::new(ROUTING_TIME_UNIX_SECONDS),
            sleeps: Mutex::new(Vec::new()),
        }
    }

    fn advance(&self, duration: Duration) {
        self.current_time.fetch_add(
            i64::try_from(duration.as_secs()).expect("test duration should fit in i64"),
            Ordering::Relaxed,
        );
    }

    fn sleeps(&self) -> Vec<Duration> {
        self.sleeps
            .lock()
            .expect("routing time sleep lock should not be poisoned")
            .clone()
    }
}

impl TimeProvider for RoutingTimeProvider {
    fn current_time(&self, _thread_id: ThreadId) -> TimeFuture<'_> {
        let timestamp = self.current_time.load(Ordering::Relaxed);
        Box::pin(async move {
            Ok(DateTime::<Utc>::from_timestamp(timestamp, 0)
                .expect("test timestamp should be valid"))
        })
    }

    fn sleep(&self, _thread_id: ThreadId, duration: Duration) -> SleepFuture<'_> {
        Box::pin(async move {
            self.sleeps
                .lock()
                .expect("routing time sleep lock should not be poisoned")
                .push(duration);
            self.advance(duration);
            Ok(())
        })
    }
}

struct BlockingRoutingTimeProvider {
    current_time: AtomicI64,
    sleeps: Mutex<Vec<Duration>>,
    sleep_started: Notify,
}

impl BlockingRoutingTimeProvider {
    fn new() -> Self {
        Self {
            current_time: AtomicI64::new(ROUTING_TIME_UNIX_SECONDS),
            sleeps: Mutex::new(Vec::new()),
            sleep_started: Notify::new(),
        }
    }

    async fn wait_until_sleeping(&self) {
        loop {
            let notified = self.sleep_started.notified();
            if !self
                .sleeps
                .lock()
                .expect("routing time sleep lock should not be poisoned")
                .is_empty()
            {
                return;
            }
            notified.await;
        }
    }
}

impl TimeProvider for BlockingRoutingTimeProvider {
    fn current_time(&self, _thread_id: ThreadId) -> TimeFuture<'_> {
        let timestamp = self.current_time.load(Ordering::Relaxed);
        Box::pin(async move {
            Ok(DateTime::<Utc>::from_timestamp(timestamp, 0)
                .expect("test timestamp should be valid"))
        })
    }

    fn sleep(&self, _thread_id: ThreadId, duration: Duration) -> SleepFuture<'_> {
        Box::pin(async move {
            self.sleeps
                .lock()
                .expect("routing time sleep lock should not be poisoned")
                .push(duration);
            self.sleep_started.notify_waiters();
            std::future::pending().await
        })
    }
}

fn routing_models() -> HashMap<String, CustomModelConfig> {
    routing_models_for(vec![
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
    ])
}

fn routing_models_for(
    candidates: Vec<ModelRoutingCandidate>,
) -> HashMap<String, CustomModelConfig> {
    HashMap::from([(
        PROFILE.to_string(),
        CustomModelConfig {
            model: PRIMARY.to_string(),
            routing_profile: Some(ModelRoutingProfile { candidates }),
            model_context_window: None,
            model_auto_compact_token_limit: None,
            trust_candidate_constraints: false,
        },
    )])
}

fn catalog_model(
    slug: &str,
    supported_efforts: &[ReasoningEffort],
    service_tiers: &[&str],
    default_service_tier: Option<&str>,
) -> ModelInfo {
    let mut model = model_info_from_slug(slug);
    model.used_fallback_model_metadata = false;
    model.default_reasoning_level = supported_efforts.first().cloned();
    model.supported_reasoning_levels = supported_efforts
        .iter()
        .cloned()
        .map(|effort| ReasoningEffortPreset {
            description: effort.to_string(),
            effort,
        })
        .collect();
    model.service_tiers = service_tiers
        .iter()
        .map(|tier| ModelServiceTier {
            id: (*tier).to_string(),
            name: (*tier).to_string(),
            description: format!("{tier} test tier"),
        })
        .collect();
    model.default_service_tier = default_service_tier.map(str::to_string);
    model
}

async fn submit_prompt(test: &core_test_support::test_codex::TestCodex) -> Result<()> {
    submit_prompt_text(test, "route this request").await
}

async fn submit_prompt_text(
    test: &core_test_support::test_codex::TestCodex,
    text: &str,
) -> Result<()> {
    test.codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: text.to_string(),
            text_elements: Vec::new(),
        }]))
        .await?;
    Ok(())
}

async fn events_until_complete(test: &core_test_support::test_codex::TestCodex) -> Vec<EventMsg> {
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

fn assert_request_models(mock: &ResponseMock, expected: &[&str]) {
    let actual = mock
        .requests()
        .into_iter()
        .map(|request| {
            request
                .body_json()
                .get("model")
                .and_then(serde_json::Value::as_str)
                .expect("request model")
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

fn assert_no_model_reroute(events: &[EventMsg]) {
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::ModelReroute(_))),
        "model routing profiles must not emit the safety-specific ModelReroute event"
    );
}

fn count_persisted_response_items_containing(rollout: &str, text: &str) -> usize {
    rollout
        .lines()
        .filter_map(|line| serde_json::from_str::<RolloutLine>(line).ok())
        .filter_map(|line| match line.item {
            RolloutItem::ResponseItem(item) => Some(item.item),
            _ => None,
        })
        .filter(|item| {
            serde_json::to_string(item).is_ok_and(|serialized| serialized.contains(text))
        })
        .count()
}

async fn build_routed_test(
    server: &wiremock::MockServer,
) -> Result<core_test_support::test_codex::TestCodex> {
    test_codex()
        .with_config(|config| {
            config.model = Some(PROFILE.to_string());
            config.custom_models = routing_models();
        })
        .build(server)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capacity_falls_through_without_emitting_safety_reroute() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mock = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![json!({
                "type": "response.failed",
                "response": {
                    "id": "response-primary",
                    "status": "failed",
                    "error": {
                        "code": "server_is_overloaded",
                        "message": "temporary diagnostic"
                    }
                }
            })])),
            sse_response(sse_completed("response-fallback")),
        ],
    )
    .await;
    let test = build_routed_test(&server).await?;

    submit_prompt(&test).await?;
    let events = events_until_complete(&test).await;

    assert_request_models(&mock, &[PRIMARY, FALLBACK]);
    assert_no_model_reroute(&events);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_)))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn continuation_failure_reroutes_after_recorded_tool_output() -> Result<()> {
    skip_if_no_network!(Ok(()));

    const CALL_ID: &str = "route-call";
    const TOOL_NAME: &str = "test_sync_tool";

    let server = start_mock_server().await;
    let primary_response = sse(vec![
        ev_response_created("response-primary-tool"),
        json!({
            "type": "response.output_item.done",
            "item": {
                "type": "message",
                "role": "assistant",
                "id": "message-primary-commentary",
                "content": [{"type": "output_text", "text": "checking with a tool"}],
                "phase": "commentary"
            }
        }),
        ev_function_call(CALL_ID, TOOL_NAME, "{}"),
        ev_completed("response-primary-tool"),
    ]);
    let continuation_failure = sse(vec![json!({
        "type": "response.failed",
        "response": {
            "id": "response-primary-continuation",
            "status": "failed",
            "error": {
                "code": "server_is_overloaded",
                "message": "temporary diagnostic"
            }
        }
    })]);
    let fallback_response = sse(vec![
        ev_response_created("response-fallback-final"),
        ev_assistant_message("message-fallback-final", "finished after the tool"),
        ev_completed("response-fallback-final"),
    ]);
    let mock = mount_response_sequence(
        &server,
        vec![
            sse_response(primary_response),
            sse_response(continuation_failure),
            sse_response(fallback_response),
        ],
    )
    .await;
    let mut primary = catalog_model(
        PRIMARY,
        &[ReasoningEffort::Medium],
        &[],
        /*default_service_tier*/ None,
    );
    primary.experimental_supported_tools = vec![TOOL_NAME.to_string()];
    let mut fallback = catalog_model(
        FALLBACK,
        &[ReasoningEffort::Medium],
        &[],
        /*default_service_tier*/ None,
    );
    fallback.experimental_supported_tools = vec![TOOL_NAME.to_string()];
    let test = test_codex()
        .with_config(move |config| {
            config.model = Some(PROFILE.to_string());
            config.custom_models = routing_models();
            config.model_catalog = Some(ModelsResponse {
                models: vec![primary, fallback],
            });
        })
        .build(&server)
        .await?;
    submit_prompt(&test).await?;
    let events = events_until_complete(&test).await;

    assert_request_models(&mock, &[PRIMARY, PRIMARY, FALLBACK]);
    let requests = mock.requests();
    let fallback_request = requests.get(2).expect("fallback request");
    let matching_items = fallback_request
        .input()
        .into_iter()
        .filter(|item| item.get("call_id").and_then(serde_json::Value::as_str) == Some(CALL_ID))
        .collect::<Vec<_>>();
    assert_eq!(
        matching_items
            .iter()
            .filter_map(|item| item.get("type").and_then(serde_json::Value::as_str))
            .collect::<Vec<_>>(),
        vec!["function_call", "function_call_output"]
    );
    assert_eq!(
        fallback_request
            .function_call_output_text(CALL_ID)
            .as_deref(),
        Some("ok")
    );
    assert_no_model_reroute(&events);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, EventMsg::TurnComplete(_)))
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
async fn preferred_candidate_recovers_after_short_cooldown() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mock = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![json!({
                "type": "response.failed",
                "response": {
                    "id": "response-primary-failed",
                    "status": "failed",
                    "error": {
                        "code": "server_is_overloaded",
                        "message": "temporary diagnostic"
                    }
                }
            })])),
            sse_response(sse_completed("response-fallback")),
            sse_response(sse_completed("response-primary-recovered")),
        ],
    )
    .await;
    let time_provider = Arc::new(RoutingTimeProvider::new());
    let test = test_codex()
        .with_config(|config| {
            config.model = Some(PROFILE.to_string());
            config.custom_models = routing_models();
            config.current_time_reminder = Some(CurrentTimeReminderConfig {
                clock_source: CurrentTimeSource::External,
                ..CurrentTimeReminderConfig::default()
            });
        })
        .with_external_time_provider(time_provider.clone())
        .build(&server)
        .await?;

    submit_prompt(&test).await?;
    let first_turn_events = events_until_complete(&test).await;
    assert_no_model_reroute(&first_turn_events);

    time_provider.advance(Duration::from_secs(31));
    submit_prompt_text(&test, "resume after routing cooldown").await?;
    let second_turn_events = events_until_complete(&test).await;

    assert_request_models(&mock, &[PRIMARY, FALLBACK, PRIMARY]);
    assert_eq!(
        mock.requests()[2]
            .body_json()
            .to_string()
            .matches("resume after routing cooldown")
            .count(),
        1
    );
    assert_no_model_reroute(&second_turn_events);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_candidates_cooling_waits_before_the_next_user_turn_requests_one() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let unavailable = |response_id: &str| {
        sse_response(sse(vec![json!({
            "type": "response.failed",
            "response": {
                "id": response_id,
                "status": "failed",
                "error": {
                    "code": "server_is_overloaded",
                    "message": "temporary diagnostic"
                }
            }
        })]))
    };
    let mock = mount_response_sequence(
        &server,
        vec![
            unavailable("response-primary-failed"),
            unavailable("response-fallback-failed"),
            sse_response(sse_completed("response-primary-recovered")),
        ],
    )
    .await;
    let time_provider = Arc::new(RoutingTimeProvider::new());
    let test = test_codex()
        .with_config(|config| {
            config.model = Some(PROFILE.to_string());
            config.custom_models = routing_models();
            config.current_time_reminder = Some(CurrentTimeReminderConfig {
                clock_source: CurrentTimeSource::External,
                ..CurrentTimeReminderConfig::default()
            });
        })
        .with_external_time_provider(time_provider.clone())
        .build(&server)
        .await?;

    submit_prompt(&test).await?;
    let first_turn_events = events_until_complete(&test).await;
    assert!(
        first_turn_events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_)))
    );

    submit_prompt_text(&test, "resume after cooling candidates").await?;
    let second_turn_events = events_until_complete(&test).await;

    assert_request_models(&mock, &[PRIMARY, FALLBACK, PRIMARY]);
    assert_eq!(
        mock.requests()[2]
            .body_json()
            .to_string()
            .matches("resume after cooling candidates")
            .count(),
        1
    );
    assert_eq!(time_provider.sleeps(), vec![Duration::from_secs(30)]);
    assert_no_model_reroute(&second_turn_events);
    assert!(
        !second_turn_events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_)))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cooling_wait_is_interruptible_before_any_provider_request() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let unavailable = |response_id: &str| {
        sse_response(sse(vec![json!({
            "type": "response.failed",
            "response": {
                "id": response_id,
                "status": "failed",
                "error": {
                    "code": "server_is_overloaded",
                    "message": "temporary diagnostic"
                }
            }
        })]))
    };
    let mock = mount_response_sequence(
        &server,
        vec![
            unavailable("response-primary-failed"),
            unavailable("response-fallback-failed"),
        ],
    )
    .await;
    let time_provider = Arc::new(BlockingRoutingTimeProvider::new());
    let test = test_codex()
        .with_config(|config| {
            config.model = Some(PROFILE.to_string());
            config.custom_models = routing_models();
            config.current_time_reminder = Some(CurrentTimeReminderConfig {
                clock_source: CurrentTimeSource::External,
                ..CurrentTimeReminderConfig::default()
            });
        })
        .with_external_time_provider(time_provider.clone())
        .build(&server)
        .await?;

    submit_prompt(&test).await?;
    events_until_complete(&test).await;
    assert_request_models(&mock, &[PRIMARY, FALLBACK]);

    submit_prompt_text(&test, "wait for routing cooldown").await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnStarted(_))
    })
    .await;
    time_provider.wait_until_sleeping().await;
    assert_request_models(&mock, &[PRIMARY, FALLBACK]);
    let user_message = wait_for_event_match(test.codex.as_ref(), |event| {
        let EventMsg::ItemCompleted(event) = event else {
            return None;
        };
        let TurnItem::UserMessage(item) = &event.item else {
            return None;
        };
        Some(item.clone())
    })
    .await;
    assert_eq!(
        user_message.content,
        vec![UserInput::Text {
            text: "wait for routing cooldown".to_string(),
            text_elements: Vec::new(),
        }]
    );
    let rollout_path = test.codex.rollout_path().expect("rollout path");
    let rollout_while_waiting = tokio::fs::read_to_string(&rollout_path).await?;
    assert_eq!(
        count_persisted_response_items_containing(
            &rollout_while_waiting,
            "wait for routing cooldown"
        ),
        1
    );

    test.codex.submit(Op::Interrupt).await?;
    loop {
        match wait_for_event(&test.codex, |_| true).await {
            EventMsg::TurnAborted(_) => break,
            EventMsg::TurnComplete(event) => {
                panic!("cooldown wait completed instead of aborting: {event:?}")
            }
            _ => {}
        }
    }
    assert_request_models(&mock, &[PRIMARY, FALLBACK]);
    let live_history = test
        .codex
        .model_history_snapshot()
        .await
        .expect("aborted idle thread should expose complete model history");
    assert_eq!(
        live_history
            .iter()
            .filter(|item| {
                serde_json::to_string(&item.item)
                    .is_ok_and(|serialized| serialized.contains("wait for routing cooldown"))
            })
            .count(),
        1
    );
    let rollout_after_abort = tokio::fs::read_to_string(rollout_path).await?;
    assert_eq!(
        count_persisted_response_items_containing(
            &rollout_after_abort,
            "wait for routing cooldown"
        ),
        1
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn structured_usage_limit_falls_through() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mock = mount_response_sequence(
        &server,
        vec![
            ResponseTemplate::new(429).set_body_json(json!({
                "error": {
                    "type": "usage_limit_reached",
                    "plan_type": "pro",
                    "resets_at": 4_102_444_800_i64
                }
            })),
            sse_response(sse_completed("response-fallback")),
        ],
    )
    .await;
    let test = build_routed_test(&server).await?;

    submit_prompt(&test).await?;
    let events = events_until_complete(&test).await;

    assert_request_models(&mock, &[PRIMARY, FALLBACK]);
    assert_no_model_reroute(&events);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_)))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn structured_model_unavailable_falls_through() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mock = mount_response_sequence(
        &server,
        vec![
            ResponseTemplate::new(400).set_body_json(json!({
                "error": {
                    "code": "model_not_supported",
                    "message": "unrelated diagnostic"
                }
            })),
            sse_response(sse_completed("response-fallback")),
        ],
    )
    .await;
    let test = build_routed_test(&server).await?;

    submit_prompt(&test).await?;
    let events = events_until_complete(&test).await;

    assert_request_models(&mock, &[PRIMARY, FALLBACK]);
    assert_no_model_reroute(&events);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_)))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unstructured_invalid_request_does_not_fall_through() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mock = mount_response_sequence(
        &server,
        vec![ResponseTemplate::new(400).set_body_json(json!({
            "error": {
                "code": "invalid_request",
                "message": "generic request rejected"
            }
        }))],
    )
    .await;
    let test = build_routed_test(&server).await?;

    submit_prompt(&test).await?;
    let events = events_until_complete(&test).await;

    assert_request_models(&mock, &[PRIMARY]);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::ModelReroute(_)))
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_)))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_visible_output_is_a_reroute_checkpoint() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let response_failed = json!({
        "type": "response.failed",
        "response": {
            "id": "response-primary",
            "status": "failed",
            "error": {
                "code": "model_not_supported",
                "message": "failed after output"
            }
        }
    });
    let mock = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("response-primary"),
                ev_assistant_message("message-primary", "visible output"),
                response_failed,
            ])),
            sse_response(sse_completed("response-fallback")),
        ],
    )
    .await;
    let test = build_routed_test(&server).await?;

    submit_prompt(&test).await?;
    let events = events_until_complete(&test).await;

    assert_request_models(&mock, &[PRIMARY, FALLBACK]);
    assert_no_model_reroute(&events);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_)))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_candidate_retry_drops_unfinished_reasoning_before_reroute() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let retryable_failure = json!({
        "type": "response.failed",
        "response": {
            "id": "response-primary-retryable",
            "status": "failed",
            "error": {
                "code": "internal_error",
                "message": "retry this request configuration"
            }
        }
    });
    let overload_failure = json!({
        "type": "response.failed",
        "response": {
            "id": "response-primary-overloaded",
            "status": "failed",
            "error": {
                "code": "server_is_overloaded",
                "message": "temporary diagnostic"
            }
        }
    });
    let mock = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("response-primary-retryable"),
                ev_reasoning_item_added("reasoning-primary", &[]),
                ev_reasoning_summary_text_delta("unfinished reasoning"),
                retryable_failure,
            ])),
            sse_response(sse(vec![
                ev_reasoning_item_added("reasoning-primary-overloaded", &[]),
                ev_reasoning_summary_text_delta("another unfinished reasoning item"),
                overload_failure,
            ])),
            sse_response(sse_completed("response-fallback")),
        ],
    )
    .await;
    let test = build_routed_test(&server).await?;

    submit_prompt(&test).await?;
    let events = events_until_complete(&test).await;

    assert_request_models(&mock, &[PRIMARY, PRIMARY, FALLBACK]);
    let requests = mock.requests();
    let fallback_request = requests.last().expect("fallback request");
    assert!(
        fallback_request
            .input()
            .iter()
            .all(|item| item["type"] != "reasoning"),
        "fallback request must not contain unfinished provider reasoning"
    );
    assert_eq!(
        fallback_request
            .body_json()
            .to_string()
            .matches("<interrupted_response>")
            .count(),
        1,
        "the turn should contain one factual interruption record"
    );
    assert_no_model_reroute(&events);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_)))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn smaller_fallback_compacts_before_its_first_sampling_request() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let overload_failure = json!({
        "type": "response.failed",
        "response": {
            "id": "response-primary-overloaded",
            "status": "failed",
            "error": {
                "code": "server_is_overloaded",
                "message": "temporary diagnostic"
            }
        }
    });
    let mock = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_assistant_message("message-primary-history", "history before reroute"),
                ev_completed_with_tokens("response-primary-history", /*total_tokens*/ 20_000),
            ])),
            sse_response(sse(vec![overload_failure])),
            sse_response(sse(vec![
                json!({
                    "type": "response.output_item.done",
                    "item": {
                        "type": "compaction",
                        "encrypted_content": "test-routed-compaction-summary"
                    }
                }),
                ev_completed_with_tokens("response-fallback-compact", /*total_tokens*/ 10),
            ])),
            sse_response(sse_completed("response-fallback-final")),
        ],
    )
    .await;
    let mut primary = catalog_model(
        PRIMARY,
        &[ReasoningEffort::Medium],
        &[],
        /*default_service_tier*/ None,
    );
    primary.context_window = Some(100_000);
    primary.auto_compact_token_limit = Some(90_000);
    primary.comp_hash = None;
    let mut fallback = catalog_model(
        FALLBACK,
        &[ReasoningEffort::Medium],
        &[],
        /*default_service_tier*/ None,
    );
    fallback.context_window = Some(10_000);
    fallback.auto_compact_token_limit = Some(9_000);
    fallback.comp_hash = None;
    let test = test_codex()
        .with_config(move |config| {
            config.model = Some(PROFILE.to_string());
            config.custom_models = routing_models();
            config.model_catalog = Some(ModelsResponse {
                models: vec![primary, fallback],
            });
        })
        .build(&server)
        .await?;
    let fallback_info = test
        .thread_manager
        .get_models_manager()
        .get_model_info(FALLBACK, &test.config.to_models_manager_config())
        .await;
    assert_eq!(test.config.model_context_window, None);
    assert_eq!(test.config.model_auto_compact_token_limit, None);
    assert_eq!(fallback_info.context_window, Some(10_000));
    assert_eq!(fallback_info.auto_compact_token_limit, Some(9_000));

    submit_prompt(&test).await?;
    let first_turn_events = events_until_complete(&test).await;
    let first_turn_token_info = first_turn_events
        .iter()
        .find_map(|event| match event {
            EventMsg::TokenCount(event) => event.info.as_ref(),
            _ => None,
        })
        .expect("first turn token info");
    assert_eq!(first_turn_token_info.last_token_usage.total_tokens, 20_000);
    assert_eq!(first_turn_token_info.model_context_window, Some(95_000));
    let cached_token_info = test
        .codex
        .token_usage_info()
        .await
        .expect("cached token info");
    assert_eq!(cached_token_info.last_token_usage.total_tokens, 20_000);
    submit_prompt(&test).await?;
    let events = events_until_complete(&test).await;

    assert!(events.iter().any(|event| matches!(
        event,
        EventMsg::ItemCompleted(event)
            if matches!(event.item, TurnItem::ContextCompaction(_))
    )));
    assert_request_models(&mock, &[PRIMARY, PRIMARY, FALLBACK, FALLBACK]);
    let requests = mock.requests();
    assert_eq!(requests[2].inputs_of_type("compaction_trigger").len(), 1);
    assert!(requests[3].inputs_of_type("compaction_trigger").is_empty());
    assert_eq!(requests[3].inputs_of_type("compaction").len(), 1);
    assert_no_model_reroute(&events);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_)))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_compaction_failure_falls_through_without_terminating_the_turn() -> Result<()> {
    skip_if_no_network!(Ok(()));

    const REJECTED_COMPACTION_OUTPUT: &str = "rejected candidate compaction output";

    let server = start_mock_server().await;
    let overload_failure = || {
        json!({
            "type": "response.failed",
            "response": {
                "id": "response-overloaded",
                "status": "failed",
                "error": {
                    "code": "server_is_overloaded",
                    "message": "temporary diagnostic"
                }
            }
        })
    };
    let mock = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_assistant_message("message-primary-history", "history before reroute"),
                ev_completed_with_tokens("response-primary-history", /*total_tokens*/ 20_000),
            ])),
            sse_response(sse(vec![overload_failure()])),
            sse_response(sse(vec![
                ev_assistant_message("message-rejected-compaction", REJECTED_COMPACTION_OUTPUT),
                overload_failure(),
            ])),
            sse_response(sse_completed("response-second-fallback-final")),
        ],
    )
    .await;
    let mut primary = catalog_model(
        PRIMARY,
        &[ReasoningEffort::Medium],
        &[],
        /*default_service_tier*/ None,
    );
    primary.context_window = Some(100_000);
    primary.auto_compact_token_limit = Some(90_000);
    let mut fallback = catalog_model(
        FALLBACK,
        &[ReasoningEffort::Medium],
        &[],
        /*default_service_tier*/ None,
    );
    fallback.context_window = Some(10_000);
    fallback.auto_compact_token_limit = Some(9_000);
    let mut second_fallback = catalog_model(
        SECOND_FALLBACK,
        &[ReasoningEffort::Medium],
        &[],
        /*default_service_tier*/ None,
    );
    second_fallback.context_window = Some(100_000);
    second_fallback.auto_compact_token_limit = Some(90_000);
    let test = test_codex()
        .with_config(move |config| {
            config.model = Some(PROFILE.to_string());
            config.custom_models = routing_models_for(vec![
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
                ModelRoutingCandidate {
                    model: SECOND_FALLBACK.to_string(),
                    reasoning_effort: None,
                    service_tier: None,
                },
            ]);
            config.model_catalog = Some(ModelsResponse {
                models: vec![primary, fallback, second_fallback],
            });
        })
        .build(&server)
        .await?;

    submit_prompt(&test).await?;
    events_until_complete(&test).await;
    submit_prompt(&test).await?;
    let events = events_until_complete(&test).await;

    assert_request_models(&mock, &[PRIMARY, PRIMARY, FALLBACK, SECOND_FALLBACK]);
    let requests = mock.requests();
    let live_history = test
        .codex
        .model_history_snapshot()
        .await
        .expect("idle thread should expose complete model history");
    let live_history = serde_json::to_string(
        &live_history
            .iter()
            .map(|item| &item.item)
            .collect::<Vec<_>>(),
    )?;
    let rollout_path = test.codex.rollout_path().expect("rollout path");
    let rollout = tokio::fs::read_to_string(rollout_path).await?;
    assert!(!requests[3].body_contains_text(REJECTED_COMPACTION_OUTPUT));
    assert!(!live_history.contains(REJECTED_COMPACTION_OUTPUT));
    assert!(!rollout.contains(REJECTED_COMPACTION_OUTPUT));
    assert_no_model_reroute(&events);
    assert!(!events.iter().any(|event| matches!(
        event,
        EventMsg::ItemStarted(event)
            if matches!(event.item, TurnItem::ContextCompaction(_))
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        EventMsg::ItemCompleted(event)
            if matches!(event.item, TurnItem::ContextCompaction(_))
    )));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_)))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn token_budget_reroute_runs_post_compact_hook_once_after_route_commit() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let overload_failure = json!({
        "type": "response.failed",
        "response": {
            "id": "response-overloaded",
            "status": "failed",
            "error": {
                "code": "server_is_overloaded",
                "message": "temporary diagnostic"
            }
        }
    });
    let mock = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_assistant_message("message-primary-history", "history before reroute"),
                ev_completed_with_tokens("response-primary-history", /*total_tokens*/ 20_000),
            ])),
            sse_response(sse(vec![overload_failure])),
            sse_response(sse_completed("response-fallback-final")),
        ],
    )
    .await;
    let mut primary = catalog_model(
        PRIMARY,
        &[ReasoningEffort::Medium],
        &[],
        /*default_service_tier*/ None,
    );
    primary.context_window = Some(100_000);
    primary.auto_compact_token_limit = Some(90_000);
    let mut fallback = catalog_model(
        FALLBACK,
        &[ReasoningEffort::Medium],
        &[],
        /*default_service_tier*/ None,
    );
    fallback.context_window = Some(10_000);
    fallback.auto_compact_token_limit = Some(9_000);
    let test = test_codex()
        .with_pre_build_hook(|home| {
            let script_path = home.join("post_compact.py");
            std::fs::write(
                &script_path,
                "import json\nimport sys\njson.load(sys.stdin)\n",
            )
            .expect("write post-compact hook");
            let hooks = json!({
                "hooks": {
                    "PostCompact": [{
                        "matcher": "auto",
                        "hooks": [{
                            "type": "command",
                            "command": format!("python3 \"{}\"", script_path.display()),
                        }]
                    }]
                }
            });
            std::fs::write(home.join("hooks.json"), hooks.to_string()).expect("write hooks config");
        })
        .with_config(move |config| {
            config.model = Some(PROFILE.to_string());
            config.custom_models = routing_models();
            config.model_catalog = Some(ModelsResponse {
                models: vec![primary, fallback],
            });
            config
                .features
                .enable(Feature::TokenBudget)
                .expect("test config should allow token budget");
            trust_discovered_hooks(config);
            let user_config = config
                .config_layer_stack
                .get_active_user_layer()
                .expect("trusted hook user layer")
                .config
                .clone();
            std::fs::write(
                config.codex_home.join("config.toml"),
                toml::to_string(&user_config).expect("serialize trusted hook config"),
            )
            .expect("persist trusted hook config");
        })
        .build(&server)
        .await?;

    submit_prompt(&test).await?;
    events_until_complete(&test).await;
    submit_prompt(&test).await?;
    let events = events_until_complete(&test).await;

    assert_request_models(&mock, &[PRIMARY, PRIMARY, FALLBACK]);
    let routing_warning_index = events
        .iter()
        .position(|event| {
            matches!(
                event,
                EventMsg::Warning(event) if event.message.contains("Model profile")
            )
        })
        .expect("model routing warning");
    let post_compact_indices = events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match event {
            EventMsg::HookCompleted(event)
                if event.run.event_name == HookEventName::PostCompact =>
            {
                Some(index)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(post_compact_indices.len(), 1);
    assert!(routing_warning_index < post_compact_indices[0]);
    assert_no_model_reroute(&events);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, EventMsg::Error(_)))
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_rejects_primary_tuple_and_selects_supported_fallback() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mock = mount_response_sequence(
        &server,
        vec![sse_response(sse_completed("response-fallback"))],
    )
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model = Some(PROFILE.to_string());
            config.custom_models = routing_models_for(vec![
                ModelRoutingCandidate {
                    model: PRIMARY.to_string(),
                    reasoning_effort: Some(ReasoningEffort::High),
                    service_tier: None,
                },
                ModelRoutingCandidate {
                    model: FALLBACK.to_string(),
                    reasoning_effort: Some(ReasoningEffort::High),
                    service_tier: None,
                },
            ]);
            config.model_catalog = Some(ModelsResponse {
                models: vec![
                    catalog_model(
                        PRIMARY,
                        &[ReasoningEffort::Medium],
                        &[],
                        /*default_service_tier*/ None,
                    ),
                    catalog_model(
                        FALLBACK,
                        &[ReasoningEffort::High],
                        &[],
                        /*default_service_tier*/ None,
                    ),
                ],
            });
        })
        .build(&server)
        .await?;

    submit_prompt(&test).await?;
    events_until_complete(&test).await;

    assert_request_models(&mock, &[FALLBACK]);
    assert_eq!(
        mock.single_request().body_json()["reasoning"]["effort"],
        json!("high")
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_locally_rejected_candidates_send_lowest_exact_tuple() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mock = mount_response_sequence(
        &server,
        vec![sse_response(sse_completed("response-fail-open"))],
    )
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model = Some(PROFILE.to_string());
            config.custom_models = routing_models_for(vec![
                ModelRoutingCandidate {
                    model: PRIMARY.to_string(),
                    reasoning_effort: Some(ReasoningEffort::High),
                    service_tier: None,
                },
                ModelRoutingCandidate {
                    model: FALLBACK.to_string(),
                    reasoning_effort: Some(ReasoningEffort::High),
                    service_tier: None,
                },
            ]);
            config.model_catalog = Some(ModelsResponse {
                models: vec![
                    catalog_model(
                        PRIMARY,
                        &[ReasoningEffort::Medium],
                        &[],
                        /*default_service_tier*/ None,
                    ),
                    catalog_model(
                        FALLBACK,
                        &[ReasoningEffort::Medium],
                        &[],
                        /*default_service_tier*/ None,
                    ),
                ],
            });
        })
        .build(&server)
        .await?;

    submit_prompt(&test).await?;
    events_until_complete(&test).await;

    assert_request_models(&mock, &[FALLBACK]);
    let body = mock.single_request().body_json();
    assert_eq!(body["model"], json!(FALLBACK));
    assert_eq!(body["reasoning"]["effort"], json!("high"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn omitted_service_tier_uses_catalog_default_when_fast_mode_is_enabled() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mock = mount_response_sequence(
        &server,
        vec![sse_response(sse_completed("response-default-tier"))],
    )
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model = Some(PROFILE.to_string());
            config.custom_models = routing_models_for(vec![ModelRoutingCandidate {
                model: PRIMARY.to_string(),
                reasoning_effort: None,
                service_tier: None,
            }]);
            config.model_catalog = Some(ModelsResponse {
                models: vec![catalog_model(
                    PRIMARY,
                    &[ReasoningEffort::Medium],
                    &["priority"],
                    Some("priority"),
                )],
            });
            config
                .features
                .set_enabled(Feature::FastMode, /*enabled*/ true)
                .expect("enable fast mode");
        })
        .build(&server)
        .await?;

    submit_prompt(&test).await?;
    events_until_complete(&test).await;

    assert_request_models(&mock, &[PRIMARY]);
    assert_eq!(
        mock.single_request().body_json()["service_tier"],
        json!("priority")
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn omitted_service_tier_stays_omitted_when_fast_mode_is_disabled() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mock = mount_response_sequence(
        &server,
        vec![sse_response(sse_completed("response-no-tier"))],
    )
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model = Some(PROFILE.to_string());
            config.custom_models = routing_models_for(vec![ModelRoutingCandidate {
                model: PRIMARY.to_string(),
                reasoning_effort: None,
                service_tier: None,
            }]);
            config.model_catalog = Some(ModelsResponse {
                models: vec![catalog_model(
                    PRIMARY,
                    &[ReasoningEffort::Medium],
                    &["priority"],
                    Some("priority"),
                )],
            });
            config
                .features
                .set_enabled(Feature::FastMode, /*enabled*/ false)
                .expect("disable fast mode");
        })
        .build(&server)
        .await?;

    submit_prompt(&test).await?;
    events_until_complete(&test).await;

    assert_request_models(&mock, &[PRIMARY]);
    assert_eq!(
        mock.single_request().body_json()["service_tier"],
        json!(null)
    );

    Ok(())
}
