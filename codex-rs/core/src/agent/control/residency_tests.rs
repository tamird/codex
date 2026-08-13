use super::is_resident_session_source;
use crate::StartThreadOptions;
use crate::ThreadManager;
use crate::agent::AgentControl;
use crate::agent::registry::AgentMetadata;
use crate::codex_thread::CodexThread;
use crate::config::Config;
use crate::config::test_config;
use crate::context::ContextualUserFragment;
use crate::context::SubagentNotification;
use crate::thread_manager::ThreadManagerState;
use codex_features::Feature;
use codex_login::CodexAuth;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadSource;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::protocol::TurnAbortedEvent;
use codex_protocol::protocol::TurnCompleteEvent;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio::time::timeout;

#[test]
fn goal_supervisor_helper_is_not_an_agent_resident() {
    let parent_thread_id = ThreadId::new();
    let worker_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: None,
        agent_nickname: None,
        agent_role: None,
    });
    let supervisor_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: None,
        agent_nickname: None,
        agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
    });

    assert!(is_resident_session_source(&worker_source));
    assert!(!is_resident_session_source(&supervisor_source));
}

#[tokio::test]
async fn residency_slot_reservation_unloads_oldest_idle_v2_agent() {
    assert_residency_slot_unloads_oldest_idle_agent(MultiAgentVersion::V2).await;
}

#[tokio::test]
async fn residency_slot_reservation_unloads_oldest_idle_v1_agent() {
    assert_residency_slot_unloads_oldest_idle_agent(MultiAgentVersion::V1).await;
}

async fn assert_residency_slot_unloads_oldest_idle_agent(multi_agent_version: MultiAgentVersion) {
    let mut config = test_config().await;
    match multi_agent_version {
        MultiAgentVersion::V1 => {
            let _ = config.features.disable(Feature::MultiAgentV2);
            let _ = config.features.enable(Feature::Collab);
            config.agent_max_threads = Some(1);
        }
        MultiAgentVersion::V2 => {
            let _ = config.features.enable(Feature::MultiAgentV2);
            config.multi_agent_v2.max_concurrent_threads_per_session = 2;
        }
        MultiAgentVersion::Disabled => panic!("residency requires multi-agent support"),
    }
    let temp_home = tempfile::tempdir().expect("create temp home");
    config.codex_home = temp_home.path().to_path_buf().try_into().unwrap();
    config.cwd = temp_home.path().to_path_buf().try_into().unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let root = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("start root thread");
    let control = manager.agent_control();
    let state = control.upgrade().expect("thread manager should be live");

    let first_slot = control
        .reserve_agent_residency_slot(
            &state,
            &config,
            multi_agent_version,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("first resident slot");
    let first = spawn_subagent(&control, &state, config.clone(), root.thread_id, "worker-1").await;
    first_slot.commit(first.thread_id);
    mark_thread_completed(first.thread.as_ref()).await;

    let second_slot = control
        .reserve_agent_residency_slot(
            &state,
            &config,
            multi_agent_version,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("second resident slot should evict the first idle agent");
    match manager.get_thread(first.thread_id).await {
        Err(err) => match err.details() {
            CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, first.thread_id),
            _ => panic!("expected evicted thread to be missing, got {err:?}"),
        },
        Ok(_) => panic!("expected evicted thread to be missing"),
    }
    let second = spawn_subagent(&control, &state, config, root.thread_id, "worker-2").await;
    second_slot.commit(second.thread_id);

    assert!(manager.get_thread(root.thread_id).await.is_ok());
    assert!(manager.get_thread(second.thread_id).await.is_ok());
}

#[tokio::test]
async fn interrupted_v2_agent_is_lost_after_residency_eviction() {
    let mut config = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.multi_agent_v2.max_concurrent_threads_per_session = 2;
    let temp_home = tempfile::tempdir().expect("create temp home");
    config.codex_home = temp_home.path().to_path_buf().try_into().unwrap();
    config.cwd = temp_home.path().to_path_buf().try_into().unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let root = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("start root thread");
    let control = manager.agent_control();
    let state = control.upgrade().expect("thread manager should be live");

    let first_slot = control
        .reserve_agent_residency_slot(
            &state,
            &config,
            MultiAgentVersion::V2,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("first resident slot");
    let first = spawn_subagent(&control, &state, config.clone(), root.thread_id, "worker-1").await;
    first_slot.commit(first.thread_id);
    mark_thread_interrupted(first.thread.as_ref()).await;

    let second_slot = control
        .reserve_agent_residency_slot(
            &state,
            &config,
            MultiAgentVersion::V2,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("second resident slot should evict the first interrupted idle agent");
    match manager.get_thread(first.thread_id).await {
        Err(err) => match err.details() {
            CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, first.thread_id),
            _ => panic!("expected evicted thread to be missing, got {err:?}"),
        },
        Ok(_) => panic!("expected evicted thread to be missing"),
    }
    let second = spawn_subagent(&control, &state, config.clone(), root.thread_id, "worker-2").await;
    second_slot.commit(second.thread_id);
    mark_thread_completed(second.thread.as_ref()).await;

    let err = control
        .ensure_v2_agent_loaded(config, first.thread_id, /*parent*/ None)
        .await
        .expect_err("evicted interrupted agent should stay lost");
    match err.details() {
        CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, first.thread_id),
        _ => panic!("expected ThreadNotFound, got {err:?}"),
    }

    assert!(manager.get_thread(root.thread_id).await.is_ok());
    assert!(manager.get_thread(second.thread_id).await.is_ok());
    match manager.get_thread(first.thread_id).await {
        Err(err) => match err.details() {
            CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, first.thread_id),
            _ => panic!("expected evicted thread to be missing, got {err:?}"),
        },
        Ok(_) => panic!("expected evicted thread to be missing"),
    }
}

#[tokio::test]
async fn pathless_v2_interrupted_watcher_does_not_block_residency_eviction() {
    let mut config = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.multi_agent_v2.max_concurrent_threads_per_session = 2;
    let temp_home = tempfile::tempdir().expect("create temp home");
    config.codex_home = temp_home.path().to_path_buf().try_into().unwrap();
    config.cwd = temp_home.path().to_path_buf().try_into().unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let root = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("start root thread");
    let control = manager.agent_control();
    let state = control.upgrade().expect("thread manager should be live");
    let source = pathless_thread_spawn_source(root.thread_id);
    let first_slot = control
        .reserve_agent_residency_slot(
            &state,
            &config,
            MultiAgentVersion::V2,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("first resident slot");
    let first = state
        .spawn_new_thread_with_source(
            config.clone(),
            control.clone(),
            source.clone(),
            /*history_mode*/ None,
            Some(root.thread_id),
            /*forked_from_thread_id*/ None,
            Some(ThreadSource::Subagent),
            /*metrics_service_name*/ None,
            /*inherited_environments*/ None,
            /*inherited_exec_policy*/ None,
            Default::default(),
            /*environments*/ None,
        )
        .await
        .expect("spawn first pathless v2 agent");
    let registry_slot = control
        .state
        .reserve_spawn_slot(/*max_threads*/ None)
        .expect("reserve first registry slot");
    registry_slot.commit(AgentMetadata {
        agent_id: Some(first.thread_id),
        ..Default::default()
    });
    first_slot.commit(first.thread_id);
    state.notify_thread_created(first.thread_id);
    assert!(control.maybe_start_completion_watcher(
        first.thread_id,
        Some(source),
        first.thread_id.to_string(),
        /*child_agent_path*/ None,
    ));
    mark_thread_interrupted(first.thread.as_ref()).await;
    timeout(Duration::from_secs(5), async {
        while control.get_status(first.thread_id).await != crate::agent::AgentStatus::Interrupted {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first pathless v2 agent should become interrupted");

    let second_slot = timeout(
        Duration::from_secs(5),
        control.reserve_agent_residency_slot(
            &state,
            &config,
            MultiAgentVersion::V2,
            /*protected_thread_id*/ None,
        ),
    )
    .await
    .expect("interrupted completion watcher should release residency eviction")
    .expect("second resident slot should evict the interrupted agent");
    drop(second_slot);

    match manager.get_thread(first.thread_id).await {
        Err(err) => match err.details() {
            CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, first.thread_id),
            _ => panic!("expected evicted thread to be missing, got {err:?}"),
        },
        Ok(_) => panic!("expected evicted thread to be missing"),
    }
    let history = root.thread.session.clone_history().await;
    assert_eq!(subagent_notification_count(history.raw_items()), 1);
}

fn pathless_thread_spawn_source(parent_thread_id: ThreadId) -> SessionSource {
    SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: None,
        agent_nickname: None,
        agent_role: Some("explorer".to_string()),
    })
}

fn subagent_notification_count<'a>(
    history_items: impl IntoIterator<Item = &'a ResponseItem>,
) -> usize {
    history_items
        .into_iter()
        .filter(|item| {
            let ResponseItem::Message { role, content, .. } = item else {
                return false;
            };
            role == "user"
                && content.iter().any(|content_item| match content_item {
                    ContentItem::InputText { text } | ContentItem::OutputText { text } => {
                        SubagentNotification::matches_text(text)
                    }
                    ContentItem::InputImage { .. } | ContentItem::InputAudio { .. } => false,
                })
        })
        .count()
}

async fn spawn_subagent(
    control: &AgentControl,
    state: &Arc<ThreadManagerState>,
    config: Config,
    parent_thread_id: ThreadId,
    label: &str,
) -> crate::thread_manager::NewThread {
    state
        .spawn_new_thread_with_source(
            config,
            control.clone(),
            SessionSource::SubAgent(SubAgentSource::Other(label.to_string())),
            /*history_mode*/ None,
            Some(parent_thread_id),
            /*forked_from_thread_id*/ None,
            Some(ThreadSource::Subagent),
            /*metrics_service_name*/ None,
            /*inherited_environments*/ None,
            /*inherited_exec_policy*/ None,
            Default::default(),
            /*environments*/ None,
        )
        .await
        .expect("spawn subagent")
}

async fn mark_thread_completed(thread: &CodexThread) {
    let turn = thread.session.new_default_turn().await;
    thread
        .session
        .send_event(
            turn.as_ref(),
            EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: turn.sub_id.clone(),
                started_at: None,
                last_agent_message: Some("done".to_string()),
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        )
        .await;
    clear_active_turn(thread).await;
}

async fn mark_thread_interrupted(thread: &CodexThread) {
    let turn = thread.session.new_default_turn().await;
    thread
        .session
        .send_event(
            turn.as_ref(),
            EventMsg::TurnAborted(TurnAbortedEvent {
                turn_id: Some(turn.sub_id.clone()),
                started_at: None,
                reason: TurnAbortReason::Interrupted,
                completed_at: None,
                duration_ms: None,
            }),
        )
        .await;
    clear_active_turn(thread).await;
}

async fn clear_active_turn(thread: &CodexThread) {
    // The fixture has no task runner to clear the turn after the terminal event.
    *thread.session.active_turn.lock().await = None;
}
