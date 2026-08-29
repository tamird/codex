use super::EvictionResult;
use super::EvictionScan;
use super::ResidencyBlocker;
use super::is_resident_session_source;
use crate::StartThreadOptions;
use crate::ThreadManager;
use crate::agent::AgentControl;
use crate::agent::registry::AgentLifecycle;
use crate::agent::registry::AgentMetadata;
use crate::codex_thread::CodexThread;
use crate::config::Config;
use crate::config::test_config;
use crate::context::ContextualUserFragment;
use crate::context::SubagentNotification;
use crate::thread_manager::NewThread;
use crate::thread_manager::ThreadManagerState;
use assert_matches::assert_matches;
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
use std::future::Future;
use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Poll;
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

struct TrimFixture {
    _home: tempfile::TempDir,
    config: Config,
    manager: ThreadManager,
    control: AgentControl,
    first: NewThread,
    first_lifecycle: Arc<AgentLifecycle>,
    second: NewThread,
    second_lifecycle: Arc<AgentLifecycle>,
}

async fn trim_fixture() -> TrimFixture {
    let mut config = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.multi_agent_v2.max_concurrent_threads_per_session = 3;
    let home = tempfile::tempdir().expect("create temp home");
    config.codex_home = home.path().to_path_buf().try_into().unwrap();
    config.cwd = home.path().to_path_buf().try_into().unwrap();
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
    let mut residents = Vec::new();
    for name in ["first", "second"] {
        let slot = control
            .reserve_agent_residency_slot(
                &state,
                &config,
                MultiAgentVersion::V2,
                /*protected_thread_id*/ None,
            )
            .await
            .expect("reserve resident slot");
        let thread = spawn_subagent(&control, &state, config.clone(), root.thread_id, name).await;
        let metadata = AgentMetadata {
            agent_id: Some(thread.thread_id),
            ..Default::default()
        };
        let lifecycle = Arc::clone(&metadata.lifecycle);
        control
            .state
            .reserve_spawn_slot(/*max_threads*/ None)
            .expect("reserve registry slot")
            .commit(metadata);
        slot.commit(thread.thread_id);
        residents.push((thread, lifecycle));
    }
    let [(first, first_lifecycle), (second, second_lifecycle)]: [(NewThread, Arc<AgentLifecycle>);
        2] = residents
        .try_into()
        .unwrap_or_else(|_| panic!("expected two residents"));
    // Reducing the execution limit makes the two existing residents exceed the warm target.
    config.multi_agent_v2.max_concurrent_threads_per_session = 2;
    TrimFixture {
        _home: home,
        config,
        manager,
        control,
        first,
        first_lifecycle,
        second,
        second_lifecycle,
    }
}

enum ReleasedBlocker {
    Transition,
    CompletionWatcher,
}

#[tokio::test]
async fn deferred_trim_resumes_after_completion_watcher_finishes() {
    assert_deferred_trim_resumes(ReleasedBlocker::CompletionWatcher).await;
}

#[tokio::test]
async fn deferred_trim_resumes_after_lifecycle_transition_finishes() {
    assert_deferred_trim_resumes(ReleasedBlocker::Transition).await;
}

async fn assert_deferred_trim_resumes(released: ReleasedBlocker) {
    let TrimFixture {
        _home,
        config: _,
        manager,
        control,
        first,
        first_lifecycle,
        second,
        second_lifecycle,
    } = trim_fixture().await;
    mark_thread_completed(first.thread.as_ref()).await;
    mark_thread_completed(second.thread.as_ref()).await;
    let transition = first_lifecycle.lock_transition().await;
    let watcher = second_lifecycle
        .try_start_completion_watcher()
        .expect("register held watcher");
    let state = control.upgrade().expect("thread manager should be live");
    let EvictionScan { result, blockers } = control
        .agent_residency
        .try_unload_one_resident(&control, &state, &[])
        .await;
    assert!(
        matches!(result, EvictionResult::Retry),
        "held lifecycle work must defer eviction"
    );
    assert!(
        matches!(
            blockers.as_slice(),
            [
                ResidencyBlocker::Transition(_),
                ResidencyBlocker::CompletionWatcher(_)
            ]
        ),
        "the scan must retain both lifecycle transition and completion watcher blockers"
    );
    drop(state);

    // Poll the production worker directly: the fixture's locks are otherwise uncontended, and
    // disabling Tokio's cooperative budget keeps Pending tied to the held lifecycle blockers.
    let mut trim = Box::pin(tokio::task::unconstrained(
        control
            .agent_residency
            .trim_idle_residents(&control, /*resident_capacity*/ 1),
    ));
    assert!(futures::poll!(trim.as_mut()).is_pending());
    let expected = match released {
        ReleasedBlocker::Transition => {
            drop(transition);
            (false, true)
        }
        ReleasedBlocker::CompletionWatcher => {
            drop(watcher);
            (true, false)
        }
    };
    timeout(Duration::from_secs(/*secs*/ 5), trim)
        .await
        .expect("releasing either blocker must finish trimming without another reservation");
    assert_eq!(
        (
            manager.get_thread(first.thread_id).await.is_ok(),
            manager.get_thread(second.thread_id).await.is_ok(),
        ),
        expected
    );
}

#[tokio::test]
async fn deferred_trim_wakes_for_a_new_completion_with_the_old_transition_held() {
    let TrimFixture {
        _home,
        config,
        manager,
        control,
        first,
        first_lifecycle,
        second,
        second_lifecycle: _,
    } = trim_fixture().await;
    mark_thread_completed(first.thread.as_ref()).await;
    let _transition = first_lifecycle.lock_transition().await;
    let residency = &control.agent_residency;
    // Own the scheduled worker while polling it directly, so a completion joins this worker
    // through the real scheduling entry point instead of starting a second one.
    assert!(!residency.trim_scheduled.swap(true, Ordering::AcqRel));
    let mut trim = Box::pin(tokio::task::unconstrained(
        residency.trim_idle_residents(&control, /*resident_capacity*/ 1),
    ));
    assert!(futures::poll!(trim.as_mut()).is_pending());
    mark_thread_completed(second.thread.as_ref()).await;
    control.schedule_agent_residency_trim(
        &config,
        MultiAgentVersion::V2,
        &second.thread.session_source,
    );
    timeout(Duration::from_secs(/*secs*/ 5), trim)
        .await
        .expect("new completion must wake trimming without releasing the old transition");
    residency.trim_scheduled.store(false, Ordering::Release);
    assert_eq!(
        (
            manager.get_thread(first.thread_id).await.is_ok(),
            manager.get_thread(second.thread_id).await.is_ok(),
        ),
        (true, false)
    );
}

#[tokio::test]
async fn deferred_trim_retains_completion_notices_while_a_candidate_is_out_of_the_lru() {
    let TrimFixture {
        _home,
        config,
        manager,
        control,
        first,
        first_lifecycle: _,
        second,
        second_lifecycle,
    } = trim_fixture().await;
    mark_thread_completed(second.thread.as_ref()).await;
    let _watcher = second_lifecycle
        .try_start_completion_watcher()
        .expect("register held watcher");
    let residency = &control.agent_residency;
    assert!(!residency.trim_scheduled.swap(true, Ordering::AcqRel));
    let mut trim = Box::pin(tokio::task::unconstrained(
        residency.trim_idle_residents(&control, /*resident_capacity*/ 1),
    ));
    poll_fn(|cx| {
        // Completion cleared this turn, and the fixture has no task runner.
        let _scan_gate = second
            .thread
            .session
            .active_turn
            .try_lock()
            .expect("completed fixture turn must be unlocked");
        assert!(trim.as_mut().poll(cx).is_pending());
        // The first resident was nonterminal when scanned. The second is now popped and waiting
        // at its active-turn lock, making the count temporarily equal to the retention target.
        assert_eq!(residency.resident_count(), 1);
        Poll::Ready(())
    })
    .await;
    // This worker advances only when polled, so the candidate stays popped while completion
    // is recorded after releasing the scan gate.
    mark_thread_completed(first.thread.as_ref()).await;
    control.schedule_agent_residency_trim(
        &config,
        MultiAgentVersion::V2,
        &first.thread.session_source,
    );
    timeout(Duration::from_secs(/*secs*/ 5), trim)
        .await
        .expect("completion during a scan must survive the temporary resident count");
    residency.trim_scheduled.store(false, Ordering::Release);
    assert!(second_lifecycle.completion_watcher_active());
    assert_eq!(
        (
            manager.get_thread(first.thread_id).await.is_ok(),
            manager.get_thread(second.thread_id).await.is_ok(),
        ),
        (false, true)
    );
}

#[tokio::test]
async fn cancelling_deferred_trim_releases_its_lifecycle_waiters() {
    let TrimFixture {
        _home,
        config: _,
        manager,
        control,
        first,
        first_lifecycle,
        second,
        second_lifecycle,
    } = trim_fixture().await;
    mark_thread_completed(first.thread.as_ref()).await;
    mark_thread_completed(second.thread.as_ref()).await;
    let transition = first_lifecycle.lock_transition().await;
    let watcher = second_lifecycle
        .try_start_completion_watcher()
        .expect("register held watcher");
    let mut trim = Box::pin(tokio::task::unconstrained(
        control
            .agent_residency
            .trim_idle_residents(&control, /*resident_capacity*/ 1),
    ));
    assert!(futures::poll!(trim.as_mut()).is_pending());
    drop(trim);
    drop(transition);
    let _transition = first_lifecycle
        .try_lock_transition()
        .expect("cancelled trim must not retain a transition waiter or guard");
    assert!(second_lifecycle.completion_watcher_active());
    assert_eq!(
        (
            manager.get_thread(first.thread_id).await.is_ok(),
            manager.get_thread(second.thread_id).await.is_ok(),
        ),
        (true, true)
    );
    drop(watcher);
}

#[tokio::test]
async fn warm_agent_access_updates_residency_eviction_order() {
    let mut config = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.multi_agent_v2.max_concurrent_threads_per_session = 3;
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
    let first = spawn_subagent(
        &control,
        &state,
        config.clone(),
        root.thread_id,
        "recently-used",
    )
    .await;
    first_slot.commit(first.thread_id);
    mark_thread_completed(first.thread.as_ref()).await;

    let second_slot = control
        .reserve_agent_residency_slot(
            &state,
            &config,
            MultiAgentVersion::V2,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("second resident slot");
    let second = spawn_subagent(
        &control,
        &state,
        config.clone(),
        root.thread_id,
        "least-recently-used",
    )
    .await;
    second_slot.commit(second.thread_id);
    mark_thread_completed(second.thread.as_ref()).await;

    control
        .state
        .reserve_spawn_slot(/*max_threads*/ None)
        .expect("first child metadata should register")
        .commit(AgentMetadata {
            agent_id: Some(first.thread_id),
            ..Default::default()
        });
    control
        .ensure_agent_loaded(config.clone(), first.thread_id)
        .await
        .expect("warm agent should remain loaded");

    let _third_slot = control
        .reserve_agent_residency_slot(
            &state,
            &config,
            MultiAgentVersion::V2,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("third reservation should evict the least recently used child");

    assert!(manager.get_thread(first.thread_id).await.is_ok());
    let err = manager
        .get_thread(second.thread_id)
        .await
        .err()
        .expect("least recently used child should have been evicted");
    match err.details() {
        CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, second.thread_id),
        _ => panic!("expected the older child to be missing, got {err:?}"),
    }
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

    control
        .state
        .reserve_spawn_slot(/*max_threads*/ None)
        .expect("reserve first registry slot")
        .commit(AgentMetadata {
            agent_id: Some(first.thread_id),
            ..Default::default()
        });
    let lifecycle = control
        .get_agent_metadata(first.thread_id)
        .expect("registered first resident")
        .lifecycle;
    let transition = lifecycle.lock_transition().await;
    let error = timeout(
        Duration::from_secs(5),
        control.reserve_agent_residency_slot(
            &state,
            &config,
            multi_agent_version,
            /*protected_thread_id*/ None,
        ),
    )
    .await
    .expect("eviction must not wait for another lifecycle transition")
    .err()
    .expect("locked resident cannot be evicted");
    assert_matches!(
        error.details(),
        CodexErrorDetails::AgentLimitReached { max_threads: _ }
    );
    drop(transition);

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
