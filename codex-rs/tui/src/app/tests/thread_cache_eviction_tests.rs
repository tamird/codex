use super::*;
use crate::app::thread_cache_eviction::HistoryPinReason;
use crate::app::thread_cache_eviction::TerminalDelivery;
use crate::app::thread_cache_eviction::TerminalNotification;
use assert_matches::assert_matches;
use codex_app_server_protocol::TurnItemsView;
use pretty_assertions::assert_eq;

#[test]
fn history_reload_merges_recap_progress_without_resetting_live_state() {
    let mut store = ThreadEventStore::new(/*capacity*/ 8);
    store.set_turns(vec![
        test_turn("completed", TurnStatus::Completed, Vec::new()),
        test_turn("live", TurnStatus::InProgress, Vec::new()),
    ]);
    store.merge_recap_progress(recap::RecapProgress {
        completed_turns: 1,
        last_recapped_turn_count: Some(1),
    });
    store.evict_history();
    assert_eq!(store.recap_progress().last_recapped_turn_count, Some(1));
    store.set_history_payload(vec![
        test_turn("completed", TurnStatus::Completed, Vec::new()),
        test_turn("live", TurnStatus::Completed, Vec::new()),
    ]);
    assert_eq!(
        (store.recap_progress(), store.active_turn_id()),
        (
            recap::RecapProgress {
                completed_turns: 2,
                last_recapped_turn_count: Some(1),
            },
            Some("live"),
        )
    );
}

#[test]
fn history_pin_reason_distinguishes_missing_history_and_first_blocker() {
    let thread_id = ThreadId::new();
    let mut store = ThreadEventStore::new(/*capacity*/ 8);
    assert_eq!(
        store.history_pin_reason(),
        Some(HistoryPinReason::MissingSession)
    );

    store.active = true;
    store.active_turn_id = Some("running".to_string());
    assert_eq!(store.history_pin_reason(), Some(HistoryPinReason::Active));
    store.active = false;
    assert_eq!(
        store.history_pin_reason(),
        Some(HistoryPinReason::RunningTurn)
    );
    store.clear_active_turn_id();

    let mut session = test_thread_session(thread_id, test_path_buf("/tmp/project"));
    session.rollout_path = None;
    store.set_session(session.clone(), Vec::new());
    assert_eq!(
        store.history_pin_reason(),
        Some(HistoryPinReason::MissingRollout)
    );
    assert!(!store.can_evict_history());

    session.rollout_path = Some(test_path_buf("/tmp/saved.jsonl"));
    store.set_session(session, Vec::new());
    assert_eq!(store.history_pin_reason(), None);
    assert!(store.can_evict_history());
}

#[tokio::test]
async fn shared_history_budget_reclaims_completed_payloads_and_preserves_control() -> Result<()> {
    let mut app = make_test_app().await;
    app.chat_widget
        .apply_external_edit("saved draft".to_string());
    let draft = app.chat_widget.capture_thread_input_state();
    let [large, small, running, approval] = std::array::from_fn(|_| ThreadId::new());
    for (thread_id, text) in [
        (large, "the larger completed transcript"),
        (small, "small"),
        (running, "still running"),
        (approval, "awaiting approval"),
    ] {
        {
            let channel = app.ensure_thread_channel(thread_id);
            let mut store = channel.store.lock().await;
            store.set_session(
                test_thread_session(thread_id, test_path_buf("/tmp/project")),
                Vec::new(),
            );
            store.input_state = draft.clone();
        }
        app.enqueue_thread_notification(thread_id, turn_started_notification(thread_id, "turn-1"))
            .await?;
        app.enqueue_thread_notification(
            thread_id,
            agent_message_delta_notification(thread_id, "turn-1", "answer", text),
        )
        .await?;
        if thread_id != running {
            let mut completed = assert_matches!(
                turn_completed_notification(thread_id, "turn-1", TurnStatus::Completed),
                ServerNotification::TurnCompleted(completed) => completed
            );
            completed.turn.items.push(ThreadItem::AgentMessage {
                id: "answer".to_string(),
                text: text.to_string(),
                phase: None,
                memory_citation: None,
                delivery: None,
            });
            app.enqueue_thread_notification(
                thread_id,
                ServerNotification::TurnCompleted(completed),
            )
            .await?;
        }
    }
    let pending_request =
        exec_approval_request(approval, "turn-2", "approval", /*approval_id*/ None);
    app.enqueue_thread_request(approval, pending_request.clone())
        .await?;
    for (thread_id, expected_reason) in [
        (small, None),
        (running, Some(HistoryPinReason::RunningTurn)),
        (approval, Some(HistoryPinReason::PendingInteractive)),
    ] {
        let channel = app.thread_event_channels.get(&thread_id).unwrap();
        let store = channel.store.lock().await;
        assert_eq!(store.history_pin_reason(), expected_reason);
    }
    {
        let channel = app.thread_event_channels.get(&small).unwrap();
        let mut store = channel.store.lock().await;
        let mut pending_input = draft.clone().expect("saved composer state");
        pending_input.acknowledge_started_turn();
        store.input_state = Some(pending_input);
        assert_eq!(
            store.history_pin_reason(),
            Some(HistoryPinReason::InFlightInput)
        );
        assert!(!store.can_evict_history());
        store.input_state = draft.clone();
    }
    let recalled = HistoryLookupResponse::Entry {
        offset: 0,
        log_id: 1,
        entry: Some("composer history".to_string()),
    };
    app.enqueue_thread_history_entry_response(large, recalled.clone())
        .await?;
    let (budget, saved_input) = {
        let store = app.thread_event_channels[&large].store.lock().await;
        (store.history_payload_bytes(), store.input_state.clone())
    };

    // Each completed thread fits individually; their combined payload exceeds the shared budget.
    app.trim_thread_cache(budget);

    {
        let store = app.thread_event_channels[&large].store.lock().await;
        assert_eq!(
            (store.history_reload_required, store.history_payload_bytes()),
            (true, 0)
        );
        assert_eq!(store.input_state, saved_input);
        let snapshot = store.snapshot();
        assert_matches!(snapshot.events.as_slice(), [ThreadBufferedEvent::HistoryEntryResponse(entry), ThreadBufferedEvent::Notification(notification)] => {
            assert_eq!(entry, &recalled);
            let completed = assert_matches!(notification.as_ref(), ServerNotification::TurnCompleted(completed) => completed);
            let mut expected = assert_matches!(turn_completed_notification(large, "turn-1", TurnStatus::Completed), ServerNotification::TurnCompleted(completed) => completed);
            expected.turn.items_view = TurnItemsView::NotLoaded;
            assert_eq!(completed, &expected);
        });
    }
    for thread_id in [small, running, approval] {
        let store = app.thread_event_channels[&thread_id].store.lock().await;
        assert!(!store.history_reload_required);
        assert!(store.history_payload_bytes() != 0);
    }
    assert_eq!(
        serde_json::to_value(
            app.thread_event_channels[&approval]
                .store
                .lock()
                .await
                .pending_replay_requests()
        )?,
        serde_json::to_value(vec![pending_request])?
    );

    // A later live start supersedes the compact completion, including after a stale cache read.
    app.enqueue_thread_notification(large, turn_started_notification(large, "turn-2"))
        .await?;
    let store = app.thread_event_channels[&large].store.lock().await;
    assert!(store.terminal_notification.is_none());
    assert_eq!(store.active_turn_id(), Some("turn-2"));
    Ok(())
}

#[tokio::test]
async fn cache_eviction_waits_for_live_completion_delivery_and_respects_history_order() -> Result<()>
{
    let mut app = make_test_app().await;
    let thread_id = ThreadId::new();
    let session = test_thread_session(thread_id, test_path_buf("/tmp/project"));
    app.ensure_thread_channel(thread_id)
        .store
        .lock()
        .await
        .set_session(session.clone(), Vec::new());
    app.chat_widget.handle_thread_session(session);
    app.activate_thread_channel(thread_id).await;
    app.enqueue_thread_notification(thread_id, turn_started_notification(thread_id, "turn-a"))
        .await?;
    app.enqueue_thread_notification(
        thread_id,
        turn_completed_notification(thread_id, "turn-a", TurnStatus::Completed),
    )
    .await?;
    app.store_active_thread_receiver().await;
    app.active_thread_id = None;
    app.trim_thread_cache(/*budget*/ 0);
    {
        let mut store = app.thread_event_channels[&thread_id].store.lock().await;
        assert!(!store.history_reload_required);
        assert_eq!(
            store.history_pin_reason(),
            Some(HistoryPinReason::PendingLiveCompletion)
        );
        store.set_history_payload(vec![
            test_turn("turn-a", TurnStatus::Completed, Vec::new()),
            test_turn("turn-b", TurnStatus::InProgress, Vec::new()),
        ]);
        let _snapshot = store.snapshot();
        assert_matches!(
            store.terminal_notification.as_ref(),
            Some(TerminalNotification::Completed {
                notification: _,
                delivery: TerminalDelivery::PendingLive,
            })
        );
    }

    app.activate_thread_channel(thread_id).await;
    let mut tui = crate::tui::test_support::make_test_tui()?;
    app.drain_active_thread_events(&mut tui).await?;
    app.store_active_thread_receiver().await;
    app.active_thread_id = None;
    app.trim_thread_cache(/*budget*/ 0);
    let mut store = app.thread_event_channels[&thread_id].store.lock().await;
    assert!(store.history_reload_required);
    assert_eq!(store.history_pin_reason(), None);
    assert_matches!(
        store.terminal_notification.as_ref(),
        Some(TerminalNotification::Completed {
            notification: _,
            delivery: TerminalDelivery::Applied,
        })
    );

    // Ordered history proves B follows the applied A; a different id without A does not.
    store.set_history_payload(vec![
        test_turn("turn-a", TurnStatus::InProgress, Vec::new()),
        test_turn("turn-b", TurnStatus::InProgress, Vec::new()),
    ]);
    assert!(store.terminal_replay_event().is_none());
    store.set_history_payload(vec![test_turn(
        "turn-b",
        TurnStatus::InProgress,
        Vec::new(),
    )]);
    let event = assert_matches!(store.terminal_replay_event(), Some(event) => event);
    let notification =
        assert_matches!(event, ThreadBufferedEvent::Notification(notification) => notification);
    assert_matches!(*notification, ServerNotification::TurnCompleted(notification) => {
        assert_eq!(notification.turn.id, "turn-a");
    });
    Ok(())
}

#[tokio::test]
async fn cache_report_counts_first_pin_reasons_without_retained_content() {
    #[derive(Clone)]
    struct LogWriter(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for LogWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut app = make_test_app().await;
    let [saved, unsaved, side] = std::array::from_fn(|_| ThreadId::new());
    for thread_id in [saved, unsaved, side] {
        let mut store = app.ensure_thread_channel(thread_id).store.lock().await;
        if thread_id == saved {
            store.set_session(
                test_thread_session(thread_id, test_path_buf("/tmp/project")),
                Vec::new(),
            );
        }
        store.push_notification(agent_message_delta_notification(
            thread_id,
            "turn",
            "answer",
            "private retained contents",
        ));
    }
    // Side-thread ownership takes precedence even when a session is also missing.
    app.side_threads.insert(side, SideThreadState::new(saved));

    let output = Arc::new(std::sync::Mutex::new(Vec::new()));
    let writer = LogWriter(Arc::clone(&output));
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(/*ansi*/ false)
        .without_time()
        .with_writer(move || writer.clone())
        .finish();
    let logs = {
        let _guard = tracing::subscriber::set_default(subscriber);
        app.report_thread_cache(saved).await;
        String::from_utf8(output.lock().unwrap().clone()).unwrap()
    };
    let summary = logs
        .lines()
        .find(|line| line.contains("operation=\"tui.thread_cache\""))
        .expect("cache summary");
    assert!(summary.contains("thread_count=3"));
    assert!(summary.contains("first_pin_reason_counts={SideThread: 1, MissingSession: 1}"));
    assert!(logs.contains("first_pin_reason=Some(SideThread)"));
    assert!(logs.contains("first_pin_reason=Some(MissingSession)"));
    assert!(!logs.contains("private retained contents"));

    // Remove the store-level blocker so only app-owned side protection can preserve this history.
    {
        let channel = app.thread_event_channels.get(&side).unwrap();
        let mut store = channel.store.lock().await;
        store.set_session(
            test_thread_session(side, test_path_buf("/tmp/project")),
            Vec::new(),
        );
        assert!(store.can_evict_history());
        assert!(store.history_payload_bytes() != 0);
    }
    app.trim_thread_cache(/*budget*/ 0);
    for (thread_id, expected_evicted) in [(saved, true), (unsaved, false), (side, false)] {
        let channel = app.thread_event_channels.get(&thread_id).unwrap();
        assert_eq!(
            channel.store.lock().await.history_reload_required,
            expected_evicted
        );
    }
}

#[tokio::test]
async fn evicted_policy_stop_survives_stale_history_and_blocks_queued_input() -> Result<()> {
    for (history_id, history_status, refresh_session) in [
        ("older", TurnStatus::Completed, false),
        ("stopped", TurnStatus::Failed, false),
        ("older", TurnStatus::Completed, true),
        ("stopped", TurnStatus::Failed, true),
    ] {
        let (mut app, _app_event_rx, mut op_rx) = make_test_app_with_channels().await;
        let thread_id = ThreadId::new();
        let session = test_thread_session(thread_id, test_path_buf("/tmp/project"));
        app.chat_widget.handle_thread_session(session.clone());
        app.chat_widget.handle_server_notification(
            turn_started_notification(thread_id, "stopped"),
            /*replay_kind*/ None,
        );
        app.chat_widget
            .apply_external_edit("queued follow-up".to_string());
        app.chat_widget
            .handle_key_event(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert_eq!(
            app.chat_widget.queued_user_message_texts(),
            vec!["queued follow-up"]
        );
        let saved_input = app.chat_widget.capture_thread_input_state();
        {
            let channel = app.ensure_thread_channel(thread_id);
            let mut store = channel.store.lock().await;
            store.set_session(session.clone(), Vec::new());
            store.input_state = saved_input;
        }
        app.enqueue_thread_notification(thread_id, turn_started_notification(thread_id, "stopped"))
            .await?;
        let mut completed = assert_matches!(
            turn_completed_notification(thread_id, "stopped", TurnStatus::Failed),
            ServerNotification::TurnCompleted(completed) => completed
        );
        completed.turn.error = Some(codex_app_server_protocol::TurnError {
            message: "policy stop".to_string(),
            codex_error_info: Some(AppServerCodexErrorInfo::MisalignmentPolicyViolation),
            additional_details: None,
            misalignment: None,
        });
        app.enqueue_thread_notification(thread_id, ServerNotification::TurnCompleted(completed))
            .await?;
        app.trim_thread_cache(/*budget*/ 0);
        let mut snapshot = {
            let mut store = app.thread_event_channels[&thread_id].store.lock().await;
            assert!(store.history_reload_required);
            store.set_history_payload(vec![test_turn(history_id, history_status, Vec::new())]);
            store.snapshot()
        };
        if refresh_session {
            let turns = snapshot.turns.clone();
            app.apply_refreshed_snapshot_thread(
                thread_id,
                AppServerStartedThread {
                    session,
                    turns,
                    blocks_direct_input: false,
                    task_tools_available: false,
                },
                &mut snapshot,
            )
            .await;
        }
        app.replay_thread_snapshot(snapshot, /*resume_restored_queue*/ true);
        assert!(app.chat_widget.has_misalignment_policy_violation());
        assert!(app.chat_widget.queued_user_message_texts().is_empty());
        while let Ok(op) = op_rx.try_recv() {
            assert!(!matches!(
                op,
                Op::UserTurn {
                    items: _,
                    cwd: _,
                    approval_policy: _,
                    approvals_reviewer: _,
                    active_permission_profile: _,
                    model: _,
                    effort: _,
                    summary: _,
                    service_tier: _,
                    final_output_json_schema: _,
                    collaboration_mode: _,
                    personality: _,
                }
            ));
        }
    }
    Ok(())
}
