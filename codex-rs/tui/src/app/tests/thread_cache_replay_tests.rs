use super::*;
use crate::app::thread_cache_eviction::TerminalDelivery;
use crate::app::thread_cache_eviction::TerminalNotification;
use crate::app_server_session::TurnPermissionsOverride;
use assert_matches::assert_matches;
use codex_app_server_client::AppServerEvent;
use core_test_support::responses;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn replayed_completion_for_another_turn_preserves_running_ui() {
    let (mut app, _events, mut ops) = make_test_app_with_channels().await;
    let thread_id = ThreadId::new();
    app.chat_widget.handle_thread_session(test_thread_session(
        thread_id,
        test_path_buf("/tmp/project"),
    ));
    app.chat_widget.handle_server_notification(
        turn_started_notification(thread_id, "running"),
        /*replay_kind*/ None,
    );
    app.chat_widget
        .apply_external_edit("queued follow-up".to_string());
    app.chat_widget
        .handle_key_event(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
    let input = app.chat_widget.capture_thread_input_state();
    let rendered = render_bottom_popup(&app.chat_widget, /*width*/ 80);
    for status in [
        TurnStatus::Completed,
        TurnStatus::Interrupted,
        TurnStatus::Failed,
    ] {
        let mut completion = assert_matches!(
            turn_completed_notification(thread_id, "older", status),
            ServerNotification::TurnCompleted(completion) => completion
        );
        completion.turn.error = Some(AppServerTurnError {
            message: "older policy stop".to_string(),
            codex_error_info: Some(AppServerCodexErrorInfo::MisalignmentPolicyViolation),
            additional_details: None,
            misalignment: None,
        });
        app.handle_thread_event_replay(ThreadBufferedEvent::Notification(Box::new(
            ServerNotification::TurnCompleted(completion),
        )));
        assert_eq!(app.chat_widget.capture_thread_input_state(), input);
        assert_eq!(
            render_bottom_popup(&app.chat_widget, /*width*/ 80),
            rendered
        );
    }
    while let Ok(op) = ops.try_recv() {
        assert!(!matches!(op, Op::UserTurn { .. }));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hydrated_running_turn_keeps_queued_input_until_live_completion() -> Result<()> {
    let mut response_streams = (0..6)
        .map(|index| {
            let response_id = format!("completed-{index}");
            vec![StreamingSseChunk {
                gate: None,
                body: responses::sse(vec![
                    responses::ev_response_created(&response_id),
                    responses::ev_assistant_message(&format!("message-{index}"), "done"),
                    responses::ev_completed(&response_id),
                ]),
            }]
        })
        .collect::<Vec<_>>();
    let (release_completion, completion_gate) = tokio::sync::oneshot::channel();
    response_streams.push(vec![
        StreamingSseChunk {
            gate: None,
            body: responses::sse(vec![responses::ev_response_created("live-response")]),
        },
        StreamingSseChunk {
            gate: Some(completion_gate),
            body: responses::sse(vec![responses::ev_completed("live-response")]),
        },
    ]);
    let (model_server, _completions) = start_streaming_sse_server(response_streams).await;
    let (mut app, _app_events, _old_ops) = make_test_app_with_channels().await;
    let codex_home = tempdir()?;
    app.config.codex_home = codex_home.path().to_path_buf().abs();
    app.config.sqlite = codex_state::SqliteConfig::new_for_testing(codex_home.path().abs());
    std::fs::write(
        codex_home.path().join("config.toml"),
        format!(
            r#"
model = "gpt-5.2"
model_provider = "replay-test"

[model_providers.replay-test]
name = "Replay test"
base_url = "{}/v1"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
"#,
            model_server.uri()
        ),
    )?;
    app.refresh_in_memory_config_from_disk().await?;
    app.config.terminal_resize_reflow.max_rows = TerminalResizeReflowMaxRows::Limit(1);
    let mut app_server = Box::pin(crate::start_embedded_app_server_for_picker(&app.config)).await?;
    let started = app_server.start_thread(&app.config).await?;
    let thread_id = started.session.thread_id;
    let mut oldest_completion = None;
    let mut live_turn_id = None;
    for index in 0..7 {
        let response = app_server
            .turn_start(
                thread_id,
                vec![AppServerUserInput::Text {
                    text: format!(
                        "Turn {index}: keep this transcript visible\n{}",
                        "context\n".repeat(/*n*/ 8)
                    ),
                    text_elements: Vec::new(),
                }],
                app.config.cwd.to_path_buf(),
                app.config.permissions.approval_policy.value().into(),
                app.config.approvals_reviewer,
                TurnPermissionsOverride::Preserve,
                app.config.permissions.user_visible_workspace_roots(),
                "gpt-5.2".to_string(),
                /*effort*/ None,
                /*summary*/ None,
                /*service_tier*/ None,
                /*collaboration_mode*/ None,
                /*personality*/ None,
                /*output_schema*/ None,
            )
            .await?;
        if index == 6 {
            live_turn_id = Some(response.turn.id);
            break;
        }
        let completed = tokio::time::timeout(std::time::Duration::from_secs(/*secs*/ 10), async {
            loop {
                let event = app_server.next_event().await.expect("open event stream");
                if let AppServerEvent::ServerNotification(notification) = event
                    && let ServerNotification::TurnCompleted(completed) = *notification
                    && completed.turn.id == response.turn.id
                {
                    break completed;
                }
            }
        })
        .await?;
        if index == 0 {
            oldest_completion = Some(completed);
        }
    }
    tokio::time::timeout(
        std::time::Duration::from_secs(/*secs*/ 10),
        model_server.wait_for_request_count(/*count*/ 7),
    )
    .await?;
    let oldest_completion = oldest_completion.expect("first actual completion");
    let oldest_turn_id = oldest_completion.turn.id.clone();
    let live_turn_id = live_turn_id.expect("gated live turn");
    let mut session = started.session;
    session.rollout_path = app_server
        .thread_read(thread_id, /*include_turns*/ false)
        .await?
        .path;
    assert!(session.rollout_path.is_some());
    app.chat_widget.handle_thread_session(session.clone());
    app.chat_widget.handle_server_notification(
        turn_started_notification(thread_id, &oldest_turn_id),
        /*replay_kind*/ None,
    );
    app.chat_widget
        .apply_external_edit("queued follow-up".to_string());
    app.chat_widget
        .handle_key_event(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
    let channel = ThreadEventChannel::new_with_session(/*capacity*/ 1, session, Vec::new());
    {
        let mut store = channel.store.lock().await;
        store.input_state = app.chat_widget.capture_thread_input_state();
        store.push_notification(turn_started_notification(thread_id, &oldest_turn_id));
        store.push_notification(ServerNotification::TurnCompleted(oldest_completion));
        assert!(store.can_evict_history());
        store.evict_history();
    }
    app.thread_event_channels.insert(thread_id, channel);
    let mut tui = crate::tui::test_support::make_test_tui()?;
    // Hydrate real bounded pages while the newest response is gated, without routing its
    // TurnStarted notification into the TUI store that still owns the first completion.
    app.reload_evicted_thread_history(&mut tui, &mut app_server, thread_id)
        .await?;
    let (receiver, snapshot) = app
        .activate_thread_for_replay(thread_id)
        .await
        .expect("activate the rehydrated channel");
    assert!(!snapshot.turns.iter().any(|turn| turn.id == oldest_turn_id));
    assert_eq!(
        snapshot.turns.last().map(|turn| (&turn.id, &turn.status)),
        Some((&live_turn_id, &TurnStatus::InProgress))
    );
    assert!(snapshot.events.iter().any(|event| {
        matches!(event, ThreadBufferedEvent::Notification(notification)
            if matches!(notification.as_ref(), ServerNotification::TurnCompleted(completed)
                if completed.turn.id == oldest_turn_id))
    }));
    let (chat_widget, _sender, _events, mut ops) = make_chatwidget_manual_with_sender().await;
    app.chat_widget = chat_widget;
    // The initial thread/start event was consumed while producing the persisted history.
    app.primary_thread_id = Some(thread_id);
    app.active_thread_id = Some(thread_id);
    app.active_thread_rx = Some(receiver);
    while ops.try_recv().is_ok() {}
    app.replay_thread_snapshot(snapshot, /*resume_restored_queue*/ true);
    assert!(app.chat_widget.is_task_running_for_test());
    assert_eq!(
        app.chat_widget.queued_user_message_texts(),
        vec!["queued follow-up"]
    );
    while let Ok(op) = ops.try_recv() {
        assert!(!matches!(op, Op::UserTurn { .. }));
    }
    {
        let store = app.thread_event_channels[&thread_id].store.lock().await;
        assert_matches!(store.terminal_notification.as_ref(), Some(TerminalNotification::Completed {
            notification,
            delivery: TerminalDelivery::Applied,
        }) => assert_eq!(notification.turn.id, oldest_turn_id));
    }

    release_completion.send(()).expect("release live response");
    tokio::time::timeout(std::time::Duration::from_secs(/*secs*/ 10), async {
        loop {
            let event = app_server.next_event().await.expect("open event stream");
            let finished = matches!(&event, AppServerEvent::ServerNotification(notification)
                if matches!(notification.as_ref(), ServerNotification::TurnCompleted(completed)
                    if completed.turn.id == live_turn_id));
            app.handle_app_server_event(&app_server, event).await;
            app.drain_active_thread_events_until(
                &mut tui,
                Instant::now() + std::time::Duration::from_secs(/*secs*/ 1),
            )
            .await?;
            if finished {
                break Ok::<(), color_eyre::Report>(());
            }
        }
    })
    .await??;
    assert_matches!(next_user_turn_op(&mut ops), Op::UserTurn { items, .. } => {
        assert_eq!(items, vec![codex_app_server_protocol::UserInput::Text {
            text: "queued follow-up".to_string(),
            text_elements: Vec::new(),
        }]);
    });
    assert!(app.chat_widget.queued_user_message_texts().is_empty());
    {
        let store = app.thread_event_channels[&thread_id].store.lock().await;
        assert_matches!(store.terminal_notification.as_ref(), Some(TerminalNotification::Completed {
            notification,
            delivery: TerminalDelivery::Applied,
        }) => assert_eq!(notification.turn.id, live_turn_id));
    }
    while let Ok(op) = ops.try_recv() {
        assert!(!matches!(op, Op::UserTurn { .. }));
    }
    app_server.shutdown().await?;
    model_server.shutdown().await;
    Ok(())
}
