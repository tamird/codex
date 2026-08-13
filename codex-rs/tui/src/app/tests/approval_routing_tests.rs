use super::*;
use futures::FutureExt;
use pretty_assertions::assert_eq;

async fn app_with_pending_approval(capacity: usize) -> Result<(App, ThreadId)> {
    let mut app = make_test_app().await;
    let main_thread_id = ThreadId::new();
    let agent_thread_id = ThreadId::new();
    let primary_session = test_thread_session(main_thread_id, test_path_buf("/tmp/main"));

    app.primary_thread_id = Some(main_thread_id);
    app.active_thread_id = Some(main_thread_id);
    app.primary_session_configured = Some(primary_session.clone());
    app.thread_event_channels.insert(
        main_thread_id,
        ThreadEventChannel::new_with_session(/*capacity*/ 1, primary_session, Vec::new()),
    );
    app.thread_event_channels.insert(
        agent_thread_id,
        ThreadEventChannel::new_with_session(
            capacity,
            ThreadSessionState {
                approval_policy: AskForApproval::OnRequest,
                permission_profile: PermissionProfile::workspace_write(),
                ..test_thread_session(agent_thread_id, test_path_buf("/tmp/agent"))
            },
            Vec::new(),
        ),
    );
    app.upsert_agent_picker_thread(
        agent_thread_id,
        Some("Robie".to_string()),
        Some("explorer".to_string()),
        /*is_closed*/ false,
    );
    app.enqueue_thread_request(
        agent_thread_id,
        exec_approval_request(
            agent_thread_id,
            "turn-approval",
            "call-approval",
            /*approval_id*/ None,
        ),
    )
    .await?;
    Ok((app, agent_thread_id))
}

#[tokio::test]
async fn agent_message_delta_does_not_wait_for_unrelated_approval_store() -> Result<()> {
    let mut app = make_test_app().await;
    let main_thread_id = ThreadId::new();
    let agent_thread_id = ThreadId::new();
    let unrelated_thread_id = ThreadId::new();

    app.primary_thread_id = Some(main_thread_id);
    app.active_thread_id = Some(main_thread_id);
    app.thread_event_channels
        .insert(main_thread_id, ThreadEventChannel::new(/*capacity*/ 1));
    app.thread_event_channels
        .insert(agent_thread_id, ThreadEventChannel::new(/*capacity*/ 1));

    let unrelated_channel = ThreadEventChannel::new(/*capacity*/ 1);
    let unrelated_store = Arc::clone(&unrelated_channel.store);
    app.thread_event_channels
        .insert(unrelated_thread_id, unrelated_channel);
    let _unrelated_store_lock = unrelated_store.lock().await;

    app.enqueue_thread_notification(
        agent_thread_id,
        agent_message_delta_notification(agent_thread_id, "turn-1", "message-1", "hello"),
    )
    .now_or_never()
    .expect("agent message delta should not scan unrelated approval stores")?;

    Ok(())
}

#[tokio::test]
async fn pending_approval_label_updates_the_thread_with_a_duplicate_label() -> Result<()> {
    let (mut app, first_thread_id) = app_with_pending_approval(/*capacity*/ 4).await?;
    assert_eq!(
        app.chat_widget.pending_thread_approvals(),
        &["Robie [explorer]".to_string()]
    );
    let second_thread_id =
        ThreadId::from_string("ffffffff-ffff-ffff-ffff-ffffffffffff").expect("valid thread id");

    app.thread_event_channels.insert(
        second_thread_id,
        ThreadEventChannel::new_with_session(
            /*capacity*/ 4,
            ThreadSessionState {
                approval_policy: AskForApproval::OnRequest,
                permission_profile: PermissionProfile::workspace_write(),
                ..test_thread_session(second_thread_id, test_path_buf("/tmp/second-agent"))
            },
            Vec::new(),
        ),
    );
    app.upsert_agent_picker_thread(
        second_thread_id,
        Some("Robie".to_string()),
        Some("explorer".to_string()),
        /*is_closed*/ false,
    );
    app.enqueue_thread_request(
        second_thread_id,
        exec_approval_request(
            second_thread_id,
            "second-turn-approval",
            "second-call-approval",
            /*approval_id*/ None,
        ),
    )
    .await?;

    app.upsert_agent_picker_thread(
        first_thread_id,
        Some("Nia".to_string()),
        Some("researcher".to_string()),
        /*is_closed*/ false,
    );

    assert_eq!(
        app.chat_widget.pending_thread_approvals(),
        &[
            "Nia [researcher]".to_string(),
            "Robie [explorer]".to_string()
        ]
    );

    Ok(())
}

#[tokio::test]
async fn evicted_approval_disappears_after_history_response() -> Result<()> {
    let (mut app, agent_thread_id) = app_with_pending_approval(/*capacity*/ 1).await?;

    app.enqueue_thread_history_entry_response(
        agent_thread_id,
        HistoryLookupResponse::Entry {
            offset: 0,
            log_id: 1,
            entry: None,
        },
    )
    .await?;

    assert!(app.chat_widget.pending_thread_approvals().is_empty());

    Ok(())
}

#[tokio::test]
async fn failed_interrupt_evicts_pending_approval_through_app_event() -> Result<()> {
    let (mut app, agent_thread_id) = app_with_pending_approval(/*capacity*/ 1).await?;
    let mut tui = crate::tui::test_support::make_test_tui()?;
    let mut app_server = Box::pin(crate::start_embedded_app_server_for_picker(&app.config)).await?;
    let (app_event_tx, mut app_event_rx) = mpsc::unbounded_channel();
    app.app_event_tx = AppEventSender::new(app_event_tx);

    assert!(
        app.try_submit_active_thread_op_via_app_server(
            &mut app_server,
            agent_thread_id,
            &AppCommand::interrupt(),
        )
        .await?
    );
    let event = tokio::time::timeout(std::time::Duration::from_secs(5), app_event_rx.recv())
        .await?
        .expect("failed interrupt should emit a thread notification");
    Box::pin(app.handle_event(&mut tui, &mut app_server, event)).await?;

    assert!(app.chat_widget.pending_thread_approvals().is_empty());

    {
        let store = app.thread_event_channels[&agent_thread_id]
            .store
            .lock()
            .await;
        assert!(matches!(
            store.buffer.front(),
            Some(ThreadBufferedEvent::Notification(notification))
                if matches!(
                    notification.as_ref(),
                    ServerNotification::Warning(notification)
                if notification.message.contains("thread not found")
                    && notification.message.contains(&agent_thread_id.to_string())
                )
        ));
    }

    app_server.shutdown().await?;

    Ok(())
}

#[tokio::test]
async fn delayed_synthetic_warning_does_not_reach_recreated_thread() -> Result<()> {
    let (mut app, agent_thread_id) = app_with_pending_approval(/*capacity*/ 1).await?;
    let mut tui = crate::tui::test_support::make_test_tui()?;
    let mut app_server = Box::pin(crate::start_embedded_app_server_for_picker(&app.config)).await?;
    let channel_identity = app.thread_event_channels[&agent_thread_id].identity();

    app.reset_thread_event_state();
    app.thread_event_channels
        .insert(agent_thread_id, ThreadEventChannel::new(/*capacity*/ 1));
    app.upsert_agent_picker_thread(
        agent_thread_id,
        Some("Nia".to_string()),
        Some("researcher".to_string()),
        /*is_closed*/ false,
    );
    app.enqueue_thread_request(
        agent_thread_id,
        exec_approval_request(
            agent_thread_id,
            "new-turn-approval",
            "new-call-approval",
            /*approval_id*/ None,
        ),
    )
    .await?;

    Box::pin(app.handle_event(
        &mut tui,
        &mut app_server,
        AppEvent::ThreadNotification {
            thread_id: agent_thread_id,
            channel_identity,
            notification: ServerNotification::Warning(WarningNotification {
                thread_id: Some(agent_thread_id.to_string()),
                message: "Failed to interrupt turn".to_string(),
            }),
        },
    ))
    .await?;

    assert_eq!(
        app.chat_widget.pending_thread_approvals(),
        &["Nia [researcher]".to_string()]
    );
    {
        let store = app.thread_event_channels[&agent_thread_id]
            .store
            .lock()
            .await;
        assert!(matches!(
            store.buffer.front(),
            Some(ThreadBufferedEvent::Request(_))
        ));
    }

    app_server.shutdown().await?;

    Ok(())
}

#[tokio::test]
async fn evicted_approval_disappears_after_feedback_submission() -> Result<()> {
    let (mut app, agent_thread_id) = app_with_pending_approval(/*capacity*/ 1).await?;

    app.enqueue_thread_feedback_event(
        agent_thread_id,
        FeedbackThreadEvent {
            category: FeedbackCategory::Bug,
            include_logs: false,
            feedback_audience: FeedbackAudience::External,
            result: Ok("feedback-thread".to_string()),
        },
    )
    .await;

    assert!(app.chat_widget.pending_thread_approvals().is_empty());

    Ok(())
}
