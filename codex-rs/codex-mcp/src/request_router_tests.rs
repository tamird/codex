use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::elicitation::ElicitationRequestManager;
use crate::elicitation::ElicitationRequestRouter;
use crate::mcp::tests::test_elicitation_config;
use crate::request_router::McpConnectionRequestRouter;
use crate::request_router::McpSessionRoute;
use codex_protocol::approvals::ElicitationRequestEvent;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_rmcp_client::Elicitation;
use codex_rmcp_client::ElicitationResponse;
use rmcp::model::ElicitationAction;
use rmcp::model::RequestId;
use tokio::sync::Notify;

fn route() -> (
    Arc<McpSessionRoute>,
    ElicitationRequestManager,
    async_channel::Receiver<Event>,
) {
    let manager = ElicitationRequestManager::new(
        test_elicitation_config(
            "test",
            AskForApproval::OnRequest,
            PermissionProfile::default(),
        ),
        /*reviewer*/ None,
        /*lifecycle*/ None,
        ElicitationRequestRouter::default(),
    );
    let (tx, rx) = async_channel::unbounded();
    (
        Arc::new(McpSessionRoute::new(
            "test-submit".to_string(),
            manager.clone(),
            Some(tx),
        )),
        manager,
        rx,
    )
}

#[tokio::test]
async fn threadless_route_declines_interactive_elicitation() -> anyhow::Result<()> {
    let manager = ElicitationRequestManager::new(
        test_elicitation_config(
            "test",
            AskForApproval::OnRequest,
            PermissionProfile::default(),
        ),
        /*reviewer*/ None,
        /*lifecycle*/ None,
        ElicitationRequestRouter::default(),
    );
    let route = Arc::new(McpSessionRoute::new(
        "threadless".to_string(),
        manager,
        /*tx_event*/ None,
    ));
    let router = McpConnectionRequestRouter::default();
    router.register(&route);
    let sender = router.make_sender("test".to_string());

    let response = router
        .run(route, async move {
            sender(
                RequestId::String("threadless-request".into()),
                Elicitation::OpenAiForm {
                    meta: None,
                    message: "requires a user response".to_string(),
                    requested_schema: serde_json::json!({ "type": "object" }),
                },
            )
            .await
        })
        .await??;

    assert_eq!(response.action, ElicitationAction::Decline);
    Ok(())
}

#[tokio::test]
async fn operations_are_serialized_across_session_routes() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (first_route, _, _) = route();
    let (second_route, _, _) = route();
    router.register(&first_route);
    router.register(&second_route);
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));

    let operations = [first_route, second_route].map(|route| {
        let router = router.clone();
        let active = Arc::clone(&active);
        let maximum = Arc::clone(&maximum);
        tokio::spawn(async move {
            router
                .run(route, async move {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(current, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                })
                .await
        })
    });
    for operation in operations {
        operation.await.expect("operation task should finish")?;
    }

    assert_eq!(maximum.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn operations_from_one_session_route_remain_concurrent() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (route, _, _) = route();
    router.register(&route);
    let rendezvous = Arc::new(tokio::sync::Barrier::new(2));

    let operations = [(), ()].map(|()| {
        let router = router.clone();
        let route = Arc::clone(&route);
        let rendezvous = Arc::clone(&rendezvous);
        tokio::spawn(async move {
            router
                .run(route, async move {
                    tokio::time::timeout(Duration::from_secs(1), rendezvous.wait()).await
                })
                .await
        })
    });
    for operation in operations {
        operation.await.expect("operation task should finish")??;
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_caller_does_not_release_route_early() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (first_route, _, _) = route();
    let (second_route, _, _) = route();
    router.register(&first_route);
    router.register(&second_route);
    let first_started = Arc::new(Notify::new());
    let release_first = Arc::new(Notify::new());
    let second_started = Arc::new(Notify::new());

    let first_caller = {
        let router = router.clone();
        let first_started = Arc::clone(&first_started);
        let release_first = Arc::clone(&release_first);
        tokio::spawn(async move {
            router
                .run(first_route, async move {
                    first_started.notify_one();
                    release_first.notified().await;
                })
                .await
        })
    };
    first_started.notified().await;
    first_caller.abort();

    let second_caller = {
        let router = router.clone();
        let second_started = Arc::clone(&second_started);
        tokio::spawn(async move {
            router
                .run(second_route, async move {
                    second_started.notify_one();
                })
                .await
        })
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(50), second_started.notified())
            .await
            .is_err()
    );
    release_first.notify_one();
    tokio::time::timeout(Duration::from_secs(1), second_started.notified()).await?;
    second_caller.await??;
    Ok(())
}

#[tokio::test]
async fn cancelling_a_queued_caller_removes_its_operation() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (first_route, _, _) = route();
    let (second_route, _, _) = route();
    router.register(&first_route);
    router.register(&second_route);
    let first_started = Arc::new(Notify::new());
    let release_first = Arc::new(Notify::new());
    let second_started = Arc::new(Notify::new());

    let first = {
        let router = router.clone();
        let first_started = Arc::clone(&first_started);
        let release_first = Arc::clone(&release_first);
        tokio::spawn(async move {
            router
                .run(first_route, async move {
                    first_started.notify_one();
                    release_first.notified().await;
                })
                .await
        })
    };
    first_started.notified().await;

    let queued = {
        let router = router.clone();
        let second_started = Arc::clone(&second_started);
        tokio::spawn(async move {
            router
                .run(second_route, async move {
                    second_started.notify_one();
                })
                .await
        })
    };
    tokio::task::yield_now().await;
    queued.abort();
    release_first.notify_one();
    first.await??;

    assert!(
        tokio::time::timeout(Duration::from_millis(50), second_started.notified())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn waiting_routes_are_scheduled_fairly() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (first_route, _, _) = route();
    let (second_route, _, _) = route();
    router.register(&first_route);
    router.register(&second_route);
    let active_started = Arc::new(Notify::new());
    let release_active = Arc::new(Notify::new());
    let (order_tx, mut order_rx) = tokio::sync::mpsc::unbounded_channel();

    let active = {
        let router = router.clone();
        let active_started = Arc::clone(&active_started);
        let release_active = Arc::clone(&release_active);
        let first_route = Arc::clone(&first_route);
        tokio::spawn(async move {
            router
                .run(first_route, async move {
                    active_started.notify_one();
                    release_active.notified().await;
                })
                .await
        })
    };
    active_started.notified().await;

    let second = {
        let router = router.clone();
        let order_tx = order_tx.clone();
        tokio::spawn(async move {
            router
                .run(second_route, async move {
                    let _ = order_tx.send("second");
                })
                .await
        })
    };
    tokio::task::yield_now().await;
    let first_again = {
        let router = router.clone();
        tokio::spawn(async move {
            router
                .run(first_route, async move {
                    let _ = order_tx.send("first");
                })
                .await
        })
    };
    release_active.notify_one();

    active.await??;
    second.await??;
    first_again.await??;
    assert_eq!(order_rx.recv().await, Some("second"));
    assert_eq!(order_rx.recv().await, Some("first"));
    Ok(())
}

#[tokio::test]
async fn late_elicitation_after_session_close_is_rejected_not_misrouted() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (first_route, _, first_events) = route();
    let (second_route, _, second_events) = route();
    router.register(&first_route);
    router.register(&second_route);
    let operation_started = Arc::new(Notify::new());
    let send_elicitation = Arc::new(Notify::new());
    let sender = router.make_sender("test".to_string());

    let caller = {
        let router = router.clone();
        let operation_route = Arc::clone(&first_route);
        let operation_started = Arc::clone(&operation_started);
        let send_elicitation = Arc::clone(&send_elicitation);
        tokio::spawn(async move {
            router
                .run(operation_route, async move {
                    operation_started.notify_one();
                    send_elicitation.notified().await;
                    sender(
                        RequestId::String("late-request".into()),
                        Elicitation::OpenAiForm {
                            meta: None,
                            message: "late request".to_string(),
                            requested_schema: serde_json::json!({ "type": "object" }),
                        },
                    )
                    .await
                })
                .await
        })
    };
    operation_started.notified().await;
    caller.abort();
    router.unregister(&first_route);
    send_elicitation.notify_one();

    tokio::time::timeout(Duration::from_secs(1), router.run(second_route, async {})).await??;
    assert!(first_events.try_recv().is_err());
    assert!(second_events.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn elicitation_is_delivered_only_to_the_active_session() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (root_route, _, root_events) = route();
    let (child_route, child_manager, child_events) = route();
    router.register(&root_route);
    router.register(&child_route);
    let sender = router.make_sender("test".to_string());

    let operation = {
        let router = router.clone();
        tokio::spawn(async move {
            router
                .run(child_route, async move {
                    sender(
                        RequestId::String("server-request".into()),
                        Elicitation::OpenAiForm {
                            meta: None,
                            message: "child only".to_string(),
                            requested_schema: serde_json::json!({ "type": "object" }),
                        },
                    )
                    .await
                })
                .await
        })
    };
    let event = tokio::time::timeout(Duration::from_secs(1), child_events.recv()).await??;
    let EventMsg::ElicitationRequest(ElicitationRequestEvent {
        server_name, id, ..
    }) = event.msg
    else {
        anyhow::bail!("expected an elicitation request event");
    };
    assert!(root_events.try_recv().is_err());
    let codex_protocol::mcp::RequestId::String(id) = id else {
        anyhow::bail!("expected a string elicitation request id");
    };
    child_manager
        .resolve(
            server_name,
            RequestId::String(id.into()),
            ElicitationResponse {
                action: ElicitationAction::Accept,
                content: Some(serde_json::json!({})),
                meta: None,
            },
        )
        .await?;
    let response = operation.await???;
    assert_eq!(response.action, ElicitationAction::Accept);
    Ok(())
}

#[tokio::test]
async fn shared_connection_preserves_route_specific_elicitation_policy() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let never_manager = ElicitationRequestManager::new(
        test_elicitation_config("test", AskForApproval::Never, PermissionProfile::Disabled),
        /*reviewer*/ None,
        /*lifecycle*/ None,
        ElicitationRequestRouter::default(),
    );
    let (never_tx, never_events) = async_channel::unbounded();
    let never_route = Arc::new(McpSessionRoute::new(
        "never".to_string(),
        never_manager,
        Some(never_tx),
    ));
    let (prompt_route, prompt_manager, prompt_events) = route();
    router.register(&never_route);
    router.register(&prompt_route);

    let never_sender = router.make_sender("test".to_string());
    let declined = router
        .run(never_route, async move {
            never_sender(
                RequestId::String("never".into()),
                Elicitation::OpenAiForm {
                    meta: None,
                    message: "decline".to_string(),
                    requested_schema: serde_json::json!({ "type": "object" }),
                },
            )
            .await
        })
        .await??;
    assert_eq!(declined.action, ElicitationAction::Decline);
    assert!(never_events.try_recv().is_err());

    let prompt_sender = router.make_sender("test".to_string());
    let request = {
        let router = router.clone();
        tokio::spawn(async move {
            router
                .run(prompt_route, async move {
                    prompt_sender(
                        RequestId::String("prompt".into()),
                        Elicitation::OpenAiForm {
                            meta: None,
                            message: "prompt".to_string(),
                            requested_schema: serde_json::json!({ "type": "object" }),
                        },
                    )
                    .await
                })
                .await
        })
    };
    let event = tokio::time::timeout(Duration::from_secs(1), prompt_events.recv()).await??;
    assert!(never_events.try_recv().is_err());
    let EventMsg::ElicitationRequest(event) = event.msg else {
        anyhow::bail!("expected an elicitation request event");
    };
    let codex_protocol::mcp::RequestId::String(id) = event.id else {
        anyhow::bail!("expected a string elicitation request id");
    };
    prompt_manager
        .resolve(
            event.server_name,
            RequestId::String(id.into()),
            ElicitationResponse {
                action: ElicitationAction::Accept,
                content: Some(serde_json::json!({})),
                meta: None,
            },
        )
        .await?;
    assert_eq!(request.await???.action, ElicitationAction::Accept);
    Ok(())
}

#[tokio::test]
async fn unattributed_elicitation_is_rejected_instead_of_borrowing_a_session_policy() {
    let router = McpConnectionRequestRouter::default();
    let (root_route, _, root_events) = route();
    let (child_route, _, child_events) = route();
    router.register(&root_route);
    router.register(&child_route);
    let sender = router.make_sender("test".to_string());
    let error = sender(
        RequestId::String("unattributed".into()),
        Elicitation::OpenAiForm {
            meta: None,
            message: "no active request".to_string(),
            requested_schema: serde_json::json!({ "type": "object" }),
        },
    )
    .await
    .expect_err("an unattributed request must fail closed");

    assert!(error.to_string().contains("no live session"));
    assert!(root_events.try_recv().is_err());
    assert!(child_events.try_recv().is_err());
}

#[tokio::test]
async fn idle_elicitation_uses_the_only_live_session_route() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (route, manager, events) = route();
    router.register(&route);
    let sender = router.make_sender("test".to_string());
    let request = tokio::spawn(async move {
        sender(
            RequestId::String("idle".into()),
            Elicitation::OpenAiForm {
                meta: None,
                message: "idle request".to_string(),
                requested_schema: serde_json::json!({ "type": "object" }),
            },
        )
        .await
    });

    let event = tokio::time::timeout(Duration::from_secs(1), events.recv()).await??;
    let EventMsg::ElicitationRequest(event) = event.msg else {
        anyhow::bail!("expected an elicitation request event");
    };
    let codex_protocol::mcp::RequestId::String(id) = event.id else {
        anyhow::bail!("expected a string elicitation request id");
    };
    manager
        .resolve(
            event.server_name,
            RequestId::String(id.into()),
            ElicitationResponse {
                action: ElicitationAction::Accept,
                content: Some(serde_json::json!({})),
                meta: None,
            },
        )
        .await?;

    assert_eq!(request.await??.action, ElicitationAction::Accept);
    Ok(())
}

#[tokio::test]
async fn startup_recovery_event_uses_a_live_route() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (root_route, _, root_events) = route();
    let (child_route, _, child_events) = route();
    router.register(&root_route);
    router.register(&child_route);
    router.unregister(&root_route);

    router.emit_startup_ready("test-server".to_string()).await;

    assert!(root_events.try_recv().is_err());
    let event = tokio::time::timeout(Duration::from_secs(1), child_events.recv()).await??;
    assert_eq!(event.id, "test-submit");
    assert!(matches!(
        event.msg,
        EventMsg::McpStartupUpdate(codex_protocol::protocol::McpStartupUpdateEvent {
            server,
            status: codex_protocol::protocol::McpStartupStatus::Ready,
        }) if server == "test-server"
    ));
    Ok(())
}

#[tokio::test]
async fn startup_recovery_event_reaches_every_live_route() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (root_route, _, root_events) = route();
    let (child_route, _, child_events) = route();
    router.register(&root_route);
    router.register(&child_route);

    router.emit_startup_ready("test-server".to_string()).await;

    for events in [root_events, child_events] {
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv()).await??;
        assert!(matches!(
            event.msg,
            EventMsg::McpStartupUpdate(codex_protocol::protocol::McpStartupUpdateEvent {
                server,
                status: codex_protocol::protocol::McpStartupStatus::Ready,
            }) if server == "test-server"
        ));
    }
    Ok(())
}

#[tokio::test]
async fn closed_connection_rejects_elicitation_and_suppresses_recovery_events() {
    let router = McpConnectionRequestRouter::default();
    let (session_route, _, events) = route();
    router.register(&session_route);
    let sender = router.make_sender("test".to_string());
    router.close();

    let error = sender(
        RequestId::String("retired".into()),
        Elicitation::OpenAiForm {
            meta: None,
            message: "retired connection".to_string(),
            requested_schema: serde_json::json!({ "type": "object" }),
        },
    )
    .await
    .expect_err("a retired connection must reject server requests");
    assert!(error.to_string().contains("no live session"));
    router.emit_startup_ready("test-server".to_string()).await;
    assert!(events.try_recv().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_close_linearizes_with_event_dispatch() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (route, _, events) = route();
    router.register(&route);
    let (dispatch_started_tx, dispatch_started_rx) = std::sync::mpsc::sync_channel(1);
    let release_dispatch = Arc::new(std::sync::Barrier::new(2));
    route.set_before_event_dispatch(Some(Arc::new({
        let release_dispatch = Arc::clone(&release_dispatch);
        move || {
            dispatch_started_tx
                .send(())
                .expect("the test should still be waiting for dispatch");
            release_dispatch.wait();
        }
    })));

    let recovery = {
        let router = router.clone();
        tokio::spawn(async move {
            router.emit_startup_ready("test-server".to_string()).await;
        })
    };
    dispatch_started_rx.recv_timeout(Duration::from_secs(1))?;
    let (close_finished_tx, close_finished_rx) = std::sync::mpsc::sync_channel(1);
    let close_route = Arc::clone(&route);
    let closer = std::thread::spawn(move || {
        close_route.close();
        close_finished_tx
            .send(())
            .expect("the test should still be waiting for close");
    });
    assert!(
        close_finished_rx
            .recv_timeout(Duration::from_millis(50))
            .is_err(),
        "close must wait for the event dispatch that already linearized"
    );

    release_dispatch.wait();
    tokio::time::timeout(Duration::from_secs(1), recovery).await??;
    close_finished_rx.recv_timeout(Duration::from_secs(1))?;
    closer.join().expect("route close thread should finish");
    route.set_before_event_dispatch(/*hook*/ None);

    let event = events.recv().await?;
    assert!(matches!(event.msg, EventMsg::McpStartupUpdate(_)));
    router.emit_startup_ready("after-close".to_string()).await;
    assert!(events.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn a_closed_route_rejects_elicitation_and_recovery_events() -> anyhow::Result<()> {
    let router = McpConnectionRequestRouter::default();
    let (route, _, events) = route();
    router.register(&route);
    let sender = router.make_sender("test".to_string());
    route.close();

    let error = sender(
        RequestId::String("after-close".into()),
        Elicitation::OpenAiForm {
            meta: None,
            message: "closed route".to_string(),
            requested_schema: serde_json::json!({ "type": "object" }),
        },
    )
    .await
    .expect_err("a closed route must reject elicitation");
    assert!(error.to_string().contains("no live session"));

    router.emit_startup_ready("test-server".to_string()).await;
    assert!(events.try_recv().is_err());
    Ok(())
}

#[test]
fn a_retired_connection_rejects_an_event_after_route_selection() {
    let router = McpConnectionRequestRouter::default();
    let (route, _, events) = route();
    router.register(&route);
    let selected_route = router
        .current_route()
        .expect("the route should be selectable before retirement");
    router.close();

    let error = router
        .dispatch_event(
            &selected_route,
            Event {
                id: "retired".to_string(),
                msg: EventMsg::McpStartupUpdate(codex_protocol::protocol::McpStartupUpdateEvent {
                    server: "retired".to_string(),
                    status: codex_protocol::protocol::McpStartupStatus::Ready,
                }),
            },
        )
        .expect_err("a retired generation must reject its selected route");

    assert!(error.to_string().contains("connection closed"));
    assert!(events.try_recv().is_err());
}
