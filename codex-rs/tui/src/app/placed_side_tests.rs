use super::*;
use crate::app::test_support::make_test_app;
use crate::app_server_session::ResumeModelSettings;
use codex_terminal_detection::Multiplexer;
use codex_terminal_detection::TerminalName;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn placed_side_launch_config_projects_live_thread_settings() {
    let mut app = make_test_app().await;
    app.config.model = Some("disk-model".to_string());
    app.config.model_reasoning_effort = Some(ReasoningEffortConfig::Low);
    app.config.service_tier = Some("disk-tier".to_string());
    app.config
        .permissions
        .approval_policy
        .set(codex_protocol::protocol::AskForApproval::Never)
        .expect("app approval policy");
    app.chat_widget.set_model("live-model");
    app.chat_widget
        .set_reasoning_effort(Some(ReasoningEffortConfig::High));
    app.chat_widget
        .set_service_tier(Some("priority".to_string()));
    app.chat_widget
        .set_approval_policy(AskForApproval::OnRequest);

    let launch_config = app.placed_side_launch_config();

    assert_eq!(
        (
            launch_config.model,
            launch_config.model_reasoning_effort,
            launch_config.service_tier,
            launch_config.permissions.approval_policy.value(),
        ),
        (
            Some("live-model".to_string()),
            Some(ReasoningEffortConfig::High),
            Some("priority".to_string()),
            codex_protocol::protocol::AskForApproval::OnRequest,
        )
    );
}

#[test]
fn placed_side_failure_messages_snapshot() {
    insta::assert_snapshot!(
        "placed_side_failure_messages",
        [
            SIDE_NO_STARTED_CONVERSATION_MESSAGE.to_string(),
            REMOTE_SIDE_PANE_UNAVAILABLE_MESSAGE.to_string(),
            SIDE_PLACEMENT_REQUIRES_PANE_HOST_MESSAGE.to_string(),
            placed_side_spawn_failure_message("tmux exited with status 1"),
        ]
        .join("\n")
    );
}

/// A persisted parent served by a real app server in the test's isolated CODEX_HOME.
struct SidePaneFixture {
    app: App,
    app_server: AppServerSession,
    parent: ThreadId,
}

impl SidePaneFixture {
    async fn new() -> Result<Self> {
        let mut app = Box::pin(make_test_app()).await;
        let config = app.chat_widget.config_ref().clone();
        let parent = ThreadId::from_string(
            &app_test_support::create_fake_rollout(
                &config.codex_home,
                "2025-01-05T12-00-00",
                "2025-01-05T12:00:00Z",
                "Preserve this parent conversation",
                Some(config.model_provider_id.as_str()),
                /*git_info*/ None,
            )
            .expect("create parent rollout"),
        )?;
        let mut app_server = Box::pin(crate::start_embedded_app_server_for_picker(&config)).await?;
        let started = app_server
            .resume_thread(config, parent, ResumeModelSettings::RestoreFromThread)
            .await?;
        app.enqueue_primary_thread_session(started.session, started.turns)
            .await?;
        Ok(Self {
            app,
            app_server,
            parent,
        })
    }

    async fn assert_side_and_return(&mut self, tui: &mut tui::Tui) -> Result<()> {
        let child = self.app.active_thread_id.expect("side should be active");
        assert_ne!(child, self.parent);
        assert_eq!(self.app.side_threads.len(), 1);
        assert_eq!(self.app.active_side_parent_thread_id(), Some(self.parent));
        let side = self
            .app_server
            .thread_read(child, /*include_turns*/ false)
            .await?;
        assert!(side.ephemeral);
        assert_eq!(side.path, None);
        assert_eq!(self.app.primary_thread_id, Some(self.parent));
        assert!(Box::pin(self.app.maybe_return_from_side(tui, &mut self.app_server)).await);
        assert_eq!(self.app.active_thread_id, Some(self.parent));
        assert!(self.app.side_threads.is_empty());
        Ok(())
    }
}

fn terminal(name: TerminalName, multiplexer: Option<Multiplexer>) -> TerminalInfo {
    TerminalInfo {
        name,
        term_program: None,
        version: None,
        term: None,
        multiplexer,
    }
}

#[tokio::test]
async fn unavailable_side_pane_falls_back_and_preserves_parent() -> Result<()> {
    let mut fixture = Box::pin(SidePaneFixture::new()).await?;
    let before = fixture
        .app_server
        .thread_read(fixture.parent, /*include_turns*/ true)
        .await?;
    let mut tui = crate::tui::test_support::make_test_tui()?;

    Box::pin(fixture.app.handle_start_placed_side(
        &mut tui,
        &mut fixture.app_server,
        fixture.parent,
        ForkPanePlacement::Right,
        &terminal(TerminalName::Unknown, /*multiplexer*/ None),
    ))
    .await?;

    Box::pin(fixture.assert_side_and_return(&mut tui)).await?;
    let after = fixture
        .app_server
        .thread_read(fixture.parent, /*include_turns*/ true)
        .await?;
    assert_eq!(after.turns, before.turns);
    fixture.app_server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn remote_side_pane_uses_inline_side_without_local_rollout() -> Result<()> {
    let mut fixture = Box::pin(SidePaneFixture::new()).await?;
    fixture.app.app_server_target = crate::AppServerTarget::Remote {
        endpoint: crate::RemoteAppServerEndpoint::WebSocket {
            websocket_url: "ws://127.0.0.1:4500".to_string(),
            auth_token: None,
        },
    };
    // Remote session metadata need not name a rollout accessible to the TUI.
    let mut session = fixture
        .app
        .primary_session_configured
        .clone()
        .expect("parent session");
    session.rollout_path = None;
    fixture.app.chat_widget.handle_thread_session(session);
    let mut tui = crate::tui::test_support::make_test_tui()?;

    Box::pin(fixture.app.handle_start_placed_side(
        &mut tui,
        &mut fixture.app_server,
        fixture.parent,
        ForkPanePlacement::Right,
        &terminal(TerminalName::Ghostty, /*multiplexer*/ None),
    ))
    .await?;

    Box::pin(fixture.assert_side_and_return(&mut tui)).await?;
    fixture.app_server.shutdown().await?;
    Ok(())
}

#[cfg(not(target_os = "macos"))]
#[tokio::test]
async fn ghostty_on_non_macos_uses_inline_side() -> Result<()> {
    let mut fixture = Box::pin(SidePaneFixture::new()).await?;
    let mut tui = crate::tui::test_support::make_test_tui()?;
    Box::pin(fixture.app.handle_start_placed_side(
        &mut tui,
        &mut fixture.app_server,
        fixture.parent,
        ForkPanePlacement::Right,
        &terminal(TerminalName::Ghostty, /*multiplexer*/ None),
    ))
    .await?;
    Box::pin(fixture.assert_side_and_return(&mut tui)).await?;
    fixture.app_server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn rejected_side_placement_after_handoff_falls_back() -> Result<()> {
    let mut fixture = Box::pin(SidePaneFixture::new()).await?;
    let mut tui = crate::tui::test_support::make_test_tui()?;

    // Zellij rejects --up before invoking a process, independently of the host environment.
    Box::pin(fixture.app.handle_start_placed_side(
        &mut tui,
        &mut fixture.app_server,
        fixture.parent,
        ForkPanePlacement::Up,
        &terminal(
            TerminalName::Ghostty,
            Some(Multiplexer::Zellij { version: None }),
        ),
    ))
    .await?;

    Box::pin(fixture.assert_side_and_return(&mut tui)).await?;
    fixture.app_server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn failed_side_handoff_preparation_falls_back() -> Result<()> {
    let mut fixture = Box::pin(SidePaneFixture::new()).await?;
    let config = App::standalone_side_config(fixture.app.chat_widget.config_ref());
    for _ in 0..2 {
        fixture
            .app_server
            .prepare_fork_handoff(config.clone(), fixture.parent)
            .await?;
    }
    let error = fixture
        .app_server
        .prepare_fork_handoff(config, fixture.parent)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("two fork handoffs are already pending")
    );
    let mut tui = crate::tui::test_support::make_test_tui()?;

    Box::pin(fixture.app.handle_start_placed_side(
        &mut tui,
        &mut fixture.app_server,
        fixture.parent,
        ForkPanePlacement::Right,
        &terminal(
            TerminalName::Ghostty,
            Some(Multiplexer::Tmux { version: None }),
        ),
    ))
    .await?;

    Box::pin(fixture.assert_side_and_return(&mut tui)).await?;
    fixture.app_server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn failed_side_spawn_falls_back_but_success_does_not_fork_inline() -> Result<()> {
    let mut fixture = Box::pin(SidePaneFixture::new()).await?;
    let mut tui = crate::tui::test_support::make_test_tui()?;
    Box::pin(fixture.app.handle_side_pane_result(
        &mut tui,
        &mut fixture.app_server,
        fixture.parent,
        ForkPaneSpawnResult::Spawned,
    ))
    .await?;
    assert_eq!(fixture.app.active_thread_id, Some(fixture.parent));
    assert!(fixture.app.side_threads.is_empty());

    for error in [
        "osascript exited with status 1",
        "osascript timed out after 10s",
    ] {
        Box::pin(fixture.app.handle_side_pane_result(
            &mut tui,
            &mut fixture.app_server,
            fixture.parent,
            ForkPaneSpawnResult::Failed(error.to_string()),
        ))
        .await?;
        Box::pin(fixture.assert_side_and_return(&mut tui)).await?;
    }
    fixture.app_server.shutdown().await?;
    Ok(())
}
