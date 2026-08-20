//! Terminal-title focused tests for live chatwidget status-surface behavior.

use super::*;
use crate::bottom_pane::goal_status_indicator_line;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn terminal_title_animation_does_not_request_full_frame_redraws() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    chat.has_codex_backend_auth = true;
    chat.config.tui_status_line = Some(vec!["workspace-headline".to_string()]);
    chat.refresh_status_surfaces();
    let request_id = match rx.try_recv() {
        Ok(AppEvent::RefreshStatusLineWorkspaceHeadline { request_id }) => request_id,
        event => panic!("expected workspace headline refresh, got {event:?}"),
    };
    assert!(chat.set_status_line_workspace_headline(
        request_id,
        Ok(
            crate::workspace_messages::WorkspaceHeadlineFetchResult::Available(Some(
                "Cached headline".to_string(),
            ))
        ),
    ));
    chat.status_line_workspace_headline_last_requested_at =
        Some(Instant::now() - crate::workspace_messages::WORKSPACE_HEADLINE_REFRESH_INTERVAL);
    chat.bottom_pane.set_task_running(/*running*/ true);
    let (frame_requester, mut draw_rx) = FrameRequester::test_channel();
    chat.frame_requester = frame_requester;
    chat.last_terminal_title = None;
    // Keep the visible spinner frame fixed while checking unchanged-title deadline renewal.
    chat.terminal_title_animation_origin = Instant::now() + Duration::from_secs(/*secs*/ 60);

    chat.refresh_terminal_title_frame();

    assert_eq!(
        (
            chat.last_terminal_title_requires_action,
            chat.terminal_title_next_refresh.is_some(),
        ),
        (false, true)
    );
    assert!(
        chat.last_terminal_title
            .as_ref()
            .is_some_and(|title| title.contains("project"))
    );
    assert!(matches!(
        draw_rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));

    let title = chat.last_terminal_title.clone();
    let before_refresh = Instant::now();
    chat.terminal_title_next_refresh = Some(before_refresh - Duration::from_secs(/*secs*/ 1));
    chat.refresh_terminal_title_frame();
    assert_eq!(chat.last_terminal_title, title);
    assert!(
        chat.terminal_title_next_refresh
            .is_some_and(|next| next > before_refresh)
    );

    for (animations, title_items) in [(false, None), (true, Some(Vec::new()))] {
        chat.config.animations = true;
        chat.config.tui_terminal_title = None;
        chat.refresh_terminal_title_frame();
        assert!(chat.terminal_title_next_refresh.is_some());
        chat.config.animations = animations;
        chat.config.tui_terminal_title = title_items;
        chat.refresh_terminal_title_frame();
        assert!(chat.terminal_title_next_refresh.is_none());
    }

    chat.refresh_status_surfaces();

    assert!(matches!(
        rx.try_recv(),
        Ok(AppEvent::RefreshStatusLineWorkspaceHeadline { .. })
    ));
}

#[tokio::test]
async fn goal_clock_refresh_redraws_only_when_elapsed_label_changes() {
    let (frame_requester, mut draw_rx) = FrameRequester::test_channel();
    let (mut chat, _rx, _op_rx) = make_chatwidget_manual_with_auth(
        /*model_override*/ None,
        /*has_chatgpt_account*/ false,
        /*has_codex_backend_auth*/ false,
        frame_requester,
    )
    .await;
    chat.set_feature_enabled(Feature::Goals, /*enabled*/ true);
    chat.bottom_pane.set_task_running(/*running*/ true);
    chat.bottom_pane.hide_status_indicator();
    let observed_at = Instant::now() - Duration::from_secs(/*secs*/ 90);
    chat.turn_lifecycle.goal_status_active_turn_started_at = Some(observed_at);
    let goal = AppThreadGoal {
        thread_id: "thread-1".to_string(),
        objective: "Keep improving the benchmark".to_string(),
        status: AppThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 60,
        created_at: 0,
        updated_at: 0,
    };
    chat.on_thread_goal_updated(goal.clone(), /*turn_id*/ None);
    let initial_indicator = chat.current_goal_status_indicator.clone();
    while draw_rx.try_recv().is_ok() {}

    chat.current_goal_status = Some(GoalStatusState::new(goal, observed_at));
    chat.refresh_goal_status_indicator_for_time_tick();
    chat.refresh_terminal_title();
    assert!(draw_rx.try_recv().is_ok());
    assert!(chat.terminal_title_next_refresh.is_some());

    chat.refresh_goal_status_indicator_for_time_tick();
    chat.refresh_terminal_title();
    assert_eq!(
        draw_rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    );
    let labels = [
        initial_indicator.as_ref(),
        chat.current_goal_status_indicator.as_ref(),
    ]
    .map(|indicator| {
        let line = goal_status_indicator_line(indicator).expect("active goal indicator");
        line.spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>()
    });
    insta::assert_snapshot!(labels.join("\n"), @r"
    Pursuing goal (1m)
    Pursuing goal (2m)
    ");
}

#[tokio::test]
async fn terminal_title_shows_action_required_while_exec_approval_is_pending() {
    let (mut chat, _rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    let (frame_requester, mut draw_rx) = FrameRequester::test_channel();
    chat.frame_requester = frame_requester;
    chat.bottom_pane.set_task_running(/*running*/ true);
    let before_refresh = Instant::now();
    chat.refresh_terminal_title();
    let spinner_interval = std::time::Duration::from_millis(/*millis*/ 100);
    assert!(
        (before_refresh + spinner_interval..=Instant::now() + spinner_interval)
            .contains(&chat.terminal_title_next_refresh.expect("spinner deadline"))
    );
    assert_eq!(
        draw_rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    );

    let request = ExecApprovalRequestEvent {
        kind: Default::default(),
        call_id: "call-action-required".into(),
        approval_id: Some("call-action-required".into()),
        turn_id: "turn-action-required".into(),
        environment_id: None,
        command: vec!["bash".into(), "-lc".into(), "echo hello".into()],
        cwd: AbsolutePathBuf::current_dir().expect("current dir"),
        reason: Some("need confirmation".into()),
        network_approval_context: None,
        proposed_execpolicy_amendment: None,
        proposed_network_policy_amendments: None,
        additional_permissions: None,
        available_decisions: None,
    };
    handle_exec_approval_request(&mut chat, "sub-action-required", request);

    let before_refresh = Instant::now();
    chat.terminal_title_animation_origin = before_refresh;
    chat.pre_draw_tick();
    let blink_interval = std::time::Duration::from_secs(/*secs*/ 1);
    assert!(
        (before_refresh + blink_interval..=Instant::now() + blink_interval).contains(
            &chat
                .terminal_title_next_refresh
                .expect("action-required deadline")
        )
    );

    assert_eq!(
        chat.last_terminal_title,
        Some("[ ! ] Action Required | project".to_string())
    );
    assert_eq!(
        (
            chat.last_terminal_title_requires_action,
            chat.terminal_title_next_refresh.is_some(),
        ),
        (true, true)
    );

    chat.handle_key_event(KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE));
    chat.pre_draw_tick();

    let title = chat
        .last_terminal_title
        .as_deref()
        .expect("terminal title should be restored after approval");
    assert!(title.contains("project"));
    assert!(!title.contains("Action Required"));
    assert_eq!(
        (
            chat.last_terminal_title_requires_action,
            chat.terminal_title_next_refresh.is_some(),
        ),
        (false, true)
    );

    for (animations, title_items) in [(false, None), (true, Some(Vec::new()))] {
        chat.config.animations = true;
        chat.config.tui_terminal_title = None;
        chat.refresh_terminal_title();
        assert!(chat.terminal_title_next_refresh.is_some());

        chat.config.animations = animations;
        chat.config.tui_terminal_title = title_items;
        chat.refresh_terminal_title();
        assert!(chat.terminal_title_next_refresh.is_none());
    }
}

#[tokio::test]
async fn terminal_title_action_required_respects_spinner_setting() {
    let (mut chat, _rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    chat.config.tui_terminal_title = Some(vec!["project".to_string()]);
    chat.bottom_pane.set_task_running(/*running*/ true);
    chat.refresh_terminal_title();

    let request = ExecApprovalRequestEvent {
        kind: Default::default(),
        call_id: "call-no-spinner".into(),
        approval_id: Some("call-no-spinner".into()),
        turn_id: "turn-no-spinner".into(),
        environment_id: None,
        command: vec!["bash".into(), "-lc".into(), "echo hello".into()],
        cwd: AbsolutePathBuf::current_dir().expect("current dir"),
        reason: Some("need confirmation".into()),
        network_approval_context: None,
        proposed_execpolicy_amendment: None,
        proposed_network_policy_amendments: None,
        additional_permissions: None,
        available_decisions: None,
    };
    handle_exec_approval_request(&mut chat, "sub-no-spinner", request);

    chat.pre_draw_tick();

    assert_eq!(chat.last_terminal_title, Some("project".to_string()));
    assert_eq!(
        (
            chat.last_terminal_title_requires_action,
            chat.terminal_title_next_refresh.is_some(),
        ),
        (false, false)
    );
}

#[tokio::test]
async fn terminal_title_action_required_blinks_when_animations_are_enabled() {
    let (mut chat, _rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    chat.bottom_pane.set_task_running(/*running*/ true);
    chat.refresh_terminal_title();

    let request = ExecApprovalRequestEvent {
        kind: Default::default(),
        call_id: "call-blink".into(),
        approval_id: Some("call-blink".into()),
        turn_id: "turn-blink".into(),
        environment_id: None,
        command: vec!["bash".into(), "-lc".into(), "echo hello".into()],
        cwd: AbsolutePathBuf::current_dir().expect("current dir"),
        reason: Some("need confirmation".into()),
        network_approval_context: None,
        proposed_execpolicy_amendment: None,
        proposed_network_policy_amendments: None,
        additional_permissions: None,
        available_decisions: None,
    };
    handle_exec_approval_request(&mut chat, "sub-blink", request);

    chat.terminal_title_animation_origin =
        Instant::now() - std::time::Duration::from_millis(/*millis*/ 1500);
    chat.pre_draw_tick();

    assert_eq!(
        chat.last_terminal_title,
        Some("[ . ] Action Required | project".to_string())
    );
    assert_eq!(
        (
            chat.last_terminal_title_requires_action,
            chat.terminal_title_next_refresh.is_some(),
        ),
        (true, true)
    );
}

#[tokio::test]
async fn terminal_title_activity_indicators_do_not_animate_when_animations_are_disabled() {
    let (mut chat, _rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    chat.config.animations = false;
    chat.bottom_pane.set_task_running(/*running*/ true);
    chat.terminal_title_animation_origin = Instant::now() - std::time::Duration::from_millis(1500);
    chat.refresh_terminal_title();

    assert_eq!(chat.last_terminal_title, Some("project".to_string()));
    assert_eq!(
        (
            chat.last_terminal_title_requires_action,
            chat.terminal_title_next_refresh.is_some(),
        ),
        (false, false)
    );

    let request = ExecApprovalRequestEvent {
        kind: Default::default(),
        call_id: "call-no-animations".into(),
        approval_id: Some("call-no-animations".into()),
        turn_id: "turn-no-animations".into(),
        environment_id: None,
        command: vec!["bash".into(), "-lc".into(), "echo hello".into()],
        cwd: AbsolutePathBuf::current_dir().expect("current dir"),
        reason: Some("need confirmation".into()),
        network_approval_context: None,
        proposed_execpolicy_amendment: None,
        proposed_network_policy_amendments: None,
        additional_permissions: None,
        available_decisions: None,
    };
    handle_exec_approval_request(&mut chat, "sub-no-animations", request);

    chat.pre_draw_tick();

    assert_eq!(
        chat.last_terminal_title,
        Some("[ ! ] Action Required | project".to_string())
    );
    assert_eq!(
        (
            chat.last_terminal_title_requires_action,
            chat.terminal_title_next_refresh.is_some(),
        ),
        (true, false)
    );
}
