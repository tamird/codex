use super::super::agent_picker::AGENT_PICKER_MAX_SCAN_DURATION;
use super::session_request_support::BlockedThreadListPage;
use super::session_request_support::start_recording_app_server;
use super::*;
use app_test_support::create_fake_parented_rollout_with_source;
use app_test_support::create_fake_rollout;
use codex_app_server_protocol::ThreadStatus;
use codex_protocol::AgentPath;
use codex_state::SqliteConfig;
use pretty_assertions::assert_eq;
use tokio::sync::oneshot;

async fn next_agent_picker_completion(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<AppEvent>,
) -> Result<AppEvent> {
    let deadline = tokio::time::Instant::now() + AGENT_PICKER_MAX_SCAN_DURATION;
    while let Some(event) = tokio::time::timeout_at(deadline, events.recv()).await? {
        if matches!(event, AppEvent::AgentPickerThreadsLoaded { .. }) {
            return Ok(event);
        }
    }
    color_eyre::eyre::bail!("background refresh completion channel closed")
}

fn create_picker_rollouts(
    codex_home: &Path,
    model_provider: &str,
    index: usize,
) -> Result<(ThreadId, ThreadId)> {
    let root = ThreadId::from_string(
        &create_fake_rollout(
            codex_home,
            &format!("2026-01-01T00-00-0{index}"),
            &format!("2026-01-01T00:00:0{index}Z"),
            "Saved user message",
            Some(model_provider),
            /*git_info*/ None,
        )
        .map_err(|err| color_eyre::eyre::eyre!(err))?,
    )?;
    let nickname = if index == 0 { "worker" } else { "unrelated" };
    let child = ThreadId::from_string(
        &create_fake_parented_rollout_with_source(
            codex_home,
            &format!("2026-01-01T00-00-0{}", index + 2),
            &format!("2026-01-01T00:00:0{}Z", index + 2),
            "Saved child message",
            Some(model_provider),
            /*git_info*/ None,
            RolloutSessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: root,
                depth: 1,
                agent_path: Some(AgentPath::try_from(format!("/root/{nickname}")).expect("valid")),
                agent_nickname: Some(nickname.to_string()),
                agent_role: Some("worker".to_string()),
            }),
            root.into(),
            root,
        )
        .map_err(|err| color_eyre::eyre::eyre!(err))?,
    )?;
    Ok((root, child))
}

#[tokio::test(flavor = "current_thread")]
async fn blocked_agent_picker_retains_selection_and_coalesces_reopens() -> Result<()> {
    let (mut app, mut app_event_rx, _op_rx) = Box::pin(make_test_app_with_channels()).await;
    let codex_home = tempdir()?;
    app.config.codex_home = codex_home.path().to_path_buf().abs();
    app.config.sqlite = SqliteConfig::new_for_testing(codex_home.path().abs());
    let model_provider = app.config.model_provider_id.as_str();
    let (root_thread_id, child_thread_id) =
        create_picker_rollouts(codex_home.path(), model_provider, /*index*/ 0)?;
    let (_other_root_thread_id, other_child_thread_id) =
        create_picker_rollouts(codex_home.path(), model_provider, /*index*/ 1)?;
    let empty_root_thread_id = ThreadId::from_string(
        &create_fake_rollout(
            codex_home.path(),
            "2026-01-01T00-00-09",
            "2026-01-01T00:00:09Z",
            "Saved empty root message",
            Some(model_provider),
            /*git_info*/ None,
        )
        .map_err(|err| color_eyre::eyre::eyre!(err))?,
    )?;
    let grandchild_thread_id = ThreadId::from_string(
        &create_fake_parented_rollout_with_source(
            codex_home.path(),
            "2025-12-31T23-59-59",
            "2025-12-31T23:59:59Z",
            "Saved grandchild message",
            Some(model_provider),
            /*git_info*/ None,
            RolloutSessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 2,
                agent_path: Some(
                    AgentPath::try_from("/root/worker/grandchild".to_string())
                        .expect("valid agent path"),
                ),
                agent_nickname: Some("grandchild".to_string()),
                agent_role: Some("worker".to_string()),
            }),
            root_thread_id.into(),
            child_thread_id,
        )
        .map_err(|err| color_eyre::eyre::eyre!(err))?,
    )?;
    let state_db = codex_state::StateRuntime::init(
        SqliteConfig::new_for_testing(codex_home.path().abs()),
        model_provider.to_string(),
    )
    .await
    .map_err(|err| color_eyre::eyre::eyre!(err))?;
    state_db
        .upsert_thread_spawn_edge(
            root_thread_id,
            child_thread_id,
            codex_state::DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .map_err(|err| color_eyre::eyre::eyre!(err))?;
    state_db
        .upsert_thread_spawn_edge(
            child_thread_id,
            grandchild_thread_id,
            codex_state::DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .map_err(|err| color_eyre::eyre::eyre!(err))?;

    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (mut app_server, requests, proxy) = Box::pin(start_recording_app_server(
        &app.config,
        Some((
            root_thread_id,
            started_tx,
            release_rx,
            BlockedThreadListPage::First,
        )),
    ))
    .await?;
    let thread_list_count = || {
        requests
            .lock()
            .expect("requests")
            .iter()
            .filter(|method| *method == "thread/list")
            .count()
    };

    let model_settings = app.resume_model_settings();
    let root = app_server
        .resume_thread(app.config.clone(), root_thread_id, model_settings)
        .await?;
    app.enqueue_primary_thread_session(root.session, root.turns)
        .await?;
    let empty_root = app_server
        .resume_thread(app.config.clone(), empty_root_thread_id, model_settings)
        .await?;
    let mut tui = crate::tui::test_support::make_test_tui()?;
    tokio::time::timeout(AGENT_PICKER_MAX_SCAN_DURATION, async {
        Box::pin(app.handle_event(&mut tui, &mut app_server, AppEvent::OpenAgentPicker)).await?;
        started_rx.await?;
        app.chat_widget
            .handle_key_event(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        Box::pin(app.handle_event(&mut tui, &mut app_server, AppEvent::OpenAgentPicker)).await
    })
    .await??;
    assert_eq!(thread_list_count(), 1);
    insta::assert_snapshot!(
        &render_bottom_popup(&app.chat_widget, /*width*/ 80)
            .replace(&root_thread_id.to_string(), "[root]"),
        @r###"
          Subagents
          Select an agent to watch. ⌥ + ← previous, ⌥ + → next.

        › 1. • Main [default] (current)  [root]

          Press enter to confirm or esc to go back
        "###
    );
    app.chat_widget.handle_server_request(
        exec_approval_request(root_thread_id, "turn", "item", /*approval_id*/ None),
        /*replay_kind*/ None,
    );
    tokio::time::pause();
    tokio::time::advance(AGENT_PICKER_MAX_SCAN_DURATION).await;
    let completion = next_agent_picker_completion(&mut app_event_rx).await?;
    assert_matches!(
        &completion,
        AppEvent::AgentPickerThreadsLoaded {
            refresh: AgentPickerRefresh::TimedOut { .. },
            ..
        }
    );
    tokio::time::resume();
    Box::pin(app.handle_event(&mut tui, &mut app_server, completion)).await?;
    assert!(render_bottom_popup(&app.chat_widget, /*width*/ 80).contains("echo hello"));
    assert!(
        app.chat_widget
            .selected_index_for_present_view(super::super::agent_picker::AGENT_PICKER_VIEW_ID)
            .is_some()
    );
    app.chat_widget
        .handle_key_event(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
    Box::pin(app.handle_event(&mut tui, &mut app_server, AppEvent::OpenAgentPicker)).await?;
    assert!(!render_bottom_popup(&app.chat_widget, /*width*/ 80).contains("/root/worker"));
    app.chat_widget
        .handle_key_event(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
    assert_eq!(
        app.chat_widget
            .selected_index_for_present_view(super::super::agent_picker::AGENT_PICKER_VIEW_ID),
        Some(0)
    );
    assert_eq!(thread_list_count(), 1);
    release_tx.send(()).expect("release blocked thread list");
    let completion = next_agent_picker_completion(&mut app_event_rx).await?;
    let concurrently_spawned_id = ThreadId::new();
    app.thread_event_channels.insert(
        concurrently_spawned_id,
        ThreadEventChannel::new(/*capacity*/ 1),
    );
    app.agent_navigation.upsert(
        concurrently_spawned_id,
        /*agent_nickname*/ None,
        /*agent_role*/ None,
        /*is_closed*/ false,
    );
    app.agent_navigation.set_agent_path(
        concurrently_spawned_id,
        Some("/root/concurrently-spawned".to_string()),
    );
    app.agent_navigation
        .set_running(concurrently_spawned_id, /*is_running*/ true);
    for (thread_id, agent_path) in [
        (child_thread_id, "/root/worker"),
        (grandchild_thread_id, "/root/worker/grandchild"),
    ] {
        app.thread_event_channels
            .insert(thread_id, ThreadEventChannel::new(/*capacity*/ 1));
        app.agent_navigation.upsert(
            thread_id, /*agent_nickname*/ None, /*agent_role*/ None,
            /*is_closed*/ false,
        );
        app.agent_navigation
            .set_agent_path(thread_id, Some(agent_path.to_string()));
        app.agent_navigation
            .set_running(thread_id, /*is_running*/ true);
    }
    Box::pin(app.handle_event(&mut tui, &mut app_server, completion)).await?;
    while app_event_rx.try_recv().is_ok() {}
    insta::assert_snapshot!(
        &render_bottom_popup(&app.chat_widget, /*width*/ 80)
            .replace(&root_thread_id.to_string(), "[root]")
            .replace(&child_thread_id.to_string(), "[child]")
            .replace(&grandchild_thread_id.to_string(), "[grandchild]")
            .replace(&concurrently_spawned_id.to_string(), "[concurrent]"),
        @r###"
          Subagents
          Select an agent to watch. ⌥ + ← previous, ⌥ + → next.

        › 1. • Main [default] (current)    [root]
          2. • /root/concurrently-spawned  [concurrent]
          3. • /root/worker                [child]
          4. • /root/worker/grandchild     [grandchild]

          Press enter to confirm or esc to go back
        "###
    );
    assert_eq!(
        app.chat_widget
            .selected_index_for_present_view(super::super::agent_picker::AGENT_PICKER_VIEW_ID),
        Some(0)
    );
    assert!(app.agent_navigation.get(&other_child_thread_id).is_none());

    for code in [KeyCode::Down, KeyCode::Down] {
        app.chat_widget
            .handle_key_event(KeyEvent::new(code, KeyModifiers::NONE));
    }
    assert_eq!(
        app.chat_widget
            .selected_index_for_present_view(super::super::agent_picker::AGENT_PICKER_VIEW_ID),
        Some(2)
    );
    app.thread_event_channels.remove(&concurrently_spawned_id);
    app.agent_navigation.mark_closed(concurrently_spawned_id);
    Box::pin(app.handle_event(&mut tui, &mut app_server, AppEvent::OpenAgentPicker)).await?;
    let completion = next_agent_picker_completion(&mut app_event_rx).await?;
    Box::pin(app.handle_event(&mut tui, &mut app_server, completion)).await?;
    assert_eq!(
        app.chat_widget
            .selected_index_for_present_view(super::super::agent_picker::AGENT_PICKER_VIEW_ID),
        Some(1)
    );
    app.chat_widget
        .handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    assert_matches!(
        app_event_rx.try_recv(),
        Ok(AppEvent::SelectAgentThread(thread_id)) if thread_id == child_thread_id
    );

    Box::pin(app.handle_event(&mut tui, &mut app_server, AppEvent::OpenAgentPicker)).await?;
    app.chat_widget
        .handle_key_event(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
    let completion = next_agent_picker_completion(&mut app_event_rx).await?;
    app.reset_thread_event_state();
    app.enqueue_primary_thread_session(empty_root.session, empty_root.turns)
        .await?;
    Box::pin(app.handle_event(&mut tui, &mut app_server, completion)).await?;
    assert_eq!(app.primary_thread_id, Some(empty_root_thread_id));

    let ghost_id = ThreadId::new();
    app.agent_navigation.upsert(
        ghost_id, /*agent_nickname*/ None, /*agent_role*/ None, /*is_closed*/ false,
    );
    let _ = app.config.features.disable(Feature::Collab);
    Box::pin(app.handle_event(&mut tui, &mut app_server, AppEvent::OpenAgentPicker)).await?;
    let completion = next_agent_picker_completion(&mut app_event_rx).await?;
    Box::pin(app.handle_event(&mut tui, &mut app_server, completion)).await?;
    assert!(app.agent_navigation.get(&ghost_id).is_some());

    let mut embedded = Box::pin(crate::start_embedded_app_server_for_picker(&app.config)).await?;
    Box::pin(app.handle_event(&mut tui, &mut embedded, AppEvent::OpenAgentPicker)).await?;
    let completion = next_agent_picker_completion(&mut app_event_rx).await?;
    Box::pin(app.handle_event(&mut tui, &mut embedded, completion)).await?;
    assert!(app.agent_navigation.get(&ghost_id).is_none());
    assert!(render_bottom_popup(&app.chat_widget, /*width*/ 80).contains("Enable subagents?"));
    assert_eq!(thread_list_count(), 7);

    let _ = app.config.features.enable(Feature::Collab);
    let (legacy_root_thread_id, legacy_child_thread_id) = create_picker_rollouts(
        codex_home.path(),
        app.config.model_provider_id.as_str(),
        /*index*/ 0,
    )?;
    let legacy_grandchild_thread_id = ThreadId::from_string(
        &create_fake_parented_rollout_with_source(
            codex_home.path(),
            "2025-12-31T23-59-59",
            "2025-12-31T23:59:59Z",
            "Saved legacy grandchild message",
            Some(app.config.model_provider_id.as_str()),
            /*git_info*/ None,
            RolloutSessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: legacy_child_thread_id,
                depth: 2,
                agent_path: Some(
                    AgentPath::try_from("/root/worker/grandchild".to_string())
                        .expect("valid agent path"),
                ),
                agent_nickname: Some("grandchild".to_string()),
                agent_role: Some("worker".to_string()),
            }),
            legacy_root_thread_id.into(),
            legacy_child_thread_id,
        )
        .map_err(|err| color_eyre::eyre::eyre!(err))?,
    )?;
    let legacy_root = app_server
        .resume_thread(app.config.clone(), legacy_root_thread_id, model_settings)
        .await?;
    app.reset_thread_event_state();
    app.enqueue_primary_thread_session(legacy_root.session, legacy_root.turns)
        .await?;
    for (thread_id, agent_path) in [
        (legacy_child_thread_id, "/root/worker"),
        (legacy_grandchild_thread_id, "/root/worker/grandchild"),
    ] {
        app.thread_event_channels
            .insert(thread_id, ThreadEventChannel::new(/*capacity*/ 1));
        app.agent_navigation.upsert(
            thread_id, /*agent_nickname*/ None, /*agent_role*/ None,
            /*is_closed*/ false,
        );
        app.agent_navigation
            .set_agent_path(thread_id, Some(agent_path.to_string()));
        app.agent_navigation
            .set_running(thread_id, /*is_running*/ true);
    }

    Box::pin(app.handle_event(&mut tui, &mut app_server, AppEvent::OpenAgentPicker)).await?;
    let completion = next_agent_picker_completion(&mut app_event_rx).await?;
    Box::pin(app.handle_event(&mut tui, &mut app_server, completion)).await?;
    assert_eq!(
        app.agent_navigation.ordered_thread_ids(),
        vec![
            legacy_root_thread_id,
            legacy_child_thread_id,
            legacy_grandchild_thread_id,
        ]
    );
    insta::assert_snapshot!(
        &render_bottom_popup(&app.chat_widget, /*width*/ 80)
            .replace(&legacy_root_thread_id.to_string(), "[root]")
            .replace(&legacy_child_thread_id.to_string(), "[child]")
            .replace(&legacy_grandchild_thread_id.to_string(), "[grandchild]"),
        @r###"
          Subagents
          Select an agent to watch. ⌥ + ← previous, ⌥ + → next.

        › 1. • Main [default] (current)  [root]
          2. • /root/worker              [child]
          3. • /root/worker/grandchild   [grandchild]

          Press enter to confirm or esc to go back
        "###
    );
    assert_eq!(thread_list_count(), 9);

    embedded.shutdown().await?;
    app_server.shutdown().await?;
    proxy.await??;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn dismissed_agent_picker_does_not_retry_invalidated_refresh() -> Result<()> {
    let (mut app, mut app_event_rx, _op_rx) = make_test_app_with_channels().await;
    let codex_home = tempdir()?;
    app.config.codex_home = codex_home.path().to_path_buf().abs();
    app.config.sqlite = SqliteConfig::new_for_testing(codex_home.path().abs());

    let (root_thread_id, child_thread_id) = create_picker_rollouts(
        codex_home.path(),
        app.config.model_provider_id.as_str(),
        /*index*/ 0,
    )?;
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (mut app_server, requests, proxy) = start_recording_app_server(
        &app.config,
        Some((
            root_thread_id,
            started_tx,
            release_rx,
            BlockedThreadListPage::First,
        )),
    )
    .await?;
    let model_settings = app.resume_model_settings();
    let root = app_server
        .resume_thread(app.config.clone(), root_thread_id, model_settings)
        .await?;
    app.enqueue_primary_thread_session(root.session, root.turns)
        .await?;

    let mut tui = crate::tui::test_support::make_test_tui()?;
    Box::pin(app.handle_event(&mut tui, &mut app_server, AppEvent::OpenAgentPicker)).await?;
    tokio::time::timeout(AGENT_PICKER_MAX_SCAN_DURATION, started_rx).await??;
    app.chat_widget
        .handle_key_event(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
    assert!(
        app.chat_widget
            .selected_index_for_present_view(super::super::agent_picker::AGENT_PICKER_VIEW_ID)
            .is_none()
    );

    app.thread_event_channels
        .insert(child_thread_id, ThreadEventChannel::new(/*capacity*/ 1));
    app.agent_navigation.upsert(
        child_thread_id,
        /*agent_nickname*/ None,
        /*agent_role*/ None,
        /*is_closed*/ false,
    );
    app.agent_navigation
        .set_agent_path(child_thread_id, Some("/root/worker".to_string()));
    app.agent_navigation.mark_running(child_thread_id);
    release_tx.send(()).expect("release blocked thread list");
    let completion = next_agent_picker_completion(&mut app_event_rx).await?;
    Box::pin(app.handle_event(&mut tui, &mut app_server, completion)).await?;

    assert!(
        app.agent_navigation
            .get(&child_thread_id)
            .is_some_and(|entry| entry.is_running && !entry.is_closed)
    );
    assert!(!app.agent_navigation.has_picker_refresh(root_thread_id));
    assert_eq!(
        requests
            .lock()
            .expect("requests")
            .iter()
            .filter(|method| *method == "thread/list")
            .count(),
        2
    );

    app_server.shutdown().await?;
    proxy.await??;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn fresh_idle_thread_read_rejects_older_active_picker_snapshots() -> Result<()> {
    fresh_idle_observations_reject_older_active_picker_snapshots(FreshPickerObservation::ThreadRead)
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn fresh_idle_backfill_rejects_older_active_picker_snapshots() -> Result<()> {
    fresh_idle_observations_reject_older_active_picker_snapshots(
        FreshPickerObservation::LoadedBackfill,
    )
    .await
}

enum FreshPickerObservation {
    ThreadRead,
    LoadedBackfill,
}

async fn fresh_idle_observations_reject_older_active_picker_snapshots(
    observation: FreshPickerObservation,
) -> Result<()> {
    let (mut app, _app_event_rx, _op_rx) = make_test_app_with_channels().await;
    let codex_home = tempdir()?;
    app.config.codex_home = codex_home.path().to_path_buf().abs();
    app.config.sqlite = SqliteConfig::new_for_testing(codex_home.path().abs());
    let (root_thread_id, child_thread_id) = create_picker_rollouts(
        codex_home.path(),
        app.config.model_provider_id.as_str(),
        /*index*/ 0,
    )?;
    let (mut app_server, requests, proxy) =
        start_recording_app_server(&app.config, /*blocked_thread_list*/ None).await?;
    let model_settings = app.resume_model_settings();
    let root = app_server
        .resume_thread(app.config.clone(), root_thread_id, model_settings)
        .await?;
    app.enqueue_primary_thread_session(root.session, root.turns)
        .await?;
    app_server
        .resume_thread(app.config.clone(), child_thread_id, model_settings)
        .await?;
    let mut old_active_thread = app_server
        .thread_read(child_thread_id, /*include_turns*/ false)
        .await?;
    assert_eq!(old_active_thread.status, ThreadStatus::Idle);
    old_active_thread.status = ThreadStatus::Active {
        active_flags: Vec::new(),
    };

    // Start with a stopped entry to exercise successful observations that do not change status.
    app.agent_navigation.upsert(
        child_thread_id,
        old_active_thread.agent_nickname.clone(),
        old_active_thread.agent_role.clone(),
        /*is_closed*/ false,
    );
    app.agent_navigation.mark_stopped(child_thread_id);
    requests.lock().expect("requests").clear();
    let expected_entry = AgentPickerThreadEntry {
        agent_nickname: Some("worker".to_string()),
        agent_role: Some("worker".to_string()),
        agent_path: Some("/root/worker".to_string()),
        is_running: false,
        is_closed: false,
    };
    for _ in 0..2 {
        let generation = app
            .agent_navigation
            .begin_picker_refresh(root_thread_id)
            .expect("start pending picker snapshot");
        match observation {
            FreshPickerObservation::ThreadRead => {
                assert!(
                    app.refresh_agent_picker_thread_liveness(&mut app_server, child_thread_id)
                        .await
                );
            }
            FreshPickerObservation::LoadedBackfill => {
                assert!(
                    app.backfill_loaded_subagent_threads(&mut app_server)
                        .await
                        .completed
                );
            }
        }
        assert_eq!(
            app.agent_navigation.get(&child_thread_id),
            Some(&expected_entry)
        );
        app.apply_agent_picker_thread_refresh(
            &app_server,
            root_thread_id,
            generation,
            AgentPickerRefresh::Completed {
                known_at_start: HashSet::from([root_thread_id, child_thread_id]),
                exhaustive: true,
                result: Ok(vec![old_active_thread.clone()]),
            },
        );
        assert_eq!(
            app.agent_navigation.get(&child_thread_id),
            Some(&expected_entry)
        );
        assert!(!app.agent_navigation.has_picker_refresh(root_thread_id));
    }
    let expected_method = match observation {
        FreshPickerObservation::ThreadRead => "thread/read",
        FreshPickerObservation::LoadedBackfill => "thread/loaded/list",
    };
    assert_eq!(
        requests
            .lock()
            .expect("requests")
            .iter()
            .filter(|method| *method == expected_method)
            .count(),
        2
    );

    app_server.shutdown().await?;
    proxy.await??;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stalled_agent_picker_page_publishes_completed_descendants() -> Result<()> {
    paged_agent_picker_publishes_completed_descendants(BlockedThreadListPage::Second).await
}

#[tokio::test(flavor = "current_thread")]
async fn failed_agent_picker_page_publishes_completed_descendants() -> Result<()> {
    paged_agent_picker_publishes_completed_descendants(BlockedThreadListPage::SecondError).await
}

async fn paged_agent_picker_publishes_completed_descendants(
    blocked_page: BlockedThreadListPage,
) -> Result<()> {
    let (mut app, mut app_event_rx, _op_rx) = make_test_app_with_channels().await;
    let codex_home = tempdir()?;
    app.config.codex_home = codex_home.path().to_path_buf().abs();
    app.config.sqlite = SqliteConfig::new_for_testing(codex_home.path().abs());
    let model_provider = app.config.model_provider_id.as_str();
    let (root_thread_id, child_thread_id) =
        create_picker_rollouts(codex_home.path(), model_provider, /*index*/ 0)?;
    let grandchild_thread_id = ThreadId::from_string(
        &create_fake_parented_rollout_with_source(
            codex_home.path(),
            "2026-01-01T00-00-04",
            "2026-01-01T00:00:04Z",
            "Saved grandchild message",
            Some(model_provider),
            /*git_info*/ None,
            RolloutSessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 2,
                agent_path: Some(
                    AgentPath::try_from("/root/worker/grandchild".to_string())
                        .expect("valid agent path"),
                ),
                agent_nickname: Some("grandchild".to_string()),
                agent_role: Some("worker".to_string()),
            }),
            root_thread_id.into(),
            child_thread_id,
        )
        .map_err(|err| color_eyre::eyre::eyre!(err))?,
    )?;
    let state_db = codex_state::StateRuntime::init(
        SqliteConfig::new_for_testing(codex_home.path().abs()),
        model_provider.to_string(),
    )
    .await
    .map_err(|err| color_eyre::eyre::eyre!(err))?;
    for (parent_thread_id, thread_id, timestamp, depth, agent_path, nickname) in [
        (
            root_thread_id,
            child_thread_id,
            "2026-01-01T00:00:02Z",
            1,
            "/root/worker",
            "worker",
        ),
        (
            child_thread_id,
            grandchild_thread_id,
            "2026-01-01T00:00:04Z",
            2,
            "/root/worker/grandchild",
            "grandchild",
        ),
    ] {
        let source = RolloutSessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id,
            depth,
            agent_path: Some(
                AgentPath::try_from(agent_path.to_string())
                    .map_err(|err| color_eyre::eyre::eyre!(err))?,
            ),
            agent_nickname: Some(nickname.to_string()),
            agent_role: Some("worker".to_string()),
        });
        let rollout_path = codex_rollout::find_thread_path_by_id_str(
            codex_home.path(),
            &thread_id.to_string(),
            /*state_db_ctx*/ None,
        )
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("missing rollout for {thread_id}"))?;
        let mut metadata = codex_state::ThreadMetadataBuilder::new(
            thread_id,
            rollout_path,
            chrono::DateTime::parse_from_rfc3339(timestamp)?.to_utc(),
            source,
        );
        metadata.agent_nickname = Some(nickname.to_string());
        metadata.agent_role = Some("worker".to_string());
        metadata.agent_path = Some(agent_path.to_string());
        metadata.model_provider = Some(model_provider.to_string());
        metadata.cwd = codex_home.path().to_path_buf();
        state_db
            .upsert_thread(&metadata.build(model_provider))
            .await
            .map_err(|err| color_eyre::eyre::eyre!(err))?;
        state_db
            .upsert_thread_spawn_edge(
                parent_thread_id,
                thread_id,
                codex_state::DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .map_err(|err| color_eyre::eyre::eyre!(err))?;
    }

    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (mut app_server, requests, proxy) = start_recording_app_server(
        &app.config,
        Some((root_thread_id, started_tx, release_rx, blocked_page)),
    )
    .await?;
    let model_settings = app.resume_model_settings();
    let root = app_server
        .resume_thread(app.config.clone(), root_thread_id, model_settings)
        .await?;
    app.enqueue_primary_thread_session(root.session, root.turns)
        .await?;
    app_server
        .resume_thread(
            app.config.clone(),
            child_thread_id,
            app.resume_model_settings(),
        )
        .await?;
    assert!(app.agent_navigation.get(&child_thread_id).is_none());

    let ghost_thread_id = ThreadId::new();
    app.agent_navigation.upsert(
        ghost_thread_id,
        /*agent_nickname*/ None,
        /*agent_role*/ None,
        /*is_closed*/ true,
    );
    let thread_list_count = || {
        requests
            .lock()
            .expect("requests")
            .iter()
            .filter(|method| *method == "thread/list")
            .count()
    };
    let mut tui = crate::tui::test_support::make_test_tui()?;
    Box::pin(app.handle_event(&mut tui, &mut app_server, AppEvent::OpenAgentPicker)).await?;
    tokio::time::timeout(AGENT_PICKER_MAX_SCAN_DURATION, started_rx).await??;
    assert_eq!(thread_list_count(), 3);
    assert!(app.agent_navigation.get(&child_thread_id).is_none());

    let mut release_tx = Some(release_tx);
    let partial = if blocked_page == BlockedThreadListPage::Second {
        tokio::time::pause();
        tokio::time::advance(AGENT_PICKER_MAX_SCAN_DURATION).await;
        let timeout = next_agent_picker_completion(&mut app_event_rx).await?;
        assert_matches!(
            &timeout,
            AppEvent::AgentPickerThreadsLoaded {
                refresh: AgentPickerRefresh::TimedOut { threads, .. },
                ..
            } if threads.iter().any(|thread| thread.id == child_thread_id.to_string())
        );
        tokio::time::resume();
        timeout
    } else {
        release_tx
            .take()
            .expect("blocked second page")
            .send(())
            .expect("release failing second page");
        let completion = next_agent_picker_completion(&mut app_event_rx).await?;
        assert_matches!(
            &completion,
            AppEvent::AgentPickerThreadsLoaded {
                refresh: AgentPickerRefresh::Completed {
                    exhaustive: false,
                    result: Ok(threads),
                    ..
                },
                ..
            } if threads.iter().any(|thread| thread.id == child_thread_id.to_string())
        );
        completion
    };
    Box::pin(app.handle_event(&mut tui, &mut app_server, partial)).await?;
    assert!(app.agent_navigation.get(&child_thread_id).is_some());
    assert!(app.agent_navigation.get(&grandchild_thread_id).is_none());
    assert!(app.agent_navigation.get(&ghost_thread_id).is_some());
    assert!(render_bottom_popup(&app.chat_widget, /*width*/ 80).contains("/root/worker"));

    if blocked_page == BlockedThreadListPage::Second {
        Box::pin(app.handle_event(&mut tui, &mut app_server, AppEvent::OpenAgentPicker)).await?;
        assert_eq!(thread_list_count(), 3);
        app.thread_event_channels.insert(
            grandchild_thread_id,
            ThreadEventChannel::new(/*capacity*/ 1),
        );
        app.agent_navigation.upsert(
            grandchild_thread_id,
            /*agent_nickname*/ None,
            /*agent_role*/ None,
            /*is_closed*/ false,
        );
        app.agent_navigation.set_agent_path(
            grandchild_thread_id,
            Some("/root/worker/grandchild".to_string()),
        );
        app.agent_navigation
            .set_running(grandchild_thread_id, /*is_running*/ true);
        release_tx
            .take()
            .expect("blocked second page")
            .send(())
            .expect("release blocked second page");
        let completion = next_agent_picker_completion(&mut app_event_rx).await?;
        Box::pin(app.handle_event(&mut tui, &mut app_server, completion)).await?;
        assert_eq!(
            app.agent_navigation.ordered_thread_ids(),
            vec![root_thread_id, child_thread_id, grandchild_thread_id,]
        );
    }
    assert_eq!(thread_list_count(), 3);

    app_server.shutdown().await?;
    proxy.await??;
    Ok(())
}
