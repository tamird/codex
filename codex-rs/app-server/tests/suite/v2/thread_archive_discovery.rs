use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_fake_rollout;
use app_test_support::create_mock_responses_server_repeating_assistant;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ThreadArchiveParams;
use codex_app_server_protocol::ThreadArchiveResponse;
use codex_app_server_protocol::ThreadForkParams;
use codex_app_server_protocol::ThreadForkResponse;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::UserInput;
use codex_protocol::ThreadId;
use codex_state::StateRuntime;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

#[tokio::test]
async fn thread_archive_loaded_fork_does_not_read_unrelated_rollouts() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(home.path())?;
    let state = StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(home.path().abs()),
        "mock_provider".to_string(),
    )
    .await?;
    state
        .mark_backfill_complete(/*last_watermark*/ None)
        .await?;
    // Keep background migration out of the I/O measurement without disabling foreground history.
    let _maintenance = codex_rollout::try_acquire_rollout_maintenance_job_lock(home.path())?
        .expect("maintenance lock");
    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .with_env_overrides(&[("FRODEX_HISTORY_IO_TRACE", Some("1"))])
        .with_json_logging("warn,codex_history_io=trace")
        .build_initialized()
        .await?;
    let parent = app
        .start_thread(ThreadStartParams {
            history_mode: Some(ThreadHistoryMode::Paginated),
            ..Default::default()
        })
        .await?
        .thread;
    app.start_turn_and_wait_for_completion(TurnStartParams {
        thread_id: parent.id.clone(),
        input: vec![UserInput::Text {
            text: "parent history".to_string(),
            text_elements: Vec::new(),
        }],
        ..Default::default()
    })
    .await?;
    let ThreadForkResponse { thread: child, .. } = app
        .request(|request_id| ClientRequest::ThreadFork {
            request_id,
            params: ThreadForkParams {
                thread_id: parent.id.clone(),
                exclude_turns: true,
                ..Default::default()
            },
        })
        .await?;
    app.start_turn_and_wait_for_completion(TurnStartParams {
        thread_id: child.id.clone(),
        input: vec![UserInput::Text {
            text: "child history".to_string(),
            text_elements: Vec::new(),
        }],
        ..Default::default()
    })
    .await?;

    let parent_before: ThreadReadResponse = app
        .request(|request_id| ClientRequest::ThreadRead {
            request_id,
            params: ThreadReadParams {
                thread_id: parent.id.clone(),
                include_turns: true,
            },
        })
        .await?;
    assert!(!parent_before.thread.turns.is_empty());
    // These files did not exist during startup or fork. Any read is unrelated archive work.
    let mut unrelated_ids = Vec::new();
    for _ in 0..64 {
        unrelated_ids.push(create_fake_rollout(
            home.path(),
            "2025-01-01T00-00-00",
            "2025-01-01T00:00:00Z",
            "unrelated history",
            Some("mock_provider"),
            /*git_info*/ None,
        )?);
    }
    let _: ThreadArchiveResponse = app
        .request(|request_id| ClientRequest::ThreadArchive {
            request_id,
            params: ThreadArchiveParams {
                thread_id: child.id.clone(),
            },
        })
        .await?;
    // A second archive must remain idempotent, not attempt to reload the stopped writer.
    let _: ThreadArchiveResponse = app
        .request(|request_id| ClientRequest::ThreadArchive {
            request_id,
            params: ThreadArchiveParams {
                thread_id: child.id.clone(),
            },
        })
        .await?;
    let parent_after: ThreadReadResponse = app
        .request(|request_id| ClientRequest::ThreadRead {
            request_id,
            params: ThreadReadParams {
                thread_id: parent.id.clone(),
                include_turns: true,
            },
        })
        .await?;
    assert_eq!(parent_after.thread.turns, parent_before.thread.turns);
    app.shutdown_gracefully().await?;

    let events = app.json_log_events()?;
    let opened_paths = events
        .iter()
        .filter(|event| event["fields"]["event.name"] == "codex.history.rollout.open")
        .filter_map(|event| event["fields"]["rollout_path"].as_str())
        .collect::<Vec<_>>();
    assert!(
        opened_paths.iter().any(|path| path.contains(&child.id)),
        "the loaded fork must establish that rollout read observation is enabled"
    );
    let unrelated_reads = opened_paths
        .iter()
        .filter(|path| unrelated_ids.iter().any(|id| path.contains(id)))
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(unrelated_reads, Vec::<&str>::new());
    let parent_metadata = state
        .get_thread(ThreadId::from_string(&parent.id)?)
        .await?
        .expect("parent metadata");
    let child_metadata = state
        .get_thread(ThreadId::from_string(&child.id)?)
        .await?
        .expect("child metadata");
    assert!(parent_metadata.archived_at.is_none());
    assert!(parent_metadata.rollout_path.exists());
    assert!(child_metadata.archived_at.is_some());
    assert!(child_metadata.rollout_path.exists());
    Ok(())
}
