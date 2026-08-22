//! Fork compatibility must hold through the public API and physical CODEX_HOME state.

use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_mock_responses_server_repeating_assistant;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ThreadForkParams;
use codex_app_server_protocol::ThreadForkResponse;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;
use pretty_assertions::assert_eq;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;

#[cfg(unix)]
#[test_case::test_case(false; "plain")]
#[test_case::test_case(true; "compressed")]
#[tokio::test]
async fn selected_rollout_alias_supports_fork_and_explicit_resume_after_restart(
    compressed: bool,
) -> Result<()> {
    use codex_app_server_protocol::ThreadResumeParams;
    use codex_app_server_protocol::ThreadResumeResponse;
    use codex_protocol::ThreadId;
    use codex_utils_absolute_path::test_support::PathExt;

    let server = create_mock_responses_server_repeating_assistant("completed").await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(home.path())?;
    let mut owner = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    let request = owner
        .send_thread_start_request_with_auto_env(ThreadStartParams {
            history_mode: Some(ThreadHistoryMode::Paginated),
            ..Default::default()
        })
        .await?;
    let source: ThreadStartResponse = owner.read_response(request).await?;
    complete_turn(&mut owner, &source.thread.id, "alias parent history").await?;
    timeout(Duration::from_secs(20), owner.shutdown_gracefully()).await??;

    let state = codex_state::StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(home.path().abs()),
        "mock_provider".to_string(),
    )
    .await?;
    let mut metadata = state
        .get_thread(ThreadId::from_string(&source.thread.id)?)
        .await?
        .expect("persisted parent metadata");
    let canonical_source = std::fs::canonicalize(&metadata.rollout_path)?;
    let canonical_sessions = std::fs::canonicalize(home.path().join("sessions"))?;
    let alias = home.path().join("sessions-alias");
    std::os::unix::fs::symlink(&canonical_sessions, &alias)?;
    if compressed {
        std::fs::write(
            canonical_source.with_extension("jsonl.zst"),
            zstd::stream::encode_all(
                std::fs::read(&canonical_source)?.as_slice(),
                /*level*/ 0,
            )?,
        )?;
        std::fs::remove_file(&canonical_source)?;
    }
    metadata.rollout_path = alias.join(canonical_source.strip_prefix(canonical_sessions)?);
    state.upsert_thread(&metadata).await?;
    drop(state);

    let mut restarted = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    let resumed: ThreadResumeResponse = restarted
        .request(|request_id| ClientRequest::ThreadResume {
            request_id,
            params: ThreadResumeParams {
                thread_id: source.thread.id.clone(),
                path: Some(canonical_source),
                exclude_turns: true,
                ..Default::default()
            },
        })
        .await?;
    assert_eq!(resumed.thread.id, source.thread.id);
    let forked: ThreadForkResponse = restarted
        .request(|request_id| ClientRequest::ThreadFork {
            request_id,
            params: ThreadForkParams {
                thread_id: source.thread.id.clone(),
                exclude_turns: true,
                ..Default::default()
            },
        })
        .await?;
    complete_turn(&mut restarted, &forked.thread.id, "alias child follow-up").await?;
    let requests = server.received_requests().await.expect("model requests");
    let body: serde_json::Value =
        serde_json::from_slice(&requests.last().expect("child request").body)?;
    let input = serde_json::to_string(&body["input"])?;
    assert_eq!(input.matches("alias parent history").count(), 1, "{input}");
    assert_eq!(input.matches("alias child follow-up").count(), 1, "{input}");
    timeout(Duration::from_secs(20), restarted.shutdown_gracefully()).await??;
    Ok(())
}

#[tokio::test]
async fn in_memory_fork_preserves_parent_model_context() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("completed").await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_root_config(
            "experimental_thread_store = { type = \"in_memory\", id = \"fork-compatibility\" }",
        )
        .write(home.path())?;
    let mut client = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    let request = client
        .send_thread_start_request_with_auto_env(ThreadStartParams {
            history_mode: Some(ThreadHistoryMode::Legacy),
            ..Default::default()
        })
        .await?;
    let source: ThreadStartResponse = client.read_response(request).await?;
    complete_turn(&mut client, &source.thread.id, "in-memory parent history").await?;
    let forked: ThreadForkResponse = client
        .request(|request_id| ClientRequest::ThreadFork {
            request_id,
            params: ThreadForkParams {
                thread_id: source.thread.id.clone(),
                exclude_turns: true,
                ..Default::default()
            },
        })
        .await?;
    complete_turn(&mut client, &forked.thread.id, "in-memory child follow-up").await?;
    let requests = server.received_requests().await.expect("model requests");
    let body: serde_json::Value =
        serde_json::from_slice(&requests.last().expect("child request").body)?;
    let input = serde_json::to_string(&body["input"])?;
    assert_eq!(
        input.matches("in-memory parent history").count(),
        1,
        "{input}"
    );
    assert_eq!(
        input.matches("in-memory child follow-up").count(),
        1,
        "{input}"
    );
    assert_eq!(forked.thread.forked_from_id, Some(source.thread.id));
    timeout(Duration::from_secs(20), client.shutdown_gracefully()).await??;
    Ok(())
}

async fn complete_turn(client: &mut TestAppServer, thread_id: &str, text: &str) -> Result<()> {
    let request = client
        .send_turn_start_request(TurnStartParams {
            thread_id: thread_id.to_owned(),
            input: vec![UserInput::Text {
                text: text.to_owned(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })
        .await?;
    let _: TurnStartResponse = client.read_response(request).await?;
    let completed: TurnCompletedNotification = timeout(
        Duration::from_secs(20),
        client.read_notification("turn/completed"),
    )
    .await??;
    assert_eq!(completed.thread_id, thread_id);
    assert_eq!(completed.turn.status, TurnStatus::Completed);
    Ok(())
}
