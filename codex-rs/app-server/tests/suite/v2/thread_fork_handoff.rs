//! New terminal panes use independent app-server processes, not a second connection to the owner.

use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_mock_responses_server_repeating_assistant;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ThreadForkImportParams;
use codex_app_server_protocol::ThreadForkParams;
use codex_app_server_protocol::ThreadForkPrepareResponse;
use codex_app_server_protocol::ThreadForkResponse;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;
use pretty_assertions::assert_eq;
use serde_json::Value;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::AsyncReadExt;
use tokio::time::timeout;

const CAPTURED: &str = "parent message captured before handoff";
const LATER: &str = "parent message written after handoff";
const CHILD: &str = "independent child follow-up";

#[tokio::test]
async fn paginated_fork_handoff_between_processes_preserves_snapshot_and_restart() -> Result<()> {
    assert_fork_handoff_between_processes(ThreadHistoryMode::Paginated).await
}

#[tokio::test]
async fn legacy_fork_handoff_between_processes_preserves_snapshot_and_restart() -> Result<()> {
    assert_fork_handoff_between_processes(ThreadHistoryMode::Legacy).await
}

#[tokio::test]
async fn abandoned_fork_handoffs_release_slots_and_source_reservations() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("completed").await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(home.path())?;
    let mut owner = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    let start_id = owner
        .send_thread_start_request_with_auto_env(ThreadStartParams {
            history_mode: Some(ThreadHistoryMode::Paginated),
            ..Default::default()
        })
        .await?;
    let source: ThreadStartResponse = owner.read_response(start_id).await?;
    complete_turn(&mut owner, &source.thread.id, CAPTURED).await?;
    let prepare = |request_id| ClientRequest::ThreadForkPrepare {
        request_id,
        params: ThreadForkParams {
            thread_id: source.thread.id.clone(),
            exclude_turns: true,
            ..Default::default()
        },
    };
    let first: ThreadForkPrepareResponse = owner.request(prepare).await?;
    let second: ThreadForkPrepareResponse = owner.request(prepare).await?;
    let full = owner.request_error(prepare).await?;
    assert!(
        full.error
            .message
            .contains("two fork handoffs are already pending")
    );

    let first_path = first
        .socket_path
        .to_inferred_abs_path()
        .expect("local socket");
    let mut abandoned = codex_uds::UnixStream::connect(first_path.as_path()).await?;
    let length = abandoned.read_u32().await?;
    let mut seed = vec![0; usize::try_from(length)?];
    abandoned.read_exact(&mut seed).await?;
    drop(abandoned);
    timeout(Duration::from_secs(10), async {
        while tokio::fs::try_exists(&first_path).await? {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let replacement: ThreadForkPrepareResponse = owner.request(prepare).await?;
    timeout(Duration::from_secs(20), owner.shutdown_gracefully()).await??;
    for handoff in [second, replacement] {
        let path = handoff
            .socket_path
            .to_inferred_abs_path()
            .expect("local socket");
        assert!(
            !tokio::fs::try_exists(path).await?,
            "owner exit removes pending sockets"
        );
    }
    let mut receiver = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    let resumed: ThreadResumeResponse = receiver
        .request(|request_id| ClientRequest::ThreadResume {
            request_id,
            params: ThreadResumeParams {
                thread_id: source.thread.id.clone(),
                exclude_turns: true,
                ..Default::default()
            },
        })
        .await?;
    assert_eq!(resumed.thread.id, source.thread.id);
    timeout(Duration::from_secs(20), receiver.shutdown_gracefully()).await??;
    Ok(())
}

async fn assert_fork_handoff_between_processes(history_mode: ThreadHistoryMode) -> Result<()> {
    for ephemeral in [false, true] {
        let server = create_mock_responses_server_repeating_assistant("completed").await;
        let home = TempDir::new()?;
        MockResponsesConfig::new(&server.uri()).write(home.path())?;
        let mut owner = TestAppServer::builder()
            .with_codex_home(home.path())
            .build_initialized()
            .await?;
        let start_id = owner
            .send_thread_start_request_with_auto_env(ThreadStartParams {
                history_mode: Some(history_mode),
                ..Default::default()
            })
            .await?;
        let source: ThreadStartResponse = owner.read_response(start_id).await?;
        complete_turn(&mut owner, &source.thread.id, CAPTURED).await?;

        let mut receiver = TestAppServer::builder()
            .with_codex_home(home.path())
            .build_initialized()
            .await?;
        let wrong_owner = receiver
            .request_error(|request_id| ClientRequest::ThreadForkPrepare {
                request_id,
                params: ThreadForkParams {
                    thread_id: source.thread.id.clone(),
                    ephemeral,
                    exclude_turns: true,
                    ..Default::default()
                },
            })
            .await?;
        assert!(wrong_owner.error.message.contains("owning the source"));
        let handoff: ThreadForkPrepareResponse = owner
            .request(|request_id| ClientRequest::ThreadForkPrepare {
                request_id,
                params: ThreadForkParams {
                    thread_id: source.thread.id.clone(),
                    ephemeral,
                    exclude_turns: true,
                    ..Default::default()
                },
            })
            .await?;
        complete_turn(&mut owner, &source.thread.id, LATER).await?;
        let imported: ThreadForkResponse = receiver
            .request(|request_id| ClientRequest::ThreadForkImport {
                request_id,
                params: ThreadForkImportParams {
                    socket_path: handoff.socket_path.clone(),
                },
            })
            .await?;
        assert_eq!(imported.thread.ephemeral, ephemeral);
        assert_eq!(imported.thread.path.is_none(), ephemeral);
        assert_eq!(imported.thread.turns, Vec::new());

        complete_turn(&mut receiver, &imported.thread.id, CHILD).await?;
        let requests = server.received_requests().await.expect("model requests");
        let child_request = requests
            .iter()
            .rfind(|request| request.url.path().ends_with("/responses"))
            .expect("child model request");
        let body: Value = serde_json::from_slice(&child_request.body)?;
        let input = serde_json::to_string(&body["input"])?;
        assert_eq!(input.matches(CAPTURED).count(), 1, "{input}");
        assert_eq!(input.matches(LATER).count(), 0, "{input}");
        assert_eq!(input.matches(CHILD).count(), 1, "{input}");

        // Consumption is one-shot even while the source remains open and can still append.
        let replay = receiver
            .request_error(|request_id| ClientRequest::ThreadForkImport {
                request_id,
                params: ThreadForkImportParams {
                    socket_path: handoff.socket_path,
                },
            })
            .await?;
        assert!(replay.error.message.contains("fork handoff failed"));
        complete_turn(&mut owner, &source.thread.id, "parent remains writable").await?;
        timeout(Duration::from_secs(20), receiver.shutdown_gracefully()).await??;
        timeout(Duration::from_secs(20), owner.shutdown_gracefully()).await??;

        if !ephemeral {
            let mut restarted = TestAppServer::builder()
                .with_codex_home(home.path())
                .build_initialized()
                .await?;
            let resumed: ThreadResumeResponse = restarted
                .request(|request_id| ClientRequest::ThreadResume {
                    request_id,
                    params: ThreadResumeParams {
                        thread_id: imported.thread.id.clone(),
                        exclude_turns: true,
                        ..Default::default()
                    },
                })
                .await?;
            assert_eq!(resumed.thread.id, imported.thread.id);
            let read: ThreadReadResponse = restarted
                .request(|request_id| ClientRequest::ThreadRead {
                    request_id,
                    params: ThreadReadParams {
                        thread_id: imported.thread.id.clone(),
                        include_turns: true,
                    },
                })
                .await?;
            let visible = serde_json::to_string(&read.thread.turns)?;
            assert_eq!(visible.matches(CAPTURED).count(), 1, "{visible}");
            assert_eq!(visible.matches(LATER).count(), 0, "{visible}");
            assert_eq!(visible.matches(CHILD).count(), 1, "{visible}");
            timeout(Duration::from_secs(20), restarted.shutdown_gracefully()).await??;
        }
    }
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
