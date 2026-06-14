//! A worktree switch must preserve ordinary client turns, not only model continuation.

use std::path::Path;
use std::process::Command;
use std::time::Duration;

use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnEnvironmentParams;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;
use codex_utils_path_uri::LegacyAppPathString;
use core_test_support::responses;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;
use tokio::time::timeout;

#[test_case::test_case(ThreadHistoryMode::Legacy; "legacy")]
#[test_case::test_case(ThreadHistoryMode::Paginated; "paginated")]
#[tokio::test]
async fn workspace_cwd_allows_next_client_turn(history_mode: ThreadHistoryMode) -> Result<()> {
    let fixture = TempDir::new()?;
    let primary = fixture.path().join("primary");
    let linked = fixture.path().join("linked");
    std::fs::create_dir(&primary)?;
    run_git(&primary, &["init", "-q"])?;
    run_git(
        &primary,
        &[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "--allow-empty",
            "-qm",
            "fixture",
        ],
    )?;
    run_git(
        &primary,
        &[
            "worktree",
            "add",
            "-q",
            "-b",
            "linked",
            linked.to_str().expect("temporary worktree path is UTF-8"),
        ],
    )?;
    let primary = std::fs::canonicalize(primary)?;
    let linked = std::fs::canonicalize(linked)?;
    std::fs::write(
        linked.join("AGENTS.md"),
        "Follow the linked worktree instructions.\n",
    )?;
    let server = responses::start_mock_server().await;
    let model = responses::mount_sse_sequence(
        &server,
        vec![
            responses::sse(vec![
                responses::ev_response_created("switch"),
                responses::ev_function_call_with_namespace(
                    "switch-cwd",
                    "workspace",
                    "set_cwd",
                    &json!({ "path": linked }).to_string(),
                ),
                responses::ev_completed("switch"),
            ]),
            responses::sse(vec![
                responses::ev_response_created("switched"),
                responses::ev_assistant_message("switched-message", "switched"),
                responses::ev_completed("switched"),
            ]),
            responses::sse(vec![
                responses::ev_response_created("continued"),
                responses::ev_assistant_message("continued-message", "continued"),
                responses::ev_completed("continued"),
            ]),
        ],
    )
    .await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_root_config("features.workspace_cwd_tool = true")
        .write(home.path())?;
    let mut client = TestAppServer::builder()
        .with_codex_home(home.path())
        // This regression needs the TUI's thread-owned local environment, not a test attachment.
        .without_auto_env()
        .build_initialized()
        .await?;
    let request = client
        .send_thread_start_request(ThreadStartParams {
            cwd: Some(primary.to_string_lossy().into_owned()),
            history_mode: Some(history_mode),
            ..Default::default()
        })
        .await?;
    let started: ThreadStartResponse = client.read_response(request).await?;
    for (text, environments) in [
        ("switch to the linked worktree", None),
        (
            "continue after switching worktrees",
            Some(vec![TurnEnvironmentParams {
                environment_id: "local".to_string(),
                cwd: LegacyAppPathString::from_path(&linked),
                runtime_workspace_roots: Some(vec![LegacyAppPathString::from_path(&linked)]),
            }]),
        ),
    ] {
        let completed = timeout(
            Duration::from_secs(20),
            client.start_turn_and_wait_for_completion(TurnStartParams {
                thread_id: started.thread.id.clone(),
                input: vec![UserInput::Text {
                    text: text.to_string(),
                    text_elements: Vec::new(),
                }],
                environments,
                ..Default::default()
            }),
        )
        .await??;
        assert_eq!(completed.turn.status, TurnStatus::Completed);
    }
    let requests = model.requests();
    assert_eq!(requests.len(), 3);
    let switched: serde_json::Value = serde_json::from_str(
        &requests[1]
            .function_call_output_text("switch-cwd")
            .expect("switch result"),
    )?;
    assert_eq!(switched["cwd"], json!(linked));
    assert_eq!(switched["changed"], true);
    assert!(requests[2].body_contains_text("Follow the linked worktree instructions."));
    assert!(requests[2].body_contains_text("continue after switching worktrees"));
    let read: ThreadReadResponse = client
        .request(|request_id| ClientRequest::ThreadRead {
            request_id,
            params: ThreadReadParams {
                thread_id: started.thread.id.clone(),
                include_turns: true,
            },
        })
        .await?;
    assert_eq!(read.thread.cwd.as_path(), linked.as_path());
    assert_eq!(read.thread.turns.len(), 2);
    timeout(Duration::from_secs(20), client.shutdown_gracefully()).await??;
    Ok(())
}

fn run_git(cwd: &Path, args: &[&str]) -> Result<()> {
    let output = Command::new("git").args(args).current_dir(cwd).output()?;
    anyhow::ensure!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
