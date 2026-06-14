#![allow(clippy::unwrap_used, clippy::expect_used)]

use anyhow::Context;
use core_test_support::responses;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex_exec::test_codex_exec;
use serde_json::Value;
use std::string::ToString;
use uuid::Uuid;
use walkdir::WalkDir;
use wiremock::MockServer;

/// Utility: scan the sessions dir for a rollout file that contains `marker`
/// in any response_item.message.content entry. Returns the absolute path.
fn find_session_file_containing_marker(
    sessions_dir: &std::path::Path,
    marker: &str,
) -> Option<std::path::PathBuf> {
    for entry in WalkDir::new(sessions_dir) {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        if !entry.file_type().is_file() {
            continue;
        }
        if !entry.file_name().to_string_lossy().ends_with(".jsonl") {
            continue;
        }
        let path = entry.path();
        if rollout_response_items_contain_marker(path, marker).unwrap_or(false) {
            return Some(path.to_path_buf());
        }
    }
    None
}

fn rollout_response_items_contain_marker(
    path: &std::path::Path,
    marker: &str,
) -> anyhow::Result<bool> {
    let content = std::fs::read_to_string(path)?;
    Ok(content.lines().skip(1).any(|line| {
        let Ok(item): Result<Value, _> = serde_json::from_str(line) else {
            return false;
        };
        item.get("type").and_then(Value::as_str) == Some("response_item")
            && item.get("payload").is_some_and(|payload| {
                payload.get("type").and_then(Value::as_str) == Some("message")
                    && payload
                        .get("content")
                        .map(ToString::to_string)
                        .unwrap_or_default()
                        .contains(marker)
            })
    }))
}

/// Extract the conversation UUID from the first SessionMeta line in the rollout file.
fn extract_conversation_id(path: &std::path::Path) -> String {
    let content = std::fs::read_to_string(path).unwrap();
    let mut lines = content.lines();
    let meta_line = lines.next().expect("missing meta line");
    let meta: Value = serde_json::from_str(meta_line).expect("invalid meta json");
    meta.get("payload")
        .and_then(|p| p.get("id"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

fn extract_forked_from_id(path: &std::path::Path) -> Option<String> {
    let content = std::fs::read_to_string(path).unwrap();
    let mut lines = content.lines();
    let meta_line = lines.next().expect("missing meta line");
    let meta: Value = serde_json::from_str(meta_line).expect("invalid meta json");
    meta.get("payload")
        .and_then(|payload| payload.get("forked_from_id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn exec_sse_response(index: usize) -> String {
    responses::sse(vec![
        responses::ev_response_created(&format!("resp-fork-{index}")),
        responses::ev_assistant_message(&format!("msg-fork-{index}"), "fork response"),
        responses::ev_completed(&format!("resp-fork-{index}")),
    ])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_fork_by_id_creates_new_session_with_copied_history() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));

    let test = test_codex_exec();
    let server = MockServer::start().await;
    let _response_mock =
        responses::mount_sse_sequence(&server, (0..2).map(exec_sse_response).collect()).await;

    let marker = format!("fork-base-{}", Uuid::new_v4());
    let prompt = format!("echo {marker}");

    test.cmd_with_server(&server)
        .arg("--skip-git-repo-check")
        .arg(&prompt)
        .assert()
        .success();

    let sessions_dir = test.home_path().join("sessions");
    let original_path = find_session_file_containing_marker(&sessions_dir, &marker)
        .context("no session file found after first run")?;
    let session_id = extract_conversation_id(&original_path);

    let marker2 = format!("fork-follow-up-{}", Uuid::new_v4());
    let prompt2 = format!("echo {marker2}");

    test.cmd_with_server(&server)
        .arg("--skip-git-repo-check")
        .arg("--fork")
        .arg(&session_id)
        .arg(&prompt2)
        .assert()
        .success();

    let forked_path = find_session_file_containing_marker(&sessions_dir, &marker2)
        .context("no forked session file found for second marker")?;

    assert_ne!(
        forked_path, original_path,
        "fork should create a new session file"
    );

    let forked_content = std::fs::read_to_string(&forked_path)?;
    assert_eq!(
        extract_forked_from_id(&forked_path).as_deref(),
        Some(session_id.as_str())
    );
    assert!(
        forked_content.contains(&marker),
        "forked session should copy ancestor rollout history"
    );
    assert!(forked_content.contains(&marker2));

    let original_content = std::fs::read_to_string(&original_path)?;
    assert!(original_content.contains(&marker));
    assert!(
        !original_content.contains(&marker2),
        "original session should not receive the forked prompt"
    );

    Ok(())
}
