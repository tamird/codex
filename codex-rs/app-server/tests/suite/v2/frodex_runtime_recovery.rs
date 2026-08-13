use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use app_test_support::TestAppServer;
use app_test_support::TestAppServerBuilder;
use app_test_support::create_fake_paginated_rollout;
use app_test_support::rollout_path;
use app_test_support::write_models_cache;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::UserInput;
use codex_protocol::ResponseItemId;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::models::AgentMessageInputContent;
use codex_protocol::models::InternalChatMessageMetadataPassthrough;
use codex_protocol::models::ResponseItem;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use core_test_support::responses;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::time::timeout;

const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);
const POISON_PAYLOAD: &str = "Resume the active synthetic release acceptance task now";
const SUPERVISOR_HEADER: &str =
    "Message Type: NEW_TASK\nTask name: /root\nSender: /root/goal_supervisor\nPayload:\n";
const FERNET_PAYLOAD: &str = "gAAAAABqfQkApRY563QcNHss2A4AKN3hJ027UTP8TRjRMwBjGzwdQ1xZ-6mLXPG8wa8TVLFB3ggULSAKlfpl7C4YbWdR4_R28-k0urBPtKk2Amtf8DdoShe_vVF4ffJ0XoIvR1ryVWmP";

fn candidate_builder(codex_home: &Path) -> TestAppServerBuilder {
    let builder = TestAppServer::builder()
        .with_codex_home(codex_home)
        .with_env_overrides(&[("OPENAI_API_KEY", Some("synthetic-acceptance-key"))]);
    match std::env::var_os("FRODEX_ACCEPTANCE_CODEX") {
        Some(program) => builder.with_program_and_prefix_args(Path::new(&program), &["app-server"]),
        None => builder,
    }
}

fn write_acceptance_config(codex_home: &Path, server_uri: &str) -> Result<()> {
    std::fs::write(
        codex_home.join("config.toml"),
        format!(
            r#"
model = "gpt-5.4"
model_provider = "openai"
openai_base_url = "{server_uri}/v1"
approval_policy = "never"
sandbox_mode = "read-only"
"#
        ),
    )?;
    Ok(())
}

fn encode_rollout(lines: &[RolloutLine]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for line in lines {
        bytes.extend(serde_json::to_vec(line)?);
        bytes.push(b'\n');
    }
    Ok(bytes)
}

fn agent_message_line(ordinal: u64, message_id: &str, payload: &str) -> RolloutLine {
    RolloutLine {
        timestamp: format!("2026-08-13T00:00:{ordinal:02}Z"),
        ordinal: Some(ordinal),
        item: RolloutItem::ResponseItem(
            ResponseItem::AgentMessage {
                id: Some(ResponseItemId::from_server(message_id.to_string())),
                author: "/root/goal_supervisor".to_string(),
                recipient: "/root".to_string(),
                content: vec![
                    AgentMessageInputContent::InputText {
                        text: SUPERVISOR_HEADER.to_string(),
                    },
                    AgentMessageInputContent::EncryptedContent {
                        encrypted_content: payload.to_string(),
                    },
                ],
                internal_chat_message_metadata_passthrough: Some(
                    InternalChatMessageMetadataPassthrough {
                        turn_id: Some(format!("01900000-0000-7000-8000-{ordinal:012}")),
                        ..Default::default()
                    },
                ),
            }
            .into(),
        ),
    }
}

fn append_delivery(lines: &mut Vec<RolloutLine>, ordinal: u64, message_id: &str, payload: &str) {
    lines.push(RolloutLine {
        timestamp: format!("2026-08-13T00:00:{ordinal:02}Z"),
        ordinal: Some(ordinal),
        item: RolloutItem::InterAgentCommunicationMetadata { trigger_turn: true },
    });
    lines.push(agent_message_line(ordinal + 1, message_id, payload));
}

fn prepare_poisoned_rollout(codex_home: &Path) -> Result<(ThreadId, SegmentId, PathBuf, Vec<u8>)> {
    let filename_timestamp = "2026-08-13T00-00-00";
    let thread_id = create_fake_paginated_rollout(
        codex_home,
        filename_timestamp,
        "2026-08-13T00:00:00Z",
        "synthetic release acceptance history",
        Some("openai"),
        /*git_info*/ None,
    )?;
    let path = rollout_path(codex_home, filename_timestamp, &thread_id);
    let mut lines = std::fs::read(path.as_path())?
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(serde_json::from_slice::<RolloutLine>)
        .collect::<Result<Vec<_>, _>>()?;
    let segment_id = SegmentId::new();
    let RolloutItem::SessionMeta(meta) = &mut lines[0].item else {
        anyhow::bail!("synthetic rollout is missing SessionMeta");
    };
    meta.meta.cli_version = "0.148.0-alpha.6+frodex.0".to_string();
    meta.meta.model_provider = Some("openai".to_string());
    meta.meta.segment_id = Some(segment_id);
    append_delivery(
        &mut lines,
        /*ordinal*/ 3,
        "amsg_01900000-0000-7000-8000-000000000004",
        POISON_PAYLOAD,
    );
    append_delivery(
        &mut lines,
        /*ordinal*/ 5,
        "amsg_01900000-0000-7000-8000-000000000006",
        FERNET_PAYLOAD,
    );
    let original = encode_rollout(lines.as_slice())?;
    std::fs::write(path.as_path(), original.as_slice())?;
    Ok((
        ThreadId::from_string(&thread_id)?,
        segment_id,
        path,
        original,
    ))
}

fn find_agent_message<'a>(lines: &'a [RolloutLine], message_id: &str) -> &'a ResponseItem {
    lines
        .iter()
        .find_map(|line| match &line.item {
            RolloutItem::ResponseItem(item)
                if matches!(
                    &**item,
                    ResponseItem::AgentMessage { id: Some(id), .. }
                        if id.as_str() == message_id
                ) =>
            {
                Some(&**item)
            }
            RolloutItem::SessionMeta(_)
            | RolloutItem::ResponseItem(_)
            | RolloutItem::InterAgentCommunication(_)
            | RolloutItem::Compacted(_)
            | RolloutItem::TurnContext(_)
            | RolloutItem::EventMsg(_)
            | RolloutItem::InterAgentCommunicationMetadata { .. }
            | RolloutItem::WorldState(_)
            | RolloutItem::SecurityRiskScore(_)
            | RolloutItem::RealtimeItem(_)
            | RolloutItem::RolloutReference(_) => None,
        })
        .expect("agent message must remain in repaired rollout")
}

fn parse_rollout(path: &Path) -> Result<Vec<RolloutLine>> {
    Ok(std::fs::read(path)?
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(serde_json::from_slice::<RolloutLine>)
        .collect::<Result<Vec<_>, _>>()?)
}

async fn resume(app: &mut TestAppServer, thread_id: ThreadId) -> Result<ThreadResumeResponse> {
    let request_id = app
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            ..Default::default()
        })
        .await?;
    timeout(DEFAULT_READ_TIMEOUT, app.read_response(request_id)).await?
}

#[tokio::test]
async fn released_codex_repairs_alpha6_supervisor_history_across_restart() -> Result<()> {
    let server = responses::start_mock_server().await;
    let response_mock = responses::mount_sse_sequence(
        &server,
        vec![
            responses::sse(vec![
                responses::ev_response_created("acceptance-response-1"),
                responses::ev_assistant_message("acceptance-message-1", "first turn complete"),
                responses::ev_completed("acceptance-response-1"),
            ]),
            responses::sse(vec![
                responses::ev_response_created("acceptance-response-2"),
                responses::ev_assistant_message("acceptance-message-2", "second turn complete"),
                responses::ev_completed("acceptance-response-2"),
            ]),
        ],
    )
    .await;
    let codex_home = TempDir::new()?;
    write_acceptance_config(codex_home.path(), &server.uri())?;
    write_models_cache(codex_home.path())?;
    let (thread_id, original_segment_id, rollout_path, original) =
        prepare_poisoned_rollout(codex_home.path())?;

    let mut first = candidate_builder(codex_home.path())
        .build_initialized()
        .await?;
    resume(&mut first, thread_id).await?;
    assert_eq!(response_mock.requests().len(), 0);

    let repaired_prefix = std::fs::read(rollout_path.as_path())?;
    assert_eq!(repaired_prefix.len(), original.len());
    assert_ne!(repaired_prefix, original);
    let repaired_lines = parse_rollout(rollout_path.as_path())?;
    let ResponseItem::AgentMessage {
        content: repaired_content,
        ..
    } = find_agent_message(
        repaired_lines.as_slice(),
        "amsg_01900000-0000-7000-8000-000000000004",
    )
    else {
        anyhow::bail!("repaired item changed type");
    };
    assert_eq!(
        repaired_content,
        &[AgentMessageInputContent::InputText {
            text: format!("{SUPERVISOR_HEADER}{POISON_PAYLOAD}"),
        }]
    );
    let ResponseItem::AgentMessage {
        content: protected_content,
        ..
    } = find_agent_message(
        repaired_lines.as_slice(),
        "amsg_01900000-0000-7000-8000-000000000006",
    )
    else {
        anyhow::bail!("protected item changed type");
    };
    assert_eq!(
        protected_content,
        &[
            AgentMessageInputContent::InputText {
                text: SUPERVISOR_HEADER.to_string(),
            },
            AgentMessageInputContent::EncryptedContent {
                encrypted_content: FERNET_PAYLOAD.to_string(),
            },
        ]
    );
    let immutable_backup = codex_home
        .path()
        .join("rotated_rollout_segments")
        .join(thread_id.to_string())
        .join(original_segment_id.to_string())
        .join(
            rollout_path
                .file_name()
                .context("rollout path must have a file name")?,
        );
    assert_eq!(std::fs::read(immutable_backup)?, original);
    assert!(!codex_home.path().join("thread_history_2.sqlite").exists());
    assert!(
        !codex_home
            .path()
            .join(".state_5.sqlite.migration.lock")
            .exists()
    );
    assert!(
        !codex_home
            .path()
            .join("rollout-history-repair-state")
            .exists()
    );
    assert!(
        !codex_home
            .path()
            .join("rollout-history-repair-backups")
            .exists()
    );

    timeout(
        DEFAULT_READ_TIMEOUT,
        first.start_turn_and_wait_for_completion(TurnStartParams {
            thread_id: thread_id.to_string(),
            input: vec![UserInput::Text {
                text: "first ordinary acceptance turn".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        }),
    )
    .await??;
    assert_eq!(response_mock.requests().len(), 1);
    assert!(response_mock.requests()[0].body_contains_text(POISON_PAYLOAD));
    timeout(DEFAULT_READ_TIMEOUT, first.shutdown_gracefully()).await??;
    drop(first);

    let mut second = candidate_builder(codex_home.path())
        .build_initialized()
        .await?;
    resume(&mut second, thread_id).await?;
    assert_eq!(response_mock.requests().len(), 1);
    assert!(std::fs::read(rollout_path.as_path())?.starts_with(repaired_prefix.as_slice()));
    timeout(
        DEFAULT_READ_TIMEOUT,
        second.start_turn_and_wait_for_completion(TurnStartParams {
            thread_id: thread_id.to_string(),
            input: vec![UserInput::Text {
                text: "second ordinary acceptance turn".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        }),
    )
    .await??;
    assert_eq!(response_mock.requests().len(), 2);
    assert!(response_mock.requests()[1].body_contains_text(POISON_PAYLOAD));
    timeout(DEFAULT_READ_TIMEOUT, second.shutdown_gracefully()).await??;
    Ok(())
}
