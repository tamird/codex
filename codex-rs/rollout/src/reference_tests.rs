use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use codex_history::CompactedItem;
use codex_history::RolloutItem;
use codex_history::RolloutLine;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::config_types::ApprovalsReviewer;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::ReasoningSummary;
use codex_protocol::config_types::Settings;
use codex_protocol::config_types::WindowsSandboxLevel;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::models::ContentItem;
use codex_protocol::models::PermissionProfile;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AgentMessageEvent;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SandboxPolicy;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadRolledBackEvent;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::ThreadSettingsSnapshot;
use codex_protocol::protocol::TokenCountEvent;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnContextItem;
use codex_protocol::protocol::TurnEnvironmentSelections;
use codex_protocol::protocol::TurnStartedEvent;
use codex_protocol::protocol::UserMessageEvent;
use codex_protocol::user_input::UserInput;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;

use super::BoundedRolloutMaterializer;
use super::ExpansionCursor;
use super::ExpansionState;
use super::MAX_REQUEST_CACHE_BYTES;
use super::MAX_ROLLOUT_REFERENCE_DEPTH;
use super::MaterializationPolicy;
use super::compose_compacted_replacement_history_filter_texts;
use super::expand_lines;
use super::materialize_bounded_rollout_lines;
use super::materialize_model_context_rollout_items_from;
use super::materialize_recent_rollout_lines;
use super::materialize_rollout_lines;
use super::resolve_rollout_reference_path;
use crate::ARCHIVED_SESSIONS_SUBDIR;
use crate::CertifiedSegmentStateCheckpoint;
use crate::ROLLOUT_SEGMENTS_SUBDIR;
use crate::ROTATED_ROLLOUT_SEGMENTS_SUBDIR;
use crate::ResponseItemEnvelope;
use crate::SESSIONS_SUBDIR;
use codex_utils_absolute_path::AbsolutePathBuf;

fn meta_line(thread_id: ThreadId, segment_id: SegmentId, ordinal: u64) -> RolloutLine {
    meta_line_with_segment(thread_id, Some(segment_id), ordinal)
}

fn legacy_meta_line(thread_id: ThreadId, ordinal: u64) -> RolloutLine {
    meta_line_with_segment(thread_id, /*segment_id*/ None, ordinal)
}

fn meta_line_with_segment(
    thread_id: ThreadId,
    segment_id: Option<SegmentId>,
    ordinal: u64,
) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:00Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                session_id: thread_id.into(),
                id: thread_id,
                segment_id,
                timestamp: "2026-07-13T00:00:00Z".to_string(),
                cwd: PathBuf::from("/tmp"),
                originator: "test".to_string(),
                cli_version: "test".to_string(),
                source: SessionSource::Exec,
                ..SessionMeta::default()
            },
            git: None,
        }),
    }
}

fn paginated_meta_line(
    thread_id: ThreadId,
    ordinal: u64,
    history_base: Option<HistoryPosition>,
) -> RolloutLine {
    let mut line = meta_line_with_segment(thread_id, /*segment_id*/ None, ordinal);
    let RolloutItem::SessionMeta(meta) = &mut line.item else {
        unreachable!();
    };
    meta.meta.history_mode = ThreadHistoryMode::Paginated;
    meta.meta.history_base = history_base;
    line
}

fn agent_line(message: &str, ordinal: u64) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:01Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
            message: message.to_string(),
            phase: None,
            delivery: None,
            memory_citation: None,
        })),
    }
}

fn user_line(message: &str, ordinal: u64) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:01Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::ResponseItem(
            ResponseItem::Message {
                id: None,
                role: "user".to_string(),
                content: vec![ContentItem::InputText {
                    text: message.to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }
            .into(),
        ),
    }
}

fn user_event_line(message: &str, ordinal: u64) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:01Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
            message: message.to_string(),
            ..Default::default()
        })),
    }
}

fn completed_user_item_line(
    thread_id: ThreadId,
    turn_id: &str,
    message: &str,
    ordinal: u64,
) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:01Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
            thread_id,
            turn_id: turn_id.to_string(),
            item: TurnItem::UserMessage(UserMessageItem {
                id: format!("user-{ordinal}"),
                client_id: None,
                content: vec![UserInput::Text {
                    text: message.to_string(),
                    text_elements: Vec::new(),
                }],
            }),
            started_at_ms: None,
            completed_at_ms: 0,
        })),
    }
}

fn turn_started_line(turn_id: &str, ordinal: u64) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:01Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
            turn_id: turn_id.to_string(),
            trace_id: None,
            started_at: None,
            model_context_window: None,
            collaboration_mode_kind: Default::default(),
        })),
    }
}

fn turn_complete_line(turn_id: &str, ordinal: u64) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:01Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
            turn_id: turn_id.to_string(),
            started_at: None,
            last_agent_message: None,
            error: None,
            completed_at: None,
            duration_ms: None,
            time_to_first_token_ms: None,
        })),
    }
}

fn turn_context_line(root: &Path, turn_id: &str, ordinal: u64) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:01Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::TurnContext(TurnContextItem {
            turn_id: Some(turn_id.to_string()),
            cwd: serde_json::from_value(json!(root)).expect("absolute test cwd"),
            workspace_roots: None,
            current_date: None,
            timezone: None,
            approval_policy: AskForApproval::Never,
            approvals_reviewer: None,
            sandbox_policy: SandboxPolicy::new_read_only_policy(),
            permission_profile: None,
            active_permission_profile: None,
            network: None,
            file_system_sandbox_policy: None,
            model: "test-model".to_string(),
            comp_hash: None,
            personality: None,
            collaboration_mode: None,
            multi_agent_version: None,
            multi_agent_mode: None,
            realtime_active: None,
            cyber_access_program: None,
            service_tier: None,
            model_profile: None,
            effort: None,
            summary: ReasoningSummary::Auto,
        }),
    }
}

fn compacted_line(message: &str, ordinal: u64) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:01Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::Compacted(CompactedItem {
            message: message.to_string(),
            replacement_history: Some(Vec::new()),
            mcp_resource_origins: None,
            window_number: Some(1),
            first_window_id: None,
            previous_window_id: None,
            window_id: None,
            segment_state_checkpoint: None,
        }),
    }
}

fn checkpoint_lines(message: &str, first_ordinal: u64) -> Vec<RolloutLine> {
    let cwd: AbsolutePathBuf = serde_json::from_value(json!("/tmp")).expect("absolute test cwd");
    let compacted = CompactedItem {
        message: message.to_string(),
        replacement_history: Some(Vec::new()),
        mcp_resource_origins: None,
        window_number: Some(1),
        first_window_id: Some("019b3f6e-0000-7000-8000-000000000001".to_string()),
        previous_window_id: None,
        window_id: Some("019b3f6e-0000-7000-8000-000000000002".to_string()),
        segment_state_checkpoint: None,
    };
    let settings = ThreadSettingsAppliedEvent {
        thread_settings: ThreadSettingsSnapshot {
            model: "test-model".to_string(),
            model_provider_id: "test-provider".to_string(),
            service_tier: None,
            approval_policy: AskForApproval::Never,
            approvals_reviewer: ApprovalsReviewer::User,
            permission_profile: PermissionProfile::workspace_write(),
            active_permission_profile: None,
            cwd: cwd.clone(),
            environments: Some(TurnEnvironmentSelections::new(cwd, Vec::new())),
            workspace_roots: Some(Vec::new()),
            profile_workspace_roots: Some(Vec::new()),
            windows_sandbox_level: Some(WindowsSandboxLevel::Disabled),
            reasoning_effort: None,
            reasoning_summary: None,
            personality: None,
            collaboration_mode: CollaborationMode {
                mode: ModeKind::Default,
                settings: Settings {
                    model: "test-model".to_string(),
                    reasoning_effort: None,
                    developer_instructions: None,
                },
            },
        },
    };
    CertifiedSegmentStateCheckpoint::new(
        compacted,
        /*previous_turn_settings*/ None,
        /*world_state*/ None,
        /*reference_context*/ None,
        settings,
        TokenCountEvent {
            info: None,
            rate_limits: None,
        },
    )
    .expect("valid checkpoint")
    .into_items()
    .into_iter()
    .enumerate()
    .map(|(index, item)| RolloutLine {
        timestamp: "2026-07-13T00:00:01Z".to_string(),
        ordinal: Some(first_ordinal + u64::try_from(index).expect("checkpoint index")),
        item,
    })
    .collect()
}

fn developer_line(message: &str, ordinal: u64) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:01Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::ResponseItem(
            ResponseItem::Message {
                id: None,
                role: "developer".to_string(),
                content: vec![ContentItem::InputText {
                    text: message.to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }
            .into(),
        ),
    }
}

fn compacted_developer_lines(messages: &[&str], ordinal: u64) -> RolloutLine {
    let mut line = compacted_line("checkpoint", ordinal);
    let RolloutItem::Compacted(compacted) = &mut line.item else {
        unreachable!("compacted fixture");
    };
    compacted.replacement_history = Some(
        messages
            .iter()
            .map(|message| {
                ResponseItem::Message {
                    id: None,
                    role: "developer".to_string(),
                    content: vec![ContentItem::InputText {
                        text: (*message).to_string(),
                    }],
                    phase: None,
                    internal_chat_message_metadata_passthrough: None,
                }
                .into()
            })
            .collect(),
    );
    line
}

fn reference_line(
    path: PathBuf,
    thread_id: ThreadId,
    segment_id: SegmentId,
    ordinal: u64,
) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:02Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::RolloutReference(RolloutReferenceItem {
            rollout_id: None,
            rollout_path: path,
            thread_id: Some(thread_id),
            rollout_timestamp: None,
            segment_id: Some(segment_id),
            max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        }),
    }
}

fn legacy_reference_line(path: PathBuf, thread_id: ThreadId, ordinal: u64) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-07-13T00:00:02Z".to_string(),
        ordinal: Some(ordinal),
        item: RolloutItem::RolloutReference(RolloutReferenceItem {
            rollout_id: None,
            rollout_path: path,
            thread_id: Some(thread_id),
            rollout_timestamp: None,
            segment_id: None,
            max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        }),
    }
}

fn write_rollout(path: &Path, lines: &[RolloutLine]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut jsonl = lines
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()?
        .join("\n");
    jsonl.push('\n');
    fs::write(path, jsonl)
}

fn write_padded_immutable_rollout_chain(
    codex_home: &Path,
    thread_id: ThreadId,
    segment_count: usize,
    padding_bytes: usize,
) -> io::Result<PathBuf> {
    let mut previous_segment = None;
    for index in 0..segment_count {
        let segment_id = SegmentId::new();
        let segment_path = codex_home
            .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
            .join(thread_id.to_string())
            .join(segment_id.to_string())
            .join("segment.jsonl");
        let ordinal = u64::try_from(index).expect("segment count fits rollout ordinal") * 3;
        let mut lines = vec![meta_line(thread_id, segment_id, ordinal)];
        if let Some((previous_path, previous_id)) = previous_segment {
            lines.push(reference_line(
                previous_path,
                thread_id,
                previous_id,
                ordinal + 1,
            ));
        }
        lines.push(agent_line(&format!("segment-{index}"), ordinal + 2));

        let parent = segment_path.parent().expect("immutable segment directory");
        fs::create_dir_all(parent)?;
        let mut jsonl = lines
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()?
            .join("\n");
        jsonl.extend(std::iter::repeat_n(' ', padding_bytes));
        jsonl.push('\n');
        fs::write(&segment_path, jsonl)?;
        previous_segment = Some((segment_path, segment_id));
    }

    let root_path = codex_home.join("active.jsonl");
    let root_segment = SegmentId::new();
    let ordinal = u64::try_from(segment_count).expect("segment count fits rollout ordinal") * 3;
    let mut root_lines = vec![meta_line(thread_id, root_segment, ordinal)];
    if let Some((previous_path, previous_id)) = previous_segment {
        root_lines.push(reference_line(
            previous_path,
            thread_id,
            previous_id,
            ordinal + 1,
        ));
    }
    write_rollout(root_path.as_path(), &root_lines)?;
    Ok(root_path)
}

fn rollout_file_name(timestamp: &str, thread_id: ThreadId) -> String {
    format!("rollout-{timestamp}-{thread_id}.jsonl")
}

fn immutable_segment_path(
    codex_home: &Path,
    thread_id: ThreadId,
    segment_id: SegmentId,
    timestamp: &str,
) -> PathBuf {
    codex_home
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(segment_id.to_string())
        .join(rollout_file_name(timestamp, thread_id))
}

fn active_rollout_path(codex_home: &Path, thread_id: ThreadId, timestamp: &str) -> PathBuf {
    codex_home
        .join(SESSIONS_SUBDIR)
        .join("2026/07/13")
        .join(rollout_file_name(timestamp, thread_id))
}

fn native_segment_path(
    codex_home: &Path,
    thread_id: ThreadId,
    rollout_id: ThreadId,
    timestamp: &str,
) -> PathBuf {
    let active = active_rollout_path(codex_home, thread_id, timestamp);
    let file_name = crate::rollout_path_with_rollout_id(active.as_path(), rollout_id)
        .expect("canonical rollout filename")
        .file_name()
        .expect("rollout file name")
        .to_owned();
    codex_home
        .join(SESSIONS_SUBDIR)
        .join(ROLLOUT_SEGMENTS_SUBDIR)
        .join("2026/07/13")
        .join(file_name)
}

fn write_native_history_base_chain(
    codex_home: &Path,
    predecessor_count: usize,
) -> io::Result<(PathBuf, Vec<String>)> {
    let thread_id = ThreadId::new();
    let mut history_base = None;
    let mut messages = Vec::new();
    for index in 0..predecessor_count {
        let rollout_id = ThreadId::new();
        let ordinal = u64::try_from(index).expect("segment index fits u64") * 2;
        let message = format!("native-segment-{index}");
        let path = native_segment_path(codex_home, thread_id, rollout_id, "2026-07-13T00-00-00");
        write_rollout(
            path.as_path(),
            &[
                paginated_meta_line(thread_id, ordinal, history_base),
                agent_line(message.as_str(), ordinal + 1),
            ],
        )?;
        history_base = Some(HistoryPosition {
            thread_id: rollout_id,
            end_ordinal_exclusive: ordinal + 2,
            end_byte_offset: fs::metadata(path.as_path())?.len(),
        });
        messages.push(message);
    }

    let active_ordinal = u64::try_from(predecessor_count).expect("segment count fits u64") * 2;
    let active_path = active_rollout_path(codex_home, thread_id, "2026-07-13T00-00-01");
    write_rollout(
        active_path.as_path(),
        &[
            paginated_meta_line(thread_id, active_ordinal, history_base),
            agent_line("native-active", active_ordinal + 1),
        ],
    )?;
    messages.push("native-active".to_string());
    Ok((active_path, messages))
}

fn event_messages(lines: &[RolloutLine]) -> Vec<&str> {
    lines
        .iter()
        .filter_map(|line| match &line.item {
            RolloutItem::EventMsg(EventMsg::AgentMessage(event)) => Some(event.message.as_str()),
            _ => None,
        })
        .collect()
}

fn developer_texts(lines: &[RolloutLine]) -> Vec<&str> {
    lines
        .iter()
        .flat_map(|line| match &line.item {
            RolloutItem::ResponseItem(item) => developer_text(&item.item).into_iter().collect(),
            RolloutItem::Compacted(compacted) => compacted
                .replacement_history
                .iter()
                .flatten()
                .filter_map(|item| developer_text(&item.item))
                .collect(),
            _ => Vec::new(),
        })
        .collect()
}

fn developer_text(item: &ResponseItem) -> Option<&str> {
    let ResponseItem::Message { role, content, .. } = item else {
        return None;
    };
    let [ContentItem::InputText { text }] = content.as_slice() else {
        return None;
    };
    (role == "developer").then_some(text.as_str())
}

#[test]
fn composes_nested_filter_texts_in_stable_order() {
    let inherited = vec![
        "outer-a".to_string(),
        "shared".to_string(),
        "outer-a".to_string(),
    ];
    let local = vec!["inner-b".to_string(), "shared".to_string()];
    assert_eq!(
        compose_compacted_replacement_history_filter_texts(
            Some(inherited.as_slice()),
            Some(local.as_slice()),
        ),
        Some(vec![
            "outer-a".to_string(),
            "shared".to_string(),
            "inner-b".to_string(),
        ])
    );
    assert_eq!(
        compose_compacted_replacement_history_filter_texts(
            /*inherited*/ None, /*local*/ None,
        ),
        None
    );
    assert_eq!(
        compose_compacted_replacement_history_filter_texts(Some(&[]), /*local*/ None,),
        Some(Vec::new())
    );
    assert_eq!(
        compose_compacted_replacement_history_filter_texts(/*inherited*/ None, Some(&[]),),
        Some(Vec::new())
    );
}

#[tokio::test]
async fn nested_reference_filters_compose_for_direct_and_compacted_messages() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let oldest_segment = SegmentId::new();
    let middle_segment = SegmentId::new();
    let root_segment = SegmentId::new();
    let segment_path = |segment_id: SegmentId, timestamp: &str| {
        home.path()
            .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
            .join(thread_id.to_string())
            .join(segment_id.to_string())
            .join(rollout_file_name(timestamp, thread_id))
    };
    let oldest_path = segment_path(oldest_segment, "2026-07-13T00-00-00");
    let middle_path = segment_path(middle_segment, "2026-07-13T00-01-00");
    let root_path = home
        .path()
        .join("sessions/2026/07/13")
        .join(rollout_file_name("2026-07-13T00-02-00", thread_id));

    write_rollout(
        oldest_path.as_path(),
        &[
            meta_line(thread_id, oldest_segment, /*ordinal*/ 0),
            developer_line("A", /*ordinal*/ 1),
            developer_line("B", /*ordinal*/ 2),
            developer_line("C", /*ordinal*/ 3),
            compacted_developer_lines(&["A", "B", "C"], /*ordinal*/ 4),
        ],
    )?;
    let mut inner_reference =
        reference_line(oldest_path, thread_id, oldest_segment, /*ordinal*/ 6);
    let RolloutItem::RolloutReference(inner) = &mut inner_reference.item else {
        unreachable!("reference fixture");
    };
    inner.compacted_replacement_history_filter_texts = Some(vec!["B".to_string(), "A".to_string()]);
    write_rollout(
        middle_path.as_path(),
        &[
            meta_line(thread_id, middle_segment, /*ordinal*/ 5),
            inner_reference,
            developer_line("A", /*ordinal*/ 7),
            developer_line("B", /*ordinal*/ 8),
            developer_line("C", /*ordinal*/ 9),
            compacted_developer_lines(&["A", "B", "C"], /*ordinal*/ 10),
        ],
    )?;
    let mut outer_reference =
        reference_line(middle_path, thread_id, middle_segment, /*ordinal*/ 12);
    let RolloutItem::RolloutReference(outer) = &mut outer_reference.item else {
        unreachable!("reference fixture");
    };
    outer.compacted_replacement_history_filter_texts = Some(vec!["A".to_string()]);
    write_rollout(
        root_path.as_path(),
        &[
            meta_line(thread_id, root_segment, /*ordinal*/ 11),
            outer_reference,
            developer_line("A", /*ordinal*/ 13),
            developer_line("B", /*ordinal*/ 14),
            developer_line("C", /*ordinal*/ 15),
            compacted_developer_lines(&["A", "B", "C"], /*ordinal*/ 16),
        ],
    )?;

    let lines = materialize_rollout_lines(home.path(), root_path.as_path()).await?;
    let texts = developer_texts(lines.as_slice());
    assert_eq!(texts.iter().filter(|text| **text == "A").count(), 2);
    assert_eq!(texts.iter().filter(|text| **text == "B").count(), 4);
    assert_eq!(texts.iter().filter(|text| **text == "C").count(), 6);
    Ok(())
}

#[tokio::test]
async fn bounded_materializer_reuses_validated_immutable_segments() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let oldest_segment = SegmentId::new();
    let middle_segment = SegmentId::new();
    let root_segment = SegmentId::new();
    let oldest_path = home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(oldest_segment.to_string())
        .join("oldest.jsonl");
    let middle_path = home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(middle_segment.to_string())
        .join("middle.jsonl");
    let root_path = home.path().join("root.jsonl");

    write_rollout(
        oldest_path.as_path(),
        &[
            meta_line(thread_id, oldest_segment, /*ordinal*/ 0),
            agent_line("oldest", /*ordinal*/ 1),
        ],
    )?;
    write_rollout(
        middle_path.as_path(),
        &[
            meta_line(thread_id, middle_segment, /*ordinal*/ 2),
            reference_line(oldest_path, thread_id, oldest_segment, /*ordinal*/ 3),
            agent_line("middle", /*ordinal*/ 4),
        ],
    )?;
    write_rollout(
        root_path.as_path(),
        &[
            meta_line(thread_id, root_segment, /*ordinal*/ 5),
            reference_line(middle_path, thread_id, middle_segment, /*ordinal*/ 6),
            agent_line("root", /*ordinal*/ 7),
        ],
    )?;

    let mut materializer = BoundedRolloutMaterializer::new(home.path(), root_path.as_path());
    let first = materializer
        .materialize(/*ordinary_reference_limit*/ 1)
        .await?;
    assert_eq!(event_messages(&first.lines), vec!["middle", "root"]);

    let second = materializer
        .materialize(/*ordinary_reference_limit*/ 2)
        .await?;
    assert_eq!(
        event_messages(&second.lines),
        vec!["oldest", "middle", "root"]
    );
    assert_eq!(materializer.cache.entries.len(), 2);
    assert_eq!(materializer.cache.hits, 1);

    let uncached = materialize_bounded_rollout_lines(
        home.path(),
        root_path.as_path(),
        /*ordinary_reference_limit*/ 2,
    )
    .await?;
    assert_eq!(
        serde_json::to_value(&second.lines)?,
        serde_json::to_value(&uncached.lines)?
    );
    assert_eq!(second.has_older_reference, uncached.has_older_reference);
    Ok(())
}

#[tokio::test]
async fn bounded_materializer_fully_reads_each_cached_segment_once_during_geometric_expansion()
-> io::Result<()> {
    const MAX_EXPECTED_CACHED_SEGMENTS: usize = 128;

    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let segment_count = MAX_EXPECTED_CACHED_SEGMENTS + 32;
    let root_path = write_padded_immutable_rollout_chain(
        home.path(),
        thread_id,
        segment_count,
        /*padding_bytes*/ 0,
    )?;
    let mut materializer = BoundedRolloutMaterializer::new(home.path(), root_path.as_path());

    for ordinary_reference_limit in [2, 4, 8, 16, 32, 64, MAX_EXPECTED_CACHED_SEGMENTS] {
        let lines = materializer.materialize(ordinary_reference_limit).await?;
        assert_eq!(event_messages(&lines.lines).len(), ordinary_reference_limit);
        assert_eq!(
            materializer.cache.full_rollout_reads,
            ordinary_reference_limit
        );
        assert_eq!(materializer.cache.entries.len(), ordinary_reference_limit);
        assert!(materializer.cache.source_bytes <= MAX_REQUEST_CACHE_BYTES);
    }

    let complete = materializer.materialize(segment_count).await?;
    assert_eq!(event_messages(&complete.lines).len(), segment_count);
    assert_eq!(materializer.cache.full_rollout_reads, segment_count);
    assert_eq!(
        materializer.cache.entries.len(),
        MAX_EXPECTED_CACHED_SEGMENTS
    );
    let uncached_segment_count = segment_count - MAX_EXPECTED_CACHED_SEGMENTS;

    let repeated = materializer.materialize(segment_count).await?;
    assert_eq!(event_messages(&repeated.lines).len(), segment_count);
    assert_eq!(
        materializer.cache.full_rollout_reads,
        segment_count + uncached_segment_count
    );
    assert!(materializer.cache.hits >= MAX_EXPECTED_CACHED_SEGMENTS);
    Ok(())
}

#[tokio::test]
async fn bounded_materializer_never_caches_more_than_eight_mebibytes_of_rollout_source()
-> io::Result<()> {
    const SEGMENT_COUNT: usize = 20;
    const MAX_EXPECTED_RETAINED_SOURCE_BYTES: usize = 8 * 1024 * 1024;

    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let root_path = write_padded_immutable_rollout_chain(
        home.path(),
        thread_id,
        SEGMENT_COUNT,
        MAX_REQUEST_CACHE_BYTES / 16,
    )?;
    let mut materializer = BoundedRolloutMaterializer::new(home.path(), root_path.as_path());

    let materialized = materializer.materialize(SEGMENT_COUNT).await?;
    assert_eq!(event_messages(&materialized.lines).len(), SEGMENT_COUNT);
    assert_eq!(materializer.cache.full_rollout_reads, SEGMENT_COUNT);
    assert!(materializer.cache.entries.len() < SEGMENT_COUNT);
    assert!(materializer.cache.source_bytes <= MAX_EXPECTED_RETAINED_SOURCE_BYTES);
    assert_eq!(
        materializer.cache.source_bytes,
        materializer
            .cache
            .entries
            .values()
            .map(|entry| usize::try_from(entry.fingerprint.len).expect("rollout size fits usize"))
            .sum::<usize>()
    );

    let uncached_segment_count = SEGMENT_COUNT - materializer.cache.entries.len();
    materializer.materialize(SEGMENT_COUNT).await?;
    assert_eq!(
        materializer.cache.full_rollout_reads,
        SEGMENT_COUNT + uncached_segment_count
    );
    assert!(materializer.cache.source_bytes <= MAX_EXPECTED_RETAINED_SOURCE_BYTES);
    Ok(())
}

#[tokio::test]
async fn bounded_materializer_does_not_retain_or_reuse_oversized_immutable_segments()
-> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let root_path = write_padded_immutable_rollout_chain(
        home.path(),
        thread_id,
        /*segment_count*/ 1,
        MAX_REQUEST_CACHE_BYTES,
    )?;
    let mut materializer = BoundedRolloutMaterializer::new(home.path(), root_path.as_path());

    for expected_full_rollout_reads in [1, 2] {
        let lines = materializer
            .materialize(/*ordinary_reference_limit*/ 1)
            .await?;
        assert_eq!(event_messages(&lines.lines), vec!["segment-0"]);
        assert_eq!(
            materializer.cache.full_rollout_reads,
            expected_full_rollout_reads
        );
        assert_eq!(materializer.cache.source_bytes, 0);
        assert!(materializer.cache.entries.is_empty());
        assert_eq!(materializer.cache.hits, 0);
    }
    Ok(())
}

#[tokio::test]
async fn model_context_materialization_recovers_all_uncompacted_legacy_segments() -> io::Result<()>
{
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let mut previous_segment = None;
    let mut latest_lines = Vec::new();

    for index in 0_u64..10 {
        let segment_id = SegmentId::new();
        let path = home
            .path()
            .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
            .join(thread_id.to_string())
            .join(segment_id.to_string())
            .join("segment.jsonl");
        let mut lines = vec![meta_line(thread_id, segment_id, index * 3)];
        if let Some((previous_path, previous_id)) = previous_segment {
            lines.push(reference_line(
                previous_path,
                thread_id,
                previous_id,
                index * 3 + 1,
            ));
        }
        lines.push(agent_line(&format!("segment-{index}"), index * 3 + 2));
        write_rollout(path.as_path(), &lines)?;
        previous_segment = Some((path, segment_id));
        latest_lines = lines;
    }

    let items = materialize_model_context_rollout_items_from(home.path(), latest_lines).await?;
    let messages = items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::EventMsg(EventMsg::AgentMessage(event)) => Some(event.message.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(
        messages,
        (0..10)
            .map(|index| format!("segment-{index}"))
            .collect::<Vec<_>>()
    );
    assert!(matches!(items.first(), Some(RolloutItem::SessionMeta(_))));
    Ok(())
}

#[tokio::test]
async fn model_context_checkpoint_does_not_open_obsolete_legacy_predecessor() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let current_segment = SegmentId::new();
    let missing_segment = SegmentId::new();
    let missing_path = home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(missing_segment.to_string())
        .join("missing.jsonl");
    let mut lines = vec![
        meta_line(thread_id, current_segment, /*ordinal*/ 0),
        reference_line(missing_path, thread_id, missing_segment, /*ordinal*/ 1),
    ];
    lines.extend(checkpoint_lines(
        "latest checkpoint",
        /*first_ordinal*/ 2,
    ));

    let items = materialize_model_context_rollout_items_from(home.path(), lines).await?;

    assert!(matches!(items.first(), Some(RolloutItem::SessionMeta(_))));
    assert!(items.iter().any(|item| {
        matches!(item, RolloutItem::Compacted(compacted) if compacted.message == "latest checkpoint")
    }));
    Ok(())
}

#[tokio::test]
async fn model_context_missing_immediate_legacy_predecessor_remains_an_error() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let root_segment = SegmentId::new();
    let missing_segment = SegmentId::new();
    let missing_path = home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(missing_segment.to_string())
        .join("missing.jsonl");
    let lines = vec![
        meta_line(thread_id, root_segment, /*ordinal*/ 0),
        reference_line(missing_path, thread_id, missing_segment, /*ordinal*/ 1),
        agent_line("recent", /*ordinal*/ 2),
    ];

    let error = materialize_model_context_rollout_items_from(home.path(), lines)
        .await
        .expect_err("an immediately referenced immutable segment cannot be ignored");

    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    Ok(())
}

#[tokio::test]
async fn model_context_nested_fork_cutoff_cannot_accept_partial_checkpoint() -> io::Result<()> {
    let home = TempDir::new()?;
    let root_thread = ThreadId::new();
    let parent_thread = ThreadId::new();
    let root_segment = SegmentId::new();
    let fork_segment = SegmentId::new();
    let parent_segment = SegmentId::new();
    let oldest_segment = SegmentId::new();
    let segment_path = |thread_id: ThreadId, segment_id: SegmentId| {
        home.path()
            .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
            .join(thread_id.to_string())
            .join(segment_id.to_string())
            .join("segment.jsonl")
    };
    let oldest_path = segment_path(parent_thread, oldest_segment);
    let parent_path = segment_path(parent_thread, parent_segment);
    let fork_path = segment_path(root_thread, fork_segment);

    write_rollout(
        oldest_path.as_path(),
        &[
            meta_line(parent_thread, oldest_segment, /*ordinal*/ 0),
            user_event_line("oldest-user", /*ordinal*/ 1),
            user_event_line("cutoff-user", /*ordinal*/ 2),
        ],
    )?;
    write_rollout(
        parent_path.as_path(),
        &[
            meta_line(parent_thread, parent_segment, /*ordinal*/ 3),
            reference_line(
                oldest_path,
                parent_thread,
                oldest_segment,
                /*ordinal*/ 4,
            ),
            turn_started_line("excluded-turn", /*ordinal*/ 5),
            user_event_line("excluded-user", /*ordinal*/ 6),
            turn_context_line(home.path(), "excluded-turn", /*ordinal*/ 7),
            compacted_line("excluded checkpoint", /*ordinal*/ 8),
            turn_complete_line("excluded-turn", /*ordinal*/ 9),
        ],
    )?;
    let mut fork_reference = reference_line(
        parent_path,
        parent_thread,
        parent_segment,
        /*ordinal*/ 10,
    );
    let RolloutItem::RolloutReference(reference) = &mut fork_reference.item else {
        unreachable!("fixture is a rollout reference");
    };
    reference.nth_user_message = Some(1);
    write_rollout(
        fork_path.as_path(),
        &[
            meta_line(root_thread, fork_segment, /*ordinal*/ 11),
            fork_reference,
        ],
    )?;
    let lines = vec![
        meta_line(root_thread, root_segment, /*ordinal*/ 12),
        reference_line(fork_path, root_thread, fork_segment, /*ordinal*/ 13),
    ];

    let expected = super::materialize_rollout_lines_from(home.path(), lines.clone())
        .await?
        .into_iter()
        .map(|line| line.item)
        .collect::<Vec<_>>();
    let actual = materialize_model_context_rollout_items_from(home.path(), lines).await?;

    assert_eq!(
        serde_json::to_value(&actual)?,
        serde_json::to_value(expected)?
    );
    assert!(!actual.iter().any(|item| {
        matches!(item, RolloutItem::Compacted(compacted) if compacted.message == "excluded checkpoint")
    }));
    Ok(())
}

#[tokio::test]
async fn model_context_cross_thread_checkpoint_does_not_open_obsolete_parent() -> io::Result<()> {
    let home = TempDir::new()?;
    let child_thread = ThreadId::new();
    let parent_thread = ThreadId::new();
    let child_segment = SegmentId::new();
    let parent_segment = SegmentId::new();
    let missing_segment = SegmentId::new();
    let segment_path = |thread_id: ThreadId, segment_id: SegmentId| {
        home.path()
            .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
            .join(thread_id.to_string())
            .join(segment_id.to_string())
            .join("segment.jsonl")
    };
    let parent_path = segment_path(parent_thread, parent_segment);
    let missing_path = segment_path(parent_thread, missing_segment);
    let mut parent_lines = vec![
        meta_line(parent_thread, parent_segment, /*ordinal*/ 0),
        reference_line(
            missing_path,
            parent_thread,
            missing_segment,
            /*ordinal*/ 1,
        ),
    ];
    parent_lines.extend(checkpoint_lines(
        "parent checkpoint",
        /*first_ordinal*/ 2,
    ));
    write_rollout(parent_path.as_path(), &parent_lines)?;
    let lines = vec![
        meta_line(child_thread, child_segment, /*ordinal*/ 7),
        reference_line(
            parent_path,
            parent_thread,
            parent_segment,
            /*ordinal*/ 8,
        ),
    ];

    let items = materialize_model_context_rollout_items_from(home.path(), lines).await?;

    assert!(matches!(
        items.first(),
        Some(RolloutItem::SessionMeta(meta)) if meta.meta.id == child_thread
    ));
    assert!(items.iter().any(|item| {
        matches!(item, RolloutItem::Compacted(compacted) if compacted.message == "parent checkpoint")
    }));
    Ok(())
}

#[tokio::test]
async fn model_context_nested_replacement_filter_preserves_compaction_boundary() -> io::Result<()> {
    let home = TempDir::new()?;
    let child_thread = ThreadId::new();
    let parent_thread = ThreadId::new();
    let child_segment = SegmentId::new();
    let parent_segment = SegmentId::new();
    let older_segment = SegmentId::new();
    let segment_path = |thread_id: ThreadId, segment_id: SegmentId| {
        home.path()
            .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
            .join(thread_id.to_string())
            .join(segment_id.to_string())
            .join("segment.jsonl")
    };
    let older_path = segment_path(parent_thread, older_segment);
    let parent_path = segment_path(parent_thread, parent_segment);
    write_rollout(
        older_path.as_path(),
        &[
            meta_line(parent_thread, older_segment, /*ordinal*/ 0),
            user_line("older parent user", /*ordinal*/ 1),
        ],
    )?;
    let mut checkpoint = compacted_line("filtered checkpoint", /*ordinal*/ 6);
    let RolloutItem::Compacted(compacted) = &mut checkpoint.item else {
        unreachable!("fixture is a compaction checkpoint");
    };
    compacted.replacement_history = Some(vec![
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "filtered developer".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        }
        .into(),
    ]);
    write_rollout(
        parent_path.as_path(),
        &[
            meta_line(parent_thread, parent_segment, /*ordinal*/ 2),
            reference_line(older_path, parent_thread, older_segment, /*ordinal*/ 3),
            turn_started_line("parent-turn", /*ordinal*/ 4),
            user_event_line("parent user", /*ordinal*/ 5),
            turn_context_line(home.path(), "parent-turn", /*ordinal*/ 6),
            checkpoint,
            turn_complete_line("parent-turn", /*ordinal*/ 7),
        ],
    )?;
    let mut parent_reference = reference_line(
        parent_path,
        parent_thread,
        parent_segment,
        /*ordinal*/ 8,
    );
    let RolloutItem::RolloutReference(reference) = &mut parent_reference.item else {
        unreachable!("fixture is a rollout reference");
    };
    reference.compacted_replacement_history_filter_texts =
        Some(vec!["filtered developer".to_string()]);
    let lines = vec![
        meta_line(child_thread, child_segment, /*ordinal*/ 9),
        parent_reference,
    ];

    let items = materialize_model_context_rollout_items_from(home.path(), lines).await?;
    let checkpoint = items
        .iter()
        .find_map(|item| match item {
            RolloutItem::Compacted(compacted) => Some(compacted),
            _ => None,
        })
        .expect("filtered checkpoint remains");

    assert_eq!(
        checkpoint.replacement_history.as_deref(),
        Some([].as_slice())
    );
    Ok(())
}

#[tokio::test]
async fn model_context_rollback_requires_complete_legacy_ancestry() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let oldest_segment = SegmentId::new();
    let current_segment = SegmentId::new();
    let oldest_path = home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(oldest_segment.to_string())
        .join("segment.jsonl");
    write_rollout(
        oldest_path.as_path(),
        &[
            meta_line(thread_id, oldest_segment, /*ordinal*/ 0),
            user_line("pre-rollback user", /*ordinal*/ 1),
        ],
    )?;
    let lines = vec![
        meta_line(thread_id, current_segment, /*ordinal*/ 2),
        reference_line(oldest_path, thread_id, oldest_segment, /*ordinal*/ 3),
        turn_started_line("recent-turn", /*ordinal*/ 4),
        user_event_line("recent user", /*ordinal*/ 5),
        turn_context_line(home.path(), "recent-turn", /*ordinal*/ 6),
        compacted_line("checkpoint after rollback", /*ordinal*/ 7),
        RolloutLine {
            timestamp: "2026-07-13T00:00:01Z".to_string(),
            ordinal: Some(8),
            item: RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
        },
        turn_complete_line("recent-turn", /*ordinal*/ 9),
    ];

    let items = materialize_model_context_rollout_items_from(home.path(), lines).await?;

    assert!(items.iter().any(|item| {
        matches!(item, RolloutItem::ResponseItem(ResponseItemEnvelope {
            item: ResponseItem::Message { content, .. },
            ..
        })
            if matches!(content.as_slice(), [ContentItem::InputText { text }] if text == "pre-rollback user"))
    }));
    Ok(())
}

#[tokio::test]
async fn bounded_materializer_rejects_modified_cached_immutable_segment() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let referenced_segment = SegmentId::new();
    let root_segment = SegmentId::new();
    let referenced_path = home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(referenced_segment.to_string())
        .join("referenced.jsonl");
    let root_path = home.path().join("root.jsonl");

    write_rollout(
        referenced_path.as_path(),
        &[
            meta_line(thread_id, referenced_segment, /*ordinal*/ 0),
            agent_line("immutable", /*ordinal*/ 1),
        ],
    )?;
    write_rollout(
        root_path.as_path(),
        &[
            meta_line(thread_id, root_segment, /*ordinal*/ 2),
            reference_line(
                referenced_path.clone(),
                thread_id,
                referenced_segment,
                /*ordinal*/ 3,
            ),
        ],
    )?;

    let mut materializer = BoundedRolloutMaterializer::new(home.path(), root_path.as_path());
    materializer
        .materialize(/*ordinary_reference_limit*/ 1)
        .await?;
    let referenced_meta = serde_json::to_string(&meta_line(
        thread_id,
        referenced_segment,
        /*ordinal*/ 0,
    ))?;
    fs::write(referenced_path, format!("{referenced_meta}\n{{malformed\n"))?;

    let error = materializer
        .materialize(/*ordinary_reference_limit*/ 1)
        .await
        .err()
        .expect("modified immutable rollout must not use cached records");
    assert!(error.to_string().contains("invalid record"));
    Ok(())
}

#[tokio::test]
async fn bounded_materializer_reapplies_reference_filters_to_cached_records() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let referenced_segment = SegmentId::new();
    let root_segment = SegmentId::new();
    let referenced_path = home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(referenced_segment.to_string())
        .join("referenced.jsonl");
    let root_path = home.path().join("root.jsonl");

    write_rollout(
        referenced_path.as_path(),
        &[
            meta_line(thread_id, referenced_segment, /*ordinal*/ 0),
            RolloutLine {
                timestamp: "2026-07-13T00:00:01Z".to_string(),
                ordinal: Some(1),
                item: RolloutItem::ResponseItem(
                    ResponseItem::Message {
                        id: None,
                        role: "developer".to_string(),
                        content: vec![ContentItem::InputText {
                            text: "remove only from the filtered reference".to_string(),
                        }],
                        phase: None,
                        internal_chat_message_metadata_passthrough: None,
                    }
                    .into(),
                ),
            },
            agent_line("retained", /*ordinal*/ 2),
        ],
    )?;

    let mut filtered_reference = reference_line(
        referenced_path.clone(),
        thread_id,
        referenced_segment,
        /*ordinal*/ 4,
    );
    let RolloutItem::RolloutReference(reference) = &mut filtered_reference.item else {
        unreachable!();
    };
    reference.compacted_replacement_history_filter_texts =
        Some(vec!["remove only from the filtered reference".to_string()]);
    write_rollout(
        root_path.as_path(),
        &[
            meta_line(thread_id, root_segment, /*ordinal*/ 3),
            filtered_reference,
            reference_line(
                referenced_path,
                thread_id,
                referenced_segment,
                /*ordinal*/ 5,
            ),
        ],
    )?;

    let mut materializer = BoundedRolloutMaterializer::new(home.path(), root_path.as_path());
    let materialized = materializer
        .materialize(/*ordinary_reference_limit*/ 1)
        .await?;
    let developer_messages = materialized
        .lines
        .iter()
        .filter(|line| {
            matches!(
                &line.item,
                RolloutItem::ResponseItem(ResponseItemEnvelope {
                    item: ResponseItem::Message { role, .. },
                    ..
                }) if role == "developer"
            )
        })
        .count();
    assert_eq!(developer_messages, 1);
    assert_eq!(
        event_messages(&materialized.lines),
        vec!["retained", "retained"]
    );
    assert_eq!(materializer.cache.hits, 1);
    Ok(())
}

#[tokio::test]
async fn bounded_materializer_does_not_cache_unfrozen_rollout_files() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let referenced_segment = SegmentId::new();
    let root_segment = SegmentId::new();
    let referenced_path = active_rollout_path(home.path(), thread_id, "2026-07-13T00-00-00");
    let root_path = home.path().join("root.jsonl");

    write_rollout(
        referenced_path.as_path(),
        &[
            meta_line(thread_id, referenced_segment, /*ordinal*/ 0),
            agent_line("unfrozen", /*ordinal*/ 1),
        ],
    )?;
    write_rollout(
        root_path.as_path(),
        &[
            meta_line(thread_id, root_segment, /*ordinal*/ 2),
            reference_line(
                referenced_path,
                thread_id,
                referenced_segment,
                /*ordinal*/ 3,
            ),
        ],
    )?;

    let mut materializer = BoundedRolloutMaterializer::new(home.path(), root_path.as_path());
    materializer
        .materialize(/*ordinary_reference_limit*/ 1)
        .await?;
    materializer
        .materialize(/*ordinary_reference_limit*/ 1)
        .await?;
    assert!(materializer.cache.entries.is_empty());
    assert_eq!(materializer.cache.hits, 0);
    Ok(())
}

#[tokio::test]
async fn nth_user_message_excludes_corresponding_turn_started_and_suffix() -> io::Result<()> {
    let home = TempDir::new()?;
    let source_thread = ThreadId::new();
    let source_segment = SegmentId::new();
    let source_path = immutable_segment_path(
        home.path(),
        source_thread,
        source_segment,
        "2026-07-13T00-00-00",
    );
    write_rollout(
        source_path.as_path(),
        &[
            meta_line(source_thread, source_segment, /*ordinal*/ 0),
            agent_line("inherited", /*ordinal*/ 1),
            turn_started_line("retained-turn", /*ordinal*/ 2),
            user_line("retained user", /*ordinal*/ 3),
            agent_line("retained answer", /*ordinal*/ 4),
            turn_complete_line("retained-turn", /*ordinal*/ 5),
            turn_started_line("turn-at-boundary", /*ordinal*/ 6),
            user_line("fork boundary", /*ordinal*/ 7),
            agent_line("after boundary", /*ordinal*/ 8),
        ],
    )?;

    let root_thread = ThreadId::new();
    let root_segment = SegmentId::new();
    let root_path = home.path().join("root.jsonl");
    let mut reference_line = reference_line(
        source_path,
        source_thread,
        source_segment,
        /*ordinal*/ 12,
    );
    let RolloutItem::RolloutReference(reference) = &mut reference_line.item else {
        unreachable!();
    };
    reference.nth_user_message = Some(1);
    write_rollout(
        root_path.as_path(),
        &[
            meta_line(root_thread, root_segment, /*ordinal*/ 11),
            reference_line,
            agent_line("local", /*ordinal*/ 13),
        ],
    )?;

    let lines = materialize_rollout_lines(home.path(), root_path.as_path()).await?;
    let RolloutItem::SessionMeta(meta) = &lines[0].item else {
        panic!("expected root session metadata");
    };
    assert_eq!(meta.meta.id, root_thread);
    assert_eq!(
        event_messages(&lines),
        vec!["inherited", "retained answer", "local"]
    );
    assert!(lines.iter().all(|line| !matches!(
        &line.item,
        RolloutItem::EventMsg(EventMsg::TurnStarted(event))
            if event.turn_id == "turn-at-boundary"
    )));
    assert_eq!(
        lines.iter().map(|line| line.ordinal).collect::<Vec<_>>(),
        vec![
            Some(11),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(13)
        ]
    );
    assert!(
        lines
            .iter()
            .all(|line| !matches!(&line.item, RolloutItem::RolloutReference(_)))
    );
    Ok(())
}

#[tokio::test]
async fn nth_user_message_uses_completed_user_items_instead_of_contextual_user_items()
-> io::Result<()> {
    let home = TempDir::new()?;
    let source_thread = ThreadId::new();
    let source_segment = SegmentId::new();
    let source_path = immutable_segment_path(
        home.path(),
        source_thread,
        source_segment,
        "2026-07-13T00-00-00",
    );
    write_rollout(
        source_path.as_path(),
        &[
            meta_line(source_thread, source_segment, /*ordinal*/ 0),
            user_line(
                "<environment_context>context</environment_context>",
                /*ordinal*/ 1,
            ),
            turn_started_line("retained-turn", /*ordinal*/ 2),
            completed_user_item_line(
                source_thread,
                "retained-turn",
                "retained user",
                /*ordinal*/ 3,
            ),
            user_line("retained user", /*ordinal*/ 4),
            turn_complete_line("retained-turn", /*ordinal*/ 5),
            turn_started_line("turn-at-boundary", /*ordinal*/ 6),
            completed_user_item_line(
                source_thread,
                "turn-at-boundary",
                "fork boundary",
                /*ordinal*/ 7,
            ),
            user_line("fork boundary", /*ordinal*/ 8),
        ],
    )?;

    let root_thread = ThreadId::new();
    let root_segment = SegmentId::new();
    let root_path = home.path().join("root.jsonl");
    let mut reference_line = reference_line(
        source_path,
        source_thread,
        source_segment,
        /*ordinal*/ 10,
    );
    let RolloutItem::RolloutReference(reference) = &mut reference_line.item else {
        unreachable!();
    };
    reference.nth_user_message = Some(1);
    write_rollout(
        root_path.as_path(),
        &[
            meta_line(root_thread, root_segment, /*ordinal*/ 9),
            reference_line,
        ],
    )?;

    let lines = materialize_rollout_lines(home.path(), root_path.as_path()).await?;
    assert!(lines.iter().any(|line| {
        matches!(
            &line.item,
            RolloutItem::ResponseItem(ResponseItemEnvelope {
                item: ResponseItem::Message { role, content, .. },
                ..
            })
                if role == "user"
                    && matches!(
                        content.as_slice(),
                        [ContentItem::InputText { text }] if text == "retained user"
                    )
        )
    }));
    assert!(lines.iter().all(|line| !matches!(
        &line.item,
        RolloutItem::EventMsg(EventMsg::TurnStarted(event))
            if event.turn_id == "turn-at-boundary"
    )));
    Ok(())
}

#[tokio::test]
async fn thread_summary_uses_inherited_reference_preview() -> io::Result<()> {
    let home = TempDir::new()?;
    let source_thread = ThreadId::new();
    let source_segment = SegmentId::new();
    let source_path = home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(source_thread.to_string())
        .join(source_segment.to_string())
        .join("source.jsonl");
    write_rollout(
        source_path.as_path(),
        &[
            meta_line(source_thread, source_segment, /*ordinal*/ 0),
            user_event_line("inherited preview", /*ordinal*/ 1),
        ],
    )?;

    let child_thread = ThreadId::new();
    let child_segment = SegmentId::new();
    let child_path = home
        .path()
        .join(crate::SESSIONS_SUBDIR)
        .join("2026/07/13")
        .join(rollout_file_name("2026-07-13T00-00-01", child_thread));
    write_rollout(
        child_path.as_path(),
        &[
            meta_line(child_thread, child_segment, /*ordinal*/ 2),
            reference_line(
                source_path,
                source_thread,
                source_segment,
                /*ordinal*/ 3,
            ),
        ],
    )?;

    let summary = crate::list::read_thread_item_from_rollout(child_path.clone())
        .await
        .expect("referenced thread should remain discoverable");
    assert_eq!(summary.preview.as_deref(), Some("inherited preview"));
    assert_eq!(
        summary.first_user_message.as_deref(),
        Some("inherited preview")
    );
    let indexed_summary =
        crate::list::read_thread_item_from_rollout_with_indexed_preview(child_path)
            .await
            .expect("indexed summaries must retain inherited rollout previews");
    assert_eq!(indexed_summary, summary);
    Ok(())
}

#[tokio::test]
async fn materialization_accepts_legacy_turn_context_file_uri_cwd() -> io::Result<()> {
    let home = TempDir::new()?;
    let source_thread = ThreadId::new();
    let source_segment = SegmentId::new();
    let source_path = immutable_segment_path(
        home.path(),
        source_thread,
        source_segment,
        "2026-07-13T00-00-00",
    );
    let source_meta = serde_json::to_string(&meta_line(
        source_thread,
        source_segment,
        /*ordinal*/ 0,
    ))?;
    let (legacy_cwd, expected_cwd) = if cfg!(windows) {
        ("file:///C:/tmp", Path::new(r"C:\tmp"))
    } else {
        ("file:///tmp", Path::new("/tmp"))
    };
    let legacy_turn_context = json!({
        "timestamp": "2026-07-13T00:00:01Z",
        "ordinal": 1,
        "type": "turn_context",
        "payload": {
            "cwd": legacy_cwd,
            "approval_policy": "never",
            "sandbox_policy": { "type": "danger-full-access" },
            "model": "gpt-5",
            "summary": "auto"
        }
    });
    fs::create_dir_all(source_path.parent().expect("immutable segment directory"))?;
    fs::write(
        source_path.as_path(),
        format!("{source_meta}\n{legacy_turn_context}\n"),
    )?;

    let root_thread = ThreadId::new();
    let root_segment = SegmentId::new();
    let root_path = home.path().join("root.jsonl");
    write_rollout(
        root_path.as_path(),
        &[
            meta_line(root_thread, root_segment, /*ordinal*/ 2),
            reference_line(
                source_path,
                source_thread,
                source_segment,
                /*ordinal*/ 3,
            ),
        ],
    )?;

    let lines = materialize_rollout_lines(home.path(), root_path.as_path()).await?;
    let turn_context = lines
        .iter()
        .find_map(|line| match &line.item {
            RolloutItem::TurnContext(turn_context) => Some(turn_context),
            _ => None,
        })
        .expect("legacy turn context should be preserved");
    assert_eq!(turn_context.cwd.as_path(), expected_cwd);
    assert_eq!(
        serde_json::to_value(turn_context)?["cwd"],
        json!(expected_cwd)
    );
    Ok(())
}

#[tokio::test]
async fn materialization_skips_torn_ordinary_records_in_active_legacy_root() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let source_segment = SegmentId::new();
    let source_path = immutable_segment_path(
        home.path(),
        thread_id,
        source_segment,
        "2026-07-13T00-00-00",
    );
    write_rollout(
        source_path.as_path(),
        &[
            meta_line(thread_id, source_segment, /*ordinal*/ 0),
            agent_line("immutable history", /*ordinal*/ 1),
        ],
    )?;

    let active_segment = SegmentId::new();
    let active_path = home.path().join("active.jsonl");
    let active_records = [
        serde_json::to_string(&meta_line(thread_id, active_segment, /*ordinal*/ 2))?,
        serde_json::to_string(&reference_line(
            source_path,
            thread_id,
            source_segment,
            /*ordinal*/ 3,
        ))?,
        serde_json::to_string(&agent_line("before disk-full write", /*ordinal*/ 4))?,
        r#"{"timestamp":"2026-07-30T07:00:17.718Z","type":"response_item","payload":{"type":"function_call""#
            .to_string(),
        serde_json::to_string(&agent_line("after disk-full recovery", /*ordinal*/ 6))?,
    ];
    fs::write(
        active_path.as_path(),
        format!("{}\n", active_records.join("\n")),
    )?;

    let expected_messages = vec![
        "immutable history",
        "before disk-full write",
        "after disk-full recovery",
    ];
    let complete = materialize_rollout_lines(home.path(), active_path.as_path()).await?;
    assert_eq!(event_messages(&complete), expected_messages);

    let recent = materialize_recent_rollout_lines(home.path(), active_path.as_path()).await?;
    assert_eq!(event_messages(&recent), expected_messages);

    let bounded = materialize_bounded_rollout_lines(
        home.path(),
        active_path.as_path(),
        /*ordinary_reference_limit*/ 1,
    )
    .await?;
    assert_eq!(event_messages(&bounded.lines), expected_messages);
    assert!(!bounded.has_older_reference);
    Ok(())
}

#[tokio::test]
async fn materialization_rejects_torn_ordinary_records_in_paginated_root() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let root_path = home.path().join("paginated.jsonl");
    let mut root_meta = meta_line(thread_id, segment_id, /*ordinal*/ 0);
    let RolloutItem::SessionMeta(session_meta) = &mut root_meta.item else {
        unreachable!();
    };
    session_meta.meta.history_mode = ThreadHistoryMode::Paginated;
    let root_records = [
        serde_json::to_string(&root_meta)?,
        r#"{"timestamp":"2026-07-30T07:00:17.718Z","type":"response_item""#.to_string(),
        serde_json::to_string(&agent_line("after malformed record", /*ordinal*/ 2))?,
    ];
    fs::write(
        root_path.as_path(),
        format!("{}\n", root_records.join("\n")),
    )?;

    let complete_error = materialize_rollout_lines(home.path(), root_path.as_path())
        .await
        .err()
        .expect("paginated active rollouts must remain strict");
    assert!(complete_error.to_string().contains("invalid record"));

    let recent_error = materialize_recent_rollout_lines(home.path(), root_path.as_path())
        .await
        .err()
        .expect("paginated recent rollout reads must remain strict");
    assert!(recent_error.to_string().contains("invalid record"));

    let bounded_error = materialize_bounded_rollout_lines(
        home.path(),
        root_path.as_path(),
        /*ordinary_reference_limit*/ 1,
    )
    .await
    .err()
    .expect("paginated bounded rollout reads must remain strict");
    assert!(bounded_error.to_string().contains("invalid record"));
    Ok(())
}

#[tokio::test]
async fn compatibility_reference_expands_native_history_base_with_remaining_depth() -> io::Result<()>
{
    for same_thread in [false, true] {
        let home = TempDir::new()?;
        let (native_path, messages) =
            write_native_history_base_chain(home.path(), /*predecessor_count*/ 8)?;
        let parent = crate::read_session_meta_line(&native_path).await?.meta.id;
        let child = if same_thread { parent } else { ThreadId::new() };
        let path = active_rollout_path(home.path(), child, "2026-07-13T00-00-02");
        write_rollout(
            &path,
            &[
                meta_line_with_segment(child, /*segment_id*/ None, /*ordinal*/ 0),
                legacy_reference_line(native_path, parent, /*ordinal*/ 1),
            ],
        )?;
        let complete = materialize_rollout_lines(home.path(), &path).await?;
        assert_eq!(
            event_messages(&complete),
            messages.iter().map(String::as_str).collect::<Vec<_>>()
        );
        let bounded = materialize_bounded_rollout_lines(
            home.path(),
            &path,
            /*ordinary_reference_limit*/ 2,
        )
        .await?;
        let retained = if same_thread { 2 } else { 3 };
        assert_eq!(
            event_messages(&bounded.lines),
            messages[messages.len() - retained..]
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        );
        assert!(bounded.has_older_reference);
    }
    Ok(())
}

#[tokio::test]
async fn mixed_reference_kinds_share_depth_and_preserve_native_predecessor_identity()
-> io::Result<()> {
    for predecessor_is_other_thread in [false, true] {
        let home = TempDir::new()?;
        let thread_id = ThreadId::new();
        let predecessor_thread = if predecessor_is_other_thread {
            ThreadId::new()
        } else {
            thread_id
        };
        let oldest = active_rollout_path(home.path(), predecessor_thread, "2026-07-13T00-00-00");
        write_rollout(
            &oldest,
            &[
                meta_line_with_segment(
                    predecessor_thread,
                    /*segment_id*/ None,
                    /*ordinal*/ 0,
                ),
                agent_line("oldest", /*ordinal*/ 1),
            ],
        )?;
        let predecessor_id = ThreadId::new();
        let predecessor = native_segment_path(
            home.path(),
            predecessor_thread,
            predecessor_id,
            "2026-07-13T00-00-01",
        );
        write_rollout(
            &predecessor,
            &[
                paginated_meta_line(
                    predecessor_thread,
                    /*ordinal*/ 2,
                    /*history_base*/ None,
                ),
                legacy_reference_line(oldest.clone(), predecessor_thread, /*ordinal*/ 3),
                agent_line("predecessor", /*ordinal*/ 4),
            ],
        )?;
        let native_id = ThreadId::new();
        let native = native_segment_path(home.path(), thread_id, native_id, "2026-07-13T00-00-02");
        write_rollout(
            &native,
            &[
                paginated_meta_line(
                    thread_id,
                    /*ordinal*/ 5,
                    Some(HistoryPosition {
                        thread_id: predecessor_id,
                        end_ordinal_exclusive: 5,
                        end_byte_offset: fs::metadata(&predecessor)?.len(),
                    }),
                ),
                agent_line("native", /*ordinal*/ 6),
            ],
        )?;
        let root = active_rollout_path(home.path(), thread_id, "2026-07-13T00-00-03");
        let mut reference = legacy_reference_line(native, thread_id, /*ordinal*/ 8);
        let RolloutItem::RolloutReference(item) = &mut reference.item else {
            unreachable!()
        };
        item.rollout_id = Some(native_id);
        write_rollout(
            &root,
            &[
                meta_line_with_segment(thread_id, /*segment_id*/ None, /*ordinal*/ 7),
                reference,
                agent_line("root", /*ordinal*/ 9),
            ],
        )?;
        assert_eq!(
            event_messages(&materialize_rollout_lines(home.path(), &root).await?),
            vec!["oldest", "predecessor", "native", "root"]
        );
        // With two predecessors permitted, the nested same-thread compatibility reference is
        // omitted even when its containing native segment belongs to a different thread.
        fs::remove_file(oldest)?;
        let bounded = materialize_bounded_rollout_lines(
            home.path(),
            &root,
            /*ordinary_reference_limit*/ 2,
        )
        .await?;
        assert_eq!(
            event_messages(&bounded.lines),
            vec!["predecessor", "native", "root"]
        );
        assert!(bounded.has_older_reference);
    }
    Ok(())
}

#[tokio::test]
async fn mixed_reference_kinds_share_fork_depth_and_cycle_detection() -> io::Result<()> {
    let home = TempDir::new()?;
    let mut thread_id = ThreadId::new();
    let mut path = active_rollout_path(home.path(), thread_id, "2026-07-13T00-00-00");
    write_rollout(
        &path,
        &[
            meta_line_with_segment(thread_id, /*segment_id*/ None, /*ordinal*/ 0),
            agent_line("oldest", /*ordinal*/ 1),
        ],
    )?;
    let mut end_ordinal_exclusive = 2;
    for index in 0..=MAX_ROLLOUT_REFERENCE_DEPTH {
        let next_thread = ThreadId::new();
        let next_path = active_rollout_path(home.path(), next_thread, "2026-07-13T00-00-00");
        let lines = if index % 2 == 0 {
            vec![
                paginated_meta_line(
                    next_thread,
                    /*ordinal*/ 0,
                    Some(HistoryPosition {
                        thread_id,
                        end_ordinal_exclusive,
                        end_byte_offset: fs::metadata(&path)?.len(),
                    }),
                ),
                agent_line("native", /*ordinal*/ 1),
            ]
        } else {
            vec![
                meta_line_with_segment(next_thread, /*segment_id*/ None, /*ordinal*/ 0),
                legacy_reference_line(path, thread_id, /*ordinal*/ 1),
                agent_line("legacy", /*ordinal*/ 2),
            ]
        };
        end_ordinal_exclusive = lines.last().unwrap().ordinal.unwrap() + 1;
        write_rollout(&next_path, &lines)?;
        thread_id = next_thread;
        path = next_path;
    }
    let error = materialize_rollout_lines(home.path(), &path)
        .await
        .err()
        .expect("mixed fork depth is bounded");
    assert!(error.to_string().contains("maximum depth"));

    let first_thread = ThreadId::new();
    let second_thread = ThreadId::new();
    let first = active_rollout_path(home.path(), first_thread, "2026-07-13T00-00-01");
    let second = active_rollout_path(home.path(), second_thread, "2026-07-13T00-00-01");
    write_rollout(
        &first,
        &[
            meta_line_with_segment(first_thread, /*segment_id*/ None, /*ordinal*/ 0),
            legacy_reference_line(second.clone(), second_thread, /*ordinal*/ 1),
        ],
    )?;
    write_rollout(
        &second,
        &[
            paginated_meta_line(
                second_thread,
                /*ordinal*/ 0,
                Some(HistoryPosition {
                    thread_id: first_thread,
                    end_ordinal_exclusive: 2,
                    end_byte_offset: fs::metadata(&first)?.len(),
                }),
            ),
            agent_line("cycle", /*ordinal*/ 1),
        ],
    )?;
    let error = materialize_rollout_lines(home.path(), &first)
        .await
        .err()
        .expect("mixed cycle is rejected");
    assert!(error.to_string().contains("cycle"));
    Ok(())
}

#[tokio::test]
async fn native_history_base_complete_and_bounded_materialization() -> io::Result<()> {
    let home = TempDir::new()?;
    let (active_path, expected_messages) =
        write_native_history_base_chain(home.path(), /*predecessor_count*/ 8)?;

    let complete = materialize_rollout_lines(home.path(), active_path.as_path()).await?;
    assert_eq!(
        event_messages(&complete),
        expected_messages
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    );

    let recent = materialize_recent_rollout_lines(home.path(), active_path.as_path()).await?;
    assert_eq!(
        event_messages(&recent),
        expected_messages[4..]
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    );

    let bounded = materialize_bounded_rollout_lines(
        home.path(),
        active_path.as_path(),
        /*ordinary_reference_limit*/ 2,
    )
    .await?;
    assert_eq!(
        event_messages(&bounded.lines),
        expected_messages[6..]
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    );
    assert!(bounded.has_older_reference);
    Ok(())
}

#[tokio::test]
async fn native_history_base_rejects_invalid_byte_and_ordinal_boundaries() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let rollout_id = ThreadId::new();
    let predecessor_path =
        native_segment_path(home.path(), thread_id, rollout_id, "2026-07-13T00-00-00");
    write_rollout(
        predecessor_path.as_path(),
        &[
            paginated_meta_line(thread_id, /*ordinal*/ 0, /*history_base*/ None),
            agent_line("predecessor", /*ordinal*/ 1),
        ],
    )?;
    let predecessor_bytes = fs::metadata(predecessor_path.as_path())?.len();
    let active_path = active_rollout_path(home.path(), thread_id, "2026-07-13T00-00-01");

    write_rollout(
        active_path.as_path(),
        &[paginated_meta_line(
            thread_id,
            /*ordinal*/ 2,
            Some(HistoryPosition {
                thread_id: rollout_id,
                end_ordinal_exclusive: 2,
                end_byte_offset: predecessor_bytes - 1,
            }),
        )],
    )?;
    let byte_error = materialize_rollout_lines(home.path(), active_path.as_path())
        .await
        .err()
        .expect("mid-record byte boundary must fail");
    assert!(byte_error.to_string().contains("record boundary"));

    write_rollout(
        active_path.as_path(),
        &[paginated_meta_line(
            thread_id,
            /*ordinal*/ 2,
            Some(HistoryPosition {
                thread_id: rollout_id,
                end_ordinal_exclusive: 99,
                end_byte_offset: predecessor_bytes,
            }),
        )],
    )?;
    let ordinal_error = materialize_rollout_lines(home.path(), active_path.as_path())
        .await
        .err()
        .expect("incorrect ordinal boundary must fail");
    assert!(ordinal_error.to_string().contains("ordinal boundary"));
    Ok(())
}

#[tokio::test]
async fn native_history_base_rejects_physical_rollout_cycles() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let rollout_id = ThreadId::new();
    let predecessor_path =
        native_segment_path(home.path(), thread_id, rollout_id, "2026-07-13T00-00-00");
    let mut predecessor_len = 0;
    loop {
        write_rollout(
            predecessor_path.as_path(),
            &[
                paginated_meta_line(
                    thread_id,
                    /*ordinal*/ 0,
                    Some(HistoryPosition {
                        thread_id: rollout_id,
                        end_ordinal_exclusive: 2,
                        end_byte_offset: predecessor_len,
                    }),
                ),
                agent_line("cyclic predecessor", /*ordinal*/ 1),
            ],
        )?;
        let next_len = fs::metadata(predecessor_path.as_path())?.len();
        if next_len == predecessor_len {
            break;
        }
        predecessor_len = next_len;
    }

    let active_path = active_rollout_path(home.path(), thread_id, "2026-07-13T00-00-01");
    write_rollout(
        active_path.as_path(),
        &[paginated_meta_line(
            thread_id,
            /*ordinal*/ 2,
            Some(HistoryPosition {
                thread_id: rollout_id,
                end_ordinal_exclusive: 2,
                end_byte_offset: predecessor_len,
            }),
        )],
    )?;

    let error = materialize_rollout_lines(home.path(), active_path.as_path())
        .await
        .err()
        .expect("physical rollout cycle must fail");
    assert!(error.to_string().contains("history_base cycle"));
    Ok(())
}

#[tokio::test]
async fn native_history_base_same_thread_chain_does_not_consume_fork_depth() -> io::Result<()> {
    let home = TempDir::new()?;
    let (active_path, expected_messages) =
        write_native_history_base_chain(home.path(), MAX_ROLLOUT_REFERENCE_DEPTH + 32)?;

    let complete = materialize_rollout_lines(home.path(), active_path.as_path()).await?;
    assert_eq!(event_messages(&complete).len(), expected_messages.len());
    assert_eq!(
        event_messages(&complete).first().copied(),
        expected_messages.first().map(String::as_str)
    );
    assert_eq!(
        event_messages(&complete).last().copied(),
        expected_messages.last().map(String::as_str)
    );
    Ok(())
}

#[tokio::test]
async fn materialization_rejects_malformed_references_in_active_legacy_root() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let root_path = home.path().join("legacy.jsonl");
    let malformed_reference = json!({
        "timestamp": "2026-07-30T07:00:17.718Z",
        "type": "rollout_reference",
        "payload": { "thread_id": thread_id }
    });
    let root_records = [
        serde_json::to_string(&meta_line(thread_id, segment_id, /*ordinal*/ 0))?,
        malformed_reference.to_string(),
    ];
    fs::write(
        root_path.as_path(),
        format!("{}\n", root_records.join("\n")),
    )?;

    let error = materialize_rollout_lines(home.path(), root_path.as_path())
        .await
        .err()
        .expect("malformed active-root references must remain strict");
    assert!(
        error
            .to_string()
            .contains("invalid rollout reference record")
    );
    Ok(())
}

#[tokio::test]
async fn resolver_rejects_missing_and_mismatched_segments() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let missing = RolloutReferenceItem {
        rollout_id: None,
        rollout_path: home.path().join("missing.jsonl"),
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(segment_id),
        max_depth: 2,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };
    assert_eq!(
        resolve_rollout_reference_path(home.path(), &missing)
            .await
            .expect_err("missing reference should fail")
            .kind(),
        io::ErrorKind::NotFound
    );

    let mismatch_path = home.path().join("mismatch.jsonl");
    write_rollout(
        mismatch_path.as_path(),
        &[meta_line(
            ThreadId::new(),
            SegmentId::new(),
            /*ordinal*/ 0,
        )],
    )?;
    let mismatch = RolloutReferenceItem {
        rollout_id: None,
        rollout_path: mismatch_path,
        ..missing
    };
    assert!(
        resolve_rollout_reference_path(home.path(), &mismatch)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn resolver_rejects_matching_recorded_path_outside_codex_home() -> io::Result<()> {
    let home = TempDir::new()?;
    let outside = TempDir::new()?;
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let outside_path = outside
        .path()
        .join(rollout_file_name("2026-07-13T00-00-00", thread_id));
    write_rollout(
        outside_path.as_path(),
        &[meta_line(thread_id, segment_id, /*ordinal*/ 0)],
    )?;
    let reference = RolloutReferenceItem {
        rollout_id: None,
        rollout_path: outside_path,
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(segment_id),
        max_depth: 2,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };

    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference)
            .await
            .expect_err("an out-of-home path must not be opened")
            .kind(),
        io::ErrorKind::NotFound
    );
    Ok(())
}

#[tokio::test]
async fn resolver_ignores_old_home_path_and_finds_current_home_identity() -> io::Result<()> {
    let home = TempDir::new()?;
    let old_home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let file_name = rollout_file_name("2026-07-13T00-00-00", thread_id);
    let old_path = old_home.path().join(file_name.as_str());
    write_rollout(
        old_path.as_path(),
        &[
            meta_line(thread_id, segment_id, /*ordinal*/ 0),
            agent_line("old home", /*ordinal*/ 1),
        ],
    )?;
    let current_path = home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(segment_id.to_string())
        .join(file_name);
    write_rollout(
        current_path.as_path(),
        &[
            meta_line(thread_id, segment_id, /*ordinal*/ 0),
            agent_line("current home", /*ordinal*/ 1),
        ],
    )?;
    let reference = RolloutReferenceItem {
        rollout_id: None,
        rollout_path: old_path,
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(segment_id),
        max_depth: 2,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };

    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference).await?,
        fs::canonicalize(current_path)?
    );
    Ok(())
}

#[tokio::test]
async fn resolver_relocates_active_segment_without_recorded_timestamp() -> io::Result<()> {
    let home = TempDir::new()?;
    let old_home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let file_name = rollout_file_name("2026-07-13T00-00-00", thread_id);
    let current_path = home
        .path()
        .join(SESSIONS_SUBDIR)
        .join("2026/07/13")
        .join(file_name.as_str());
    write_rollout(
        current_path.as_path(),
        &[meta_line(thread_id, segment_id, /*ordinal*/ 0)],
    )?;
    let reference = RolloutReferenceItem {
        rollout_id: None,
        rollout_path: old_home.path().join(file_name),
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(segment_id),
        max_depth: 2,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };

    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference).await?,
        fs::canonicalize(current_path)?
    );
    Ok(())
}

#[tokio::test]
async fn resolver_infers_legacy_fork_identity_after_home_relocation() -> io::Result<()> {
    let home = TempDir::new()?;
    let old_home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let file_name = rollout_file_name("2026-07-13T00-00-00", thread_id);
    let current_path = home
        .path()
        .join(SESSIONS_SUBDIR)
        .join("2026/07/13")
        .join(file_name.as_str());
    write_rollout(
        current_path.as_path(),
        &[legacy_meta_line(thread_id, /*ordinal*/ 0)],
    )?;
    let reference = RolloutReferenceItem {
        rollout_id: None,
        rollout_path: old_home.path().join(file_name),
        thread_id: None,
        rollout_timestamp: None,
        segment_id: None,
        max_depth: 2,
        nth_user_message: Some(usize::MAX),
        compacted_replacement_history_filter_texts: None,
    };

    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference).await?,
        fs::canonicalize(current_path)?
    );
    Ok(())
}

#[tokio::test]
async fn resolver_rejects_inferred_legacy_fork_identity_mismatch() -> io::Result<()> {
    let home = TempDir::new()?;
    let old_home = TempDir::new()?;
    let recorded_thread_id = ThreadId::new();
    let stored_thread_id = ThreadId::new();
    let file_name = rollout_file_name("2026-07-13T00-00-00", recorded_thread_id);
    let current_path = home
        .path()
        .join(SESSIONS_SUBDIR)
        .join("2026/07/13")
        .join(file_name.as_str());
    write_rollout(
        current_path.as_path(),
        &[legacy_meta_line(stored_thread_id, /*ordinal*/ 0)],
    )?;
    let reference = RolloutReferenceItem {
        rollout_id: None,
        rollout_path: old_home.path().join(file_name),
        thread_id: None,
        rollout_timestamp: None,
        segment_id: None,
        max_depth: 2,
        nth_user_message: Some(usize::MAX),
        compacted_replacement_history_filter_texts: None,
    };

    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference)
            .await
            .expect_err("filename identity must match SessionMeta")
            .kind(),
        io::ErrorKind::NotFound
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn resolver_finds_moved_identity_through_symlinked_codex_home() -> io::Result<()> {
    use std::os::unix::fs::symlink;

    let real_home = TempDir::new()?;
    let home_link_parent = TempDir::new()?;
    let old_home = TempDir::new()?;
    let codex_home = home_link_parent.path().join("codex-home");
    symlink(real_home.path(), codex_home.as_path())?;
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let file_name = rollout_file_name("2026-07-13T00-00-00", thread_id);
    let old_path = old_home.path().join(file_name.as_str());
    write_rollout(
        old_path.as_path(),
        &[meta_line(thread_id, segment_id, /*ordinal*/ 0)],
    )?;
    let current_path = real_home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(segment_id.to_string())
        .join(file_name);
    write_rollout(
        current_path.as_path(),
        &[meta_line(thread_id, segment_id, /*ordinal*/ 0)],
    )?;
    let reference = RolloutReferenceItem {
        rollout_id: None,
        rollout_path: old_path,
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(segment_id),
        max_depth: 2,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };

    assert_eq!(
        resolve_rollout_reference_path(codex_home.as_path(), &reference).await?,
        fs::canonicalize(current_path)?,
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn resolver_rejects_file_symlink_that_escapes_codex_home() -> io::Result<()> {
    use std::os::unix::fs::symlink;

    let home = TempDir::new()?;
    let outside = TempDir::new()?;
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let outside_path = outside.path().join("outside.jsonl");
    write_rollout(
        outside_path.as_path(),
        &[meta_line(thread_id, segment_id, /*ordinal*/ 0)],
    )?;
    let symlink_path = home
        .path()
        .join("sessions/2026/07/13")
        .join(rollout_file_name("2026-07-13T00-00-00", thread_id));
    fs::create_dir_all(symlink_path.parent().expect("sessions directory"))?;
    symlink(outside_path, symlink_path.as_path())?;
    let reference = RolloutReferenceItem {
        rollout_id: None,
        rollout_path: symlink_path,
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(segment_id),
        max_depth: 2,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };

    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference)
            .await
            .expect_err("an escaping file symlink must not be opened")
            .kind(),
        io::ErrorKind::NotFound
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn resolver_rejects_segment_directory_symlink_that_escapes_codex_home() -> io::Result<()> {
    use std::os::unix::fs::symlink;

    let home = TempDir::new()?;
    let outside = TempDir::new()?;
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let outside_path = outside
        .path()
        .join(rollout_file_name("2026-07-13T00-00-00", thread_id));
    write_rollout(
        outside_path.as_path(),
        &[meta_line(thread_id, segment_id, /*ordinal*/ 0)],
    )?;
    let segment_directory = home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(segment_id.to_string());
    fs::create_dir_all(segment_directory.parent().expect("thread directory"))?;
    symlink(outside.path(), segment_directory.as_path())?;
    let reference = RolloutReferenceItem {
        rollout_id: None,
        rollout_path: home.path().join("sessions/missing.jsonl"),
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(segment_id),
        max_depth: 2,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };

    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference)
            .await
            .expect_err("an escaping segment directory must not be enumerated")
            .kind(),
        io::ErrorKind::NotFound
    );
    Ok(())
}

#[tokio::test]
async fn resolver_rejects_timestamp_path_traversal() -> io::Result<()> {
    let home = TempDir::new()?;
    let outside = TempDir::new()?;
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let outside_path = outside
        .path()
        .join(rollout_file_name("2026-07-13T00-00-00", thread_id));
    write_rollout(
        outside_path.as_path(),
        &[meta_line(thread_id, segment_id, /*ordinal*/ 0)],
    )?;
    let reference = RolloutReferenceItem {
        rollout_id: None,
        rollout_path: home.path().join("sessions/missing.jsonl"),
        thread_id: Some(thread_id),
        rollout_timestamp: Some("../../../../../../outside".to_string()),
        segment_id: Some(segment_id),
        max_depth: 2,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };

    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference)
            .await
            .expect_err("timestamp components must not escape approved roots")
            .kind(),
        io::ErrorKind::NotFound
    );
    Ok(())
}

#[tokio::test]
async fn resolver_requires_the_referenced_physical_rollout_id() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let recorded_rollout_id = ThreadId::new();
    let referenced_rollout_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let segment_dir = home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(segment_id.to_string());
    let recorded_path = segment_dir.join(format!(
        "rollout-2026-08-13T00-00-00-{thread_id}_{recorded_rollout_id}.jsonl"
    ));
    let referenced_path = segment_dir.join(format!(
        "rollout-2026-08-13T00-00-01-{thread_id}_{referenced_rollout_id}.jsonl"
    ));
    let lines = [meta_line(thread_id, segment_id, /*ordinal*/ 0)];
    write_rollout(recorded_path.as_path(), &lines)?;
    write_rollout(referenced_path.as_path(), &lines)?;

    let reference = RolloutReferenceItem {
        rollout_id: Some(referenced_rollout_id),
        rollout_path: recorded_path.clone(),
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(segment_id),
        max_depth: 2,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };
    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference).await?,
        referenced_path
    );

    fs::remove_file(referenced_path)?;
    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference)
            .await
            .expect_err("a different physical rollout must not satisfy the reference")
            .kind(),
        io::ErrorKind::NotFound
    );
    Ok(())
}

#[tokio::test]
async fn resolver_uses_validated_rotated_compressed_segment() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let segment_dir = home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(segment_id.to_string());
    let plain_path = segment_dir.join("rollout-segment.jsonl");
    write_rollout(
        plain_path.as_path(),
        &[
            meta_line(thread_id, segment_id, /*ordinal*/ 0),
            agent_line("compressed", /*ordinal*/ 1),
        ],
    )?;
    let compressed_path = plain_path.with_extension("jsonl.zst");
    let encoded = zstd::stream::encode_all(fs::File::open(&plain_path)?, 1)?;
    fs::write(&compressed_path, encoded)?;
    fs::remove_file(&plain_path)?;

    let reference = RolloutReferenceItem {
        rollout_id: None,
        rollout_path: home.path().join("stale.jsonl"),
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(segment_id),
        max_depth: 2,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };
    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference).await?,
        compressed_path
    );
    Ok(())
}

#[tokio::test]
async fn legacy_reference_accepts_matching_recorded_path() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let recorded_path = active_rollout_path(home.path(), thread_id, "2026-07-13T00-00-00");
    write_rollout(
        recorded_path.as_path(),
        &[
            legacy_meta_line(thread_id, /*ordinal*/ 0),
            agent_line("legacy", /*ordinal*/ 1),
        ],
    )?;
    let RolloutItem::RolloutReference(reference) =
        legacy_reference_line(recorded_path.clone(), thread_id, /*ordinal*/ 2).item
    else {
        unreachable!();
    };

    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference).await?,
        recorded_path
    );
    Ok(())
}

#[tokio::test]
async fn legacy_reference_uses_initial_after_stable_path_is_replaced() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let file_name = rollout_file_name("2026-07-13T00-00-00", thread_id);
    let stable_path = home.path().join(file_name.as_str());
    write_rollout(
        stable_path.as_path(),
        &[
            meta_line(thread_id, SegmentId::new(), /*ordinal*/ 0),
            agent_line("new", /*ordinal*/ 1),
        ],
    )?;
    let initial_path = home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join("initial")
        .join(file_name);
    write_rollout(
        initial_path.as_path(),
        &[
            legacy_meta_line(thread_id, /*ordinal*/ 0),
            agent_line("legacy", /*ordinal*/ 1),
        ],
    )?;

    let RolloutItem::RolloutReference(reference) =
        legacy_reference_line(stable_path, thread_id, /*ordinal*/ 2).item
    else {
        unreachable!();
    };
    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference).await?,
        initial_path
    );
    Ok(())
}

#[tokio::test]
async fn legacy_reference_rejects_replaced_stable_path_without_initial() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let stable_path = home.path().join("stable.jsonl");
    write_rollout(
        stable_path.as_path(),
        &[meta_line(thread_id, SegmentId::new(), /*ordinal*/ 0)],
    )?;
    let RolloutItem::RolloutReference(reference) =
        legacy_reference_line(stable_path, thread_id, /*ordinal*/ 1).item
    else {
        unreachable!();
    };

    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference)
            .await
            .expect_err("a replacement segment must not satisfy a legacy reference")
            .kind(),
        io::ErrorKind::NotFound
    );
    Ok(())
}

#[tokio::test]
async fn legacy_reference_resolves_archived_initial_segment() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let archived_path = home
        .path()
        .join(ARCHIVED_SESSIONS_SUBDIR)
        .join(thread_id.to_string())
        .join("initial")
        .join(rollout_file_name("2026-07-13T00-00-00", thread_id));
    write_rollout(
        archived_path.as_path(),
        &[
            legacy_meta_line(thread_id, /*ordinal*/ 0),
            agent_line("archived", /*ordinal*/ 1),
        ],
    )?;
    let RolloutItem::RolloutReference(reference) = legacy_reference_line(
        home.path()
            .join("elsewhere")
            .join(rollout_file_name("2026-07-13T00-00-00", thread_id)),
        thread_id,
        /*ordinal*/ 2,
    )
    .item
    else {
        unreachable!();
    };

    assert_eq!(
        resolve_rollout_reference_path(home.path(), &reference).await?,
        archived_path
    );
    Ok(())
}

#[tokio::test]
async fn legacy_reference_rejects_ambiguous_initial_segments() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let initial_directory = home
        .path()
        .join(ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join("initial");
    for file_name in [
        rollout_file_name("2026-07-13T00-00-00", thread_id),
        rollout_file_name("2026-07-13T00-01-00", thread_id),
    ] {
        write_rollout(
            initial_directory.join(file_name).as_path(),
            &[legacy_meta_line(thread_id, /*ordinal*/ 0)],
        )?;
    }
    let RolloutItem::RolloutReference(reference) =
        legacy_reference_line(PathBuf::new(), thread_id, /*ordinal*/ 1).item
    else {
        unreachable!();
    };

    let error = resolve_rollout_reference_path(home.path(), &reference)
        .await
        .expect_err("multiple legacy initial segments must be rejected");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("ambiguous"));
    Ok(())
}

#[tokio::test]
async fn materialization_rejects_reference_cycles() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_a = ThreadId::new();
    let segment_a = SegmentId::new();
    let thread_b = ThreadId::new();
    let segment_b = SegmentId::new();
    let path_a = immutable_segment_path(home.path(), thread_a, segment_a, "2026-07-13T00-00-00");
    let path_b = immutable_segment_path(home.path(), thread_b, segment_b, "2026-07-13T00-01-00");
    write_rollout(
        path_a.as_path(),
        &[
            meta_line(thread_a, segment_a, /*ordinal*/ 0),
            reference_line(path_b.clone(), thread_b, segment_b, /*ordinal*/ 1),
        ],
    )?;
    write_rollout(
        path_b.as_path(),
        &[
            meta_line(thread_b, segment_b, /*ordinal*/ 0),
            reference_line(path_a.clone(), thread_a, segment_a, /*ordinal*/ 1),
        ],
    )?;

    let error = materialize_rollout_lines(home.path(), path_a.as_path())
        .await
        .err()
        .expect("cycle should fail");
    assert!(error.to_string().contains("cycle"));
    Ok(())
}

#[tokio::test]
async fn materialization_rejects_legacy_reference_cycles() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_a = ThreadId::new();
    let thread_b = ThreadId::new();
    let path_a = active_rollout_path(home.path(), thread_a, "2026-07-13T00-00-00");
    let path_b = active_rollout_path(home.path(), thread_b, "2026-07-13T00-01-00");
    write_rollout(
        path_a.as_path(),
        &[
            legacy_meta_line(thread_a, /*ordinal*/ 0),
            legacy_reference_line(path_b.clone(), thread_b, /*ordinal*/ 1),
        ],
    )?;
    write_rollout(
        path_b.as_path(),
        &[
            legacy_meta_line(thread_b, /*ordinal*/ 0),
            legacy_reference_line(path_a.clone(), thread_a, /*ordinal*/ 1),
        ],
    )?;

    let error = materialize_rollout_lines(home.path(), path_a.as_path())
        .await
        .err()
        .expect("legacy cycle should fail");
    assert!(error.to_string().contains("cycle"));
    Ok(())
}

#[tokio::test]
async fn materialization_rejects_depth_exhaustion() -> io::Result<()> {
    let home = TempDir::new()?;
    let referenced_thread = ThreadId::new();
    let referenced_segment = SegmentId::new();
    let current_thread = ThreadId::new();
    let mut has_older_reference = false;
    let mut state = ExpansionState {
        retain_source_metadata: false,
        active_segments: HashSet::new(),
        active_history_bases: HashSet::new(),
        cache: None,
    };
    let error = expand_lines(
        home.path(),
        vec![
            meta_line_with_segment(current_thread, /*segment_id*/ None, /*ordinal*/ 0),
            reference_line(
                home.path().join("unresolved.jsonl"),
                referenced_thread,
                referenced_segment,
                /*ordinal*/ 0,
            ),
        ],
        &mut state,
        ExpansionCursor {
            graph_depth: MAX_ROLLOUT_REFERENCE_DEPTH,
            ordinary_reference_depth: 0,
            current_thread_id: current_thread,
        },
        /*inherited_filter_texts*/ None,
        MaterializationPolicy::Complete,
        &mut has_older_reference,
    )
    .await
    .err()
    .expect("depth exhaustion should fail before resolving the reference");
    assert!(error.to_string().contains("maximum depth"));
    Ok(())
}

#[tokio::test]
async fn recent_materialization_bounds_existing_deep_reference_chains() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let deepest_segment = SegmentId::new();
    let ancient_segment = SegmentId::new();
    let older_segment = SegmentId::new();
    let old_segment = SegmentId::new();
    let middle_segment = SegmentId::new();
    let current_segment = SegmentId::new();
    let fork_thread = ThreadId::new();
    let fork_segment = SegmentId::new();

    let deepest_path = immutable_segment_path(
        home.path(),
        thread_id,
        deepest_segment,
        "2026-07-13T00-00-00",
    );
    let old_path =
        immutable_segment_path(home.path(), thread_id, old_segment, "2026-07-13T00-03-00");
    let ancient_path = immutable_segment_path(
        home.path(),
        thread_id,
        ancient_segment,
        "2026-07-13T00-01-00",
    );
    let older_path =
        immutable_segment_path(home.path(), thread_id, older_segment, "2026-07-13T00-02-00");
    let middle_path = immutable_segment_path(
        home.path(),
        thread_id,
        middle_segment,
        "2026-07-13T00-04-00",
    );
    let current_path = immutable_segment_path(
        home.path(),
        thread_id,
        current_segment,
        "2026-07-13T00-05-00",
    );
    let fork_path = home.path().join("fork.jsonl");

    let deepest_meta =
        serde_json::to_string(&meta_line(thread_id, deepest_segment, /*ordinal*/ 0))?;
    fs::create_dir_all(deepest_path.parent().expect("immutable segment directory"))?;
    fs::write(
        deepest_path.as_path(),
        format!("{deepest_meta}\n{{malformed rollout line\n"),
    )?;

    let mut deepest_reference = reference_line(
        deepest_path.clone(),
        thread_id,
        deepest_segment,
        /*ordinal*/ 1,
    );
    let RolloutItem::RolloutReference(reference) = &mut deepest_reference.item else {
        unreachable!();
    };
    reference.max_depth = MAX_ROLLOUT_REFERENCE_DEPTH;
    write_rollout(
        ancient_path.as_path(),
        &[
            meta_line(thread_id, ancient_segment, /*ordinal*/ 2),
            deepest_reference,
        ],
    )?;

    let mut ancient_reference = reference_line(
        ancient_path.clone(),
        thread_id,
        ancient_segment,
        /*ordinal*/ 3,
    );
    let RolloutItem::RolloutReference(reference) = &mut ancient_reference.item else {
        unreachable!();
    };
    reference.max_depth = MAX_ROLLOUT_REFERENCE_DEPTH;
    write_rollout(
        older_path.as_path(),
        &[
            meta_line(thread_id, older_segment, /*ordinal*/ 4),
            ancient_reference,
        ],
    )?;

    let mut older_reference = reference_line(
        older_path.clone(),
        thread_id,
        older_segment,
        /*ordinal*/ 5,
    );
    let RolloutItem::RolloutReference(reference) = &mut older_reference.item else {
        unreachable!();
    };
    reference.max_depth = MAX_ROLLOUT_REFERENCE_DEPTH;
    write_rollout(
        old_path.as_path(),
        &[
            meta_line(thread_id, old_segment, /*ordinal*/ 6),
            older_reference,
            agent_line("old", /*ordinal*/ 7),
        ],
    )?;

    let mut old_reference =
        reference_line(old_path.clone(), thread_id, old_segment, /*ordinal*/ 8);
    let RolloutItem::RolloutReference(reference) = &mut old_reference.item else {
        unreachable!();
    };
    reference.max_depth = MAX_ROLLOUT_REFERENCE_DEPTH;
    write_rollout(
        middle_path.as_path(),
        &[
            meta_line(thread_id, middle_segment, /*ordinal*/ 9),
            old_reference,
            agent_line("middle", /*ordinal*/ 10),
        ],
    )?;

    let mut middle_reference = reference_line(
        middle_path.clone(),
        thread_id,
        middle_segment,
        /*ordinal*/ 11,
    );
    let RolloutItem::RolloutReference(reference) = &mut middle_reference.item else {
        unreachable!();
    };
    reference.max_depth = MAX_ROLLOUT_REFERENCE_DEPTH;
    write_rollout(
        current_path.as_path(),
        &[
            meta_line(thread_id, current_segment, /*ordinal*/ 12),
            middle_reference,
            agent_line("current", /*ordinal*/ 13),
        ],
    )?;

    let bounded = materialize_recent_rollout_lines(home.path(), current_path.as_path()).await?;
    assert_eq!(event_messages(&bounded), vec!["old", "middle", "current"]);

    let bounded_window = materialize_bounded_rollout_lines(
        home.path(),
        current_path.as_path(),
        /*ordinary_reference_limit*/ 2,
    )
    .await?;
    assert_eq!(
        event_messages(&bounded_window.lines),
        vec!["old", "middle", "current"]
    );
    assert!(bounded_window.has_older_reference);

    let complete_error = materialize_rollout_lines(home.path(), current_path.as_path())
        .await
        .err()
        .expect("complete materialization must inspect the malformed deepest segment");
    assert!(complete_error.to_string().contains("invalid record"));

    let bounded_error = materialize_recent_rollout_lines(home.path(), middle_path.as_path())
        .await
        .err()
        .expect("the malformed segment is inside the window when middle is the root");
    assert!(bounded_error.to_string().contains("invalid record"));

    let mut fork_reference = reference_line(
        current_path.clone(),
        thread_id,
        current_segment,
        /*ordinal*/ 15,
    );
    let RolloutItem::RolloutReference(reference) = &mut fork_reference.item else {
        unreachable!();
    };
    reference.max_depth = MAX_ROLLOUT_REFERENCE_DEPTH;
    write_rollout(
        fork_path.as_path(),
        &[
            meta_line(fork_thread, fork_segment, /*ordinal*/ 14),
            fork_reference,
        ],
    )?;

    let forked = materialize_recent_rollout_lines(home.path(), fork_path.as_path()).await?;
    assert_eq!(event_messages(&forked), event_messages(&bounded));
    Ok(())
}

#[tokio::test]
async fn materialization_accepts_reference_chain_longer_than_legacy_limit() -> io::Result<()> {
    const LEGACY_MAX_ROLLOUT_REFERENCE_DEPTH: usize = 64;

    let home = TempDir::new()?;
    let identities = (0..=LEGACY_MAX_ROLLOUT_REFERENCE_DEPTH + 1)
        .map(|_| (ThreadId::new(), SegmentId::new()))
        .collect::<Vec<_>>();
    let paths = identities
        .iter()
        .enumerate()
        .map(|(index, (thread_id, segment_id))| {
            immutable_segment_path(
                home.path(),
                *thread_id,
                *segment_id,
                format!("2026-07-13T00-{index:02}-00").as_str(),
            )
        })
        .collect::<Vec<_>>();
    for index in 0..identities.len() {
        let (thread_id, segment_id) = identities[index];
        let mut lines = vec![meta_line(thread_id, segment_id, /*ordinal*/ 0)];
        if let Some((next_thread_id, next_segment_id)) = identities.get(index + 1).copied() {
            lines.push(reference_line(
                paths[index + 1].clone(),
                next_thread_id,
                next_segment_id,
                /*ordinal*/ 1,
            ));
        }
        write_rollout(paths[index].as_path(), &lines)?;
    }

    let lines = materialize_rollout_lines(home.path(), paths[0].as_path()).await?;
    assert_eq!(lines.len(), 1);
    Ok(())
}

#[tokio::test]
async fn materialization_accepts_512_same_thread_segments() -> io::Result<()> {
    const SEGMENT_COUNT: usize = MAX_ROLLOUT_REFERENCE_DEPTH * 2;

    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let segment_ids = (0..SEGMENT_COUNT)
        .map(|_| SegmentId::new())
        .collect::<Vec<_>>();
    let paths = (0..SEGMENT_COUNT)
        .map(|index| {
            immutable_segment_path(
                home.path(),
                thread_id,
                segment_ids[index],
                "2026-07-13T00-00-00",
            )
        })
        .collect::<Vec<_>>();

    for index in 0..SEGMENT_COUNT {
        let mut lines = vec![meta_line(thread_id, segment_ids[index], /*ordinal*/ 0)];
        if let Some(next_index) = index.checked_add(1).filter(|next| *next < SEGMENT_COUNT) {
            lines.push(reference_line(
                paths[next_index].clone(),
                thread_id,
                segment_ids[next_index],
                /*ordinal*/ 1,
            ));
        }
        lines.push(agent_line(&format!("segment-{index}"), /*ordinal*/ 2));
        write_rollout(paths[index].as_path(), &lines)?;
    }

    let lines = materialize_rollout_lines(home.path(), paths[0].as_path()).await?;
    assert_eq!(event_messages(&lines).len(), SEGMENT_COUNT);

    let overflow_segment = SegmentId::new();
    let overflow_path = home.path().join("same-thread-overflow.jsonl");
    write_rollout(
        overflow_path.as_path(),
        &[
            meta_line(thread_id, overflow_segment, /*ordinal*/ 0),
            reference_line(
                paths[0].clone(),
                thread_id,
                segment_ids[0],
                /*ordinal*/ 1,
            ),
        ],
    )?;
    let lines = materialize_rollout_lines(home.path(), overflow_path.as_path()).await?;
    assert_eq!(event_messages(&lines).len(), SEGMENT_COUNT);
    Ok(())
}

#[tokio::test]
async fn materialization_rejects_same_thread_segment_cycles() -> io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let first_segment = SegmentId::new();
    let second_segment = SegmentId::new();
    let first_path = home.path().join("same-thread-cycle-first.jsonl");
    let second_path = home.path().join("same-thread-cycle-second.jsonl");

    write_rollout(
        first_path.as_path(),
        &[
            meta_line(thread_id, first_segment, /*ordinal*/ 0),
            reference_line(
                second_path.clone(),
                thread_id,
                second_segment,
                /*ordinal*/ 1,
            ),
        ],
    )?;
    write_rollout(
        second_path.as_path(),
        &[
            meta_line(thread_id, second_segment, /*ordinal*/ 0),
            reference_line(
                first_path.clone(),
                thread_id,
                first_segment,
                /*ordinal*/ 1,
            ),
        ],
    )?;

    let error = materialize_rollout_lines(home.path(), first_path.as_path())
        .await
        .err()
        .expect("same-thread segment cycles must remain invalid");
    assert!(error.to_string().contains("cycle"));
    Ok(())
}
