use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

use codex_app_server_protocol::CollabAgentState;
use codex_app_server_protocol::CollabAgentStatus;
use codex_app_server_protocol::CollabAgentTool;
use codex_app_server_protocol::CollabAgentToolCallStatus;
use codex_app_server_protocol::SubAgentActivityKind;
use codex_app_server_protocol::ThreadItem;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::CollabAgentInteractionBeginEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_rollout::RolloutRecorder;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::SubagentHistoryProjection;

fn projection(retained_thread_ids: &[&str]) -> SubagentHistoryProjection {
    SubagentHistoryProjection {
        retained_thread_ids: retained_thread_ids
            .iter()
            .map(ToString::to_string)
            .collect::<HashSet<_>>(),
    }
}

fn spawn_item(receiver_thread_ids: &[&str]) -> ThreadItem {
    ThreadItem::CollabAgentToolCall {
        id: "spawn-1".to_string(),
        tool: CollabAgentTool::SpawnAgent,
        status: CollabAgentToolCallStatus::Completed,
        sender_thread_id: "root".to_string(),
        receiver_thread_ids: receiver_thread_ids
            .iter()
            .map(ToString::to_string)
            .collect(),
        receiver_agent_nickname: None,
        receiver_agent_role: None,
        prompt: Some("delegate".to_string()),
        model: None,
        reasoning_effort: None,
        agents_states: receiver_thread_ids
            .iter()
            .map(|thread_id| {
                (
                    thread_id.to_string(),
                    CollabAgentState {
                        status: CollabAgentStatus::Running,
                        message: None,
                    },
                )
            })
            .collect::<HashMap<_, _>>(),
    }
}

#[test]
fn removes_stale_activity_and_prunes_mixed_spawn() {
    let projection = projection(&["recent"]);
    let mut items = vec![
        ThreadItem::SubAgentActivity {
            id: "activity-stale".to_string(),
            kind: SubAgentActivityKind::Started,
            agent_thread_id: "stale".to_string(),
            agent_path: "/root/stale".to_string(),
        },
        ThreadItem::SubAgentActivity {
            id: "activity-recent".to_string(),
            kind: SubAgentActivityKind::Started,
            agent_thread_id: "recent".to_string(),
            agent_path: "/root/recent".to_string(),
        },
        spawn_item(&["stale", "recent"]),
    ];

    projection.project_items(&mut items);

    assert_eq!(
        items,
        vec![
            ThreadItem::SubAgentActivity {
                id: "activity-recent".to_string(),
                kind: SubAgentActivityKind::Started,
                agent_thread_id: "recent".to_string(),
                agent_path: "/root/recent".to_string(),
            },
            spawn_item(&["recent"]),
        ]
    );
}

#[test]
fn drops_emptied_spawn_but_keeps_failed_spawn_and_other_tools() {
    let projection = projection(&[]);
    let mut failed_spawn = spawn_item(&[]);
    if let ThreadItem::CollabAgentToolCall { status, .. } = &mut failed_spawn {
        *status = CollabAgentToolCallStatus::Failed;
    }
    let mut send_input = spawn_item(&["stale"]);
    if let ThreadItem::CollabAgentToolCall { tool, .. } = &mut send_input {
        *tool = CollabAgentTool::SendInput;
    }
    let mut items = vec![
        spawn_item(&["stale"]),
        failed_spawn.clone(),
        send_input.clone(),
    ];

    projection.project_items(&mut items);

    assert_eq!(items, vec![failed_spawn, send_input]);
}

#[tokio::test]
async fn exact_five_same_thread_segments_disable_projection() {
    let home = TempDir::new().expect("temporary rollout home");
    let thread_id = ThreadId::new();
    let paths =
        write_same_thread_chain(home.path(), thread_id, /*segment_count*/ 5, &[], &[]).await;

    let projection = SubagentHistoryProjection::load(
        home.path(),
        paths.last().expect("active rollout").as_path(),
        thread_id,
        [],
    )
    .await
    .expect("load projection");

    assert!(projection.is_none());
}

#[tokio::test]
async fn sixth_segment_enables_projection_and_current_ids_survive() {
    let home = TempDir::new().expect("temporary rollout home");
    let thread_id = ThreadId::new();
    let stale_id = ThreadId::new();
    let current_id = ThreadId::new();
    let recent_id = ThreadId::new();
    let paths = write_same_thread_chain(
        home.path(),
        thread_id,
        /*segment_count*/ 6,
        &[(0, stale_id), (0, current_id), (1, recent_id)],
        &[],
    )
    .await;
    let projection = SubagentHistoryProjection::load(
        home.path(),
        paths.last().expect("active rollout").as_path(),
        thread_id,
        [current_id],
    )
    .await
    .expect("load projection")
    .expect("sixth segment enables projection");
    let mut items = vec![
        activity_item("stale", stale_id),
        activity_item("current", current_id),
        activity_item("recent", recent_id),
    ];

    projection.project_items(&mut items);

    assert_eq!(
        items,
        vec![
            activity_item("current", current_id),
            activity_item("recent", recent_id),
        ]
    );
}

#[tokio::test]
async fn fork_boundary_does_not_contribute_recent_ids() {
    let home = TempDir::new().expect("temporary rollout home");
    let thread_id = ThreadId::new();
    let fork_thread_id = ThreadId::new();
    let fork_agent_id = ThreadId::new();
    let fork_segment_id = SegmentId::new();
    let fork_path = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(fork_thread_id.to_string())
        .join(fork_segment_id.to_string())
        .join(format!(
            "rollout-2026-08-11T00-00-00-{fork_thread_id}.jsonl"
        ));
    write_rollout(
        fork_path.as_path(),
        &[
            meta_line(fork_thread_id, fork_segment_id),
            interaction_line(fork_thread_id, fork_agent_id),
        ],
    )
    .await;
    let fork_reference = RolloutReferenceItem {
        rollout_path: fork_path,
        thread_id: Some(fork_thread_id),
        rollout_id: None,
        rollout_timestamp: None,
        segment_id: Some(fork_segment_id),
        max_depth: 2,
        nth_user_message: Some(1),
        compacted_replacement_history_filter_texts: None,
    };
    let paths = write_same_thread_chain(
        home.path(),
        thread_id,
        /*segment_count*/ 6,
        &[],
        &[fork_reference],
    )
    .await;
    let projection = SubagentHistoryProjection::load(
        home.path(),
        paths.last().expect("active rollout").as_path(),
        thread_id,
        [],
    )
    .await
    .expect("load projection")
    .expect("sixth segment enables projection");
    let mut items = vec![activity_item("fork", fork_agent_id)];

    projection.project_items(&mut items);

    assert_eq!(items, Vec::<ThreadItem>::new());
}

#[tokio::test]
async fn native_projection_counts_only_same_thread_physical_segments() {
    for segment_count in [5, 6] {
        for include_foreign_base in [false, true] {
            let home = TempDir::new().expect("temporary rollout home");
            let thread_id = ThreadId::new();
            let [stale_id, current_id, recent_id, foreign_id] =
                std::array::from_fn(|_| ThreadId::new());
            let foreign = write_native_chain(
                home.path(),
                ThreadId::new(),
                /*segment_count*/ 1,
                /*history_base*/ None,
                &[(0, foreign_id)],
            )
            .await;
            let segments = write_native_chain(
                home.path(),
                thread_id,
                segment_count,
                include_foreign_base.then_some(foreign[0].1),
                &[(0, stale_id), (0, current_id), (1, recent_id)],
            )
            .await;
            let projection = SubagentHistoryProjection::load(
                home.path(),
                &segments.last().expect("active rollout").0,
                thread_id,
                [current_id],
            )
            .await
            .expect("load native projection");
            if segment_count == 5 {
                assert!(
                    projection.is_none(),
                    "foreign ancestry is not a sixth owned segment"
                );
            } else {
                let mut items = vec![
                    activity_item("stale", stale_id),
                    activity_item("current", current_id),
                    activity_item("recent", recent_id),
                    activity_item("foreign", foreign_id),
                ];
                projection
                    .expect("six owned segments enable projection")
                    .project_items(&mut items);
                assert_eq!(
                    items,
                    vec![
                        activity_item("current", current_id),
                        activity_item("recent", recent_id)
                    ]
                );
            }
        }
    }
}

#[tokio::test]
async fn mixed_native_and_legacy_segments_share_the_retention_window() -> std::io::Result<()> {
    let home = TempDir::new()?;
    let thread_id = ThreadId::new();
    let [stale_id, native_id, legacy_id] = std::array::from_fn(|_| ThreadId::new());
    let native = write_native_chain(
        home.path(),
        thread_id,
        /*segment_count*/ 3,
        /*history_base*/ None,
        &[(0, stale_id), (1, native_id)],
    )
    .await;
    let legacy = write_same_thread_chain(
        home.path(),
        thread_id,
        /*segment_count*/ 3,
        &[(2, legacy_id)],
        &[],
    )
    .await;
    let (mut lines, _, _) = RolloutRecorder::load_rollout_lines(&legacy[0]).await?;
    let RolloutItem::SessionMeta(metadata) = &mut lines[0].item else {
        panic!("session metadata")
    };
    metadata.meta.history_mode = ThreadHistoryMode::Paginated;
    metadata.meta.history_base = Some(native.last().expect("native predecessor").1);
    write_rollout(&legacy[0], &lines).await;
    let projection = SubagentHistoryProjection::load(
        home.path(),
        legacy.last().expect("active rollout"),
        thread_id,
        [],
    )
    .await?
    .expect("six owned segments");
    let mut items = vec![
        activity_item("stale", stale_id),
        activity_item("native", native_id),
        activity_item("legacy", legacy_id),
    ];
    projection.project_items(&mut items);
    assert_eq!(
        items,
        vec![
            activity_item("native", native_id),
            activity_item("legacy", legacy_id)
        ]
    );
    Ok(())
}

#[tokio::test]
async fn native_projection_respects_prefix_cutoffs_without_rewriting_storage() -> std::io::Result<()>
{
    for compressed in [false, true] {
        let home = TempDir::new()?;
        let thread_id = ThreadId::new();
        let [recent_id, suffix_id] = std::array::from_fn(|_| ThreadId::new());
        let segments = write_native_chain(
            home.path(),
            thread_id,
            /*segment_count*/ 6,
            /*history_base*/ None,
            &[(1, recent_id)],
        )
        .await;
        let prefix_path = &segments[1].0;
        let (mut lines, _, _) = RolloutRecorder::load_rollout_lines(prefix_path).await?;
        let mut suffix = interaction_line(thread_id, suffix_id);
        suffix.ordinal = Some(segments[1].1.end_ordinal_exclusive);
        lines.push(suffix);
        write_rollout(prefix_path, &lines).await;
        let mut source_path = prefix_path.clone();
        if compressed {
            let bytes = tokio::fs::read(prefix_path).await?;
            source_path = prefix_path.with_extension("jsonl.zst");
            tokio::fs::write(
                &source_path,
                zstd::stream::encode_all(bytes.as_slice(), /*level*/ 0)?,
            )
            .await?;
            tokio::fs::remove_file(prefix_path).await?;
        }
        let source_bytes = tokio::fs::read(&source_path).await?;
        let source_modified = tokio::fs::metadata(&source_path).await?.modified()?;
        let projection = SubagentHistoryProjection::load(
            home.path(),
            &segments.last().expect("active rollout").0,
            thread_id,
            [],
        )
        .await?
        .expect("six owned segments");
        let mut items = vec![
            activity_item("recent", recent_id),
            activity_item("suffix", suffix_id),
        ];
        projection.project_items(&mut items);
        assert_eq!(items, vec![activity_item("recent", recent_id)]);
        assert_eq!(tokio::fs::read(&source_path).await?, source_bytes);
        assert_eq!(
            tokio::fs::metadata(&source_path).await?.modified()?,
            source_modified
        );
        assert_eq!(prefix_path.exists(), !compressed);
    }
    Ok(())
}

#[tokio::test]
async fn invalid_native_ancestry_cannot_enable_projection() -> std::io::Result<()> {
    enum InvalidAncestry {
        ByteCutoff,
        OrdinalCutoff,
        DuplicatePredecessor,
        MixedCycle,
    }
    for invalid in [
        InvalidAncestry::ByteCutoff,
        InvalidAncestry::OrdinalCutoff,
        InvalidAncestry::DuplicatePredecessor,
        InvalidAncestry::MixedCycle,
    ] {
        let home = TempDir::new()?;
        let thread_id = ThreadId::new();
        let segments = write_native_chain(
            home.path(),
            thread_id,
            /*segment_count*/ 2,
            /*history_base*/ None,
            &[],
        )
        .await;
        let active = &segments[1].0;
        let (mut lines, _, _) = RolloutRecorder::load_rollout_lines(active).await?;
        let RolloutItem::SessionMeta(metadata) = &mut lines[0].item else {
            panic!("session metadata")
        };
        let segment_id = metadata.meta.segment_id;
        let position = metadata
            .meta
            .history_base
            .as_mut()
            .expect("native predecessor");
        let reference = reference_line(RolloutReferenceItem {
            rollout_path: active.clone(),
            thread_id: Some(thread_id),
            rollout_id: Some(segments[1].1.thread_id),
            rollout_timestamp: None,
            segment_id,
            max_depth: 2,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        });
        match invalid {
            InvalidAncestry::ByteCutoff => position.end_byte_offset -= 1,
            InvalidAncestry::OrdinalCutoff => position.end_ordinal_exclusive += 1,
            InvalidAncestry::DuplicatePredecessor => lines.push(reference),
            InvalidAncestry::MixedCycle => {
                let (mut predecessor, _, _) =
                    RolloutRecorder::load_rollout_lines(&segments[0].0).await?;
                predecessor.push(reference);
                write_rollout(&segments[0].0, &predecessor).await;
                position.end_byte_offset = tokio::fs::metadata(&segments[0].0).await?.len();
                position.end_ordinal_exclusive += 1;
            }
        }
        write_rollout(active, &lines).await;
        let error = SubagentHistoryProjection::load(home.path(), active, thread_id, [])
            .await
            .err()
            .expect("invalid ancestry cannot construct a projection");
        let expected = match invalid {
            InvalidAncestry::ByteCutoff => "record boundary",
            InvalidAncestry::OrdinalCutoff => "ordinal boundary",
            InvalidAncestry::DuplicatePredecessor => "multiple same-thread predecessors",
            InvalidAncestry::MixedCycle => "cycle",
        };
        assert!(error.to_string().contains(expected), "{error}");
    }
    Ok(())
}

async fn write_native_chain(
    directory: &Path,
    thread_id: ThreadId,
    segment_count: usize,
    mut history_base: Option<HistoryPosition>,
    interactions: &[(usize, ThreadId)],
) -> Vec<(PathBuf, HistoryPosition)> {
    let mut segments = Vec::new();
    for index in 0..segment_count {
        let rollout_id = ThreadId::new();
        let path = directory
            .join(codex_rollout::SESSIONS_SUBDIR)
            .join(codex_rollout::ROLLOUT_SEGMENTS_SUBDIR)
            .join("2026/08/11")
            .join(format!(
                "rollout-2026-08-11T00-00-00-{thread_id}_{rollout_id}.jsonl"
            ));
        let mut metadata = meta_line(thread_id, SegmentId::new());
        let RolloutItem::SessionMeta(session_meta) = &mut metadata.item else {
            panic!("session metadata")
        };
        session_meta.meta.history_mode = ThreadHistoryMode::Paginated;
        session_meta.meta.history_base = history_base;
        let mut lines = vec![metadata];
        lines.extend(
            interactions
                .iter()
                .filter(|(segment_index, _)| *segment_index == index)
                .map(|(_, receiver)| interaction_line(thread_id, *receiver)),
        );
        let start = history_base.map_or(0, |position| position.end_ordinal_exclusive);
        for (offset, line) in lines.iter_mut().enumerate() {
            line.ordinal = Some(start + offset as u64);
        }
        write_rollout(&path, &lines).await;
        let position = HistoryPosition {
            thread_id: rollout_id,
            end_ordinal_exclusive: start + lines.len() as u64,
            end_byte_offset: tokio::fs::metadata(&path)
                .await
                .expect("native prefix length")
                .len(),
        };
        history_base = Some(position);
        segments.push((path, position));
    }
    segments
}

fn activity_item(id: &str, thread_id: ThreadId) -> ThreadItem {
    ThreadItem::SubAgentActivity {
        id: id.to_string(),
        kind: SubAgentActivityKind::Interacted,
        agent_thread_id: thread_id.to_string(),
        agent_path: format!("/root/{id}"),
    }
}

async fn write_same_thread_chain(
    directory: &Path,
    thread_id: ThreadId,
    segment_count: usize,
    interactions: &[(usize, ThreadId)],
    active_extra_references: &[RolloutReferenceItem],
) -> Vec<PathBuf> {
    let segment_ids = (0..segment_count)
        .map(|_| SegmentId::new())
        .collect::<Vec<_>>();
    let paths = segment_ids
        .iter()
        .map(|segment_id| {
            directory
                .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
                .join(thread_id.to_string())
                .join(segment_id.to_string())
                .join(format!("rollout-2026-08-11T00-00-00-{thread_id}.jsonl"))
        })
        .collect::<Vec<_>>();
    for index in 0..segment_count {
        let mut lines = vec![meta_line(thread_id, segment_ids[index])];
        if index > 0 {
            lines.push(reference_line(RolloutReferenceItem {
                rollout_path: paths[index - 1].clone(),
                thread_id: Some(thread_id),
                rollout_id: None,
                rollout_timestamp: None,
                segment_id: Some(segment_ids[index - 1]),
                max_depth: 2,
                nth_user_message: None,
                compacted_replacement_history_filter_texts: None,
            }));
        }
        if index + 1 == segment_count {
            lines.extend(active_extra_references.iter().cloned().map(reference_line));
        }
        lines.extend(
            interactions
                .iter()
                .filter(|(segment_index, _)| *segment_index == index)
                .map(|(_, receiver_thread_id)| interaction_line(thread_id, *receiver_thread_id)),
        );
        write_rollout(paths[index].as_path(), lines.as_slice()).await;
    }
    paths
}

fn meta_line(thread_id: ThreadId, segment_id: SegmentId) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-08-11T00:00:00Z".to_string(),
        ordinal: Some(0),
        item: RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                session_id: thread_id.into(),
                id: thread_id,
                segment_id: Some(segment_id),
                timestamp: "2026-08-11T00:00:00Z".to_string(),
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

fn reference_line(reference: RolloutReferenceItem) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-08-11T00:00:01Z".to_string(),
        ordinal: Some(1),
        item: RolloutItem::RolloutReference(reference),
    }
}

fn interaction_line(sender_thread_id: ThreadId, receiver_thread_id: ThreadId) -> RolloutLine {
    RolloutLine {
        timestamp: "2026-08-11T00:00:02Z".to_string(),
        ordinal: Some(2),
        item: RolloutItem::EventMsg(EventMsg::CollabAgentInteractionBegin(
            CollabAgentInteractionBeginEvent {
                call_id: format!("send-{receiver_thread_id}"),
                started_at_ms: 0,
                sender_thread_id,
                receiver_thread_id,
                prompt: "continue".to_string(),
            },
        )),
    }
}

async fn write_rollout(path: &Path, lines: &[RolloutLine]) {
    tokio::fs::create_dir_all(path.parent().expect("rollout parent directory"))
        .await
        .expect("create rollout parent directory");
    let mut contents = lines
        .iter()
        .map(|line| serde_json::to_string(line).expect("serialize rollout line"))
        .collect::<Vec<_>>()
        .join("\n");
    contents.push('\n');
    tokio::fs::write(path, contents)
        .await
        .expect("write rollout");
}
