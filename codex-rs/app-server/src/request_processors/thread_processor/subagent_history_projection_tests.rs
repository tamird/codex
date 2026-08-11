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
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
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
