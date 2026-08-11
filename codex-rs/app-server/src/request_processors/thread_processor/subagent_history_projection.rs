use std::collections::HashSet;
use std::io;
use std::io::ErrorKind;
use std::path::Path;

use codex_app_server_protocol::CollabAgentTool;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::Turn;
use codex_protocol::ThreadId;
use codex_protocol::items::TurnItem as CoreTurnItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;

const UNFILTERED_SEGMENT_COUNT: usize = 5;

/// Retained subagent identities for the app-server historical subagent projection.
///
/// This projection changes only app-server response items. Persisted rollout records and model
/// context remain complete. It is enabled only when a same-thread predecessor exists beyond the
/// active rollout and its four newest same-thread predecessors.
#[derive(Clone)]
pub(crate) struct SubagentHistoryProjection {
    retained_thread_ids: HashSet<String>,
}

impl SubagentHistoryProjection {
    /// Builds a projection from direct records in the five newest same-thread rollout segments.
    ///
    /// Fork-boundary references and compacted replacement history are intentionally not expanded:
    /// neither represents interaction that physically occurred in one of these five segments.
    pub(crate) async fn load(
        codex_home: &Path,
        active_rollout_path: &Path,
        thread_id: ThreadId,
        current_thread_ids: impl IntoIterator<Item = ThreadId>,
    ) -> io::Result<Option<Self>> {
        let mut retained_thread_ids = current_thread_ids
            .into_iter()
            .map(|thread_id| thread_id.to_string())
            .collect::<HashSet<_>>();
        let mut rollout_path = active_rollout_path.to_path_buf();
        let mut visited_paths = HashSet::new();

        for segment_index in 0..UNFILTERED_SEGMENT_COUNT {
            if !visited_paths.insert(rollout_path.clone()) {
                return Err(invalid_data("same-thread rollout reference cycle"));
            }
            let predecessor =
                scan_segment(rollout_path.as_path(), thread_id, &mut retained_thread_ids).await?;
            let Some(predecessor) = predecessor else {
                return Ok(None);
            };
            if segment_index + 1 == UNFILTERED_SEGMENT_COUNT {
                return Ok(Some(Self {
                    retained_thread_ids,
                }));
            }
            rollout_path =
                codex_rollout::resolve_rollout_reference_path(codex_home, &predecessor).await?;
        }

        Ok(None)
    }

    /// Removes membership-producing history for subagents outside the retained identity set.
    pub(crate) fn project_turns(&self, turns: &mut [Turn]) {
        for turn in turns {
            self.project_items(&mut turn.items);
        }
    }

    /// Removes membership-producing history from one app-server item collection.
    pub(super) fn project_items(&self, items: &mut Vec<ThreadItem>) {
        items.retain_mut(|item| self.retain_item(item));
    }

    /// Projects one item and reports whether the caller should retain it.
    pub(super) fn retain_item(&self, item: &mut ThreadItem) -> bool {
        match item {
            ThreadItem::SubAgentActivity {
                agent_thread_id, ..
            } => self.retained_thread_ids.contains(agent_thread_id),
            ThreadItem::CollabAgentToolCall {
                tool: CollabAgentTool::SpawnAgent,
                receiver_thread_ids,
                agents_states,
                ..
            } => {
                let originally_had_receiver = !receiver_thread_ids.is_empty();
                receiver_thread_ids
                    .retain(|thread_id| self.retained_thread_ids.contains(thread_id));
                agents_states.retain(|thread_id, _| receiver_thread_ids.contains(thread_id));
                !originally_had_receiver || !receiver_thread_ids.is_empty()
            }
            _ => true,
        }
    }
}

async fn scan_segment(
    rollout_path: &Path,
    expected_thread_id: ThreadId,
    retained_thread_ids: &mut HashSet<String>,
) -> io::Result<Option<RolloutReferenceItem>> {
    let mut reader = codex_rollout::open_rollout_line_reader(rollout_path).await?;
    let mut saw_session_meta = false;
    let mut predecessor = None;
    while let Some(line) = reader.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let line = serde_json::from_str::<RolloutLine>(&line).map_err(|error| {
            invalid_data(format!(
                "failed to decode rollout {}: {error}",
                rollout_path.display()
            ))
        })?;
        match line.item {
            RolloutItem::SessionMeta(session_meta) => {
                if saw_session_meta || session_meta.meta.id != expected_thread_id {
                    return Err(invalid_data(format!(
                        "rollout {} has unexpected session metadata",
                        rollout_path.display()
                    )));
                }
                saw_session_meta = true;
            }
            RolloutItem::RolloutReference(reference)
                if reference.nth_user_message.is_none()
                    && reference.thread_id == Some(expected_thread_id) =>
            {
                if predecessor.replace(reference).is_some() {
                    return Err(invalid_data(format!(
                        "rollout {} has multiple same-thread predecessors",
                        rollout_path.display()
                    )));
                }
            }
            RolloutItem::EventMsg(event) => collect_event_thread_ids(&event, retained_thread_ids),
            RolloutItem::RolloutReference(_)
            | RolloutItem::ResponseItem(_)
            | RolloutItem::InterAgentCommunication(_)
            | RolloutItem::InterAgentCommunicationMetadata { .. }
            | RolloutItem::Compacted(_)
            | RolloutItem::TurnContext(_)
            | RolloutItem::SecurityRiskScore(_)
            | RolloutItem::RealtimeItem(_)
            | RolloutItem::WorldState(_) => {}
        }
    }
    if !saw_session_meta {
        return Err(invalid_data(format!(
            "rollout {} has no session metadata",
            rollout_path.display()
        )));
    }
    Ok(predecessor)
}

fn collect_event_thread_ids(event: &EventMsg, retained_thread_ids: &mut HashSet<String>) {
    match event {
        EventMsg::SubAgentActivity(event) => {
            retain_thread_id(retained_thread_ids, event.agent_thread_id);
        }
        EventMsg::CollabAgentSpawnEnd(event) => {
            if let Some(thread_id) = event.new_thread_id {
                retain_thread_id(retained_thread_ids, thread_id);
            }
        }
        EventMsg::CollabAgentInteractionBegin(event) => {
            retain_thread_id(retained_thread_ids, event.receiver_thread_id);
        }
        EventMsg::CollabAgentInteractionEnd(event) => {
            retain_thread_id(retained_thread_ids, event.receiver_thread_id);
        }
        EventMsg::CollabWaitingBegin(event) => {
            retain_thread_ids(
                retained_thread_ids,
                event.receiver_thread_ids.iter().copied(),
            );
            retain_thread_ids(
                retained_thread_ids,
                event.receiver_agents.iter().map(|agent| agent.thread_id),
            );
        }
        EventMsg::CollabWaitingEnd(event) => {
            retain_thread_ids(retained_thread_ids, event.statuses.keys().copied());
            retain_thread_ids(
                retained_thread_ids,
                event.agent_statuses.iter().map(|agent| agent.thread_id),
            );
        }
        EventMsg::CollabCloseBegin(event) => {
            retain_thread_id(retained_thread_ids, event.receiver_thread_id);
        }
        EventMsg::CollabCloseEnd(event) => {
            retain_thread_id(retained_thread_ids, event.receiver_thread_id);
        }
        EventMsg::CollabResumeBegin(event) => {
            retain_thread_id(retained_thread_ids, event.receiver_thread_id);
        }
        EventMsg::CollabResumeEnd(event) => {
            retain_thread_id(retained_thread_ids, event.receiver_thread_id);
        }
        EventMsg::ItemStarted(event) => {
            collect_turn_item_thread_ids(&event.item, retained_thread_ids);
        }
        EventMsg::ItemCompleted(event) => {
            collect_turn_item_thread_ids(&event.item, retained_thread_ids);
        }
        _ => {}
    }
}

fn collect_turn_item_thread_ids(item: &CoreTurnItem, retained_thread_ids: &mut HashSet<String>) {
    match item {
        CoreTurnItem::SubAgentActivity(item) => {
            retain_thread_id(retained_thread_ids, item.agent_thread_id);
        }
        CoreTurnItem::CollabAgentToolCall(item) => {
            retain_thread_ids(
                retained_thread_ids,
                item.receiver_thread_ids.iter().copied(),
            );
            retain_thread_ids(
                retained_thread_ids,
                item.receiver_agents.iter().map(|agent| agent.thread_id),
            );
            retain_thread_ids(retained_thread_ids, item.agents_states.keys().copied());
        }
        _ => {}
    }
}

fn retain_thread_ids(
    retained_thread_ids: &mut HashSet<String>,
    thread_ids: impl IntoIterator<Item = ThreadId>,
) {
    retained_thread_ids.extend(
        thread_ids
            .into_iter()
            .map(|thread_id| thread_id.to_string()),
    );
}

fn retain_thread_id(retained_thread_ids: &mut HashSet<String>, thread_id: ThreadId) {
    retained_thread_ids.insert(thread_id.to_string());
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
#[path = "subagent_history_projection_tests.rs"]
mod tests;
