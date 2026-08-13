use super::AgentControl;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::protocol::AgentStatus;

const CURRENT_AGENT_STATUS_MESSAGE_BYTES: usize = 1024;
const CURRENT_AGENT_PATH_BYTES: usize = 4 * 1024;

/// One descendant that is currently visible in a root thread's agent registry.
///
/// Persisted spawn edges are intentionally not sufficient for membership. They retain ownership
/// and closure history, while this record reflects the loaded root's current lifecycle state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurrentAgentMember {
    pub thread_id: ThreadId,
    pub parent_thread_id: ThreadId,
    pub agent_path: Option<AgentPath>,
    /// Nickname retained by the root registry after the agent runtime is evicted.
    pub agent_nickname: Option<String>,
    /// Role retained by the root registry after the agent runtime is evicted.
    pub agent_role: Option<String>,
    pub status: AgentStatus,
    pub last_task_message: Option<String>,
}

/// One root-scoped current membership projection selected by [`ThreadManager`](crate::ThreadManager).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurrentAgentMembershipSnapshot {
    /// Root thread whose `AgentRegistry` owns this projection.
    pub registry_root_thread_id: ThreadId,
    /// Current descendants in stable canonical path and thread ID order.
    pub members: Vec<CurrentAgentMember>,
}

impl AgentControl {
    pub(crate) async fn current_agent_members(&self) -> CodexResult<Vec<CurrentAgentMember>> {
        let state = self.upgrade_for_tools()?;
        let mut members = Vec::new();

        for metadata in self.state.live_agents() {
            let (Some(thread_id), Some(parent_thread_id)) =
                (metadata.agent_id, metadata.parent_thread_id)
            else {
                continue;
            };
            let status = match state.get_thread(thread_id).await {
                Ok(thread) => thread.agent_status().await,
                Err(err)
                    if matches!(err.details(), CodexErrorDetails::ThreadNotFound(_))
                        && metadata.lifecycle.is_visible_when_cold() =>
                {
                    metadata
                        .lifecycle
                        .cold_terminal_status()
                        .unwrap_or(AgentStatus::Completed(None))
                }
                Err(err) if matches!(err.details(), CodexErrorDetails::ThreadNotFound(_)) => {
                    continue;
                }
                Err(err) => return Err(err),
            };
            let agent_path = metadata
                .agent_path
                .filter(|path| path.as_str().len() <= CURRENT_AGENT_PATH_BYTES);
            members.push(CurrentAgentMember {
                thread_id,
                parent_thread_id,
                agent_path,
                agent_nickname: metadata.agent_nickname,
                agent_role: metadata.agent_role,
                status: bounded_current_agent_status(status),
                last_task_message: metadata.last_task_message,
            });
        }

        members.sort_by(|left, right| {
            left.agent_path
                .as_deref()
                .unwrap_or_default()
                .cmp(right.agent_path.as_deref().unwrap_or_default())
                .then_with(|| left.thread_id.to_string().cmp(&right.thread_id.to_string()))
        });
        Ok(members)
    }

    /// Stop and forget exact current descendants without changing persisted ownership.
    pub(crate) async fn evict_current_agent_ids(
        &self,
        thread_ids: &[ThreadId],
    ) -> CodexResult<usize> {
        self.upgrade_for_tools()?;
        let mut members = thread_ids
            .iter()
            .copied()
            .filter_map(|thread_id| {
                let metadata = self.state.agent_metadata_for_thread(thread_id)?;
                Some((metadata.depth.unwrap_or_default(), thread_id))
            })
            .collect::<Vec<_>>();
        members.sort_unstable_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then_with(|| right.1.to_string().cmp(&left.1.to_string()))
        });
        members.dedup_by_key(|(_, thread_id)| *thread_id);
        if members.is_empty() {
            return Ok(0);
        }
        let member_ids = members
            .into_iter()
            .map(|(_, thread_id)| thread_id)
            .collect::<Vec<_>>();
        let mut first_error = None;
        for thread_id in &member_ids {
            if let Err(err) = self.shutdown_live_agent(*thread_id).await
                && !matches!(
                    err.details(),
                    CodexErrorDetails::ThreadNotFound(_) | CodexErrorDetails::InternalAgentDied
                )
                && first_error.is_none()
            {
                first_error = Some(err);
            }
        }
        match first_error {
            Some(err) => Err(CodexErr::Fatal(format!(
                "failed to evict current agent identities: {err}"
            ))),
            None => Ok(member_ids.len()),
        }
    }
}

/// Bound status text once in the canonical projection shared by model and app-server callers.
pub(crate) fn bounded_current_agent_status(status: AgentStatus) -> AgentStatus {
    match status {
        AgentStatus::Completed(Some(message)) => AgentStatus::Completed(Some(
            bounded_utf8_with_ellipsis(&message, CURRENT_AGENT_STATUS_MESSAGE_BYTES),
        )),
        AgentStatus::Errored(message) => AgentStatus::Errored(bounded_utf8_with_ellipsis(
            &message,
            CURRENT_AGENT_STATUS_MESSAGE_BYTES,
        )),
        status => status,
    }
}

fn bounded_utf8_with_ellipsis(message: &str, maximum_bytes: usize) -> String {
    if message.len() <= maximum_bytes {
        return message.to_string();
    }
    let mut end = maximum_bytes.saturating_sub(3);
    while !message.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    format!("{}...", &message[..end])
}
