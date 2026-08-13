use super::AgentControl;
use crate::agent::AgentStatus;
use crate::codex_thread::CodexThread;
use crate::config::Config;
use crate::thread_manager::ThreadManagerState;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::SessionSource;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use tracing::warn;

/// Session-scoped LRU of loaded V1 and V2 agents that can be reconstructed from persisted rollout.
#[derive(Default)]
pub(super) struct AgentResidency {
    /// Loaded residents plus in-flight reservations for new or reloaded agents.
    state: Mutex<AgentResidencyState>,
}

/// Mutable residency accounting protected by `AgentResidency::state`.
#[derive(Default)]
struct AgentResidencyState {
    /// Loaded agent IDs, ordered from least to most recently used.
    residents: VecDeque<ThreadId>,
    /// Slots reserved before a thread has finished loading and can enter `residents`.
    pending_slots: usize,
}

/// A pending resident slot that must be committed after a thread loads successfully.
pub(super) struct AgentResidencySlot {
    /// Shared LRU that owns the pending slot.
    residency: Arc<AgentResidency>,
    /// Whether dropping this reservation must return the pending slot.
    active: bool,
}

impl AgentResidencySlot {
    pub(super) fn commit(mut self, thread_id: ThreadId) {
        self.residency.commit_slot(thread_id);
        self.active = false;
    }
}

impl Drop for AgentResidencySlot {
    fn drop(&mut self) {
        if self.active {
            self.residency.release_pending_slot();
        }
    }
}

impl AgentControl {
    pub(super) async fn reserve_agent_residency_slot(
        &self,
        state: &Arc<ThreadManagerState>,
        config: &Config,
        multi_agent_version: MultiAgentVersion,
        protected_thread_id: Option<ThreadId>,
    ) -> CodexResult<AgentResidencySlot> {
        let capacity = config
            .effective_agent_max_threads(multi_agent_version)
            .unwrap_or(usize::MAX);
        Arc::clone(&self.agent_residency)
            .reserve_slot(self, state, capacity, protected_thread_id)
            .await
    }

    pub(super) async fn touch_loaded_agent_residency(
        &self,
        state: &Arc<ThreadManagerState>,
        thread_id: ThreadId,
    ) {
        if let Ok(thread) = state.get_thread(thread_id).await
            && is_resident_candidate(thread.as_ref())
        {
            self.agent_residency.touch(thread_id);
        }
    }

    pub(super) fn forget_agent_residency(&self, thread_id: ThreadId) {
        self.agent_residency.remove(thread_id);
    }
}

/// Result of scanning the current LRU once for an unloadable resident.
enum EvictionResult {
    Unloaded,
    Retry,
    Unavailable,
}

impl AgentResidency {
    async fn reserve_slot(
        self: Arc<Self>,
        control: &AgentControl,
        manager: &Arc<ThreadManagerState>,
        capacity: usize,
        protected_thread_id: Option<ThreadId>,
    ) -> CodexResult<AgentResidencySlot> {
        loop {
            if self.try_reserve_pending_slot(capacity) {
                return Ok(AgentResidencySlot {
                    residency: self,
                    active: true,
                });
            }
            match self
                .try_unload_one_resident(control, manager, protected_thread_id)
                .await
            {
                EvictionResult::Unloaded | EvictionResult::Retry => {}
                EvictionResult::Unavailable => {
                    return Err(CodexErr::new(CodexErrorDetails::AgentLimitReached {
                        max_threads: capacity,
                    }));
                }
            }
        }
    }

    fn try_reserve_pending_slot(&self, capacity: usize) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.residents.len().saturating_add(state.pending_slots) >= capacity {
            return false;
        }
        state.pending_slots += 1;
        true
    }

    async fn try_unload_one_resident(
        &self,
        control: &AgentControl,
        manager: &Arc<ThreadManagerState>,
        protected_thread_id: Option<ThreadId>,
    ) -> EvictionResult {
        let candidates_to_scan = self.resident_count();
        for _ in 0..candidates_to_scan {
            let Some(candidate_thread_id) = self.pop_lru_candidate(protected_thread_id) else {
                return EvictionResult::Unavailable;
            };
            let lifecycle = control
                .get_agent_metadata(candidate_thread_id)
                .map(|metadata| metadata.lifecycle)
                .unwrap_or_default();
            let _transition = lifecycle.lock_transition().await;
            let Some(candidate_thread) = manager
                .get_thread(candidate_thread_id)
                .await
                .ok()
                .filter(|thread| is_resident_candidate(thread))
            else {
                continue;
            };
            if !is_unloadable(candidate_thread.as_ref()).await {
                self.touch(candidate_thread_id);
                continue;
            }
            if lifecycle.completion_watcher_active() {
                self.touch(candidate_thread_id);
                drop(_transition);
                lifecycle.wait_for_completion_watcher().await;
                return EvictionResult::Retry;
            }
            if let Err(err) = control
                .unload_agent_thread(manager, candidate_thread_id)
                .await
            {
                warn!(
                    "failed to shut down resident agent before unloading {candidate_thread_id}: {err}"
                );
                self.touch(candidate_thread_id);
                continue;
            }
            return EvictionResult::Unloaded;
        }
        EvictionResult::Unavailable
    }

    fn resident_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .residents
            .len()
    }

    fn pop_lru_candidate(&self, protected_thread_id: Option<ThreadId>) -> Option<ThreadId> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let candidates_to_scan = state.residents.len();
        for _ in 0..candidates_to_scan {
            let candidate_thread_id = state.residents.pop_front()?;
            if Some(candidate_thread_id) == protected_thread_id {
                state.residents.push_back(candidate_thread_id);
                continue;
            }
            return Some(candidate_thread_id);
        }
        None
    }

    fn touch(&self, thread_id: ThreadId) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        touch_resident(&mut state.residents, thread_id);
    }

    fn remove(&self, thread_id: ThreadId) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .residents
            .retain(|resident_thread_id| *resident_thread_id != thread_id);
    }

    fn commit_slot(&self, thread_id: ThreadId) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.pending_slots = state.pending_slots.saturating_sub(1);
        touch_resident(&mut state.residents, thread_id);
    }

    fn release_pending_slot(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.pending_slots = state.pending_slots.saturating_sub(1);
    }
}

fn touch_resident(residents: &mut VecDeque<ThreadId>, thread_id: ThreadId) {
    residents.retain(|resident_thread_id| *resident_thread_id != thread_id);
    residents.push_back(thread_id);
}

fn is_resident_candidate(thread: &CodexThread) -> bool {
    is_resident_session_source(&thread.session_source)
}

pub(super) fn is_resident_session_source(session_source: &SessionSource) -> bool {
    matches!(session_source, SessionSource::SubAgent(_))
        && !crate::goal_supervisor::is_goal_supervisor_helper_source(session_source)
}

pub(super) async fn is_unloadable(thread: &CodexThread) -> bool {
    let has_active_task = thread
        .session
        .active_turn
        .lock()
        .await
        .as_ref()
        .is_some_and(|active_turn| active_turn.task.is_some());
    matches!(
        thread.agent_status().await,
        AgentStatus::Completed(_)
            | AgentStatus::Errored(_)
            | AgentStatus::Interrupted
            | AgentStatus::Shutdown
    ) && !has_active_task
        && !thread.session.input_queue.has_pending_mailbox_items().await
}

impl AgentControl {
    /// Persist and stop a loaded agent without releasing its addressability metadata.
    pub(super) async fn unload_agent_thread(
        &self,
        manager: &Arc<ThreadManagerState>,
        thread_id: ThreadId,
    ) -> CodexResult<bool> {
        let Ok(thread) = manager.get_thread(thread_id).await else {
            return Ok(false);
        };
        thread.ensure_rollout_materialized().await;
        thread.flush_rollout().await?;
        let environments = thread.environment_selections().await;
        thread.shutdown_and_wait().await?;
        thread
            .session
            .services
            .agent_control
            .state
            .save_evicted_environments(thread_id, environments);
        Ok(manager.remove_thread(&thread_id).await.is_some())
    }
}

#[cfg(test)]
#[path = "residency_tests.rs"]
mod tests;
