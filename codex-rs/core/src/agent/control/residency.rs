use super::AgentControl;
use crate::agent::AgentStatus;
use crate::agent::registry::AgentLifecycle;
use crate::codex_thread::CodexThread;
use crate::config::Config;
use crate::goal_supervisor::is_goal_supervisor_helper_source;
use crate::thread_manager::ThreadManagerState;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::SessionSource;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::sync::Notify;
use tracing::warn;

/// Idle agents remain warm without making the execution limit a memory-retention limit.
const DEFAULT_AGENT_RESIDENCY_LIMIT: usize = 8;

/// Session-scoped LRU of loaded V1 and V2 agents that can be reconstructed from persisted rollout.
#[derive(Default)]
pub(super) struct AgentResidency {
    /// Loaded residents plus in-flight reservations for new or reloaded agents.
    state: Mutex<AgentResidencyState>,
    /// Prevents simultaneous completed agents from starting redundant eviction tasks.
    trim_scheduled: AtomicBool,
    /// Records completion notices that arrive while an eviction task is already running.
    trim_generation: AtomicUsize,
    /// Wakes a deferred trim when another completion or resident removal changes its scan.
    trim_requested: Notify,
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
        let protected_thread_ids = protected_thread_id.into_iter().collect();
        let execution_capacity = config
            .effective_agent_max_threads(multi_agent_version)
            .unwrap_or(usize::MAX);
        let resident_capacity = execution_capacity.min(DEFAULT_AGENT_RESIDENCY_LIMIT);
        Arc::clone(&self.agent_residency)
            .reserve_slot(
                self,
                state,
                resident_capacity,
                execution_capacity,
                protected_thread_ids,
            )
            .await
    }

    /// Unloads completed residents after their turn and completion notification finish.
    pub(crate) fn schedule_agent_residency_trim(
        &self,
        config: &Config,
        multi_agent_version: MultiAgentVersion,
        session_source: &SessionSource,
    ) {
        if !is_resident_session_source(session_source) {
            return;
        }

        let execution_capacity = config
            .effective_agent_max_threads(multi_agent_version)
            .unwrap_or(usize::MAX);
        let resident_capacity = execution_capacity.min(DEFAULT_AGENT_RESIDENCY_LIMIT);
        let residency = Arc::clone(&self.agent_residency);
        // Scans temporarily pop candidates from the LRU across awaits. Notify an existing
        // worker before trusting that transient count to decide whether to start a new one.
        residency.trim_generation.fetch_add(1, Ordering::AcqRel);
        residency.trim_requested.notify_one();
        if residency.resident_count() <= resident_capacity {
            return;
        }
        if residency.trim_scheduled.swap(true, Ordering::AcqRel) {
            return;
        }

        let control = self.clone();
        tokio::spawn(async move {
            loop {
                let observed_generation = residency.trim_generation.load(Ordering::Acquire);
                residency
                    .trim_idle_residents(&control, resident_capacity)
                    .await;
                residency.trim_scheduled.store(false, Ordering::Release);
                if residency.trim_generation.load(Ordering::Acquire) == observed_generation
                    || residency
                        .trim_scheduled
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                {
                    return;
                }
            }
        });
    }

    pub(super) fn touch_loaded_agent_residency(&self, thread: &CodexThread) {
        if is_resident_candidate(thread) {
            self.agent_residency.touch(thread.session.thread_id);
        }
    }

    pub(crate) fn forget_agent_residency(&self, thread_id: ThreadId) {
        self.agent_residency.remove(thread_id);
    }
}

/// Result of scanning the current LRU once for an unloadable resident.
enum EvictionResult {
    Unloaded,
    Retry,
    Unavailable,
}

struct EvictionScan {
    result: EvictionResult,
    blockers: Vec<ResidencyBlocker>,
}

enum ResidencyBlocker {
    Transition(Arc<AgentLifecycle>),
    CompletionWatcher(Arc<AgentLifecycle>),
}

impl AgentResidency {
    async fn reserve_slot(
        self: Arc<Self>,
        control: &AgentControl,
        manager: &Arc<ThreadManagerState>,
        resident_capacity: usize,
        execution_capacity: usize,
        protected_thread_ids: Vec<ThreadId>,
    ) -> CodexResult<AgentResidencySlot> {
        loop {
            if self.try_reserve_pending_slot(resident_capacity) {
                return Ok(AgentResidencySlot {
                    residency: self,
                    active: true,
                });
            }
            let EvictionScan {
                result,
                blockers: _,
            } = self
                .try_unload_one_resident(control, manager, &protected_thread_ids)
                .await;
            // Admission can hold another lifecycle transition, so only the independent trim
            // worker may wait for the blockers found by this scan.
            match result {
                EvictionResult::Unloaded => {}
                EvictionResult::Retry => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                EvictionResult::Unavailable => {
                    if self.try_reserve_pending_slot(execution_capacity) {
                        return Ok(AgentResidencySlot {
                            residency: self,
                            active: true,
                        });
                    }
                    return Err(CodexErr::new(CodexErrorDetails::AgentLimitReached {
                        max_threads: execution_capacity,
                    }));
                }
            }
        }
    }

    async fn trim_idle_residents(&self, control: &AgentControl, resident_capacity: usize) {
        while self.resident_count() > resident_capacity {
            let Ok(manager) = control.upgrade() else {
                return;
            };
            let EvictionScan { result, blockers } =
                self.try_unload_one_resident(control, &manager, &[]).await;
            // Waiting for a lifecycle must not keep the thread manager alive during shutdown.
            drop(manager);
            match result {
                EvictionResult::Unloaded => continue,
                EvictionResult::Retry | EvictionResult::Unavailable => {}
            }
            if blockers.is_empty() {
                return;
            }
            let mut ready = blockers
                .into_iter()
                .map(|blocker| async move {
                    match blocker {
                        ResidencyBlocker::Transition(lifecycle) => {
                            drop(lifecycle.lock_transition().await);
                        }
                        ResidencyBlocker::CompletionWatcher(lifecycle) => {
                            lifecycle.wait_for_completion_watcher().await;
                        }
                    }
                })
                .collect::<FuturesUnordered<_>>();
            // No transition guard is retained from the scan. A new completion can make a
            // different resident unloadable while all of these blockers are still pending.
            tokio::select! {
                Some(()) = ready.next() => {}
                () = self.trim_requested.notified() => {}
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
        protected_thread_ids: &[ThreadId],
    ) -> EvictionScan {
        let candidates_to_scan = self.resident_count();
        let mut saw_active_watcher = false;
        let mut blockers = Vec::new();
        for _ in 0..candidates_to_scan {
            let Some(candidate_thread_id) = self.pop_lru_candidate(protected_thread_ids) else {
                break;
            };
            let registered_lifecycle = control
                .get_agent_metadata(candidate_thread_id)
                .map(|metadata| metadata.lifecycle);
            let lifecycle = registered_lifecycle.clone().unwrap_or_default();
            // Reload can hold a descendant's transition while promotion holds this ancestor's.
            // Eviction is opportunistic: never invert that lock order by waiting here.
            let Some(_transition) = lifecycle.try_lock_transition() else {
                self.touch(candidate_thread_id);
                blockers.push(ResidencyBlocker::Transition(lifecycle));
                continue;
            };
            let Some(candidate_thread) = manager
                .get_thread(candidate_thread_id)
                .await
                .ok()
                .filter(|thread| is_resident_candidate(thread))
            else {
                continue;
            };
            if !Arc::ptr_eq(
                &control.state,
                &candidate_thread.session.services.agent_control.state,
            ) {
                continue;
            }
            // A failed ownership transfer can restore the same control with a new lifecycle.
            let registration_matches = match (
                registered_lifecycle,
                control
                    .get_agent_metadata(candidate_thread_id)
                    .map(|metadata| metadata.lifecycle),
            ) {
                (Some(previous), Some(current)) => Arc::ptr_eq(&previous, &current),
                (None, None) => true,
                (Some(_), None) | (None, Some(_)) => false,
            };
            if !registration_matches {
                self.touch(candidate_thread_id);
                continue;
            }
            if !is_unloadable(candidate_thread.as_ref()).await {
                self.touch(candidate_thread_id);
                continue;
            }
            if lifecycle.completion_watcher_active() {
                self.touch(candidate_thread_id);
                saw_active_watcher = true;
                blockers.push(ResidencyBlocker::CompletionWatcher(lifecycle));
                continue;
            }
            let status = candidate_thread.agent_status().await;
            if matches!(
                status,
                AgentStatus::Completed(_)
                    | AgentStatus::Errored(_)
                    | AgentStatus::Interrupted
                    | AgentStatus::Shutdown
            ) {
                lifecycle.remember_cold_terminal_status(
                    status,
                    candidate_thread.multi_agent_version() == Some(MultiAgentVersion::V2),
                );
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
            return EvictionScan {
                result: EvictionResult::Unloaded,
                blockers: Vec::new(),
            };
        }
        EvictionScan {
            result: if saw_active_watcher {
                EvictionResult::Retry
            } else {
                EvictionResult::Unavailable
            },
            blockers,
        }
    }

    fn resident_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .residents
            .len()
    }

    fn pop_lru_candidate(&self, protected_thread_ids: &[ThreadId]) -> Option<ThreadId> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let candidates_to_scan = state.residents.len();
        for _ in 0..candidates_to_scan {
            let candidate_thread_id = state.residents.pop_front()?;
            if protected_thread_ids.contains(&candidate_thread_id) {
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
        self.trim_requested.notify_one();
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
        && !is_goal_supervisor_helper_source(session_source)
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
