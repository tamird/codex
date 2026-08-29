//! Multi-agent picker navigation and labeling state for the TUI app.
//!
//! This module exists to keep the pure parts of multi-agent navigation out of [`crate::app::App`].
//! It owns the stable spawn-order cache used by the `/subagents` picker, keyboard next/previous
//! navigation, and the contextual footer label for the thread currently being watched.
//!
//! Responsibilities here are intentionally narrow:
//! - remember picker entries and their first-seen order
//! - remember which V2 child threads are owned by their parent agent
//! - answer traversal questions like "what is the next thread?"
//! - derive user-facing picker/footer text from cached thread metadata
//!
//! Responsibilities that stay in `App`:
//! - discovering threads from the backend
//! - deciding which thread is currently displayed
//! - mutating UI state such as switching threads or updating the footer widget
//!
//! The key invariant is that traversal follows first-seen spawn order rather than thread-id sort
//! order. Once a thread id is observed it keeps its place in the cycle even if the entry is later
//! updated or marked closed.

use crate::multi_agents::AgentPickerThreadEntry;
use crate::multi_agents::SubAgentActivityDisplay;
use crate::multi_agents::format_agent_picker_item_name;
use crate::multi_agents::next_agent_shortcut;
use crate::multi_agents::previous_agent_shortcut;
use codex_protocol::ThreadId;
use ratatui::text::Span;
use std::collections::HashMap;
use std::collections::HashSet;

/// Remote requests cannot be canceled, so bound root changes without dropping pending replies.
const MAX_IN_FLIGHT_PICKER_ROOTS: usize = 8;

/// Small state container for multi-agent picker ordering and labeling.
///
/// `App` owns thread lifecycle and UI side effects. This type keeps the pure rules for stable
/// spawn-order traversal, picker copy, and active-agent labels together and separately testable.
///
/// The core invariant is that `order` records first-seen thread ids exactly once, while `threads`
/// stores the latest metadata for those ids. Mutation is intentionally funneled through `upsert`,
/// `mark_closed`, and `clear` so those two collections do not drift semantically even if they are
/// temporarily out of sync during teardown races.
#[derive(Debug, Default)]
pub(crate) struct AgentNavigationState {
    /// Latest picker metadata for each tracked thread id.
    threads: HashMap<ThreadId, AgentPickerThreadEntry>,
    /// Stable first-seen traversal order for picker rows and keyboard cycling.
    order: Vec<ThreadId>,
    /// Threads with observed terminal liveness that must not be revived by delayed activity.
    stopped_threads: HashSet<ThreadId>,
    /// Spawned child threads whose instructions are owned by their parent agent.
    parent_owned_threads: HashSet<ThreadId>,
    /// Assigns a distinct generation to each remote picker refresh.
    picker_refresh_generation: u64,
    /// Shares each unfinished request and its timeout state across opens of the same root.
    picker_refreshes: HashMap<ThreadId, (u64, bool)>,
}

/// Direction of keyboard traversal through the stable picker order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AgentNavigationDirection {
    /// Move toward the entry that was seen earlier in spawn order, wrapping at the front.
    Previous,
    /// Move toward the entry that was seen later in spawn order, wrapping at the end.
    Next,
}

impl AgentNavigationState {
    pub(crate) fn begin_picker_refresh(&mut self, primary_thread_id: ThreadId) -> Option<u64> {
        if self.picker_refreshes.contains_key(&primary_thread_id)
            || self.picker_refreshes.len() >= MAX_IN_FLIGHT_PICKER_ROOTS
        {
            return None;
        }

        self.picker_refresh_generation = self.picker_refresh_generation.wrapping_add(1);
        let generation = self.picker_refresh_generation;
        self.picker_refreshes
            .insert(primary_thread_id, (generation, /*timed_out*/ false));
        Some(generation)
    }

    pub(crate) fn finish_picker_refresh(
        &mut self,
        root: ThreadId,
        generation: u64,
    ) -> Option<bool> {
        if self
            .picker_refreshes
            .get(&root)
            .is_none_or(|(pending, _)| *pending != generation)
        {
            return None;
        }
        self.picker_refreshes.remove(&root);
        Some(self.picker_refresh_generation == generation)
    }

    pub(crate) fn is_current_picker_refresh(&self, root: ThreadId, generation: u64) -> bool {
        self.picker_refreshes
            .get(&root)
            .is_some_and(|(pending, _)| *pending == generation)
    }

    pub(crate) fn is_current_picker_refresh_epoch(&self, root: ThreadId, generation: u64) -> bool {
        self.picker_refresh_generation == generation
            && self.is_current_picker_refresh(root, generation)
    }

    pub(crate) fn mark_picker_refresh_timed_out(
        &mut self,
        root: ThreadId,
        generation: u64,
    ) -> bool {
        if !self.is_current_picker_refresh(root, generation) {
            return false;
        }
        if let Some((_, timed_out)) = self.picker_refreshes.get_mut(&root) {
            *timed_out = true;
            return true;
        }
        false
    }

    pub(crate) fn is_timed_out_picker_refresh(&self, root: ThreadId) -> bool {
        self.picker_refreshes
            .get(&root)
            .is_some_and(|(_, timed_out)| *timed_out)
    }

    pub(crate) fn has_picker_refresh(&self, root: ThreadId) -> bool {
        self.picker_refreshes.contains_key(&root)
    }

    pub(crate) fn picker_refreshes_at_capacity(&self) -> bool {
        self.picker_refreshes.len() >= MAX_IN_FLIGHT_PICKER_ROOTS
    }

    /// Fresh server observations supersede pending snapshots even when cached status is unchanged.
    pub(crate) fn invalidate_pending_picker_snapshots(&mut self) {
        if !self.picker_refreshes.is_empty() {
            self.picker_refresh_generation = self.picker_refresh_generation.wrapping_add(1);
        }
    }

    /// Returns the cached picker entry for a specific thread id.
    ///
    /// Callers use this when they already know which thread they care about and need the last
    /// metadata captured for picker or footer rendering. If a caller assumes every tracked thread
    /// must be present here, shutdown races can turn that assumption into a panic elsewhere, so
    /// this stays optional.
    pub(crate) fn get(&self, thread_id: &ThreadId) -> Option<&AgentPickerThreadEntry> {
        self.threads.get(thread_id)
    }

    pub(crate) fn is_parent_owned(&self, thread_id: ThreadId) -> bool {
        self.parent_owned_threads.contains(&thread_id)
    }

    /// Marks a spawned child thread as view-only for direct user instructions.
    pub(crate) fn mark_parent_owned(&mut self, thread_id: ThreadId) {
        self.parent_owned_threads.insert(thread_id);
    }

    /// Inserts or updates a picker entry while preserving first-seen traversal order.
    ///
    /// The key invariant of this module is enforced here: a thread id is appended to `order` only
    /// the first time it is seen. Later updates may change nickname, role, or closed state, but
    /// they must not move the thread in the cycle or keyboard navigation would feel unstable.
    pub(crate) fn upsert(
        &mut self,
        thread_id: ThreadId,
        agent_nickname: Option<String>,
        agent_role: Option<String>,
        is_closed: bool,
    ) {
        if !self.threads.contains_key(&thread_id) {
            self.order.push(thread_id);
        }
        let (previous_agent_path, previous_is_running) = self
            .threads
            .get(&thread_id)
            .map(|entry| (entry.agent_path.clone(), entry.is_running))
            .unwrap_or((None, false));
        self.threads.insert(
            thread_id,
            AgentPickerThreadEntry {
                agent_nickname,
                agent_role,
                agent_path: previous_agent_path,
                is_running: previous_is_running && !is_closed,
                is_closed,
            },
        );
    }

    pub(crate) fn record_sub_agent_activity(&mut self, activity: SubAgentActivityDisplay) {
        if !self.threads.contains_key(&activity.thread_id) {
            self.order.push(activity.thread_id);
        }
        let entry =
            self.threads
                .entry(activity.thread_id)
                .or_insert_with(|| AgentPickerThreadEntry {
                    agent_nickname: None,
                    agent_role: None,
                    agent_path: None,
                    is_running: false,
                    is_closed: false,
                });
        entry.agent_path = Some(activity.agent_path);
        if activity.is_running_hint
            && !entry.is_closed
            && !self.stopped_threads.contains(&activity.thread_id)
        {
            self.mark_running(activity.thread_id);
        } else {
            self.mark_stopped(activity.thread_id);
        }
    }

    pub(crate) fn mark_running(&mut self, thread_id: ThreadId) {
        let was_running = self
            .threads
            .get(&thread_id)
            .is_some_and(|entry| entry.is_running);
        self.mark_running_from_snapshot(thread_id);
        if !was_running
            && self
                .threads
                .get(&thread_id)
                .is_some_and(|entry| entry.is_running)
            && !self.picker_refreshes.is_empty()
        {
            self.picker_refresh_generation = self.picker_refresh_generation.wrapping_add(1);
        }
    }

    pub(crate) fn mark_running_from_snapshot(&mut self, thread_id: ThreadId) {
        if self
            .threads
            .get(&thread_id)
            .is_some_and(|entry| entry.is_closed)
        {
            return;
        }
        self.stopped_threads.remove(&thread_id);
        self.set_running(thread_id, /*is_running*/ true);
    }

    pub(crate) fn mark_stopped(&mut self, thread_id: ThreadId) {
        if self.stopped_threads.insert(thread_id) && !self.picker_refreshes.is_empty() {
            self.picker_refresh_generation = self.picker_refresh_generation.wrapping_add(1);
        }
        self.set_running(thread_id, /*is_running*/ false);
    }

    pub(crate) fn set_running(&mut self, thread_id: ThreadId, is_running: bool) {
        if let Some(entry) = self.threads.get_mut(&thread_id) {
            entry.is_running = is_running;
        }
    }

    pub(crate) fn set_agent_path(&mut self, thread_id: ThreadId, agent_path: Option<String>) {
        if let Some(agent_path) = agent_path
            && let Some(entry) = self.threads.get_mut(&thread_id)
        {
            entry.agent_path = Some(agent_path);
        }
    }

    /// Marks a thread as closed without removing it from the traversal cache.
    ///
    /// Closed threads stay in the picker and in spawn order so users can still review them and so
    /// next/previous navigation does not reshuffle around disappearing entries. If a caller "cleans
    /// this up" by deleting the entry instead, wraparound navigation will silently change shape
    /// mid-session.
    pub(crate) fn mark_closed(&mut self, thread_id: ThreadId) {
        if let Some(entry) = self.threads.get_mut(&thread_id) {
            entry.is_closed = true;
            entry.is_running = false;
        } else {
            self.upsert(
                thread_id, /*agent_nickname*/ None, /*agent_role*/ None,
                /*is_closed*/ true,
            );
        }
    }

    /// Drops all cached picker state.
    ///
    /// This is used when `App` tears down thread event state and needs the picker cache to return
    /// to a pristine single-session state.
    pub(crate) fn clear(&mut self) {
        self.threads.clear();
        self.order.clear();
        self.stopped_threads.clear();
        self.parent_owned_threads.clear();
        self.picker_refresh_generation = self.picker_refresh_generation.wrapping_add(1);
    }

    /// Removes a tracked thread entirely from picker metadata and traversal order.
    ///
    /// This is reserved for entries that were only discovered opportunistically and never became
    /// replayable local threads. Keeping those around after the backend confirms they are gone
    /// would leave ghost rows in `/subagents`.
    /// Invalidate pending refreshes so their older snapshots cannot restore the removed entry.
    pub(crate) fn remove(&mut self, thread_id: ThreadId) {
        if self.threads.remove(&thread_id).is_some() && !self.picker_refreshes.is_empty() {
            self.picker_refresh_generation = self.picker_refresh_generation.wrapping_add(1);
        }
        self.order.retain(|candidate| *candidate != thread_id);
        self.stopped_threads.remove(&thread_id);
        self.parent_owned_threads.remove(&thread_id);
    }

    /// Returns whether the picker has a user-visible thread other than the primary one.
    pub(crate) fn has_non_primary_thread(&self, primary_thread_id: Option<ThreadId>) -> bool {
        self.threads.iter().any(|(thread_id, entry)| {
            Some(*thread_id) != primary_thread_id && !entry.is_goal_supervisor()
        })
    }

    /// Returns live picker rows in the same order users cycle through them.
    ///
    /// The `order` vector is intentionally historical and may briefly contain thread ids that no
    /// longer have cached metadata, so this filters through the map instead of assuming both
    /// collections are perfectly synchronized.
    pub(crate) fn ordered_threads(&self) -> Vec<(ThreadId, &AgentPickerThreadEntry)> {
        self.order
            .iter()
            .filter_map(|thread_id| self.threads.get(thread_id).map(|entry| (*thread_id, entry)))
            .collect()
    }

    pub(crate) fn ordered_path_backed_subagent_threads(
        &self,
        primary_thread_id: Option<ThreadId>,
    ) -> Vec<(ThreadId, &AgentPickerThreadEntry)> {
        self.ordered_threads()
            .into_iter()
            .filter(|(thread_id, entry)| {
                Some(*thread_id) != primary_thread_id
                    && !entry.is_goal_supervisor()
                    && entry
                        .agent_path
                        .as_deref()
                        .is_some_and(|agent_path| !agent_path.trim().is_empty())
            })
            .collect()
    }

    /// Returns tracked thread ids in the same stable order used by the picker.
    pub(crate) fn tracked_thread_ids(&self) -> Vec<ThreadId> {
        self.ordered_threads()
            .into_iter()
            .map(|(thread_id, _)| thread_id)
            .collect()
    }

    fn ordered_user_visible_threads(&self) -> Vec<(ThreadId, &AgentPickerThreadEntry)> {
        self.ordered_threads()
            .into_iter()
            .filter(|(_, entry)| !entry.is_goal_supervisor())
            .collect()
    }

    /// Returns the adjacent thread id for keyboard navigation in stable spawn order.
    ///
    /// The caller must pass the thread whose transcript is actually being shown to the user, not
    /// just whichever thread bookkeeping most recently marked active. If the wrong current thread
    /// is supplied, next/previous navigation will jump in a way that feels nondeterministic even
    /// though the cache itself is correct.
    pub(crate) fn adjacent_thread_id(
        &self,
        current_displayed_thread_id: Option<ThreadId>,
        direction: AgentNavigationDirection,
    ) -> Option<ThreadId> {
        let ordered_threads = self.ordered_user_visible_threads();
        if ordered_threads.len() < 2 {
            return None;
        }

        let current_thread_id = current_displayed_thread_id?;
        let Some(current_idx) = ordered_threads
            .iter()
            .position(|(thread_id, _)| *thread_id == current_thread_id)
        else {
            return match direction {
                AgentNavigationDirection::Next => ordered_threads.first(),
                AgentNavigationDirection::Previous => ordered_threads.last(),
            }
            .map(|(thread_id, _)| *thread_id);
        };
        let next_idx = match direction {
            AgentNavigationDirection::Next => (current_idx + 1) % ordered_threads.len(),
            AgentNavigationDirection::Previous => {
                if current_idx == 0 {
                    ordered_threads.len() - 1
                } else {
                    current_idx - 1
                }
            }
        };
        Some(ordered_threads[next_idx].0)
    }

    /// Derives the contextual footer label for the currently displayed thread.
    ///
    /// This intentionally returns `None` until there is more than one tracked thread so
    /// single-thread sessions do not waste footer space restating the obvious. When metadata for
    /// the displayed thread is missing, the label falls back to the same generic naming rules used
    /// by the picker.
    pub(crate) fn active_agent_label(
        &self,
        current_displayed_thread_id: Option<ThreadId>,
        primary_thread_id: Option<ThreadId>,
    ) -> Option<String> {
        let ordered_threads = self.ordered_user_visible_threads();
        if ordered_threads.len() <= 1 {
            return None;
        }

        let thread_id = current_displayed_thread_id?;
        if !ordered_threads
            .iter()
            .any(|(candidate, _)| *candidate == thread_id)
        {
            return None;
        }
        let is_primary = primary_thread_id == Some(thread_id);
        Some(
            self.threads
                .get(&thread_id)
                .map(|entry| {
                    if !is_primary
                        && let Some(agent_path) = entry
                            .agent_path
                            .as_deref()
                            .filter(|agent_path| !agent_path.trim().is_empty())
                    {
                        return format!("`{agent_path}`");
                    }
                    format_agent_picker_item_name(
                        entry.agent_nickname.as_deref(),
                        entry.agent_role.as_deref(),
                        is_primary,
                    )
                })
                .unwrap_or_else(|| {
                    format_agent_picker_item_name(
                        /*agent_nickname*/ None, /*agent_role*/ None, is_primary,
                    )
                }),
        )
    }

    /// Builds the `/subagents` picker subtitle from the same canonical bindings used by key handling.
    ///
    /// Keeping this text derived from the actual shortcut helpers prevents the picker copy from
    /// drifting if the bindings ever change on one platform.
    pub(crate) fn picker_subtitle() -> String {
        let previous: Span<'static> = previous_agent_shortcut().into();
        let next: Span<'static> = next_agent_shortcut().into();
        format!(
            "Select an agent to watch. {} previous, {} next.",
            previous.content, next.content
        )
    }

    #[cfg(test)]
    /// Returns only the ordered thread ids for focused tests of traversal invariants.
    ///
    /// This helper exists so tests can assert on ordering without embedding the full picker entry
    /// payload in every expectation.
    pub(crate) fn ordered_thread_ids(&self) -> Vec<ThreadId> {
        self.ordered_threads()
            .into_iter()
            .map(|(thread_id, _)| thread_id)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn populated_state() -> (AgentNavigationState, ThreadId, ThreadId, ThreadId) {
        let mut state = AgentNavigationState::default();
        let main_thread_id =
            ThreadId::from_string("00000000-0000-0000-0000-000000000101").expect("valid thread");
        let first_agent_id =
            ThreadId::from_string("00000000-0000-0000-0000-000000000102").expect("valid thread");
        let second_agent_id =
            ThreadId::from_string("00000000-0000-0000-0000-000000000103").expect("valid thread");

        state.upsert(
            main_thread_id,
            /*agent_nickname*/ None,
            /*agent_role*/ None,
            /*is_closed*/ false,
        );
        state.upsert(
            first_agent_id,
            Some("Robie".to_string()),
            Some("explorer".to_string()),
            /*is_closed*/ false,
        );
        state.upsert(
            second_agent_id,
            Some("Bob".to_string()),
            Some("worker".to_string()),
            /*is_closed*/ false,
        );

        (state, main_thread_id, first_agent_id, second_agent_id)
    }

    #[test]
    fn upsert_preserves_first_seen_order() {
        let (mut state, main_thread_id, first_agent_id, second_agent_id) = populated_state();

        state.upsert(
            first_agent_id,
            Some("Robie".to_string()),
            Some("worker".to_string()),
            /*is_closed*/ true,
        );

        assert_eq!(
            state.ordered_thread_ids(),
            vec![main_thread_id, first_agent_id, second_agent_id]
        );
    }

    #[test]
    fn parent_owned_state_is_removed_with_thread_metadata() {
        let (mut state, main_thread_id, first_agent_id, second_agent_id) = populated_state();
        let generation = state.begin_picker_refresh(main_thread_id).expect("refresh");
        state.mark_parent_owned(first_agent_id);
        assert!(state.is_parent_owned(first_agent_id));
        state.remove(first_agent_id);
        assert!(!state.is_parent_owned(first_agent_id));

        state.mark_parent_owned(second_agent_id);
        state.clear();
        assert!(state.begin_picker_refresh(first_agent_id).is_some());
        assert!(state.mark_picker_refresh_timed_out(main_thread_id, generation));
        assert!(state.is_timed_out_picker_refresh(main_thread_id));
        state.clear();
        assert!(state.is_timed_out_picker_refresh(main_thread_id));
        assert!(!state.is_parent_owned(second_agent_id));
        assert_eq!(state.begin_picker_refresh(main_thread_id), None);
        assert!(state.is_current_picker_refresh(main_thread_id, generation));
        assert_eq!(
            state.finish_picker_refresh(main_thread_id, generation),
            Some(false)
        );
        assert!(!state.is_timed_out_picker_refresh(main_thread_id));
        assert!(state.begin_picker_refresh(main_thread_id).is_some());
    }

    #[test]
    fn adopted_picker_refresh_accepts_timeout_without_accepting_stale_epoch() {
        let mut state = AgentNavigationState::default();
        let first_root = ThreadId::new();
        let second_root = ThreadId::new();
        let generation = state
            .begin_picker_refresh(first_root)
            .expect("first root refresh");

        state.clear();
        assert!(state.begin_picker_refresh(second_root).is_some());
        state.clear();

        assert!(state.is_current_picker_refresh(first_root, generation));
        assert!(!state.is_current_picker_refresh_epoch(first_root, generation));
        assert!(state.mark_picker_refresh_timed_out(first_root, generation));
        assert!(state.is_timed_out_picker_refresh(first_root));
        assert_eq!(state.begin_picker_refresh(first_root), None);
        assert_eq!(
            state.finish_picker_refresh(first_root, generation),
            Some(false)
        );
    }

    #[test]
    fn pending_picker_requests_are_bounded_without_dropping_a_root() {
        let mut state = AgentNavigationState::default();
        let root = ThreadId::new();
        let generation = state.begin_picker_refresh(root).expect("first request");
        for _ in 1..MAX_IN_FLIGHT_PICKER_ROOTS {
            assert!(state.begin_picker_refresh(ThreadId::new()).is_some());
        }
        let next_root = ThreadId::new();
        assert!(state.picker_refreshes_at_capacity());
        assert_eq!(state.begin_picker_refresh(root), None);
        assert_eq!(state.begin_picker_refresh(next_root), None);
        assert_eq!(state.finish_picker_refresh(root, generation), Some(false));
        assert!(state.begin_picker_refresh(next_root).is_some());
    }

    #[test]
    fn removing_thread_cached_during_picker_refresh_invalidates_stale_response() {
        let mut state = AgentNavigationState::default();
        let root = ThreadId::new();
        let child = ThreadId::new();
        let generation = state.begin_picker_refresh(root).expect("first request");

        state.remove(ThreadId::new());
        assert_eq!(state.picker_refresh_generation, generation);

        state.upsert(
            child, /*agent_nickname*/ None, /*agent_role*/ None, /*is_closed*/ false,
        );
        state.remove(child);

        assert!(state.get(&child).is_none());
        assert!(state.is_current_picker_refresh(root, generation));
        assert_eq!(state.begin_picker_refresh(root), None);
        assert_eq!(state.finish_picker_refresh(root, generation), Some(false));
        assert!(state.begin_picker_refresh(root).is_some());
    }

    #[test]
    fn stopping_thread_invalidates_pending_picker_refresh_without_preventing_revival() {
        let (mut state, root, child, _) = populated_state();
        state.mark_running(child);
        let generation = state.begin_picker_refresh(root).expect("first request");

        state.record_sub_agent_activity(SubAgentActivityDisplay {
            thread_id: child,
            agent_path: "/root/worker".to_string(),
            is_running_hint: false,
        });

        assert!(state.get(&child).is_some_and(|entry| !entry.is_running));
        assert!(state.is_current_picker_refresh(root, generation));
        assert!(!state.is_current_picker_refresh_epoch(root, generation));
        let stopped_generation = state.picker_refresh_generation;
        state.mark_stopped(child);
        assert_eq!(state.picker_refresh_generation, stopped_generation);
        assert_eq!(state.finish_picker_refresh(root, generation), Some(false));

        state.mark_running(child);
        assert!(state.get(&child).is_some_and(|entry| entry.is_running));
        let generation = state.begin_picker_refresh(root).expect("fresh request");
        state.mark_stopped(child);
        assert_eq!(state.finish_picker_refresh(root, generation), Some(false));
    }

    #[test]
    fn running_activity_invalidates_pending_picker_refresh_only_on_transition() {
        let (mut state, root, child, _) = populated_state();
        let generation = state.begin_picker_refresh(root).expect("first request");

        state.record_sub_agent_activity(SubAgentActivityDisplay {
            thread_id: child,
            agent_path: "/root/worker".to_string(),
            is_running_hint: true,
        });

        assert!(state.get(&child).is_some_and(|entry| entry.is_running));
        assert!(!state.is_current_picker_refresh_epoch(root, generation));
        let running_generation = state.picker_refresh_generation;
        state.record_sub_agent_activity(SubAgentActivityDisplay {
            thread_id: child,
            agent_path: "/root/worker".to_string(),
            is_running_hint: true,
        });
        assert_eq!(state.picker_refresh_generation, running_generation);
        assert_eq!(state.finish_picker_refresh(root, generation), Some(false));

        let generation = state.begin_picker_refresh(root).expect("fresh request");
        let new_child = ThreadId::new();
        state.record_sub_agent_activity(SubAgentActivityDisplay {
            thread_id: new_child,
            agent_path: "/root/new-worker".to_string(),
            is_running_hint: true,
        });
        assert!(state.get(&new_child).is_some_and(|entry| entry.is_running));
        assert_eq!(state.finish_picker_refresh(root, generation), Some(false));
    }

    #[test]
    fn authoritative_liveness_invalidates_refresh_but_snapshot_does_not() {
        let (mut state, root, child, snapshot_child) = populated_state();
        let generation = state.begin_picker_refresh(root).expect("first request");

        state.mark_running(child);

        assert!(state.get(&child).is_some_and(|entry| entry.is_running));
        assert!(!state.is_current_picker_refresh_epoch(root, generation));
        assert_eq!(state.finish_picker_refresh(root, generation), Some(false));

        let generation = state.begin_picker_refresh(root).expect("fresh request");
        state.mark_running_from_snapshot(snapshot_child);

        assert!(
            state
                .get(&snapshot_child)
                .is_some_and(|entry| entry.is_running)
        );
        assert!(state.is_current_picker_refresh_epoch(root, generation));
        assert_eq!(state.finish_picker_refresh(root, generation), Some(true));
    }

    #[test]
    fn adjacent_thread_id_wraps_in_spawn_order() {
        let (state, main_thread_id, first_agent_id, second_agent_id) = populated_state();

        assert_eq!(
            state.adjacent_thread_id(Some(second_agent_id), AgentNavigationDirection::Next),
            Some(main_thread_id)
        );
        assert_eq!(
            state.adjacent_thread_id(Some(second_agent_id), AgentNavigationDirection::Previous),
            Some(first_agent_id)
        );
        assert_eq!(
            state.adjacent_thread_id(Some(main_thread_id), AgentNavigationDirection::Previous),
            Some(second_agent_id)
        );
    }

    #[test]
    fn adjacent_thread_id_skips_goal_supervisors() {
        let (mut state, main_thread_id, worker_id, supervisor_id) = populated_state();
        state.upsert(
            supervisor_id,
            Some("Goal supervisor".to_string()),
            Some("goal_supervisor".to_string()),
            /*is_closed*/ false,
        );

        assert_eq!(
            state.adjacent_thread_id(Some(main_thread_id), AgentNavigationDirection::Next),
            Some(worker_id)
        );
        assert_eq!(
            state.adjacent_thread_id(Some(worker_id), AgentNavigationDirection::Next),
            Some(main_thread_id)
        );
        assert_eq!(
            state.adjacent_thread_id(Some(main_thread_id), AgentNavigationDirection::Previous),
            Some(worker_id)
        );
    }

    #[test]
    fn adjacent_thread_id_escapes_thread_hidden_by_late_supervisor_metadata() {
        let (mut state, main_thread_id, worker_id, supervisor_id) = populated_state();
        state.upsert(
            supervisor_id,
            Some("Goal supervisor".to_string()),
            Some("goal_supervisor".to_string()),
            /*is_closed*/ false,
        );

        assert_eq!(
            state.adjacent_thread_id(Some(supervisor_id), AgentNavigationDirection::Next),
            Some(main_thread_id)
        );
        assert_eq!(
            state.adjacent_thread_id(Some(supervisor_id), AgentNavigationDirection::Previous),
            Some(worker_id)
        );
        assert_eq!(
            state.active_agent_label(Some(supervisor_id), Some(main_thread_id)),
            None
        );
    }

    #[test]
    fn picker_subtitle_mentions_shortcuts() {
        let previous: Span<'static> = previous_agent_shortcut().into();
        let next: Span<'static> = next_agent_shortcut().into();
        let subtitle = AgentNavigationState::picker_subtitle();

        assert!(subtitle.contains(previous.content.as_ref()));
        assert!(subtitle.contains(next.content.as_ref()));
    }

    #[test]
    fn active_agent_label_tracks_current_thread() {
        let (state, main_thread_id, first_agent_id, _) = populated_state();

        assert_eq!(
            state.active_agent_label(Some(first_agent_id), Some(main_thread_id)),
            Some("Robie [explorer]".to_string())
        );
        assert_eq!(
            state.active_agent_label(Some(main_thread_id), Some(main_thread_id)),
            Some("Main [default]".to_string())
        );
    }

    #[test]
    fn active_agent_label_ignores_hidden_goal_supervisor_cardinality() {
        let mut state = AgentNavigationState::default();
        let main_thread_id = ThreadId::new();
        let supervisor_id = ThreadId::new();
        state.upsert(
            main_thread_id,
            /*agent_nickname*/ None,
            /*agent_role*/ None,
            /*is_closed*/ false,
        );
        state.upsert(
            supervisor_id,
            Some("Goal supervisor".to_string()),
            Some("goal_supervisor".to_string()),
            /*is_closed*/ false,
        );

        assert_eq!(
            state.active_agent_label(Some(main_thread_id), Some(main_thread_id)),
            None
        );
    }
}
