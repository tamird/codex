use super::*;

/// Retains one current agent registry across archive or delete runtime teardown.
pub struct CurrentAgentMembershipHandle {
    /// Registry selected while the requested current subtree was fenced.
    control: Option<AgentControl>,
    state: Arc<ThreadManagerState>,
    /// Original registry root, retained even when that root is an exact storage success.
    registry_root_thread_id: Option<ThreadId>,
    /// Target plus descendants reachable through persisted Open ownership edges.
    persisted_candidate_thread_ids: Vec<ThreadId>,
    /// Registered current-only children keyed to their canonical registered parent.
    current_only_parent_by_thread_id: HashMap<ThreadId, ThreadId>,
    /// Every persisted and current-only identity protected by this operation.
    fenced_thread_ids: Vec<ThreadId>,
    /// Whether teardown removed a runtime while preserving its current identity.
    unloaded_current_runtime: AtomicBool,
}

impl CurrentAgentMembershipHandle {
    /// Exact open-owned subtree captured and fenced before archive or delete storage mutation.
    pub fn candidate_thread_ids(&self) -> &[ThreadId] {
        &self.persisted_candidate_thread_ids
    }

    /// Add captured current-only descendants below exact successful persisted candidates.
    pub fn current_ids_with_current_only_descendants(
        &self,
        thread_ids: &[ThreadId],
    ) -> Vec<ThreadId> {
        let fenced = self
            .fenced_thread_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut expanded = thread_ids
            .iter()
            .copied()
            .filter(|thread_id| fenced.contains(thread_id))
            .collect::<HashSet<_>>();
        loop {
            let mut changed = false;
            for (thread_id, parent_thread_id) in &self.current_only_parent_by_thread_id {
                if expanded.contains(parent_thread_id) && expanded.insert(*thread_id) {
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        let mut expanded = expanded.into_iter().collect::<Vec<_>>();
        expanded.sort_by_key(ToString::to_string);
        expanded
    }

    /// Stop one fenced runtime while retaining its current registry identity.
    ///
    /// Archive and delete storage operations learn their exact successes only after every live
    /// writer has stopped. Failed candidates must remain addressable, so runtime teardown records
    /// a cold-visible status and leaves identity removal to [`Self::evict_exact`].
    pub async fn unload_candidate_runtime_preserving_identity(
        &self,
        thread_id: ThreadId,
    ) -> CodexResult<bool> {
        if !self.fenced_thread_ids.contains(&thread_id) {
            return Err(CodexErr::UnsupportedOperation(format!(
                "thread {thread_id} is not part of this archive or delete operation"
            )));
        }
        let Some(control) = self.control.as_ref() else {
            return Ok(false);
        };
        let Some(metadata) = control.get_agent_metadata(thread_id) else {
            return Ok(false);
        };
        let lifecycle = metadata.lifecycle;
        let _transition = lifecycle.lock_transition().await;
        let Ok(thread) = self.state.get_thread(thread_id).await else {
            return Ok(false);
        };
        let cold_status = match thread.agent_status().await {
            status @ (AgentStatus::Completed(_)
            | AgentStatus::Errored(_)
            | AgentStatus::Interrupted
            | AgentStatus::Shutdown) => status,
            AgentStatus::PendingInit | AgentStatus::Running | AgentStatus::NotFound => {
                AgentStatus::Interrupted
            }
        };
        thread.ensure_rollout_materialized().await;
        let flush_result = thread.flush_rollout().await;
        let removed = self.state.remove_thread(&thread_id).await.is_some();
        lifecycle.remember_cold_terminal_status(cold_status, /*visible_when_cold*/ true);
        control.forget_agent_residency(thread_id);
        if removed {
            self.unloaded_current_runtime.store(true, Ordering::Release);
        }
        let shutdown_result = thread.shutdown_and_wait().await;
        flush_result?;
        shutdown_result?;
        Ok(removed)
    }

    /// Evict exactly the successfully archived or deleted current identities.
    pub async fn evict_exact(self, thread_ids: &[ThreadId]) -> CodexResult<usize> {
        let Some(control) = self.control.as_ref() else {
            return Ok(0);
        };
        let result = control.evict_current_agent_ids(thread_ids).await;
        if let Some(registry_root_thread_id) = self.registry_root_thread_id {
            self.state
                .reconcile_retained_agent_control(registry_root_thread_id, control.clone());
        }
        result
    }
}

impl Drop for CurrentAgentMembershipHandle {
    fn drop(&mut self) {
        if self.unloaded_current_runtime.load(Ordering::Acquire)
            && let (Some(registry_root_thread_id), Some(control)) =
                (self.registry_root_thread_id, self.control.as_ref())
        {
            self.state
                .reconcile_retained_agent_control(registry_root_thread_id, control.clone());
        }
        self.state
            .unmark_threads_for_membership_eviction(self.fenced_thread_ids.iter().copied());
    }
}

impl ThreadManager {
    /// Returns the descendants currently visible to the loaded root thread's own agent control.
    ///
    /// Persisted spawn edges remain available to archive and ownership operations, but they do
    /// not make an unloaded historical descendant current.
    pub async fn current_agent_members(
        &self,
        root_thread_id: ThreadId,
    ) -> CodexResult<Vec<CurrentAgentMember>> {
        Ok(self
            .current_agent_membership_snapshot(root_thread_id)
            .await?
            .members)
    }

    /// Return the canonical root registry and its current member projection for one identity.
    pub async fn current_agent_membership_snapshot(
        &self,
        thread_id: ThreadId,
    ) -> CodexResult<CurrentAgentMembershipSnapshot> {
        let _lifecycle_mutation = self.state.lock_lifecycle_mutation().await;
        let (registry_root_thread_id, control) = self
            .agent_control_containing(thread_id)
            .await
            .ok_or(CodexErr::ThreadNotFound(thread_id))?;
        let members = control.current_agent_members().await?;
        let members = if thread_id == registry_root_thread_id {
            members
        } else {
            let subtree_thread_ids = control
                .current_membership_subtree_thread_ids(thread_id)
                .into_iter()
                .filter(|member_thread_id| *member_thread_id != thread_id)
                .collect::<HashSet<_>>();
            members
                .into_iter()
                .filter(|member| subtree_thread_ids.contains(&member.thread_id))
                .collect()
        };
        Ok(CurrentAgentMembershipSnapshot {
            registry_root_thread_id,
            members,
        })
    }

    /// Capture and fence the open-owned persisted and current subtree before archive or delete.
    ///
    /// Spawn persistence and lazy identity registration use the same lifecycle mutex, so the
    /// returned candidate set cannot gain another descendant until this handle is dropped.
    pub async fn prepare_current_agent_membership_eviction(
        &self,
        thread_id: ThreadId,
    ) -> CodexResult<CurrentAgentMembershipHandle> {
        let lifecycle_mutation = self.state.lock_lifecycle_mutation().await;
        let control_entry = self.agent_control_containing(thread_id).await;
        let registry_root_thread_id = control_entry.as_ref().map(|(root, _)| *root);
        let control = control_entry.map(|(_, control)| control);
        let mut persisted_candidate_thread_ids = vec![thread_id];
        let mut seen_thread_ids = HashSet::from([thread_id]);
        if let Some(agent_graph_store) = self.state.agent_graph_store() {
            for descendant_id in agent_graph_store
                .list_thread_spawn_descendants(
                    thread_id,
                    Some(codex_agent_graph_store::ThreadSpawnEdgeStatus::Open),
                )
                .await
                .map_err(|err| {
                    CodexErr::Fatal(format!(
                        "failed to prepare thread-spawn descendants for archive or delete: {err}"
                    ))
                })?
            {
                if seen_thread_ids.insert(descendant_id) {
                    persisted_candidate_thread_ids.push(descendant_id);
                }
            }
        }
        let persisted_candidate_thread_id_set = persisted_candidate_thread_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut fenced_thread_ids = persisted_candidate_thread_ids.clone();
        let mut current_only_parent_by_thread_id = HashMap::new();
        if let Some(control) = control.as_ref() {
            current_only_parent_by_thread_id = control
                .current_only_descendant_parents_within(
                    thread_id,
                    &persisted_candidate_thread_id_set,
                    self.state.agent_graph_store().as_deref(),
                )
                .await?;
            fenced_thread_ids.extend(current_only_parent_by_thread_id.keys().copied());
        }
        fenced_thread_ids.sort_by_key(ToString::to_string);
        fenced_thread_ids.dedup();
        self.state
            .mark_threads_for_membership_eviction(fenced_thread_ids.iter().copied());
        drop(lifecycle_mutation);
        Ok(CurrentAgentMembershipHandle {
            control,
            state: Arc::clone(&self.state),
            registry_root_thread_id,
            persisted_candidate_thread_ids,
            current_only_parent_by_thread_id,
            fenced_thread_ids,
            unloaded_current_runtime: AtomicBool::new(false),
        })
    }

    async fn agent_control_containing(
        &self,
        thread_id: ThreadId,
    ) -> Option<(ThreadId, AgentControl)> {
        #[expect(
            clippy::needless_collect,
            reason = "collecting releases the thread-map read guard before querying each AgentControl"
        )]
        let threads = self
            .state
            .threads
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut candidates = threads
            .into_iter()
            .filter_map(|thread| {
                let control = thread.session.services.agent_control.clone();
                let registry_root_thread_id = control.current_membership_root_thread_id();
                let requested_is_root = registry_root_thread_id == thread_id;
                let requested_owns_current_descendant = control
                    .current_membership_subtree_thread_ids(thread_id)
                    .into_iter()
                    .any(|member_thread_id| member_thread_id != thread_id);
                if !requested_is_root
                    && control.get_agent_metadata(thread_id).is_none()
                    && !requested_owns_current_descendant
                {
                    return None;
                }
                Some((
                    requested_is_root,
                    true,
                    registry_root_thread_id,
                    thread.session.thread_id(),
                    control,
                ))
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then_with(|| right.1.cmp(&left.1))
                .then_with(|| left.2.to_string().cmp(&right.2.to_string()))
                .then_with(|| left.3.to_string().cmp(&right.3.to_string()))
        });
        if let Some((_, _, registry_root_thread_id, _, control)) = candidates.into_iter().next() {
            return Some((registry_root_thread_id, control));
        }
        self.state
            .retained_agent_controls()
            .into_iter()
            .filter(|(registry_root_thread_id, control)| {
                *registry_root_thread_id == thread_id
                    || control.get_agent_metadata(thread_id).is_some()
                    || control
                        .current_membership_subtree_thread_ids(thread_id)
                        .into_iter()
                        .any(|member_thread_id| member_thread_id != thread_id)
            })
            .min_by_key(|(registry_root_thread_id, _)| registry_root_thread_id.to_string())
    }

    /// List a thread and descendants reachable only through current Open ownership.
    ///
    /// Ordinary Closed promotion or adoption edges are privacy boundaries. Current-only
    /// descendants are included only when their registered parent chain stays in this set.
    pub async fn list_open_agent_subtree_thread_ids(
        &self,
        thread_id: ThreadId,
    ) -> CodexResult<Vec<ThreadId>> {
        let _lifecycle_mutation = self.state.lock_lifecycle_mutation().await;
        let mut subtree_thread_ids = vec![thread_id];
        let mut seen_thread_ids = HashSet::from([thread_id]);
        if let Some(agent_graph_store) = self.state.agent_graph_store() {
            for descendant_id in agent_graph_store
                .list_thread_spawn_descendants(
                    thread_id,
                    Some(codex_agent_graph_store::ThreadSpawnEdgeStatus::Open),
                )
                .await
                .map_err(|err| {
                    CodexErr::Fatal(format!(
                        "failed to load open thread-spawn descendants: {err}"
                    ))
                })?
            {
                if seen_thread_ids.insert(descendant_id) {
                    subtree_thread_ids.push(descendant_id);
                }
            }
        }
        if let Some((_, control)) = self.agent_control_containing(thread_id).await {
            let current_only_parent_by_thread_id = control
                .current_only_descendant_parents_within(
                    thread_id,
                    &seen_thread_ids,
                    self.state.agent_graph_store().as_deref(),
                )
                .await?;
            for descendant_id in current_only_parent_by_thread_id.into_keys() {
                if seen_thread_ids.insert(descendant_id) {
                    subtree_thread_ids.push(descendant_id);
                }
            }
        }
        Ok(subtree_thread_ids)
    }
}

impl ThreadManagerState {
    /// Reject a membership mutation whose ownership chain is being archived or deleted.
    ///
    /// Callers must hold `lifecycle_mutation` while checking these identities and while committing
    /// the corresponding registry or graph mutation. The temporary marker outlives the capture
    /// lock so a mutation that starts after capture fails instead of extending a frozen subtree.
    pub(crate) fn ensure_current_membership_mutation_allowed(
        &self,
        thread_ids: impl IntoIterator<Item = ThreadId>,
    ) -> CodexResult<()> {
        if let Some(thread_id) = thread_ids
            .into_iter()
            .find(|thread_id| self.is_thread_under_membership_eviction(*thread_id))
        {
            return Err(CodexErr::UnsupportedOperation(format!(
                "thread {thread_id} is being archived or deleted"
            )));
        }
        Ok(())
    }

    fn reconcile_retained_agent_control(
        &self,
        registry_root_thread_id: ThreadId,
        control: AgentControl,
    ) {
        let Ok(mut retained) = self.retained_agent_controls.lock() else {
            return;
        };
        if control.has_current_agent_members() {
            retained.insert(registry_root_thread_id, control);
            return;
        }
        let remove_matching_empty_control = retained
            .get(&registry_root_thread_id)
            .is_some_and(|existing| existing.shares_current_agent_registry(&control));
        if remove_matching_empty_control {
            retained.remove(&registry_root_thread_id);
        }
    }

    fn retained_agent_controls(&self) -> Vec<(ThreadId, AgentControl)> {
        let Ok(mut retained) = self.retained_agent_controls.lock() else {
            return Vec::new();
        };
        retained.retain(|_, control| control.has_current_agent_members());
        retained
            .iter()
            .map(|(root_thread_id, control)| (*root_thread_id, control.clone()))
            .collect()
    }

    /// Return the registry retained for a root whose runtime was removed during partial eviction.
    ///
    /// The entry stays in the map until the shared registry is empty. A failed root resume must not
    /// consume the only handle that keeps failed archive or delete candidates current.
    pub(crate) fn retained_agent_control(
        &self,
        registry_root_thread_id: ThreadId,
    ) -> Option<AgentControl> {
        self.retained_agent_controls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&registry_root_thread_id)
            .cloned()
    }

    pub(crate) fn mark_threads_for_membership_eviction(
        &self,
        thread_ids: impl IntoIterator<Item = ThreadId>,
    ) {
        let mut temporary_thread_ids = self
            .temporary_membership_eviction_thread_ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for thread_id in thread_ids {
            *temporary_thread_ids.entry(thread_id).or_default() += 1;
        }
    }

    pub(crate) fn unmark_threads_for_membership_eviction(
        &self,
        thread_ids: impl IntoIterator<Item = ThreadId>,
    ) {
        let mut temporary_thread_ids = self
            .temporary_membership_eviction_thread_ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for thread_id in thread_ids {
            match temporary_thread_ids.entry(thread_id) {
                std::collections::hash_map::Entry::Occupied(mut entry) if *entry.get() > 1 => {
                    *entry.get_mut() -= 1;
                }
                std::collections::hash_map::Entry::Occupied(entry) => {
                    entry.remove();
                }
                std::collections::hash_map::Entry::Vacant(_) => {}
            }
        }
    }

    pub(crate) fn is_thread_under_membership_eviction(&self, thread_id: ThreadId) -> bool {
        self.temporary_membership_eviction_thread_ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&thread_id)
    }
}
