use super::ownership::persisted_thread_workspace_roots;
use super::ownership::restore_cold_root_config;
use super::ownership::wait_for_adoptable_root;
use super::residency::is_unloadable;
use super::*;
use codex_agent_graph_store::ThreadSpawnEdgeStatus;
use codex_thread_store::PersistContext;
use codex_thread_store::StoredThread;
use codex_thread_store::ThreadMetadataPatch;
use std::collections::HashSet;
use tokio::sync::OwnedMutexGuard;

/// Original ownership and persisted identity of one movable descendant.
struct OwnedDescendant {
    thread_id: ThreadId,
    parent_thread_id: ThreadId,
    original_source: SessionSource,
    original_control: AgentControl,
    original_config: Config,
    original_metadata: StoredThread,
    original_visible_when_cold: bool,
    original_cold_terminal_status: Option<AgentStatus>,
    was_loaded: bool,
}

/// Breadth-first descendants held idle while ownership is being transferred.
pub(super) struct OwnedDescendantTree {
    root_thread_id: ThreadId,
    original_root_path: AgentPath,
    original_root_depth: i32,
    descendants: Vec<OwnedDescendant>,
    _transition_guards: Vec<OwnedMutexGuard<()>>,
}

impl OwnedDescendantTree {
    /// Reject ownership changes that would turn an existing descendant into its own ancestor.
    pub(super) fn contains_thread(&self, thread_id: ThreadId) -> bool {
        self.root_thread_id == thread_id
            || self
                .descendants
                .iter()
                .any(|descendant| descendant.thread_id == thread_id)
    }
}

/// Destination registrations reserved before any existing thread is stopped.
pub(super) struct PreparedOwnedDescendantTree {
    descendants: Vec<PreparedOwnedDescendant>,
}

struct PreparedOwnedDescendant {
    reservation: crate::agent::registry::SpawnReservation,
    metadata: AgentMetadata,
    session_source: SessionSource,
}

impl AgentControl {
    /// Check ancestry without waiting for an agent that might be the active caller.
    pub(super) async fn contains_open_owned_descendant(
        &self,
        root_thread_id: ThreadId,
        candidate_thread_id: ThreadId,
    ) -> CodexResult<bool> {
        if root_thread_id == candidate_thread_id {
            return Ok(true);
        }
        Ok(self
            .open_owned_descendant_edges(root_thread_id)
            .await?
            .into_iter()
            .any(|(_, child_thread_id)| child_thread_id == candidate_thread_id))
    }

    /// Merge live and persisted parent relationships without loading a descendant.
    async fn open_owned_descendant_edges(
        &self,
        root_thread_id: ThreadId,
    ) -> CodexResult<Vec<(ThreadId, ThreadId)>> {
        let state = self.upgrade()?;
        let mut live_children = HashMap::<ThreadId, Vec<ThreadId>>::new();
        for (parent_thread_id, child_thread_id) in state.list_live_thread_spawn_edges().await {
            live_children
                .entry(parent_thread_id)
                .or_default()
                .push(child_thread_id);
        }

        let graph_store = state.agent_graph_store();
        let mut visited = HashSet::from([root_thread_id]);
        let mut queue = VecDeque::from([root_thread_id]);
        let mut descendant_edges = Vec::new();
        while let Some(parent_thread_id) = queue.pop_front() {
            let mut child_ids = live_children.remove(&parent_thread_id).unwrap_or_default();
            if let Some(graph_store) = graph_store.as_ref() {
                child_ids.extend(
                    graph_store
                        .list_thread_spawn_children(
                            parent_thread_id,
                            Some(ThreadSpawnEdgeStatus::Open),
                        )
                        .await
                        .map_err(|err| {
                            CodexErr::Fatal(format!(
                                "failed to read descendants of thread {parent_thread_id}: {err}"
                            ))
                        })?,
                );
            }
            child_ids.sort_by_key(ToString::to_string);
            child_ids.dedup();
            for child_thread_id in child_ids {
                if !visited.insert(child_thread_id) {
                    return Err(CodexErr::InvalidRequest(format!(
                        "thread {child_thread_id} has a cyclic or duplicate parent relationship"
                    )));
                }
                descendant_edges.push((parent_thread_id, child_thread_id));
                queue.push_back(child_thread_id);
            }
        }

        Ok(descendant_edges)
    }

    /// Freeze a durable descendant tree without starting cold descendants.
    pub(super) async fn snapshot_idle_owned_descendants(
        &self,
        root_thread_id: ThreadId,
        root_source: &SessionSource,
        root_config: &Config,
    ) -> CodexResult<OwnedDescendantTree> {
        let state = self.upgrade()?;
        let descendant_edges = self.open_owned_descendant_edges(root_thread_id).await?;
        let mut descendants = Vec::with_capacity(descendant_edges.len());
        for (parent_thread_id, thread_id) in descendant_edges {
            let loaded_thread = match state.get_thread(thread_id).await {
                Ok(thread) => {
                    if thread.session.live_thread().is_none() {
                        return Err(CodexErr::InvalidRequest(format!(
                            "descendant thread {thread_id} has no materializable conversation recorder"
                        )));
                    }
                    wait_for_adoptable_root(thread.as_ref()).await?;
                    thread
                        .session
                        .try_ensure_rollout_materialized(PersistContext::Standard)
                        .await?;
                    Some(thread)
                }
                Err(err) if matches!(err.details(), CodexErrorDetails::ThreadNotFound(_)) => None,
                Err(err) => return Err(err),
            };
            let original_metadata = state
                .read_stored_thread(ReadThreadParams {
                    thread_id,
                    include_archived: false,
                    include_history: false,
                })
                .await?;
            if original_metadata.rollout_path.is_none() {
                return Err(CodexErr::InvalidRequest(format!(
                    "descendant thread {thread_id} must have a persisted rollout before it can be transferred"
                )));
            }

            let loaded_terminal_status = if let Some(thread) = loaded_thread.as_ref() {
                let status = thread.agent_status().await;
                matches!(
                    status,
                    AgentStatus::Completed(_)
                        | AgentStatus::Errored(_)
                        | AgentStatus::Interrupted
                        | AgentStatus::Shutdown
                )
                .then_some(status)
            } else {
                None
            };
            let (original_source, original_control, original_config, was_loaded) =
                match loaded_thread {
                    Some(thread) => {
                        let mut original_config = thread.config().await.as_ref().clone();
                        original_config.ephemeral = false;
                        (
                            thread.session_source.clone(),
                            thread.session.services.agent_control.clone(),
                            original_config,
                            true,
                        )
                    }
                    None => {
                        let workspace_roots =
                            persisted_thread_workspace_roots(&state, &original_metadata).await?;
                        (
                            original_metadata.source.clone(),
                            self.clone(),
                            restore_cold_root_config(
                                root_config.clone(),
                                &original_metadata,
                                workspace_roots,
                            )
                            .await?,
                            false,
                        )
                    }
                };
            let original_agent_metadata = original_control.get_agent_metadata(thread_id);
            let original_visible_when_cold = original_agent_metadata
                .as_ref()
                .is_some_and(|metadata| metadata.lifecycle.is_visible_when_cold());
            let original_cold_terminal_status = loaded_terminal_status.or_else(|| {
                original_agent_metadata
                    .and_then(|metadata| metadata.lifecycle.cold_terminal_status())
            });
            descendants.push(OwnedDescendant {
                thread_id,
                parent_thread_id,
                original_source,
                original_control,
                original_config,
                original_metadata,
                original_visible_when_cold,
                original_cold_terminal_status,
                was_loaded,
            });
        }

        let mut lock_order = (0..descendants.len()).collect::<Vec<_>>();
        lock_order.sort_by_key(|index| descendants[*index].thread_id.to_string());
        let mut transition_guards = Vec::with_capacity(lock_order.len());
        for index in lock_order {
            let descendant = &descendants[index];
            let Some(metadata) = descendant
                .original_control
                .get_agent_metadata(descendant.thread_id)
            else {
                if descendant.was_loaded {
                    return Err(CodexErr::InvalidRequest(format!(
                        "loaded descendant thread {} has no ownership registration",
                        descendant.thread_id
                    )));
                }
                continue;
            };
            let transition = metadata.lifecycle.lock_transition().await;
            if metadata.lifecycle.completion_watcher_active() {
                return Err(CodexErr::InvalidRequest(format!(
                    "descendant thread {} has an active completion watcher",
                    descendant.thread_id
                )));
            }
            if descendant.was_loaded {
                let thread = state.get_thread(descendant.thread_id).await?;
                if !is_unloadable(thread.as_ref()).await {
                    return Err(CodexErr::InvalidRequest(format!(
                        "descendant thread {} started a turn during ownership transfer",
                        descendant.thread_id
                    )));
                }
            }
            transition_guards.push(transition);
        }

        Ok(OwnedDescendantTree {
            root_thread_id,
            original_root_path: root_source.get_agent_path().unwrap_or_else(AgentPath::root),
            original_root_depth: thread_spawn_depth(root_source).unwrap_or(0),
            descendants,
            _transition_guards: transition_guards,
        })
    }

    /// Reserve every destination name before interrupting the original tree.
    pub(super) fn prepare_owned_descendants(
        &self,
        tree: &OwnedDescendantTree,
        destination_root_source: &SessionSource,
    ) -> CodexResult<PreparedOwnedDescendantTree> {
        let destination_root_path = destination_root_source
            .get_agent_path()
            .unwrap_or_else(AgentPath::root);
        let destination_root_depth = thread_spawn_depth(destination_root_source).unwrap_or(0);
        let mut descendants = Vec::with_capacity(tree.descendants.len());

        for descendant in &tree.descendants {
            let original_path = descendant
                .original_metadata
                .agent_path
                .as_deref()
                .map(AgentPath::try_from)
                .transpose()
                .map_err(|err| {
                    CodexErr::InvalidRequest(format!(
                        "invalid agent path for descendant thread {}: {err}",
                        descendant.thread_id
                    ))
                })?
                .or_else(|| descendant.original_source.get_agent_path());
            let destination_path = original_path
                .as_ref()
                .map(|path| {
                    rebase_owned_agent_path(path, &tree.original_root_path, &destination_root_path)
                })
                .transpose()?;
            let original_depth =
                thread_spawn_depth(&descendant.original_source).ok_or_else(|| {
                    CodexErr::InvalidRequest(format!(
                        "descendant thread {} does not have a thread-spawn source",
                        descendant.thread_id
                    ))
                })?;
            let depth = destination_root_depth
                .checked_add(original_depth - tree.original_root_depth)
                .ok_or_else(|| {
                    CodexErr::InvalidRequest(format!(
                        "agent depth overflow for descendant thread {}",
                        descendant.thread_id
                    ))
                })?;
            let role = descendant
                .original_metadata
                .agent_role
                .clone()
                .or_else(|| descendant.original_source.get_agent_role());
            let nickname = descendant
                .original_metadata
                .agent_nickname
                .clone()
                .or_else(|| descendant.original_source.get_nickname());
            let is_goal_supervisor =
                role.as_deref() == Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME);
            let mut reservation = if is_goal_supervisor {
                self.state.reserve_uncounted_spawn_slot()
            } else {
                self.state.reserve_spawn_slot(
                    descendant.original_config.effective_agent_max_threads(
                        descendant
                            .original_config
                            .multi_agent_version_from_features(),
                    ),
                )?
            };
            let (session_source, mut metadata) = self.prepare_thread_spawn(
                &mut reservation,
                &descendant.original_config,
                descendant.parent_thread_id,
                depth,
                destination_path,
                role,
                nickname,
            )?;
            metadata.agent_id = Some(descendant.thread_id);
            descendants.push(PreparedOwnedDescendant {
                reservation,
                metadata,
                session_source,
            });
        }

        Ok(PreparedOwnedDescendantTree { descendants })
    }

    /// Stop loaded descendants from leaves upward while preserving open graph edges.
    pub(super) async fn unload_owned_descendants(
        &self,
        state: &Arc<ThreadManagerState>,
        tree: &OwnedDescendantTree,
    ) -> CodexResult<()> {
        for descendant in tree.descendants.iter().rev() {
            if descendant.was_loaded {
                descendant
                    .original_control
                    .unload_agent_thread(state, descendant.thread_id)
                    .await?;
            }
        }
        Ok(())
    }

    /// Register transferred descendants without reopening their model or MCP runtimes.
    pub(super) async fn commit_owned_descendants(
        &self,
        state: &Arc<ThreadManagerState>,
        tree: &OwnedDescendantTree,
        prepared: PreparedOwnedDescendantTree,
    ) -> CodexResult<()> {
        if tree.descendants.len() != prepared.descendants.len() {
            return Err(CodexErr::Fatal(format!(
                "descendant reservation count does not match thread {}",
                tree.root_thread_id
            )));
        }

        let mut persisted = Vec::new();
        for (descendant, prepared_descendant) in
            tree.descendants.iter().zip(prepared.descendants.iter())
        {
            let patch = descendant_metadata_patch(
                &prepared_descendant.session_source,
                &prepared_descendant.metadata,
            );
            if let Err(err) = state
                .update_thread_metadata(descendant.thread_id, patch, /*include_archived*/ true)
                .await
            {
                for restored in persisted.into_iter().rev() {
                    restore_persisted_descendant_metadata(state, restored).await?;
                }
                return Err(err);
            }
            persisted.push(descendant);
        }

        for (descendant, prepared_descendant) in tree.descendants.iter().zip(prepared.descendants) {
            if prepared_descendant.metadata.agent_role.as_deref()
                != Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME)
            {
                if let Some(status) = descendant.original_cold_terminal_status.as_ref() {
                    prepared_descendant
                        .metadata
                        .lifecycle
                        .remember_cold_terminal_status(
                            status.clone(),
                            /*visible_when_cold*/ true,
                        );
                } else {
                    prepared_descendant
                        .metadata
                        .lifecycle
                        .mark_visible_when_cold();
                }
            }
            prepared_descendant
                .reservation
                .commit(prepared_descendant.metadata);
        }
        for descendant in &tree.descendants {
            descendant
                .original_control
                .forget_agent_residency(descendant.thread_id);
            descendant
                .original_control
                .state
                .release_spawned_thread(descendant.thread_id);
        }
        Ok(())
    }

    /// Restore persisted metadata and the original loaded or cold ownership on rollback.
    pub(super) async fn restore_owned_descendants(
        &self,
        state: &Arc<ThreadManagerState>,
        tree: &OwnedDescendantTree,
    ) -> CodexResult<()> {
        for descendant in &tree.descendants {
            let original_thread_is_loaded = match state.get_thread(descendant.thread_id).await {
                Ok(thread)
                    if Arc::ptr_eq(
                        &thread.session.services.agent_control.state,
                        &descendant.original_control.state,
                    ) =>
                {
                    true
                }
                Ok(_) => {
                    self.unload_agent_thread(state, descendant.thread_id)
                        .await?;
                    false
                }
                Err(err) if matches!(err.details(), CodexErrorDetails::ThreadNotFound(_)) => false,
                Err(err) => return Err(err),
            };
            if !Arc::ptr_eq(&self.state, &descendant.original_control.state) {
                self.forget_agent_residency(descendant.thread_id);
                self.state.release_spawned_thread(descendant.thread_id);
            }
            restore_persisted_descendant_metadata(state, descendant).await?;

            if descendant.was_loaded && !original_thread_is_loaded {
                descendant
                    .original_control
                    .state
                    .release_spawned_thread(descendant.thread_id);
                descendant
                    .original_control
                    .resume_agent_from_rollout_with_ownership(
                        descendant.original_config.clone(),
                        descendant.thread_id,
                        descendant.original_source.clone(),
                        super::ownership::ResumedThreadOwnership::Transfer,
                    )
                    .await?;
                if let Some(metadata) = descendant
                    .original_control
                    .get_agent_metadata(descendant.thread_id)
                {
                    if let Some(status) = descendant.original_cold_terminal_status.as_ref() {
                        metadata.lifecycle.remember_cold_terminal_status(
                            status.clone(),
                            /*visible_when_cold*/ true,
                        );
                    } else if descendant.original_visible_when_cold {
                        metadata.lifecycle.mark_visible_when_cold();
                    }
                }
            } else if descendant
                .original_control
                .get_agent_metadata(descendant.thread_id)
                .is_none()
            {
                let role = descendant
                    .original_metadata
                    .agent_role
                    .clone()
                    .or_else(|| descendant.original_source.get_agent_role());
                let mut reservation =
                    if role.as_deref() == Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME) {
                        descendant
                            .original_control
                            .state
                            .reserve_uncounted_spawn_slot()
                    } else {
                        descendant
                            .original_control
                            .state
                            .reserve_spawn_slot(/*max_threads*/ None)?
                    };
                let original_path = descendant
                    .original_metadata
                    .agent_path
                    .as_deref()
                    .map(AgentPath::try_from)
                    .transpose()
                    .map_err(CodexErr::InvalidRequest)?;
                let (_, mut metadata) = descendant.original_control.prepare_thread_spawn(
                    &mut reservation,
                    &descendant.original_config,
                    descendant.parent_thread_id,
                    thread_spawn_depth(&descendant.original_source).unwrap_or(1),
                    original_path,
                    role,
                    descendant.original_metadata.agent_nickname.clone(),
                )?;
                metadata.agent_id = Some(descendant.thread_id);
                if let Some(status) = descendant.original_cold_terminal_status.as_ref() {
                    metadata.lifecycle.remember_cold_terminal_status(
                        status.clone(),
                        descendant.original_visible_when_cold,
                    );
                } else if descendant.original_visible_when_cold {
                    metadata.lifecycle.mark_visible_when_cold();
                }
                reservation.commit(metadata);
            }
        }
        Ok(())
    }
}

fn rebase_owned_agent_path(
    original: &AgentPath,
    original_root: &AgentPath,
    destination_root: &AgentPath,
) -> CodexResult<AgentPath> {
    let suffix = original
        .as_str()
        .strip_prefix(original_root.as_str())
        .filter(|suffix| suffix.is_empty() || suffix.starts_with('/'))
        .ok_or_else(|| {
            CodexErr::InvalidRequest(format!(
                "agent path `{original}` is not a descendant of `{original_root}`"
            ))
        })?;
    AgentPath::try_from(format!("{destination_root}{suffix}")).map_err(CodexErr::InvalidRequest)
}

fn descendant_metadata_patch(
    source: &SessionSource,
    metadata: &AgentMetadata,
) -> ThreadMetadataPatch {
    ThreadMetadataPatch {
        source: Some(source.clone()),
        thread_source: Some(Some(ThreadSource::Subagent)),
        agent_path: Some(metadata.agent_path.as_ref().map(ToString::to_string)),
        agent_nickname: Some(metadata.agent_nickname.clone()),
        agent_role: Some(metadata.agent_role.clone()),
        ..Default::default()
    }
}

async fn restore_persisted_descendant_metadata(
    state: &Arc<ThreadManagerState>,
    descendant: &OwnedDescendant,
) -> CodexResult<()> {
    state
        .update_thread_metadata(
            descendant.thread_id,
            ThreadMetadataPatch {
                source: Some(descendant.original_metadata.source.clone()),
                thread_source: Some(descendant.original_metadata.thread_source.clone()),
                agent_path: Some(descendant.original_metadata.agent_path.clone()),
                agent_nickname: Some(descendant.original_metadata.agent_nickname.clone()),
                agent_role: Some(descendant.original_metadata.agent_role.clone()),
                ..Default::default()
            },
            /*include_archived*/ true,
        )
        .await?;
    Ok(())
}

#[cfg(test)]
#[path = "ownership_tree_tests.rs"]
mod tests;
