use super::ownership_tree::OwnedDescendantTree;
use super::residency::is_unloadable;
use super::resume::load_agent_model_context;
use super::*;
use crate::codex_thread::CodexThread;
use codex_agent_graph_store::ThreadSpawnEdgeStatus;
use codex_thread_store::PersistContext;
use codex_thread_store::StoredThread;
use codex_thread_store::ThreadMetadataPatch;
use std::time::Duration;

const ACTIVE_ADOPTION_TIMEOUT: Duration = Duration::from_secs(30);
const ACTIVE_ADOPTION_IDLE_RECHECK: Duration = Duration::from_millis(10);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ResumedThreadOwnership {
    #[cfg(test)]
    Preserve,
    Transfer,
}

/// Whether rollback must leave the original root running or persisted but unloaded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OriginalRootResidency {
    Loaded,
    Cold,
}

/// Original root ownership and configuration required for adoption rollback.
struct OriginalRoot {
    control: AgentControl,
    config: Config,
    source: SessionSource,
    residency: OriginalRootResidency,
}

impl AgentControl {
    /// Transfer a root into this agent tree without copying or interrupting its history.
    pub(crate) async fn adopt_agent_with_communication(
        &self,
        config: Config,
        thread_id: ThreadId,
        communication: InterAgentCommunication,
        context: AgentCommunicationContext,
        session_source: SessionSource,
        start_options: TurnStartOptions,
    ) -> CodexResult<LiveAgent> {
        let state = self.upgrade()?;
        let Some(parent_thread_id) = session_source.parent_thread_id() else {
            return Err(CodexErr::InvalidRequest(
                "adoption requires a thread-spawn parent".to_string(),
            ));
        };
        if thread_id == parent_thread_id {
            return Err(CodexErr::InvalidRequest(
                "a thread cannot adopt itself".to_string(),
            ));
        }
        if self
            .contains_open_owned_descendant(thread_id, parent_thread_id)
            .await?
        {
            return Err(CodexErr::InvalidRequest(
                "a thread cannot adopt its own ancestor".to_string(),
            ));
        }

        let thread = match state.get_thread(thread_id).await {
            Ok(thread) => {
                if thread.session_source.is_non_root_agent() {
                    return Err(CodexErr::InvalidRequest(
                        "only an independent root thread can be adopted".to_string(),
                    ));
                }
                if thread.session.live_thread().is_none() {
                    return Err(CodexErr::InvalidRequest(
                        "the root thread has no materializable conversation recorder".to_string(),
                    ));
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
        let stored_thread = state
            .read_stored_thread(ReadThreadParams {
                thread_id,
                include_archived: false,
                include_history: false,
            })
            .await?;
        if stored_thread.rollout_path.is_none() {
            return Err(CodexErr::InvalidRequest(
                "the root thread has no persisted conversation history".to_string(),
            ));
        }
        if stored_thread.parent_thread_id.is_some()
            || stored_thread.source.is_non_root_agent()
            || state
                .list_live_thread_spawn_edges()
                .await
                .into_iter()
                .any(|(_, child)| child == thread_id)
        {
            return Err(CodexErr::InvalidRequest(
                "an adopted root thread must not already have another parent".to_string(),
            ));
        }

        let (original, lifecycle) = match thread {
            Some(thread) => {
                let original_control = thread.session.services.agent_control.clone();
                let lifecycle = original_control
                    .get_agent_metadata(thread_id)
                    .map(|metadata| metadata.lifecycle)
                    .unwrap_or_default();
                let mut original_config = thread.config().await.as_ref().clone();
                original_config.ephemeral = false;
                (
                    OriginalRoot {
                        control: original_control,
                        config: original_config,
                        source: thread.session_source.clone(),
                        residency: OriginalRootResidency::Loaded,
                    },
                    Some((lifecycle, thread)),
                )
            }
            None => {
                let workspace_roots =
                    persisted_thread_workspace_roots(&state, &stored_thread).await?;
                let original_config =
                    restore_cold_root_config(config, &stored_thread, workspace_roots).await?;
                let original_control = AgentControl::new(
                    Arc::downgrade(&state),
                    self.thread_id_generator.clone(),
                    original_config.rollout_budget.clone(),
                );
                (
                    OriginalRoot {
                        control: original_control,
                        config: original_config,
                        source: stored_thread.source.clone(),
                        residency: OriginalRootResidency::Cold,
                    },
                    None,
                )
            }
        };
        let _transition = match lifecycle.as_ref() {
            Some((lifecycle, thread)) => {
                let transition = lifecycle.lock_transition().await;
                let current = state.get_thread(thread_id).await?;
                if !Arc::ptr_eq(&current, thread) || current.session_source.is_non_root_agent() {
                    return Err(CodexErr::InvalidRequest(
                        "the root thread was adopted or replaced before ownership could transfer"
                            .to_string(),
                    ));
                }
                if lifecycle.completion_watcher_active() || !is_unloadable(thread.as_ref()).await {
                    return Err(CodexErr::InvalidRequest(
                        "the root thread started another turn or completion watcher during adoption"
                            .to_string(),
                    ));
                }
                Some(transition)
            }
            None => None,
        };
        let tree = original
            .control
            .snapshot_idle_owned_descendants(thread_id, &original.source, &original.config)
            .await?;
        if tree.contains_thread(parent_thread_id) {
            return Err(CodexErr::InvalidRequest(
                "a thread cannot adopt its own ancestor".to_string(),
            ));
        }
        let prepared_descendants = self.prepare_owned_descendants(&tree, &session_source)?;
        if let Err(err) = original
            .control
            .unload_owned_descendants(&state, &tree)
            .await
        {
            if let Err(rollback_err) = self.restore_owned_descendants(&state, &tree).await {
                return Err(CodexErr::Fatal(format!(
                    "failed to prepare descendants of root thread {thread_id}: {err}; restoring the original descendants also failed: {rollback_err}"
                )));
            }
            return Err(err);
        }
        if original.residency == OriginalRootResidency::Loaded
            && let Err(err) = self.unload_agent_thread(&state, thread_id).await
        {
            if state.get_thread(thread_id).await.is_err()
                && let Err(rollback_err) = original
                    .control
                    .resume_root_from_rollout(
                        original.config.clone(),
                        thread_id,
                        original.source.clone(),
                    )
                    .await
            {
                return Err(CodexErr::Fatal(format!(
                    "failed to prepare root thread {thread_id}: {err}; restoring the original root also failed: {rollback_err}"
                )));
            }
            if let Err(rollback_err) = self.restore_owned_descendants(&state, &tree).await {
                return Err(CodexErr::Fatal(format!(
                    "failed to prepare root thread {thread_id}: {err}; restoring its descendants also failed: {rollback_err}"
                )));
            }
            return Err(err);
        }

        let adopted_thread_id = match self
            .resume_agent_from_rollout_with_ownership(
                original.config.clone(),
                thread_id,
                session_source,
                ResumedThreadOwnership::Transfer,
            )
            .await
        {
            Ok(adopted_thread_id) => adopted_thread_id,
            Err(err) => {
                if let Err(rollback_err) = self
                    .restore_root_after_failed_adoption(&state, thread_id, &original, &tree)
                    .await
                {
                    return Err(CodexErr::Fatal(format!(
                        "failed to adopt root thread {thread_id}: {err}; restoring the original root also failed: {rollback_err}"
                    )));
                }
                return Err(err);
            }
        };
        if let Err(err) = self
            .commit_owned_descendants(&state, &tree, prepared_descendants)
            .await
        {
            if let Err(rollback_err) = self
                .restore_root_after_failed_adoption(&state, thread_id, &original, &tree)
                .await
            {
                return Err(CodexErr::Fatal(format!(
                    "failed to adopt descendants of root thread {thread_id}: {err}; restoring the original thread tree also failed: {rollback_err}"
                )));
            }
            return Err(err);
        }
        if let Err(err) = self
            .send_inter_agent_communication_after_capacity_check(
                adopted_thread_id,
                &state,
                communication,
                context,
                start_options,
            )
            .await
        {
            if let Err(rollback_err) = self
                .restore_root_after_failed_adoption(&state, thread_id, &original, &tree)
                .await
            {
                return Err(CodexErr::Fatal(format!(
                    "failed to deliver the adopted thread's initial task: {err}; restoring root thread {thread_id} also failed: {rollback_err}"
                )));
            }
            return Err(err);
        }
        let metadata = self.ensure_agent_known(adopted_thread_id)?;
        let status = self.get_status(adopted_thread_id).await;
        Ok(LiveAgent {
            thread_id: adopted_thread_id,
            metadata,
            status,
        })
    }

    async fn restore_root_after_failed_adoption(
        &self,
        state: &Arc<ThreadManagerState>,
        thread_id: ThreadId,
        original: &OriginalRoot,
        tree: &OwnedDescendantTree,
    ) -> CodexResult<()> {
        if state.get_thread(thread_id).await.is_ok() {
            self.unload_agent_thread(state, thread_id).await?;
        }
        if let Some(graph_store) = state.agent_graph_store() {
            graph_store
                .set_thread_spawn_edge_status(thread_id, ThreadSpawnEdgeStatus::Closed)
                .await
                .map_err(|err| {
                    CodexErr::Fatal(format!(
                        "failed to close the unsuccessful adoption edge for {thread_id}: {err}"
                    ))
                })?;
        }
        self.forget_agent_residency(thread_id);
        self.state.release_spawned_thread(thread_id);
        original
            .control
            .resume_root_from_rollout(original.config.clone(), thread_id, original.source.clone())
            .await?;
        self.restore_owned_descendants(state, tree).await?;
        if original.residency == OriginalRootResidency::Cold {
            original
                .control
                .unload_agent_thread(state, thread_id)
                .await?;
        }
        Ok(())
    }

    /// Detach a completed child and resume its existing history as an independent root.
    pub(crate) async fn promote_agent(&self, thread_id: ThreadId) -> CodexResult<ThreadId> {
        let state = self.upgrade()?;
        let metadata = self.ensure_agent_known(thread_id)?;
        if metadata.agent_path.as_ref().is_some_and(AgentPath::is_root)
            || metadata.agent_role.as_deref()
                == Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME)
        {
            return Err(CodexErr::InvalidRequest(
                "only a user-visible subagent can be promoted".to_string(),
            ));
        }
        let thread = state.get_thread(thread_id).await?;
        if !thread.session_source.is_non_root_agent() || thread.session.live_thread().is_none() {
            return Err(CodexErr::InvalidRequest(
                "only a user-visible subagent with a materializable conversation can be promoted"
                    .to_string(),
            ));
        }
        wait_for_adoptable_root(thread.as_ref()).await?;
        let _transition = metadata.lifecycle.lock_transition().await;
        let current = state.get_thread(thread_id).await?;
        if !Arc::ptr_eq(&current, &thread) || !current.session_source.is_non_root_agent() {
            return Err(CodexErr::InvalidRequest(
                "the subagent was promoted or replaced before ownership could transfer".to_string(),
            ));
        }
        if !is_unloadable(thread.as_ref()).await || metadata.lifecycle.completion_watcher_active() {
            return Err(CodexErr::InvalidRequest(
                "the subagent started another turn or completion watcher during promotion"
                    .to_string(),
            ));
        }
        thread
            .session
            .try_ensure_rollout_materialized(PersistContext::Standard)
            .await?;
        let mut config = thread.config().await.as_ref().clone();
        config.ephemeral = false;
        let original_source = thread.session_source.clone();
        let stored_thread = state
            .read_stored_thread(ReadThreadParams {
                thread_id,
                include_archived: false,
                include_history: false,
            })
            .await?;
        let promoted_root_source =
            load_agent_model_context(&state, thread_id, stored_thread.history_mode)
                .await?
                .into_iter()
                .flatten()
                .find_map(|item| match item {
                    RolloutItem::SessionMeta(line) if !line.meta.source.is_non_root_agent() => {
                        Some(line.meta.source)
                    }
                    RolloutItem::SessionMeta(_)
                    | RolloutItem::RolloutReference(_)
                    | RolloutItem::ResponseItem(_)
                    | RolloutItem::InterAgentCommunication(_)
                    | RolloutItem::InterAgentCommunicationMetadata { .. }
                    | RolloutItem::Compacted(_)
                    | RolloutItem::TurnContext(_)
                    | RolloutItem::WorldState(_)
                    | RolloutItem::SecurityRiskScore(_)
                    | RolloutItem::RealtimeItem(_)
                    | RolloutItem::EventMsg(_) => None,
                })
                .unwrap_or(SessionSource::Cli);
        let root_control = AgentControl::new(
            Arc::downgrade(&state),
            self.thread_id_generator.clone(),
            config.rollout_budget.clone(),
        );
        let tree = self
            .snapshot_idle_owned_descendants(thread_id, &original_source, &config)
            .await?;
        let prepared_descendants =
            root_control.prepare_owned_descendants(&tree, &promoted_root_source)?;
        if let Err(err) = self.unload_owned_descendants(&state, &tree).await {
            if let Err(rollback_err) = root_control.restore_owned_descendants(&state, &tree).await {
                return Err(CodexErr::Fatal(format!(
                    "failed to prepare descendants of subagent {thread_id}: {err}; restoring the original descendants also failed: {rollback_err}"
                )));
            }
            return Err(err);
        }
        if let Err(err) = self.unload_agent_thread(&state, thread_id).await {
            if state.get_thread(thread_id).await.is_err()
                && let Err(rollback_err) = self
                    .resume_agent_from_rollout_with_ownership(
                        config.clone(),
                        thread_id,
                        original_source.clone(),
                        ResumedThreadOwnership::Transfer,
                    )
                    .await
            {
                return Err(CodexErr::Fatal(format!(
                    "failed to prepare subagent {thread_id}: {err}; restoring the original subagent also failed: {rollback_err}"
                )));
            }
            if let Err(rollback_err) = root_control.restore_owned_descendants(&state, &tree).await {
                return Err(CodexErr::Fatal(format!(
                    "failed to prepare subagent {thread_id}: {err}; restoring its descendants also failed: {rollback_err}"
                )));
            }
            return Err(err);
        }
        if let Err(err) = root_control
            .resume_root_from_rollout(config.clone(), thread_id, promoted_root_source)
            .await
        {
            if state.get_thread(thread_id).await.is_ok()
                && let Err(rollback_err) = root_control.unload_agent_thread(&state, thread_id).await
            {
                return Err(CodexErr::Fatal(format!(
                    "failed to promote subagent {thread_id}: {err}; stopping the partially resumed root also failed: {rollback_err}"
                )));
            }
            self.forget_agent_residency(thread_id);
            self.state.release_spawned_thread(thread_id);
            if let Err(rollback_err) = self
                .resume_agent_from_rollout_with_ownership(
                    config.clone(),
                    thread_id,
                    original_source.clone(),
                    ResumedThreadOwnership::Transfer,
                )
                .await
            {
                return Err(CodexErr::Fatal(format!(
                    "failed to promote subagent {thread_id}: {err}; restoring the subagent also failed: {rollback_err}"
                )));
            }
            if let Err(rollback_err) = root_control.restore_owned_descendants(&state, &tree).await {
                return Err(CodexErr::Fatal(format!(
                    "failed to promote subagent {thread_id}: {err}; restoring its descendants also failed: {rollback_err}"
                )));
            }
            return Err(err);
        }
        if let Err(err) = root_control
            .commit_owned_descendants(&state, &tree, prepared_descendants)
            .await
        {
            if let Err(rollback_err) = root_control.unload_agent_thread(&state, thread_id).await {
                return Err(CodexErr::Fatal(format!(
                    "failed to promote descendants of subagent {thread_id}: {err}; stopping the promoted root also failed: {rollback_err}"
                )));
            }
            self.forget_agent_residency(thread_id);
            self.state.release_spawned_thread(thread_id);
            if let Err(rollback_err) = self
                .resume_agent_from_rollout_with_ownership(
                    config.clone(),
                    thread_id,
                    original_source.clone(),
                    ResumedThreadOwnership::Transfer,
                )
                .await
            {
                return Err(CodexErr::Fatal(format!(
                    "failed to promote descendants of subagent {thread_id}: {err}; restoring the subagent also failed: {rollback_err}"
                )));
            }
            if let Err(rollback_err) = root_control.restore_owned_descendants(&state, &tree).await {
                return Err(CodexErr::Fatal(format!(
                    "failed to promote descendants of subagent {thread_id}: {err}; restoring the original descendants also failed: {rollback_err}"
                )));
            }
            return Err(err);
        }
        if let Some(graph_store) = state.agent_graph_store()
            && let Err(err) = graph_store
                .set_thread_spawn_edge_status(thread_id, ThreadSpawnEdgeStatus::Closed)
                .await
        {
            if let Err(rollback_err) = root_control.unload_agent_thread(&state, thread_id).await {
                return Err(CodexErr::Fatal(format!(
                    "failed to close the promoted subagent edge for {thread_id}: {err}; stopping the promoted root also failed: {rollback_err}"
                )));
            }
            self.forget_agent_residency(thread_id);
            self.state.release_spawned_thread(thread_id);
            if let Err(rollback_err) = self
                .resume_agent_from_rollout_with_ownership(
                    config,
                    thread_id,
                    original_source,
                    ResumedThreadOwnership::Transfer,
                )
                .await
            {
                return Err(CodexErr::Fatal(format!(
                    "failed to close the promoted subagent edge for {thread_id}: {err}; restoring the subagent also failed: {rollback_err}"
                )));
            }
            if let Err(rollback_err) = root_control.restore_owned_descendants(&state, &tree).await {
                return Err(CodexErr::Fatal(format!(
                    "failed to close the promoted subagent edge for {thread_id}: {err}; restoring the original descendants also failed: {rollback_err}"
                )));
            }
            return Err(CodexErr::Fatal(format!(
                "failed to close the promoted subagent edge for {thread_id}: {err}"
            )));
        }
        self.forget_agent_residency(thread_id);
        self.state.release_spawned_thread(thread_id);
        Ok(thread_id)
    }

    async fn resume_root_from_rollout(
        &self,
        config: Config,
        thread_id: ThreadId,
        session_source: SessionSource,
    ) -> CodexResult<ThreadId> {
        let state = self.upgrade()?;
        let stored_thread = state
            .read_stored_thread(ReadThreadParams {
                thread_id,
                include_archived: true,
                include_history: false,
            })
            .await?;
        let mut history = load_agent_model_context(&state, thread_id, stored_thread.history_mode)
            .await?
            .ok_or(CodexErr::ThreadNotFound(thread_id))?;
        normalize_resumed_session_metadata(
            &mut history,
            thread_id,
            &session_source,
            /*parent_thread_id*/ None,
            /*agent_metadata*/ None,
            SessionId::from(thread_id),
        )?;
        let thread_source = history.iter().find_map(|item| match item {
            RolloutItem::SessionMeta(line) => Some(line.meta.thread_source.clone()),
            _ => None,
        });
        let resumed = state
            .resume_thread_with_history_with_source(ResumeThreadWithHistoryOptions {
                config,
                initial_history: InitialHistory::Resumed(ResumedHistory {
                    conversation_id: thread_id,
                    history: history.into(),
                    rollout_path: stored_thread.rollout_path,
                }),
                agent_control: self.clone(),
                session_source: session_source.clone(),
                parent_thread_id: None,
                environment_selections: None,
                inherited_environments: None,
                inherited_exec_policy: None,
                client_mcp_extensions: None,
                inherited_thread_state: Default::default(),
            })
            .await?;
        resumed
            .thread
            .update_thread_metadata(
                ThreadMetadataPatch {
                    source: Some(session_source),
                    thread_source,
                    agent_path: Some(None),
                    agent_nickname: Some(None),
                    agent_role: Some(None),
                    ..Default::default()
                },
                /*include_archived*/ true,
            )
            .await
            .map_err(|err| {
                CodexErr::Fatal(format!("failed to persist root thread metadata: {err}"))
            })?;
        self.register_session_root(thread_id, /*current_parent_thread_id*/ None);
        state.notify_thread_created(thread_id);
        Ok(resumed.thread_id)
    }
}

/// Wait for a running root to finish without interrupting or discarding its queued turns.
pub(super) async fn wait_for_adoptable_root(thread: &CodexThread) -> CodexResult<()> {
    let mut status = thread.subscribe_status();
    tokio::time::timeout(ACTIVE_ADOPTION_TIMEOUT, async {
        loop {
            let mut task_done = {
                let active_turn = thread.session.active_turn.lock().await;
                active_turn
                    .as_ref()
                    .and_then(|active_turn| active_turn.task.as_ref())
                    .map(|task| {
                        let mut notified = Box::pin(Arc::clone(&task.done).notified_owned());
                        let _ = notified.as_mut().enable();
                        notified
                    })
            };
            if let Some(notified) = task_done.as_mut() {
                if is_unloadable(thread).await {
                    return Ok(());
                }
                notified.as_mut().await;
                continue;
            }

            if is_unloadable(thread).await {
                return Ok(());
            }
            if thread.session.input_queue.has_pending_mailbox_items().await
                && !thread.session.has_pending_turn_start_work().await
            {
                return Err(CodexErr::InvalidRequest(
                    "cannot adopt a root with queued non-triggering messages".to_string(),
                ));
            }
            if matches!(
                *status.borrow(),
                AgentStatus::PendingInit | AgentStatus::NotFound
            ) {
                return Err(CodexErr::InvalidRequest(
                    "the root thread has not completed initialization or a conversation turn"
                        .to_string(),
                ));
            }

            tokio::select! {
                changed = status.changed() => {
                    if changed.is_err() {
                        return Err(CodexErr::InvalidRequest(
                            "the adopted root stopped before becoming idle".to_string(),
                        ));
                    }
                }
                () = tokio::time::sleep(ACTIVE_ADOPTION_IDLE_RECHECK) => {}
            }
        }
    })
    .await
    .map_err(|_| {
        CodexErr::InvalidRequest(format!(
            "the adopted root did not become idle within {} seconds",
            ACTIVE_ADOPTION_TIMEOUT.as_secs()
        ))
    })?
}

/// Reconstruct a persisted root without replacing its model, provider, or permissions.
pub(super) async fn restore_cold_root_config(
    mut config: Config,
    stored_thread: &StoredThread,
    workspace_roots: Option<Vec<codex_utils_absolute_path::AbsolutePathBuf>>,
) -> CodexResult<Config> {
    config.ephemeral = false;
    if let Some(role) = stored_thread.agent_role.as_deref() {
        crate::agent::role::apply_role_to_config(&mut config, Some(role))
            .await
            .map_err(CodexErr::InvalidRequest)?;
    }

    config.model_provider = config
        .model_providers
        .get(&stored_thread.model_provider)
        .cloned()
        .ok_or_else(|| {
            CodexErr::InvalidRequest(format!(
                "cannot adopt thread {} because its original model provider `{}` is unavailable",
                stored_thread.thread_id, stored_thread.model_provider
            ))
        })?;
    config.model_provider_id = stored_thread.model_provider.clone();
    if let Some(model) = &stored_thread.model {
        config.model = Some(model.clone());
    }
    config.model_reasoning_effort = stored_thread.reasoning_effort.clone();
    config.cwd = stored_thread.cwd.clone().try_into().map_err(|err| {
        CodexErr::InvalidRequest(format!(
            "cannot restore the original working directory for thread {}: {err}",
            stored_thread.thread_id
        ))
    })?;
    config.workspace_roots_explicit = workspace_roots.is_some();
    config.workspace_roots = workspace_roots.unwrap_or_else(|| vec![config.cwd.clone()]);
    config
        .permissions
        .set_workspace_roots(config.workspace_roots.clone());
    config
        .permissions
        .approval_policy
        .set(stored_thread.approval_mode)
        .map_err(|err| {
            CodexErr::InvalidRequest(format!(
                "cannot restore the original approval policy for thread {}: {err}",
                stored_thread.thread_id
            ))
        })?;
    config
        .permissions
        .set_permission_profile_from_session_snapshot(
            crate::config::PermissionProfileSnapshot::legacy(
                stored_thread.permission_profile.clone(),
            ),
        )
        .map_err(|err| {
            CodexErr::InvalidRequest(format!(
                "cannot restore the original permission profile for thread {}: {err}",
                stored_thread.thread_id
            ))
        })?;
    Ok(config)
}

/// Restore the latest recorded workspace roots without importing another thread's permissions.
pub(super) async fn persisted_thread_workspace_roots(
    state: &ThreadManagerState,
    stored_thread: &StoredThread,
) -> CodexResult<Option<Vec<codex_utils_absolute_path::AbsolutePathBuf>>> {
    Ok(
        load_agent_model_context(state, stored_thread.thread_id, stored_thread.history_mode)
            .await?
            .into_iter()
            .flatten()
            .rev()
            .find_map(|item| match item {
                RolloutItem::TurnContext(context) => context.workspace_roots,
                RolloutItem::SessionMeta(_)
                | RolloutItem::RolloutReference(_)
                | RolloutItem::ResponseItem(_)
                | RolloutItem::InterAgentCommunication(_)
                | RolloutItem::InterAgentCommunicationMetadata { .. }
                | RolloutItem::Compacted(_)
                | RolloutItem::WorldState(_)
                | RolloutItem::SecurityRiskScore(_)
                | RolloutItem::RealtimeItem(_)
                | RolloutItem::EventMsg(_) => None,
            }),
    )
}

pub(super) fn normalize_resumed_session_metadata(
    history: &mut [RolloutItem],
    thread_id: ThreadId,
    session_source: &SessionSource,
    parent_thread_id: Option<ThreadId>,
    agent_metadata: Option<&AgentMetadata>,
    session_id: SessionId,
) -> CodexResult<()> {
    let metadata = history
        .iter_mut()
        .find_map(|item| match item {
            RolloutItem::SessionMeta(line) => Some(&mut line.meta),
            _ => None,
        })
        .ok_or_else(|| {
            CodexErr::InvalidRequest(format!(
                "cannot transfer thread {thread_id} without persisted session metadata"
            ))
        })?;
    if metadata.id != thread_id {
        return Err(CodexErr::InvalidRequest(format!(
            "session metadata does not match transferred thread {thread_id}"
        )));
    }
    let original_source_was_subagent = metadata.source.is_non_root_agent();
    let original_thread_source = metadata.thread_source.clone();
    metadata.session_id = session_id;
    metadata.source = session_source.clone();
    metadata.parent_thread_id = parent_thread_id;
    metadata.thread_source = if session_source.is_non_root_agent() {
        Some(ThreadSource::Subagent)
    } else if original_source_was_subagent {
        Some(ThreadSource::User)
    } else {
        original_thread_source
    };
    metadata.agent_path = agent_metadata
        .and_then(|agent| agent.agent_path.as_ref())
        .map(ToString::to_string);
    metadata.agent_nickname = agent_metadata.and_then(|agent| agent.agent_nickname.clone());
    metadata.agent_role = agent_metadata.and_then(|agent| agent.agent_role.clone());
    Ok(())
}

#[cfg(test)]
#[path = "ownership_tests.rs"]
mod tests;
