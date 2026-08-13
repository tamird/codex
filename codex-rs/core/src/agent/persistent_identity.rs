use super::AgentControl;
use super::registry::AgentMetadata;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadSource;
use codex_thread_store::ReadThreadParams;
use codex_thread_store::ThreadMetadataPatch;

/// Controls whether a persisted identity may become current while its thread remains archived.
#[derive(Clone, Copy)]
enum PersistedIdentityArchivePolicy {
    CurrentOnly,
    AllowArchivedForExplicitResume,
}

impl AgentControl {
    pub(super) async fn ensure_new_agent_path_available(
        &self,
        parent_thread_id: ThreadId,
        depth: i32,
        agent_path: Option<&AgentPath>,
    ) -> CodexResult<()> {
        let Some(agent_path) = agent_path else {
            return Ok(());
        };
        let state = self.upgrade_for_tools()?;
        let Some(agent_graph_store) = state.agent_graph_store() else {
            return Ok(());
        };
        let root_thread_id = self
            .state
            .agent_id_for_path(&AgentPath::root())
            .or((depth == 1).then_some(parent_thread_id))
            .ok_or_else(|| {
                CodexErr::UnsupportedOperation("agent root is not registered".to_string())
            })?;
        if agent_graph_store
            .find_open_thread_spawn_descendant_by_path(root_thread_id, agent_path.as_str())
            .await
            .map_err(|err| {
                CodexErr::Fatal(format!(
                    "failed to check persisted agent path `{agent_path}`: {err}"
                ))
            })?
            .is_some()
        {
            return Err(CodexErr::UnsupportedOperation(format!(
                "agent path `{agent_path}` already exists"
            )));
        }
        Ok(())
    }

    pub(crate) async fn ensure_open_agent_known_by_id(
        &self,
        current_thread_id: ThreadId,
        agent_id: ThreadId,
    ) -> CodexResult<AgentMetadata> {
        self.ensure_open_agent_known_by_id_with_archive_policy(
            current_thread_id,
            agent_id,
            PersistedIdentityArchivePolicy::CurrentOnly,
        )
        .await
    }

    pub(crate) async fn ensure_open_agent_known_by_id_for_explicit_resume(
        &self,
        current_thread_id: ThreadId,
        agent_id: ThreadId,
    ) -> CodexResult<AgentMetadata> {
        self.ensure_open_agent_known_by_id_with_archive_policy(
            current_thread_id,
            agent_id,
            PersistedIdentityArchivePolicy::AllowArchivedForExplicitResume,
        )
        .await
    }

    async fn ensure_open_agent_known_by_id_with_archive_policy(
        &self,
        current_thread_id: ThreadId,
        agent_id: ThreadId,
        archive_policy: PersistedIdentityArchivePolicy,
    ) -> CodexResult<AgentMetadata> {
        let state = self.upgrade_for_tools()?;
        if let Some(metadata) = self.get_agent_metadata(agent_id) {
            return Ok(metadata);
        }
        let root_thread_id = self.root_thread_id()?;
        let Some(agent_graph_store) = state.agent_graph_store() else {
            return Err(CodexErr::ThreadNotFound(agent_id));
        };
        let identity = agent_graph_store
            .find_open_thread_spawn_descendant_by_id(root_thread_id, agent_id)
            .await
            .map_err(|err| {
                CodexErr::Fatal(format!(
                    "failed to resolve persisted agent {agent_id}: {err}"
                ))
            })?
            .ok_or(CodexErr::ThreadNotFound(agent_id))?;
        self.register_open_persisted_identity(current_thread_id, identity, archive_policy)
            .await
    }

    pub(crate) async fn ensure_open_agent_known_by_path(
        &self,
        current_thread_id: ThreadId,
        agent_path: &AgentPath,
    ) -> CodexResult<AgentMetadata> {
        let state = self.upgrade_for_tools()?;
        if let Some(agent_id) = self.state.agent_id_for_path(agent_path) {
            return self.ensure_agent_known(agent_id);
        }
        let root_thread_id = self.root_thread_id()?;
        let agent_graph_store = state.agent_graph_store().ok_or_else(|| {
            CodexErr::UnsupportedOperation("agent ownership store is unavailable".to_string())
        })?;
        let identity = agent_graph_store
            .find_open_thread_spawn_descendant_by_path(root_thread_id, agent_path.as_str())
            .await
            .map_err(|err| {
                CodexErr::Fatal(format!(
                    "failed to resolve persisted agent path `{agent_path}`: {err}"
                ))
            })?
            .ok_or_else(|| {
                CodexErr::UnsupportedOperation(format!(
                    "open owned agent path `{agent_path}` not found"
                ))
            })?;
        self.register_open_persisted_identity(
            current_thread_id,
            identity,
            PersistedIdentityArchivePolicy::CurrentOnly,
        )
        .await
    }

    fn root_thread_id(&self) -> CodexResult<ThreadId> {
        self.state
            .agent_id_for_path(&AgentPath::root())
            .ok_or_else(|| {
                CodexErr::UnsupportedOperation("agent root is not registered".to_string())
            })
    }

    async fn register_open_persisted_identity(
        &self,
        current_thread_id: ThreadId,
        selected_identity: codex_state::ThreadSpawnDescendantIdentity,
        archive_policy: PersistedIdentityArchivePolicy,
    ) -> CodexResult<AgentMetadata> {
        let state = self.upgrade_for_tools()?;
        let _lifecycle_mutation = state.lock_lifecycle_mutation().await;
        state.ensure_current_membership_mutation_allowed([
            current_thread_id,
            selected_identity.parent_thread_id,
            selected_identity.thread_id,
            self.current_membership_root_thread_id(),
        ])?;
        let archived = if let Some(thread) = state
            .indexed_thread_metadata(selected_identity.thread_id)
            .await
        {
            thread.archived_at.is_some()
        } else {
            state
                .read_stored_thread(ReadThreadParams {
                    thread_id: selected_identity.thread_id,
                    include_archived: true,
                    include_history: false,
                })
                .await?
                .archived_at
                .is_some()
        };
        if archived && matches!(archive_policy, PersistedIdentityArchivePolicy::CurrentOnly) {
            return Err(CodexErr::ThreadNotFound(selected_identity.thread_id));
        }
        if let Some(metadata) = self.get_agent_metadata(selected_identity.thread_id) {
            return Ok(metadata);
        }
        let root_thread_id = self.root_thread_id()?;
        let agent_graph_store = state.agent_graph_store().ok_or_else(|| {
            CodexErr::UnsupportedOperation("agent ownership store is unavailable".to_string())
        })?;
        let identity = agent_graph_store
            .find_open_thread_spawn_descendant_by_id(root_thread_id, selected_identity.thread_id)
            .await
            .map_err(|err| {
                CodexErr::Fatal(format!(
                    "failed to verify persisted agent {}: {err}",
                    selected_identity.thread_id
                ))
            })?
            .ok_or(CodexErr::ThreadNotFound(selected_identity.thread_id))?;
        let current_thread = state.get_thread(current_thread_id).await?;
        let config = current_thread.session.get_config().await;
        let persisted_source = identity
            .source
            .as_deref()
            .and_then(parse_persisted_session_source);
        let agent_path = identity
            .agent_path
            .as_deref()
            .and_then(|path| AgentPath::try_from(path).ok())
            .or_else(|| {
                persisted_source
                    .as_ref()
                    .and_then(SessionSource::get_agent_path)
            });
        let agent_role = identity.agent_role.or_else(|| {
            persisted_source
                .as_ref()
                .and_then(SessionSource::get_agent_role)
        });
        let agent_nickname = identity.agent_nickname.or_else(|| {
            persisted_source
                .as_ref()
                .and_then(SessionSource::get_nickname)
        });
        let depth = i32::try_from(identity.depth)
            .map_err(|_| CodexErr::InvalidRequest("stored agent depth is too large".to_string()))?;
        let source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: identity.parent_thread_id,
            depth,
            agent_path: agent_path.clone(),
            agent_nickname: agent_nickname.clone(),
            agent_role: agent_role.clone(),
        });
        let patch = ThreadMetadataPatch {
            source: Some(source.clone()),
            thread_source: Some(Some(ThreadSource::Subagent)),
            agent_path: Some(agent_path.as_ref().map(ToString::to_string)),
            agent_nickname: Some(agent_nickname.clone()),
            agent_role: Some(agent_role.clone()),
            ..Default::default()
        };
        if let Some(state_db) = state.state_db().await
            && let Some(mut thread) =
                state_db
                    .get_thread(identity.thread_id)
                    .await
                    .map_err(|err| {
                        CodexErr::Fatal(format!(
                            "failed to read indexed agent metadata for {}: {err}",
                            identity.thread_id
                        ))
                    })?
        {
            thread.source = serde_json::to_string(&source).map_err(|err| {
                CodexErr::Fatal(format!(
                    "failed to serialize canonical agent source for {}: {err}",
                    identity.thread_id
                ))
            })?;
            thread.thread_source = Some(ThreadSource::Subagent);
            thread.agent_path = agent_path.as_ref().map(ToString::to_string);
            thread.agent_nickname = agent_nickname.clone();
            thread.agent_role = agent_role.clone();
            state_db.upsert_thread(&thread).await.map_err(|err| {
                CodexErr::Fatal(format!(
                    "failed to repair indexed agent metadata for {}: {err}",
                    identity.thread_id
                ))
            })?;
        } else {
            state
                .update_thread_metadata(identity.thread_id, patch, /*include_archived*/ true)
                .await?;
        }
        let mut reservation = if crate::goal_supervisor::is_goal_supervisor_helper_source(&source) {
            self.state.reserve_uncounted_spawn_slot()
        } else {
            self.state.reserve_spawn_slot(/*max_threads*/ None)?
        };
        let mut metadata = self.prepare_agent_metadata(
            &mut reservation,
            &config,
            agent_path.clone(),
            agent_role.clone(),
            agent_nickname.clone(),
        )?;
        metadata.agent_id = Some(identity.thread_id);
        metadata.parent_thread_id = Some(identity.parent_thread_id);
        metadata.depth = Some(depth);
        metadata.agent_path = agent_path;
        metadata.agent_role = agent_role;
        metadata.agent_nickname = agent_nickname;
        reservation.commit(metadata.clone());
        Ok(metadata)
    }
}

fn parse_persisted_session_source(source: &str) -> Option<SessionSource> {
    serde_json::from_str(source)
        .or_else(|_| serde_json::from_value(serde_json::Value::String(source.to_string())))
        .ok()
}
