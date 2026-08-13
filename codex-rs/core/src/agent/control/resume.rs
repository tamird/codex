use super::ownership::ResumedThreadOwnership;
use super::ownership::normalize_resumed_session_metadata;
use super::residency::is_resident_session_source;
use super::*;
use codex_thread_store::ThreadMetadataPatch;

pub(super) async fn load_agent_model_context(
    state: &ThreadManagerState,
    thread_id: ThreadId,
    history_mode: ThreadHistoryMode,
) -> CodexResult<Option<Vec<RolloutItem>>> {
    match history_mode {
        ThreadHistoryMode::Legacy => Ok(state
            .read_stored_thread(ReadThreadParams {
                thread_id,
                include_archived: true,
                include_history: true,
            })
            .await?
            .history
            .map(|history| history.items)),
        ThreadHistoryMode::Paginated => Ok(Some(
            state
                .load_latest_model_context(LoadThreadHistoryParams {
                    thread_id,
                    include_archived: true,
                })
                .await?
                .items,
        )),
    }
}

impl AgentControl {
    pub(crate) async fn ensure_agent_loaded(
        &self,
        config: Config,
        thread_id: ThreadId,
    ) -> CodexResult<()> {
        let state = self.upgrade()?;
        let lifecycle = self.ensure_agent_known(thread_id)?.lifecycle;
        let _transition = lifecycle.lock_transition().await;
        Box::pin(self.ensure_agent_loaded_locked(&state, config, thread_id)).await?;
        Ok(())
    }

    pub(super) async fn ensure_agent_loaded_locked(
        &self,
        state: &Arc<ThreadManagerState>,
        config: Config,
        thread_id: ThreadId,
    ) -> CodexResult<MultiAgentVersion> {
        if let Ok(thread) = state.get_thread(thread_id).await {
            self.touch_loaded_agent_residency(state, thread_id).await;
            return Ok(thread
                .multi_agent_version()
                .unwrap_or(MultiAgentVersion::V1));
        }
        let registered_agent = self
            .state
            .agent_metadata_for_thread(thread_id)
            .ok_or(CodexErr::ThreadNotFound(thread_id))?;

        let stored_thread = state
            .read_stored_thread(ReadThreadParams {
                thread_id,
                include_archived: true,
                include_history: false,
            })
            .await?;
        let stored_source = stored_thread.source.clone();
        let stored_parent_thread_id = stored_thread.parent_thread_id;
        let history = load_agent_model_context(state, thread_id, stored_thread.history_mode)
            .await?
            .ok_or(CodexErr::ThreadNotFound(thread_id))?;
        let mut initial_history = InitialHistory::Resumed(ResumedHistory {
            conversation_id: thread_id,
            history: history.into(),
            rollout_path: stored_thread.rollout_path.clone(),
        });
        let (mut session_source, _) = initial_history
            .get_resumed_session_sources()
            .unwrap_or((stored_source.clone(), None));
        if (session_source != stored_source
            || session_source.get_agent_path() != registered_agent.agent_path)
            && let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth,
                agent_path,
                agent_nickname,
                agent_role,
            }) = stored_source
        {
            let canonical_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: stored_parent_thread_id.unwrap_or(parent_thread_id),
                depth,
                agent_path: registered_agent.agent_path.clone().or(agent_path),
                agent_nickname: registered_agent.agent_nickname.clone().or(agent_nickname),
                agent_role: registered_agent.agent_role.clone().or(agent_role),
            });
            if let InitialHistory::Resumed(resumed) = &mut initial_history {
                normalize_resumed_session_metadata(
                    Arc::make_mut(&mut resumed.history).as_mut_slice(),
                    thread_id,
                    &canonical_source,
                    canonical_source.parent_thread_id(),
                    Some(&registered_agent),
                    self.session_id(),
                )?;
            }
            session_source = canonical_source;
        }
        let multi_agent_version = state
            .effective_multi_agent_version_for_spawn(
                &initial_history,
                Some(&session_source),
                stored_parent_thread_id,
                /*forked_from_thread_id*/ None,
                &config,
            )
            .await;
        let mut config = config;
        let runtime_approvals_reviewer = config.approvals_reviewer;
        if let Some(role) = stored_thread
            .agent_role
            .as_deref()
            .or(registered_agent.agent_role.as_deref())
        {
            crate::agent::role::apply_role_to_config(&mut config, Some(role))
                .await
                .map_err(CodexErr::InvalidRequest)?;
        }
        config.approvals_reviewer = runtime_approvals_reviewer;
        // Role configuration supplies the agent-specific defaults. Persisted execution settings
        // remain authoritative when a cold agent is loaded into a later root session.
        if config.model_provider_id != stored_thread.model_provider {
            config.model_provider = config
                .model_providers
                .get(&stored_thread.model_provider)
                .cloned()
                .ok_or_else(|| {
                    CodexErr::InvalidRequest(format!(
                        "cannot restore agent {} because its original model provider `{}` is unavailable",
                        thread_id, stored_thread.model_provider
                    ))
                })?;
            config.model_provider_id = stored_thread.model_provider.clone();
        }
        if let Some(model) = &stored_thread.model {
            config.model = Some(model.clone());
        }
        config.model_reasoning_effort = stored_thread.reasoning_effort.clone();
        config
            .permissions
            .approval_policy
            .set(stored_thread.approval_mode)
            .map_err(|err| {
                CodexErr::InvalidRequest(format!(
                    "cannot restore the stored agent approval policy: {err}"
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
                    "cannot restore the stored agent permission profile: {err}"
                ))
            })?;
        // Reserving a resident can unload a nested parent. Capture its attachments first.
        let environment_selections = self.state.evicted_environments(thread_id);
        let inherited_environments = self
            .inherited_environments_for_source(state, Some(&session_source))
            .await;
        let inherited_exec_policy = self
            .inherited_exec_policy_for_source(state, Some(&session_source), &config)
            .await;
        let residency_slot = if is_resident_session_source(&session_source) {
            Some(
                self.reserve_agent_residency_slot(
                    state,
                    &config,
                    multi_agent_version,
                    Some(thread_id),
                )
                .await?,
            )
        } else {
            None
        };
        let parent_thread_id = initial_history
            .get_resumed_parent_thread_id()
            .or(stored_parent_thread_id);
        match state
            .resume_thread_with_history_with_source(ResumeThreadWithHistoryOptions {
                config,
                initial_history,
                agent_control: self.clone(),
                session_source,
                parent_thread_id,
                environment_selections,
                inherited_environments,
                inherited_exec_policy,
                client_mcp_extensions: None,
                inherited_thread_state: Default::default(),
            })
            .await
        {
            Ok(reloaded_thread) => {
                self.state.clear_evicted_environments(thread_id);
                registered_agent.lifecycle.clear_cold_terminal_status();
                if let Some(residency_slot) = residency_slot {
                    residency_slot.commit(reloaded_thread.thread_id);
                }
                state.notify_thread_created(reloaded_thread.thread_id);
                Ok(multi_agent_version)
            }
            Err(err) => {
                if state.get_thread(thread_id).await.is_ok() {
                    self.state.clear_evicted_environments(thread_id);
                    drop(residency_slot);
                    self.touch_loaded_agent_residency(state, thread_id).await;
                    return Ok(state
                        .get_thread(thread_id)
                        .await?
                        .multi_agent_version()
                        .unwrap_or(MultiAgentVersion::V1));
                }
                Err(err)
            }
        }
    }

    /// Resume an existing agent thread from a recorded rollout file.
    #[cfg(test)]
    pub(crate) async fn resume_agent_from_rollout(
        &self,
        config: Config,
        thread_id: ThreadId,
        session_source: SessionSource,
    ) -> CodexResult<ThreadId> {
        self.resume_agent_from_rollout_with_ownership(
            config,
            thread_id,
            session_source,
            ResumedThreadOwnership::Preserve,
        )
        .await
    }

    pub(super) async fn resume_agent_from_rollout_with_ownership(
        &self,
        config: Config,
        thread_id: ThreadId,
        session_source: SessionSource,
        ownership: ResumedThreadOwnership,
    ) -> CodexResult<ThreadId> {
        let (resumed_thread_id, _) = Box::pin(self.resume_single_agent_from_rollout(
            config,
            thread_id,
            session_source,
            ownership,
        ))
        .await?;
        Ok(resumed_thread_id)
    }

    async fn resume_single_agent_from_rollout(
        &self,
        config: Config,
        thread_id: ThreadId,
        session_source: SessionSource,
        ownership: ResumedThreadOwnership,
    ) -> CodexResult<(ThreadId, MultiAgentVersion)> {
        let state = self.upgrade()?;
        let stored_thread = state
            .read_stored_thread(ReadThreadParams {
                thread_id,
                include_archived: true,
                include_history: false,
            })
            .await?;
        let resumed_agent_path = stored_thread
            .agent_path
            .as_deref()
            .map(AgentPath::try_from)
            .transpose()
            .map_err(|err| CodexErr::InvalidRequest(format!("invalid stored agent path: {err}")))?;
        let resumed_agent_nickname = stored_thread.agent_nickname.clone();
        let resumed_agent_role = stored_thread.agent_role.clone();
        let history = load_agent_model_context(&state, thread_id, stored_thread.history_mode)
            .await?
            .ok_or(CodexErr::ThreadNotFound(thread_id))?;
        let mut initial_history = InitialHistory::Resumed(ResumedHistory {
            conversation_id: thread_id,
            history: history.into(),
            rollout_path: stored_thread.rollout_path,
        });
        let parent_thread_id = match ownership {
            #[cfg(test)]
            ResumedThreadOwnership::Preserve => stored_thread.parent_thread_id,
            ResumedThreadOwnership::Transfer => session_source
                .parent_thread_id()
                .or(stored_thread.parent_thread_id),
        };
        let multi_agent_version = state
            .effective_multi_agent_version_for_spawn(
                &initial_history,
                Some(&session_source),
                parent_thread_id,
                /*forked_from_thread_id*/ None,
                &config,
            )
            .await;
        let uses_residency = is_resident_session_source(&session_source);
        let residency_slot = if uses_residency {
            Some(
                self.reserve_agent_residency_slot(
                    &state,
                    &config,
                    multi_agent_version,
                    Some(thread_id),
                )
                .await?,
            )
        } else {
            None
        };
        let agent_max_threads = config.effective_agent_max_threads(multi_agent_version);
        let reservation_max_threads = if uses_residency {
            None
        } else {
            agent_max_threads
        };
        let mut reservation =
            if crate::goal_supervisor::is_goal_supervisor_helper_source(&session_source) {
                self.state.reserve_uncounted_spawn_slot()
            } else {
                self.state.reserve_spawn_slot(reservation_max_threads)?
            };
        let (session_source, agent_metadata) = match session_source {
            SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth,
                agent_path,
                agent_role: _,
                agent_nickname: _,
            }) => self.prepare_thread_spawn(
                &mut reservation,
                &config,
                parent_thread_id,
                depth,
                agent_path.or(resumed_agent_path),
                resumed_agent_role,
                resumed_agent_nickname,
            )?,
            other => (other, AgentMetadata::default()),
        };
        if ownership == ResumedThreadOwnership::Transfer
            && let InitialHistory::Resumed(resumed) = &mut initial_history
        {
            normalize_resumed_session_metadata(
                Arc::make_mut(&mut resumed.history).as_mut_slice(),
                thread_id,
                &session_source,
                parent_thread_id,
                Some(&agent_metadata),
                if session_source.is_non_root_agent() {
                    self.session_id()
                } else {
                    SessionId::from(thread_id)
                },
            )?;
        }
        let notification_source = session_source.clone();
        let inherited_environments = self
            .inherited_environments_for_source(&state, Some(&session_source))
            .await;
        let inherited_exec_policy = self
            .inherited_exec_policy_for_source(&state, Some(&session_source), &config)
            .await;

        let resumed_thread = state
            .resume_thread_with_history_with_source(ResumeThreadWithHistoryOptions {
                config: config.clone(),
                initial_history,
                agent_control: self.clone(),
                session_source: session_source.clone(),
                parent_thread_id,
                environment_selections: None,
                inherited_environments,
                inherited_exec_policy,
                client_mcp_extensions: None,
                inherited_thread_state: Default::default(),
            })
            .await?;
        if ownership == ResumedThreadOwnership::Transfer {
            resumed_thread
                .thread
                .update_thread_metadata(
                    ThreadMetadataPatch {
                        source: Some(session_source.clone()),
                        thread_source: Some(Some(if session_source.is_non_root_agent() {
                            ThreadSource::Subagent
                        } else {
                            ThreadSource::User
                        })),
                        agent_path: Some(
                            agent_metadata.agent_path.as_ref().map(ToString::to_string),
                        ),
                        agent_nickname: Some(agent_metadata.agent_nickname.clone()),
                        agent_role: Some(agent_metadata.agent_role.clone()),
                        ..Default::default()
                    },
                    /*include_archived*/ true,
                )
                .await
                .map_err(|err| {
                    CodexErr::Fatal(format!("failed to persist resumed agent metadata: {err}"))
                })?;
        }
        let mut agent_metadata = agent_metadata;
        agent_metadata.agent_id = Some(resumed_thread.thread_id);
        reservation.commit(agent_metadata.clone());
        // Resumed threads are re-registered in-memory and need the same listener
        // attachment path as freshly spawned threads.
        state.notify_thread_created(resumed_thread.thread_id);
        let pathless_multi_agent_child = agent_metadata.agent_path.is_none();
        if multi_agent_version != MultiAgentVersion::V2 || pathless_multi_agent_child {
            let child_reference = agent_metadata
                .agent_path
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| resumed_thread.thread_id.to_string());
            self.maybe_start_completion_watcher(
                resumed_thread.thread_id,
                Some(notification_source.clone()),
                child_reference,
                agent_metadata.agent_path.clone(),
            );
        }
        self.persist_thread_spawn_edge_for_source(
            resumed_thread.thread.as_ref(),
            resumed_thread.thread_id,
            Some(&notification_source),
        )
        .await;
        if let Some(residency_slot) = residency_slot {
            residency_slot.commit(resumed_thread.thread_id);
        }

        Ok((resumed_thread.thread_id, multi_agent_version))
    }
}
