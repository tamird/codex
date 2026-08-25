use super::*;
use crate::agent::status::is_final;
use crate::context::SubagentNotification;
use crate::session_prefix::format_inter_agent_completion_message;

/// Input whose submission remains paired with any required communication telemetry context.
enum AgentDeliveryInput {
    UserInput(Vec<UserInput>),
    InterAgentCommunication {
        communication: Box<InterAgentCommunication>,
        context: AgentCommunicationContext,
    },
}

impl AgentDeliveryInput {
    fn starts_turn(&self) -> bool {
        match self {
            Self::UserInput(_) => true,
            Self::InterAgentCommunication { communication, .. } => communication.trigger_turn,
        }
    }
}

impl AgentControl {
    /// Deliver work to an addressable agent while serializing cold reload with completion cleanup.
    pub(crate) async fn deliver_input_to_agent(
        &self,
        config: Config,
        agent_id: ThreadId,
        input: Vec<UserInput>,
        delivery: AgentInputDelivery,
        start_options: TurnStartOptions,
    ) -> CodexResult<String> {
        self.deliver_agent_input(
            config,
            agent_id,
            AgentDeliveryInput::UserInput(input),
            delivery,
            start_options,
        )
        .await
    }

    /// Deliver a context-bearing communication to an addressable agent, reloading it if needed.
    pub(crate) async fn deliver_inter_agent_communication_to_agent(
        &self,
        config: Config,
        agent_id: ThreadId,
        communication: InterAgentCommunication,
        context: AgentCommunicationContext,
        delivery: AgentInputDelivery,
        start_options: TurnStartOptions,
    ) -> CodexResult<String> {
        self.deliver_agent_input(
            config,
            agent_id,
            AgentDeliveryInput::InterAgentCommunication {
                communication: Box::new(communication),
                context,
            },
            delivery,
            start_options,
        )
        .await
    }

    async fn deliver_agent_input(
        &self,
        config: Config,
        agent_id: ThreadId,
        input: AgentDeliveryInput,
        delivery: AgentInputDelivery,
        start_options: TurnStartOptions,
    ) -> CodexResult<String> {
        let metadata = self.ensure_agent_known(agent_id)?;
        let lifecycle = metadata.lifecycle;
        loop {
            let transition = lifecycle.lock_transition().await;
            let state = self.upgrade()?;
            let completion_transition_pending = if lifecycle.completion_watcher_active() {
                match state.get_thread(agent_id).await {
                    Ok(thread) => completion_watcher_status_is_terminal(
                        &thread.agent_status().await,
                        thread.multi_agent_version() == Some(MultiAgentVersion::V2)
                            && metadata.agent_path.is_some(),
                    ),
                    Err(_) => true,
                }
            } else {
                false
            };
            if completion_transition_pending {
                drop(transition);
                lifecycle.wait_for_completion_watcher().await;
                continue;
            }

            let multi_agent_version = Box::pin(self.ensure_agent_loaded_locked(
                &state,
                config.clone(),
                agent_id,
                &lifecycle,
            ))
            .await?;
            if input.starts_turn() {
                let thread = state.get_thread(agent_id).await?;
                self.ensure_execution_capacity_for_turn_start(&thread)
                    .await?;
            }
            if multi_agent_version != MultiAgentVersion::V2 {
                self.maybe_start_completion_watcher_for_loaded_agent(&state, agent_id)
                    .await;
            }
            if delivery == AgentInputDelivery::Interrupt {
                self.interrupt_agent(agent_id).await?;
            }
            return match &input {
                AgentDeliveryInput::UserInput(input) => {
                    self.send_input_after_capacity_check(
                        agent_id,
                        &state,
                        input.clone(),
                        start_options,
                    )
                    .await
                }
                AgentDeliveryInput::InterAgentCommunication {
                    communication,
                    context,
                } => {
                    self.send_inter_agent_communication_after_capacity_check(
                        agent_id,
                        &state,
                        communication.as_ref().clone(),
                        context.clone(),
                        start_options,
                    )
                    .await
                }
            };
        }
    }

    /// Starts a detached watcher for sub-agents spawned from another thread.
    ///
    /// This is only enabled for `SubAgentSource::ThreadSpawn`, where a parent thread exists and
    /// can receive completion notifications.
    pub(super) fn maybe_start_completion_watcher(
        &self,
        child_thread_id: ThreadId,
        session_source: Option<SessionSource>,
        child_reference: String,
        child_agent_path: Option<AgentPath>,
    ) -> bool {
        let Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id,
            agent_role,
            ..
        })) = session_source
        else {
            return false;
        };
        let is_goal_supervisor_helper =
            agent_role.as_deref() == Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME);
        let lifecycle = self
            .get_agent_metadata(child_thread_id)
            .map(|metadata| metadata.lifecycle)
            .unwrap_or_default();
        let Some(watcher_registration) = lifecycle.try_start_completion_watcher() else {
            return false;
        };
        let control = self.clone();
        tokio::spawn(async move {
            let _watcher_registration = watcher_registration;
            let mut status_rx = control.subscribe_status(child_thread_id).await.ok();
            let child_uses_multi_agent_v2 = match control.upgrade() {
                Ok(state) => state
                    .get_thread(child_thread_id)
                    .await
                    .ok()
                    .is_none_or(|thread| {
                        thread.multi_agent_version() == Some(MultiAgentVersion::V2)
                    }),
                Err(_) => true,
            };
            let mut status = status_rx
                .as_ref()
                .map(|status_rx| status_rx.borrow().clone())
                .unwrap_or_else(|| AgentStatus::NotFound);
            let uses_inter_agent_completion =
                child_uses_multi_agent_v2 && child_agent_path.is_some();

            loop {
                while !completion_watcher_status_is_terminal_for_agent(
                    &status,
                    uses_inter_agent_completion,
                    is_goal_supervisor_helper,
                ) {
                    let Some(receiver) = status_rx.as_mut() else {
                        status = control.get_status(child_thread_id).await;
                        break;
                    };
                    if receiver.changed().await.is_err() {
                        status = control.get_status(child_thread_id).await;
                        break;
                    }
                    status = receiver.borrow().clone();
                }
                if !completion_watcher_status_is_terminal_for_agent(
                    &status,
                    uses_inter_agent_completion,
                    is_goal_supervisor_helper,
                ) {
                    return;
                }
                if is_goal_supervisor_helper {
                    let _ = control
                        .defer_failed_goal_supervisor_helper(
                            parent_thread_id,
                            child_thread_id,
                            status.clone(),
                        )
                        .await;
                    return;
                }

                let Ok(state) = control.upgrade() else {
                    return;
                };
                if child_agent_path.is_some() && child_uses_multi_agent_v2 {
                    let Some(child_agent_path) = child_agent_path.clone() else {
                        return;
                    };
                    let Some(parent_agent_path) = child_agent_path
                        .as_str()
                        .rsplit_once('/')
                        .and_then(|(parent, _)| AgentPath::try_from(parent).ok())
                    else {
                        return;
                    };
                    let Some(message) = format_inter_agent_completion_message(
                        parent_agent_path.clone(),
                        child_agent_path.clone(),
                        &status,
                    ) else {
                        return;
                    };
                    let communication = InterAgentCommunication::new(
                        child_agent_path,
                        parent_agent_path,
                        Vec::new(),
                        message,
                        /*trigger_turn*/ false,
                    );
                    let context = AgentCommunicationContext::new(
                        AgentCommunicationKind::Result,
                        child_thread_id,
                    );
                    let _ = control
                        .send_inter_agent_communication(
                            parent_thread_id,
                            communication,
                            context,
                            TurnStartOptions::default(),
                        )
                        .await;
                    return;
                }

                let _transition = lifecycle.lock_transition().await;
                let child_thread = state.get_thread(child_thread_id).await.ok();
                let status_advanced = status_rx
                    .as_ref()
                    .is_some_and(|receiver| receiver.has_changed().unwrap_or(false));
                let completion_quiescent = if status_advanced {
                    false
                } else if let Some(child_thread) = child_thread.as_ref() {
                    residency::is_unloadable(child_thread.as_ref()).await
                } else {
                    true
                };
                if let Ok(parent_thread) = state.get_thread(parent_thread_id).await {
                    parent_thread
                        .inject_fragment_without_turn(SubagentNotification::new(
                            child_reference.as_str(),
                            status.clone(),
                        ))
                        .await;
                }
                if child_uses_multi_agent_v2 {
                    return;
                }
                if completion_quiescent {
                    return;
                }
                drop(_transition);

                let Some(receiver) = status_rx.as_mut() else {
                    return;
                };
                if receiver.changed().await.is_err() {
                    return;
                }
                status = receiver.borrow().clone();
            }
        });
        true
    }

    async fn maybe_start_completion_watcher_for_loaded_agent(
        &self,
        state: &Arc<ThreadManagerState>,
        child_thread_id: ThreadId,
    ) {
        let Ok(child_thread) = state.get_thread(child_thread_id).await else {
            return;
        };
        let thread_config = child_thread.config_snapshot().await;
        let metadata = self.get_agent_metadata(child_thread_id).unwrap_or_default();
        let child_reference = metadata
            .agent_path
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| child_thread_id.to_string());
        self.maybe_start_completion_watcher(
            child_thread_id,
            Some(thread_config.session_source),
            child_reference,
            metadata.agent_path,
        );
    }
}

fn is_legacy_completion_status(status: &AgentStatus) -> bool {
    is_final(status) || matches!(status, AgentStatus::Interrupted)
}

fn completion_watcher_status_is_terminal(
    status: &AgentStatus,
    uses_inter_agent_completion: bool,
) -> bool {
    if uses_inter_agent_completion {
        is_final(status)
    } else {
        is_legacy_completion_status(status)
    }
}

fn completion_watcher_status_is_terminal_for_agent(
    status: &AgentStatus,
    uses_inter_agent_completion: bool,
    is_goal_supervisor_helper: bool,
) -> bool {
    (is_goal_supervisor_helper && matches!(status, AgentStatus::Interrupted))
        || completion_watcher_status_is_terminal(status, uses_inter_agent_completion)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interrupted_goal_supervisor_is_terminal_for_completion_watcher() {
        assert!(completion_watcher_status_is_terminal_for_agent(
            &AgentStatus::Interrupted,
            /*uses_inter_agent_completion*/ true,
            /*is_goal_supervisor_helper*/ true,
        ));
        assert!(!completion_watcher_status_is_terminal_for_agent(
            &AgentStatus::Interrupted,
            /*uses_inter_agent_completion*/ true,
            /*is_goal_supervisor_helper*/ false,
        ));
    }
}
