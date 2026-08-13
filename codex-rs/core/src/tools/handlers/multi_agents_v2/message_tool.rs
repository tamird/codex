//! Shared argument parsing and dispatch for the v2 agent messaging tools.
//!
//! `send_message` and `followup_task` share the same submission path and differ only in whether the
//! resulting `InterAgentCommunication` should wake the target immediately.

use super::analytics::ToolCallAnalytics;
use super::*;
use crate::agent::control::AgentInputDelivery;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::tools::context::FunctionToolOutput;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum MessageDeliveryMode {
    QueueOnly,
    TriggerTurn,
}

impl MessageDeliveryMode {
    fn trigger_turn(self) -> bool {
        match self {
            Self::QueueOnly => false,
            Self::TriggerTurn => true,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// Input for the MultiAgentV2 `send_message` tool.
pub(crate) struct SendMessageArgs {
    pub(crate) target: String,
    pub(crate) message: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// Input for the MultiAgentV2 `followup_task` tool.
pub(crate) struct FollowupTaskArgs {
    pub(crate) target: String,
    pub(crate) message: String,
}

pub(super) fn message_content(message: String) -> Result<String, FunctionCallError> {
    if message.trim().is_empty() {
        return Err(FunctionCallError::RespondToModel(
            "Empty message can't be sent to an agent".to_string(),
        ));
    }
    Ok(message)
}

/// Handles the shared MultiAgentV2 message flow for both `send_message` and `followup_task`.
pub(super) async fn handle_message_string_tool(
    invocation: ToolInvocation,
    mode: MessageDeliveryMode,
    target: String,
    message: String,
    analytics: &mut ToolCallAnalytics,
) -> Result<FunctionToolOutput, FunctionCallError> {
    let message = message_content(message)?;
    let ToolInvocation {
        session,
        turn,
        call_id,
        source,
        ..
    } = invocation;
    let target_is_parent = target == "parent";
    let direct_parent_thread_id = direct_parent_thread_id(&turn.session_source);
    let receiver_thread_id = if target_is_parent {
        direct_parent_thread_id.ok_or_else(|| {
            FunctionCallError::RespondToModel(
                "target `parent` is only available from a spawned agent.".to_string(),
            )
        })?
    } else {
        resolve_agent_target(&session, &turn, &target).await?
    };
    analytics.set_receiver(receiver_thread_id);
    let is_direct_parent = direct_parent_thread_id == Some(receiver_thread_id);
    let is_goal_supervisor_parent = session
        .services
        .agent_control
        .goal_supervisor_parent_for_helper(session.thread_id)
        .await
        == Some(receiver_thread_id);
    if mode == MessageDeliveryMode::QueueOnly && is_goal_supervisor_parent {
        return Err(FunctionCallError::RespondToModel(
            "supervisor check-in threads must use followup_task with target `parent` to message their parent."
                .to_string(),
        ));
    }
    if mode == MessageDeliveryMode::TriggerTurn && target_is_parent && !is_goal_supervisor_parent {
        return Err(FunctionCallError::RespondToModel(
            "Only supervisor check-in threads can use followup_task with target `parent`; use send_message for parent updates."
                .to_string(),
        ));
    }
    let receiver_agent = if is_direct_parent {
        session
            .services
            .agent_control
            .get_agent_metadata(receiver_thread_id)
            .unwrap_or_default()
    } else {
        session
            .services
            .agent_control
            .ensure_agent_known(receiver_thread_id)
            .map_err(|err| collab_agent_error(receiver_thread_id, err))?
    };
    if mode == MessageDeliveryMode::TriggerTurn
        && receiver_agent
            .agent_path
            .as_ref()
            .is_some_and(AgentPath::is_root)
        && !is_goal_supervisor_parent
    {
        return Err(FunctionCallError::RespondToModel(
            "Follow-up tasks can't target the root agent".to_string(),
        ));
    }
    let receiver_agent_path = receiver_agent
        .agent_path
        .clone()
        .or_else(|| {
            is_direct_parent
                .then(|| direct_parent_path(&turn.session_source))
                .flatten()
        })
        .ok_or_else(|| {
            FunctionCallError::RespondToModel("target agent is missing an agent_path".to_string())
        })?;
    let resume_config = (!is_direct_parent)
        .then(|| build_agent_resume_config(turn.as_ref()))
        .transpose()?;
    let author = turn
        .session_source
        .get_agent_path()
        .unwrap_or_else(AgentPath::root);
    let communication = communication_from_tool_message(
        author,
        receiver_agent_path.clone(),
        message,
        &source,
        mode.trigger_turn(),
    );
    let kind = match mode {
        MessageDeliveryMode::QueueOnly => AgentCommunicationKind::Message,
        MessageDeliveryMode::TriggerTurn => AgentCommunicationKind::Followup,
    };
    let context = AgentCommunicationContext::new(kind, session.thread_id);
    let parent_turn_id =
        matches!(mode, MessageDeliveryMode::TriggerTurn).then(|| turn.sub_id.clone());
    let delivered_communication = communication;
    let start_options = crate::TurnStartOptions {
        parent_turn_id,
        root_turn_id: turn.turn_metadata_state.root_turn_id(),
        cyber_access_program: turn.cyber_access_program,
        ..Default::default()
    };
    let result = match resume_config {
        Some(resume_config) => {
            session
                .services
                .agent_control
                .deliver_inter_agent_communication_to_agent(
                    resume_config,
                    receiver_thread_id,
                    delivered_communication.clone(),
                    context,
                    AgentInputDelivery::Queue,
                    start_options,
                )
                .await
        }
        None => {
            session
                .services
                .agent_control
                .send_inter_agent_communication(
                    receiver_thread_id,
                    delivered_communication.clone(),
                    context,
                    start_options,
                )
                .await
        }
    }
    .map_err(|err| collab_agent_error(receiver_thread_id, err));
    result?;
    emit_sub_agent_activity(
        &session,
        &turn,
        SubAgentActivityItem {
            id: call_id,
            agent_thread_id: receiver_thread_id,
            agent_path: receiver_agent_path,
            kind: SubAgentActivityKind::Interacted,
        },
    )
    .await;
    if mode == MessageDeliveryMode::TriggerTurn && is_goal_supervisor_parent {
        let _ = session
            .services
            .agent_control
            .record_goal_supervisor_followup_action(receiver_thread_id, &delivered_communication)
            .await;
        let _ = session
            .services
            .agent_control
            .finish_goal_supervisor_helper_after_followup(session.thread_id)
            .await;
    }

    let output = FunctionToolOutput::from_text(String::new(), Some(true));
    if mode == MessageDeliveryMode::TriggerTurn && is_goal_supervisor_parent {
        Ok(output.into_terminal_no_response())
    } else {
        Ok(output)
    }
}

fn direct_parent_thread_id(session_source: &SessionSource) -> Option<ThreadId> {
    match session_source {
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id, ..
        }) => Some(*parent_thread_id),
        SessionSource::Cli
        | SessionSource::VSCode
        | SessionSource::Exec
        | SessionSource::Mcp
        | SessionSource::Custom(_)
        | SessionSource::Internal(_)
        | SessionSource::SubAgent(SubAgentSource::Review)
        | SessionSource::SubAgent(SubAgentSource::Compact)
        | SessionSource::SubAgent(SubAgentSource::MemoryConsolidation)
        | SessionSource::SubAgent(SubAgentSource::Other(_))
        | SessionSource::Unknown => None,
    }
}

fn direct_parent_path(session_source: &SessionSource) -> Option<AgentPath> {
    let agent_path = session_source.get_agent_path()?;
    if agent_path.is_root() {
        return None;
    }
    let parent = agent_path.as_str().rsplit_once('/')?.0;
    if parent.is_empty() {
        return None;
    }
    AgentPath::try_from(parent).ok()
}
