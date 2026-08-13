use super::*;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::context::ContextualUserFragment;
use crate::context::InterAgentMessage;
use crate::context::InterAgentMessageType;
use crate::tools::context::ToolCallSource;
use crate::tools::handlers::multi_agents_spec::create_supervisor_followup_parent_tool;
use crate::tools::handlers::multi_agents_spec::create_supervisor_tools_namespace;
use codex_protocol::AgentPath;
use codex_protocol::protocol::InterAgentCommunication;
use codex_tools::ToolSpec;

/// Delivers a Goal Supervisor result without extending `collaboration.*`.
pub(crate) struct Handler;

impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> ToolName {
        ToolName::namespaced("supervisor", "followup_parent")
    }

    fn spec(&self) -> ToolSpec {
        create_supervisor_tools_namespace(vec![create_supervisor_followup_parent_tool()])
    }

    fn handle<'a>(&'a self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(async move {
            handle_followup_parent(invocation)
                .await
                .map(boxed_tool_output)
        })
    }
}

async fn handle_followup_parent(
    invocation: ToolInvocation,
) -> Result<SupervisorFollowupParentResult, FunctionCallError> {
    let ToolInvocation {
        session,
        turn,
        payload,
        source,
        ..
    } = invocation;
    let arguments = function_arguments(payload)?;
    let args: SupervisorFollowupParentArgs = parse_arguments(&arguments)?;
    if args.message.trim().is_empty() {
        return Err(FunctionCallError::RespondToModel(
            "Empty message can't be sent to the supervised parent".to_string(),
        ));
    }
    let Some(parent_thread_id) = session
        .services
        .agent_control
        .goal_supervisor_parent_for_helper(session.thread_id)
        .await
    else {
        return Err(FunctionCallError::RespondToModel(
            "supervisor.followup_parent is only available in goal supervisor check-in threads."
                .to_string(),
        ));
    };
    let receiver_agent = session
        .services
        .agent_control
        .get_agent_metadata(parent_thread_id)
        .unwrap_or_default();
    let receiver_path = receiver_agent.agent_path.unwrap_or_else(AgentPath::root);
    let author = turn
        .session_source
        .get_agent_path()
        .unwrap_or_else(AgentPath::root);
    let communication =
        plaintext_followup_communication(author, receiver_path, args.message, &source)?;
    let context =
        AgentCommunicationContext::new(AgentCommunicationKind::Followup, session.thread_id);
    session
        .services
        .agent_control
        .send_inter_agent_communication(
            parent_thread_id,
            communication.clone(),
            context,
            crate::TurnStartOptions {
                parent_turn_id: Some(turn.sub_id.clone()),
                root_turn_id: turn.turn_metadata_state.root_turn_id(),
                cyber_access_program: turn.cyber_access_program,
                ..Default::default()
            },
        )
        .await
        .map_err(|err| collab_agent_error(parent_thread_id, err))?;
    let recorded = session
        .services
        .agent_control
        .record_goal_supervisor_followup_action(parent_thread_id, &communication)
        .await;
    if !recorded {
        return Err(FunctionCallError::RespondToModel(
            "The parent received the follow-up, but its goal supervisor action was not recorded."
                .to_string(),
        ));
    }
    let delivered = session
        .services
        .agent_control
        .finish_goal_supervisor_helper_after_followup(session.thread_id)
        .await;
    if !delivered {
        return Err(FunctionCallError::RespondToModel(
            "The parent received the follow-up, but the goal supervisor check-in did not finish."
                .to_string(),
        ));
    }

    Ok(SupervisorFollowupParentResult { delivered })
}

/// Builds the only persisted representation accepted by this unencrypted tool schema.
fn plaintext_followup_communication(
    author: AgentPath,
    recipient: AgentPath,
    message: String,
    source: &ToolCallSource,
) -> Result<InterAgentCommunication, FunctionCallError> {
    match source {
        ToolCallSource::DirectPlaintextMessage | ToolCallSource::CodeMode { .. } => {}
        ToolCallSource::Direct => {
            return Err(FunctionCallError::RespondToModel(
                "supervisor.followup_parent does not accept encrypted direct arguments."
                    .to_string(),
            ));
        }
    }
    let content = InterAgentMessage::new(
        InterAgentMessageType::NewTask,
        recipient.clone(),
        author.clone(),
        message,
    )
    .render();
    Ok(InterAgentCommunication::new(
        author,
        recipient,
        Vec::new(),
        content,
        /*trigger_turn*/ true,
    ))
}

impl CoreToolRuntime for Handler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SupervisorFollowupParentArgs {
    message: String,
}

#[derive(Debug, Serialize)]
struct SupervisorFollowupParentResult {
    delivered: bool,
}

impl ToolOutput for SupervisorFollowupParentResult {
    fn log_output(&self) -> String {
        tool_output_json_text(self, "followup_parent")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn terminal_no_response(&self) -> bool {
        self.delivered
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "followup_parent")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "followup_parent")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(value: &str) -> AgentPath {
        AgentPath::try_from(value).expect("valid agent path")
    }

    #[test]
    fn direct_plaintext_and_code_mode_build_plaintext_followups() {
        for source in [
            ToolCallSource::DirectPlaintextMessage,
            ToolCallSource::CodeMode {
                cell_id: "cell-1".to_string(),
                runtime_tool_call_id: "runtime-1".to_string(),
            },
        ] {
            let communication = plaintext_followup_communication(
                path("/root/goal_supervisor"),
                AgentPath::root(),
                "continue the active goal".to_string(),
                &source,
            )
            .expect("plaintext sources should be accepted");

            assert!(communication.encrypted_content.is_none());
            assert!(communication.content.contains("continue the active goal"));
            assert!(communication.trigger_turn);
        }
    }

    #[test]
    fn encrypted_direct_source_is_rejected_before_communication() {
        let error = plaintext_followup_communication(
            path("/root/goal_supervisor"),
            AgentPath::root(),
            "opaque direct value".to_string(),
            &ToolCallSource::Direct,
        )
        .expect_err("encrypted direct source should be rejected");

        assert_eq!(
            error,
            FunctionCallError::RespondToModel(
                "supervisor.followup_parent does not accept encrypted direct arguments."
                    .to_string()
            )
        );
    }
}
