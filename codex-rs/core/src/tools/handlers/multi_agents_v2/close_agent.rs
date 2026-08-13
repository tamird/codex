use super::*;
use crate::tools::handlers::multi_agents::CloseAgentHandler;
use crate::tools::handlers::multi_agents_spec::create_close_agent_tool_v2;
use codex_tools::ToolSpec;

/// Authorizes MultiAgentV2 ownership before reusing the existing close-agent operation.
pub(crate) struct Handler;

impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain("close_agent")
    }

    fn spec(&self) -> ToolSpec {
        create_close_agent_tool_v2()
    }

    fn handle<'a>(&'a self, mut invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(async move {
            let arguments = function_arguments(invocation.payload.clone())?;
            let args: CloseAgentArgs = parse_arguments(&arguments)?;
            let caller_agent_path = match invocation.turn.session_source.get_agent_path() {
                Some(agent_path) => agent_path,
                None if invocation.turn.parent_thread_id.is_none()
                    && !invocation.turn.session_source.is_non_root_agent() =>
                {
                    AgentPath::root()
                }
                None => {
                    return Err(FunctionCallError::RespondToModel(
                        "calling agent is missing an agent_path".to_string(),
                    ));
                }
            };
            let agent_id =
                resolve_agent_target(&invocation.session, &invocation.turn, &args.target).await?;
            let receiver_agent = invocation
                .session
                .services
                .agent_control
                .ensure_agent_known(agent_id)
                .map_err(|err| collab_agent_error(agent_id, err))?;

            if receiver_agent
                .agent_path
                .as_ref()
                .is_some_and(AgentPath::is_root)
            {
                return Err(FunctionCallError::RespondToModel(
                    "root is not a spawned agent".to_string(),
                ));
            }
            if agent_id == invocation.session.thread_id {
                return Err(FunctionCallError::RespondToModel(
                    "an agent cannot close itself".to_string(),
                ));
            }
            if receiver_agent.agent_role.as_deref()
                == Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME)
            {
                return Err(FunctionCallError::RespondToModel(
                    "goal supervisor agents cannot be closed with close_agent".to_string(),
                ));
            }

            let receiver_agent_path = receiver_agent.agent_path.ok_or_else(|| {
                FunctionCallError::RespondToModel(
                    "target agent is missing an agent_path".to_string(),
                )
            })?;
            let is_owned_descendant = receiver_agent_path
                .as_str()
                .strip_prefix(caller_agent_path.as_str())
                .is_some_and(|suffix| suffix.starts_with('/'));
            if !is_owned_descendant {
                return Err(FunctionCallError::RespondToModel(
                    "target agent is not an owned descendant".to_string(),
                ));
            }

            invocation.payload = ToolPayload::Function {
                arguments: serde_json::json!({ "target": agent_id.to_string() }).to_string(),
            };
            CloseAgentHandler.handle(invocation).await
        })
    }
}

impl CoreToolRuntime for Handler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

/// Model-facing target accepted as a canonical task name or thread UUID.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CloseAgentArgs {
    target: String,
}
