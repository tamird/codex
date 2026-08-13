use super::*;
use crate::tools::handlers::multi_agents_spec::create_promote_agent_tool;
use codex_protocol::ThreadId;
use codex_tools::ToolSpec;

pub(crate) struct Handler;

impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain("promote_agent")
    }

    fn spec(&self) -> ToolSpec {
        create_promote_agent_tool()
    }

    fn handle<'a>(&'a self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(async move {
            handle_promote_agent(invocation)
                .await
                .map(boxed_tool_output)
        })
    }
}

async fn handle_promote_agent(
    invocation: ToolInvocation,
) -> Result<PromoteAgentResult, FunctionCallError> {
    let ToolInvocation {
        session,
        turn,
        payload,
        ..
    } = invocation;
    if !turn.config.multi_agent_v2.enable_thread_adoption {
        return Err(FunctionCallError::RespondToModel(
            "Thread adoption is disabled. Set `[features.multi_agent_v2] enable_thread_adoption = true` in config.toml to enable it."
                .to_string(),
        ));
    }
    let arguments = function_arguments(payload)?;
    let args: PromoteAgentArgs = parse_arguments(&arguments)?;
    let thread_id = resolve_agent_target(&session, &turn, &args.target).await?;
    let thread_id = session
        .services
        .agent_control
        .promote_agent(thread_id)
        .await
        .map_err(|err| collab_agent_error(thread_id, err))?;
    Ok(PromoteAgentResult { thread_id })
}

impl CoreToolRuntime for Handler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PromoteAgentArgs {
    target: String,
}

#[derive(Debug, Serialize)]
struct PromoteAgentResult {
    thread_id: ThreadId,
}

impl ToolOutput for PromoteAgentResult {
    fn log_output(&self) -> String {
        tool_output_json_text(self, "promote_agent")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "promote_agent")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "promote_agent")
    }
}
