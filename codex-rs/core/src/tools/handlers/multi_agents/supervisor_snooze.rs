use super::*;
use crate::tools::handlers::multi_agents_spec::create_supervisor_snooze_tool;
use crate::tools::handlers::multi_agents_spec::create_supervisor_tools_namespace;
use codex_tools::ToolSpec;

pub(crate) struct Handler;

impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> ToolName {
        ToolName::namespaced("supervisor", "snooze")
    }

    fn spec(&self) -> ToolSpec {
        create_supervisor_tools_namespace(vec![create_supervisor_snooze_tool()])
    }

    fn handle<'a>(&'a self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(async move { handle_snooze(invocation).await.map(boxed_tool_output) })
    }
}

async fn handle_snooze(
    invocation: ToolInvocation,
) -> Result<SupervisorSnoozeResult, FunctionCallError> {
    let ToolInvocation {
        session, payload, ..
    } = invocation;
    let arguments = function_arguments(payload)?;
    let args: SupervisorSnoozeArgs = parse_arguments(&arguments)?;
    let Some(delay_seconds) = session
        .services
        .agent_control
        .snooze_goal_supervisor_helper(
            session.thread_id,
            args.delay_seconds,
            args.reason.as_deref(),
        )
        .await
    else {
        return Err(FunctionCallError::RespondToModel(
            "supervisor.snooze is only available in goal supervisor check-in threads.".to_string(),
        ));
    };
    Ok(SupervisorSnoozeResult { delay_seconds })
}

impl CoreToolRuntime for Handler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

#[derive(Debug, Deserialize)]
struct SupervisorSnoozeArgs {
    delay_seconds: u64,
    reason: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SupervisorSnoozeResult {
    delay_seconds: u64,
}

impl ToolOutput for SupervisorSnoozeResult {
    fn log_output(&self) -> String {
        tool_output_json_text(self, "snooze")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn terminal_no_response(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "snooze")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "snooze")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snooze_result_ends_the_supervisor_turn() {
        let result = SupervisorSnoozeResult { delay_seconds: 60 };

        assert!(result.terminal_no_response());
    }
}
