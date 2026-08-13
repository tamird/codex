use super::*;
use crate::agent::SupervisorParentCompactionResult;
use crate::tools::handlers::multi_agents_spec::create_supervisor_compact_parent_context_tool;
use crate::tools::handlers::multi_agents_spec::create_supervisor_tools_namespace;
use codex_tools::ToolSpec;

pub(crate) struct Handler;

impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> ToolName {
        ToolName::namespaced("supervisor", "compact_parent_context")
    }

    fn spec(&self) -> ToolSpec {
        create_supervisor_tools_namespace(vec![create_supervisor_compact_parent_context_tool()])
    }

    fn handle<'a>(&'a self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(async move {
            handle_compact_parent_context(invocation)
                .await
                .map(boxed_tool_output)
        })
    }
}

async fn handle_compact_parent_context(
    invocation: ToolInvocation,
) -> Result<CompactParentContextResult, FunctionCallError> {
    let ToolInvocation {
        session, payload, ..
    } = invocation;
    let arguments = function_arguments(payload)?;
    let args: CompactParentContextArgs = parse_arguments(&arguments)?;
    let _ = (args.reason, args.evidence);
    let result = session
        .services
        .agent_control
        .compact_parent_for_goal_supervisor_helper(session.thread_id)
        .await
        .map_err(|err| {
            FunctionCallError::RespondToModel(format!("compact_parent_context failed: {err}"))
        })?;
    if !matches!(
        &result,
        SupervisorParentCompactionResult::NotSupervisorHelper
    ) {
        let _ = session
            .services
            .agent_control
            .finish_goal_supervisor_helper(session.thread_id)
            .await;
    }
    Ok(CompactParentContextResult::from(result))
}

impl CoreToolRuntime for Handler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

#[derive(Debug, Deserialize)]
struct CompactParentContextArgs {
    reason: Option<String>,
    evidence: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct CompactParentContextResult {
    kind: &'static str,
    parent_thread_id: Option<String>,
    submission_id: Option<String>,
}

impl From<SupervisorParentCompactionResult> for CompactParentContextResult {
    fn from(value: SupervisorParentCompactionResult) -> Self {
        match value {
            SupervisorParentCompactionResult::NotSupervisorHelper => Self {
                kind: "not_supervisor_helper",
                parent_thread_id: None,
                submission_id: None,
            },
            SupervisorParentCompactionResult::ParentBusy { parent_thread_id } => Self {
                kind: "parent_busy",
                parent_thread_id: Some(parent_thread_id.to_string()),
                submission_id: None,
            },
            SupervisorParentCompactionResult::Submitted {
                parent_thread_id,
                submission_id,
            } => Self {
                kind: "submitted",
                parent_thread_id: Some(parent_thread_id.to_string()),
                submission_id: Some(submission_id),
            },
        }
    }
}

impl ToolOutput for CompactParentContextResult {
    fn log_output(&self) -> String {
        tool_output_json_text(self, "compact_parent_context")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn terminal_no_response(&self) -> bool {
        self.kind != "not_supervisor_helper"
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "compact_parent_context")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "compact_parent_context")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submitted_compaction_ends_the_supervisor_turn() {
        let result = CompactParentContextResult {
            kind: "submitted",
            parent_thread_id: Some("parent".to_string()),
            submission_id: Some("submission".to_string()),
        };

        assert!(result.terminal_no_response());
    }

    #[test]
    fn rejected_non_supervisor_compaction_keeps_the_turn_active() {
        let result = CompactParentContextResult {
            kind: "not_supervisor_helper",
            parent_thread_id: None,
            submission_id: None,
        };

        assert!(!result.terminal_no_response());
    }
}
