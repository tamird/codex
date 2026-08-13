use super::analytics::ToolCallAnalytics;
use super::*;
use crate::agent::control::ListedAgent;
use crate::tools::handlers::multi_agents_spec::create_list_agents_tool;
use codex_tools::ToolSpec;

pub(crate) struct Handler;

impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain("list_agents")
    }

    fn spec(&self) -> ToolSpec {
        create_list_agents_tool()
    }

    fn handle<'a>(&'a self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(async move {
            let analytics = ToolCallAnalytics::new(&invocation, CollabAgentTool::ListAgents);
            let result = self.handle_call(invocation).await;
            analytics.finish(&result);
            result
        })
    }
}

impl Handler {
    async fn handle_call(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn crate::tools::context::ToolOutput>, FunctionCallError> {
        let ToolInvocation {
            session,
            turn,
            payload,
            ..
        } = invocation;
        let arguments = function_arguments(payload)?;
        let args: ListAgentsArgs = parse_arguments(&arguments)?;
        session
            .services
            .agent_control
            .register_session_root(session.thread_id, turn.parent_thread_id);
        let page = session
            .services
            .agent_control
            .list_agents_page(
                &turn.session_source,
                args.path_prefix.as_deref(),
                args.cursor.as_deref(),
                args.limit,
            )
            .await
            .map_err(collab_spawn_error)?;

        Ok(boxed_tool_output(ListAgentsResult {
            agents: page.agents,
            next_cursor: page.next_cursor,
            total_count: page.total_count,
        }))
    }
}

impl CoreToolRuntime for Handler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListAgentsArgs {
    path_prefix: Option<String>,
    cursor: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ListAgentsResult {
    agents: Vec<ListedAgent>,
    next_cursor: Option<String>,
    total_count: usize,
}

impl ToolOutput for ListAgentsResult {
    fn log_output(&self) -> String {
        tool_output_json_text(self, "list_agents")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "list_agents")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "list_agents")
    }
}
