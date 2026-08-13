use super::*;
use crate::agent::control::AgentInputDelivery;
use crate::agent::control::render_input_preview;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::tools::handlers::multi_agents_spec::create_send_input_tool_v1;
use codex_protocol::AgentPath;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::SessionSource;
use codex_tools::ToolSpec;

pub(crate) struct Handler;

impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> ToolName {
        ToolName::namespaced(MULTI_AGENT_V1_NAMESPACE, "send_input")
    }

    fn spec(&self) -> ToolSpec {
        create_send_input_tool_v1()
    }

    fn search_info(&self) -> Option<ToolSearchInfo> {
        multi_agent_tool_search_info(
            "send_input send message existing agent subagent follow up interrupt redirect queue target",
            self.spec(),
        )
    }

    fn handle<'a>(&'a self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(self.handle_call(invocation))
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
            call_id,
            ..
        } = invocation;
        let arguments = function_arguments(payload)?;
        let args: SendInputArgs = parse_arguments(&arguments)?;
        let receiver_thread_id = parse_agent_id_target(&args.target)?;
        let message = args.message.clone();
        let items = args.items.clone();
        let input_items = parse_collab_input(args.message, args.items)?;
        let prompt = render_input_preview(&input_items);
        let receiver_agent = session
            .services
            .agent_control
            .get_agent_metadata(receiver_thread_id);
        let resume_config = receiver_agent
            .as_ref()
            .map(|_| build_agent_resume_config(turn.as_ref()))
            .transpose()?;
        let receiver_agent = receiver_agent.unwrap_or_default();
        if args.interrupt && resume_config.is_none() {
            session
                .services
                .agent_control
                .interrupt_agent(receiver_thread_id)
                .await
                .map_err(|err| collab_agent_error(receiver_thread_id, err))?;
        }
        session
            .emit_turn_item_started(
                &turn,
                &TurnItem::CollabAgentToolCall(CollabAgentToolCallItem {
                    id: call_id.clone(),
                    tool: CollabAgentTool::SendInput,
                    status: CollabAgentToolCallStatus::InProgress,
                    sender_thread_id: session.thread_id,
                    receiver_thread_ids: vec![receiver_thread_id],
                    receiver_agents: Vec::new(),
                    prompt: Some(prompt.clone()),
                    model: None,
                    reasoning_effort: None,
                    agents_states: Default::default(),
                }),
            )
            .await;
        let agent_control = session.services.agent_control.clone();
        let sender_is_subagent = matches!(&turn.session_source, SessionSource::SubAgent(_));
        let communication = match (sender_is_subagent, message, items) {
            (true, Some(message), None) => {
                let sender_path = turn
                    .session_source
                    .get_agent_path()
                    .or_else(|| {
                        agent_control
                            .get_agent_metadata(session.thread_id)
                            .and_then(|metadata| metadata.agent_path)
                    })
                    .unwrap_or_else(|| fallback_agent_path(session.thread_id));
                let receiver_path = receiver_agent
                    .agent_path
                    .clone()
                    .unwrap_or_else(|| fallback_agent_path(receiver_thread_id));
                Some(InterAgentCommunication::new(
                    sender_path,
                    receiver_path,
                    Vec::new(),
                    message,
                    /*trigger_turn*/ true,
                ))
            }
            _ => None,
        };
        let context =
            AgentCommunicationContext::new(AgentCommunicationKind::Followup, session.thread_id);
        let delivery = if args.interrupt {
            AgentInputDelivery::Interrupt
        } else {
            AgentInputDelivery::Queue
        };
        let start_options = crate::TurnStartOptions {
            parent_turn_id: Some(turn.sub_id.clone()),
            root_turn_id: turn.turn_metadata_state.root_turn_id(),
            cyber_access_program: turn.cyber_access_program,
            ..Default::default()
        };
        let result = match (resume_config, communication) {
            (Some(resume_config), Some(communication)) => {
                agent_control
                    .deliver_inter_agent_communication_to_agent(
                        resume_config,
                        receiver_thread_id,
                        communication,
                        context,
                        delivery,
                        start_options,
                    )
                    .await
            }
            (Some(resume_config), None) => {
                agent_control
                    .deliver_input_to_agent(
                        resume_config,
                        receiver_thread_id,
                        input_items,
                        delivery,
                        start_options,
                    )
                    .await
            }
            (None, Some(communication)) => {
                agent_control
                    .send_inter_agent_communication(
                        receiver_thread_id,
                        communication,
                        context,
                        start_options,
                    )
                    .await
            }
            (None, None) => {
                agent_control
                    .send_input(receiver_thread_id, input_items, start_options)
                    .await
            }
        }
        .map_err(|err| collab_agent_error(receiver_thread_id, err));
        let status = session
            .services
            .agent_control
            .get_status(receiver_thread_id)
            .await;
        session
            .emit_turn_item_completed(
                &turn,
                TurnItem::CollabAgentToolCall(CollabAgentToolCallItem {
                    id: call_id,
                    tool: CollabAgentTool::SendInput,
                    status: collab_tool_call_status(&status, Some(receiver_thread_id)),
                    sender_thread_id: session.thread_id,
                    receiver_thread_ids: vec![receiver_thread_id],
                    receiver_agents: vec![CollabAgentRef {
                        thread_id: receiver_thread_id,
                        agent_nickname: receiver_agent.agent_nickname,
                        agent_role: receiver_agent.agent_role,
                    }],
                    prompt: Some(prompt),
                    model: None,
                    reasoning_effort: None,
                    agents_states: [(receiver_thread_id, status)].into_iter().collect(),
                }),
            )
            .await;
        let submission_id = result?;

        Ok(boxed_tool_output(SendInputResult { submission_id }))
    }
}

impl CoreToolRuntime for Handler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

fn fallback_agent_path(thread_id: ThreadId) -> AgentPath {
    let name = format!("thread_{}", thread_id.to_string().replace('-', "_"));
    AgentPath::root()
        .join(&name)
        .unwrap_or_else(|_| AgentPath::root())
}

#[derive(Debug, Deserialize)]
struct SendInputArgs {
    target: String,
    message: Option<String>,
    items: Option<Vec<UserInput>>,
    #[serde(default)]
    interrupt: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct SendInputResult {
    submission_id: String,
}

impl ToolOutput for SendInputResult {
    fn log_output(&self) -> String {
        tool_output_json_text(self, "send_input")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "send_input")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "send_input")
    }
}
