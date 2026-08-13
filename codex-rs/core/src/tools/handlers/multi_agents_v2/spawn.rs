use super::*;
use crate::agent::control::SpawnAgentForkMode;
use crate::agent::control::SpawnAgentOptions;
use crate::agent::next_thread_spawn_depth;
use crate::agent::role::DEFAULT_ROLE_NAME;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::codex_thread::ThreadConfigSnapshot;
use crate::session::multi_agents::resolve_usage_hints;
use crate::tools::handlers::multi_agents::collab_tool_call_status;
use crate::tools::handlers::multi_agents_spec::SpawnAgentToolOptions;
use crate::tools::handlers::multi_agents_spec::create_adopt_agent_tool;
use crate::tools::handlers::multi_agents_spec::create_spawn_agent_tool_v2;
use crate::tools::handlers::multi_agents_v2::message_tool::message_content;
use crate::turn_timing::now_unix_timestamp_ms;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::protocol::MultiAgentVersion;
use codex_tools::ToolSpec;

#[derive(Default)]
pub(crate) struct Handler {
    options: SpawnAgentToolOptions,
}

/// Executes ownership transfer without changing the reserved spawn-agent contract.
pub(crate) struct AdoptHandler {
    hide_agent_metadata: bool,
}

impl Handler {
    pub(crate) fn new(options: SpawnAgentToolOptions) -> Self {
        Self { options }
    }
}

impl AdoptHandler {
    pub(crate) fn new(hide_agent_metadata: bool) -> Self {
        Self {
            hide_agent_metadata,
        }
    }
}

impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain("spawn_agent")
    }

    fn spec(&self) -> ToolSpec {
        create_spawn_agent_tool_v2(self.options.clone())
    }

    fn handle<'a>(&'a self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(async move {
            let analytics = invocation.session.services.analytics_events_client.clone();
            let sender_thread_id = invocation.session.thread_id;
            let turn_id = invocation.step_context.turn.sub_id.clone();
            let call_id = invocation.call_id.clone();
            let started_at_ms = now_unix_timestamp_ms();
            let result = handle_agent_start(invocation, AgentStartOperation::Spawn).await;
            let completed_at_ms = now_unix_timestamp_ms();
            let (status, receiver_thread_ids, agents_states) = match &result {
                Ok((_, thread_id, agent_status, _)) => (
                    collab_tool_call_status(agent_status, Some(*thread_id)),
                    vec![*thread_id],
                    [(*thread_id, agent_status.clone())].into_iter().collect(),
                ),
                Err(_) => (
                    CollabAgentToolCallStatus::Failed,
                    Vec::new(),
                    Default::default(),
                ),
            };
            let agent_snapshot = result
                .as_ref()
                .ok()
                .and_then(|(_, _, _, snapshot)| snapshot.as_ref());

            analytics.track_collab_tool_call(
                turn_id,
                CollabAgentToolCallItem {
                    id: call_id,
                    tool: CollabAgentTool::SpawnAgent,
                    status,
                    sender_thread_id,
                    receiver_thread_ids,
                    receiver_agents: Vec::new(),
                    prompt: None,
                    model: agent_snapshot.map(|snapshot| snapshot.model.clone()),
                    reasoning_effort: agent_snapshot
                        .and_then(|snapshot| snapshot.reasoning_effort.clone()),
                    agents_states,
                },
                started_at_ms,
                completed_at_ms,
            );

            result.map(|(output, _, _, _)| boxed_tool_output(output))
        })
    }
}

impl ToolExecutor<ToolInvocation> for AdoptHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain("adopt_agent")
    }

    fn spec(&self) -> ToolSpec {
        create_adopt_agent_tool(self.hide_agent_metadata)
    }

    fn handle<'a>(&'a self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(async move {
            handle_agent_start(invocation, AgentStartOperation::Adopt)
                .await
                .map(|(output, _, _, _)| boxed_tool_output(output))
        })
    }
}

/// Selects the contract-specific validation before the shared agent-start operation.
#[derive(Clone, Copy)]
enum AgentStartOperation {
    Spawn,
    Adopt,
}

async fn handle_agent_start(
    invocation: ToolInvocation,
    operation: AgentStartOperation,
) -> Result<
    (
        SpawnAgentResult,
        ThreadId,
        AgentStatus,
        Option<ThreadConfigSnapshot>,
    ),
    FunctionCallError,
> {
    let ToolInvocation {
        session,
        step_context,
        payload,
        call_id,
        source,
        ..
    } = invocation;
    let turn = &step_context.turn;
    let arguments = function_arguments(payload)?;
    let args: SpawnAgentArgs = parse_arguments(&arguments)?;
    if matches!(operation, AgentStartOperation::Adopt)
        && !turn.config.multi_agent_v2.enable_thread_adoption
    {
        return Err(FunctionCallError::RespondToModel(
            "Thread adoption is disabled. Set `[features.multi_agent_v2] enable_thread_adoption = true` in config.toml to enable it."
                .to_string(),
        ));
    }
    if matches!(operation, AgentStartOperation::Spawn) && args.existing_thread_id.is_some() {
        return Err(FunctionCallError::RespondToModel(
            "existing_thread_id is only accepted by frodex.adopt_agent".to_string(),
        ));
    }
    let is_adoption = matches!(operation, AgentStartOperation::Adopt);
    let adopted_thread_id = if is_adoption {
        Some(args.existing_thread_id.ok_or_else(|| {
            FunctionCallError::RespondToModel(
                "frodex.adopt_agent requires existing_thread_id".to_string(),
            )
        })?)
    } else {
        None
    };
    let fork_mode = if is_adoption {
        args.validate_adoption_options()?;
        None
    } else {
        args.fork_mode()?
    };
    let message = message_content(args.message)?;
    let role_name = args
        .agent_type
        .as_deref()
        .map(str::trim)
        .filter(|role| !role.is_empty());

    let session_source = turn.session_source.clone();
    let child_depth = next_thread_spawn_depth(&session_source);
    let mut config = if is_adoption {
        build_agent_resume_config(turn.as_ref())?
    } else {
        build_agent_spawn_config(&session.get_base_instructions().await, turn.as_ref())?
    };
    let is_full_history_fork = matches!(fork_mode, Some(SpawnAgentForkMode::FullHistory));
    if !is_adoption {
        apply_requested_spawn_agent_model_overrides(
            &session,
            turn.as_ref(),
            &mut config,
            args.model.as_deref(),
            args.reasoning_effort.clone(),
        )
        .await?;
        if !is_full_history_fork || role_name.is_some() {
            apply_spawn_agent_role(&session, &mut config, role_name).await?;
            if is_full_history_fork && config.developer_instructions.is_none() {
                config
                    .developer_instructions
                    .clone_from(&turn.developer_instructions);
            }
        }
        apply_spawn_agent_service_tier(&session, &mut config).await?;
        apply_spawn_agent_runtime_overrides(&mut config, turn.as_ref())?;
    }

    // Remember an applied configured default so cold reload reapplies its restrictions.
    let persisted_role_name = role_name.or_else(|| {
        (!is_full_history_fork
            && config
                .agent_roles
                .get(DEFAULT_ROLE_NAME)
                .is_some_and(|role| role.config_file.is_some()))
        .then_some(DEFAULT_ROLE_NAME)
    });
    let spawn_source = thread_spawn_source(
        session.thread_id,
        &turn.session_source,
        child_depth,
        persisted_role_name,
        Some(args.task_name.clone()),
    )?;
    let new_agent_path = spawn_source.get_agent_path().ok_or_else(|| {
        FunctionCallError::RespondToModel(
            "spawned agent is missing a canonical task name".to_string(),
        )
    })?;
    let author = turn
        .session_source
        .get_agent_path()
        .unwrap_or_else(AgentPath::root);
    let communication = communication_from_tool_message(
        author,
        new_agent_path.clone(),
        message,
        &source,
        /*trigger_turn*/ true,
    );
    let context = AgentCommunicationContext::new(AgentCommunicationKind::Spawn, session.thread_id);
    let multi_agent_v2_usage_hints =
        if is_full_history_fork && turn.multi_agent_version == MultiAgentVersion::V2 {
            let child_model_info = match config.model.as_deref() {
                Some(model) if model != turn.model_info().slug => Some(
                    session
                        .services
                        .models_manager
                        .get_model_info(model, &config.to_models_manager_config())
                        .await,
                ),
                _ => None,
            };
            let child_catalog = child_model_info
                .as_ref()
                .unwrap_or(turn.model_info())
                .model_messages
                .as_ref()
                .and_then(|messages| messages.multi_agent.as_ref())
                .and_then(|messages| messages.role.as_ref());
            Some(resolve_usage_hints(&config.multi_agent_v2, child_catalog))
        } else {
            None
        };
    let spawned_agent = if let Some(thread_id) = adopted_thread_id {
        Box::pin(
            session
                .services
                .agent_control
                .adopt_agent_with_communication(
                    config,
                    thread_id,
                    communication,
                    context,
                    spawn_source,
                    crate::TurnStartOptions {
                        parent_turn_id: Some(turn.sub_id.clone()),
                        root_turn_id: turn.turn_metadata_state.root_turn_id(),
                        cyber_access_program: turn.cyber_access_program,
                        ..Default::default()
                    },
                ),
        )
        .await
    } else {
        Box::pin(
            session
                .services
                .agent_control
                .spawn_agent_with_communication(
                    config,
                    communication,
                    context,
                    Some(spawn_source),
                    SpawnAgentOptions {
                        fork_parent_spawn_call_id: fork_mode.as_ref().map(|_| call_id.clone()),
                        fork_mode,
                        parent_thread_id: Some(session.thread_id),
                        parent_turn_id: Some(turn.sub_id.clone()),
                        root_turn_id: turn.turn_metadata_state.root_turn_id(),
                        environments: Some(step_context.environments.to_spawn_selections()),
                        multi_agent_v2_usage_hints,
                        cyber_access_program: turn.cyber_access_program,
                        initial_task_message: None,
                    },
                ),
        )
        .await
    }
    .map_err(collab_spawn_error)?;
    let new_thread_id = spawned_agent.thread_id;
    let agent_status = spawned_agent.status;
    let agent_snapshot = session
        .services
        .agent_control
        .get_agent_config_snapshot(new_thread_id)
        .await;
    let nickname = agent_snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.session_source.get_nickname())
        .or(spawned_agent.metadata.agent_nickname);
    emit_sub_agent_activity(
        &session,
        turn,
        SubAgentActivityItem {
            id: call_id,
            agent_thread_id: new_thread_id,
            agent_path: new_agent_path.clone(),
            kind: SubAgentActivityKind::Started,
        },
    )
    .await;
    let role_tag = role_name.unwrap_or(DEFAULT_ROLE_NAME);
    turn.session_telemetry.counter(
        "codex.multi_agent.spawn",
        /*inc*/ 1,
        &[("role", role_tag), ("version", "v2")],
    );
    let task_name = String::from(new_agent_path);

    let hide_agent_metadata = turn.config.multi_agent_v2.hide_spawn_agent_metadata;
    let output = if hide_agent_metadata {
        SpawnAgentResult::HiddenMetadata { task_name }
    } else {
        SpawnAgentResult::WithNickname {
            task_name,
            nickname,
        }
    };
    Ok((output, new_thread_id, agent_status, agent_snapshot))
}

impl CoreToolRuntime for Handler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

impl CoreToolRuntime for AdoptHandler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SpawnAgentArgs {
    message: String,
    task_name: String,
    existing_thread_id: Option<ThreadId>,
    agent_type: Option<String>,
    model: Option<String>,
    reasoning_effort: Option<ReasoningEffort>,
    fork_turns: Option<String>,
    fork_context: Option<bool>,
}

impl SpawnAgentArgs {
    fn validate_adoption_options(&self) -> Result<(), FunctionCallError> {
        if self.fork_turns.is_some()
            || self.fork_context.is_some()
            || self.agent_type.is_some()
            || self.model.is_some()
            || self.reasoning_effort.is_some()
        {
            return Err(FunctionCallError::RespondToModel(
                "existing_thread_id cannot be combined with fork, agent type, model, or reasoning effort overrides".to_string(),
            ));
        }

        Ok(())
    }

    fn fork_mode(&self) -> Result<Option<SpawnAgentForkMode>, FunctionCallError> {
        if self.fork_context.is_some() {
            return Err(FunctionCallError::RespondToModel(
                "fork_context is not supported in MultiAgentV2; use fork_turns instead".to_string(),
            ));
        }

        let fork_turns = self
            .fork_turns
            .as_deref()
            .map(str::trim)
            .filter(|fork_turns| !fork_turns.is_empty())
            .unwrap_or("all");

        if fork_turns.eq_ignore_ascii_case("none") {
            return Ok(None);
        }
        if fork_turns.eq_ignore_ascii_case("all") {
            return Ok(Some(SpawnAgentForkMode::FullHistory));
        }

        let last_n_turns = fork_turns.parse::<usize>().map_err(|_| {
            FunctionCallError::RespondToModel(
                "fork_turns must be `none`, `all`, or a positive integer string".to_string(),
            )
        })?;
        if last_n_turns == 0 {
            return Err(FunctionCallError::RespondToModel(
                "fork_turns must be `none`, `all`, or a positive integer string".to_string(),
            ));
        }

        Ok(Some(SpawnAgentForkMode::LastNTurns(last_n_turns)))
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub(crate) enum SpawnAgentResult {
    WithNickname {
        task_name: String,
        nickname: Option<String>,
    },
    HiddenMetadata {
        task_name: String,
    },
}

impl ToolOutput for SpawnAgentResult {
    fn log_output(&self) -> String {
        tool_output_json_text(self, "spawn_agent")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "spawn_agent")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "spawn_agent")
    }
}
