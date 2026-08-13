use super::*;
use crate::CodexThread;
use crate::StateDbHandle;
use crate::ThreadManager;
use crate::agent::agent_status_from_event;
use crate::agent::next_thread_spawn_depth;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::config::AgentRoleConfig;
use crate::config::Config;
use crate::config::ConfigBuilder;
use crate::context::ContextualUserFragment;
use crate::context::ManagedDeveloperInstructions;
use crate::context::MultiAgentRoleInstructions;
use crate::context::SubagentNotification;
use crate::init_state_db;
use crate::state::ActiveTurn;
use crate::thread_manager::StartThreadOptions;
use crate::tools::handlers::multi_agents_common::thread_spawn_source;
use assert_matches::assert_matches;
use codex_config::types::McpServerConfig;
use codex_config::types::McpServerTransportConfig;
use codex_extension_api::ExtensionDataInit;
use codex_extension_api::empty_extension_registry;
use codex_features::Feature;
use codex_history::CompactedItem;
use codex_history::RolloutItem;
use codex_history::RolloutLine;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_protocol::AgentPath;
use codex_protocol::ResponseItemId;
use codex_protocol::capabilities::CapabilityRootLocation;
use codex_protocol::capabilities::SelectedCapabilityRoot;
use codex_protocol::config_types::ApprovalsReviewer;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::ServiceTier;
use codex_protocol::config_types::Settings;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::mcp::ClientMcpExtensions;
use codex_protocol::mcp::OPENAI_FORM_EXTENSION_ID;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ContentItemKind;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::InternalChatMessageMetadataPassthrough;
use codex_protocol::models::MessagePhase;
use codex_protocol::models::PermissionProfile;
use codex_protocol::models::ResponseItem;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::EnvironmentConfigState;
use codex_protocol::protocol::ErrorEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadGoal;
use codex_protocol::protocol::ThreadGoalStatus;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::protocol::ThreadSettingsSnapshot;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::protocol::TurnAbortedEvent;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use codex_rollout::RolloutRecorder;
use codex_state::DirectionalThreadSpawnEdgeStatus;
use codex_thread_store::AppendThreadItemsParams;
use codex_thread_store::ArchiveThreadParams;
use codex_thread_store::CreateThreadParams;
use codex_thread_store::InMemoryThreadStore;
use codex_thread_store::LocalThreadStore;
use codex_thread_store::LocalThreadStoreConfig;
use codex_thread_store::PersistContext;
use codex_thread_store::ThreadPersistenceMetadata;
use codex_thread_store::ThreadPersistenceMode;
use codex_thread_store::ThreadStore;
use codex_utils_path_uri::PathUri;
use core_test_support::responses::assert_parent_turn;
use core_test_support::responses::assert_root_turn;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_response_sequence;
use core_test_support::responses::mount_sse_once;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::sse_failed;
use core_test_support::responses::sse_response;
use core_test_support::responses::start_mock_server;
use core_test_support::responses::strip_response_item_ids;
use pretty_assertions::assert_eq;
use serial_test::serial;
use std::ffi::OsStr;
use std::ffi::OsString;
use tempfile::TempDir;
use tokio::time::Duration;
use tokio::time::Instant;
use tokio::time::sleep;
use tokio::time::timeout;
use toml::Value as TomlValue;

async fn test_config_with_cli_overrides(
    mut cli_overrides: Vec<(String, TomlValue)>,
) -> (TempDir, Config) {
    let home = TempDir::new().expect("create temp dir");
    cli_overrides.push((
        "model".to_string(),
        TomlValue::String("gpt-5.5".to_string()),
    ));
    let config = ConfigBuilder::without_managed_config_for_tests()
        .codex_home(home.path().to_path_buf())
        .cli_overrides(cli_overrides)
        .build()
        .await
        .expect("load default test config");
    (home, config)
}

async fn test_config() -> (TempDir, Config) {
    test_config_with_cli_overrides(Vec::new()).await
}

async fn create_active_thread_goal_for_test(
    state_db: &StateDbHandle,
    parent_thread_id: ThreadId,
    parent_session: &std::sync::Arc<crate::session::session::Session>,
    objective: &str,
) -> anyhow::Result<(String, ThreadGoal)> {
    let parent_metadata = codex_state::ThreadMetadataBuilder::new(
        parent_thread_id,
        parent_session
            .get_config()
            .await
            .codex_home
            .join(format!("{parent_thread_id}.jsonl"))
            .to_path_buf(),
        chrono::Utc::now(),
        SessionSource::Exec,
    )
    .build("openai");
    state_db.upsert_thread(&parent_metadata).await?;
    let state_goal = state_db
        .thread_goals()
        .replace_thread_goal(
            parent_thread_id,
            objective,
            codex_state::ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await?;
    let protocol_goal = crate::goal_supervisor::protocol_goal_from_state(state_goal.clone());
    Ok((state_goal.goal_id, protocol_goal))
}

fn text_input(text: &str) -> Vec<UserInput> {
    vec![UserInput::Text {
        text: text.to_string(),
        text_elements: Vec::new(),
    }]
}

fn captured_op_matches(actual: &(ThreadId, Op), expected: &(ThreadId, Op)) -> bool {
    if actual.0 != expected.0 {
        return false;
    }
    match (&actual.1, &expected.1) {
        (
            Op::InterAgentCommunication {
                communication: actual,
                ..
            },
            Op::InterAgentCommunication {
                communication: expected,
                ..
            },
        ) => actual == expected,
        _ => false,
    }
}

fn rollout_response_item(item: ResponseItem) -> RolloutItem {
    RolloutItem::ResponseItem(item.into())
}

fn user_message(text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: "user".to_string(),
        content: vec![ContentItem::InputText {
            text: text.to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

fn assistant_message(text: &str, phase: Option<MessagePhase>) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: "assistant".to_string(),
        content: vec![ContentItem::OutputText {
            text: text.to_string(),
        }],
        phase,
        internal_chat_message_metadata_passthrough: None,
    }
}

fn run_goal_supervisor_test<F, T>(name: &'static str, future: F) -> T
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let test_thread = std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build goal supervisor test runtime")
                .block_on(future)
        })
        .expect("spawn goal supervisor test thread");
    match test_thread.join() {
        Ok(result) => result,
        Err(err) => std::panic::resume_unwind(err),
    }
}

async fn spawned_thread_id_after(
    manager: &ThreadManager,
    before_thread_ids: &[ThreadId],
) -> ThreadId {
    let mut spawned_thread_ids = manager
        .list_thread_ids()
        .await
        .into_iter()
        .filter(|thread_id| !before_thread_ids.contains(thread_id))
        .collect::<Vec<_>>();
    spawned_thread_ids.sort_by_key(ToString::to_string);
    assert_eq!(
        spawned_thread_ids.len(),
        1,
        "spawn should add exactly one child thread"
    );
    spawned_thread_ids
        .pop()
        .expect("spawned thread id should be present")
}

fn goal_supervisor_continuity_from_history(
    history: &crate::context_manager::ContextManager,
) -> serde_json::Value {
    history
        .raw_items()
        .filter_map(|item| match item {
            ResponseItem::Message { content, .. } => {
                content.iter().find_map(|content| match content {
                    ContentItem::InputText { text } => text
                        .strip_prefix("# Goal Supervisor Continuity\n\n")
                        .and_then(|json| serde_json::from_str(json).ok()),
                    _ => None,
                })
            }
            _ => None,
        })
        .next_back()
        .expect("helper history should contain goal supervisor continuity")
}

async fn wait_for_turn_complete(thread: &CodexThread) {
    timeout(Duration::from_secs(5), async {
        loop {
            let event = thread
                .next_event()
                .await
                .expect("event channel should stay open");
            if matches!(event.msg, EventMsg::TurnComplete(_)) {
                break;
            }
        }
    })
    .await
    .expect("turn should complete");
}

fn request_tool_signatures(body: &serde_json::Value) -> std::collections::BTreeSet<String> {
    let mut signatures = std::collections::BTreeSet::new();
    let tools = body["tools"].as_array().expect("tools should be an array");
    for tool in tools {
        let tool_type = tool.get("type").and_then(serde_json::Value::as_str);
        let Some(name) = tool.get("name").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if tool_type == Some("namespace") {
            let child_tools = tool
                .get("tools")
                .and_then(serde_json::Value::as_array)
                .expect("namespace tools should have child tools");
            for child_tool in child_tools {
                let child_name = child_tool
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .expect("child tool should have a name");
                signatures.insert(format!("{name}.{child_name}"));
            }
        } else {
            signatures.insert(name.to_string());
        }
    }
    signatures
}

#[test]
fn register_session_root_skips_threads_with_explicit_parent() {
    let control = AgentControl::default();

    control.register_session_root(ThreadId::new(), Some(ThreadId::new()));

    assert_eq!(control.state.agent_id_for_path(&AgentPath::root()), None);
}

#[test]
fn fork_previous_response_id_env_value_parses_truthy_values() {
    for value in ["1", "true", "TRUE", "yes", "on"] {
        assert!(
            fork_previous_response_id_value_enabled(value),
            "{value} should enable previous response forking"
        );
    }

    for value in ["", "0", "false", "off", "no", "enabled"] {
        assert!(
            !fork_previous_response_id_value_enabled(value),
            "{value} should not enable previous response forking"
        );
    }
}

#[tokio::test]
async fn goal_supervisor_helper_uses_full_history_fork_without_spawn_call_id() {
    let harness = AgentControlHarness::new().await;
    let mut parent_config = harness.config.clone();
    let _ = parent_config.features.enable(Feature::AgentPromptInjection);
    let _ = parent_config.features.enable(Feature::Goals);
    let _ = parent_config.features.enable(Feature::GoalSupervisor);
    let _ = parent_config.features.enable(Feature::MultiAgentV2);
    let parent = harness
        .manager
        .start_thread(StartThreadOptions::new(parent_config))
        .await
        .expect("start parent thread");
    parent
        .thread
        .session
        .inject_no_new_turn(
            vec![user_message("parent seed context")],
            /*current_turn_context*/ None,
        )
        .await;
    parent.thread.ensure_rollout_materialized().await;
    parent
        .thread
        .flush_rollout()
        .await
        .expect("parent rollout should flush");
    let state_db = harness
        .state_db
        .as_ref()
        .expect("goal supervisor test requires state db");
    let (_goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent.thread_id,
        &parent.thread.session,
        "Ship the active user goal.",
    )
    .await
    .expect("active goal should persist");

    let helper_thread_id =
        crate::goal_supervisor::spawn_supervisor_helper_for_test(&parent.thread.session, &goal)
            .await
            .expect("goal supervisor helper should spawn without a parent tool-call id");
    let helper_thread = harness
        .manager
        .get_thread(helper_thread_id)
        .await
        .expect("supervisor helper should be registered");
    assert_eq!(
        helper_thread.session.prompt_cache_key(),
        parent.thread.session.prompt_cache_key(),
        "supervisor helper should retain the parent prompt-cache identity"
    );

    let helper_history = helper_thread.session.clone_history().await;
    assert!(
        history_contains_text(helper_history.raw_items(), "parent seed context"),
        "supervisor helper should inherit the parent conversation prefix"
    );
    let supervisor_prompt =
        crate::session::load_supervisor_agent_prompt(&harness.config.codex_home).await;
    assert_eq!(
        helper_history
            .raw_items()
            .filter(|item| {
                matches!(
                    item,
                    ResponseItem::Message { role, content, .. }
                        if role == "developer"
                            && content.iter().any(|content_item| matches!(
                                content_item,
                                ContentItem::InputText { text } if text == &supervisor_prompt
                            ))
                )
            })
            .count(),
        1,
        "supervisor role prompt should be installed exactly once"
    );
    assert!(
        history_contains_text(helper_history.raw_items(), "# Goal Supervisor Continuity"),
        "supervisor helper should receive persisted goal continuity"
    );
    assert!(helper_history.raw_items().any(|item| matches!(
        item,
        ResponseItem::FunctionCall { name, call_id, .. }
            if name == "list_agents" && call_id == SUPERVISOR_BOOT_LIST_AGENTS_CALL_ID
    )));

    let captured_assignment = harness
        .manager
        .captured_ops()
        .into_iter()
        .find_map(|(thread_id, op)| {
            (thread_id == helper_thread_id)
                .then_some(op)
                .and_then(|op| match op {
                    Op::TurnInput { request, .. } => match request.input {
                        codex_protocol::turn_input::TurnInput::UserInput { content, .. } => {
                            content.into_iter().find_map(|item| match item {
                                UserInput::Text { text, .. } => Some(text),
                                _ => None,
                            })
                        }
                        codex_protocol::turn_input::TurnInput::ResponseItem(_)
                        | codex_protocol::turn_input::TurnInput::InterAgentCommunication(_) => None,
                    },
                    _ => None,
                })
        })
        .expect("supervisor assignment should be submitted as user input");
    assert!(captured_assignment.contains("# Goal Supervisor Assignment"));
    assert!(captured_assignment.contains("Ship the active user goal."));
    assert!(!captured_assignment.contains("You are also a **goal supervisor**"));
}

fn spawn_agent_call(call_id: &str) -> ResponseItem {
    ResponseItem::FunctionCall {
        id: None,
        name: "spawn_agent".to_string(),
        namespace: None,
        arguments: "{}".to_string(),
        call_id: call_id.to_string(),
        encrypted_function_args: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

struct AgentControlHarness {
    _home: TempDir,
    config: Config,
    state_db: Option<StateDbHandle>,
    manager: ThreadManager,
    control: AgentControl,
}

impl AgentControlHarness {
    async fn new() -> Self {
        let (home, config) = test_config().await;
        Self::new_with_config(home, config).await
    }

    async fn new_with_multi_agent_v1() -> Self {
        let (home, mut config) = test_config().await;
        let _ = config.features.disable(Feature::MultiAgentV2);
        Self::new_with_config(home, config).await
    }

    async fn new_with_config(home: TempDir, config: Config) -> Self {
        let state_db = init_state_db(&config).await;
        let manager = ThreadManager::with_models_provider_home_and_state_for_tests(
            CodexAuth::from_api_key("dummy"),
            config.model_provider.clone(),
            config.codex_home.to_path_buf(),
            std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
            state_db.clone(),
        );
        let control = manager.agent_control();
        Self {
            _home: home,
            config,
            state_db,
            manager,
            control,
        }
    }

    async fn new_without_state_db() -> Self {
        let (home, config) = test_config().await;
        let manager = ThreadManager::with_models_provider_home_and_state_for_tests(
            CodexAuth::from_api_key("dummy"),
            config.model_provider.clone(),
            config.codex_home.to_path_buf(),
            std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
            /*state_db*/ None,
        );
        let control = manager.agent_control();
        Self {
            _home: home,
            config,
            state_db: None,
            manager,
            control,
        }
    }

    async fn start_thread(&self) -> (ThreadId, Arc<CodexThread>) {
        let new_thread = self
            .manager
            .start_thread(StartThreadOptions::new(self.config.clone()))
            .await
            .expect("start thread");
        (new_thread.thread_id, new_thread.thread)
    }

    async fn start_paginated_thread(&self) -> (ThreadId, Arc<CodexThread>) {
        let new_thread = self
            .manager
            .start_thread(StartThreadOptions {
                history_mode: Some(ThreadHistoryMode::Paginated),
                environments: Some(Vec::new()),
                ..StartThreadOptions::new(self.config.clone())
            })
            .await
            .expect("start paginated thread");
        (new_thread.thread_id, new_thread.thread)
    }

    async fn spawn_anonymous_child(
        &self,
        parent_thread_id: ThreadId,
        options: SpawnAgentOptions,
    ) -> ThreadId {
        self.control
            .spawn_agent_with_metadata(
                self.config.clone(),
                text_input("child task"),
                Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    parent_thread_id,
                    depth: 1,
                    agent_path: None,
                    agent_nickname: None,
                    agent_role: None,
                })),
                options,
            )
            .await
            .expect("child spawn should succeed")
            .thread_id
    }
}

struct EnvVarGuard {
    key: &'static str,
    original: Option<OsString>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: &OsStr) -> Self {
        let original = std::env::var_os(key);
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, original }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        unsafe {
            match &self.original {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

fn assert_goal_supervisor_boot_history<'a>(
    history: impl Clone + IntoIterator<Item = &'a ResponseItem>,
    supervisor_prompt: &str,
    generic_subagent_hint: &str,
) {
    assert!(
        history_contains_text(history.clone(), "parent seed context"),
        "goal supervisor helper should inherit the parent conversation prefix"
    );
    let supervisor_prompt_count = history
        .clone()
        .into_iter()
        .filter(|item| {
            matches!(
                item,
                ResponseItem::Message { role, content, .. }
                    if role == "developer"
                        && content.iter().any(|content_item| matches!(
                            content_item,
                            ContentItem::InputText { text } if text == supervisor_prompt
                        ))
            )
        })
        .count();
    assert_eq!(
        supervisor_prompt_count, 1,
        "goal supervisor role prompt should be injected exactly once"
    );
    assert!(
        history_contains_text(history.clone(), "# Goal Supervisor Continuity"),
        "goal supervisor helper should receive durable continuity context"
    );
    assert!(
        history.clone().into_iter().any(|item| matches!(
            item,
            ResponseItem::FunctionCall { name, call_id, .. }
                if name == "list_agents" && call_id == "synthetic_supervisor_list_agents"
        )),
        "goal supervisor helper should receive a synthetic list_agents call"
    );
    assert!(
        history.clone().into_iter().any(|item| matches!(
            item,
            ResponseItem::FunctionCallOutput { call_id, .. }
                if call_id.as_deref() == Some("synthetic_supervisor_list_agents")
        )),
        "goal supervisor helper should receive the synthetic list_agents output"
    );
    assert!(
        !history_contains_text(history, generic_subagent_hint),
        "goal supervisor helper should not receive generic subagent usage guidance"
    );
}

#[test]
#[serial(fork_env)]
fn goal_supervisor_full_history_bootstrap_survives_cold_resume() {
    run_goal_supervisor_test(
        "goal_supervisor_full_history_bootstrap_survives_cold_resume",
        goal_supervisor_full_history_bootstrap_survives_cold_resume_inner(),
    );
}

async fn goal_supervisor_full_history_bootstrap_survives_cold_resume_inner() {
    const MATERIALIZE_EPHEMERAL_ROLLOUTS: &str = "CODEX_MATERIALIZE_EPHEMERAL_ROLLOUTS";
    const GENERIC_SUBAGENT_HINT: &str = "generic subagent guidance must not reach the supervisor";
    let _materialize_ephemeral_rollouts =
        EnvVarGuard::set(MATERIALIZE_EPHEMERAL_ROLLOUTS, OsStr::new("1"));

    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::AgentPromptInjection);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    config.multi_agent_v2.subagent_usage_hint_text = Some(GENERIC_SUBAGENT_HINT.to_string());
    let harness = AgentControlHarness::new_with_config(home, config.clone()).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread
        .session
        .inject_no_new_turn(
            vec![user_message("parent seed context")],
            /*current_turn_context*/ None,
        )
        .await;
    parent_thread.ensure_rollout_materialized().await;
    parent_thread
        .flush_rollout()
        .await
        .expect("parent rollout should flush");

    let state_db = harness
        .state_db
        .as_ref()
        .expect("goal supervisor test requires state db");
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Ship the active user goal.",
    )
    .await
    .expect("active goal should persist");
    let before_thread_ids = harness.manager.list_thread_ids().await;

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await
    .expect("active goal should spawn a goal supervisor helper");

    let helper_thread_id = spawned_thread_id_after(&harness.manager, &before_thread_ids).await;
    let helper_thread = harness
        .manager
        .get_thread(helper_thread_id)
        .await
        .expect("goal supervisor helper should be registered");
    let helper_snapshot = helper_thread.config_snapshot().await;
    let helper_source = helper_snapshot.session_source.clone();
    let expected_path = AgentPath::root()
        .join(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME)
        .expect("canonical goal supervisor path");
    assert!(matches!(
        &helper_source,
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: source_parent_thread_id,
            agent_path: Some(agent_path),
            agent_role: Some(agent_role),
            ..
        }) if *source_parent_thread_id == parent_thread_id
            && agent_path == &expected_path
            && agent_role == crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME
    ));
    assert_eq!(
        helper_thread.session.prompt_cache_key(),
        parent_thread.session.prompt_cache_key(),
        "internal goal supervisor helper should reuse the parent prompt cache key"
    );

    let supervisor_prompt =
        crate::session::load_supervisor_agent_prompt(&harness.config.codex_home).await;
    let helper_history = helper_thread.session.clone_history().await;
    assert_goal_supervisor_boot_history(
        helper_history.raw_items(),
        &supervisor_prompt,
        GENERIC_SUBAGENT_HINT,
    );
    let captured_assignment = harness
        .manager
        .captured_ops()
        .into_iter()
        .find_map(|(thread_id, op)| {
            (thread_id == helper_thread_id)
                .then_some(op)
                .and_then(|op| match op {
                    Op::TurnInput { request, .. } => match request.input {
                        codex_protocol::turn_input::TurnInput::UserInput { content, .. } => {
                            content.into_iter().find_map(|item| match item {
                                UserInput::Text { text, .. } => Some(text),
                                _ => None,
                            })
                        }
                        codex_protocol::turn_input::TurnInput::ResponseItem(_)
                        | codex_protocol::turn_input::TurnInput::InterAgentCommunication(_) => None,
                    },
                    _ => None,
                })
        })
        .expect("goal supervisor assignment should be submitted as user input");
    assert!(captured_assignment.contains("# Goal Supervisor Assignment"));
    assert!(captured_assignment.contains("Ship the active user goal."));
    assert!(
        !captured_assignment.contains(&supervisor_prompt),
        "role prompt should not be duplicated in the user assignment"
    );

    helper_thread
        .flush_rollout()
        .await
        .expect("materialized goal supervisor rollout should flush");
    assert!(
        helper_thread.rollout_path().is_some(),
        "debug materialization should give the ephemeral helper a durable rollout"
    );
    harness
        .control
        .shutdown_live_agent(helper_thread_id)
        .await
        .expect("goal supervisor helper should shut down before cold resume");

    let mut helper_config = config;
    helper_config.ephemeral = true;
    let resumed_helper_id = harness
        .control
        .resume_agent_from_rollout(helper_config, helper_thread_id, helper_source)
        .await
        .expect("materialized goal supervisor helper should cold resume");
    assert_eq!(resumed_helper_id, helper_thread_id);
    let resumed_helper = harness
        .manager
        .get_thread(resumed_helper_id)
        .await
        .expect("cold-resumed goal supervisor helper should be registered");
    let resumed_history = resumed_helper.session.clone_history().await;
    assert_goal_supervisor_boot_history(
        resumed_history.raw_items(),
        &supervisor_prompt,
        GENERIC_SUBAGENT_HINT,
    );
}

async fn persisted_originator(thread: &CodexThread) -> String {
    thread.ensure_rollout_materialized().await;
    thread
        .flush_rollout()
        .await
        .expect("thread rollout should flush");
    let stored_thread = thread
        .read_thread(
            /*include_archived*/ true, /*include_history*/ true,
        )
        .await
        .expect("thread should be readable");
    let history = stored_thread.history.expect("history should be loaded");
    history
        .items
        .iter()
        .find_map(|item| match item {
            RolloutItem::SessionMeta(meta_line) => Some(meta_line.meta.originator.clone()),
            RolloutItem::ResponseItem(_)
            | RolloutItem::RolloutReference(_)
            | RolloutItem::InterAgentCommunication(_)
            | RolloutItem::InterAgentCommunicationMetadata { .. }
            | RolloutItem::EventMsg(_)
            | RolloutItem::Compacted(_)
            | RolloutItem::WorldState(_)
            | RolloutItem::RealtimeItem(_)
            | RolloutItem::SecurityRiskScore(_)
            | RolloutItem::TurnContext(_) => None,
        })
        .expect("session metadata should be persisted")
}

fn has_subagent_notification<'a>(
    history_items: impl IntoIterator<Item = &'a ResponseItem>,
) -> bool {
    history_items.into_iter().any(|item| {
        let ResponseItem::Message { role, content, .. } = item else {
            return false;
        };
        if role != "user" {
            return false;
        }
        content.iter().any(|content_item| match content_item {
            ContentItem::InputText { text } | ContentItem::OutputText { text } => {
                SubagentNotification::matches_text(text)
            }
            ContentItem::InputImage { .. } | ContentItem::InputAudio { .. } => false,
        })
    })
}

/// Returns true when any message item contains `needle` in a text span.
fn history_contains_text<'a>(
    history_items: impl IntoIterator<Item = &'a ResponseItem>,
    needle: &str,
) -> bool {
    history_items.into_iter().any(|item| {
        let ResponseItem::Message { content, .. } = item else {
            return false;
        };
        content.iter().any(|content_item| match content_item {
            ContentItem::InputText { text } | ContentItem::OutputText { text } => {
                text.contains(needle)
            }
            ContentItem::InputImage { .. } | ContentItem::InputAudio { .. } => false,
        })
    })
}

async fn wait_for_recorded_user_message(thread: &CodexThread, needle: &str) {
    timeout(Duration::from_secs(5), async {
        loop {
            let event = thread
                .next_event()
                .await
                .expect("event stream should stay open");
            if let EventMsg::ItemCompleted(ItemCompletedEvent {
                item: TurnItem::UserMessage(item),
                ..
            }) = event.msg
                && item.content.iter().any(
                    |input| matches!(input, UserInput::Text { text, .. } if text.contains(needle)),
                )
            {
                return;
            }
        }
    })
    .await
    .expect("timed out waiting for user message recording");
}

fn history_text_match_count<'a>(
    history_items: impl IntoIterator<Item = &'a ResponseItem>,
    needle: &str,
) -> usize {
    history_items
        .into_iter()
        .filter(|item| {
            let ResponseItem::Message { content, .. } = item else {
                return false;
            };
            content.iter().any(|content_item| match content_item {
                ContentItem::InputText { text } | ContentItem::OutputText { text } => {
                    text.contains(needle)
                }
                ContentItem::InputImage { .. } | ContentItem::InputAudio { .. } => false,
            })
        })
        .count()
}

fn history_contains_assistant_inter_agent_communication<'a>(
    history_items: impl IntoIterator<Item = &'a ResponseItem>,
    expected: &InterAgentCommunication,
) -> bool {
    history_items.into_iter().any(|item| {
        let ResponseItem::Message { role, content, .. } = item else {
            return false;
        };
        if role != "assistant" {
            return false;
        }
        content.iter().any(|content_item| match content_item {
            ContentItem::OutputText { text } => {
                serde_json::from_str::<InterAgentCommunication>(text)
                    .ok()
                    .as_ref()
                    == Some(expected)
            }
            ContentItem::InputText { .. }
            | ContentItem::InputImage { .. }
            | ContentItem::InputAudio { .. } => false,
        })
    })
}

async fn wait_for_subagent_notification(parent_thread: &Arc<CodexThread>) -> bool {
    let wait = async {
        loop {
            let history = parent_thread.session.clone_history().await;
            if has_subagent_notification(history.raw_items()) {
                return true;
            }
            sleep(Duration::from_millis(25)).await;
        }
    };
    // CI can take several seconds to schedule the detached completion watcher,
    // especially on slower Windows runners.
    timeout(Duration::from_secs(10), wait).await.is_ok()
}

async fn wait_for_agent_status(
    control: &AgentControl,
    thread_id: ThreadId,
    predicate: impl Fn(&AgentStatus) -> bool,
) {
    timeout(Duration::from_secs(10), async {
        loop {
            let status = control.get_status(thread_id).await;
            if predicate(&status) {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("agent should reach the expected status");
}

async fn make_cold_multi_agent_v1_child(
    harness: &AgentControlHarness,
    parent_thread_id: ThreadId,
) -> ThreadId {
    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            harness.config.clone(),
            text_input("initial v1 child task"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
            SpawnAgentOptions {
                parent_thread_id: Some(parent_thread_id),
                ..Default::default()
            },
        )
        .await
        .expect("v1 child spawn should succeed")
        .thread_id;
    wait_for_agent_status(&harness.control, child_thread_id, |status| {
        matches!(status, AgentStatus::Completed(_))
    })
    .await;
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("v1 child should be loaded before becoming cold");
    child_thread.ensure_rollout_materialized().await;
    child_thread
        .flush_rollout()
        .await
        .expect("v1 child rollout should flush");
    let lifecycle = harness
        .control
        .get_agent_metadata(child_thread_id)
        .expect("v1 child metadata should remain registered")
        .lifecycle;
    lifecycle.wait_for_completion_watcher().await;
    child_thread
        .shutdown_and_wait()
        .await
        .expect("v1 child should shut down before cold reload");
    assert!(
        harness
            .manager
            .remove_thread(&child_thread_id)
            .await
            .is_some(),
        "v1 child should be removed from the live manager"
    );
    drop(child_thread);
    assert!(matches!(
        harness.manager.get_thread(child_thread_id).await,
        Err(err) if matches!(err.details(), CodexErrorDetails::ThreadNotFound(id) if *id == child_thread_id)
    ));
    child_thread_id
}

async fn persist_thread_for_tree_resume(thread: &Arc<CodexThread>, message: &str) {
    // These tests only need a durable resume fixture. Stop the child prompt
    // first so this marker records directly instead of waiting behind an
    // unrelated active turn.
    thread
        .session
        .abort_all_tasks(TurnAbortReason::Interrupted)
        .await;
    thread
        .inject_response_items(vec![user_message(message)])
        .await
        .expect("inject thread resume context");
    thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    thread
        .session
        .flush_rollout()
        .await
        .expect("test thread rollout should flush");
}

async fn wait_for_live_thread_spawn_children(
    control: &AgentControl,
    parent_thread_id: ThreadId,
    expected_children: &[ThreadId],
) {
    let mut expected_children = expected_children.to_vec();
    expected_children.sort_by_key(std::string::ToString::to_string);

    timeout(Duration::from_secs(5), async {
        loop {
            let mut child_ids = control
                .open_thread_spawn_children(parent_thread_id)
                .await
                .expect("live child list should load")
                .into_iter()
                .map(|(thread_id, _)| thread_id)
                .collect::<Vec<_>>();
            child_ids.sort_by_key(std::string::ToString::to_string);
            if child_ids == expected_children {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("expected persisted child tree");
}

async fn assert_thread_not_loaded(manager: &ThreadManager, thread_id: ThreadId) {
    match manager.get_thread(thread_id).await {
        Err(err) => match err.details() {
            CodexErrorDetails::ThreadNotFound(id) => assert_eq!(*id, thread_id),
            _ => panic!("expected ThreadNotFound, got {err:?}"),
        },
        Ok(_) => panic!("expected thread not to be loaded"),
    }
}

#[tokio::test]
async fn restore_v2_agent_metadata_uses_indexed_identity_without_reading_rollout() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _) = harness.start_thread().await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("metadata restoration requires state db");
    let indexed_thread_id = ThreadId::new();
    let indexed_path = AgentPath::root()
        .join("indexed_worker")
        .expect("indexed agent path");
    let source_path = AgentPath::root()
        .join("source_worker")
        .expect("source agent path");
    let malformed_rollout = harness.config.codex_home.join("malformed-rollout.jsonl");
    tokio::fs::write(&malformed_rollout, "not a rollout record\n")
        .await
        .expect("malformed rollout should exist");
    let source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: Some(source_path),
        agent_nickname: Some("source-name".to_string()),
        agent_role: Some("source-role".to_string()),
    });
    let mut builder = codex_state::ThreadMetadataBuilder::new(
        indexed_thread_id,
        malformed_rollout.to_path_buf(),
        chrono::Utc::now(),
        source,
    );
    builder.agent_path = Some(indexed_path.to_string());
    builder.agent_nickname = Some("indexed-name".to_string());
    builder.agent_role = Some("indexed-role".to_string());
    state_db
        .upsert_thread(&builder.build("openai"))
        .await
        .expect("indexed metadata should persist");
    state_db
        .upsert_thread_spawn_edge(
            parent_thread_id,
            indexed_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("indexed spawn edge should persist");

    let anonymous_thread_id = ThreadId::new();
    let anonymous_rollout = harness.config.codex_home.join("anonymous-malformed.jsonl");
    tokio::fs::write(&anonymous_rollout, "not a rollout record\n")
        .await
        .expect("anonymous malformed rollout should exist");
    let anonymous_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: None,
        agent_nickname: Some("anonymous-name".to_string()),
        agent_role: None,
    });
    let anonymous_metadata = codex_state::ThreadMetadataBuilder::new(
        anonymous_thread_id,
        anonymous_rollout.to_path_buf(),
        chrono::Utc::now(),
        anonymous_source,
    )
    .build("openai");
    state_db
        .upsert_thread(&anonymous_metadata)
        .await
        .expect("anonymous metadata should persist");
    state_db
        .upsert_thread_spawn_edge(
            parent_thread_id,
            anonymous_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("anonymous spawn edge should persist");

    harness
        .control
        .restore_v2_agent_metadata(&harness.config, parent_thread_id)
        .await;

    let indexed_metadata = harness
        .control
        .state
        .agent_metadata_for_thread(indexed_thread_id)
        .expect("indexed agent should be restored without reading its malformed rollout");
    assert_eq!(indexed_metadata.agent_path, Some(indexed_path));
    assert_eq!(indexed_metadata.agent_role.as_deref(), Some("indexed-role"));
    assert_eq!(
        indexed_metadata.agent_nickname.as_deref(),
        Some("indexed-name")
    );

    let anonymous_metadata = harness
        .control
        .state
        .agent_metadata_for_thread(anonymous_thread_id)
        .expect("missing optional path and role should not require reading the rollout");
    assert_eq!(anonymous_metadata.agent_path, None);
    assert_eq!(anonymous_metadata.agent_role, None);
    assert_eq!(
        anonymous_metadata.agent_nickname.as_deref(),
        Some("anonymous-name")
    );
    assert_thread_not_loaded(&harness.manager, indexed_thread_id).await;
    assert_thread_not_loaded(&harness.manager, anonymous_thread_id).await;
}

#[tokio::test]
async fn send_input_errors_when_manager_dropped() {
    let control = AgentControl::default();
    let err = control
        .send_input(
            ThreadId::new(),
            vec![UserInput::Text {
                text: "hello".to_string(),
                text_elements: Vec::new(),
            }],
            Default::default(),
        )
        .await
        .expect_err("send_input should fail without a manager");
    assert_eq!(
        err.to_string(),
        "unsupported operation: thread manager dropped"
    );
}

#[tokio::test]
async fn get_status_returns_not_found_without_manager() {
    let control = AgentControl::default();
    let got = control.get_status(ThreadId::new()).await;
    assert_eq!(got, AgentStatus::NotFound);
}

#[tokio::test]
async fn on_event_updates_status_from_task_started() {
    let status = agent_status_from_event(&EventMsg::TurnStarted(TurnStartedEvent {
        turn_id: "turn-1".to_string(),
        trace_id: None,
        started_at: None,
        model_context_window: None,
        collaboration_mode_kind: ModeKind::Default,
    }));
    assert_eq!(status, Some(AgentStatus::Running));
}

#[tokio::test]
async fn on_event_updates_status_from_task_complete() {
    for (error, expected) in [
        (None, AgentStatus::Completed(Some("done".to_string()))),
        (
            Some(ErrorEvent {
                misalignment: None,
                message: "denied".to_string(),
                codex_error_info: None,
            }),
            AgentStatus::Errored("denied".to_string()),
        ),
    ] {
        let status = agent_status_from_event(&EventMsg::TurnComplete(TurnCompleteEvent {
            turn_id: "turn-1".to_string(),
            started_at: None,
            last_agent_message: Some("done".to_string()),
            error,
            completed_at: None,
            duration_ms: None,
            time_to_first_token_ms: None,
        }));
        assert_eq!(status, Some(expected));
    }
}

#[tokio::test]
async fn on_event_updates_status_from_failed_task_complete() {
    let status = agent_status_from_event(&EventMsg::TurnComplete(TurnCompleteEvent {
        turn_id: "turn-1".to_string(),
        started_at: None,
        last_agent_message: None,
        error: Some(ErrorEvent {
            message: "boom".to_string(),
            codex_error_info: None,
            misalignment: None,
        }),
        completed_at: None,
        duration_ms: None,
        time_to_first_token_ms: None,
    }));
    assert_eq!(status, Some(AgentStatus::Errored("boom".to_string())));
}

#[tokio::test]
async fn on_event_updates_status_from_error() {
    let status = agent_status_from_event(&EventMsg::Error(ErrorEvent {
        misalignment: None,
        message: "boom".to_string(),
        codex_error_info: None,
    }));

    let expected = AgentStatus::Errored("boom".to_string());
    assert_eq!(status, Some(expected));
}

#[tokio::test]
async fn on_event_updates_status_from_turn_aborted() {
    let status = agent_status_from_event(&EventMsg::TurnAborted(TurnAbortedEvent {
        turn_id: Some("turn-1".to_string()),
        started_at: None,
        reason: TurnAbortReason::Interrupted,
        completed_at: None,
        duration_ms: None,
    }));

    let expected = AgentStatus::Interrupted;
    assert_eq!(status, Some(expected));
}

#[tokio::test]
async fn on_event_updates_status_from_shutdown_complete() {
    let status = agent_status_from_event(&EventMsg::ShutdownComplete);
    assert_eq!(status, Some(AgentStatus::Shutdown));
}

#[tokio::test]
async fn spawn_agent_errors_when_manager_dropped() {
    let control = AgentControl::default();
    let (_home, config) = test_config().await;
    let err = control
        .spawn_agent(config, text_input("hello"), /*session_source*/ None)
        .await
        .expect_err("spawn_agent should fail without a manager");
    assert_eq!(
        err.to_string(),
        "unsupported operation: thread manager dropped"
    );
}

#[tokio::test]
async fn resume_agent_errors_when_manager_dropped() {
    let control = AgentControl::default();
    let (_home, config) = test_config().await;
    let err = control
        .resume_agent_from_rollout(config, ThreadId::new(), SessionSource::Exec)
        .await
        .expect_err("resume_agent should fail without a manager");
    assert_eq!(
        err.to_string(),
        "unsupported operation: thread manager dropped"
    );
}

#[tokio::test]
async fn send_input_errors_when_thread_missing() {
    let harness = AgentControlHarness::new().await;
    let thread_id = ThreadId::new();
    let err = harness
        .control
        .send_input(
            thread_id,
            vec![UserInput::Text {
                text: "hello".to_string(),
                text_elements: Vec::new(),
            }],
            Default::default(),
        )
        .await
        .expect_err("send_input should fail for missing thread");
    assert_matches!(
        err.details(),
        CodexErrorDetails::ThreadNotFound(id) if *id == thread_id
    );
}

#[tokio::test]
async fn get_status_returns_not_found_for_missing_thread() {
    let harness = AgentControlHarness::new().await;
    let status = harness.control.get_status(ThreadId::new()).await;
    assert_eq!(status, AgentStatus::NotFound);
}

#[tokio::test]
async fn get_status_returns_pending_init_for_new_thread() {
    let harness = AgentControlHarness::new().await;
    let (thread_id, _) = harness.start_thread().await;
    let status = harness.control.get_status(thread_id).await;
    assert_eq!(status, AgentStatus::PendingInit);
}

#[tokio::test]
async fn subscribe_status_errors_for_missing_thread() {
    let harness = AgentControlHarness::new().await;
    let thread_id = ThreadId::new();
    let err = harness
        .control
        .subscribe_status(thread_id)
        .await
        .expect_err("subscribe_status should fail for missing thread");
    assert_matches!(
        err.details(),
        CodexErrorDetails::ThreadNotFound(id) if *id == thread_id
    );
}

#[tokio::test]
async fn subscribe_status_updates_on_shutdown() {
    let harness = AgentControlHarness::new().await;
    let (thread_id, thread) = harness.start_thread().await;
    let mut status_rx = harness
        .control
        .subscribe_status(thread_id)
        .await
        .expect("subscribe_status should succeed");
    assert_eq!(status_rx.borrow().clone(), AgentStatus::PendingInit);

    let _ = thread
        .submit(Op::Shutdown {})
        .await
        .expect("shutdown should submit");

    let _ = status_rx.changed().await;
    assert_eq!(status_rx.borrow().clone(), AgentStatus::Shutdown);
}

#[tokio::test]
async fn send_input_submits_user_message() {
    let harness = AgentControlHarness::new().await;
    let (thread_id, thread) = harness.start_thread().await;

    let submission_id = harness
        .control
        .send_input(
            thread_id,
            vec![UserInput::Text {
                text: "hello from tests".to_string(),
                text_elements: Vec::new(),
            }],
            Default::default(),
        )
        .await
        .expect("send_input should succeed");
    assert!(!submission_id.is_empty());
    wait_for_recorded_user_message(thread.as_ref(), "hello from tests").await;
}

#[tokio::test]
async fn send_inter_agent_communication_without_turn_queues_message_without_triggering_turn() {
    let harness = AgentControlHarness::new().await;
    let (thread_id, thread) = harness.start_thread().await;
    let communication = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::try_from("/root/worker").expect("agent path"),
        Vec::new(),
        "hello from tests".to_string(),
        /*trigger_turn*/ false,
    );

    let submission_id = harness
        .control
        .send_inter_agent_communication(
            thread_id,
            communication.clone(),
            AgentCommunicationContext::new(AgentCommunicationKind::Message, ThreadId::new()),
            Default::default(),
        )
        .await
        .expect("send_inter_agent_communication should succeed");
    assert!(!submission_id.is_empty());

    let expected = (
        thread_id,
        Op::InterAgentCommunication {
            communication: communication.clone(),
            start_options: Default::default(),
        },
    );
    let captured = harness
        .manager
        .captured_ops()
        .into_iter()
        .find(|entry| captured_op_matches(entry, &expected));
    assert!(captured.is_some());

    timeout(Duration::from_secs(5), async {
        loop {
            if thread
                .session
                .input_queue
                .has_pending_input(&thread.session.active_turn)
                .await
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("inter-agent communication should stay pending");

    let history = thread.session.clone_history().await;
    assert!(!history_contains_assistant_inter_agent_communication(
        history.raw_items(),
        &communication
    ));
}

#[tokio::test]
async fn ensure_v2_agent_loaded_reloads_registered_unloaded_agent() {
    check_v2_agent_reload(V2ReloadRoute::Sender).await;
}

#[tokio::test]
async fn ensure_v2_child_loaded_preserves_evicted_parent_authority() {
    check_v2_agent_reload(V2ReloadRoute::NestedParent).await;
}

#[derive(Clone, Copy)]
enum V2ReloadRoute {
    Sender,
    NestedParent,
}

async fn spawn_v2_reload_test_child(
    control: &AgentControl,
    config: Config,
    parent: &CodexThread,
    task_name: &str,
) -> LiveAgent {
    let source = thread_spawn_source(
        parent.session.thread_id,
        &parent.session_source,
        next_thread_spawn_depth(&parent.session_source),
        /*agent_role*/ None,
        Some(task_name.to_string()),
    )
    .expect("child source");
    control
        .spawn_agent_with_metadata(
            config,
            text_input("hello child"),
            Some(source),
            SpawnAgentOptions {
                parent_thread_id: Some(parent.session.thread_id),
                ..Default::default()
            },
        )
        .await
        .expect("spawn_agent should succeed")
}

async fn check_v2_agent_reload(route: V2ReloadRoute) {
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Sqlite);
    config.model = Some("gpt-5.6-sol".to_string());
    config.multi_agent_v2.max_concurrent_threads_per_session = 3;
    config.permissions.allow_login_shell = true;
    config
        .permissions
        .set_permission_profile(PermissionProfile::read_only())
        .expect("read-only parent profile");
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let client_mcp_extensions =
        ClientMcpExtensions::new([(OPENAI_FORM_EXTENSION_ID.to_string(), serde_json::json!({}))]);
    let root = harness
        .manager
        .start_thread(StartThreadOptions {
            history_mode: Some(ThreadHistoryMode::Paginated),
            client_mcp_extensions: client_mcp_extensions.clone(),
            ..StartThreadOptions::new(harness.config.clone())
        })
        .await
        .expect("start root thread");
    let control = root.thread.session.services.agent_control.clone();
    let parent_thread = match route {
        V2ReloadRoute::Sender => root.thread,
        V2ReloadRoute::NestedParent => {
            let parent = spawn_v2_reload_test_child(
                &control,
                harness.config.clone(),
                &root.thread,
                "parent",
            )
            .await;
            harness
                .manager
                .get_thread(parent.thread_id)
                .await
                .expect("nested parent should exist")
        }
    };
    let parent_thread_id = parent_thread.session.thread_id;
    let mut child_config = harness.config.clone();
    child_config.model = Some("gpt-5.6-luna".to_string());
    let spawned_agent =
        spawn_v2_reload_test_child(&control, child_config, &parent_thread, "worker").await;
    let agent_path = spawned_agent
        .metadata
        .agent_path
        .clone()
        .expect("agent path");
    let child_thread = harness
        .manager
        .get_thread(spawned_agent.thread_id)
        .await
        .expect("child thread should exist");
    child_thread
        .inject_response_items(vec![assistant_message(
            "child persisted",
            Some(MessagePhase::FinalAnswer),
        )])
        .await
        .expect("child rollout should persist with v2 metadata");
    child_thread
        .shutdown_and_wait()
        .await
        .expect("child thread should shut down");
    let stored_child = child_thread
        .read_thread(
            /*include_archived*/ true, /*include_history*/ false,
        )
        .await
        .expect("child metadata should be readable");
    assert_eq!(stored_child.history_mode, ThreadHistoryMode::Paginated);

    assert!(
        harness
            .manager
            .remove_thread(&spawned_agent.thread_id)
            .await
            .is_some()
    );
    match harness.manager.get_thread(spawned_agent.thread_id).await {
        Err(err) => match err.details() {
            CodexErrorDetails::ThreadNotFound(id) => assert_eq!(*id, spawned_agent.thread_id),
            _ => panic!("expected ThreadNotFound, got {err:?}"),
        },
        Ok(_) => panic!("expected thread to be removed"),
    }

    let mut sender_config = harness.config.clone();
    sender_config.model_provider_id = "ollama".to_string();
    sender_config.model_provider = sender_config
        .model_providers
        .get("ollama")
        .cloned()
        .expect("ollama provider should be configured");

    let mut parent_turn = parent_thread.session.new_default_turn().await;
    match route {
        V2ReloadRoute::Sender => control
            .ensure_v2_agent_loaded(sender_config, spawned_agent.thread_id, /*parent*/ None)
            .await
            .expect("known v2 agent should reload"),
        V2ReloadRoute::NestedParent => {
            let environment = parent_turn
                .environments
                .primary()
                .expect("parent environment");
            let thread_config = environment.config().clone();
            let mut owner_config = thread_config.clone();
            owner_config.allow_login_shell = false;
            let mut selection = environment.selection();
            selection.config = EnvironmentConfigState::Ready(owner_config);
            parent_thread
                .session
                .services
                .turn_environments
                .update_selections(std::slice::from_ref(&selection), &thread_config);
            parent_turn = parent_thread.session.new_default_turn().await;
            parent_thread.session.mark_interrupted();
            // The fixture has no task runner to finish the turn or consume child results.
            *parent_thread.session.active_turn.lock().await = None;
            let _ = parent_thread
                .session
                .input_queue
                .drain_mailbox_input_items()
                .await;
            harness
                .manager
                .ensure_multi_agent_v2_child_loaded(spawned_agent.thread_id)
                .await
                .expect("known child should reload through its parent");
            assert!(harness.manager.get_thread(parent_thread_id).await.is_err());
        }
    }
    let reloaded_child = harness
        .manager
        .get_thread(spawned_agent.thread_id)
        .await
        .expect("reloaded child thread should exist");
    if matches!(route, V2ReloadRoute::NestedParent) {
        let reloaded_turn = reloaded_child.session.new_default_turn().await;
        assert_eq!(
            (
                reloaded_turn.environments.to_selections(),
                reloaded_turn.permission_profile(),
                reloaded_child.client_mcp_extensions(),
            ),
            (
                parent_turn.environments.to_selections(),
                parent_turn.permission_profile(),
                client_mcp_extensions,
            ),
        );
        assert!(Arc::ptr_eq(
            &reloaded_child.session.services.exec_policy,
            &parent_thread.session.services.exec_policy,
        ));
    }
    assert_eq!(
        reloaded_child.config_snapshot().await.model,
        "gpt-5.6-luna",
        "residency reload must preserve the worker model instead of inheriting its parent model",
    );
    assert_eq!(
        (
            reloaded_child.config_snapshot().await.model_provider_id,
            reloaded_child
                .session
                .new_default_turn()
                .await
                .provider
                .info()
                .clone(),
        ),
        (
            stored_child.model_provider,
            harness.config.model_provider.clone()
        ),
        "residency reload must preserve the worker provider instead of inheriting its sender's provider",
    );

    let communication = InterAgentCommunication::new(
        AgentPath::root(),
        agent_path,
        Vec::new(),
        "hello after reload".to_string(),
        /*trigger_turn*/ false,
    );
    control
        .send_inter_agent_communication(
            spawned_agent.thread_id,
            communication.clone(),
            AgentCommunicationContext::new(AgentCommunicationKind::Message, ThreadId::new()),
            Default::default(),
        )
        .await
        .expect("send_inter_agent_communication should succeed after reload");
    let expected = (
        spawned_agent.thread_id,
        Op::InterAgentCommunication {
            communication,
            start_options: Default::default(),
        },
    );
    let captured = harness
        .manager
        .captured_ops()
        .into_iter()
        .find(|entry| captured_op_matches(entry, &expected));
    assert!(captured.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_agent_v1_cold_delivery_reloads_and_preserves_turn_ancestry() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    let responses = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-v1-initial"),
                ev_completed("resp-v1-initial"),
            ]),
            sse(vec![
                ev_response_created("resp-v1-cold"),
                ev_completed("resp-v1-cold"),
            ]),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    config
        .features
        .disable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    config
        .features
        .enable(Feature::Sqlite)
        .expect("test config should allow feature update");
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    config.model_providers.insert(
        config.model_provider_id.clone(),
        config.model_provider.clone(),
    );
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let (parent_thread_id, _parent_thread) = harness.start_thread().await;
    let child_thread_id = make_cold_multi_agent_v1_child(&harness, parent_thread_id).await;

    let parent_turn_id = "parent-turn-v1-cold";
    let root_turn_id = "root-turn-v1-cold";
    harness
        .control
        .deliver_input_to_agent(
            harness.config.clone(),
            child_thread_id,
            text_input("cold v1 delivery"),
            AgentInputDelivery::Queue,
            TurnStartOptions {
                parent_turn_id: Some(parent_turn_id.to_string()),
                root_turn_id: Some(root_turn_id.to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("cold v1 delivery should reload and submit");

    timeout(Duration::from_secs(10), async {
        loop {
            if responses.requests().len() == 2 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cold v1 delivery should reach the model");
    let requests = responses.requests();
    assert_eq!(requests.len(), 2);
    let cold_request = requests[1].body_json();
    assert_parent_turn(&cold_request, Some(parent_turn_id))?;
    assert_root_turn(&cold_request, Some(root_turn_id))?;
    assert!(
        requests[1].body_contains_text("cold v1 delivery"),
        "the reloaded v1 agent should receive the submitted input"
    );
    assert!(
        harness.manager.get_thread(child_thread_id).await.is_ok(),
        "cold v1 delivery should restore the child in the live manager"
    );
    wait_for_agent_status(&harness.control, child_thread_id, |status| {
        matches!(status, AgentStatus::Completed(_))
    })
    .await;
    harness
        .manager
        .get_thread(child_thread_id)
        .await?
        .shutdown_and_wait()
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_delivery_waits_for_completion_cleanup_before_reloading() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    let _initial_response = mount_sse_once(
        &server,
        sse(vec![
            ev_response_created("resp-v1-race-initial"),
            ev_completed("resp-v1-race-initial"),
        ]),
    )
    .await;
    let (home, mut config) = test_config().await;
    config
        .features
        .disable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    config
        .features
        .enable(Feature::Sqlite)
        .expect("test config should allow feature update");
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    config.model_providers.insert(
        config.model_provider_id.clone(),
        config.model_provider.clone(),
    );
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let (parent_thread_id, _parent_thread) = harness.start_thread().await;
    let child_thread_id = make_cold_multi_agent_v1_child(&harness, parent_thread_id).await;
    let lifecycle = harness
        .control
        .get_agent_metadata(child_thread_id)
        .expect("cold child metadata should remain registered")
        .lifecycle;
    let completion_registration = lifecycle
        .try_start_completion_watcher()
        .expect("test should own the simulated completion cleanup");
    let cleanup_transition = lifecycle.lock_transition().await;
    let communication = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::try_from("/root/cold_v1_child").expect("test agent path"),
        Vec::new(),
        "queue after completion cleanup".to_string(),
        /*trigger_turn*/ false,
    );
    let expected = (
        child_thread_id,
        Op::InterAgentCommunication {
            communication: communication.clone(),
            start_options: TurnStartOptions::default(),
        },
    );
    let control = harness.control.clone();
    let delivery_config = harness.config.clone();
    let mut delivery = tokio::spawn(async move {
        control
            .deliver_inter_agent_communication_to_agent(
                delivery_config,
                child_thread_id,
                communication,
                AgentCommunicationContext::new(AgentCommunicationKind::Message, parent_thread_id),
                AgentInputDelivery::Queue,
                TurnStartOptions::default(),
            )
            .await
    });

    assert!(
        timeout(Duration::from_millis(100), &mut delivery)
            .await
            .is_err(),
        "delivery must wait while completion cleanup owns the transition"
    );
    assert!(matches!(
        harness.manager.get_thread(child_thread_id).await,
        Err(err) if matches!(err.details(), CodexErrorDetails::ThreadNotFound(id) if *id == child_thread_id)
    ));
    drop(cleanup_transition);
    assert!(
        timeout(Duration::from_millis(100), &mut delivery)
            .await
            .is_err(),
        "delivery must still wait for the completion watcher to publish cleanup completion"
    );
    assert!(matches!(
        harness.manager.get_thread(child_thread_id).await,
        Err(err) if matches!(err.details(), CodexErrorDetails::ThreadNotFound(id) if *id == child_thread_id)
    ));

    drop(completion_registration);
    timeout(Duration::from_secs(10), delivery)
        .await
        .expect("delivery should finish after completion cleanup")
        .expect("delivery task should not panic")
        .expect("delivery should reload the child");
    assert!(
        harness
            .manager
            .captured_ops()
            .iter()
            .any(|entry| captured_op_matches(entry, &expected)),
        "delivery should submit exactly after the cold reload is admitted"
    );
    assert!(
        harness.manager.get_thread(child_thread_id).await.is_ok(),
        "delivery should reload the child once cleanup is complete"
    );
    harness
        .manager
        .get_thread(child_thread_id)
        .await?
        .shutdown_and_wait()
        .await?;
    Ok(())
}

#[tokio::test]
async fn resume_agent_from_rollout_does_not_reopen_v2_descendants() {
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Sqlite);
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let worker_path = AgentPath::root().join("worker").expect("worker path");
    let worker_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello worker"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(worker_path.clone()),
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("worker spawn should succeed");
    let reviewer_path = worker_path.join("reviewer").expect("reviewer path");
    let reviewer_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello reviewer"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: worker_thread_id,
                depth: 2,
                agent_path: Some(reviewer_path.clone()),
                agent_nickname: None,
                agent_role: Some("reviewer".to_string()),
            })),
        )
        .await
        .expect("reviewer spawn should succeed");
    let sibling_thread_id = harness
        .spawn_anonymous_child(parent_thread_id, SpawnAgentOptions::default())
        .await;

    let worker_thread = harness
        .manager
        .get_thread(worker_thread_id)
        .await
        .expect("worker thread should exist");
    let reviewer_thread = harness
        .manager
        .get_thread(reviewer_thread_id)
        .await
        .expect("reviewer thread should exist");
    let sibling_thread = harness
        .manager
        .get_thread(sibling_thread_id)
        .await
        .expect("sibling thread should exist");
    persist_thread_for_tree_resume(&parent_thread, "parent persisted").await;
    persist_thread_for_tree_resume(&worker_thread, "worker persisted").await;
    persist_thread_for_tree_resume(&reviewer_thread, "reviewer persisted").await;
    persist_thread_for_tree_resume(&sibling_thread, "sibling persisted").await;
    wait_for_live_thread_spawn_children(
        &harness.control,
        parent_thread_id,
        &[worker_thread_id, sibling_thread_id],
    )
    .await;
    wait_for_live_thread_spawn_children(&harness.control, worker_thread_id, &[reviewer_thread_id])
        .await;

    let report = harness
        .manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    assert_eq!(report.submit_failed, Vec::<ThreadId>::new());
    assert_eq!(report.timed_out, Vec::<ThreadId>::new());

    let resumed_manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        harness.config.model_provider.clone(),
        harness.config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        harness.state_db.clone(),
    );
    let resumed_control = resumed_manager.agent_control();
    let resumed_parent_thread_id = resumed_control
        .resume_agent_from_rollout(
            harness.config.clone(),
            parent_thread_id,
            SessionSource::Exec,
        )
        .await
        .expect("v2 root resume should succeed");
    assert_eq!(resumed_parent_thread_id, parent_thread_id);
    assert_ne!(
        resumed_control.get_status(parent_thread_id).await,
        AgentStatus::NotFound
    );
    assert_thread_not_loaded(&resumed_manager, worker_thread_id).await;
    assert_thread_not_loaded(&resumed_manager, reviewer_thread_id).await;
    assert_thread_not_loaded(&resumed_manager, sibling_thread_id).await;
    resumed_control
        .restore_v2_agent_metadata(&harness.config, parent_thread_id)
        .await;
    for thread_id in [worker_thread_id, sibling_thread_id] {
        assert!(resumed_control.ensure_agent_known(thread_id).is_ok());
    }

    resumed_control
        .close_agent(worker_thread_id)
        .await
        .expect("closing a restored sibling should succeed");

    let closed_worker = resumed_control.ensure_agent_known(worker_thread_id);
    let surviving_sibling = resumed_control.ensure_agent_known(sibling_thread_id);
    assert!(closed_worker.is_err());
    assert!(surviving_sibling.is_ok());
    assert_thread_not_loaded(&resumed_manager, sibling_thread_id).await;
}

#[tokio::test]
async fn spawn_agent_creates_thread_and_sends_prompt() {
    let harness = AgentControlHarness::new().await;
    let thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("spawned"),
            /*session_source*/ None,
        )
        .await
        .expect("spawn_agent should succeed");
    let thread = harness
        .manager
        .get_thread(thread_id)
        .await
        .expect("thread should be registered");
    wait_for_recorded_user_message(thread.as_ref(), "spawned").await;
}

#[tokio::test]
async fn ephemeral_spawn_does_not_persist_agent_graph_edge() {
    let (home, mut config) = test_config().await;
    config.ephemeral = true;
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let (parent_thread_id, _parent_thread) = harness.start_thread().await;
    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("spawned"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
        )
        .await
        .expect("ephemeral agent spawn should succeed");

    let persisted_children = harness
        .state_db
        .as_ref()
        .expect("manager should retain state db")
        .list_thread_spawn_children(parent_thread_id)
        .await
        .expect("persisted child list should load");
    assert_eq!(persisted_children, Vec::<ThreadId>::new());
    assert!(
        harness.manager.get_thread(child_thread_id).await.is_ok(),
        "ephemeral child should remain live"
    );
}

#[tokio::test]
async fn spawn_agent_fork_from_paginated_parent_uses_model_context_prefix() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_paginated_thread().await;
    parent_thread
        .inject_response_items(vec![user_message("paginated parent context")])
        .await
        .expect("inject paginated parent context");
    let turn_context = parent_thread.session.new_default_turn().await;
    let parent_spawn_call_id = "spawn-call-paginated".to_string();
    parent_thread
        .session
        .record_conversation_items(
            turn_context.as_ref(),
            &[spawn_agent_call(&parent_spawn_call_id)],
        )
        .await;
    parent_thread
        .session
        .persist_rollout_items(&[
            rollout_response_item(ResponseItem::Message {
                id: None,
                role: "developer".to_string(),
                content: vec![ContentItem::InputText {
                    text: "id-less inherited context".to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }),
            RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
                thread_id: parent_thread_id,
                turn_id: "parent-turn".to_string(),
                item: TurnItem::UserMessage(UserMessageItem {
                    id: "parent-user".to_string(),
                    client_id: None,
                    content: Vec::new(),
                }),
                started_at_ms: Some(0),
                completed_at_ms: 1,
            })),
            RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(
                ThreadSettingsAppliedEvent {
                    thread_settings: ThreadSettingsSnapshot {
                        model: "parent-only-model".to_string(),
                        model_provider_id: "parent-only-provider".to_string(),
                        service_tier: None,
                        approval_policy: AskForApproval::Never,
                        approvals_reviewer: ApprovalsReviewer::User,
                        permission_profile: PermissionProfile::workspace_write(),
                        active_permission_profile: None,
                        cwd: harness.config.cwd.clone(),
                        reasoning_effort: None,
                        reasoning_summary: None,
                        personality: None,
                        collaboration_mode: CollaborationMode {
                            mode: ModeKind::Default,
                            settings: Settings {
                                model: "parent-only-model".to_string(),
                                reasoning_effort: None,
                                developer_instructions: None,
                            },
                        },
                    },
                },
            )),
        ])
        .await;

    let child_thread_id = harness
        .spawn_anonymous_child(
            parent_thread_id,
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id),
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                ..Default::default()
            },
        )
        .await;
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    assert!(
        history_contains_text(
            child_thread.session.clone_history().await.raw_items(),
            "paginated parent context",
        ),
        "bounded parent context should remain model-visible to the child"
    );
    child_thread.ensure_rollout_materialized().await;
    child_thread
        .flush_rollout()
        .await
        .expect("child rollout should flush");
    let rollout_path = child_thread
        .rollout_path()
        .expect("child rollout should exist");
    let lines = std::fs::read_to_string(&rollout_path)
        .expect("read child rollout")
        .lines()
        .map(|line| serde_json::from_str::<RolloutLine>(line).expect("parse rollout line"))
        .collect::<Vec<_>>();
    let RolloutItem::SessionMeta(meta_line) = &lines[0].item else {
        panic!("child rollout should start with session metadata");
    };
    assert_eq!(meta_line.meta.history_mode, ThreadHistoryMode::Paginated);
    assert_eq!(meta_line.meta.parent_thread_id, Some(parent_thread_id));
    assert_eq!(meta_line.meta.forked_from_id, Some(parent_thread_id));
    let prefix_end = usize::try_from(
        meta_line
            .meta
            .subagent_history_start_ordinal
            .expect("paginated child should mark its local history boundary"),
    )
    .expect("history boundary should fit in usize");
    let copied_prefix = &lines[1..prefix_end];
    let copied_idless_context = copied_prefix
        .iter()
        .find_map(|line| match &line.item {
            RolloutItem::ResponseItem(response_item)
                if serde_json::to_string(&response_item.item)
                    .expect("serialize response item")
                    .contains("id-less inherited context") =>
            {
                Some(response_item)
            }
            _ => None,
        })
        .expect("copied prefix should contain inherited response item");
    assert!(
        copied_idless_context.id().is_some_and(|id| !id.is_empty()),
        "copied model context should receive response item ids before persistence"
    );
    let copied_parent_context_count = lines
        .iter()
        .filter(|line| {
            serde_json::to_string(&line.item)
                .expect("serialize rollout item")
                .contains("paginated parent context")
        })
        .count();
    assert_eq!(
        copied_parent_context_count, 1,
        "copied model context should be persisted once"
    );
    assert!(
        !copied_prefix.iter().any(|line| {
            matches!(
                &line.item,
                RolloutItem::EventMsg(
                    EventMsg::ItemCompleted(_) | EventMsg::ThreadSettingsApplied(_)
                )
            )
        }),
        "copied non-structural presentation and metadata records should not enter the child rollout"
    );

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

#[tokio::test]
async fn self_contained_paginated_forks_work_without_state_db() {
    let harness = AgentControlHarness::new_without_state_db().await;
    let (parent_thread_id, parent_thread) = harness.start_paginated_thread().await;
    parent_thread
        .session
        .inject_no_new_turn(
            vec![user_message("paginated source without sqlite")],
            /*current_turn_context*/ None,
        )
        .await;
    let parent_spawn_call_id = "spawn-call-without-sqlite";
    let turn_context = parent_thread.session.new_default_turn().await;
    parent_thread
        .session
        .record_conversation_items(
            turn_context.as_ref(),
            &[spawn_agent_call(parent_spawn_call_id)],
        )
        .await;
    parent_thread.ensure_rollout_materialized().await;
    parent_thread
        .flush_rollout()
        .await
        .expect("parent rollout should flush");

    let full_history_child_id = harness
        .spawn_anonymous_child(
            parent_thread_id,
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id.to_string()),
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                ..Default::default()
            },
        )
        .await;
    let full_history_child = harness
        .manager
        .get_thread(full_history_child_id)
        .await
        .expect("FullHistory child should exist");
    assert!(history_contains_text(
        full_history_child.session.clone_history().await.raw_items(),
        "paginated source without sqlite",
    ));

    let snapshot_child = harness
        .manager
        .spawn_subagent(
            parent_thread_id,
            StartThreadOptions::new(harness.config.clone()),
        )
        .await
        .expect("snapshot child should spawn without sqlite");
    assert!(history_contains_text(
        snapshot_child
            .thread
            .session
            .clone_history()
            .await
            .raw_items(),
        "paginated source without sqlite",
    ));
}

#[tokio::test]
async fn full_history_fork_copies_paginated_history_base_lineage_across_resume() {
    let harness = AgentControlHarness::new().await;
    let store = LocalThreadStore::new(
        LocalThreadStoreConfig::from_config(&harness.config),
        harness.state_db.clone(),
    );
    let source_thread_id = ThreadId::new();
    let lineage_thread_id = ThreadId::new();
    let create_params =
        |thread_id: ThreadId, history_base: Option<HistoryPosition>| CreateThreadParams {
            session_id: source_thread_id.into(),
            thread_id,
            extra_config: None,
            forked_from_id: history_base.map(|_| source_thread_id),
            forked_from_ordinal_exclusive: None,
            parent_thread_id: history_base.map(|_| source_thread_id),
            source: SessionSource::Exec,
            thread_source: None,
            originator: "test_originator".to_string(),
            base_instructions: BaseInstructions::default(),
            dynamic_tools: Vec::new(),
            selected_capability_roots: Vec::new(),
            multi_agent_version: None,
            history_mode: ThreadHistoryMode::Paginated,
            history_base,
            subagent_history_start_ordinal: None,
            persistence_mode: ThreadPersistenceMode::Durable,
            initial_rollout_ordinal: 0,
            initial_window_id: "window-history-base".to_string(),
            metadata: ThreadPersistenceMetadata {
                cwd: Some(harness.config.cwd.to_path_buf()),
                model_provider: harness.config.model_provider_id.clone(),
                memory_mode: ThreadMemoryMode::Enabled,
            },
        };
    let user_item = |text: &str| {
        rollout_response_item(ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: text.to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        })
    };

    store
        .create_thread(create_params(source_thread_id, None))
        .await
        .expect("create history source");
    store
        .append_items(AppendThreadItemsParams {
            thread_id: source_thread_id,
            items: vec![user_item("source before child boundary")],
        })
        .await
        .expect("append source prefix");
    store
        .persist_thread(source_thread_id, PersistContext::Standard)
        .await
        .expect("persist source prefix");
    let source_path = store
        .live_rollout_path(source_thread_id)
        .await
        .expect("source rollout path");
    let source_bytes = std::fs::read(&source_path).expect("read source prefix");
    let source_end_ordinal = source_bytes
        .split_inclusive(|byte| *byte == b'\n')
        .map(|line| {
            serde_json::from_slice::<RolloutLine>(line)
                .expect("parse source rollout line")
                .ordinal
                .expect("paginated source ordinal")
        })
        .max()
        .expect("source rollout ordinal")
        + 1;
    let history_base = HistoryPosition {
        thread_id: source_thread_id,
        end_ordinal_exclusive: source_end_ordinal,
        end_byte_offset: u64::try_from(source_bytes.len()).expect("source prefix byte offset"),
    };

    store
        .create_thread(create_params(lineage_thread_id, Some(history_base)))
        .await
        .expect("create history-base child");
    store
        .append_items(AppendThreadItemsParams {
            thread_id: lineage_thread_id,
            items: vec![user_item("history-base child suffix")],
        })
        .await
        .expect("append history-base child suffix");
    store
        .persist_thread(lineage_thread_id, PersistContext::Standard)
        .await
        .expect("persist history-base child suffix");
    store
        .append_items(AppendThreadItemsParams {
            thread_id: source_thread_id,
            items: vec![user_item("source after child boundary")],
        })
        .await
        .expect("append source suffix after child boundary");
    store
        .shutdown_thread(source_thread_id)
        .await
        .expect("close history source");
    store
        .shutdown_thread(lineage_thread_id)
        .await
        .expect("close history-base child");

    let lineage_prepared = store
        .prepare_fork(codex_thread_store::PrepareForkParams {
            thread_id: lineage_thread_id,
            boundary: codex_thread_store::ForkBoundary::Latest,
        })
        .await
        .expect("freeze history-base child");
    let reference_thread_id = ThreadId::new();
    let mut reference_params = create_params(reference_thread_id, None);
    reference_params.forked_from_id = Some(lineage_thread_id);
    reference_params.parent_thread_id = Some(lineage_thread_id);
    reference_params.initial_rollout_ordinal = lineage_prepared
        .frozen_segment
        .next_rollout_ordinal
        .unwrap_or_default();
    store
        .create_thread(reference_params)
        .await
        .expect("create pre-fix reference-backed descendant");
    store
        .persist_thread(reference_thread_id, PersistContext::Standard)
        .await
        .expect("persist pre-fix reference-backed descendant");
    store
        .append_items(AppendThreadItemsParams {
            thread_id: reference_thread_id,
            items: vec![RolloutItem::RolloutReference(
                lineage_prepared.frozen_segment.reference.clone(),
            )],
        })
        .await
        .expect("append pre-fix reference-backed descendant");
    store
        .shutdown_thread(reference_thread_id)
        .await
        .expect("close pre-fix reference-backed descendant");
    drop(lineage_prepared);

    let prepared = store
        .prepare_fork_without_response_history(codex_thread_store::PrepareForkParams {
            thread_id: reference_thread_id,
            boundary: codex_thread_store::ForkBoundary::Latest,
        })
        .await
        .expect("prepare excludeTurns history-base child for thread/fork");
    assert!(
        prepared.copied_history.as_ref().is_some_and(|history| {
            let serialized = serde_json::to_string(history.as_slice())
                .expect("serialize copied persistence history");
            serialized.contains("source before child boundary")
                && serialized.contains("history-base child suffix")
        }),
        "excludeTurns preparation must retain full copied persistence history"
    );
    let (prepared_child, _) = harness
        .manager
        .fork_prepared_thread(
            harness.config.clone(),
            prepared,
            /*thread_source*/ None,
            /*parent_trace*/ None,
            codex_protocol::mcp::ClientMcpExtensions::default(),
            /*reserved_thread_id*/ None,
        )
        .await
        .expect("thread/fork should copy the complete history-base lineage");
    let prepared_child_history = prepared_child.thread.session.clone_history().await;
    assert!(history_contains_text(
        prepared_child_history.raw_items(),
        "source before child boundary"
    ));
    assert!(history_contains_text(
        prepared_child_history.raw_items(),
        "history-base child suffix"
    ));
    assert!(!history_contains_text(
        prepared_child_history.raw_items(),
        "source after child boundary"
    ));
    prepared_child.thread.ensure_rollout_materialized().await;
    prepared_child
        .thread
        .flush_rollout()
        .await
        .expect("persist prepared full-history child");
    let prepared_child_lines = std::fs::read_to_string(
        prepared_child
            .thread
            .rollout_path()
            .expect("prepared full-history child rollout path"),
    )
    .expect("read prepared full-history child rollout")
    .lines()
    .map(|line| serde_json::from_str::<RolloutLine>(line).expect("parse child rollout line"))
    .collect::<Vec<_>>();
    assert!(
        prepared_child_lines
            .iter()
            .all(|line| !matches!(line.item, RolloutItem::RolloutReference(_)))
    );
    assert!(prepared_child_lines.iter().any(|line| {
        serde_json::to_string(&line.item)
            .expect("serialize prepared child item")
            .contains("source before child boundary")
    }));
    assert!(prepared_child_lines.iter().any(|line| {
        serde_json::to_string(&line.item)
            .expect("serialize prepared child item")
            .contains("history-base child suffix")
    }));
    prepared_child
        .thread
        .submit(Op::Shutdown {})
        .await
        .expect("shutdown prepared full-history child");

    harness
        .control
        .resume_agent_from_rollout(
            harness.config.clone(),
            reference_thread_id,
            SessionSource::Exec,
        )
        .await
        .expect("resume history-base child");
    let lineage_thread = harness
        .manager
        .get_thread(reference_thread_id)
        .await
        .expect("resumed history-base child should exist");
    let spawn_call_id = "spawn-call-history-base";
    let turn_context = lineage_thread.session.new_default_turn().await;
    lineage_thread
        .session
        .record_conversation_items(turn_context.as_ref(), &[spawn_agent_call(spawn_call_id)])
        .await;
    let full_history_child_id = harness
        .spawn_anonymous_child(
            reference_thread_id,
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(spawn_call_id.to_string()),
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                ..Default::default()
            },
        )
        .await;
    let full_history_child = harness
        .manager
        .get_thread(full_history_child_id)
        .await
        .expect("full-history child should exist");
    let child_history = full_history_child.session.clone_history().await;
    assert!(history_contains_text(
        child_history.raw_items(),
        "source before child boundary"
    ));
    assert!(history_contains_text(
        child_history.raw_items(),
        "history-base child suffix"
    ));
    assert!(!history_contains_text(
        child_history.raw_items(),
        "source after child boundary"
    ));
    full_history_child.ensure_rollout_materialized().await;
    full_history_child
        .flush_rollout()
        .await
        .expect("persist full-history child");
    let persisted_lines = std::fs::read_to_string(
        full_history_child
            .rollout_path()
            .expect("full-history child rollout path"),
    )
    .expect("read full-history child rollout")
    .lines()
    .map(|line| serde_json::from_str::<RolloutLine>(line).expect("parse child rollout line"))
    .collect::<Vec<_>>();
    assert!(
        persisted_lines
            .iter()
            .all(|line| !matches!(line.item, RolloutItem::RolloutReference(_)))
    );

    let spawned_child = harness
        .manager
        .spawn_subagent(
            reference_thread_id,
            StartThreadOptions::new(harness.config.clone()),
        )
        .await
        .expect("spawn_subagent should copy the complete history-base lineage");
    let spawned_history = spawned_child.thread.session.clone_history().await;
    assert!(history_contains_text(
        spawned_history.raw_items(),
        "source before child boundary"
    ));
    assert!(history_contains_text(
        spawned_history.raw_items(),
        "history-base child suffix"
    ));
    assert!(!history_contains_text(
        spawned_history.raw_items(),
        "source after child boundary"
    ));
    spawned_child.thread.ensure_rollout_materialized().await;
    spawned_child
        .thread
        .flush_rollout()
        .await
        .expect("persist spawn_subagent child");
    let spawned_lines = std::fs::read_to_string(
        spawned_child
            .thread
            .rollout_path()
            .expect("spawn_subagent child rollout path"),
    )
    .expect("read spawn_subagent child rollout")
    .lines()
    .map(|line| serde_json::from_str::<RolloutLine>(line).expect("parse child rollout line"))
    .collect::<Vec<_>>();
    assert!(
        spawned_lines
            .iter()
            .all(|line| !matches!(line.item, RolloutItem::RolloutReference(_)))
    );
    assert!(spawned_lines.iter().any(|line| {
        serde_json::to_string(&line.item)
            .expect("serialize spawn_subagent child item")
            .contains("source before child boundary")
    }));
    assert!(spawned_lines.iter().any(|line| {
        serde_json::to_string(&line.item)
            .expect("serialize spawn_subagent child item")
            .contains("history-base child suffix")
    }));
    spawned_child
        .thread
        .submit(Op::Shutdown {})
        .await
        .expect("shutdown spawn_subagent child");

    harness
        .control
        .shutdown_live_agent(full_history_child_id)
        .await
        .expect("shutdown full-history child");
    harness
        .control
        .shutdown_live_agent(reference_thread_id)
        .await
        .expect("shutdown reference-backed parent");
    harness
        .control
        .resume_agent_from_rollout(
            harness.config.clone(),
            full_history_child_id,
            SessionSource::Exec,
        )
        .await
        .expect("reopen full-history child");
    let reopened_child = harness
        .manager
        .get_thread(full_history_child_id)
        .await
        .expect("reopened full-history child should exist");
    let reopened_history = reopened_child.session.clone_history().await;
    assert!(history_contains_text(
        reopened_history.raw_items(),
        "source before child boundary"
    ));
    assert!(history_contains_text(
        reopened_history.raw_items(),
        "history-base child suffix"
    ));
    assert!(!history_contains_text(
        reopened_history.raw_items(),
        "source after child boundary"
    ));
    harness
        .control
        .shutdown_live_agent(full_history_child_id)
        .await
        .expect("shutdown reopened full-history child");
}

#[tokio::test]
async fn spawn_agent_without_fork_from_paginated_parent_stays_fresh_and_paginated() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_paginated_thread().await;
    parent_thread
        .inject_response_items(vec![user_message("parent-only context")])
        .await
        .expect("inject parent-only context");

    let child_thread_id = harness
        .spawn_anonymous_child(
            parent_thread_id,
            SpawnAgentOptions {
                parent_thread_id: Some(parent_thread_id),
                ..Default::default()
            },
        )
        .await;
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    assert!(
        !history_contains_text(
            child_thread.session.clone_history().await.raw_items(),
            "parent-only context",
        ),
        "fork_turns=none should not copy parent context"
    );
    child_thread.ensure_rollout_materialized().await;
    child_thread
        .flush_rollout()
        .await
        .expect("child rollout should flush");
    let meta = codex_rollout::read_session_meta_line(
        &child_thread
            .rollout_path()
            .expect("child rollout should exist"),
    )
    .await
    .expect("read child session metadata");
    assert_eq!(meta.meta.history_mode, ThreadHistoryMode::Paginated);
    assert_eq!(meta.meta.subagent_history_start_ordinal, None);

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

#[tokio::test]
async fn spawn_agent_numeric_fork_from_compacted_paginated_parent_clamps_to_provable_turns() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_paginated_thread().await;
    let parent_spawn_call_id = "spawn-call-paginated-numeric".to_string();
    parent_thread
        .session
        .persist_rollout_items(&[
            RolloutItem::Compacted(CompactedItem {
                message: String::new(),
                replacement_history: Some(vec![
                    ResponseItem::Message {
                        id: None,
                        role: "user".to_string(),
                        content: vec![ContentItem::InputText {
                            text: "compacted summary".to_string(),
                        }],
                        phase: None,
                        internal_chat_message_metadata_passthrough: None,
                    }
                    .into(),
                ]),
                mcp_resource_origins: None,
                window_number: None,
                first_window_id: None,
                previous_window_id: None,
                window_id: None,
            }),
            rollout_response_item(ResponseItem::Message {
                id: None,
                role: "user".to_string(),
                content: vec![ContentItem::InputText {
                    text: "recent parent turn".to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }),
            rollout_response_item(spawn_agent_call(&parent_spawn_call_id)),
        ])
        .await;

    let clamped_child_thread_id = harness
        .spawn_anonymous_child(
            parent_thread_id,
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id),
                fork_mode: Some(SpawnAgentForkMode::LastNTurns(2)),
                ..Default::default()
            },
        )
        .await;
    let clamped_child_thread = harness
        .manager
        .get_thread(clamped_child_thread_id)
        .await
        .expect("clamped child thread should be registered");
    let clamped_history = clamped_child_thread.session.clone_history().await;
    assert!(
        history_contains_text(clamped_history.raw_items(), "recent parent turn"),
        "clamped numeric fork should keep the provable recent turn"
    );
    assert!(
        !history_contains_text(clamped_history.raw_items(), "compacted summary"),
        "clamped numeric fork should not expand into compacted parent context"
    );

    let _ = harness
        .control
        .shutdown_live_agent(clamped_child_thread_id)
        .await
        .expect("clamped child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

#[tokio::test]
async fn spawn_agent_can_fork_parent_thread_history_with_sanitized_items() {
    let managed_fragment = "<managed_developer_instructions>\nParent developer instructions.\n</managed_developer_instructions>";
    let persistent_fragment =
        "<persistent_mode>\nParent developer instructions.\n</persistent_mode>";
    let harness = AgentControlHarness::new().await;
    let mut parent_config = harness.config.clone();
    let _ = parent_config.features.enable(Feature::MultiAgentV2);
    parent_config.developer_instructions = Some("Parent developer instructions.".to_string());
    parent_config.multi_agent_v2.root_agent_usage_hint_text =
        Some("Parent root guidance.".to_string());
    parent_config.multi_agent_v2.subagent_usage_hint_text =
        Some("Parent subagent guidance.".to_string());
    let mut child_config = harness.config.clone();
    let _ = child_config.features.enable(Feature::MultiAgentV2);
    child_config.developer_instructions = Some("Child developer instructions.".to_string());
    child_config.multi_agent_v2.subagent_developer_instructions =
        Some("Child developer instructions.".to_string());
    child_config.multi_agent_v2.root_agent_usage_hint_text =
        Some("Child root guidance.".to_string());
    child_config.multi_agent_v2.subagent_usage_hint_text =
        Some("Child subagent guidance.".to_string());
    let new_thread = harness
        .manager
        .start_thread(StartThreadOptions::new(parent_config.clone()))
        .await
        .expect("start parent thread");
    let parent_thread_id = new_thread.thread_id;
    let parent_thread = new_thread.thread;
    parent_thread
        .session
        .inject_no_new_turn(
            vec![user_message("parent seed context")],
            /*current_turn_context*/ None,
        )
        .await;
    let expected_parent_seed = parent_thread
        .session
        .clone_history()
        .await
        .raw_items()
        .next()
        .cloned()
        .expect("parent seed should be recorded");
    let turn_context = parent_thread.session.new_default_turn().await;
    let parent_spawn_call_id = "spawn-call-history".to_string();
    let trigger_message = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::try_from("/root/worker").expect("agent path"),
        Vec::new(),
        "parent trigger message".to_string(),
        /*trigger_turn*/ true,
    );
    let standalone_output = ResponseItem::FunctionCallOutput {
        id: None,
        call_id: None,
        name: Some("notifications".to_string()),
        namespace: Some("slack".to_string()),
        output: FunctionCallOutputPayload::from_text("parent notification".to_string()),
        internal_chat_message_metadata_passthrough: None,
    };
    parent_thread
        .session
        .record_conversation_items(
            turn_context.as_ref(),
            &[
                ResponseItem::Message {
                    id: None,
                    role: "developer".to_string(),
                    content: vec![ContentItem::InputText {
                        text: "Parent root guidance.".to_string(),
                    }],
                    phase: None,
                    internal_chat_message_metadata_passthrough: None,
                },
                ResponseItem::Message {
                    id: None,
                    role: "developer".to_string(),
                    content: vec![ContentItem::InputText {
                        text: "Parent subagent guidance.".to_string(),
                    }],
                    phase: None,
                    internal_chat_message_metadata_passthrough: None,
                },
                ResponseItem::Message {
                    id: None,
                    role: "developer".to_string(),
                    content: vec![
                        ContentItem::InputText {
                            text: "Developer context before.\nParent developer instructions.\nDeveloper context after."
                                .to_string(),
                        },
                        ContentItem::InputText {
                            text: "<multi_agent_mode>Proactive multi-agent delegation is active.</multi_agent_mode>"
                                .to_string(),
                        },
                        ContentItem::InputText {
                            text: "Preserved developer context.".to_string(),
                        },
                        ContentItem::InputText {
                            text: managed_fragment.to_string(),
                        },
                        ContentItem::InputText {
                            text: persistent_fragment.to_string(),
                        },
                    ],
                    phase: None,
                    internal_chat_message_metadata_passthrough: Some(
                        InternalChatMessageMetadataPassthrough {
                            content_item_kinds: Some(vec![
                                ContentItemKind("generic.developer_instructions".to_string()),
                                ContentItemKind("multi_agent.mode_instructions".to_string()),
                                ContentItemKind("generic.developer_policy".to_string()),
                                ContentItemKind("managed_config.developer_instructions".to_string()),
                                ContentItemKind("persistent_mode.instructions".to_string()),
                            ]),
                            ..Default::default()
                        },
                    ),
                },
                assistant_message("parent commentary", Some(MessagePhase::Commentary)),
                assistant_message("parent final answer", Some(MessagePhase::FinalAnswer)),
                standalone_output,
                assistant_message("parent unknown phase", /*phase*/ None),
                ResponseItem::Reasoning {
                    id: Some(ResponseItemId::with_suffix("rs", "parent-reasoning")),
                    summary: Vec::new(),
                    content: None,
                    encrypted_content: None,
                    internal_chat_message_metadata_passthrough: None,
                },
                trigger_message.to_response_input_item().into(),
                spawn_agent_call(&parent_spawn_call_id),
            ],
        )
        .await;
    let expected_standalone_output = parent_thread
        .session
        .clone_history()
        .await
        .raw_items()
        .find(|item| matches!(item, ResponseItem::FunctionCallOutput { call_id: None, .. }))
        .cloned()
        .expect("standalone output should be recorded");
    let parent_reference_context_item = turn_context.to_turn_context_item();
    parent_thread
        .session
        .persist_rollout_items(&[RolloutItem::TurnContext(
            parent_reference_context_item.clone(),
        )])
        .await;
    parent_thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");
    let child_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: None,
        agent_nickname: None,
        agent_role: None,
    });
    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            child_config.clone(),
            text_input("child task"),
            Some(child_source.clone()),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id.clone()),
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                initial_task_message: Some("child task".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("forked spawn should succeed")
        .thread_id;

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    assert_ne!(child_thread_id, parent_thread_id);
    assert_eq!(
        child_thread.config_snapshot().await.history_mode,
        ThreadHistoryMode::Legacy
    );
    assert_eq!(
        child_thread.session.prompt_cache_key(),
        parent_thread.session.prompt_cache_key(),
    );
    let child_mcp_runtime = Arc::clone(&child_thread.session.services.mcp_runtime);
    let parent_mcp_runtime = Arc::clone(&parent_thread.session.services.mcp_runtime);
    assert!(!Arc::ptr_eq(&child_mcp_runtime, &parent_mcp_runtime));
    let mcp_tool_snapshot = child_thread
        .session
        .services
        .mcp_tool_snapshot
        .lock()
        .await
        .clone()
        .expect("forked child should inherit an MCP tool snapshot");
    let parent_binding = parent_mcp_runtime
        .current_binding()
        .await
        .expect("parent should have a published MCP binding");
    assert_eq!(
        serde_json::to_value(&mcp_tool_snapshot.tools).expect("serialize inherited MCP tools"),
        serde_json::to_value(parent_binding.tools()).expect("serialize parent MCP tools"),
    );
    let history = child_thread.session.clone_history().await;
    let history_items = history.raw_items().cloned().collect::<Vec<_>>();
    let expected_final_answer = parent_thread
        .session
        .clone_history()
        .await
        .raw_items()
        .find(|item| {
            matches!(
                item,
                ResponseItem::Message {
                    role,
                    phase: Some(MessagePhase::FinalAnswer),
                    ..
                } if role == "assistant"
            )
        })
        .cloned()
        .expect("parent final answer should be recorded");
    let mut expected_developer_message = ResponseItem::Message {
        id: None,
        role: "developer".to_string(),
        content: vec![
            ContentItem::InputText {
                text: "Developer context before.\nChild developer instructions.\nDeveloper context after."
                    .to_string(),
            },
            ContentItem::InputText {
                text: "Preserved developer context.".to_string(),
            },
            ContentItem::InputText {
                text: managed_fragment.to_string(),
            },
            ContentItem::InputText {
                text: persistent_fragment.to_string(),
            },
        ],
        phase: None,
        internal_chat_message_metadata_passthrough: Some(
            InternalChatMessageMetadataPassthrough {
                content_item_kinds: Some(vec![
                    ContentItemKind("generic.developer_instructions".to_string()),
                    ContentItemKind("generic.developer_policy".to_string()),
                    ContentItemKind("managed_config.developer_instructions".to_string()),
                    ContentItemKind("persistent_mode.instructions".to_string()),
                ]),
                ..Default::default()
            },
        ),
    };
    expected_developer_message.set_turn_id_if_missing(&turn_context.sub_id);
    expected_developer_message.set_create_time_if_missing(
        history_items[1]
            .executed_tool_call_metadata()
            .and_then(|metadata| metadata.create_time.clone())
            .expect("recorded developer message should have a creation timestamp"),
    );
    let expected_history = [
        expected_parent_seed,
        expected_developer_message,
        expected_final_answer,
        expected_standalone_output,
        ContextualUserFragment::into(MultiAgentRoleInstructions::unmarked(
            "Child subagent guidance.",
        )),
        subagent_assignment_item(&child_source, "child task".to_string()),
    ];
    assert_eq!(
        strip_response_item_ids(&history_items),
        strip_response_item_ids(&expected_history),
        "full-history forked child history should replace parent usage hints with the child subagent hint while filtering non-final assistant/tool chatter"
    );
    child_thread.ensure_rollout_materialized().await;
    child_thread
        .flush_rollout()
        .await
        .expect("sanitized child rollout should flush");
    let physical_items = RolloutRecorder::load_rollout_items(
        child_thread
            .rollout_path()
            .expect("sanitized child rollout should exist")
            .as_path(),
    )
    .await
    .expect("load sanitized child rollout")
    .0;
    assert!(
        physical_items
            .iter()
            .all(|item| !matches!(item, RolloutItem::RolloutReference(_))),
        "a changed parent prefix must be copied after sanitization instead of persisted by reference"
    );
    assert_eq!(
        serde_json::to_value(child_thread.session.reference_context_item().await)
            .expect("serialize child reference context item"),
        serde_json::to_value(Some(parent_reference_context_item))
            .expect("serialize expected reference context item"),
        "full-history forked child should preserve the parent diff baseline"
    );

    let mut no_hint_child_config = harness.config.clone();
    let _ = no_hint_child_config.features.enable(Feature::MultiAgentV2);
    no_hint_child_config.developer_instructions = Some(String::new());
    no_hint_child_config
        .multi_agent_v2
        .subagent_developer_instructions = Some(String::new());
    no_hint_child_config.multi_agent_v2.subagent_usage_hint_text = Some(String::new());
    let no_hint_child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            no_hint_child_config,
            text_input("child task without hints"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id.clone()),
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                ..Default::default()
            },
        )
        .await
        .expect("forked spawn should honor an empty subagent usage hint")
        .thread_id;
    let no_hint_child_thread = harness
        .manager
        .get_thread(no_hint_child_thread_id)
        .await
        .expect("no-hint child thread should be registered");
    let no_hint_history = no_hint_child_thread.session.clone_history().await;
    assert!(
        !history_contains_text(no_hint_history.raw_items(), "Child subagent guidance.")
            && !history_contains_text(
                no_hint_history.raw_items(),
                "You are an agent in a team of agents"
            ),
        "full-history forked child should not add configured or bundled subagent guidance"
    );
    assert!(
        !history_contains_text(
            no_hint_history.raw_items(),
            "Developer context before.\nParent developer instructions."
        ),
        "empty child developer instructions should remove parent developer instructions"
    );
    assert!(
        history_contains_text(no_hint_history.raw_items(), managed_fragment)
            && history_contains_text(no_hint_history.raw_items(), persistent_fragment),
        "clearing child instructions must preserve overlapping managed and persistent instructions"
    );
    assert!(
        history_contains_text(
            no_hint_history.raw_items(),
            "Developer context before.\n\nDeveloper context after."
        ),
        "empty child developer instructions should preserve surrounding developer context"
    );
    assert!(
        history_contains_text(no_hint_history.raw_items(), "Preserved developer context."),
        "empty child developer instructions should preserve unrelated developer fragments"
    );

    wait_for_recorded_user_message(child_thread.as_ref(), "child task").await;

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    let resumed_child_thread_id = harness
        .control
        .resume_agent_from_rollout(child_config, child_thread_id, child_source)
        .await
        .expect("sanitized child should resume from persisted history");
    let resumed_child_thread = harness
        .manager
        .get_thread(resumed_child_thread_id)
        .await
        .expect("resumed sanitized child should be registered");
    let resumed_history = resumed_child_thread.session.clone_history().await;
    for retained_text in [
        "parent seed context",
        "Child developer instructions.",
        "parent final answer",
        "Child subagent guidance.",
        "# Subagent Assignment",
        "Your direct assignment from your parent agent is:\n\nchild task",
    ] {
        assert!(
            history_contains_text(resumed_history.raw_items(), retained_text),
            "cold resume must retain sanitized inherited text {retained_text:?}"
        );
    }
    for excluded_text in [
        "Parent root guidance.",
        "Parent subagent guidance.",
        "Developer context before.\nParent developer instructions.",
        "parent commentary",
        "parent unknown phase",
        "parent trigger message",
        "parent-reasoning",
    ] {
        assert!(
            !history_contains_text(resumed_history.raw_items(), excluded_text),
            "cold resume must not restore parent-only text {excluded_text:?} through the old reference"
        );
    }
    let _ = harness
        .control
        .shutdown_live_agent(resumed_child_thread_id)
        .await
        .expect("resumed child shutdown should submit");
    let _ = harness
        .control
        .shutdown_live_agent(no_hint_child_thread_id)
        .await
        .expect("no-hint child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forked_spawn_first_request_uses_parent_cache_key_and_mcp_snapshot() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    let child_response_mock = mount_sse_once(
        &server,
        sse(vec![ev_response_created("resp-1"), ev_completed("resp-1")]),
    )
    .await;
    let (_home, mut config) = test_config().await;
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    let mcp_server_path = config.codex_home.join("fake_mcp_server.py");
    std::fs::write(
        &mcp_server_path,
        r#"import json
import sys

def read_message():
    line = sys.stdin.buffer.readline()
    if not line:
        return None
    return json.loads(line)

def write_message(message):
    body = json.dumps(message).encode("utf-8")
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.write(b"\n")
    sys.stdout.buffer.flush()

while True:
    message = read_message()
    if message is None:
        break
    method = message.get("method")
    request_id = message.get("id")
    if request_id is None:
        continue
    if method == "initialize":
        write_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "fake-mcp", "version": "1.0.0"},
            },
        })
    elif method == "tools/list":
        write_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "tools": [{
                    "name": "echo",
                    "description": "Echo from fake MCP",
                    "inputSchema": {
                        "type": "object",
                        "properties": {},
                        "additionalProperties": False,
                    },
                }],
            },
        })
    else:
        write_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": "method not found"},
        })
"#,
    )?;
    config
        .mcp_servers
        .set(std::collections::HashMap::from([(
            "rmcp".to_string(),
            McpServerConfig {
                auth: Default::default(),
                transport: McpServerTransportConfig::Stdio {
                    command: "python3".to_string(),
                    args: vec![mcp_server_path.to_string_lossy().to_string()],
                    env: None,
                    env_vars: Vec::new(),
                    cwd: None,
                },
                environment_id: codex_config::DEFAULT_MCP_SERVER_ENVIRONMENT_ID.to_string(),
                enabled: true,
                required: false,
                supports_parallel_tool_calls: false,
                omit_tools_from: None,
                disabled_reason: None,
                oauth: None,
                startup_timeout_sec: Some(Duration::from_secs(5)),
                tool_timeout_sec: None,
                default_tools_approval_mode: None,
                enabled_tools: None,
                disabled_tools: None,
                scopes: None,
                oauth_resource: None,
                tools: std::collections::HashMap::new(),
            },
        )]))
        .expect("test config should allow MCP servers");

    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let control = manager.agent_control();
    let parent = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await?;
    let parent_thread_id = parent.thread_id;
    let parent_prompt_cache_key = parent.thread.session.prompt_cache_key();
    let mcp_runtime = Arc::clone(&parent.thread.session.services.mcp_runtime);
    assert!(
        mcp_runtime
            .latest_wait_for_server_ready("rmcp", Duration::from_secs(5))
            .await,
        "parent MCP server should become ready before forking"
    );
    let parent_mcp_tools = mcp_runtime.latest_list_all_tools().await;
    assert!(
        parent_mcp_tools
            .iter()
            .any(|tool| tool.server_name == "rmcp" && tool.tool.name == "echo"),
        "parent MCP manager should expose live MCP tools before forking: tools={parent_mcp_tools:#?}"
    );
    parent
        .thread
        .session
        .inject_no_new_turn(
            vec![user_message("parent seed")],
            /*current_turn_context*/ None,
        )
        .await;
    parent
        .thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent.thread.session.flush_rollout().await?;

    // The child has no independently configured MCP servers. Its first request can only advertise
    // the parent's tool catalog if full-history fork inheritance applies the snapshot.
    config
        .mcp_servers
        .set(std::collections::HashMap::new())
        .expect("test config should allow clearing MCP servers");

    let child_thread_id = control
        .spawn_agent_with_metadata(
            config,
            text_input("child request boundary"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: Some("worker".to_string()),
                agent_role: None,
            })),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some("spawn-call-request-boundary".to_string()),
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                ..Default::default()
            },
        )
        .await?
        .thread_id;
    let child_thread = manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");

    timeout(Duration::from_secs(5), async {
        loop {
            let event = child_thread
                .next_event()
                .await
                .expect("child event channel should stay open");
            if matches!(event.msg, EventMsg::TurnComplete(_)) {
                break;
            }
        }
    })
    .await
    .expect("child turn should complete");
    let body = child_response_mock.single_request().body_json();
    let expected_prompt_cache_key = parent_prompt_cache_key.to_string();
    assert_eq!(
        body["prompt_cache_key"].as_str(),
        Some(expected_prompt_cache_key.as_str())
    );
    assert!(
        body["tools"]
            .as_array()
            .is_some_and(|tools| tools.iter().any(|tool| {
                tool["type"] == "tool_search"
                    && tool["description"]
                        .as_str()
                        .is_some_and(|description| description.contains("\n- rmcp\n"))
            })),
        "forked child request should advertise the inherited parent MCP tool catalog: {body:#}"
    );

    Ok(())
}

#[tokio::test]
async fn spawn_agent_fork_strips_parent_usage_hints_from_compacted_history() {
    let harness = AgentControlHarness::new().await;
    let mut parent_config = harness.config.clone();
    let _ = parent_config.features.enable(Feature::MultiAgentV2);
    parent_config.developer_instructions = Some("Parent developer instructions.".to_string());
    parent_config.multi_agent_v2.root_agent_usage_hint_text =
        Some("Parent root guidance.".to_string());
    parent_config.multi_agent_v2.subagent_usage_hint_text =
        Some("Parent subagent guidance.".to_string());
    let mut child_config = harness.config.clone();
    let _ = child_config.features.enable(Feature::MultiAgentV2);
    child_config.developer_instructions = Some("Child developer instructions.".to_string());
    child_config.multi_agent_v2.subagent_developer_instructions =
        Some("Child developer instructions.".to_string());
    child_config.multi_agent_v2.root_agent_usage_hint_text =
        Some("Child root guidance.".to_string());
    child_config.multi_agent_v2.subagent_usage_hint_text =
        Some("Child subagent guidance.".to_string());
    let new_thread = harness
        .manager
        .start_thread(StartThreadOptions::new(parent_config))
        .await
        .expect("start parent thread");
    let parent_thread_id = new_thread.thread_id;
    let parent_thread = new_thread.thread;
    let turn_context = parent_thread.session.new_default_turn().await;
    let parent_spawn_call_id = "spawn-call-compacted-usage-hints".to_string();
    let parent_task = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::root().join("worker").expect("valid worker path"),
        Vec::new(),
        "compacted parent delegated task".to_string(),
        /*trigger_turn*/ true,
    );
    let replacement_history = vec![
        ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: "compacted parent summary".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        },
        ContextualUserFragment::into(MultiAgentRoleInstructions::catalog(
            "Catalog parent root guidance.",
        )),
        parent_task.to_model_input_item(),
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "Parent root guidance.".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        },
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![
                ContentItem::InputText {
                    text: "Compacted context before.\nParent developer instructions.\nCompacted context after."
                        .to_string(),
                },
                ContentItem::InputText {
                    text: "<multi_agent_mode>Proactive multi-agent delegation is active.</multi_agent_mode>"
                        .to_string(),
                },
                ContentItem::InputText {
                    text: "Preserved compacted developer context.".to_string(),
                },
            ],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        },
    ];
    parent_thread
        .session
        .persist_rollout_items(&[
            RolloutItem::Compacted(CompactedItem {
                message: String::new(),
                replacement_history: Some(
                    replacement_history.into_iter().map(Into::into).collect(),
                ),
                mcp_resource_origins: None,
                window_number: None,
                first_window_id: None,
                previous_window_id: None,
                window_id: None,
            }),
            RolloutItem::TurnContext(turn_context.to_turn_context_item()),
            rollout_response_item(spawn_agent_call(&parent_spawn_call_id)),
        ])
        .await;
    parent_thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");

    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            child_config,
            text_input("child task"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id),
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                multi_agent_v2_usage_hints: Some(ResolvedMultiAgentV2UsageHints {
                    root: None,
                    subagent: Some(MultiAgentRoleInstructions::catalog(
                        "Catalog child subagent guidance.",
                    )),
                }),
                ..Default::default()
            },
        )
        .await
        .expect("forked spawn should sanitize compacted usage hints")
        .thread_id;

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let history = child_thread.session.clone_history().await;
    assert!(
        history_contains_text(history.raw_items(), "compacted parent summary"),
        "forked child history should retain compacted non-hint content"
    );
    assert!(
        !history_contains_text(history.raw_items(), "Catalog parent root guidance."),
        "forked child history should strip the resolved parent hint from compacted replacement history"
    );
    assert!(
        history_contains_text(history.raw_items(), "Catalog child subagent guidance."),
        "full-history forked child should add the resolved child hint after compacted-history sanitization"
    );
    assert!(
        !history
            .raw_items()
            .any(|item| matches!(item, ResponseItem::AgentMessage { .. })),
        "forked child history should not inherit compacted parent agent messages"
    );
    assert!(
        !history_contains_text(history.raw_items(), "Parent root guidance."),
        "forked child history should strip stale parent hints from compacted replacement history"
    );
    assert!(
        !history_contains_text(
            history.raw_items(),
            "Proactive multi-agent delegation is active."
        ),
        "forked child history should strip stale policy fragments from compound compacted messages"
    );
    assert!(
        !history_contains_text(history.raw_items(), "Parent developer instructions."),
        "forked child history should replace parent instructions in compacted replacement history"
    );
    assert!(
        history_contains_text(
            history.raw_items(),
            "Compacted context before.\nChild developer instructions.\nCompacted context after."
        ),
        "forked child history should replace compacted parent instructions without removing surrounding context"
    );
    assert!(
        history_contains_text(
            history.raw_items(),
            "Preserved compacted developer context."
        ),
        "forked child history should preserve unrelated compacted developer fragments"
    );

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

/// Full-history forks must restore child instructions when compaction discarded
/// the only matching parent instruction fragment from effective history.
#[tokio::test]
async fn spawn_agent_full_fork_restores_instructions_after_compaction_discards_parent_fragment() {
    let harness = AgentControlHarness::new().await;
    let mut parent_config = harness.config.clone();
    let _ = parent_config.features.enable(Feature::MultiAgentV2);
    parent_config.developer_instructions = Some("Parent developer instructions.".to_string());
    let mut child_config = parent_config.clone();
    child_config.developer_instructions = Some("Child developer instructions.".to_string());
    child_config.multi_agent_v2.subagent_developer_instructions =
        Some("Child developer instructions.".to_string());

    let new_thread = harness
        .manager
        .start_thread(StartThreadOptions::new(parent_config))
        .await
        .expect("start parent thread");
    let parent_thread_id = new_thread.thread_id;
    let parent_thread = new_thread.thread;
    let turn_context = parent_thread.session.new_default_turn().await;
    let parent_spawn_call_id = "spawn-call-compacted-stale-instructions".to_string();
    let replacement_history = vec![
        ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: "compacted parent summary".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        },
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "Preserved compacted developer context.".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        },
    ];

    // Preserve the parent's live baseline while its durable checkpoint omits the
    // developer fragment that appeared in obsolete pre-compaction history.
    parent_thread
        .session
        .replace_history(
            replacement_history.clone(),
            Some(turn_context.to_turn_context_item()),
        )
        .await;
    parent_thread
        .session
        .persist_rollout_items(&[
            rollout_response_item(ResponseItem::Message {
                id: None,
                role: "developer".to_string(),
                content: vec![ContentItem::InputText {
                    text: "Parent developer instructions.".to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }),
            RolloutItem::Compacted(CompactedItem {
                message: String::new(),
                replacement_history: Some(
                    replacement_history.into_iter().map(Into::into).collect(),
                ),
                mcp_resource_origins: None,
                window_number: None,
                first_window_id: None,
                previous_window_id: None,
                window_id: None,
            }),
            RolloutItem::TurnContext(turn_context.to_turn_context_item()),
            rollout_response_item(spawn_agent_call(&parent_spawn_call_id)),
        ])
        .await;
    parent_thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");

    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            child_config,
            text_input("child task"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id),
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                ..Default::default()
            },
        )
        .await
        .expect("forked spawn should preserve effective compacted instructions")
        .thread_id;
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let history = child_thread.session.clone_history().await;
    assert!(
        history_contains_text(
            history.raw_items(),
            "Preserved compacted developer context."
        ),
        "full-history fork should preserve unrelated compacted developer fragments"
    );
    assert!(
        !history_contains_text(history.raw_items(), "Parent developer instructions."),
        "full-history fork should not restore stale pre-compaction parent instructions"
    );
    assert!(
        history_contains_text(history.raw_items(), "Child developer instructions."),
        "full-history fork should append child instructions absent from effective compacted history"
    );

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

/// A legacy compaction clears the child's baseline, so its first turn must
/// rebuild configured developer instructions exactly once.
#[tokio::test]
async fn spawn_agent_full_fork_legacy_compaction_rebuilds_child_instructions_once() {
    let managed_policy = "Managed policy for every agent.";
    let current_managed_fragment = format!(
        "<managed_developer_instructions>\n{managed_policy}\n</managed_developer_instructions>"
    );
    let stale_managed_fragment =
        "<managed_developer_instructions>\nOld managed policy.\n</managed_developer_instructions>";
    for (case, parent_developer_instructions) in [
        ("without parent instructions", None),
        (
            "with parent instructions",
            Some("Parent developer instructions."),
        ),
    ] {
        let harness = AgentControlHarness::new().await;
        let mut parent_config = harness.config.clone();
        let _ = parent_config.features.enable(Feature::MultiAgentV2);
        parent_config.developer_instructions = parent_developer_instructions.map(str::to_string);
        let mut requirements = parent_config.config_layer_stack.requirements().clone();
        requirements.additional_developer_instructions = Some(codex_config::Sourced::new(
            managed_policy.to_string(),
            codex_config::RequirementSource::Unknown,
        ));
        let mut requirements_toml = parent_config.config_layer_stack.requirements_toml().clone();
        requirements_toml.additional_developer_instructions = Some(managed_policy.to_string());
        parent_config.config_layer_stack = codex_config::ConfigLayerStack::new(
            parent_config
                .config_layer_stack
                .all_layers_low_to_high()
                .cloned()
                .collect(),
            requirements,
            requirements_toml,
        )
        .expect("managed requirements stack");
        let mut child_config = parent_config.clone();
        child_config.developer_instructions = Some("Child developer instructions.".to_string());
        child_config.multi_agent_v2.subagent_developer_instructions =
            Some("Child developer instructions.".to_string());

        let new_thread = harness
            .manager
            .start_thread(StartThreadOptions::new(parent_config))
            .await
            .expect("start parent thread");
        let parent_thread_id = new_thread.thread_id;
        let parent_thread = new_thread.thread;
        let turn_context = parent_thread.session.new_default_turn().await;
        let parent_spawn_call_id = match parent_developer_instructions {
            Some(_) => "spawn-call-legacy-compact-with-parent",
            None => "spawn-call-legacy-compact-without-parent",
        };
        let parent_user_message = ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: "parent task before legacy compaction".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        };

        // A live parent can reestablish its baseline after resuming a rollout
        // whose older compaction record cannot restore that baseline to a child.
        parent_thread
            .session
            .replace_history(
                vec![parent_user_message.clone()],
                Some(turn_context.to_turn_context_item()),
            )
            .await;
        let mut rollout_items = vec![
            rollout_response_item(parent_user_message),
            RolloutItem::Compacted(CompactedItem {
                message: "legacy compacted summary".to_string(),
                replacement_history: None,
                mcp_resource_origins: None,
                window_number: None,
                first_window_id: None,
                previous_window_id: None,
                window_id: None,
            }),
        ];
        if let Some(instructions) = parent_developer_instructions {
            rollout_items.push(rollout_response_item(ResponseItem::Message {
                id: None,
                role: "developer".to_string(),
                content: vec![ContentItem::InputText {
                    text: instructions.to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }));
        }
        rollout_items.push(rollout_response_item(ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: stale_managed_fragment.to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        }));
        rollout_items.push(RolloutItem::TurnContext(
            turn_context.to_turn_context_item(),
        ));
        rollout_items.push(rollout_response_item(spawn_agent_call(
            parent_spawn_call_id,
        )));
        parent_thread
            .session
            .persist_rollout_items(&rollout_items)
            .await;
        parent_thread
            .session
            .ensure_rollout_materialized(PersistContext::Standard)
            .await;
        parent_thread
            .session
            .flush_rollout()
            .await
            .expect("parent rollout should flush");

        let child_thread_id = harness
            .control
            .spawn_agent_with_metadata(
                child_config,
                text_input("child task"),
                Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    parent_thread_id,
                    depth: 1,
                    agent_path: None,
                    agent_nickname: None,
                    agent_role: None,
                })),
                SpawnAgentOptions {
                    fork_parent_spawn_call_id: Some(parent_spawn_call_id.to_string()),
                    fork_mode: Some(SpawnAgentForkMode::FullHistory),
                    ..Default::default()
                },
            )
            .await
            .expect("forked spawn should preserve legacy compacted history")
            .thread_id;
        let child_thread = harness
            .manager
            .get_thread(child_thread_id)
            .await
            .expect("child thread should be registered");
        while child_thread
            .session
            .reference_context_item()
            .await
            .is_none()
        {
            tokio::task::yield_now().await;
        }
        let history = child_thread.session.clone_history().await;
        let mut instruction_count = 0;
        let mut managed_instructions = Vec::new();
        for item in history.raw_items() {
            let ResponseItem::Message { role, content, .. } = item else {
                continue;
            };
            if role != "developer" {
                continue;
            }
            for content_item in content {
                if let ContentItem::InputText { text } = content_item {
                    instruction_count += usize::from(text == "Child developer instructions.");
                    if ManagedDeveloperInstructions::matches_text(text) {
                        managed_instructions.push(text.as_str());
                    }
                }
            }
        }
        assert_eq!(
            (instruction_count, managed_instructions),
            (1, vec![current_managed_fragment.as_str()]),
            "{case}: canonical context reconstruction must keep only the current child and managed developer instructions"
        );

        let _ = harness
            .control
            .shutdown_live_agent(child_thread_id)
            .await
            .expect("child shutdown should submit");
        let _ = parent_thread
            .submit(Op::Shutdown {})
            .await
            .expect("parent shutdown should submit");
    }
}

#[tokio::test]
async fn spawn_agent_fork_flushes_parent_rollout_before_loading_history() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let turn_context = parent_thread.session.new_default_turn().await;
    let parent_spawn_call_id = "spawn-call-unflushed".to_string();
    parent_thread
        .session
        .record_conversation_items(
            turn_context.as_ref(),
            &[
                assistant_message("unflushed final answer", Some(MessagePhase::FinalAnswer)),
                spawn_agent_call(&parent_spawn_call_id),
            ],
        )
        .await;

    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            harness.config.clone(),
            text_input("child task"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id.clone()),
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                initial_task_message: Some("child task".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("forked spawn should flush parent rollout before loading history")
        .thread_id;

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let history = child_thread.session.clone_history().await;
    assert!(
        history_contains_text(history.raw_items(), "unflushed final answer"),
        "forked child history should include unflushed assistant final answers after flushing the parent rollout"
    );
    assert_eq!(
        history_text_match_count(history.raw_items(), "# Subagent Assignment"),
        1,
        "forked child history should contain one explicit assignment"
    );

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

#[tokio::test]
async fn reference_backed_fork_persists_assignment_after_settings_across_resume() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread
        .session
        .inject_no_new_turn(
            vec![user_message("parent seed context")],
            /*current_turn_context*/ None,
        )
        .await;
    parent_thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");
    let child_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: None,
        agent_nickname: None,
        agent_role: None,
    });
    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            harness.config.clone(),
            text_input("child task"),
            Some(child_source.clone()),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some("synthetic-spawn-call".to_string()),
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                initial_task_message: Some("child task".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("reference-backed fork should spawn")
        .thread_id;
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    child_thread.ensure_rollout_materialized().await;
    child_thread
        .flush_rollout()
        .await
        .expect("child rollout should flush");

    let physical_items = RolloutRecorder::load_rollout_items(
        child_thread
            .rollout_path()
            .expect("child rollout should exist")
            .as_path(),
    )
    .await
    .expect("load child rollout")
    .0;
    let reference_index = physical_items
        .iter()
        .position(|item| matches!(item, RolloutItem::RolloutReference(_)))
        .expect("child should preserve the parent reference");
    let settings_index = physical_items
        .iter()
        .position(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(_))
            )
        })
        .expect("child should persist effective settings");
    let assignment_index = physical_items
        .iter()
        .position(|item| {
            matches!(
                item,
                RolloutItem::ResponseItem(envelope)
                    if history_contains_text([&envelope.item], "# Subagent Assignment")
            )
        })
        .expect("child should persist its assignment");
    assert!(reference_index < settings_index && settings_index < assignment_index);
    let live_history = child_thread.session.clone_history().await;
    assert_eq!(
        history_text_match_count(live_history.raw_items(), "# Subagent Assignment"),
        1
    );

    harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    harness
        .control
        .resume_agent_from_rollout(harness.config.clone(), child_thread_id, child_source)
        .await
        .expect("child should resume");
    let resumed_child = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("resumed child should be registered");
    let resumed_history = resumed_child.session.clone_history().await;
    assert_eq!(
        history_text_match_count(resumed_history.raw_items(), "# Subagent Assignment"),
        1,
        "cold resume should retain exactly one explicit assignment"
    );

    let _ = harness.control.shutdown_live_agent(child_thread_id).await;
    let _ = parent_thread.submit(Op::Shutdown {}).await;
}

#[tokio::test]
async fn spawn_agent_fork_last_n_turns_keeps_only_recent_turns() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;

    parent_thread
        .inject_response_items(vec![user_message("old parent context")])
        .await
        .expect("inject old parent context");
    let queued_communication = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::try_from("/root/worker").expect("agent path"),
        Vec::new(),
        "queued message".to_string(),
        /*trigger_turn*/ false,
    );
    let queued_turn_context = parent_thread.session.new_default_turn().await;
    parent_thread
        .session
        .record_conversation_items(
            queued_turn_context.as_ref(),
            &[queued_communication.to_response_input_item().into()],
        )
        .await;

    let triggered_communication = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::try_from("/root/worker").expect("agent path"),
        Vec::new(),
        "triggered context".to_string(),
        /*trigger_turn*/ true,
    );
    let triggered_turn_context = parent_thread.session.new_default_turn().await;
    parent_thread
        .session
        .record_conversation_items(
            triggered_turn_context.as_ref(),
            &[triggered_communication.to_response_input_item().into()],
        )
        .await;
    parent_thread
        .inject_response_items(vec![user_message("current parent task")])
        .await
        .expect("inject current parent task");
    let spawn_turn_context = parent_thread.session.new_default_turn().await;
    let parent_spawn_call_id = "spawn-call-last-n".to_string();
    parent_thread
        .session
        .record_conversation_items(
            spawn_turn_context.as_ref(),
            &[spawn_agent_call(&parent_spawn_call_id)],
        )
        .await;
    parent_thread
        .session
        .persist_rollout_items(&[RolloutItem::TurnContext(
            spawn_turn_context.to_turn_context_item(),
        )])
        .await;
    parent_thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");

    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            harness.config.clone(),
            text_input("child task"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id.clone()),
                fork_mode: Some(SpawnAgentForkMode::LastNTurns(2)),
                ..Default::default()
            },
        )
        .await
        .expect("forked spawn should keep only the last two turns")
        .thread_id;

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let history = child_thread.session.clone_history().await;

    assert!(
        !history_contains_text(history.raw_items(), "old parent context"),
        "forked child history should drop parent context outside the requested last-N turn window"
    );
    assert!(
        !history_contains_text(history.raw_items(), "queued message"),
        "forked child history should drop queued inter-agent messages outside the requested last-N turn window"
    );
    assert!(
        !history_contains_text(history.raw_items(), "triggered context"),
        "forked child history should filter assistant inter-agent messages even when they fall inside the requested last-N turn window"
    );
    assert!(
        history_contains_text(history.raw_items(), "current parent task"),
        "forked child history should keep the parent user message from the requested last-N turn window"
    );
    assert!(
        child_thread
            .session
            .reference_context_item()
            .await
            .is_none(),
        "last-N forked child should rebuild context after truncating the cached prefix"
    );

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

#[tokio::test]
async fn spawn_agent_fork_last_n_turns_drops_parent_startup_prefix_when_under_limit() {
    let harness = AgentControlHarness::new().await;
    let selected_capability_roots = vec![SelectedCapabilityRoot {
        id: "demo@1".to_string(),
        location: CapabilityRootLocation::Environment {
            environment_id: "build".to_string(),
            path: PathUri::parse("file:///plugins/demo").expect("plugin root URI"),
        },
    }];
    let mut thread_extension_init = ExtensionDataInit::new();
    thread_extension_init.insert(selected_capability_roots.clone());
    let parent = harness
        .manager
        .start_thread(StartThreadOptions {
            environments: Some(Vec::new()),
            thread_extension_init,
            ..StartThreadOptions::new(harness.config.clone())
        })
        .await
        .expect("start parent thread");
    let parent_thread_id = parent.thread_id;
    let parent_thread = parent.thread;
    let startup_turn_context = parent_thread.session.new_default_turn().await;
    parent_thread
        .session
        .record_conversation_items(
            startup_turn_context.as_ref(),
            &[ResponseItem::Message {
                id: None,
                role: "developer".to_string(),
                content: vec![ContentItem::InputText {
                    text: "parent startup developer context".to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }],
        )
        .await;
    parent_thread
        .inject_response_items(vec![user_message("current parent task")])
        .await
        .expect("inject current parent task");
    let spawn_turn_context = parent_thread.session.new_default_turn().await;
    let parent_spawn_call_id = "spawn-call-last-n-under-limit".to_string();
    parent_thread
        .session
        .record_conversation_items(
            spawn_turn_context.as_ref(),
            &[spawn_agent_call(&parent_spawn_call_id)],
        )
        .await;
    parent_thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");

    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            harness.config.clone(),
            text_input("child task"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id),
                fork_mode: Some(SpawnAgentForkMode::LastNTurns(2)),
                ..Default::default()
            },
        )
        .await
        .expect("bounded forked spawn should drop startup prefix")
        .thread_id;

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let history = child_thread.session.clone_history().await;
    assert!(
        history_contains_text(history.raw_items(), "current parent task"),
        "bounded fork should retain the requested recent parent turn"
    );
    assert!(
        !history_contains_text(history.raw_items(), "parent startup developer context"),
        "bounded fork should drop parent startup context even when fewer turns exist than requested"
    );
    assert_eq!(
        &child_thread.session.services.selected_capability_roots,
        &selected_capability_roots
    );
    assert!(
        child_thread
            .session
            .reference_context_item()
            .await
            .is_none(),
        "bounded forked child should still rebuild context after truncating the cached prefix"
    );

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

#[tokio::test]
async fn spawn_agent_fork_last_n_turns_strips_parent_usage_hints() {
    let persistent_fragment =
        "<persistent_mode>\nParent persistent instructions.\n</persistent_mode>";
    let harness = AgentControlHarness::new().await;
    let mut parent_config = harness.config.clone();
    let _ = parent_config.features.enable(Feature::MultiAgentV2);
    parent_config.developer_instructions = Some("Parent developer instructions.".to_string());
    parent_config.multi_agent_v2.root_agent_usage_hint_text =
        Some("Parent root guidance.".to_string());
    let mut child_config = harness.config.clone();
    let _ = child_config.features.enable(Feature::MultiAgentV2);
    child_config.developer_instructions = Some("Child developer instructions.".to_string());
    child_config.multi_agent_v2.subagent_developer_instructions =
        Some("Child developer instructions.".to_string());
    child_config.multi_agent_v2.subagent_usage_hint_text =
        Some("Child subagent guidance.".to_string());
    let new_thread = harness
        .manager
        .start_thread(StartThreadOptions::new(parent_config))
        .await
        .expect("start parent thread");
    let parent_thread_id = new_thread.thread_id;
    let parent_thread = new_thread.thread;
    parent_thread
        .inject_response_items(vec![user_message("parent task")])
        .await
        .expect("inject parent task");
    let turn_context = parent_thread.session.new_default_turn().await;
    let parent_spawn_call_id = "spawn-call-last-n-usage-hints".to_string();
    parent_thread
        .session
        .record_conversation_items(
            turn_context.as_ref(),
            &[
                ResponseItem::Message {
                    id: None,
                    role: "developer".to_string(),
                    content: vec![ContentItem::InputText {
                        text: "Parent root guidance.".to_string(),
                    }],
                    phase: None,
                    internal_chat_message_metadata_passthrough: None,
                },
                ResponseItem::Message {
                    id: None,
                    role: "developer".to_string(),
                    content: vec![
                        ContentItem::InputText {
                            text: "Parent developer instructions.".to_string(),
                        },
                        ContentItem::InputText {
                            text: "Preserved bounded developer context.".to_string(),
                        },
                        ContentItem::InputText {
                            text: persistent_fragment.to_string(),
                        },
                    ],
                    phase: None,
                    internal_chat_message_metadata_passthrough: None,
                },
                spawn_agent_call(&parent_spawn_call_id),
            ],
        )
        .await;
    parent_thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");

    let child_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            child_config,
            text_input("child task"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            })),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some(parent_spawn_call_id),
                fork_mode: Some(SpawnAgentForkMode::LastNTurns(2)),
                ..Default::default()
            },
        )
        .await
        .expect("bounded forked spawn should sanitize parent usage hints")
        .thread_id;

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let history = child_thread.session.clone_history().await;
    assert!(
        history_contains_text(history.raw_items(), "parent task"),
        "bounded fork should retain the requested recent parent turn"
    );
    assert!(
        !history_contains_text(history.raw_items(), "Parent root guidance."),
        "bounded fork should strip stale parent root hints before the child rebuilds startup context"
    );
    assert!(
        !history_contains_text(history.raw_items(), "Parent developer instructions."),
        "bounded fork should remove parent instructions before the child rebuilds startup context"
    );
    assert!(
        !history_contains_text(history.raw_items(), "Child developer instructions."),
        "bounded fork should not inject child instructions before its canonical context rebuild"
    );
    assert!(
        !history_contains_text(history.raw_items(), persistent_fragment),
        "bounded fork should remove persistent instructions before rebuilding context for the child's effort"
    );
    assert!(
        history_contains_text(history.raw_items(), "Preserved bounded developer context."),
        "bounded fork should preserve unrelated developer fragments"
    );

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

#[tokio::test]
async fn spawn_agent_respects_legacy_max_threads_alias() {
    let max_threads = 1usize;
    let (_home, config) = test_config_with_cli_overrides(vec![(
        "agents.max_threads".to_string(),
        TomlValue::Integer(max_threads as i64),
    )])
    .await;
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let control = manager.agent_control();

    let _ = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("start thread");

    let first_agent_id = control
        .spawn_agent(
            config.clone(),
            text_input("hello"),
            /*session_source*/ None,
        )
        .await
        .expect("spawn_agent should succeed");

    let err = control
        .spawn_agent(
            config,
            text_input("hello again"),
            /*session_source*/ None,
        )
        .await
        .expect_err("spawn_agent should respect max threads");
    let CodexErrorDetails::AgentLimitReached {
        max_threads: seen_max_threads,
    } = err.details()
    else {
        panic!("expected AgentLimitReached");
    };
    assert_eq!(*seen_max_threads, max_threads);

    let _ = control
        .shutdown_live_agent(first_agent_id)
        .await
        .expect("shutdown agent");
}

#[tokio::test]
async fn spawn_agent_releases_slot_after_shutdown() {
    let max_threads = 1usize;
    let (_home, config) = test_config_with_cli_overrides(vec![(
        "agents.max_concurrent_threads_per_session".to_string(),
        TomlValue::Integer(max_threads as i64),
    )])
    .await;
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let control = manager.agent_control();

    let first_agent_id = control
        .spawn_agent(
            config.clone(),
            text_input("hello"),
            /*session_source*/ None,
        )
        .await
        .expect("spawn_agent should succeed");
    let _ = control
        .shutdown_live_agent(first_agent_id)
        .await
        .expect("shutdown agent");

    let second_agent_id = control
        .spawn_agent(
            config.clone(),
            text_input("hello again"),
            /*session_source*/ None,
        )
        .await
        .expect("spawn_agent should succeed after shutdown");
    let _ = control
        .shutdown_live_agent(second_agent_id)
        .await
        .expect("shutdown agent");
}

#[tokio::test]
async fn spawn_agent_limit_shared_across_clones() {
    let max_threads = 1usize;
    let (_home, config) = test_config_with_cli_overrides(vec![(
        "agents.max_concurrent_threads_per_session".to_string(),
        TomlValue::Integer(max_threads as i64),
    )])
    .await;
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let control = manager.agent_control();
    let cloned = control.clone();

    let first_agent_id = cloned
        .spawn_agent(
            config.clone(),
            text_input("hello"),
            /*session_source*/ None,
        )
        .await
        .expect("spawn_agent should succeed");

    let err = control
        .spawn_agent(
            config,
            text_input("hello again"),
            /*session_source*/ None,
        )
        .await
        .expect_err("spawn_agent should respect shared guard");
    let CodexErrorDetails::AgentLimitReached { max_threads } = err.details() else {
        panic!("expected AgentLimitReached");
    };
    assert_eq!(*max_threads, 1);

    let _ = control
        .shutdown_live_agent(first_agent_id)
        .await
        .expect("shutdown agent");
}

#[tokio::test]
async fn resume_agent_respects_max_threads_limit() {
    let max_threads = 1usize;
    let (_home, config) = test_config_with_cli_overrides(vec![(
        "agents.max_concurrent_threads_per_session".to_string(),
        TomlValue::Integer(max_threads as i64),
    )])
    .await;
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let control = manager.agent_control();

    let resumable_id = control
        .spawn_agent(
            config.clone(),
            text_input("hello"),
            /*session_source*/ None,
        )
        .await
        .expect("spawn_agent should succeed");
    let _ = control
        .shutdown_live_agent(resumable_id)
        .await
        .expect("shutdown resumable thread");

    let active_id = control
        .spawn_agent(
            config.clone(),
            text_input("occupy"),
            /*session_source*/ None,
        )
        .await
        .expect("spawn_agent should succeed for active slot");

    let err = control
        .resume_agent_from_rollout(config, resumable_id, SessionSource::Exec)
        .await
        .expect_err("resume should respect max threads");
    let CodexErrorDetails::AgentLimitReached {
        max_threads: seen_max_threads,
    } = err.details()
    else {
        panic!("expected AgentLimitReached");
    };
    assert_eq!(*seen_max_threads, max_threads);

    let _ = control
        .shutdown_live_agent(active_id)
        .await
        .expect("shutdown active thread");
}

#[tokio::test]
async fn resume_agent_releases_slot_after_resume_failure() {
    let max_threads = 1usize;
    let (_home, config) = test_config_with_cli_overrides(vec![(
        "agents.max_concurrent_threads_per_session".to_string(),
        TomlValue::Integer(max_threads as i64),
    )])
    .await;
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let control = manager.agent_control();

    let _ = control
        .resume_agent_from_rollout(config.clone(), ThreadId::new(), SessionSource::Exec)
        .await
        .expect_err("resume should fail for missing rollout path");

    let resumed_id = control
        .spawn_agent(config, text_input("hello"), /*session_source*/ None)
        .await
        .expect("spawn should succeed after failed resume");
    let _ = control
        .shutdown_live_agent(resumed_id)
        .await
        .expect("shutdown resumed thread");
}

#[tokio::test]
async fn spawn_child_completion_notifies_parent_history() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    let _ = child_thread
        .submit(Op::Shutdown {})
        .await
        .expect("child shutdown should submit");

    assert_eq!(wait_for_subagent_notification(&parent_thread).await, true);
}

#[tokio::test]
async fn multi_agent_v2_completion_ignores_dead_direct_parent() {
    let harness = AgentControlHarness::new().await;
    let mut config = harness.config.clone();
    let _ = config.features.enable(Feature::MultiAgentV2);
    let root = harness
        .manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("root thread should start");
    let root_thread_id = root.thread_id;
    let root_thread = root.thread;
    let worker_path = AgentPath::root().join("worker_a").expect("worker path");
    let worker_thread_id = harness
        .control
        .spawn_agent(
            config.clone(),
            text_input("hello worker"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: root_thread_id,
                depth: 1,
                agent_path: Some(worker_path.clone()),
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("worker spawn should succeed");
    let tester_path = worker_path.join("tester").expect("tester path");
    let tester_thread_id = harness
        .control
        .spawn_agent(
            config,
            text_input("hello tester"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: worker_thread_id,
                depth: 2,
                agent_path: Some(tester_path.clone()),
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("tester spawn should succeed");
    harness
        .control
        .shutdown_live_agent(worker_thread_id)
        .await
        .expect("worker shutdown should succeed");

    let tester_thread = harness
        .manager
        .get_thread(tester_thread_id)
        .await
        .expect("tester thread should exist");
    let tester_turn = tester_thread.session.new_default_turn().await;
    tester_thread
        .session
        .send_event(
            tester_turn.as_ref(),
            EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: tester_turn.sub_id.clone(),
                started_at: None,
                last_agent_message: Some("done".to_string()),
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        )
        .await;

    sleep(Duration::from_millis(100)).await;

    assert!(
        !harness
            .manager
            .captured_ops()
            .into_iter()
            .any(|(thread_id, op)| {
                thread_id == worker_thread_id
                    && matches!(
                        op,
                        Op::InterAgentCommunication { communication, .. }
                            if communication.author == tester_path
                                && communication.recipient == worker_path
                                && communication.content == "done"
                    )
            })
    );

    let root_history = root_thread.session.clone_history().await;
    assert!(!history_contains_assistant_inter_agent_communication(
        root_history.raw_items(),
        &InterAgentCommunication::new(
            tester_path,
            AgentPath::root(),
            Vec::new(),
            "done".to_string(),
            /*trigger_turn*/ true,
        )
    ));
    assert!(!has_subagent_notification(root_history.raw_items()));
}

#[tokio::test]
async fn multi_agent_v2_completion_queues_message_for_direct_parent() {
    let harness = AgentControlHarness::new().await;
    let (_root_thread_id, root_thread) = harness.start_thread().await;
    let (worker_thread_id, _worker_thread) = harness.start_thread().await;
    let mut tester_config = harness.config.clone();
    let _ = tester_config.features.enable(Feature::MultiAgentV2);
    let tester_thread_id = harness
        .manager
        .start_thread(StartThreadOptions::new(tester_config.clone()))
        .await
        .expect("tester thread should start")
        .thread_id;
    let tester_thread = harness
        .manager
        .get_thread(tester_thread_id)
        .await
        .expect("tester thread should exist");
    let worker_path = AgentPath::root().join("worker_a").expect("worker path");
    let tester_path = worker_path.join("tester").expect("tester path");
    harness.control.maybe_start_completion_watcher(
        tester_thread_id,
        Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: worker_thread_id,
            depth: 2,
            agent_path: Some(tester_path.clone()),
            agent_nickname: None,
            agent_role: Some("explorer".to_string()),
        })),
        tester_path.to_string(),
        Some(tester_path.clone()),
    );
    let tester_turn = tester_thread.session.new_default_turn().await;
    tester_thread
        .session
        .send_event(
            tester_turn.as_ref(),
            EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: tester_turn.sub_id.clone(),
                started_at: None,
                last_agent_message: Some("done".to_string()),
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        )
        .await;

    let expected_message = crate::session_prefix::format_inter_agent_completion_message(
        worker_path.clone(),
        tester_path.clone(),
        &AgentStatus::Completed(Some("done".to_string())),
    )
    .expect("completed status should render");
    let expected = (
        worker_thread_id,
        Op::InterAgentCommunication {
            communication: InterAgentCommunication::new(
                tester_path.clone(),
                worker_path.clone(),
                Vec::new(),
                expected_message.clone(),
                /*trigger_turn*/ false,
            ),
            start_options: Default::default(),
        },
    );

    timeout(Duration::from_secs(5), async {
        loop {
            let captured = harness
                .manager
                .captured_ops()
                .into_iter()
                .find(|entry| captured_op_matches(entry, &expected));
            if captured.is_some() {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("completion watcher should queue a direct-parent message");

    let root_history = root_thread.session.clone_history().await;
    assert!(!history_contains_assistant_inter_agent_communication(
        root_history.raw_items(),
        &InterAgentCommunication::new(
            tester_path,
            AgentPath::root(),
            Vec::new(),
            expected_message,
            /*trigger_turn*/ false,
        )
    ));
}

#[tokio::test]
async fn completion_watcher_notifies_parent_when_child_is_missing() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let child_thread_id = ThreadId::new();

    harness.control.maybe_start_completion_watcher(
        child_thread_id,
        Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id,
            depth: 1,
            agent_path: None,
            agent_nickname: None,
            agent_role: Some("explorer".to_string()),
        })),
        child_thread_id.to_string(),
        /*child_agent_path*/ None,
    );

    assert_eq!(wait_for_subagent_notification(&parent_thread).await, true);

    let history = parent_thread.session.clone_history().await;
    assert_eq!(
        history_contains_text(
            history.raw_items(),
            &format!("\"agent_path\":\"{child_thread_id}\"")
        ),
        true
    );
    assert_eq!(
        history_contains_text(history.raw_items(), "\"status\":\"not_found\""),
        true
    );
}

#[tokio::test]
async fn spawn_thread_subagent_gets_random_nickname_in_session_source() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let snapshot = child_thread.config_snapshot().await;

    let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: seen_parent_thread_id,
        depth,
        agent_nickname,
        agent_role,
        ..
    }) = snapshot.session_source
    else {
        panic!("expected thread-spawn sub-agent source");
    };
    assert_eq!(seen_parent_thread_id, parent_thread_id);
    assert_eq!(depth, 1);
    assert!(agent_nickname.is_some());
    assert_eq!(agent_role, Some("explorer".to_string()));
}

#[tokio::test]
async fn spawn_thread_subagents_persist_parent_originator_across_new_and_truncated_fork() {
    let harness = AgentControlHarness::new().await;
    let parent = harness
        .manager
        .start_thread(StartThreadOptions {
            metrics_service_name: Some("codex_work_desktop".to_string()),
            environments: Some(Vec::new()),
            ..StartThreadOptions::new(harness.config.clone())
        })
        .await
        .expect("parent thread should start");
    let parent_originator = persisted_originator(&parent.thread).await;
    assert_eq!(parent_originator, "codex_work_desktop");

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: parent.thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let child_originator = persisted_originator(&child_thread).await;
    assert_eq!(child_originator, parent_originator);

    let child = harness
        .control
        .spawn_agent_with_metadata(
            harness.config.clone(),
            text_input("hello forked child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: parent.thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: Some("spawn-call-last-n".to_string()),
                fork_mode: Some(SpawnAgentForkMode::LastNTurns(1)),
                ..Default::default()
            },
        )
        .await
        .expect("forked child spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child.thread_id)
        .await
        .expect("child thread should be registered");
    let child_originator = persisted_originator(&child_thread).await;
    assert_eq!(child_originator, parent_originator);
}

#[tokio::test]
async fn spawn_thread_subagent_uses_role_specific_nickname_candidates() {
    let mut harness = AgentControlHarness::new().await;
    harness.config.agent_roles.insert(
        "researcher".to_string(),
        AgentRoleConfig {
            description: Some("Research role".to_string()),
            config_file: None,
            nickname_candidates: Some(vec!["Atlas".to_string()]),
        },
    );
    let (parent_thread_id, _parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("researcher".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let snapshot = child_thread.config_snapshot().await;

    let SessionSource::SubAgent(SubAgentSource::ThreadSpawn { agent_nickname, .. }) =
        snapshot.session_source
    else {
        panic!("expected thread-spawn sub-agent source");
    };
    assert_eq!(agent_nickname, Some("Atlas".to_string()));
}

#[tokio::test]
async fn resume_thread_subagent_restores_stored_metadata() {
    let (home, config) = test_config().await;
    let thread_store = Arc::new(InMemoryThreadStore::default());
    let auth_manager = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("dummy"));
    let manager = ThreadManager::new(
        &config,
        auth_manager.clone(),
        crate::thread_manager::build_models_manager(&config, auth_manager),
        crate::CodexAppsToolsCache::default(),
        SessionSource::Exec,
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        empty_extension_registry(),
        Arc::new(crate::test_support::EmptyUserInstructionsProvider),
        /*analytics_events_client*/ None,
        thread_store.clone(),
        /*agent_graph_store*/ None,
        uuid::Uuid::new_v4().to_string(),
        /*attestation_provider*/ None,
        /*external_time_provider*/ None,
    );
    let control = manager.agent_control();
    let harness = AgentControlHarness {
        _home: home,
        config,
        state_db: None,
        manager,
        control,
    };
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let agent_path = AgentPath::from_string("/root/explorer".to_string())
        .expect("test agent path should be valid");

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(agent_path.clone()),
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    child_thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    child_thread
        .session
        .flush_rollout()
        .await
        .expect("flush child rollout");
    let mut status_rx = harness
        .control
        .subscribe_status(child_thread_id)
        .await
        .expect("status subscription should succeed");
    if matches!(status_rx.borrow().clone(), AgentStatus::PendingInit) {
        timeout(Duration::from_secs(5), async {
            loop {
                status_rx
                    .changed()
                    .await
                    .expect("child status should advance past pending init");
                if !matches!(status_rx.borrow().clone(), AgentStatus::PendingInit) {
                    break;
                }
            }
        })
        .await
        .expect("child should initialize before shutdown");
    }
    let original_snapshot = child_thread.config_snapshot().await;
    let original_nickname = original_snapshot
        .session_source
        .get_nickname()
        .expect("spawned sub-agent should have a nickname");
    timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(stored_thread) = thread_store
                .read_thread(ReadThreadParams {
                    thread_id: child_thread_id,
                    include_archived: true,
                    include_history: false,
                })
                .await
                && stored_thread.agent_nickname.is_some()
                && stored_thread.agent_role.as_deref() == Some("explorer")
                && stored_thread.agent_path.as_deref() == Some(agent_path.as_str())
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("child thread metadata should be persisted to sqlite before shutdown");

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should submit");

    let resumed_thread_id = harness
        .control
        .resume_agent_from_rollout(
            harness.config.clone(),
            child_thread_id,
            SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            }),
        )
        .await
        .expect("resume should succeed");
    assert_eq!(resumed_thread_id, child_thread_id);

    let resumed_thread = harness
        .manager
        .get_thread(resumed_thread_id)
        .await
        .expect("resumed child thread should exist");
    assert_eq!(
        resumed_thread.session.prompt_cache_key(),
        resumed_thread_id,
        "resume should keep the resumed thread's own cache key"
    );
    assert_ne!(
        resumed_thread.session.prompt_cache_key(),
        parent_thread.session.prompt_cache_key(),
        "resume must not opportunistically inherit cache state from a live parent"
    );
    let resumed_snapshot = resumed_thread.config_snapshot().await;
    let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: resumed_parent_thread_id,
        depth: resumed_depth,
        agent_path: resumed_agent_path,
        agent_nickname: resumed_nickname,
        agent_role: resumed_role,
        ..
    }) = resumed_snapshot.session_source
    else {
        panic!("expected thread-spawn sub-agent source");
    };
    assert_eq!(resumed_parent_thread_id, parent_thread_id);
    assert_eq!(resumed_depth, 1);
    assert_eq!(resumed_agent_path, Some(agent_path));
    assert_eq!(resumed_nickname, Some(original_nickname));
    assert_eq!(resumed_role, Some("explorer".to_string()));

    let _ = harness
        .control
        .shutdown_live_agent(resumed_thread_id)
        .await
        .expect("resumed child shutdown should submit");
}

#[tokio::test]
async fn resume_agent_from_rollout_reads_archived_rollout_path() {
    let harness = AgentControlHarness::new().await;
    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello"),
            /*session_source*/ None,
        )
        .await
        .expect("child spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    persist_thread_for_tree_resume(&child_thread, "persist before archiving").await;
    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should succeed");
    let store = LocalThreadStore::new(
        LocalThreadStoreConfig::from_config(&harness.config),
        harness.state_db.clone(),
    );
    store
        .archive_thread(ArchiveThreadParams {
            thread_id: child_thread_id,
        })
        .await
        .expect("child thread should archive");

    let resumed_thread_id = harness
        .control
        .resume_agent_from_rollout(harness.config.clone(), child_thread_id, SessionSource::Exec)
        .await
        .expect("resume should find archived rollout");
    assert_eq!(resumed_thread_id, child_thread_id);

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("resumed child shutdown should succeed");
}

#[tokio::test]
async fn resume_agent_from_paginated_rollout_loads_model_context() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_paginated_thread().await;
    let child_thread_id = harness
        .spawn_anonymous_child(
            parent_thread_id,
            SpawnAgentOptions {
                parent_thread_id: Some(parent_thread_id),
                ..Default::default()
            },
        )
        .await;
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    assert_eq!(
        child_thread.config_snapshot().await.history_mode,
        ThreadHistoryMode::Paginated
    );
    persist_thread_for_tree_resume(&child_thread, "persist before resume").await;
    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("child shutdown should succeed");

    let resumed_thread_id = harness
        .control
        .resume_agent_from_rollout(harness.config.clone(), child_thread_id, SessionSource::Exec)
        .await
        .expect("resume should load paginated model context");
    assert_eq!(resumed_thread_id, child_thread_id);
    let resumed_thread = harness
        .manager
        .get_thread(resumed_thread_id)
        .await
        .expect("resumed child thread should exist");
    assert!(
        history_contains_text(
            resumed_thread.session.clone_history().await.raw_items(),
            "persist before resume",
        ),
        "resumed child should keep its persisted model context"
    );

    let _ = harness
        .control
        .shutdown_live_agent(child_thread_id)
        .await
        .expect("resumed child shutdown should succeed");
    let _ = parent_thread
        .submit(Op::Shutdown {})
        .await
        .expect("parent shutdown should submit");
}

#[tokio::test]
async fn list_agent_subtree_thread_ids_includes_anonymous_and_closed_descendants() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _parent_thread) = harness.start_thread().await;
    let worker_path = AgentPath::root().join("worker").expect("worker path");
    let reviewer_path = AgentPath::root().join("reviewer").expect("reviewer path");

    let worker_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello worker"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(worker_path.clone()),
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("worker spawn should succeed");
    let worker_child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello worker child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: worker_thread_id,
                depth: 2,
                agent_path: Some(
                    worker_path
                        .join("child")
                        .expect("worker child path should be valid"),
                ),
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("worker child spawn should succeed");
    let no_path_child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello anonymous child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: worker_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("no-path child spawn should succeed");
    let no_path_grandchild_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello anonymous grandchild"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: no_path_child_thread_id,
                depth: 3,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("no-path grandchild spawn should succeed");
    let _reviewer_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello reviewer"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(reviewer_path),
                agent_nickname: None,
                agent_role: Some("reviewer".to_string()),
            })),
        )
        .await
        .expect("reviewer spawn should succeed");

    let _ = harness
        .control
        .shutdown_live_agent(no_path_grandchild_thread_id)
        .await
        .expect("no-path grandchild shutdown should succeed");

    let mut worker_subtree_thread_ids = harness
        .manager
        .list_agent_subtree_thread_ids(worker_thread_id)
        .await
        .expect("worker subtree thread ids should load");
    worker_subtree_thread_ids.sort_by_key(ToString::to_string);
    let mut expected_worker_subtree_thread_ids = vec![
        worker_thread_id,
        worker_child_thread_id,
        no_path_child_thread_id,
        no_path_grandchild_thread_id,
    ];
    expected_worker_subtree_thread_ids.sort_by_key(ToString::to_string);
    assert_eq!(
        worker_subtree_thread_ids,
        expected_worker_subtree_thread_ids
    );

    let mut no_path_child_subtree_thread_ids = harness
        .manager
        .list_agent_subtree_thread_ids(no_path_child_thread_id)
        .await
        .expect("no-path subtree thread ids should load");
    no_path_child_subtree_thread_ids.sort_by_key(ToString::to_string);
    let mut expected_no_path_child_subtree_thread_ids =
        vec![no_path_child_thread_id, no_path_grandchild_thread_id];
    expected_no_path_child_subtree_thread_ids.sort_by_key(ToString::to_string);
    assert_eq!(
        no_path_child_subtree_thread_ids,
        expected_no_path_child_subtree_thread_ids
    );
}

#[tokio::test]
async fn list_agent_subtree_thread_ids_finds_live_descendants_of_unloaded_root() {
    let (_home, config) = test_config().await;
    let manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        std::sync::Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        /*state_db*/ None,
    );
    let control = manager.agent_control();
    let parent_thread_id = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("parent should start")
        .thread_id;

    let child_thread_id = control
        .spawn_agent(
            config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");
    let grandchild_thread_id = control
        .spawn_agent(
            config,
            text_input("hello grandchild"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("grandchild spawn should succeed");

    manager.remove_thread(&parent_thread_id).await;

    let mut subtree_thread_ids = manager
        .list_agent_subtree_thread_ids(parent_thread_id)
        .await
        .expect("live subtree should load");
    subtree_thread_ids.sort_by_key(ToString::to_string);
    let mut expected_subtree_thread_ids =
        vec![parent_thread_id, child_thread_id, grandchild_thread_id];
    expected_subtree_thread_ids.sort_by_key(ToString::to_string);

    assert_eq!(subtree_thread_ids, expected_subtree_thread_ids);
}

#[tokio::test]
async fn shutdown_agent_tree_closes_live_descendants() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");
    let grandchild_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello grandchild"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("grandchild spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    let grandchild_thread = harness
        .manager
        .get_thread(grandchild_thread_id)
        .await
        .expect("grandchild thread should exist");
    persist_thread_for_tree_resume(&child_thread, "child persisted").await;
    persist_thread_for_tree_resume(&grandchild_thread, "grandchild persisted").await;
    wait_for_live_thread_spawn_children(&harness.control, parent_thread_id, &[child_thread_id])
        .await;
    wait_for_live_thread_spawn_children(&harness.control, child_thread_id, &[grandchild_thread_id])
        .await;

    let _ = harness
        .control
        .shutdown_agent_tree(parent_thread_id)
        .await
        .expect("tree shutdown should succeed");

    assert_eq!(
        harness.control.get_status(parent_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(child_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(grandchild_thread_id).await,
        AgentStatus::NotFound
    );

    let shutdown_ids = harness
        .manager
        .captured_ops()
        .into_iter()
        .filter_map(|(thread_id, op)| matches!(op, Op::Shutdown).then_some(thread_id))
        .collect::<Vec<_>>();
    let mut expected_shutdown_ids = vec![parent_thread_id, child_thread_id, grandchild_thread_id];
    expected_shutdown_ids.sort_by_key(std::string::ToString::to_string);
    let mut shutdown_ids = shutdown_ids;
    shutdown_ids.sort_by_key(std::string::ToString::to_string);
    assert_eq!(shutdown_ids, expected_shutdown_ids);
}

#[tokio::test]
async fn shutdown_agent_tree_closes_descendants_when_started_at_child() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");
    let grandchild_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello grandchild"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("grandchild spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    let grandchild_thread = harness
        .manager
        .get_thread(grandchild_thread_id)
        .await
        .expect("grandchild thread should exist");
    persist_thread_for_tree_resume(&child_thread, "child persisted").await;
    persist_thread_for_tree_resume(&grandchild_thread, "grandchild persisted").await;
    wait_for_live_thread_spawn_children(&harness.control, parent_thread_id, &[child_thread_id])
        .await;
    wait_for_live_thread_spawn_children(&harness.control, child_thread_id, &[grandchild_thread_id])
        .await;

    let _ = harness
        .control
        .close_agent(child_thread_id)
        .await
        .expect("child close should succeed");

    let _ = harness
        .control
        .shutdown_agent_tree(parent_thread_id)
        .await
        .expect("tree shutdown should succeed");

    assert_eq!(
        harness.control.get_status(child_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(grandchild_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(parent_thread_id).await,
        AgentStatus::NotFound
    );

    let shutdown_ids = harness
        .manager
        .captured_ops()
        .into_iter()
        .filter_map(|(thread_id, op)| matches!(op, Op::Shutdown).then_some(thread_id))
        .collect::<Vec<_>>();
    let mut expected_shutdown_ids = vec![parent_thread_id, child_thread_id, grandchild_thread_id];
    expected_shutdown_ids.sort_by_key(std::string::ToString::to_string);
    let mut shutdown_ids = shutdown_ids;
    shutdown_ids.sort_by_key(std::string::ToString::to_string);
    assert_eq!(shutdown_ids, expected_shutdown_ids);
}

#[tokio::test]
async fn resume_agent_from_rollout_does_not_reopen_closed_descendants() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");
    let grandchild_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello grandchild"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("grandchild spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    let grandchild_thread = harness
        .manager
        .get_thread(grandchild_thread_id)
        .await
        .expect("grandchild thread should exist");
    persist_thread_for_tree_resume(&parent_thread, "parent persisted").await;
    persist_thread_for_tree_resume(&child_thread, "child persisted").await;
    persist_thread_for_tree_resume(&grandchild_thread, "grandchild persisted").await;
    wait_for_live_thread_spawn_children(&harness.control, parent_thread_id, &[child_thread_id])
        .await;
    wait_for_live_thread_spawn_children(&harness.control, child_thread_id, &[grandchild_thread_id])
        .await;

    let _ = harness
        .control
        .close_agent(child_thread_id)
        .await
        .expect("child close should succeed");
    let _ = harness
        .control
        .shutdown_live_agent(parent_thread_id)
        .await
        .expect("parent shutdown should succeed");

    let resumed_parent_thread_id = harness
        .control
        .resume_agent_from_rollout(
            harness.config.clone(),
            parent_thread_id,
            SessionSource::Exec,
        )
        .await
        .expect("single-thread resume should succeed");
    assert_eq!(resumed_parent_thread_id, parent_thread_id);
    assert_ne!(
        harness.control.get_status(parent_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(child_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(grandchild_thread_id).await,
        AgentStatus::NotFound
    );

    let _ = harness
        .control
        .shutdown_agent_tree(parent_thread_id)
        .await
        .expect("tree shutdown after resume should succeed");
}

#[test]
fn goal_supervisor_spawn_reconciles_stale_persisted_state() {
    run_goal_supervisor_test(
        "goal_supervisor_spawn_reconciles_stale_persisted_state",
        goal_supervisor_spawn_reconciles_stale_persisted_state_inner(),
    );
}

async fn goal_supervisor_spawn_reconciles_stale_persisted_state_inner() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("goal supervisor test requires state db");
    let goal = ThreadGoal {
        thread_id: parent_thread_id,
        objective: "Continue the daily release cycle.".to_string(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: 1,
        updated_at: 1,
    };
    let supervisor_path = AgentPath::root()
        .join("goal_supervisor")
        .expect("supervisor path");
    let stale_helper_thread_ids = [ThreadId::new(), ThreadId::new()];
    for stale_helper_thread_id in stale_helper_thread_ids {
        let stale_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id,
            depth: 1,
            agent_path: Some(supervisor_path.clone()),
            agent_nickname: None,
            agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
        });
        let stale_metadata = codex_state::ThreadMetadataBuilder::new(
            stale_helper_thread_id,
            harness
                .config
                .codex_home
                .join(format!("{stale_helper_thread_id}.jsonl"))
                .to_path_buf(),
            chrono::Utc::now(),
            stale_source,
        )
        .build("openai");
        state_db
            .upsert_thread(&stale_metadata)
            .await
            .expect("stale supervisor metadata should persist");
        state_db
            .upsert_thread_spawn_edge(
                parent_thread_id,
                stale_helper_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("stale supervisor edge should persist");
    }
    harness
        .control
        .restore_v2_agent_metadata(&harness.config, parent_thread_id)
        .await;
    assert!(
        harness
            .control
            .state
            .agent_id_for_path(&supervisor_path)
            .is_some_and(|thread_id| stale_helper_thread_ids.contains(&thread_id)),
        "cold restore should reproduce the stale canonical path collision"
    );
    for stale_helper_thread_id in stale_helper_thread_ids {
        assert_eq!(
            harness.control.get_status(stale_helper_thread_id).await,
            AgentStatus::NotFound,
            "restored supervisor metadata must not be mistaken for a live helper"
        );
    }

    let replacement_thread_id =
        crate::goal_supervisor::spawn_supervisor_helper_for_test(&parent_thread.session, &goal)
            .await
            .expect("new supervisor spawn should reconcile stale persisted state");

    assert!(!stale_helper_thread_ids.contains(&replacement_thread_id));
    let open_children = state_db
        .list_thread_spawn_children_with_status(
            parent_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("open child query should succeed");
    let closed_children = state_db
        .list_thread_spawn_children_with_status(
            parent_thread_id,
            DirectionalThreadSpawnEdgeStatus::Closed,
        )
        .await
        .expect("closed child query should succeed");
    for stale_helper_thread_id in stale_helper_thread_ids {
        assert!(!open_children.contains(&stale_helper_thread_id));
        assert!(closed_children.contains(&stale_helper_thread_id));
    }
}

#[test]
fn goal_supervisor_reconciliation_preserves_running_supervisor_and_worker() {
    run_goal_supervisor_test(
        "goal_supervisor_reconciliation_preserves_running_supervisor_and_worker",
        goal_supervisor_reconciliation_preserves_running_supervisor_and_worker_inner(),
    );
}

async fn goal_supervisor_reconciliation_preserves_running_supervisor_and_worker_inner() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, _) = harness.start_thread().await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("goal supervisor test requires state db");
    let supervisor_path = AgentPath::root()
        .join("goal_supervisor")
        .expect("supervisor path");
    let mut helper_config = harness.config.clone();
    helper_config.ephemeral = true;
    let helper_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            helper_config,
            text_input("supervise"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(supervisor_path.clone()),
                agent_nickname: None,
                agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
            })),
            SpawnAgentOptions::default(),
        )
        .await
        .expect("supervisor helper should spawn")
        .thread_id;
    let worker_thread_id = harness
        .control
        .spawn_agent_with_metadata(
            harness.config.clone(),
            text_input("work"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: Some(AgentPath::root().join("worker").expect("worker path")),
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
            SpawnAgentOptions::default(),
        )
        .await
        .expect("worker should spawn")
        .thread_id;
    for child_thread_id in [helper_thread_id, worker_thread_id] {
        state_db
            .upsert_thread_spawn_edge(
                parent_thread_id,
                child_thread_id,
                DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("child edge should persist");
    }
    let foreign_supervisor_thread_id = ThreadId::new();
    let foreign_supervisor_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: ThreadId::new(),
        depth: 1,
        agent_path: Some(
            AgentPath::root()
                .join("foreign_goal_supervisor")
                .expect("foreign supervisor path"),
        ),
        agent_nickname: None,
        agent_role: Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string()),
    });
    let foreign_supervisor_metadata = codex_state::ThreadMetadataBuilder::new(
        foreign_supervisor_thread_id,
        harness
            .config
            .codex_home
            .join(format!("{foreign_supervisor_thread_id}.jsonl"))
            .to_path_buf(),
        chrono::Utc::now(),
        foreign_supervisor_source,
    )
    .build("openai");
    state_db
        .upsert_thread(&foreign_supervisor_metadata)
        .await
        .expect("foreign supervisor metadata should persist");
    state_db
        .upsert_thread_spawn_edge(
            parent_thread_id,
            foreign_supervisor_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("inaccurate foreign supervisor edge should persist");

    let first_result = harness
        .control
        .reconcile_goal_supervisor_state(parent_thread_id, &supervisor_path)
        .await
        .expect("first reconciliation should succeed");
    let second_result = harness
        .control
        .reconcile_goal_supervisor_state(parent_thread_id, &supervisor_path)
        .await
        .expect("second reconciliation should be idempotent");

    assert_eq!(first_result, Some(helper_thread_id));
    assert_eq!(second_result, Some(helper_thread_id));
    let open_children = state_db
        .list_thread_spawn_children_with_status(
            parent_thread_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await
        .expect("open child query should succeed");
    assert!(open_children.contains(&helper_thread_id));
    assert!(open_children.contains(&worker_thread_id));
    assert!(
        open_children.contains(&foreign_supervisor_thread_id),
        "reconciliation must not trust an inaccurate edge over the stored supervisor parent"
    );
}

#[test]
fn goal_supervisor_waits_for_parent_turn_to_finish() {
    run_goal_supervisor_test(
        "goal_supervisor_waits_for_parent_turn_to_finish",
        goal_supervisor_waits_for_parent_turn_to_finish_inner(),
    );
}

async fn goal_supervisor_waits_for_parent_turn_to_finish_inner() {
    let harness = AgentControlHarness::new().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");
    let goal = ThreadGoal {
        thread_id: parent_thread_id,
        objective: "Wait for the parent turn, then continue.".to_string(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: 1,
        updated_at: 1,
    };
    let parent_only = harness.manager.list_thread_ids().await;
    *parent_thread.session.active_turn.lock().await = Some(ActiveTurn::default());

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        "goal-supervisor-busy-parent-test",
        &goal,
    )
    .await
    .expect("busy parent should defer the supervisor");

    assert_eq!(harness.manager.list_thread_ids().await, parent_only);

    *parent_thread.session.active_turn.lock().await = None;
    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        "goal-supervisor-busy-parent-test",
        &goal,
    )
    .await
    .expect("idle parent should start the deferred supervisor");

    let helper_thread_id = spawned_thread_id_after(&harness.manager, &parent_only).await;
    assert!(harness.manager.get_thread(helper_thread_id).await.is_ok());
}

#[test]
fn goal_supervisor_finish_serializes_with_the_next_start() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_finish_serializes_with_the_next_start",
        goal_supervisor_finish_serializes_with_the_next_start_inner(),
    )
}

async fn goal_supervisor_finish_serializes_with_the_next_start_inner() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    let delayed_response = Duration::from_secs(30);
    let request_log = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("first-supervisor"),
                ev_completed("first-supervisor"),
            ]))
            .set_delay(delayed_response),
            sse_response(sse(vec![
                ev_response_created("replacement-supervisor"),
                ev_completed("replacement-supervisor"),
            ]))
            .set_delay(delayed_response),
            sse_response(sse(vec![
                ev_response_created("post-followup-supervisor"),
                ev_completed("post-followup-supervisor"),
            ]))
            .set_delay(delayed_response),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread.ensure_rollout_materialized().await;
    parent_thread.flush_rollout().await?;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("goal supervisor test requires state db");
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Serialize supervisor retirement and replacement.",
    )
    .await?;
    let parent_only = harness.manager.list_thread_ids().await;
    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let first_helper_thread_id = spawned_thread_id_after(&harness.manager, &parent_only).await;
    timeout(Duration::from_secs(5), async {
        while request_log.requests().is_empty() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first supervisor request should start");

    let transition =
        crate::goal_supervisor::hold_supervisor_transition_for_test(&parent_thread.session).await;
    let finish_session = Arc::clone(&parent_thread.session);
    let finish_task = tokio::spawn(async move {
        crate::goal_supervisor::finish_supervisor_helper(&finish_session, first_helper_thread_id)
            .await
    });
    tokio::task::yield_now().await;
    assert!(
        !finish_task.is_finished(),
        "supervisor retirement must wait for the lifecycle transition lock"
    );
    drop(transition);
    assert!(
        finish_task.await.expect("finish task should not panic")?,
        "the active supervisor should retire"
    );

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let replacement_thread_id = spawned_thread_id_after(&harness.manager, &parent_only).await;
    assert_ne!(replacement_thread_id, first_helper_thread_id);
    timeout(Duration::from_secs(5), async {
        while request_log.requests().len() < 2 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("replacement supervisor request should start");

    crate::goal_supervisor::record_followup_action(
        &parent_thread.session,
        &InterAgentCommunication::new(
            AgentPath::root()
                .join("goal_supervisor")
                .expect("supervisor path"),
            AgentPath::root(),
            Vec::new(),
            "continue".to_string(),
            /*trigger_turn*/ true,
        ),
    )
    .await;
    assert!(
        crate::goal_supervisor::finish_supervisor_helper_after_followup(
            &parent_thread.session,
            replacement_thread_id,
        )
        .await?,
        "the replacement supervisor should retire after its followup"
    );
    timeout(Duration::from_secs(5), async {
        while request_log.requests().len() < 3 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("idle recheck should replace a supervisor after its delivered followup");

    let _ = harness
        .manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    Ok(())
}

#[test]
fn goal_supervisor_helper_does_not_consume_multi_agent_v2_thread_limit() {
    run_goal_supervisor_test(
        "goal_supervisor_helper_does_not_consume_multi_agent_v2_thread_limit",
        goal_supervisor_helper_does_not_consume_multi_agent_v2_thread_limit_inner(),
    );
}

async fn goal_supervisor_helper_does_not_consume_multi_agent_v2_thread_limit_inner() {
    let (home, mut config) = test_config().await;
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow multi-agent v2");
    config
        .features
        .enable(Feature::Goals)
        .expect("test config should allow goals");
    config
        .features
        .enable(Feature::GoalSupervisor)
        .expect("test config should allow goal supervisor");
    config.agent_max_threads = None;
    config.multi_agent_v2.max_concurrent_threads_per_session = 2;
    assert_eq!(
        (
            config.agent_max_threads,
            config.multi_agent_v2.max_concurrent_threads_per_session,
            config.effective_agent_max_threads(MultiAgentVersion::V2),
        ),
        (None, 2, Some(1))
    );
    let harness = AgentControlHarness::new_with_config(home, config.clone()).await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let worker_thread_id = ThreadId::new();
    harness
        .control
        .state
        .reserve_spawn_slot(Some(1))
        .expect("the user-visible worker slot should be available")
        .commit(AgentMetadata {
            agent_id: Some(worker_thread_id),
            ..Default::default()
        });
    parent_thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent_thread
        .session
        .flush_rollout()
        .await
        .expect("parent rollout should flush");
    let before_thread_ids = harness.manager.list_thread_ids().await;
    let goal = ThreadGoal {
        thread_id: parent_thread_id,
        objective: "Verify supervisor thread accounting.".to_string(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: 1,
        updated_at: 1,
    };

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        "goal-supervisor-limit-test",
        &goal,
    )
    .await
    .expect("goal supervisor should bypass the user-visible agent limit");
    let helper_thread_id = spawned_thread_id_after(&harness.manager, &before_thread_ids).await;

    let err = match harness
        .control
        .state
        .reserve_spawn_slot(config.effective_agent_max_threads(MultiAgentVersion::V2))
    {
        Ok(_) => panic!("the goal supervisor must not free the counted worker slot"),
        Err(err) => err,
    };
    let CodexErrorDetails::AgentLimitReached { max_threads } = err.details() else {
        panic!("expected AgentLimitReached");
    };
    assert_eq!(*max_threads, 1);
    assert!(harness.manager.get_thread(helper_thread_id).await.is_ok());

    harness
        .control
        .state
        .release_spawned_thread(worker_thread_id);
    let _ = harness.control.shutdown_live_agent(helper_thread_id).await;
    let _ = parent_thread.submit(Op::Shutdown {}).await;
}

#[test]
fn goal_supervisor_goal_resume_clears_snooze_and_spawns_helper() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_goal_resume_clears_snooze_and_spawns_helper",
        goal_supervisor_goal_resume_clears_snooze_and_spawns_helper_inner(),
    )
}

async fn goal_supervisor_goal_resume_clears_snooze_and_spawns_helper_inner() -> anyhow::Result<()> {
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("sqlite state db should be available");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent_thread.session.flush_rollout().await?;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Resume the paused goal now.",
    )
    .await?;
    state_db
        .thread_goals()
        .set_thread_goal_supervisor_snoozed_until_ms(
            parent_thread_id,
            goal_id.as_str(),
            Some(chrono::Utc::now().timestamp_millis() + 60_000),
        )
        .await?;

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let before_resume_thread_ids = harness.manager.list_thread_ids().await;
    assert_eq!(
        vec![parent_thread_id],
        before_resume_thread_ids,
        "plain idle continuation should honor the supervisor snooze"
    );

    parent_thread
        .maybe_start_goal_supervisor_checkin_after_goal_resume(goal_id.as_str(), &goal)
        .await?;

    let child_thread_id =
        spawned_thread_id_after(&harness.manager, &before_resume_thread_ids).await;
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("supervisor helper thread should be registered");
    let child_config = child_thread.config_snapshot().await;
    assert_matches!(
        child_config.session_source,
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            agent_role: Some(agent_role),
            ..
        }) if agent_role == crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME
    );
    assert_eq!(
        None,
        state_db
            .thread_goals()
            .get_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, goal_id.as_str())
            .await?,
        "manual goal resume should clear the persisted supervisor snooze"
    );
    let _ = parent_thread.submit(Op::Shutdown {}).await;
    Ok(())
}

#[test]
fn goal_supervisor_snooze_persists_wakes_and_invalidates_stale_generation() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_snooze_persists_wakes_and_invalidates_stale_generation",
        goal_supervisor_snooze_persists_wakes_and_invalidates_stale_generation_inner(),
    )
}

async fn goal_supervisor_snooze_persists_wakes_and_invalidates_stale_generation_inner()
-> anyhow::Result<()> {
    let server = start_mock_server().await;
    let delayed_response = Duration::from_secs(30);
    let request_log = mount_response_sequence(
        &server,
        (0..3)
            .map(|index| {
                let response_id = format!("goal-supervisor-snooze-{index}");
                sse_response(sse(vec![
                    ev_response_created(&response_id),
                    ev_completed(&response_id),
                ]))
                .set_delay(delayed_response)
            })
            .collect(),
    )
    .await;
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Sqlite);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("goal supervisor test requires a state db");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent_thread.session.flush_rollout().await?;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Resume the scheduled goal after each snooze.",
    )
    .await?;
    let before_thread_ids = harness.manager.list_thread_ids().await;

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let first_helper_id = spawned_thread_id_after(&harness.manager, &before_thread_ids).await;
    timeout(Duration::from_secs(5), async {
        while request_log.requests().is_empty() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first supervisor request should start");
    assert_eq!(
        harness
            .control
            .snooze_goal_supervisor_helper(
                first_helper_id,
                /*delay_seconds*/ 60,
                Some("external work is not ready"),
            )
            .await,
        Some(60)
    );

    let snoozed_until_ms = state_db
        .thread_goals()
        .get_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, goal_id.as_str())
        .await?
        .expect("snooze deadline should be persisted");
    assert!(snoozed_until_ms > chrono::Utc::now().timestamp_millis());
    let first_generation = crate::goal_supervisor::scheduled_supervisor_wakeup_generation_for_test(
        &parent_thread.session,
    )
    .await
    .expect("snooze should schedule a wakeup");

    state_db
        .thread_goals()
        .set_thread_goal_supervisor_snoozed_until_ms(
            parent_thread_id,
            goal_id.as_str(),
            /*snoozed_until_ms*/ None,
        )
        .await?;
    crate::goal_supervisor::fire_scheduled_supervisor_wakeup_for_test(&parent_thread.session).await;
    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    timeout(Duration::from_secs(5), async {
        while request_log.requests().len() < 2 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("scheduled wakeup should start exactly one replacement helper");
    let second_helper_id = spawned_thread_id_after(&harness.manager, &[parent_thread_id]).await;
    assert_ne!(second_helper_id, first_helper_id);
    assert_eq!(
        crate::goal_supervisor::scheduled_supervisor_wakeup_generation_for_test(
            &parent_thread.session,
        )
        .await,
        None
    );

    assert_eq!(
        harness
            .control
            .snooze_goal_supervisor_helper(
                second_helper_id,
                /*delay_seconds*/ 60,
                Some("still waiting for external work"),
            )
            .await,
        Some(60)
    );
    let stale_generation = crate::goal_supervisor::scheduled_supervisor_wakeup_generation_for_test(
        &parent_thread.session,
    )
    .await
    .expect("second snooze should schedule a wakeup");
    assert_ne!(stale_generation, first_generation);
    let before_manual_resume = harness.manager.list_thread_ids().await;
    parent_thread
        .maybe_start_goal_supervisor_checkin_after_goal_resume(goal_id.as_str(), &goal)
        .await?;
    let third_helper_id = spawned_thread_id_after(&harness.manager, &before_manual_resume).await;
    timeout(Duration::from_secs(5), async {
        while request_log.requests().len() < 3 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("manual resume should start one replacement helper");
    assert_ne!(third_helper_id, second_helper_id);
    assert_eq!(
        state_db
            .thread_goals()
            .get_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, goal_id.as_str())
            .await?,
        None
    );
    assert_eq!(
        crate::goal_supervisor::scheduled_supervisor_wakeup_generation_for_test(
            &parent_thread.session,
        )
        .await,
        None
    );

    let generation_after_resume =
        crate::goal_supervisor::supervisor_wakeup_generation_for_test(&parent_thread.session);
    assert_ne!(generation_after_resume, stale_generation);
    crate::goal_supervisor::fire_supervisor_wakeup_generation_for_test(
        &parent_thread.session,
        stale_generation,
    )
    .await;
    assert_eq!(
        crate::goal_supervisor::supervisor_wakeup_generation_for_test(&parent_thread.session),
        generation_after_resume,
        "an invalidated wakeup must not advance the current generation"
    );
    assert_eq!(request_log.requests().len(), 3);

    let third_helper = harness
        .manager
        .get_thread(third_helper_id)
        .await
        .expect("manual resume helper should remain loaded");
    let continuity =
        goal_supervisor_continuity_from_history(&third_helper.session.clone_history().await);
    assert_eq!(continuity["previous_supervisor_action"]["kind"], "snooze");
    assert_eq!(
        continuity["goal_timing"]["snooze_count_since_goal_created"],
        2
    );

    let _ = harness
        .manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    Ok(())
}

#[test]
fn goal_supervisor_execution_settings_change_restarts_running_helper() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_execution_settings_change_restarts_running_helper",
        goal_supervisor_execution_settings_change_restarts_running_helper_inner(),
    )
}

async fn goal_supervisor_execution_settings_change_restarts_running_helper_inner()
-> anyhow::Result<()> {
    let server = start_mock_server().await;
    let delayed_response = Duration::from_secs(30);
    let request_log = mount_response_sequence(
        &server,
        vec![
            sse_response(sse(vec![
                ev_response_created("resp-before-settings-change"),
                ev_completed("resp-before-settings-change"),
            ]))
            .set_delay(delayed_response),
            sse_response(sse(vec![
                ev_response_created("resp-after-settings-change"),
                ev_completed("resp-after-settings-change"),
            ]))
            .set_delay(delayed_response),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("goal supervisor test requires a state db");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent_thread.session.flush_rollout().await?;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Restart the supervisor when execution settings change.",
    )
    .await?;
    let before_thread_ids = harness.manager.list_thread_ids().await;

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;
    let first_helper_thread_id =
        spawned_thread_id_after(&harness.manager, &before_thread_ids).await;
    timeout(Duration::from_secs(5), async {
        while request_log.requests().is_empty() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first supervisor request should start");

    let original_model = parent_thread.session.thread_config_snapshot().await.model;
    let next_model = if original_model == "gpt-5.4" {
        "gpt-5.2"
    } else {
        "gpt-5.4"
    };
    let settings_changed_at = Instant::now();
    parent_thread
        .submit(Op::ThreadSettings {
            thread_settings: ThreadSettingsOverrides {
                model: Some(next_model.to_string()),
                effort: Some(Some(ReasoningEffort::High)),
                service_tier: Some(Some(ServiceTier::Fast.request_value().to_string())),
                ..Default::default()
            },
        })
        .await?;
    timeout(Duration::from_secs(5), async {
        while !harness
            .manager
            .captured_ops()
            .into_iter()
            .any(|(thread_id, op)| {
                thread_id == first_helper_thread_id && matches!(op, Op::Shutdown)
            })
        {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("settings update should stop the running supervisor helper");
    timeout(Duration::from_secs(5), async {
        while request_log.requests().len() < 2 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("replacement supervisor should start before the old response finishes");
    assert!(settings_changed_at.elapsed() < delayed_response);

    let requests = request_log.requests();
    assert_eq!(requests.len(), 2);
    let replacement_body = requests[1].body_json();
    assert_eq!(replacement_body["model"].as_str(), Some(next_model));
    assert_eq!(
        replacement_body["reasoning"]["effort"].as_str(),
        Some("high")
    );
    assert_eq!(
        replacement_body["service_tier"].as_str(),
        Some(ServiceTier::Fast.request_value())
    );

    let _ = harness
        .manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    Ok(())
}

#[test]
fn failed_goal_supervisor_waits_for_one_persisted_retry() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "failed_goal_supervisor_waits_for_one_persisted_retry",
        failed_goal_supervisor_waits_for_one_persisted_retry_inner(),
    )
}

async fn failed_goal_supervisor_waits_for_one_persisted_retry_inner() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    let request_log = mount_sse_sequence(
        &server,
        vec![
            sse_failed(
                "supervisor-failure-1",
                "model_not_found",
                "saved model unavailable",
            ),
            sse_failed(
                "supervisor-failure-2",
                "model_not_found",
                "saved model unavailable",
            ),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    config.model_provider.request_max_retries = Some(0);
    config.model_provider.stream_max_retries = Some(0);
    let harness = AgentControlHarness::new_with_config(home, config).await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("sqlite state db should be available");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    parent_thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent_thread.session.flush_rollout().await?;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Keep retrying after transient supervisor failures.",
    )
    .await?;

    crate::goal_supervisor::maybe_start_supervisor_checkin(
        &parent_thread.session,
        goal_id.as_str(),
        &goal,
    )
    .await?;

    let first_deadline_ms = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(deadline_ms) = state_db
                .thread_goals()
                .get_thread_goal_supervisor_snoozed_until_ms(parent_thread_id, goal_id.as_str())
                .await?
                && deadline_ms > chrono::Utc::now().timestamp_millis()
                && harness.manager.list_thread_ids().await == vec![parent_thread_id]
            {
                break Ok::<_, anyhow::Error>(deadline_ms);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert_eq!(request_log.requests().len(), 1);
    assert_eq!(
        crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await,
        1
    );
    assert!(
        first_deadline_ms - chrono::Utc::now().timestamp_millis() <= 60_000,
        "first failure retry should use the one-minute backoff tier"
    );
    let persisted_goal = state_db
        .thread_goals()
        .get_thread_goal(parent_thread_id)
        .await?
        .expect("active goal should remain persisted");
    assert_eq!(
        persisted_goal.status,
        codex_state::ThreadGoalStatus::Active,
        "supervisor failure must not pause, block, or complete the goal"
    );
    let warning = timeout(Duration::from_secs(5), async {
        loop {
            let event = parent_thread
                .next_event()
                .await
                .expect("parent event channel should stay open");
            if let EventMsg::Warning(warning) = event.msg
                && warning.message.contains("Goal supervisor check-in failed")
            {
                break warning.message;
            }
        }
    })
    .await
    .expect("failed supervisor should warn the user");
    assert!(warning.contains("saved model unavailable"));
    assert!(warning.contains("Retrying in"));

    let scheduled_generation =
        crate::goal_supervisor::scheduled_supervisor_wakeup_generation_for_test(
            &parent_thread.session,
        )
        .await
        .expect("failure should schedule one retry wakeup");
    for _ in 0..3 {
        crate::goal_supervisor::maybe_start_supervisor_checkin(
            &parent_thread.session,
            goal_id.as_str(),
            &goal,
        )
        .await?;
    }
    assert_eq!(
        request_log.requests().len(),
        1,
        "idle signals before the deadline must not replace the failed helper"
    );
    assert_eq!(
        crate::goal_supervisor::scheduled_supervisor_wakeup_generation_for_test(
            &parent_thread.session,
        )
        .await,
        Some(scheduled_generation),
        "idle signals for the same deadline must reuse the existing sleeping timer"
    );

    state_db
        .thread_goals()
        .set_thread_goal_supervisor_snoozed_until_ms(
            parent_thread_id,
            goal_id.as_str(),
            /*snoozed_until_ms*/ None,
        )
        .await?;
    crate::goal_supervisor::fire_scheduled_supervisor_wakeup_for_test(&parent_thread.session).await;
    for _ in 0..3 {
        crate::goal_supervisor::maybe_start_supervisor_checkin(
            &parent_thread.session,
            goal_id.as_str(),
            &goal,
        )
        .await?;
    }
    timeout(Duration::from_secs(5), async {
        loop {
            if request_log.requests().len() == 2
                && harness.manager.list_thread_ids().await == vec![parent_thread_id]
                // Removing the helper precedes the parent's persisted retry update.
                && crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await == 2
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("second failed supervisor should finish");
    let requests_after_retry = request_log.requests();
    assert_eq!(
        requests_after_retry.len(),
        2,
        "one failure retry should run after the in-memory deadline; loaded threads: {:?}; captured ops: {:?}",
        harness.manager.list_thread_ids().await,
        harness.manager.captured_ops(),
    );
    assert_eq!(
        request_log.requests().len(),
        2,
        "duplicate idle signals must still produce exactly one retry"
    );
    assert_eq!(
        crate::goal_supervisor::supervisor_failure_count_for_test(&parent_thread.session).await,
        2,
        "the second implicit failure should advance the backoff tier"
    );
    Ok(())
}

#[test]
#[serial(fork_env)]
fn goal_supervisor_helper_request_uses_parent_cache_key_and_mcp_snapshot() -> anyhow::Result<()> {
    run_goal_supervisor_test(
        "goal_supervisor_helper_request_uses_parent_cache_key_and_mcp_snapshot",
        goal_supervisor_helper_request_uses_parent_cache_key_and_mcp_snapshot_inner(),
    )
}

async fn goal_supervisor_helper_request_uses_parent_cache_key_and_mcp_snapshot_inner()
-> anyhow::Result<()> {
    let server = start_mock_server().await;
    let request_log = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-parent"),
                ev_completed("resp-parent"),
            ]),
            sse(vec![
                ev_response_created("resp-child"),
                ev_completed("resp-child"),
            ]),
        ],
    )
    .await;
    let (home, mut config) = test_config().await;
    let _ = config.features.enable(Feature::AgentPromptInjection);
    let _ = config.features.enable(Feature::MultiAgentV2);
    let _ = config.features.enable(Feature::Goals);
    let _ = config.features.enable(Feature::GoalSupervisor);
    let _ = config.features.enable(Feature::Sqlite);
    config.model_provider.base_url = Some(format!("{}/v1", server.uri()));
    config.model_provider.supports_websockets = false;
    let mcp_server_path = config.codex_home.join("fake_mcp_server.py");
    std::fs::write(
        &mcp_server_path,
        r#"import json
import sys

def read_message():
    line = sys.stdin.buffer.readline()
    if not line:
        return None
    return json.loads(line)

def write_message(message):
    body = json.dumps(message).encode("utf-8")
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.write(b"\n")
    sys.stdout.buffer.flush()

while True:
    message = read_message()
    if message is None:
        break
    method = message.get("method")
    request_id = message.get("id")
    if request_id is None:
        continue
    if method == "initialize":
        write_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "fake-mcp", "version": "1.0.0"},
            },
        })
    elif method == "tools/list":
        write_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "tools": [{
                    "name": "echo",
                    "description": "Echo from fake MCP",
                    "inputSchema": {
                        "type": "object",
                        "properties": {},
                        "additionalProperties": False,
                    },
                }],
            },
        })
    else:
        write_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": "method not found"},
        })
"#,
    )?;
    config
        .mcp_servers
        .set(std::collections::HashMap::from([(
            "rmcp".to_string(),
            McpServerConfig {
                auth: Default::default(),
                transport: McpServerTransportConfig::Stdio {
                    command: "python3".to_string(),
                    args: vec![mcp_server_path.to_string_lossy().to_string()],
                    env: None,
                    env_vars: Vec::new(),
                    cwd: None,
                },
                environment_id: codex_config::DEFAULT_MCP_SERVER_ENVIRONMENT_ID.to_string(),
                enabled: true,
                required: false,
                supports_parallel_tool_calls: false,
                omit_tools_from: None,
                disabled_reason: None,
                oauth: None,
                startup_timeout_sec: Some(Duration::from_secs(5)),
                tool_timeout_sec: None,
                default_tools_approval_mode: None,
                enabled_tools: None,
                disabled_tools: None,
                scopes: None,
                oauth_resource: None,
                tools: std::collections::HashMap::new(),
            },
        )]))
        .expect("test config should allow MCP servers");

    let harness = AgentControlHarness::new_with_config(home, config).await;
    let state_db = harness
        .state_db
        .as_ref()
        .expect("sqlite state db should be available");
    let (parent_thread_id, parent_thread) = harness.start_thread().await;
    let parent_prompt_cache_key = parent_thread.session.prompt_cache_key();
    let mcp_runtime = Arc::clone(&parent_thread.session.services.mcp_runtime);
    assert!(
        mcp_runtime
            .latest_wait_for_server_ready("rmcp", Duration::from_secs(5))
            .await,
        "parent MCP server should become ready before forking"
    );
    let parent_mcp_tools = mcp_runtime.latest_list_all_tools().await;
    assert!(
        parent_mcp_tools
            .iter()
            .any(|tool| tool.server_name == "rmcp" && tool.tool.name == "echo"),
        "parent MCP manager should expose live MCP tools before forking: tools={parent_mcp_tools:#?}"
    );
    parent_thread
        .start_or_steer_turn(TurnInputRequest::user_input(text_input("parent seed")))
        .await?;
    wait_for_turn_complete(parent_thread.as_ref()).await;
    parent_thread
        .session
        .ensure_rollout_materialized(PersistContext::Standard)
        .await;
    parent_thread.session.flush_rollout().await?;
    let before_thread_ids = harness.manager.list_thread_ids().await;
    let (goal_id, goal) = create_active_thread_goal_for_test(
        state_db,
        parent_thread_id,
        &parent_thread.session,
        "Supervise the parent with inherited MCP tools.",
    )
    .await?;

    crate::goal_supervisor::maybe_start_supervisor_checkin(&parent_thread.session, &goal_id, &goal)
        .await?;
    let child_thread_id = spawned_thread_id_after(&harness.manager, &before_thread_ids).await;
    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should be registered");
    let child_mcp_tool_snapshot = child_thread
        .session
        .services
        .mcp_tool_snapshot
        .lock()
        .await
        .clone()
        .expect("goal supervisor helper should inherit the parent MCP tool snapshot");
    assert!(
        child_mcp_tool_snapshot
            .tools
            .iter()
            .any(|tool| tool.server_name == "rmcp" && tool.tool.name == "echo"),
        "goal supervisor helper should inherit the parent MCP tool snapshot"
    );

    wait_for_turn_complete(child_thread.as_ref()).await;
    let requests = request_log.requests();
    assert_eq!(requests.len(), 2);
    let parent_body = requests[0].body_json();
    let child_body = requests[1].body_json();
    let parent_input = parent_body["input"]
        .as_array()
        .expect("parent input should be an array");
    let child_input = child_body["input"]
        .as_array()
        .expect("child input should be an array");
    let expected_prompt_cache_key = parent_prompt_cache_key.to_string();
    assert_eq!(
        child_body["prompt_cache_key"].as_str(),
        Some(expected_prompt_cache_key.as_str())
    );
    assert_eq!(
        &child_input[..parent_input.len()],
        parent_input,
        "goal supervisor helpers must preserve the exact parent request prefix through the fork point"
    );
    let child_suffix = &child_input[parent_input.len()..];
    assert!(
        child_suffix.first().is_some_and(|item| {
            item["role"] == "developer"
                && item["content"].as_array().is_some_and(|content| {
                    content.iter().any(|content_item| {
                        content_item["text"]
                            .as_str()
                            .is_some_and(|text| text.contains("You are also a **goal supervisor**"))
                    })
                })
        }),
        "goal supervisor helpers should append the supervisor role prompt immediately after the inherited parent request prefix: suffix={child_suffix:#?}"
    );
    for unexpected_child_context in [
        "# AGENTS.md instructions",
        "<permissions instructions>",
        "<apps_instructions>",
        "<skills_instructions>",
        "<plugins_instructions>",
    ] {
        assert!(
            child_suffix.iter().all(|item| {
                item["content"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .all(|content_item| {
                        content_item["text"]
                            .as_str()
                            .is_none_or(|text| !text.contains(unexpected_child_context))
                    })
            }),
            "goal supervisor helpers must not append fresh child startup context after forking: found {unexpected_child_context} in suffix={child_suffix:#?}"
        );
    }
    assert_eq!(
        child_body["parallel_tool_calls"], parent_body["parallel_tool_calls"],
        "goal supervisor helpers must keep the same parallel tool-call setting as their parent"
    );
    let parent_tool_signatures = request_tool_signatures(&parent_body);
    let child_tool_signatures = request_tool_signatures(&child_body);
    let supervisor_tool_signatures = std::collections::BTreeSet::from([
        "supervisor.close_self".to_string(),
        "supervisor.snooze".to_string(),
        "supervisor.compact_parent_context".to_string(),
    ]);
    assert_eq!(
        child_tool_signatures
            .difference(&supervisor_tool_signatures)
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        parent_tool_signatures,
        "goal supervisor helpers must inherit every non-supervisor tool from their parent"
    );
    assert!(
        parent_tool_signatures.is_disjoint(&supervisor_tool_signatures),
        "supervisor tools must not be exposed to the parent request: tools={parent_tool_signatures:#?}"
    );
    assert!(
        supervisor_tool_signatures.is_subset(&child_tool_signatures),
        "the exact goal supervisor helper must expose the supervisor tools: tools={child_tool_signatures:#?}"
    );
    for expected_tool in [
        "collaboration.spawn_agent",
        "collaboration.send_message",
        "collaboration.followup_task",
        "collaboration.wait_agent",
        "collaboration.list_agents",
        "collaboration.interrupt_agent",
        "supervisor.close_self",
        "supervisor.snooze",
        "supervisor.compact_parent_context",
    ] {
        assert!(
            child_tool_signatures.contains(expected_tool),
            "expected forked child request to expose `{expected_tool}`; tools={child_tool_signatures:#?}"
        );
    }
    assert!(
        child_body["tools"].as_array().is_some_and(|tools| {
            tools
                .iter()
                .any(|tool| tool["type"].as_str() == Some("tool_search"))
        }),
        "the inherited MCP snapshot should be discoverable through tool_search: {child_body:#}"
    );

    Ok(())
}

#[tokio::test]
async fn resume_closed_child_reopens_open_descendants() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");
    let grandchild_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello grandchild"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("grandchild spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    let grandchild_thread = harness
        .manager
        .get_thread(grandchild_thread_id)
        .await
        .expect("grandchild thread should exist");
    persist_thread_for_tree_resume(&parent_thread, "parent persisted").await;
    persist_thread_for_tree_resume(&child_thread, "child persisted").await;
    persist_thread_for_tree_resume(&grandchild_thread, "grandchild persisted").await;
    wait_for_live_thread_spawn_children(&harness.control, parent_thread_id, &[child_thread_id])
        .await;
    wait_for_live_thread_spawn_children(&harness.control, child_thread_id, &[grandchild_thread_id])
        .await;

    let _ = harness
        .control
        .close_agent(child_thread_id)
        .await
        .expect("child close should succeed");

    let resumed_child_thread_id = harness
        .control
        .resume_agent_from_rollout(
            harness.config.clone(),
            child_thread_id,
            SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            }),
        )
        .await
        .expect("child resume should succeed");
    assert_eq!(resumed_child_thread_id, child_thread_id);
    assert_ne!(
        harness.control.get_status(child_thread_id).await,
        AgentStatus::NotFound
    );
    assert_ne!(
        harness.control.get_status(grandchild_thread_id).await,
        AgentStatus::NotFound
    );

    let _ = harness
        .control
        .close_agent(child_thread_id)
        .await
        .expect("child close after resume should succeed");
    let _ = harness
        .control
        .shutdown_live_agent(parent_thread_id)
        .await
        .expect("parent shutdown should succeed");
}

#[tokio::test]
async fn resume_agent_from_rollout_reopens_open_descendants_after_manager_shutdown() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");
    let grandchild_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello grandchild"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("grandchild spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    let grandchild_thread = harness
        .manager
        .get_thread(grandchild_thread_id)
        .await
        .expect("grandchild thread should exist");
    persist_thread_for_tree_resume(&parent_thread, "parent persisted").await;
    persist_thread_for_tree_resume(&child_thread, "child persisted").await;
    persist_thread_for_tree_resume(&grandchild_thread, "grandchild persisted").await;
    wait_for_live_thread_spawn_children(&harness.control, parent_thread_id, &[child_thread_id])
        .await;
    wait_for_live_thread_spawn_children(&harness.control, child_thread_id, &[grandchild_thread_id])
        .await;

    let report = harness
        .manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    assert_eq!(report.submit_failed, Vec::<ThreadId>::new());
    assert_eq!(report.timed_out, Vec::<ThreadId>::new());

    let resumed_parent_thread_id = harness
        .control
        .resume_agent_from_rollout(
            harness.config.clone(),
            parent_thread_id,
            SessionSource::Exec,
        )
        .await
        .expect("tree resume should succeed");
    assert_eq!(resumed_parent_thread_id, parent_thread_id);
    assert_ne!(
        harness.control.get_status(parent_thread_id).await,
        AgentStatus::NotFound
    );
    assert_ne!(
        harness.control.get_status(child_thread_id).await,
        AgentStatus::NotFound
    );
    assert_ne!(
        harness.control.get_status(grandchild_thread_id).await,
        AgentStatus::NotFound
    );

    let _ = harness
        .control
        .shutdown_agent_tree(parent_thread_id)
        .await
        .expect("tree shutdown after subtree resume should succeed");
}

#[tokio::test]
async fn resume_agent_from_rollout_uses_edge_data_when_descendant_metadata_source_is_stale() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");
    let grandchild_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello grandchild"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("grandchild spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    let grandchild_thread = harness
        .manager
        .get_thread(grandchild_thread_id)
        .await
        .expect("grandchild thread should exist");
    persist_thread_for_tree_resume(&parent_thread, "parent persisted").await;
    persist_thread_for_tree_resume(&child_thread, "child persisted").await;
    persist_thread_for_tree_resume(&grandchild_thread, "grandchild persisted").await;
    wait_for_live_thread_spawn_children(&harness.control, parent_thread_id, &[child_thread_id])
        .await;
    wait_for_live_thread_spawn_children(&harness.control, child_thread_id, &[grandchild_thread_id])
        .await;

    let state_db = grandchild_thread
        .state_db()
        .expect("sqlite state db should be available");
    let mut stale_metadata = state_db
        .get_thread(grandchild_thread_id)
        .await
        .expect("grandchild metadata query should succeed")
        .expect("grandchild metadata should exist");
    stale_metadata.source =
        serde_json::to_string(&SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: ThreadId::new(),
            depth: 99,
            agent_path: None,
            agent_nickname: None,
            agent_role: Some("worker".to_string()),
        }))
        .expect("stale session source should serialize");
    state_db
        .upsert_thread(&stale_metadata)
        .await
        .expect("stale grandchild metadata should persist");

    let report = harness
        .manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    assert_eq!(report.submit_failed, Vec::<ThreadId>::new());
    assert_eq!(report.timed_out, Vec::<ThreadId>::new());

    let resumed_parent_thread_id = harness
        .control
        .resume_agent_from_rollout(
            harness.config.clone(),
            parent_thread_id,
            SessionSource::Exec,
        )
        .await
        .expect("tree resume should succeed");
    assert_eq!(resumed_parent_thread_id, parent_thread_id);
    assert_ne!(
        harness.control.get_status(parent_thread_id).await,
        AgentStatus::NotFound
    );
    assert_ne!(
        harness.control.get_status(child_thread_id).await,
        AgentStatus::NotFound
    );
    assert_ne!(
        harness.control.get_status(grandchild_thread_id).await,
        AgentStatus::NotFound
    );

    let resumed_grandchild_snapshot = harness
        .manager
        .get_thread(grandchild_thread_id)
        .await
        .expect("resumed grandchild thread should exist")
        .config_snapshot()
        .await;
    let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: resumed_parent_thread_id,
        depth: resumed_depth,
        ..
    }) = resumed_grandchild_snapshot.session_source
    else {
        panic!("expected thread-spawn sub-agent source");
    };
    assert_eq!(resumed_parent_thread_id, child_thread_id);
    assert_eq!(resumed_depth, 2);

    let _ = harness
        .control
        .shutdown_agent_tree(parent_thread_id)
        .await
        .expect("tree shutdown after subtree resume should succeed");
}

#[tokio::test]
async fn resume_agent_from_rollout_skips_descendants_when_parent_resume_fails() {
    let harness = AgentControlHarness::new_with_multi_agent_v1().await;
    let (parent_thread_id, parent_thread) = harness.start_thread().await;

    let child_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello child"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("explorer".to_string()),
            })),
        )
        .await
        .expect("child spawn should succeed");
    let grandchild_thread_id = harness
        .control
        .spawn_agent(
            harness.config.clone(),
            text_input("hello grandchild"),
            Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: child_thread_id,
                depth: 2,
                agent_path: None,
                agent_nickname: None,
                agent_role: Some("worker".to_string()),
            })),
        )
        .await
        .expect("grandchild spawn should succeed");

    let child_thread = harness
        .manager
        .get_thread(child_thread_id)
        .await
        .expect("child thread should exist");
    let grandchild_thread = harness
        .manager
        .get_thread(grandchild_thread_id)
        .await
        .expect("grandchild thread should exist");
    persist_thread_for_tree_resume(&parent_thread, "parent persisted").await;
    persist_thread_for_tree_resume(&child_thread, "child persisted").await;
    persist_thread_for_tree_resume(&grandchild_thread, "grandchild persisted").await;
    wait_for_live_thread_spawn_children(&harness.control, parent_thread_id, &[child_thread_id])
        .await;
    wait_for_live_thread_spawn_children(&harness.control, child_thread_id, &[grandchild_thread_id])
        .await;

    let child_rollout_path = child_thread
        .rollout_path()
        .expect("child thread should have rollout path");
    let report = harness
        .manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    assert_eq!(report.submit_failed, Vec::<ThreadId>::new());
    assert_eq!(report.timed_out, Vec::<ThreadId>::new());
    tokio::fs::remove_file(&child_rollout_path)
        .await
        .expect("child rollout path should be removable");

    let resumed_parent_thread_id = harness
        .control
        .resume_agent_from_rollout(
            harness.config.clone(),
            parent_thread_id,
            SessionSource::Exec,
        )
        .await
        .expect("root resume should succeed");
    assert_eq!(resumed_parent_thread_id, parent_thread_id);
    assert_ne!(
        harness.control.get_status(parent_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(child_thread_id).await,
        AgentStatus::NotFound
    );
    assert_eq!(
        harness.control.get_status(grandchild_thread_id).await,
        AgentStatus::NotFound
    );

    let _ = harness
        .control
        .shutdown_agent_tree(parent_thread_id)
        .await
        .expect("tree shutdown after partial subtree resume should succeed");
}
