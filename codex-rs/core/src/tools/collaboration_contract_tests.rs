use std::collections::HashMap;
use std::sync::Arc;

use codex_features::Feature;
use codex_models_manager::CustomModelConfig;
use codex_models_manager::bundled_models_response;
use codex_models_manager::manager::ModelsManager;
use codex_models_manager::manager::StaticModelsManager;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::openai_models::ToolMode;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_tools::ResponsesApiNamespaceTool;
use codex_tools::ToolExposure;
use codex_tools::ToolSpec;
use codex_tools::create_tools_json_for_responses_api;
use codex_tools::create_tools_json_for_responses_lite;
use pretty_assertions::assert_eq;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;

use super::build_core_tool_registry;
use crate::config::AgentRoleConfig;
use crate::session::step_context::StepContext;
use crate::session::tests::make_session_and_context;
use crate::session::tests::update_turn_settings_for_test;
use crate::session::turn_context::TurnContext;
use crate::tools::handlers::ToolSearchHandlerCache;
use crate::tools::router::ToolRouter;

const PINNED_UPSTREAM: &str = "d9511fb7888d98f89526d4ae019dd9be2f14199e";
const V1_NAMESPACE: &str = "multi_agent_v1";
const V2_NAMESPACE: &str = "collaboration";
const V1_TOOLS: &[&str] = &[
    "close_agent",
    "resume_agent",
    "send_input",
    "spawn_agent",
    "wait_agent",
];
const V2_TOOLS: &[&str] = &[
    "followup_task",
    "interrupt_agent",
    "list_agents",
    "send_message",
    "spawn_agent",
    "wait_agent",
];

/// One configuration whose canonical collaboration contract is pinned to upstream.
#[derive(Clone, Copy)]
enum Scenario {
    V1Direct,
    V1Deferred,
    V1DepthLimit,
    V1UsageHint,
    V2Root,
    V2WaitDisabled,
    V2DirectModelOnly,
    V2CodeModeOnly,
    V2CodeModeOnlyDirectModel,
    V2CustomNamespace,
    V2HiddenSpawnMetadata,
    V2RoleAndUsageHint,
    V2SubagentSupported,
    V2SubagentUnsupported,
    V2GoalHelperUnsupported,
}

impl Scenario {
    fn name(self) -> &'static str {
        match self {
            Self::V1Direct => "v1_direct",
            Self::V1Deferred => "v1_deferred",
            Self::V1DepthLimit => "v1_depth_limit",
            Self::V1UsageHint => "v1_usage_hint",
            Self::V2Root => "v2_root",
            Self::V2WaitDisabled => "v2_wait_disabled",
            Self::V2DirectModelOnly => "v2_direct_model_only",
            Self::V2CodeModeOnly => "v2_code_mode_only",
            Self::V2CodeModeOnlyDirectModel => "v2_code_mode_only_direct_model",
            Self::V2CustomNamespace => "v2_custom_namespace",
            Self::V2HiddenSpawnMetadata => "v2_hidden_spawn_metadata",
            Self::V2RoleAndUsageHint => "v2_role_and_usage_hint",
            Self::V2SubagentSupported => "v2_subagent_supported",
            Self::V2SubagentUnsupported => "v2_subagent_unsupported",
            Self::V2GoalHelperUnsupported => "v2_goal_helper_unsupported",
        }
    }

    fn namespace(self) -> &'static str {
        match self {
            Self::V1Direct | Self::V1Deferred | Self::V1DepthLimit | Self::V1UsageHint => {
                V1_NAMESPACE
            }
            Self::V2CustomNamespace => "agents",
            Self::V2Root
            | Self::V2WaitDisabled
            | Self::V2DirectModelOnly
            | Self::V2CodeModeOnly
            | Self::V2CodeModeOnlyDirectModel
            | Self::V2HiddenSpawnMetadata
            | Self::V2RoleAndUsageHint
            | Self::V2SubagentSupported
            | Self::V2SubagentUnsupported
            | Self::V2GoalHelperUnsupported => V2_NAMESPACE,
        }
    }

    fn tool_names(self) -> &'static [&'static str] {
        match self {
            Self::V1Direct | Self::V1Deferred | Self::V1DepthLimit | Self::V1UsageHint => V1_TOOLS,
            Self::V2Root
            | Self::V2WaitDisabled
            | Self::V2DirectModelOnly
            | Self::V2CodeModeOnly
            | Self::V2CodeModeOnlyDirectModel
            | Self::V2CustomNamespace
            | Self::V2HiddenSpawnMetadata
            | Self::V2RoleAndUsageHint
            | Self::V2SubagentSupported
            | Self::V2SubagentUnsupported
            | Self::V2GoalHelperUnsupported => V2_TOOLS,
        }
    }

    fn configure(self, turn: &mut TurnContext) {
        let mut config = (*turn.config).clone();
        match self {
            Self::V1Direct | Self::V1Deferred | Self::V1DepthLimit | Self::V1UsageHint => {
                config
                    .features
                    .enable(Feature::Collab)
                    .expect("Collab should be configurable");
                config
                    .features
                    .disable(Feature::MultiAgentV2)
                    .expect("MultiAgentV2 should be configurable");
            }
            Self::V2Root
            | Self::V2WaitDisabled
            | Self::V2DirectModelOnly
            | Self::V2CodeModeOnly
            | Self::V2CodeModeOnlyDirectModel
            | Self::V2CustomNamespace
            | Self::V2HiddenSpawnMetadata
            | Self::V2RoleAndUsageHint
            | Self::V2SubagentSupported
            | Self::V2SubagentUnsupported
            | Self::V2GoalHelperUnsupported => {
                config
                    .features
                    .enable(Feature::MultiAgentV2)
                    .expect("MultiAgentV2 should be configurable");
            }
        }
        match self {
            Self::V1Deferred => {
                update_turn_settings_for_test(turn, |settings| {
                    Arc::make_mut(&mut settings.model_info).supports_search_tool = true;
                });
            }
            Self::V1DepthLimit => {
                turn.session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    parent_thread_id: ThreadId::new(),
                    depth: config.agent_max_depth,
                    agent_path: Some(
                        AgentPath::try_from("/root/depth_limit").expect("valid test agent path"),
                    ),
                    agent_nickname: None,
                    agent_role: None,
                });
            }
            Self::V1UsageHint => {
                config.multi_agent_v2.usage_hint_text =
                    Some("Pinned collaboration usage hint.".to_string());
            }
            Self::V2WaitDisabled => config.multi_agent_v2.wait_agent_enabled = false,
            Self::V2DirectModelOnly => config.multi_agent_v2.non_code_mode_only = true,
            Self::V2CodeModeOnly => {
                config.multi_agent_v2.non_code_mode_only = false;
                config
                    .features
                    .enable(Feature::CodeModeOnly)
                    .expect("CodeModeOnly should be configurable");
                update_turn_settings_for_test(turn, |settings| {
                    Arc::make_mut(&mut settings.model_info).tool_mode =
                        Some(ToolMode::CodeModeOnly);
                });
            }
            Self::V2CodeModeOnlyDirectModel => {
                config.multi_agent_v2.non_code_mode_only = true;
                config
                    .features
                    .enable(Feature::CodeModeOnly)
                    .expect("CodeModeOnly should be configurable");
                update_turn_settings_for_test(turn, |settings| {
                    Arc::make_mut(&mut settings.model_info).tool_mode =
                        Some(ToolMode::CodeModeOnly);
                });
            }
            Self::V2CustomNamespace => {
                config.multi_agent_v2.tool_namespace = Some("agents".to_string());
            }
            Self::V2HiddenSpawnMetadata => {
                config.multi_agent_v2.hide_spawn_agent_metadata = true;
                config.multi_agent_v2.expose_spawn_agent_model_overrides = false;
            }
            Self::V2RoleAndUsageHint => {
                config.agent_roles.insert(
                    "contract_reviewer".to_string(),
                    AgentRoleConfig {
                        description: Some(
                            "Review the canonical collaboration contract.".to_string(),
                        ),
                        config_file: None,
                        nickname_candidates: None,
                    },
                );
                config.multi_agent_v2.usage_hint_text =
                    Some("Pinned collaboration usage hint.".to_string());
            }
            Self::V2SubagentSupported
            | Self::V2SubagentUnsupported
            | Self::V2GoalHelperUnsupported => {
                turn.session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    parent_thread_id: ThreadId::new(),
                    depth: 1,
                    agent_path: Some(
                        AgentPath::try_from("/root/contract_worker")
                            .expect("valid test agent path"),
                    ),
                    agent_nickname: None,
                    agent_role: matches!(self, Self::V2GoalHelperUnsupported)
                        .then(|| "goal_supervisor".to_string()),
                });
                update_turn_settings_for_test(turn, |settings| {
                    Arc::make_mut(&mut settings.model_info).multi_agent_version =
                        Some(if matches!(self, Self::V2SubagentSupported) {
                            MultiAgentVersion::V2
                        } else {
                            MultiAgentVersion::Disabled
                        });
                });
            }
            Self::V1Direct | Self::V2Root => {}
        }
        turn.multi_agent_version = config.multi_agent_version_from_features();
        turn.config = Arc::new(config);
    }
}

fn exposure_name(exposure: ToolExposure) -> &'static str {
    match exposure {
        ToolExposure::Direct => "direct",
        ToolExposure::Deferred => "deferred",
        ToolExposure::DeferredModelOnly => "deferred_model_only",
        ToolExposure::DirectModelOnly => "direct_model_only",
        ToolExposure::CodeModeOnly => "code_mode_only",
        ToolExposure::Hidden => "hidden",
    }
}

fn full_function_manifest(tool: &codex_tools::ResponsesApiTool) -> Value {
    json!({
        "name": tool.name,
        "description": tool.description,
        "strict": tool.strict,
        "defer_loading": tool.defer_loading,
        "parameters": tool.parameters,
        "output_schema": tool.output_schema,
    })
}

fn full_spec_manifest(spec: &ToolSpec) -> Value {
    match spec {
        ToolSpec::Function(tool) => json!({
            "type": "function",
            "function": full_function_manifest(tool),
        }),
        ToolSpec::Namespace(namespace) => json!({
            "type": "namespace",
            "name": namespace.name,
            "description": namespace.description,
            "tools": namespace.tools.iter().map(|tool| match tool {
                ResponsesApiNamespaceTool::Function(tool) => json!({
                    "type": "function",
                    "function": full_function_manifest(tool),
                }),
                ResponsesApiNamespaceTool::Custom(tool) => json!({
                    "type": "custom",
                    "tool": tool,
                }),
            }).collect::<Vec<_>>(),
        }),
        ToolSpec::ToolSearch {
            execution,
            description,
            parameters,
        } => json!({
            "type": "tool_search",
            "execution": execution,
            "description": description,
            "parameters": parameters,
        }),
        ToolSpec::WebSearch { .. } | ToolSpec::Freeform(_) => {
            serde_json::to_value(spec).expect("serialize tool spec")
        }
    }
}

fn search_manifest(info: codex_tools::ToolSearchInfo) -> Value {
    json!({
        "search_text": info.entry.search_text,
        "output": info.entry.output,
        "source": info.source_info.map(|source| json!({
            "name": source.name,
            "description": source.description,
        })),
    })
}

fn is_canonical_tool_name(name: &codex_tools::ToolName, scenario: Scenario) -> bool {
    name.namespace.as_deref() == Some(scenario.namespace())
        && scenario.tool_names().contains(&name.name.as_str())
}

fn canonical_visible_specs(specs: &[ToolSpec], scenario: Scenario) -> Vec<ToolSpec> {
    specs
        .iter()
        .filter(|spec| match spec {
            ToolSpec::Namespace(namespace) => namespace.name == scenario.namespace(),
            ToolSpec::Function(tool) => scenario.tool_names().contains(&tool.name.as_str()),
            ToolSpec::ToolSearch { .. } => matches!(scenario, Scenario::V1Deferred),
            ToolSpec::WebSearch { .. } | ToolSpec::Freeform(_) => false,
        })
        .cloned()
        .collect()
}

async fn scenario_manifest_with(
    scenario: Scenario,
    configure: impl FnOnce(&mut TurnContext),
) -> Value {
    let (_session, mut turn) = make_session_and_context().await;
    // Match the inputs used to capture the independent public-upstream oracle.
    assert_eq!(turn.model_info().slug, "gpt-5.5");
    assert!(turn.config.model_catalog.is_none());
    assert!(turn.model_info().used_fallback_model_metadata);
    assert_eq!(turn.config.service_tier, None);
    scenario.configure(&mut turn);
    configure(&mut turn);
    let turn = Arc::new(turn);
    let step_context = StepContext::for_test(Arc::clone(&turn));
    let registry = build_core_tool_registry(
        step_context.turn.as_ref(),
        step_context.settings.model_info.as_ref(),
        &step_context.environments,
        step_context.mcp.as_ref(),
        /*tool_suggest_candidates*/ None,
        /*wait_for_environment_tool_config*/ None,
    );
    let registered = registry
        .entries()
        .filter_map(|entry| {
            let name = entry.runtime.tool_name();
            is_canonical_tool_name(&name, scenario).then(|| {
                json!({
                    "name": name.to_string(),
                    "exposure": exposure_name(entry.exposure),
                    "spec": full_spec_manifest(&entry.runtime.spec()),
                    "search": entry.runtime.search_info().map(search_manifest),
                })
            })
        })
        .collect::<Vec<_>>();
    let router = ToolRouter::from_registry(
        step_context.turn.as_ref(),
        step_context.settings.model_info.as_ref(),
        registry,
        Vec::new(),
        &ToolSearchHandlerCache::default(),
    );
    let all_visible = router.model_visible_specs();
    let visible = canonical_visible_specs(all_visible.as_ref(), scenario);

    json!({
        "name": scenario.name(),
        "namespace": scenario.namespace(),
        "registered": registered,
        "visible_full_specs": visible.iter().map(full_spec_manifest).collect::<Vec<_>>(),
        "responses_api": create_tools_json_for_responses_api(&visible).expect("Responses API tools"),
        "responses_lite": create_tools_json_for_responses_lite(&visible).expect("Responses Lite tools"),
    })
}

async fn scenario_manifest(scenario: Scenario) -> Value {
    scenario_manifest_with(scenario, |_| {}).await
}

async fn collaboration_manifest() -> Value {
    let scenarios = [
        Scenario::V1Direct,
        Scenario::V1Deferred,
        Scenario::V1DepthLimit,
        Scenario::V1UsageHint,
        Scenario::V2Root,
        Scenario::V2WaitDisabled,
        Scenario::V2DirectModelOnly,
        Scenario::V2CodeModeOnly,
        Scenario::V2CodeModeOnlyDirectModel,
        Scenario::V2CustomNamespace,
        Scenario::V2HiddenSpawnMetadata,
        Scenario::V2RoleAndUsageHint,
        Scenario::V2SubagentSupported,
        Scenario::V2SubagentUnsupported,
        Scenario::V2GoalHelperUnsupported,
    ];
    let mut manifests = Map::new();
    for scenario in scenarios {
        manifests.insert(
            scenario.name().to_string(),
            scenario_manifest(scenario).await,
        );
    }
    json!({
        "pinned_upstream": PINNED_UPSTREAM,
        "scenarios": manifests,
    })
}

#[tokio::test]
async fn canonical_collaboration_contract_matches_pinned_upstream() {
    let actual = collaboration_manifest().await;
    let path = codex_utils_cargo_bin::find_resource!(
        "tests/fixtures/collaboration_contract_d9511fb7.json"
    )
    .expect("resolve pinned collaboration contract fixture path");
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|err| {
        panic!(
            "read pinned collaboration contract fixture {}: {err}",
            path.display()
        )
    });
    let expected: Value = serde_json::from_str(&expected).expect("parse contract manifest");
    // JSON object insertion order is not part of the contract; tool arrays remain ordered.
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn frodex_namespaces_do_not_change_the_pinned_collaboration_contract() {
    let expected = scenario_manifest(Scenario::V2Root).await;
    let goal_supervisor = scenario_manifest_with(Scenario::V2Root, |turn| {
        let mut config = (*turn.config).clone();
        config
            .features
            .enable(Feature::GoalSupervisor)
            .expect("GoalSupervisor should be configurable");
        turn.config = Arc::new(config);
    })
    .await;
    assert_eq!(goal_supervisor, expected);
}

#[tokio::test]
async fn custom_model_aliases_do_not_change_the_pinned_collaboration_contract() {
    let expected = scenario_manifest_with(Scenario::V2Root, |turn| {
        let manager = StaticModelsManager::new(
            /*auth_manager*/ None,
            bundled_models_response().expect("bundled model catalog"),
        );
        turn.available_models = manager
            .try_list_upstream_models()
            .expect("static model catalog is unlocked");
    })
    .await;
    let actual = scenario_manifest_with(Scenario::V2Root, |turn| {
        let catalog = bundled_models_response().expect("bundled model catalog");
        let remote_slug = catalog
            .models
            .first()
            .expect("bundled model catalog")
            .slug
            .clone();
        let aliases = HashMap::from([
            (
                "frontier-local".to_string(),
                CustomModelConfig {
                    model: remote_slug.clone(),
                    routing_profile: None,
                    model_context_window: None,
                    model_auto_compact_token_limit: None,
                    trust_candidate_constraints: false,
                },
            ),
            (
                remote_slug,
                CustomModelConfig {
                    model: "alias-collision-must-not-replace-upstream".to_string(),
                    routing_profile: None,
                    model_context_window: None,
                    model_auto_compact_token_limit: None,
                    trust_candidate_constraints: false,
                },
            ),
        ]);
        let manager = StaticModelsManager::new_with_custom_models(
            /*auth_manager*/ None, catalog, aliases,
        );
        turn.available_models = manager
            .try_list_upstream_models()
            .expect("static model catalog is unlocked");
    })
    .await;

    assert_eq!(actual, expected);
}

#[test]
fn canonical_handler_sources_without_internal_adapters_match_pinned_upstream() {
    for (name, actual) in [
        (
            "send_message.rs",
            include_str!("handlers/multi_agents_v2/send_message.rs"),
        ),
        (
            "followup_task.rs",
            include_str!("handlers/multi_agents_v2/followup_task.rs"),
        ),
        ("wait.rs", include_str!("handlers/multi_agents_v2/wait.rs")),
        (
            "interrupt_agent.rs",
            include_str!("handlers/multi_agents_v2/interrupt_agent.rs"),
        ),
    ] {
        let relative_path = format!("tests/fixtures/collaboration_source_d9511fb7/{name}");
        let path = codex_utils_cargo_bin::find_resource!(relative_path)
            .expect("resolve pinned collaboration source fixture");
        let expected = std::fs::read_to_string(path).expect("read pinned collaboration source");
        assert_eq!(actual, expected, "{name} differs from pinned upstream");
    }
}
