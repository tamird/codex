use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::agents_md::LoadedAgentsMd;
use crate::config::TokenBudgetConfig;
use crate::environment_selection::TurnEnvironmentSnapshot;
use crate::session::step_settings::ResolvedStepSettings;
use crate::session::turn_context::TurnContext;
use crate::tools::router::ToolRouter;
use codex_exec_server::ExecutorCapabilityDiscoverySnapshot;
use codex_exec_server::ResolvedSelectedCapabilityRoot;
use codex_mcp::McpBinding;
use codex_otel::SessionTelemetry;

/// Request-scoped state that may change between model sampling requests.
pub(crate) struct StepContext {
    pub(crate) turn: Arc<TurnContext>,
    /// One immutable settings version captured before request preparation.
    pub(crate) settings: Arc<ResolvedStepSettings>,
    /// Frozen turn preferences resolved against this step's captured model.
    pub(crate) token_budget: Option<TokenBudgetConfig>,
    /// Telemetry context tagged with this sampling request's model.
    pub(crate) session_telemetry: SessionTelemetry,
    pub(crate) environments: TurnEnvironmentSnapshot,
    /// Capability roots bound to ready environments in this exact step.
    pub(crate) selected_capability_roots: Vec<ResolvedSelectedCapabilityRoot>,
    /// Executor-materialized capability files shared by MCP and skills in this exact step.
    pub(crate) executor_capability_discovery: Option<Arc<ExecutorCapabilityDiscoverySnapshot>>,
    /// The exact MCP connections, configuration, and catalog captured for this step.
    pub(crate) mcp: Arc<McpBinding>,
    /// MCP servers required by user input included in this sampling request.
    ///
    /// A model reroute preserves these names when it rebuilds the step so steering input does not
    /// lose a server that was not required by the original turn input.
    pub(crate) required_mcp_servers: Vec<String>,
    /// The finalized tool plan advertised and executed for this exact sampling request.
    pub(crate) tool_router: Arc<ToolRouter>,
    /// The canonical AGENTS.md value observed with this environment snapshot.
    pub(crate) loaded_agents_md: Option<Arc<LoadedAgentsMd>>,
    /// Coordinates a tool-triggered context transition at a model sampling boundary.
    pub(super) context_transition: ContextTransitionState,
}

/// Request-scoped state for a tool that replaces the active turn context.
///
/// A transition tool must be the only tool call in its model response. Once it succeeds, the
/// caller rebuilds the turn context before sending another model request.
#[derive(Debug, Default)]
pub(super) struct ContextTransitionState {
    mixed_with_sibling_tool: AtomicBool,
    refresh_requested: AtomicBool,
}

impl StepContext {
    pub(crate) fn reject_context_transition_mixed_with_sibling_tool(&self) {
        self.context_transition
            .mixed_with_sibling_tool
            .store(true, Ordering::Release);
    }

    pub(crate) fn context_transition_has_sibling_tool(&self) -> bool {
        self.context_transition
            .mixed_with_sibling_tool
            .load(Ordering::Acquire)
    }

    pub(crate) fn request_turn_context_refresh(&self) {
        self.context_transition
            .refresh_requested
            .store(true, Ordering::Release);
    }

    pub(crate) fn turn_context_refresh_requested(&self) -> bool {
        self.context_transition
            .refresh_requested
            .load(Ordering::Acquire)
    }
}
