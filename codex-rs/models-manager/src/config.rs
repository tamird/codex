use codex_protocol::config_types::Personality;
use codex_protocol::openai_models::ModelsResponse;
use codex_protocol::openai_models::ReasoningEffort;
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct ModelsManagerConfig {
    pub model_context_window: Option<i64>,
    pub model_auto_compact_token_limit: Option<i64>,
    pub tool_output_token_limit: Option<usize>,
    pub base_instructions: Option<String>,
    pub personality_enabled: bool,
    pub personality: Option<Personality>,
    pub model_catalog: Option<ModelsResponse>,
    pub custom_models: HashMap<String, CustomModelConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomModelConfig {
    /// Provider-facing model slug used for direct aliases and picker metadata.
    pub model: String,
    /// Ordered request configurations when this alias is a routing profile.
    pub routing_profile: Option<ModelRoutingProfile>,
    /// Optional context window override applied to every candidate in this alias.
    pub model_context_window: Option<i64>,
    /// Optional auto-compaction limit applied to every candidate in this alias.
    pub model_auto_compact_token_limit: Option<i64>,
}

impl CustomModelConfig {
    /// Returns the ordered candidates when this custom model is a routing profile.
    pub fn routing_candidates(&self) -> Option<&[ModelRoutingCandidate]> {
        self.routing_profile
            .as_ref()
            .map(|profile| profile.candidates.as_slice())
    }
}

/// Ordered concrete request configurations behind one custom model alias.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelRoutingProfile {
    /// Candidates in descending preference order.
    pub candidates: Vec<ModelRoutingCandidate>,
}

/// One model, reasoning effort, and service tier tuple in a routing profile.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelRoutingCandidate {
    /// Provider-facing model slug used on the request.
    pub model: String,
    /// Reasoning effort override used on the request.
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Service tier override used on the request.
    pub service_tier: Option<String>,
}
