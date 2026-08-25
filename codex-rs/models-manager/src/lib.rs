pub mod cache;
pub mod collaboration_mode_presets;
pub(crate) mod config;
pub mod manager;
pub mod model_info;
pub mod model_presets;
pub mod routing;
pub mod test_support;

pub use codex_protocol::auth::AuthMode;
pub use config::CustomModelConfig;
pub use config::ModelRoutingCandidate;
pub use config::ModelRoutingProfile;
pub use config::ModelsManagerConfig;

/// Load the bundled model catalog shipped with `codex-models-manager`.
pub fn bundled_models_response()
-> std::result::Result<codex_protocol::openai_models::ModelsResponse, serde_json::Error> {
    serde_json::from_str(include_str!("../models.json"))
}

/// Return the client version used for Codex catalog requests and cache identity.
pub fn client_version() -> String {
    let whole = client_version_to_whole();
    let prerelease = env!("CARGO_PKG_VERSION_PRE");
    if prerelease.is_empty() {
        whole
    } else {
        format!("{whole}-{prerelease}")
    }
}

/// Return only the major, minor, and patch components of the client version.
pub fn client_version_to_whole() -> String {
    format!(
        "{}.{}.{}",
        env!("CARGO_PKG_VERSION_MAJOR"),
        env!("CARGO_PKG_VERSION_MINOR"),
        env!("CARGO_PKG_VERSION_PATCH")
    )
}
