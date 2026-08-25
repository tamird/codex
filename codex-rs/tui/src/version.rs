use crate::performance::Deployment;
use std::sync::OnceLock;

/// The current Codex CLI version as embedded at compile time.
pub const CODEX_CLI_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns the user-facing version, including validated local-build provenance.
pub fn display_version() -> &'static str {
    static DISPLAY_VERSION: OnceLock<String> = OnceLock::new();

    DISPLAY_VERSION.get_or_init(|| {
        Deployment::from_environment()
            .as_ref()
            .map(Deployment::display_version)
            .unwrap_or_else(|| CODEX_CLI_VERSION.to_string())
    })
}
