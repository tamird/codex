//! Durable environment selection identities, without attachment-owned configuration.
//!
//! Cold resume cannot reacquire owner policies by itself, so those attachments restore as failed
//! until their owner reattaches configuration. Readers predating the ownership marker must not
//! resume these owner records: they would incorrectly infer permissions from thread settings.

use crate::environment::EnvironmentConfigState;
use crate::protocol::TurnEnvironmentSelection;
use crate::protocol::TurnEnvironmentSelections;
use codex_utils_absolute_path::AbsolutePathBuf;
use codex_utils_path_uri::PathUri;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use ts_rs::TS;

/// Sticky environment identities and the fallback directory needed after resume.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, JsonSchema, TS)]
pub struct PersistedTurnEnvironmentSelections {
    pub legacy_fallback_cwd: AbsolutePathBuf,
    pub environments: Vec<PersistedTurnEnvironmentSelection>,
}

/// A persisted attachment preserves its configuration owner, never that owner's policies.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, JsonSchema, TS)]
pub struct PersistedTurnEnvironmentSelection {
    pub environment_id: String,
    pub cwd: PathUri,
    pub workspace_roots: Vec<PathUri>,
    #[serde(default)]
    pub config_origin: PersistedEnvironmentConfigOrigin,
}

/// Legacy selections used thread configuration; owner-provided configuration must be reacquired.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename_all = "snake_case")]
pub enum PersistedEnvironmentConfigOrigin {
    #[default]
    Thread,
    Owner,
}

impl From<TurnEnvironmentSelections> for PersistedTurnEnvironmentSelections {
    fn from(selections: TurnEnvironmentSelections) -> Self {
        Self {
            legacy_fallback_cwd: selections.legacy_fallback_cwd,
            environments: selections
                .environments
                .into_iter()
                .map(|selection| PersistedTurnEnvironmentSelection {
                    environment_id: selection.environment_id,
                    cwd: selection.cwd,
                    workspace_roots: selection.workspace_roots,
                    config_origin: match selection.config {
                        EnvironmentConfigState::FromThread => {
                            PersistedEnvironmentConfigOrigin::Thread
                        }
                        EnvironmentConfigState::Pending
                        | EnvironmentConfigState::Ready(_)
                        | EnvironmentConfigState::Failed(_) => {
                            PersistedEnvironmentConfigOrigin::Owner
                        }
                    },
                })
                .collect(),
        }
    }
}

impl From<PersistedTurnEnvironmentSelections> for TurnEnvironmentSelections {
    fn from(selections: PersistedTurnEnvironmentSelections) -> Self {
        Self {
            legacy_fallback_cwd: selections.legacy_fallback_cwd,
            environments: selections
                .environments
                .into_iter()
                .map(|selection| TurnEnvironmentSelection {
                    environment_id: selection.environment_id,
                    cwd: selection.cwd,
                    workspace_roots: selection.workspace_roots,
                    config: match selection.config_origin {
                        PersistedEnvironmentConfigOrigin::Thread => EnvironmentConfigState::FromThread,
                        PersistedEnvironmentConfigOrigin::Owner => EnvironmentConfigState::Failed(
                            "This persisted environment requires its owner to reattach configuration before it can be used.".to_string(),
                        ),
                    },
                })
                .collect(),
        }
    }
}
