//! Resolves selected custom-model aliases when validated configuration changes.

use std::collections::HashMap;

use codex_models_manager::CustomModelConfig;
use codex_models_manager::ModelRoutingCandidate;

/// Reapplies custom-model overrides that exist only in a materialized runtime config.
///
/// Reloading a user layer rebuilds its derived aliases from `ConfigLayerStack`. Runtime callers
/// may also supply a validated `Config` whose custom-model map differs from that derivation. The
/// difference is a runtime override and must survive an unrelated user-layer reload.
pub(super) fn preserve_materialized_custom_model_overrides(
    previous_layer_models: &HashMap<String, CustomModelConfig>,
    previous_materialized_models: &HashMap<String, CustomModelConfig>,
    next_layer_models: &mut HashMap<String, CustomModelConfig>,
) {
    for (alias, previous_layer_model) in previous_layer_models {
        match previous_materialized_models.get(alias) {
            Some(materialized_model) if materialized_model == previous_layer_model => {}
            Some(materialized_model) => {
                next_layer_models.insert(alias.clone(), materialized_model.clone());
            }
            None => {
                next_layer_models.remove(alias);
            }
        }
    }
    for (alias, materialized_model) in previous_materialized_models {
        if !previous_layer_models.contains_key(alias) {
            next_layer_models.insert(alias.clone(), materialized_model.clone());
        }
    }
}

/// The selected-model update required after replacing the custom-model map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SelectedAliasUpdate {
    Unchanged,
    Renamed { alias: String },
    DetachedProfile { candidate: ModelRoutingCandidate },
    DetachedAlias { model: String },
}

/// Resolves an alias removal without retaining deleted configuration or guessing an ambiguous
/// rename.
pub(super) fn selected_alias_update(
    selected_model: &str,
    previous: &HashMap<String, CustomModelConfig>,
    next: &HashMap<String, CustomModelConfig>,
    last_success: Option<&ModelRoutingCandidate>,
) -> SelectedAliasUpdate {
    if next.contains_key(selected_model) {
        return SelectedAliasUpdate::Unchanged;
    }
    let Some(previous_alias) = previous.get(selected_model) else {
        return SelectedAliasUpdate::Unchanged;
    };

    let mut rename_matches = next
        .iter()
        .filter(|(alias, config)| !previous.contains_key(*alias) && *config == previous_alias);
    if let Some((alias, _)) = rename_matches.next()
        && rename_matches.next().is_none()
    {
        return SelectedAliasUpdate::Renamed {
            alias: alias.clone(),
        };
    }

    let Some(candidates) = previous_alias.routing_candidates() else {
        return SelectedAliasUpdate::DetachedAlias {
            model: previous_alias.model.clone(),
        };
    };
    let Some(candidate) = last_success
        .filter(|candidate| candidates.contains(*candidate))
        .cloned()
        .or_else(|| candidates.first().cloned())
    else {
        return SelectedAliasUpdate::DetachedAlias {
            model: previous_alias.model.clone(),
        };
    };
    SelectedAliasUpdate::DetachedProfile { candidate }
}

#[cfg(test)]
#[path = "model_alias_refresh_tests.rs"]
mod tests;
