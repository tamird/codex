use std::path::Path;

use crate::existing_rollout_path;
use crate::plain_rollout_path;

/// Compares rollout selections across compression and directory aliases.
///
/// Logical `.jsonl` names can exist only as `.jsonl.zst`, so resolve the physical
/// files before canonicalizing and stripping compression. This does not establish
/// CODEX_HOME confinement or replace a selection check under writer ownership.
pub async fn rollout_paths_match(left: &Path, right: &Path) -> bool {
    if plain_rollout_path(left) == plain_rollout_path(right) {
        return true;
    }
    let (Some(left), Some(right)) = (
        existing_rollout_path(left).await,
        existing_rollout_path(right).await,
    ) else {
        return false;
    };
    let (Ok(left), Ok(right)) = (
        codex_utils_path::normalize_for_path_comparison(left),
        codex_utils_path::normalize_for_path_comparison(right),
    ) else {
        return false;
    };
    plain_rollout_path(&left) == plain_rollout_path(&right)
}

#[cfg(test)]
#[path = "path_identity_tests.rs"]
mod tests;
