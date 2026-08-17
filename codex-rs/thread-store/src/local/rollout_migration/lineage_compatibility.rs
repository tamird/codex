//! Rejects migrations that would change a bounded Legacy Desktop history view.
//!
//! A segmented Legacy page is reconstructed from a bounded suffix of the reference graph. Legacy
//! rollback and synthetic item IDs therefore can depend on the oldest segment included in that
//! request. Paginated projection is lineage-wide. Migration may publish only when every bounded
//! Legacy view is an exact ordered subset of the canonical Paginated view.

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;

use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_app_server_protocol::ThreadHistoryItemChange;
use codex_app_server_protocol::ThreadHistoryTurnChange;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::Turn;
use codex_app_server_protocol::TurnItemsView;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::project_rollout_line;
use codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;

use super::MAX_BOUNDED_DESKTOP_COMPATIBILITY_BYTES;
use super::lineage::LegacyLineageMigrationPlan;
use super::lineage_stage::stage_legacy_lineage;
use super::migration_error;
use crate::ThreadStoreResult;

pub(super) async fn validate_bounded_desktop_history(
    codex_home: &Path,
    plan: &LegacyLineageMigrationPlan,
) -> ThreadStoreResult<()> {
    let source_bytes = plan.sources.iter().try_fold(0_u64, |total, source| {
        total
            .checked_add(source.byte_count)
            .ok_or_else(|| migration_error("lineage migration source byte count overflowed"))
    })?;
    ensure_bounded_compatibility_size(source_bytes)?;
    let stage = tempfile::tempdir().map_err(migration_error)?;
    let staged = stage_legacy_lineage(plan, stage.path()).await?;
    let staged_paths = staged
        .iter()
        .map(|target| target.staged_path.clone())
        .collect::<Vec<_>>();
    let selected = plan
        .sources
        .last()
        .ok_or_else(|| migration_error("lineage migration has no selected source"))?;
    let inherited = if let Some(dependency) = plan.history_bases.first() {
        codex_rollout::materialize_rollout_lines(codex_home, selected.path.as_path())
            .await
            .map_err(migration_error)?
            .into_iter()
            .filter(|line| {
                line.ordinal
                    .is_some_and(|ordinal| ordinal < dependency.position.end_ordinal_exclusive)
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let canonical =
        canonical_turns_from_rollouts(inherited.as_slice(), staged_paths.as_slice()).await?;

    let mut materializer =
        codex_rollout::BoundedRolloutMaterializer::new(codex_home, selected.path.as_path());
    let mut reference_limit = DEFAULT_ROLLOUT_REFERENCE_DEPTH;
    loop {
        let bounded = materializer
            .materialize(reference_limit)
            .await
            .map_err(migration_error)?;
        let bounded_turns = turns_from_items(
            bounded.lines.iter().map(|line| &line.item),
            selected.history_mode,
        );
        compare_turns(
            reference_limit,
            bounded_turns.as_slice(),
            canonical.as_slice(),
        )?;
        if !bounded.has_older_reference {
            return Ok(());
        }
        if reference_limit >= codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH {
            return Err(migration_error(format!(
                "bounded Legacy Desktop history exceeds {} references",
                codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH
            )));
        }
        reference_limit = reference_limit
            .checked_mul(2)
            .unwrap_or(codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH)
            .min(codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH);
    }
}

struct CanonicalTurn {
    ordinal: u64,
    turn: Turn,
}

struct CanonicalItem {
    ordinal: u64,
    item: ThreadItem,
}

async fn canonical_turns_from_rollouts(
    inherited: &[codex_rollout::RolloutLine],
    paths: &[PathBuf],
) -> ThreadStoreResult<Vec<Turn>> {
    let mut turns = HashMap::<String, CanonicalTurn>::new();
    let mut items = HashMap::<(String, String), CanonicalItem>::new();
    for line in inherited {
        let ordinal = line
            .ordinal
            .ok_or_else(|| migration_error("inherited Paginated line is missing its ordinal"))?;
        apply_projected_line(&mut turns, &mut items, ordinal, line);
    }
    for path in paths {
        let mut reader = codex_rollout::open_rollout_line_reader(path.as_path())
            .await
            .map_err(migration_error)?;
        while let Some(line) = reader.next_line().await.map_err(migration_error)? {
            let Ok(line) = serde_json::from_str::<codex_rollout::RolloutLine>(line.as_str()) else {
                continue;
            };
            let ordinal = line
                .ordinal
                .ok_or_else(|| migration_error("staged rollout line is missing its ordinal"))?;
            apply_projected_line(&mut turns, &mut items, ordinal, &line);
        }
    }
    let mut items_by_turn = HashMap::<String, Vec<CanonicalItem>>::new();
    for ((turn_id, _), item) in items {
        items_by_turn.entry(turn_id).or_default().push(item);
    }
    let mut turns = turns.into_values().collect::<Vec<_>>();
    turns.sort_by_key(|turn| turn.ordinal);
    Ok(turns
        .into_iter()
        .map(|mut turn| {
            let mut turn_items = items_by_turn.remove(&turn.turn.id).unwrap_or_default();
            turn_items.sort_by_key(|item| item.ordinal);
            turn.turn.items = turn_items.into_iter().map(|item| item.item).collect();
            turn.turn
        })
        .collect())
}

fn apply_projected_line(
    turns: &mut HashMap<String, CanonicalTurn>,
    items: &mut HashMap<(String, String), CanonicalItem>,
    ordinal: u64,
    line: &codex_rollout::RolloutLine,
) {
    let changes = project_rollout_line(line);
    for turn_id in changes.removed_turn_ids {
        turns.remove(turn_id.as_str());
        items.retain(|(item_turn_id, _), _| item_turn_id != &turn_id);
    }
    for turn in changes.changed_turns {
        apply_turn_change(turns, ordinal, turn);
    }
    for item in changes.changed_items {
        apply_item_change(items, ordinal, item);
    }
}

fn ensure_bounded_compatibility_size(source_bytes: u64) -> ThreadStoreResult<()> {
    if source_bytes <= MAX_BOUNDED_DESKTOP_COMPATIBILITY_BYTES {
        return Ok(());
    }
    Err(migration_error(format!(
        "bounded Legacy Desktop compatibility proof requires {source_bytes} source bytes, which exceeds the fixed {MAX_BOUNDED_DESKTOP_COMPATIBILITY_BYTES}-byte memory-safety limit; source files were not changed"
    )))
}

fn apply_turn_change(
    turns: &mut HashMap<String, CanonicalTurn>,
    ordinal: u64,
    change: ThreadHistoryTurnChange,
) {
    if let Some(existing) = turns.get_mut(change.turn_id.as_str()) {
        if existing.turn.status == TurnStatus::InProgress {
            existing.turn.status = change.status;
            existing.turn.error = change.error;
            existing.turn.started_at = change.started_at;
            existing.turn.completed_at = change.completed_at;
            existing.turn.duration_ms = change.duration_ms;
        }
        return;
    }
    turns.insert(
        change.turn_id.clone(),
        CanonicalTurn {
            ordinal,
            turn: Turn {
                id: change.turn_id,
                items: Vec::new(),
                items_view: TurnItemsView::Full,
                status: change.status,
                error: change.error,
                started_at: change.started_at,
                completed_at: change.completed_at,
                duration_ms: change.duration_ms,
            },
        },
    );
}

fn apply_item_change(
    items: &mut HashMap<(String, String), CanonicalItem>,
    ordinal: u64,
    change: ThreadHistoryItemChange,
) {
    let key = (change.turn_id, change.item.id().to_string());
    if let Some(existing) = items.get_mut(&key) {
        existing.item = change.item;
    } else {
        items.insert(
            key,
            CanonicalItem {
                ordinal,
                item: change.item,
            },
        );
    }
}

fn turns_from_items<'a>(
    items: impl IntoIterator<Item = &'a RolloutItem>,
    history_mode: ThreadHistoryMode,
) -> Vec<Turn> {
    let mut builder = ThreadHistoryBuilder::new();
    for item in items {
        if codex_rollout::is_persisted_rollout_item(item, history_mode) {
            builder.handle_rollout_item(item);
        }
    }
    builder.finish()
}

fn compare_turns(
    reference_limit: usize,
    bounded: &[Turn],
    canonical: &[Turn],
) -> ThreadStoreResult<()> {
    let canonical_by_id = canonical
        .iter()
        .enumerate()
        .map(|(index, turn)| (turn.id.as_str(), (index, turn)))
        .collect::<HashMap<_, _>>();
    let mut previous_index = None;
    let mut matched_migrated_turn = false;
    for turn in bounded {
        let Some((index, canonical_turn)) = canonical_by_id.get(turn.id.as_str()).copied() else {
            if matched_migrated_turn {
                return Err(incompatible(
                    reference_limit,
                    turn.id.as_str(),
                    "turn is absent",
                ));
            }
            // Existing Paginated history_base and immutable-reference dependencies remain
            // byte-for-byte unchanged. They precede the Legacy suffix staged by this migration,
            // so their turns are intentionally absent from `canonical`.
            continue;
        };
        matched_migrated_turn = true;
        if previous_index.is_some_and(|previous| index <= previous) {
            return Err(incompatible(
                reference_limit,
                turn.id.as_str(),
                "turn order changed",
            ));
        }
        if canonical_turn != turn {
            let reason = first_turn_difference(turn, canonical_turn);
            return Err(incompatible(
                reference_limit,
                turn.id.as_str(),
                reason.as_str(),
            ));
        }
        previous_index = Some(index);
    }
    if !canonical.is_empty() && !matched_migrated_turn {
        let bounded_ids = bounded
            .iter()
            .map(|turn| turn.id.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let canonical_ids = canonical
            .iter()
            .map(|turn| turn.id.as_str())
            .collect::<Vec<_>>()
            .join(",");
        return Err(migration_error(format!(
            "bounded Legacy Desktop history is not canonical at reference depth {reference_limit}: no migrated turn retained its stable ID (bounded [{bounded_ids}], migrated [{canonical_ids}]); source files were not changed"
        )));
    }
    Ok(())
}

fn first_turn_difference(bounded: &Turn, canonical: &Turn) -> String {
    if bounded.status != canonical.status {
        "turn status changed".to_string()
    } else if bounded.error != canonical.error {
        "turn error changed".to_string()
    } else if bounded.items_view != canonical.items_view {
        "turn items view changed".to_string()
    } else if bounded.started_at != canonical.started_at {
        "turn start timestamp changed".to_string()
    } else if bounded.completed_at != canonical.completed_at {
        "turn completion timestamp changed".to_string()
    } else if bounded.duration_ms != canonical.duration_ms {
        "turn duration changed".to_string()
    } else if bounded.items.len() != canonical.items.len() {
        format!(
            "turn item count changed from {} to {}",
            bounded.items.len(),
            canonical.items.len()
        )
    } else if let Some((bounded, canonical)) = bounded
        .items
        .iter()
        .zip(&canonical.items)
        .find(|(bounded, canonical)| bounded.id() != canonical.id())
    {
        format!(
            "synthetic item ID changed from {} to {}",
            bounded.id(),
            canonical.id()
        )
    } else {
        "turn item content changed".to_string()
    }
}

fn incompatible(reference_limit: usize, turn_id: &str, reason: &str) -> crate::ThreadStoreError {
    migration_error(format!(
        "bounded Legacy Desktop history is not canonical at reference depth {reference_limit}: turn {turn_id} {reason}; source files were not changed"
    ))
}

#[cfg(test)]
mod tests {
    use super::MAX_BOUNDED_DESKTOP_COMPATIBILITY_BYTES;
    use super::ensure_bounded_compatibility_size;

    #[test]
    fn bounded_compatibility_size_limit_is_inclusive_and_fail_closed() {
        assert!(ensure_bounded_compatibility_size(MAX_BOUNDED_DESKTOP_COMPATIBILITY_BYTES).is_ok());
        let error = ensure_bounded_compatibility_size(MAX_BOUNDED_DESKTOP_COMPATIBILITY_BYTES + 1)
            .expect_err("oversized compatibility proof must fail closed");
        assert!(error.to_string().contains("source files were not changed"));
    }
}
