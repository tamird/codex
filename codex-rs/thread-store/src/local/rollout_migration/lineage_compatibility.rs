//! Preserves the initial bounded Legacy response while converting it to Paginated history.
//!
//! `ThreadHistoryBuilder` numbers synthesized items from the oldest segment included in one
//! request. The initial Desktop request includes only the active segment and two predecessors, so
//! its IDs can differ from a complete-lineage replay. Migration keeps the IDs from that initial
//! response and gives older colliding items new explicit IDs. Paginated reads then use one stable
//! ID for every item regardless of page depth.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_app_server_protocol::ThreadHistoryChangeSet;
use codex_app_server_protocol::ThreadHistoryItemChange;
use codex_app_server_protocol::ThreadHistoryTurnChange;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::Turn;
use codex_app_server_protocol::TurnItemsView;
use codex_app_server_protocol::TurnStatus;
use codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;

use super::canonical_projection::project_canonical_record;
use super::jsonl_spans::JsonlSpanKind;
use super::lineage::LegacyLineageMigrationPlan;
use super::lineage_rewrite::rewrite_generated_item_ids;
use super::lineage_stage::StagedLineageTarget;
use super::lineage_stage::stage_legacy_lineage;
use super::migration_error;
use crate::ThreadStoreResult;

pub(super) async fn validate_bounded_desktop_history(
    codex_home: &Path,
    plan: &mut LegacyLineageMigrationPlan,
) -> ThreadStoreResult<()> {
    let stage = tempfile::tempdir().map_err(migration_error)?;
    stage_compatible_lineage(codex_home, plan, stage.path()).await?;
    Ok(())
}

/// Returns the exact staged files whose visible Legacy history was checked. Publication must use
/// these files rather than replaying the entire source lineage after the check succeeds.
pub(super) async fn stage_compatible_lineage(
    codex_home: &Path,
    plan: &mut LegacyLineageMigrationPlan,
    stage_root: &Path,
) -> ThreadStoreResult<Vec<StagedLineageTarget>> {
    // Derive the remap from original generated IDs even when a caller reuses a validated plan.
    plan.synthetic_item_id_remap.clear();
    if plan
        .sources
        .iter()
        .all(|source| source.history_mode == ThreadHistoryMode::Paginated)
    {
        return stage_legacy_lineage(plan, stage_root).await;
    }
    let removed_turn_ids = if plan.replay_native_rollbacks {
        super::lineage_stage::build_rollback_plan(
            plan,
            &mut super::turn_context_cache::TurnContextCache::default(),
        )
        .await?
        .map(|plan| plan.removed_turn_ids().clone())
        .unwrap_or_default()
    } else {
        HashSet::new()
    };
    let selected = plan
        .sources
        .last()
        .ok_or_else(|| migration_error("lineage migration has no selected source"))?;
    let mut materializer =
        codex_rollout::BoundedRolloutMaterializer::new(codex_home, selected.path.as_path())
            .retaining_source_metadata();
    let reference_limit = DEFAULT_ROLLOUT_REFERENCE_DEPTH;
    let initial = materializer
        .materialize(reference_limit)
        .await
        .map_err(migration_error)?;
    let initial_turns = turns_from_items(
        initial.lines.iter().map(|line| &line.item),
        selected.history_mode,
    );
    let retained_turn_ids = initial_turns
        .iter()
        .map(|turn| turn.id.clone())
        .collect::<HashSet<_>>();
    let retained_item_ids = initial_turns
        .iter()
        .flat_map(|turn| turn.items.iter().map(|item| item.id().to_string()))
        .collect::<HashSet<_>>();

    let mut staged = stage_legacy_lineage(plan, stage_root).await?;
    let mut canonical = canonical_turns_from_rollouts(
        staged_paths(staged.as_slice()).as_slice(),
        &retained_turn_ids,
        &retained_item_ids,
    )
    .await?;
    if !canonical.all_turn_ids.is_disjoint(&removed_turn_ids) {
        return Err(migration_error(
            "mixed-format rollback retained a removed turn; source files were not changed",
        ));
    }
    let generated_items = staged
        .iter()
        .flat_map(|target| target.generated_item_edits.iter())
        .map(|edit| (edit.turn_id.as_str(), edit.item_id.as_str()))
        .collect::<HashSet<_>>();
    plan.synthetic_item_id_remap = derive_initial_synthetic_item_id_remap(
        reference_limit,
        initial_turns.as_slice(),
        canonical.turns.as_slice(),
        &canonical.retained_item_ids,
        canonical.max_synthetic_item_index,
        &generated_items,
    )?;
    if !plan.synthetic_item_id_remap.is_empty() {
        tracing::info!(thread_id = %plan.selected_thread_id, remapped_ids = plan.synthetic_item_id_remap.len(), "rewriting generated IDs to preserve initial Desktop item IDs");
        rewrite_generated_item_ids(&mut staged, &plan.synthetic_item_id_remap).await?;
        canonical = canonical_turns_from_rollouts(
            staged_paths(staged.as_slice()).as_slice(),
            &retained_turn_ids,
            &retained_item_ids,
        )
        .await?;
    }
    compare_turns(
        reference_limit,
        initial_turns.as_slice(),
        canonical.turns.as_slice(),
        /*compare_item_ids*/ true,
    )?;
    Ok(staged)
}

fn staged_paths(staged: &[super::lineage_stage::StagedLineageTarget]) -> Vec<PathBuf> {
    staged
        .iter()
        .map(|target| target.staged_path.clone())
        .collect()
}

struct CanonicalTurn {
    ordinal: u64,
    turn: Turn,
}

struct CanonicalItem {
    ordinal: u64,
    item: ThreadItem,
}

/// Canonical state needed to preserve the existing bounded Desktop response.
///
/// The staged lineage can be arbitrarily large. This projection retains only turns already
/// visible in the bounded Legacy response, the subset of generated IDs that could collide with
/// those turns, and the largest generated ID needed to allocate collision-free replacements.
struct CanonicalProjection {
    /// Every emitted turn, including turns outside the bounded Desktop response.
    all_turn_ids: HashSet<String>,
    /// Canonical versions of turns visible before migration.
    turns: Vec<Turn>,
    /// Canonical item IDs that are also used by the bounded Legacy response.
    retained_item_ids: HashSet<String>,
    /// Largest numeric suffix observed across every staged `item-N` ID.
    max_synthetic_item_index: u64,
}

/// Incremental projection restricted to the bounded Desktop response.
struct CanonicalProjectionBuilder<'a> {
    all_turn_ids: HashSet<String>,
    turns: HashMap<String, CanonicalTurn>,
    items: HashMap<(String, String), CanonicalItem>,
    retained_item_owners: HashMap<String, HashSet<String>>,
    max_synthetic_item_index: u64,
    retained_turn_ids: &'a HashSet<String>,
    retained_item_ids: &'a HashSet<String>,
}

impl<'a> CanonicalProjectionBuilder<'a> {
    fn new(retained_turn_ids: &'a HashSet<String>, retained_item_ids: &'a HashSet<String>) -> Self {
        Self {
            all_turn_ids: HashSet::new(),
            turns: HashMap::new(),
            items: HashMap::new(),
            retained_item_owners: HashMap::new(),
            max_synthetic_item_index: 0,
            retained_turn_ids,
            retained_item_ids,
        }
    }

    fn apply(&mut self, ordinal: u64, changes: ThreadHistoryChangeSet) {
        for turn_id in changes.removed_turn_ids {
            self.all_turn_ids.remove(&turn_id);
            for owners in self.retained_item_owners.values_mut() {
                owners.remove(turn_id.as_str());
            }
            if self.retained_turn_ids.contains(turn_id.as_str()) {
                self.turns.remove(turn_id.as_str());
                self.items
                    .retain(|(item_turn_id, _), _| item_turn_id != &turn_id);
            }
        }
        for turn in changes.changed_turns {
            self.all_turn_ids.insert(turn.turn_id.clone());
            if self.retained_turn_ids.contains(turn.turn_id.as_str()) {
                apply_turn_change(&mut self.turns, ordinal, turn);
            }
        }
        for item in changes.changed_items {
            let item_id = item.item.id().to_string();
            if self.retained_item_ids.contains(item_id.as_str()) {
                self.retained_item_owners
                    .entry(item_id.clone())
                    .or_default()
                    .insert(item.turn_id.clone());
            }
            if let Some(index) = item_id
                .strip_prefix("item-")
                .and_then(|index| index.parse::<u64>().ok())
            {
                self.max_synthetic_item_index = self.max_synthetic_item_index.max(index);
            }
            if self.retained_turn_ids.contains(item.turn_id.as_str()) {
                apply_item_change(&mut self.items, ordinal, item);
            }
        }
    }

    fn finish(self) -> CanonicalProjection {
        let mut items_by_turn = HashMap::<String, Vec<CanonicalItem>>::new();
        for ((turn_id, _), item) in self.items {
            items_by_turn.entry(turn_id).or_default().push(item);
        }
        let mut turns = self.turns.into_values().collect::<Vec<_>>();
        turns.sort_by_key(|turn| turn.ordinal);
        let turns = turns
            .into_iter()
            .map(|mut turn| {
                let mut turn_items = items_by_turn.remove(&turn.turn.id).unwrap_or_default();
                turn_items.sort_by_key(|item| item.ordinal);
                turn.turn.items = turn_items.into_iter().map(|item| item.item).collect();
                turn.turn
            })
            .collect();
        CanonicalProjection {
            all_turn_ids: self.all_turn_ids,
            turns,
            retained_item_ids: self
                .retained_item_owners
                .into_iter()
                .filter_map(|(item_id, owners)| (!owners.is_empty()).then_some(item_id))
                .collect(),
            max_synthetic_item_index: self.max_synthetic_item_index,
        }
    }
}

async fn canonical_turns_from_rollouts(
    paths: &[PathBuf],
    retained_turn_ids: &HashSet<String>,
    retained_item_ids: &HashSet<String>,
) -> ThreadStoreResult<CanonicalProjection> {
    let mut projection = CanonicalProjectionBuilder::new(retained_turn_ids, retained_item_ids);
    for path in paths {
        let file = tokio::fs::File::open(path).await.map_err(migration_error)?;
        let mut reader =
            tokio::io::BufReader::with_capacity(super::PROJECTION_BATCH_BYTES as usize, file);
        let mut bytes = Vec::new();
        while super::canonical_projection::read_canonical_chunk(&mut reader, &mut bytes).await? {
            for span in super::canonical_projection::candidate_spans(&bytes)? {
                if span.kind == JsonlSpanKind::Copy {
                    continue;
                }
                for record in bytes[span.range].split_inclusive(|byte| *byte == b'\n') {
                    let line = project_canonical_record(record).map_err(migration_error)?;
                    projection.apply(line.ordinal, line.changes);
                }
            }
        }
    }
    Ok(projection.finish())
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
    replay_materialized_history(items, history_mode).finish()
}

/// Replays source-format transitions without counting them as additional physical records.
pub(super) fn replay_materialized_history<'a>(
    items: impl IntoIterator<Item = &'a RolloutItem>,
    mut history_mode: ThreadHistoryMode,
) -> ThreadHistoryBuilder {
    let mut builder = ThreadHistoryBuilder::new();
    let mut saw_metadata = false;
    for item in items {
        if let RolloutItem::SessionMeta(metadata) = item {
            history_mode = metadata.meta.history_mode;
            if !saw_metadata {
                builder.handle_rollout_item(item);
                saw_metadata = true;
            }
            continue;
        }
        if !codex_rollout::is_persisted_rollout_item(item, history_mode) {
            continue;
        }
        if history_mode == ThreadHistoryMode::Paginated {
            builder.handle_paginated_rollout_item(item);
        } else {
            builder.handle_rollout_item(item);
        }
    }
    builder
}

fn derive_initial_synthetic_item_id_remap(
    reference_limit: usize,
    bounded: &[Turn],
    canonical: &[Turn],
    canonical_retained_item_ids: &HashSet<String>,
    canonical_max_synthetic_item_index: u64,
    generated_items: &HashSet<(&str, &str)>,
) -> ThreadStoreResult<HashMap<String, String>> {
    let canonical_by_id = canonical
        .iter()
        .enumerate()
        .map(|(index, turn)| (turn.id.as_str(), (index, turn)))
        .collect::<HashMap<_, _>>();
    let mut previous_index = None;
    let mut matched_migrated_turn = false;
    let mut visible_ids = HashMap::<String, String>::new();
    // Collision replacements affect immutable target bytes. Allocate them in a stable order.
    let mut desired_owners = BTreeMap::<String, String>::new();
    for turn in bounded {
        let Some((index, canonical_turn)) = canonical_by_id.get(turn.id.as_str()).copied() else {
            if matched_migrated_turn {
                return Err(incompatible(
                    reference_limit,
                    turn.id.as_str(),
                    "turn is absent",
                ));
            }
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
        if let Some(reason) =
            first_turn_difference(turn, canonical_turn, /*compare_item_ids*/ false)?
        {
            return Err(incompatible(
                reference_limit,
                turn.id.as_str(),
                reason.as_str(),
            ));
        }
        for (bounded_item, canonical_item) in turn.items.iter().zip(&canonical_turn.items) {
            let canonical_id = canonical_item.id().to_string();
            let desired_id = bounded_item.id().to_string();
            // Native ancestors may already use an explicit `item-N`. Only IDs allocated by
            // this migration participate in the generated-ID rewrite.
            if generated_items.contains(&(turn.id.as_str(), canonical_id.as_str())) {
                if let Some(previous) = visible_ids.insert(canonical_id.clone(), desired_id.clone())
                    && previous != desired_id
                {
                    return Err(migration_error(format!(
                        "Legacy item {canonical_id} has two initial Desktop IDs: {previous} and {desired_id}"
                    )));
                }
            } else if canonical_id != desired_id {
                return Err(incompatible(
                    reference_limit,
                    &turn.id,
                    "explicit native item ID changed",
                ));
            }
            if let Some(previous_owner) =
                desired_owners.insert(desired_id.clone(), canonical_id.clone())
                && previous_owner != canonical_id
            {
                return Err(migration_error(format!(
                    "initial Legacy Desktop ID {desired_id} identifies both {previous_owner} and {canonical_id}"
                )));
            }
        }
        previous_index = Some(index);
    }
    if !canonical.is_empty() && !matched_migrated_turn {
        return Err(no_migrated_turn_error(reference_limit, bounded, canonical));
    }

    let mut next_item_index = desired_owners
        .keys()
        .filter_map(|id| id.strip_prefix("item-")?.parse::<u64>().ok())
        .max()
        .unwrap_or(0)
        .max(canonical_max_synthetic_item_index)
        .checked_add(1)
        .ok_or_else(|| migration_error("Legacy synthetic item ID overflowed"))?;
    let mut remap = visible_ids
        .iter()
        .filter(|(canonical_id, desired_id)| canonical_id != desired_id)
        .map(|(canonical_id, desired_id)| (canonical_id.clone(), desired_id.clone()))
        .collect::<HashMap<_, _>>();
    for (desired_id, canonical_owner) in &desired_owners {
        // An existing entry already moves the canonical owner away from this ID. Replacing that
        // entry would lose the bounded response's requested mapping when both IDs are visible.
        if desired_id == canonical_owner
            || remap.contains_key(desired_id)
            || !canonical_retained_item_ids.contains(desired_id)
        {
            continue;
        }
        let replacement = format!("item-{next_item_index}");
        next_item_index = next_item_index
            .checked_add(1)
            .ok_or_else(|| migration_error("Legacy synthetic item ID overflowed"))?;
        remap.insert(desired_id.clone(), replacement);
    }
    Ok(remap)
}

fn compare_turns(
    reference_limit: usize,
    bounded: &[Turn],
    canonical: &[Turn],
    compare_item_ids: bool,
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
        if let Some(reason) = first_turn_difference(turn, canonical_turn, compare_item_ids)? {
            return Err(incompatible(
                reference_limit,
                turn.id.as_str(),
                reason.as_str(),
            ));
        }
        previous_index = Some(index);
    }
    if !canonical.is_empty() && !matched_migrated_turn {
        return Err(no_migrated_turn_error(reference_limit, bounded, canonical));
    }
    Ok(())
}

fn first_turn_difference(
    bounded: &Turn,
    canonical: &Turn,
    compare_item_ids: bool,
) -> ThreadStoreResult<Option<String>> {
    if bounded.status != canonical.status {
        Ok(Some("turn status changed".to_string()))
    } else if bounded.error != canonical.error {
        Ok(Some("turn error changed".to_string()))
    } else if bounded.items_view != canonical.items_view {
        Ok(Some("turn items view changed".to_string()))
    } else if bounded.started_at != canonical.started_at {
        Ok(Some("turn start timestamp changed".to_string()))
    } else if bounded.completed_at != canonical.completed_at {
        Ok(Some("turn completion timestamp changed".to_string()))
    } else if bounded.duration_ms != canonical.duration_ms {
        Ok(Some("turn duration changed".to_string()))
    } else if bounded.items.len() != canonical.items.len() {
        Ok(Some(format!(
            "turn item count changed from {} to {}",
            bounded.items.len(),
            canonical.items.len()
        )))
    } else {
        for (bounded_item, canonical_item) in bounded.items.iter().zip(&canonical.items) {
            if item_without_id(bounded_item)? != item_without_id(canonical_item)? {
                return Ok(Some("turn item content changed".to_string()));
            }
            if compare_item_ids && bounded_item.id() != canonical_item.id() {
                return Ok(Some(format!(
                    "synthetic item ID changed from {} to {}",
                    bounded_item.id(),
                    canonical_item.id()
                )));
            }
        }
        Ok(None)
    }
}

fn item_without_id(item: &ThreadItem) -> ThreadStoreResult<serde_json::Value> {
    let mut value = serde_json::to_value(item).map_err(migration_error)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| migration_error("ThreadItem did not serialize as an object"))?;
    object.remove("id");
    Ok(value)
}

fn no_migrated_turn_error(
    reference_limit: usize,
    bounded: &[Turn],
    canonical: &[Turn],
) -> crate::ThreadStoreError {
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
    migration_error(format!(
        "bounded Legacy Desktop history is not canonical at reference depth {reference_limit}: no migrated turn retained its stable ID (bounded [{bounded_ids}], migrated [{canonical_ids}]); source files were not changed"
    ))
}

fn incompatible(reference_limit: usize, turn_id: &str, reason: &str) -> crate::ThreadStoreError {
    migration_error(format!(
        "bounded Legacy Desktop history is not canonical at reference depth {reference_limit}: turn {turn_id} {reason}; source files were not changed"
    ))
}
