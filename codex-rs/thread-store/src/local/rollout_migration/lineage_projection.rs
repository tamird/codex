//! Identifies staged lineages that can populate the selected root without another rollout scan.

use codex_protocol::RolloutId;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;

use super::MAX_ROLLOUT_LINE_BYTES;
use super::PROJECTION_BATCH_BYTES;
use super::RolloutMigrationRateLimiter;
use super::canonical_projection;
use super::lineage::LegacyLineageMigrationPlan;
use super::lineage::LegacyLineagePredecessor;
use super::lineage_journal::LineageMigrationJournal;
use super::lineage_journal::LineageMigrationPhase;
use super::migration_error;
use super::parse_rollout_timestamp;
use crate::ThreadStoreResult;
use crate::local::LocalThreadStore;
use crate::local::thread_history::BulkProjection;
use crate::local::thread_history::ProjectedRolloutLine;

/// Charge input bytes and allocation overhead for each visible change. Extremely large histories
/// use the existing bounded SQL writer instead of retaining the complete root in memory.
pub(super) const BULK_PROJECTION_BUDGET: u64 = 128 * 1024 * 1024;

/// The caller has authenticated the TargetsDurable journal and retains all lineage writers.
/// Returning false leaves only unpublished rows, which the ordinary projection writer replaces.
pub(super) async fn try_project_staged_targets(
    store: &LocalThreadStore,
    journal: &LineageMigrationJournal,
    complete_root: Option<RolloutId>,
    limiter: &mut RolloutMigrationRateLimiter,
    memory_budget: u64,
) -> ThreadStoreResult<bool> {
    if journal.phase != LineageMigrationPhase::TargetsDurable {
        return Err(migration_error(
            "bulk projection requires durable staged targets",
        ));
    }
    let mut root = complete_root.map(|_| BulkProjection::new(/*initial_ordinal*/ 0));
    let mut root_charge = 0_u64;
    for target in &journal.targets {
        let path = target
            .staged_path
            .as_ref()
            .ok_or_else(|| migration_error("lineage target is missing its staged path"))?;
        let start = target
            .start_ordinal
            .ok_or_else(|| migration_error("lineage target is missing its start ordinal"))?;
        let expected_end = target
            .end_ordinal_exclusive
            .ok_or_else(|| migration_error("lineage target is missing its ordinal boundary"))?;
        let expected_bytes = target
            .byte_count
            .ok_or_else(|| migration_error("lineage target is missing its byte count"))?;
        let metadata = codex_rollout::read_session_meta_line(path)
            .await
            .map_err(migration_error)?;
        let mut physical =
            (complete_root != Some(target.rollout_id)).then(|| BulkProjection::new(start));
        if let Some(root) = &mut root {
            root.begin_segment(start)?;
        }
        let file = tokio::fs::File::open(path).await.map_err(migration_error)?;
        let mut reader = tokio::io::BufReader::with_capacity(PROJECTION_BATCH_BYTES as usize, file);
        let mut bytes = Vec::new();
        let mut offset = 0_u64;
        let mut physical_charge = 0_u64;
        loop {
            bytes.clear();
            let count = (&mut reader)
                .take((MAX_ROLLOUT_LINE_BYTES + 1) as u64)
                .read_until(b'\n', &mut bytes)
                .await
                .map_err(migration_error)?;
            if count == 0 {
                break;
            }
            if count > MAX_ROLLOUT_LINE_BYTES {
                return Err(migration_error(
                    "staged rollout contains an oversized record",
                ));
            }
            limiter.account(count as u64).await;
            let record =
                canonical_projection::project_canonical_record(&bytes).map_err(migration_error)?;
            let next_offset = offset
                .checked_add(count as u64)
                .ok_or_else(|| migration_error("staged rollout byte offset overflow"))?;
            let is_inherited_subagent_history = metadata
                .meta
                .subagent_history_start_ordinal
                .is_some_and(|start| record.ordinal < start);
            let (changes, realtime_item) = if is_inherited_subagent_history {
                (Default::default(), None)
            } else {
                (record.changes, record.realtime_item)
            };
            let change_count = changes
                .changed_turns
                .len()
                .saturating_add(changes.changed_items.len())
                .saturating_add(changes.removed_turn_ids.len())
                .saturating_add(usize::from(realtime_item.is_some()))
                as u64;
            let charge = if change_count == 0 {
                0
            } else {
                (count as u64).saturating_add(change_count.saturating_mul(1024))
            };
            if physical.is_some() {
                physical_charge = physical_charge.saturating_add(charge);
            }
            if root.is_some() {
                root_charge = root_charge.saturating_add(charge);
            }
            if root_charge.saturating_add(physical_charge) > memory_budget {
                tracing::info!(thread_id = %journal.selected_thread_id, "using bounded SQL projection for large migration");
                return Ok(false);
            }
            let line = ProjectedRolloutLine {
                ordinal: record.ordinal,
                start_byte_offset: offset,
                end_byte_offset: next_offset,
                fallback_created_at_ms: Some(
                    parse_rollout_timestamp(&record.timestamp)?.timestamp_millis(),
                ),
                changes,
                realtime_item,
            };
            if let Some(physical) = &mut physical {
                physical.apply(&line)?;
            }
            if let Some(root) = &mut root {
                root.apply(&line)?;
            }
            offset = next_offset;
        }
        for projection in physical.iter().chain(root.iter()) {
            if projection.position() != (expected_end, expected_bytes) {
                return Err(migration_error(
                    "bulk projection does not cover its authenticated target",
                ));
            }
        }
        if let Some(physical) = physical {
            physical
                .replace_unpublished(
                    store,
                    target.rollout_id,
                    metadata.meta.history_base.is_none(),
                )
                .await?;
        }
    }
    if let (Some(root), Some(rollout_id)) = (root, complete_root) {
        root.replace_unpublished(store, rollout_id, /*lineage_complete*/ true)
            .await?;
    }
    Ok(true)
}

/// Returns the unpublished selected identity only when ordered staging contains every visible
/// record. External prefixes, effective fork cutoffs, filters, and subagent boundaries still need
/// the general lineage resolver and projection rebuild.
pub(super) async fn complete_staged_root(
    plan: &LegacyLineageMigrationPlan,
    journal: &LineageMigrationJournal,
) -> ThreadStoreResult<Option<RolloutId>> {
    if !plan.history_bases.is_empty()
        || !plan.reference_dependencies.is_empty()
        || plan
            .sources
            .first()
            .is_none_or(|source| source.predecessor.is_some() && !source.materialized_predecessor)
        || plan
            .sources
            .iter()
            .filter(|source| !source.materialized_predecessor)
            .any(|source| match &source.predecessor {
                Some(LegacyLineagePredecessor::RolloutReference(reference)) => {
                    // The Legacy fork reader treats usize::MAX as no cutoff. The persisted
                    // reference still remains unchanged; only projection eligibility uses this fact.
                    reference
                        .nth_user_message
                        .is_some_and(|nth| nth != usize::MAX)
                        || reference
                            .compacted_replacement_history_filter_texts
                            .is_some()
                }
                Some(LegacyLineagePredecessor::HistoryBase(_)) => false,
                None => false,
            })
    {
        return Ok(None);
    }
    let mut next_ordinal = 0;
    for target in &journal.targets {
        if target.start_ordinal != Some(next_ordinal) {
            return Ok(None);
        }
        next_ordinal = target.end_ordinal_exclusive.ok_or_else(|| {
            migration_error("staged lineage target is missing its ordinal boundary")
        })?;
        let path = target
            .staged_path
            .as_ref()
            .ok_or_else(|| migration_error("staged lineage target is missing its path"))?;
        let metadata = codex_rollout::read_session_meta_line(path)
            .await
            .map_err(migration_error)?;
        if metadata.meta.subagent_history_start_ordinal.is_some() {
            return Ok(None);
        }
    }
    Ok(journal
        .targets
        .last()
        .filter(|target| target.selected)
        .map(|target| target.rollout_id))
}
