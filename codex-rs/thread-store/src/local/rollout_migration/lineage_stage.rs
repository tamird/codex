//! Stages a complete lineage without making any target discoverable.
//!
//! Staging replays every authenticated source through one lineage-wide ordinal and synthesized
//! item-ID sequence. The staged files live under the migration journal directory; publication and
//! selected-rollout changes are separate phases.

use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use codex_protocol::RolloutId;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use sha2::Digest;
use sha2::Sha256;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;

use super::MAX_ROLLOUT_LINE_BYTES;
use super::canonicalizer::LegacyCanonicalizerCheckpoint;
use super::canonicalizer::LegacyRolloutCanonicalizer;
use super::line_parser;
use super::lineage::LegacyLineageMigrationPlan;
use super::lineage::LegacyLineagePredecessor;
use super::lineage::reference_is_history_base_compatible;
use super::lineage_rewrite::GeneratedItemEdit;
use super::migration_error;
use super::rollback_plan::RollbackPlan;
use super::rollback_plan::RollbackPlanner;
use super::turn_context_cache::DecodeMode;
use super::turn_context_cache::TurnContextCache;
use crate::ThreadStoreResult;

/// One durable staged file and its verified ordinal and byte range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StagedLineageTarget {
    pub(super) thread_id: ThreadId,
    pub(super) rollout_id: RolloutId,
    pub(super) segment_id: Option<SegmentId>,
    pub(super) staged_path: PathBuf,
    pub(super) final_path: PathBuf,
    pub(super) start_ordinal: u64,
    pub(super) end_ordinal_exclusive: u64,
    pub(super) byte_count: u64,
    pub(super) record_count: u64,
    pub(super) sha256: String,
    pub(super) selected: bool,
    /// Byte ranges of IDs synthesized while writing this unpublished target.
    pub(super) generated_item_edits: Vec<GeneratedItemEdit>,
}

/// Exact output measurements produced without writing a target file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct MeasuredLineageTarget {
    pub(super) thread_id: ThreadId,
    pub(super) rollout_id: RolloutId,
    pub(super) segment_id: Option<SegmentId>,
    pub(super) final_path: PathBuf,
    pub(super) history_base: Option<HistoryPosition>,
    pub(super) start_ordinal: u64,
    pub(super) end_ordinal_exclusive: u64,
    pub(super) byte_count: u64,
    pub(super) record_count: u64,
    pub(super) sha256: String,
    pub(super) selected: bool,
}

/// Canonicalize all planned sources into a private transaction directory.
pub(super) async fn stage_legacy_lineage(
    plan: &LegacyLineageMigrationPlan,
    stage_root: &Path,
) -> ThreadStoreResult<Vec<StagedLineageTarget>> {
    stage_lineage_with_context_cache(plan, stage_root, TurnContextCache::default()).await
}

#[cfg(test)]
pub(super) async fn stage_legacy_lineage_without_context_cache(
    plan: &LegacyLineageMigrationPlan,
    stage_root: &Path,
) -> ThreadStoreResult<Vec<StagedLineageTarget>> {
    stage_lineage_with_context_cache(plan, stage_root, TurnContextCache::disabled()).await
}

async fn stage_lineage_with_context_cache(
    plan: &LegacyLineageMigrationPlan,
    stage_root: &Path,
    mut context_cache: TurnContextCache,
) -> ThreadStoreResult<Vec<StagedLineageTarget>> {
    validate_plan_shape(plan)?;
    let started = std::time::Instant::now();
    let rollback_plan = build_rollback_plan(plan, &mut context_cache).await?;
    tracing::info!(thread_id = %plan.selected_thread_id, phase = "plan_rollbacks", elapsed_ms = started.elapsed().as_millis() as u64, "rollout migration phase complete");
    let started = std::time::Instant::now();
    tokio::fs::create_dir_all(stage_root)
        .await
        .map_err(migration_error)?;
    let mut replay = LineageReplayState::new(plan, context_cache);
    let mut staged: Vec<StagedLineageTarget> = Vec::with_capacity(plan.sources.len());

    for (index, (source, target)) in plan.sources.iter().zip(&plan.targets).enumerate() {
        let filename = codex_rollout::plain_rollout_path(target.path.as_path())
            .file_name()
            .ok_or_else(|| migration_error("lineage target has no filename"))?
            .to_owned();
        let staged_path = stage_root.join(format!("{index:08}")).join(filename);
        let parent = staged_path
            .parent()
            .ok_or_else(|| migration_error("staged lineage target has no parent"))?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(migration_error)?;
        let source_permissions = tokio::fs::metadata(source.path.as_path())
            .await
            .map_err(migration_error)?
            .permissions();
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(staged_path.as_path())
            .await
            .map_err(migration_error)?;
        file.set_permissions(source_permissions)
            .await
            .map_err(migration_error)?;
        let mut writer = MeasuringWriter::new(BufWriter::with_capacity(256 * 1024, file));
        let history_base = target_history_base(
            plan,
            index,
            staged.last().map(|target| {
                (
                    target.rollout_id,
                    target.end_ordinal_exclusive,
                    target.byte_count,
                )
            }),
        )?;
        let mut generated_item_edits = Vec::new();
        let (start_ordinal, end_ordinal_exclusive) = replay_target(
            plan,
            rollback_plan.as_ref(),
            index,
            history_base,
            &mut replay,
            &mut writer,
            Some(&mut generated_item_edits),
        )
        .await?;
        writer.flush().await.map_err(migration_error)?;
        writer
            .inner
            .get_ref()
            .sync_all()
            .await
            .map_err(migration_error)?;
        let (byte_count, record_count, sha256) = writer.finish();
        staged.push(StagedLineageTarget {
            thread_id: target.thread_id,
            rollout_id: target.rollout_id,
            segment_id: target.segment_id,
            staged_path,
            final_path: target.path.clone(),
            start_ordinal,
            end_ordinal_exclusive,
            byte_count,
            record_count,
            sha256,
            selected: target.selected,
            generated_item_edits,
        });
    }
    replay.verify_complete(rollback_plan.as_ref())?;
    tracing::info!(thread_id = %plan.selected_thread_id, phase = "write_canonical_targets", elapsed_ms = started.elapsed().as_millis() as u64, "rollout migration phase complete");
    Ok(staged)
}

/// Canonicalize a complete lineage into a counting sink for an exact dry-run manifest.
pub(super) async fn measure_legacy_lineage(
    plan: &LegacyLineageMigrationPlan,
) -> ThreadStoreResult<Vec<MeasuredLineageTarget>> {
    validate_plan_shape(plan)?;
    let mut context_cache = TurnContextCache::default();
    let rollback_plan = build_rollback_plan(plan, &mut context_cache).await?;
    let mut replay = LineageReplayState::new(plan, context_cache);
    let mut measured = Vec::with_capacity(plan.targets.len());
    for (index, target) in plan.targets.iter().enumerate() {
        let mut writer = MeasuringWriter::default();
        let history_base = target_history_base(
            plan,
            index,
            measured.last().map(|target: &MeasuredLineageTarget| {
                (
                    target.rollout_id,
                    target.end_ordinal_exclusive,
                    target.byte_count,
                )
            }),
        )?;
        let (start_ordinal, end_ordinal_exclusive) = replay_target(
            plan,
            rollback_plan.as_ref(),
            index,
            history_base,
            &mut replay,
            &mut writer,
            /*generated_item_edits*/ None,
        )
        .await?;
        writer.flush().await.map_err(migration_error)?;
        let (byte_count, record_count, sha256) = writer.finish();
        measured.push(MeasuredLineageTarget {
            thread_id: target.thread_id,
            rollout_id: target.rollout_id,
            segment_id: target.segment_id,
            final_path: target.path.clone(),
            history_base,
            start_ordinal,
            end_ordinal_exclusive,
            byte_count,
            record_count,
            sha256,
            selected: target.selected,
        });
    }
    replay.verify_complete(rollback_plan.as_ref())?;
    Ok(measured)
}

fn validate_plan_shape(plan: &LegacyLineageMigrationPlan) -> ThreadStoreResult<()> {
    if plan.sources.len() != plan.targets.len() || plan.sources.is_empty() {
        return Err(migration_error(
            "lineage migration source and target manifests do not match",
        ));
    }
    Ok(())
}

fn target_history_base(
    plan: &LegacyLineageMigrationPlan,
    index: usize,
    previous: Option<(RolloutId, u64, u64)>,
) -> ThreadStoreResult<Option<HistoryPosition>> {
    let source = plan
        .sources
        .get(index)
        .ok_or_else(|| migration_error("lineage target has no source"))?;
    if source.materialized_predecessor {
        return Ok(
            previous.map(
                |(thread_id, end_ordinal_exclusive, end_byte_offset)| HistoryPosition {
                    thread_id,
                    end_ordinal_exclusive,
                    end_byte_offset,
                },
            ),
        );
    }
    if matches!(
        source.predecessor.as_ref(),
        Some(LegacyLineagePredecessor::RolloutReference(reference))
            if !reference_is_history_base_compatible(reference)
    ) {
        return Ok(None);
    }
    if let Some((rollout_id, end_ordinal_exclusive, end_byte_offset)) = previous {
        return Ok(Some(HistoryPosition {
            thread_id: rollout_id,
            end_ordinal_exclusive,
            end_byte_offset,
        }));
    }
    match source.predecessor.as_ref() {
        Some(LegacyLineagePredecessor::HistoryBase(position)) => Ok(Some(*position)),
        Some(LegacyLineagePredecessor::RolloutReference(_)) => {
            let dependency = plan
                .reference_dependencies
                .iter()
                .find(|dependency| dependency.successor_rollout_id == source.rollout_id)
                .ok_or_else(|| {
                    migration_error("oldest lineage target has no authenticated predecessor")
                })?;
            Ok(Some(HistoryPosition {
                thread_id: dependency.rollout_id,
                end_ordinal_exclusive: dependency.end_ordinal_exclusive,
                end_byte_offset: dependency.end_byte_offset,
            }))
        }
        None => Ok(None),
    }
}

fn target_rollout_reference(
    plan: &LegacyLineageMigrationPlan,
    index: usize,
) -> ThreadStoreResult<Option<RolloutReferenceItem>> {
    let source = plan
        .sources
        .get(index)
        .ok_or_else(|| migration_error("lineage target has no source"))?;
    if source.materialized_predecessor {
        return Ok(None);
    }
    let Some(LegacyLineagePredecessor::RolloutReference(reference)) = source.predecessor.as_ref()
    else {
        return Ok(None);
    };
    if reference_is_history_base_compatible(reference) {
        return Ok(None);
    }
    let mut reference = reference.clone();
    if let Some(predecessor) = plan
        .reference_dependencies
        .iter()
        .find(|dependency| dependency.successor_rollout_id == source.rollout_id)
    {
        reference.rollout_path = predecessor.path.clone();
        reference.thread_id = Some(predecessor.thread_id);
        reference.rollout_id = Some(predecessor.rollout_id);
        reference.segment_id = Some(predecessor.segment_id);
    } else {
        let predecessor = plan
            .targets
            .get(index.wrapping_sub(1))
            .ok_or_else(|| migration_error("filtered reference has no planned predecessor"))?;
        reference.rollout_path = predecessor.path.clone();
        reference.thread_id = Some(predecessor.thread_id);
        reference.rollout_id = Some(predecessor.rollout_id);
        reference.rollout_timestamp = plan
            .sources
            .get(index.wrapping_sub(1))
            .map(|source| source.timestamp.clone());
        reference.segment_id = predecessor.segment_id;
    }
    Ok(Some(reference))
}

struct LineageReplayState {
    context_cache: TurnContextCache,
    parsed_record_index: usize,
    next_ordinal: u64,
    source_next_ordinal: u64,
    next_item_index: u64,
    source_line_index: u64,
    continuation: Option<LegacyCanonicalizerCheckpoint>,
}

impl LineageReplayState {
    fn new(plan: &LegacyLineageMigrationPlan, context_cache: TurnContextCache) -> Self {
        let next_ordinal = plan
            .sources
            .first()
            .filter(|source| !source.materialized_predecessor)
            .and_then(|source| match source.predecessor.as_ref() {
                Some(LegacyLineagePredecessor::HistoryBase(position)) => {
                    Some(position.end_ordinal_exclusive)
                }
                Some(LegacyLineagePredecessor::RolloutReference(_)) => plan
                    .reference_dependencies
                    .iter()
                    .find(|dependency| dependency.successor_rollout_id == source.rollout_id)
                    .map(|dependency| dependency.end_ordinal_exclusive),
                _ => None,
            })
            .unwrap_or(0);
        Self {
            context_cache,
            parsed_record_index: 0,
            next_ordinal,
            source_next_ordinal: next_ordinal,
            // Bounded Legacy materialization presents one selected SessionMeta followed by the
            // logical lineage records. An existing Paginated prefix already occupies its
            // monotonic ordinal positions; otherwise the selected SessionMeta occupies position
            // zero. Per-segment SessionMeta and RolloutReference records are transport metadata
            // and do not occupy additional positions in that materialized stream.
            source_line_index: plan
                .sources
                .first()
                .map_or(1, |source| source.initial_source_line_index),
            next_item_index: plan
                .sources
                .first()
                .map_or(1, |source| source.initial_next_item_index),
            continuation: None,
        }
    }

    fn verify_complete(&self, rollback_plan: Option<&RollbackPlan>) -> ThreadStoreResult<()> {
        if rollback_plan.is_some_and(|plan| self.parsed_record_index != plan.record_count()) {
            return Err(migration_error(
                "lineage rollback plan source length changed during replay",
            ));
        }
        Ok(())
    }
}

async fn replay_target<W>(
    plan: &LegacyLineageMigrationPlan,
    rollback_plan: Option<&RollbackPlan>,
    index: usize,
    history_base: Option<HistoryPosition>,
    replay: &mut LineageReplayState,
    writer: &mut W,
    generated_item_edits: Option<&mut Vec<GeneratedItemEdit>>,
) -> ThreadStoreResult<(u64, u64)>
where
    W: AsyncWrite + Unpin,
{
    let source = plan
        .sources
        .get(index)
        .ok_or_else(|| migration_error("lineage replay source is missing"))?;
    let target = plan
        .targets
        .get(index)
        .ok_or_else(|| migration_error("lineage replay target is missing"))?;
    if source.history_mode == ThreadHistoryMode::Paginated {
        return replay_paginated_target(plan, rollback_plan, index, history_base, replay, writer)
            .await;
    }
    let mut canonicalizer = match replay.continuation.take() {
        Some(checkpoint) => {
            LegacyRolloutCanonicalizer::from_checkpoint(source.thread_id, checkpoint)
        }
        None => LegacyRolloutCanonicalizer::new_at(
            source.thread_id,
            replay.next_ordinal,
            source.initial_next_item_index,
            source.initial_source_line_index,
        )
        .with_synthetic_item_id_remap(Arc::new(plan.synthetic_item_id_remap.clone())),
    };
    if generated_item_edits.is_some() {
        canonicalizer = canonicalizer.record_generated_items();
    }
    canonicalizer.reset_output_position();
    let start_ordinal = canonicalizer.next_ordinal();
    let session_meta = codex_rollout::read_session_meta_line(source.path.as_path())
        .await
        .map_err(migration_error)?;
    canonicalizer
        .write_segment_head_session_meta(
            RolloutLine {
                timestamp: source.timestamp.clone(),
                ordinal: None,
                item: RolloutItem::SessionMeta(session_meta),
            },
            history_base,
            target.segment_id,
            writer,
        )
        .await?;
    if let Some(reference) = target_rollout_reference(plan, index)? {
        canonicalizer
            .write_rollout_reference(writer, source.timestamp.as_str(), reference)
            .await?;
    }
    let mut reader = super::open_migration_line_reader(source.path.as_path())
        .await
        .map_err(migration_error)?;
    while let Some(raw) = reader.next_line().await.map_err(migration_error)? {
        if raw.len() > MAX_ROLLOUT_LINE_BYTES {
            continue;
        }
        if let Some(context) = replay
            .context_cache
            .parse(raw.as_bytes(), DecodeMode::Legacy)
        {
            let retained = match rollback_plan {
                Some(plan) => plan
                    .apply(replay.parsed_record_index, context.rollout_line())?
                    .is_some(),
                None => true,
            };
            replay.parsed_record_index = replay
                .parsed_record_index
                .checked_add(1)
                .ok_or_else(|| migration_error("lineage migration record index overflow"))?;
            if retained {
                canonicalizer
                    .process_prepared_turn_context(&context, writer)
                    .await?;
            } else {
                canonicalizer.skip_source_line()?;
            }
            continue;
        }
        let Ok(Some(line)) = line_parser::parse_legacy_rollout_line(raw.as_bytes()) else {
            continue;
        };
        let planned = match rollback_plan {
            Some(plan) => plan.apply(replay.parsed_record_index, line)?,
            None => Some(line),
        };
        replay.parsed_record_index = replay
            .parsed_record_index
            .checked_add(1)
            .ok_or_else(|| migration_error("lineage migration record index overflow"))?;
        let Some(line) = planned else {
            canonicalizer.skip_source_line()?;
            continue;
        };
        if matches!(
            line.item,
            RolloutItem::SessionMeta(_) | RolloutItem::RolloutReference(_)
        ) {
            continue;
        }
        canonicalizer.process_line(line, writer).await?;
    }
    let carries_turn_state = plan
        .sources
        .get(index + 1)
        .is_some_and(|successor| ordinary_same_thread_successor(source, successor));
    if !carries_turn_state {
        canonicalizer
            .finish(writer, source.timestamp.as_str())
            .await?;
    }
    if let Some(edits) = generated_item_edits {
        *edits = canonicalizer.take_generated_item_edits();
    }
    let checkpoint = canonicalizer.into_checkpoint();
    replay.next_ordinal = checkpoint.next_ordinal();
    replay.next_item_index = checkpoint.next_item_index();
    replay.source_line_index = checkpoint.source_line_index();
    replay.continuation = carries_turn_state.then_some(checkpoint);
    Ok((start_ordinal, replay.next_ordinal))
}

async fn replay_paginated_target<W>(
    plan: &LegacyLineageMigrationPlan,
    rollback_plan: Option<&RollbackPlan>,
    index: usize,
    history_base: Option<HistoryPosition>,
    replay: &mut LineageReplayState,
    writer: &mut W,
) -> ThreadStoreResult<(u64, u64)>
where
    W: AsyncWrite + Unpin,
{
    let source = plan
        .sources
        .get(index)
        .ok_or_else(|| migration_error("Paginated replay source is missing"))?;
    let target = plan
        .targets
        .get(index)
        .ok_or_else(|| migration_error("Paginated replay target is missing"))?;
    if replay.continuation.take().is_some() {
        return Err(migration_error(
            "Paginated source cannot follow an unfinished Legacy turn",
        ));
    }
    let start_ordinal = replay.next_ordinal;
    let mut session_meta = codex_rollout::read_session_meta_line(source.path.as_path())
        .await
        .map_err(migration_error)?;
    // Older writers reused a trailing token_count ordinal when its numeric payload failed
    // decoding. Only an unbounded selected source can be corrected without translating an
    // inherited cutoff. Published sources and their already-issued boundaries stay unchanged.
    let recover_token_count_ordinal = target.selected
        && index + 1 == plan.sources.len()
        && source.thread_id == plan.selected_thread_id
        && source.rollout_id == plan.selected_rollout_id
        && target.thread_id == plan.selected_thread_id
        && source.replay_end.is_none()
        && source.native_replay.is_none()
        && !source.materialized_predecessor
        && !plan.replay_native_rollbacks
        && session_meta.meta.subagent_history_start_ordinal.is_none()
        && source
            .predecessor
            .as_ref()
            .is_none_or(|predecessor| match predecessor {
                LegacyLineagePredecessor::RolloutReference(reference) => {
                    reference_is_history_base_compatible(reference)
                }
                LegacyLineagePredecessor::HistoryBase(_) => true,
            });
    let advance_source_ordinal = |expected: &mut u64,
                                  ordinal: u64,
                                  previous_raw: Option<&str>|
     -> ThreadStoreResult<()> {
        if ordinal != *expected {
            let reused_token_count = recover_token_count_ordinal
                && expected.checked_sub(1) == Some(ordinal)
                && previous_raw.is_some_and(|raw| {
                    line_parser::parse_paginated_rollout_line(raw.as_bytes()).is_ok_and(
                        |previous| {
                            previous.ordinal == Some(ordinal)
                                && matches!(
                                    previous.item,
                                    RolloutItem::EventMsg(EventMsg::TokenCount(_))
                                )
                        },
                    )
                });
            if !reused_token_count {
                return Err(migration_error(format!(
                    "Paginated source {} has a non-contiguous ordinal: expected {expected}, found {ordinal}",
                    source.path.display(),
                )));
            }
        }
        *expected = ordinal
            .checked_add(1)
            .ok_or_else(|| migration_error("Paginated source ordinal overflow"))?;
        Ok(())
    };
    session_meta.meta.history_mode = ThreadHistoryMode::Paginated;
    session_meta.meta.history_base = history_base;
    session_meta.meta.segment_id = target.segment_id;
    session_meta.meta.subagent_history_start_ordinal = session_meta
        .meta
        .subagent_history_start_ordinal
        .map(|boundary| translate_paginated_ordinal(plan, boundary))
        .transpose()?;
    write_paginated_item(
        writer,
        source.timestamp.as_str(),
        replay.next_ordinal,
        RolloutItem::SessionMeta(session_meta),
    )
    .await?;
    replay.next_ordinal = replay
        .next_ordinal
        .checked_add(1)
        .ok_or_else(|| migration_error("Paginated rollout ordinal overflow"))?;

    let mut saw_session_meta = false;
    let mut saw_reference = false;
    let mut previous_raw = None;
    let retained_reference = target_rollout_reference(plan, index)?;
    let mut reader = super::open_migration_line_reader(source.path.as_path())
        .await
        .map_err(migration_error)?;
    while let Some(raw) = reader.next_line().await.map_err(migration_error)? {
        if raw.trim().is_empty() {
            continue;
        }
        if raw.len() > MAX_ROLLOUT_LINE_BYTES {
            return Err(migration_error(format!(
                "Paginated source {} contains an oversized record",
                source.path.display()
            )));
        }
        if source.canonical_paginated_suffix && !plan.replay_native_rollbacks {
            let envelope =
                match super::ordinal_rewrite::OrdinalRecord::from_canonical(raw.as_bytes()) {
                    Some(envelope) => envelope,
                    None => serde_json::from_str(&raw).map_err(migration_error)?,
                };
            if !matches!(
                envelope.kind.as_ref(),
                "session_meta" | "rollout_reference" | "fork_reference"
            ) {
                let ordinal = envelope.ordinal()?;
                advance_source_ordinal(
                    &mut replay.source_next_ordinal,
                    ordinal,
                    previous_raw.as_deref(),
                )?;
                envelope
                    .write_with_ordinal(raw.as_bytes(), replay.next_ordinal, writer)
                    .await?;
                replay.next_ordinal = replay
                    .next_ordinal
                    .checked_add(1)
                    .ok_or_else(|| migration_error("Paginated rollout ordinal overflow"))?;
                if recover_token_count_ordinal {
                    previous_raw = Some(raw);
                }
                continue;
            }
        }
        let line = line_parser::parse_paginated_rollout_line(raw.as_bytes()).map_err(|error| {
            migration_error(format!(
                "Paginated source {} contains an invalid record: {error}",
                source.path.display()
            ))
        })?;
        let ordinal = line.ordinal.ok_or_else(|| {
            migration_error(format!(
                "Paginated source {} contains an unordinaled record",
                source.path.display()
            ))
        })?;
        if source
            .replay_end
            .map(|end| end.end_ordinal_exclusive)
            .or_else(|| {
                source
                    .native_replay
                    .as_ref()
                    .and_then(|range| range.end_ordinal_exclusive)
            })
            .is_some_and(|end| ordinal >= end)
        {
            break;
        }
        if plan.replay_native_rollbacks && !saw_session_meta {
            replay.source_next_ordinal = ordinal;
        }
        advance_source_ordinal(
            &mut replay.source_next_ordinal,
            ordinal,
            previous_raw.as_deref(),
        )?;
        let Some(line) = source.filter_native_line(line)? else {
            continue;
        };
        let line = if plan.replay_native_rollbacks {
            let planned = match rollback_plan {
                Some(rollback_plan) => rollback_plan.apply(replay.parsed_record_index, line)?,
                None => Some(line),
            };
            replay.parsed_record_index = replay
                .parsed_record_index
                .checked_add(1)
                .ok_or_else(|| migration_error("lineage migration record index overflow"))?;
            let Some(line) = planned else {
                continue;
            };
            line
        } else {
            line
        };
        match line.item {
            RolloutItem::SessionMeta(_) if !saw_session_meta => {
                saw_session_meta = true;
            }
            RolloutItem::SessionMeta(_) => {
                return Err(migration_error(format!(
                    "Paginated source {} contains more than one SessionMeta",
                    source.path.display()
                )));
            }
            RolloutItem::RolloutReference(_) if saw_session_meta && !saw_reference => {
                saw_reference = true;
                if let Some(reference) = retained_reference.clone() {
                    write_paginated_item(
                        writer,
                        line.timestamp.as_str(),
                        replay.next_ordinal,
                        RolloutItem::RolloutReference(reference),
                    )
                    .await?;
                    replay.next_ordinal = replay
                        .next_ordinal
                        .checked_add(1)
                        .ok_or_else(|| migration_error("Paginated rollout ordinal overflow"))?;
                }
            }
            RolloutItem::RolloutReference(_) => {
                return Err(migration_error(format!(
                    "Paginated source {} contains a non-leading RolloutReference",
                    source.path.display()
                )));
            }
            item => {
                if source.canonical_paginated_suffix && !matches!(item, RolloutItem::Compacted(_)) {
                    let envelope =
                        match super::ordinal_rewrite::OrdinalRecord::from_canonical(raw.as_bytes())
                        {
                            Some(envelope) => envelope,
                            None => serde_json::from_str(&raw).map_err(migration_error)?,
                        };
                    envelope
                        .write_with_ordinal(raw.as_bytes(), replay.next_ordinal, writer)
                        .await?;
                } else {
                    write_paginated_item(
                        writer,
                        line.timestamp.as_str(),
                        replay.next_ordinal,
                        item,
                    )
                    .await?;
                }
                replay.next_ordinal = replay
                    .next_ordinal
                    .checked_add(1)
                    .ok_or_else(|| migration_error("Paginated rollout ordinal overflow"))?;
            }
        }
        if recover_token_count_ordinal {
            previous_raw = Some(raw);
        }
    }
    if !saw_session_meta {
        return Err(migration_error(format!(
            "Paginated source {} contains no SessionMeta",
            source.path.display()
        )));
    }
    if source
        .replay_end
        .map(|end| end.end_ordinal_exclusive)
        .or_else(|| {
            source
                .native_replay
                .as_ref()
                .and_then(|range| range.end_ordinal_exclusive)
        })
        .is_some_and(|end| replay.source_next_ordinal != end)
    {
        return Err(migration_error(
            "native rollback source ended before its history_base boundary",
        ));
    }
    Ok((start_ordinal, replay.next_ordinal))
}

fn translate_paginated_ordinal(
    plan: &LegacyLineageMigrationPlan,
    source_ordinal: u64,
) -> ThreadStoreResult<u64> {
    let removed = u64::try_from(
        plan.sources
            .iter()
            .filter(|source| {
                matches!(
                    source.predecessor.as_ref(),
                    Some(LegacyLineagePredecessor::RolloutReference(reference))
                        if reference_is_history_base_compatible(reference)
                )
            })
            .filter_map(|source| source.reference_ordinal)
            .filter(|ordinal| *ordinal < source_ordinal)
            .count(),
    )
    .map_err(|_| migration_error("too many RolloutReference records"))?;
    source_ordinal
        .checked_sub(removed)
        .ok_or_else(|| migration_error("subagent history boundary precedes the migrated lineage"))
}

async fn write_paginated_item<W>(
    writer: &mut W,
    timestamp: &str,
    ordinal: u64,
    item: RolloutItem,
) -> ThreadStoreResult<()>
where
    W: AsyncWrite + Unpin,
{
    let bytes = serde_json::to_vec(&RolloutLine {
        timestamp: timestamp.to_string(),
        ordinal: Some(ordinal),
        item,
    })
    .map_err(migration_error)?;
    writer
        .write_all(bytes.as_slice())
        .await
        .map_err(migration_error)?;
    writer.write_all(b"\n").await.map_err(migration_error)
}

/// Measures exactly the bytes accepted by a writer, including short writes. Staging and dry-run
/// manifests use the same accounting so hashing and counting do not require another file scan.
pub(super) struct MeasuringWriter<W = tokio::io::Sink> {
    pub(super) inner: W,
    byte_count: u64,
    record_count: u64,
    hasher: Sha256,
}

impl Default for MeasuringWriter {
    fn default() -> Self {
        Self::new(tokio::io::sink())
    }
}

impl<W> MeasuringWriter<W> {
    pub(super) fn new(inner: W) -> Self {
        Self {
            inner,
            byte_count: 0,
            record_count: 0,
            hasher: Sha256::new(),
        }
    }

    pub(super) fn finish(self) -> (u64, u64, String) {
        (
            self.byte_count,
            self.record_count,
            format!("{:x}", self.hasher.finalize()),
        )
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for MeasuringWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let written = std::task::ready!(Pin::new(&mut self.inner).poll_write(cx, buf))?;
        let buf = &buf[..written];
        let byte_count = self
            .byte_count
            .checked_add(buf.len() as u64)
            .ok_or_else(|| std::io::Error::other("measured lineage byte count overflow"))?;
        let record_count = self
            .record_count
            .checked_add(buf.iter().filter(|byte| **byte == b'\n').count() as u64)
            .ok_or_else(|| std::io::Error::other("measured lineage record count overflow"))?;
        self.hasher.update(buf);
        self.byte_count = byte_count;
        self.record_count = record_count;
        Poll::Ready(Ok(written))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

fn ordinary_same_thread_successor(
    source: &super::lineage::LegacyLineageSource,
    successor: &super::lineage::LegacyLineageSource,
) -> bool {
    if source.thread_id != successor.thread_id {
        return false;
    }
    matches!(
        successor.predecessor.as_ref(),
        Some(LegacyLineagePredecessor::RolloutReference(reference))
            if reference.thread_id == Some(source.thread_id)
                && reference.nth_user_message.is_none()
                && reference.compacted_replacement_history_filter_texts.is_none()
    )
}

pub(super) async fn build_rollback_plan(
    plan: &LegacyLineageMigrationPlan,
    context_cache: &mut TurnContextCache,
) -> ThreadStoreResult<Option<RollbackPlan>> {
    // Source authentication already inspected every record. Without a rollback marker, the
    // ownership and reverse-compaction planners leave every record unchanged.
    if !plan
        .sources
        .iter()
        .any(|source| source.history_mode == ThreadHistoryMode::Legacy && source.has_rollback)
    {
        return Ok(None);
    }
    let mut planner = RollbackPlanner::new();
    for source in &plan.sources {
        if source.history_mode == ThreadHistoryMode::Paginated && !plan.replay_native_rollbacks {
            continue;
        }
        let mut reader = super::open_migration_line_reader(source.path.as_path())
            .await
            .map_err(migration_error)?;
        while let Some(raw) = reader.next_line().await.map_err(migration_error)? {
            if raw.len() > MAX_ROLLOUT_LINE_BYTES {
                continue;
            }
            if source.history_mode == ThreadHistoryMode::Paginated {
                if raw.trim().is_empty() {
                    continue;
                }
                let line = line_parser::parse_paginated_rollout_line(raw.as_bytes())
                    .map_err(migration_error)?;
                if source
                    .replay_end
                    .map(|end| end.end_ordinal_exclusive)
                    .or_else(|| {
                        source
                            .native_replay
                            .as_ref()
                            .and_then(|range| range.end_ordinal_exclusive)
                    })
                    .is_some_and(|end| line.ordinal.is_some_and(|ordinal| ordinal >= end))
                {
                    break;
                }
                let Some(line) = source.filter_native_line(line)? else {
                    continue;
                };
                planner.observe_paginated(&line)?;
                continue;
            }
            if let Some(context) = context_cache.parse(raw.as_bytes(), DecodeMode::Legacy) {
                planner.observe(&context.rollout_line())?;
                continue;
            }
            let Ok(Some(line)) = line_parser::parse_legacy_rollout_line(raw.as_bytes()) else {
                continue;
            };
            planner.observe(&line)?;
        }
    }
    Ok(Some(planner.finish()))
}
