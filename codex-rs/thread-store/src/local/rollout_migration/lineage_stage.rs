//! Stages a complete lineage without making any target discoverable.
//!
//! Staging replays every authenticated source through one lineage-wide ordinal and synthesized
//! item-ID sequence. The staged files live under the migration journal directory; publication and
//! selected-rollout changes are separate phases.

use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use codex_protocol::RolloutId;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
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
use super::lineage::LegacyLineageSource;
use super::lineage::LegacyLineageTarget;
use super::lineage::hash_file;
use super::lineage::reference_is_history_base_compatible;
use super::migration_error;
use super::rollback_plan::RollbackPlan;
use super::rollback_plan::RollbackPlanner;
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
    validate_plan_shape(plan)?;
    let rollback_plan = build_rollback_plan(plan).await?;
    tokio::fs::create_dir_all(stage_root)
        .await
        .map_err(migration_error)?;
    let mut replay = LineageReplayState::new(plan);
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
        let mut writer = BufWriter::new(file);
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
        let (start_ordinal, end_ordinal_exclusive) = replay_target(
            plan,
            &rollback_plan,
            index,
            history_base,
            &mut replay,
            &mut writer,
        )
        .await?;
        writer.flush().await.map_err(migration_error)?;
        writer.get_ref().sync_all().await.map_err(migration_error)?;
        drop(writer);
        let (byte_count, sha256) = hash_file(staged_path.as_path()).await?;
        let record_count = count_records(staged_path.as_path()).await?;
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
        });
    }
    replay.verify_complete(&rollback_plan)?;
    Ok(staged)
}

/// Canonicalize a complete lineage into a counting sink for an exact dry-run manifest.
pub(super) async fn measure_legacy_lineage(
    plan: &LegacyLineageMigrationPlan,
) -> ThreadStoreResult<Vec<MeasuredLineageTarget>> {
    validate_plan_shape(plan)?;
    let rollback_plan = build_rollback_plan(plan).await?;
    let mut replay = LineageReplayState::new(plan);
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
            &rollback_plan,
            index,
            history_base,
            &mut replay,
            &mut writer,
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
    replay.verify_complete(&rollback_plan)?;
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
                end_byte_offset: dependency.byte_count,
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
    parsed_record_index: usize,
    next_ordinal: u64,
    source_next_ordinal: u64,
    next_item_index: u64,
    source_line_index: u64,
    continuation: Option<LegacyCanonicalizerCheckpoint>,
}

impl LineageReplayState {
    fn new(plan: &LegacyLineageMigrationPlan) -> Self {
        let next_ordinal = plan
            .sources
            .first()
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

    fn verify_complete(&self, rollback_plan: &RollbackPlan) -> ThreadStoreResult<()> {
        if self.parsed_record_index != rollback_plan.record_count() {
            return Err(migration_error(
                "lineage rollback plan source length changed during replay",
            ));
        }
        Ok(())
    }
}

async fn replay_target<W>(
    plan: &LegacyLineageMigrationPlan,
    rollback_plan: &RollbackPlan,
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
        .ok_or_else(|| migration_error("lineage replay source is missing"))?;
    let target = plan
        .targets
        .get(index)
        .ok_or_else(|| migration_error("lineage replay target is missing"))?;
    if source.history_mode == ThreadHistoryMode::Paginated {
        return replay_paginated_target(plan, index, source, target, history_base, replay, writer)
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
        ),
    };
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
    let mut reader = codex_rollout::open_rollout_line_reader(source.path.as_path())
        .await
        .map_err(migration_error)?;
    while let Some(raw) = reader.next_line().await.map_err(migration_error)? {
        if raw.len() > MAX_ROLLOUT_LINE_BYTES {
            continue;
        }
        let Ok(Some(line)) = line_parser::parse_legacy_rollout_line(raw.as_bytes()) else {
            continue;
        };
        let planned = rollback_plan.apply(replay.parsed_record_index, line)?;
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
    let checkpoint = canonicalizer.into_checkpoint();
    replay.next_ordinal = checkpoint.next_ordinal();
    replay.next_item_index = checkpoint.next_item_index();
    replay.source_line_index = checkpoint.source_line_index();
    replay.continuation = carries_turn_state.then_some(checkpoint);
    Ok((start_ordinal, replay.next_ordinal))
}

async fn replay_paginated_target<W>(
    plan: &LegacyLineageMigrationPlan,
    index: usize,
    source: &LegacyLineageSource,
    target: &LegacyLineageTarget,
    history_base: Option<HistoryPosition>,
    replay: &mut LineageReplayState,
    writer: &mut W,
) -> ThreadStoreResult<(u64, u64)>
where
    W: AsyncWrite + Unpin,
{
    if replay.continuation.take().is_some() {
        return Err(migration_error(
            "Paginated source cannot follow an unfinished Legacy turn",
        ));
    }
    let start_ordinal = replay.next_ordinal;
    let mut session_meta = codex_rollout::read_session_meta_line(source.path.as_path())
        .await
        .map_err(migration_error)?;
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
    let retained_reference = target_rollout_reference(plan, index)?;
    let mut reader = codex_rollout::open_rollout_line_reader(source.path.as_path())
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
        let line = serde_json::from_str::<RolloutLine>(raw.as_str()).map_err(|error| {
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
        if ordinal != replay.source_next_ordinal {
            return Err(migration_error(format!(
                "Paginated source {} has a non-contiguous ordinal: expected {}, found {ordinal}",
                source.path.display(),
                replay.source_next_ordinal
            )));
        }
        replay.source_next_ordinal = replay
            .source_next_ordinal
            .checked_add(1)
            .ok_or_else(|| migration_error("Paginated source ordinal overflow"))?;
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
                write_paginated_item(writer, line.timestamp.as_str(), replay.next_ordinal, item)
                    .await?;
                replay.next_ordinal = replay
                    .next_ordinal
                    .checked_add(1)
                    .ok_or_else(|| migration_error("Paginated rollout ordinal overflow"))?;
            }
        }
    }
    if !saw_session_meta {
        return Err(migration_error(format!(
            "Paginated source {} contains no SessionMeta",
            source.path.display()
        )));
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

#[derive(Default)]
pub(super) struct MeasuringWriter {
    byte_count: u64,
    record_count: u64,
    hasher: Sha256,
}

impl MeasuringWriter {
    pub(super) fn finish(self) -> (u64, u64, String) {
        (
            self.byte_count,
            self.record_count,
            format!("{:x}", self.hasher.finalize()),
        )
    }
}

impl AsyncWrite for MeasuringWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
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
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
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

async fn build_rollback_plan(plan: &LegacyLineageMigrationPlan) -> ThreadStoreResult<RollbackPlan> {
    let mut planner = RollbackPlanner::new();
    for source in &plan.sources {
        if source.history_mode == ThreadHistoryMode::Paginated {
            continue;
        }
        let mut reader = codex_rollout::open_rollout_line_reader(source.path.as_path())
            .await
            .map_err(migration_error)?;
        while let Some(raw) = reader.next_line().await.map_err(migration_error)? {
            if raw.len() > MAX_ROLLOUT_LINE_BYTES {
                continue;
            }
            let Ok(Some(line)) = line_parser::parse_legacy_rollout_line(raw.as_bytes()) else {
                continue;
            };
            planner.observe(&line)?;
        }
    }
    Ok(planner.finish())
}

async fn count_records(path: &Path) -> ThreadStoreResult<u64> {
    let mut reader = codex_rollout::open_rollout_line_reader(path)
        .await
        .map_err(migration_error)?;
    let mut count = 0_u64;
    while reader.next_line().await.map_err(migration_error)?.is_some() {
        count = count
            .checked_add(1)
            .ok_or_else(|| migration_error("staged lineage record count overflow"))?;
    }
    Ok(count)
}
