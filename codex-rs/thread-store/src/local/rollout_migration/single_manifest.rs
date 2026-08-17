//! Measures the exact canonical output of the existing one-file migration.
//!
//! A dry-run must describe the file that apply will publish without creating that file. This
//! module replays the same legacy parser, rollback plan, subagent context selection, and canonical
//! writer into a counting SHA-256 sink. Compressed subagents use a private temporary decompression
//! only for the reverse context scan; the temporary file is deleted when measurement returns.

use std::path::Path;

use codex_protocol::RolloutId;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use super::MAX_ROLLOUT_LINE_BYTES;
use super::RolloutMigrationKind;
use super::canonicalizer::LegacyRolloutCanonicalizer;
use super::decompress_rollout_to_path;
use super::line_parser;
use super::lineage::hash_file;
use super::lineage_stage::MeasuringWriter;
use super::migration_error;
use super::rollback_plan::RollbackPlan;
use super::rollback_plan::RollbackPlanner;
use super::rollout_path_is_compressed;
use super::subagent;
use crate::ThreadStoreResult;

/// Source and exact target measurements for one in-place migration.
pub(super) struct MeasuredSingleRollout {
    pub(super) thread_id: ThreadId,
    pub(super) rollout_id: RolloutId,
    pub(super) segment_id: Option<SegmentId>,
    pub(super) source_byte_count: u64,
    pub(super) source_record_count: u64,
    pub(super) source_sha256: String,
    pub(super) target_byte_count: u64,
    pub(super) target_record_count: u64,
    pub(super) target_sha256: String,
    pub(super) target_end_ordinal_exclusive: u64,
}

/// Measure one unreferenced Legacy rollout without publishing migration artifacts.
pub(super) async fn measure_single_rollout(
    path: &Path,
    kind: RolloutMigrationKind,
) -> ThreadStoreResult<MeasuredSingleRollout> {
    let canonical_session_meta = canonical_session_meta(path).await?;
    let RolloutItem::SessionMeta(session_meta) = &canonical_session_meta.item else {
        return Err(migration_error("canonical session metadata is missing"));
    };
    let thread_id = session_meta.meta.id;
    let rollout_id =
        codex_rollout::rollout_id_from_path(codex_rollout::plain_rollout_path(path).as_path())
            .unwrap_or(thread_id);
    let rollback_plan = build_rollback_plan(path).await?;
    let (source_byte_count, source_sha256) = hash_file(path).await?;
    let source_record_count = u64::try_from(rollback_plan.record_count())
        .map_err(|_| migration_error("source record count does not fit u64"))?;

    let mut decompressed = None;
    let context_path = if kind == RolloutMigrationKind::Subagent && rollout_path_is_compressed(path)
    {
        let file = tempfile::NamedTempFile::new().map_err(migration_error)?;
        decompress_rollout_to_path(path, file.path()).await?;
        let context_path = file.path().to_path_buf();
        decompressed = Some(file);
        context_path
    } else {
        path.to_path_buf()
    };
    let bounded_items = if kind == RolloutMigrationKind::Subagent {
        subagent::select_bounded_context(context_path, session_meta.clone()).await?
    } else {
        None
    };

    let mut subagent_history_start_ordinal = None;
    if kind == RolloutMigrationKind::Subagent && bounded_items.is_some() {
        let first = measure_replay(
            path,
            thread_id,
            &canonical_session_meta,
            bounded_items.as_deref(),
            &rollback_plan,
            /*subagent_history_start_ordinal*/ None,
        )
        .await?;
        subagent_history_start_ordinal = Some(first.end_ordinal_exclusive);
    }
    let measured = measure_replay(
        path,
        thread_id,
        &canonical_session_meta,
        bounded_items.as_deref(),
        &rollback_plan,
        subagent_history_start_ordinal,
    )
    .await?;
    drop(decompressed);

    Ok(MeasuredSingleRollout {
        thread_id,
        rollout_id,
        segment_id: session_meta.meta.segment_id,
        source_byte_count,
        source_record_count,
        source_sha256,
        target_byte_count: measured.byte_count,
        target_record_count: measured.record_count,
        target_sha256: measured.sha256,
        target_end_ordinal_exclusive: measured.end_ordinal_exclusive,
    })
}

struct MeasuredReplay {
    byte_count: u64,
    record_count: u64,
    sha256: String,
    end_ordinal_exclusive: u64,
}

async fn measure_replay(
    source_path: &Path,
    thread_id: ThreadId,
    canonical_session_meta: &RolloutLine,
    bounded_items: Option<&[RolloutItem]>,
    rollback_plan: &RollbackPlan,
    subagent_history_start_ordinal: Option<u64>,
) -> ThreadStoreResult<MeasuredReplay> {
    let mut writer = MeasuringWriter::default();
    let end_ordinal_exclusive = replay(
        source_path,
        thread_id,
        canonical_session_meta,
        bounded_items,
        rollback_plan,
        subagent_history_start_ordinal,
        &mut writer,
    )
    .await?;
    writer.flush().await.map_err(migration_error)?;
    let (byte_count, record_count, sha256) = writer.finish();
    Ok(MeasuredReplay {
        byte_count,
        record_count,
        sha256,
        end_ordinal_exclusive,
    })
}

async fn replay<W>(
    source_path: &Path,
    thread_id: ThreadId,
    canonical_session_meta: &RolloutLine,
    bounded_items: Option<&[RolloutItem]>,
    rollback_plan: &RollbackPlan,
    subagent_history_start_ordinal: Option<u64>,
    writer: &mut W,
) -> ThreadStoreResult<u64>
where
    W: AsyncWrite + Unpin,
{
    let mut canonicalizer = LegacyRolloutCanonicalizer::new(thread_id);
    if let Some(boundary) = subagent_history_start_ordinal {
        let mut head = Vec::new();
        canonicalizer
            .write_head_session_meta(canonical_session_meta.clone(), &mut head)
            .await?;
        let mut line = serde_json::from_slice::<RolloutLine>(&head).map_err(migration_error)?;
        let RolloutItem::SessionMeta(session_meta) = &mut line.item else {
            return Err(migration_error(
                "canonical rollout head is not session metadata",
            ));
        };
        session_meta.meta.subagent_history_start_ordinal = Some(boundary);
        writer
            .write_all(
                serde_json::to_string(&line)
                    .map_err(migration_error)?
                    .as_bytes(),
            )
            .await
            .map_err(migration_error)?;
        writer.write_all(b"\n").await.map_err(migration_error)?;
    } else {
        canonicalizer
            .write_head_session_meta(canonical_session_meta.clone(), writer)
            .await?;
    }

    if let Some(items) = bounded_items {
        for item in items {
            canonicalizer
                .process_line(
                    RolloutLine {
                        timestamp: canonical_session_meta.timestamp.clone(),
                        ordinal: None,
                        item: item.clone(),
                    },
                    writer,
                )
                .await?;
        }
        canonicalizer
            .finish(writer, canonical_session_meta.timestamp.as_str())
            .await?;
        return Ok(canonicalizer.next_ordinal());
    }

    let mut parsed_record_index = 0_usize;
    let mut last_timestamp = canonical_session_meta.timestamp.clone();
    let mut reader = codex_rollout::open_rollout_line_reader(source_path)
        .await
        .map_err(migration_error)?;
    while let Some(raw) = reader.next_line().await.map_err(migration_error)? {
        if raw.len() > MAX_ROLLOUT_LINE_BYTES {
            continue;
        }
        let Ok(Some(line)) = line_parser::parse_legacy_rollout_line(raw.as_bytes()) else {
            continue;
        };
        let planned = rollback_plan.apply(parsed_record_index, line)?;
        parsed_record_index = parsed_record_index
            .checked_add(1)
            .ok_or_else(|| migration_error("legacy rollout record index overflow"))?;
        let Some(line) = planned else {
            canonicalizer.skip_source_line()?;
            continue;
        };
        last_timestamp = line.timestamp.clone();
        canonicalizer.process_line(line, writer).await?;
    }
    if parsed_record_index != rollback_plan.record_count() {
        return Err(migration_error(
            "rollback plan source length changed during dry-run replay",
        ));
    }
    canonicalizer.finish(writer, &last_timestamp).await?;
    Ok(canonicalizer.next_ordinal())
}

async fn canonical_session_meta(path: &Path) -> ThreadStoreResult<RolloutLine> {
    let mut reader = codex_rollout::open_rollout_line_reader(path)
        .await
        .map_err(migration_error)?;
    while let Some(raw) = reader.next_line().await.map_err(migration_error)? {
        if raw.len() > MAX_ROLLOUT_LINE_BYTES {
            continue;
        }
        let Ok(Some(line)) = line_parser::parse_legacy_rollout_line(raw.as_bytes()) else {
            continue;
        };
        if matches!(line.item, RolloutItem::SessionMeta(_)) {
            return Ok(line);
        }
    }
    Err(migration_error("rollout contains no session metadata"))
}

async fn build_rollback_plan(path: &Path) -> ThreadStoreResult<RollbackPlan> {
    let mut reader = codex_rollout::open_rollout_line_reader(path)
        .await
        .map_err(migration_error)?;
    let mut planner = RollbackPlanner::new();
    while let Some(raw) = reader.next_line().await.map_err(migration_error)? {
        if raw.len() > MAX_ROLLOUT_LINE_BYTES {
            continue;
        }
        let Ok(Some(line)) = line_parser::parse_legacy_rollout_line(raw.as_bytes()) else {
            continue;
        };
        planner.observe(&line)?;
    }
    Ok(planner.finish())
}
