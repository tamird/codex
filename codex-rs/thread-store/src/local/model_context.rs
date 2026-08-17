use std::fs::File;
use std::fs::Metadata;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::ModelContextScan;
use codex_rollout::ModelContextScanProgress;
use codex_rollout::ReverseJsonlScanner;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_rollout::RolloutRecorder;
use codex_rollout::ScanOutcome;

use super::LocalThreadStore;
use super::read_thread;
use super::rollout_lineage::RolloutLineage;
use crate::LoadThreadHistoryParams;
use crate::StoredModelContext;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

/// Maximum source bytes an interactive latest-state request may scan for model context.
///
/// Valid segmented histories place a certified checkpoint inside the bounded active segment.
/// When an older one-file history exceeds this limit, the bounded checkpoint probe yields to the
/// complete compatibility reader rather than rejecting an otherwise valid rollout.
pub(super) const MAX_INTERACTIVE_MODEL_CONTEXT_SCAN_BYTES: u64 = 64 * 1024 * 1024;
// `MAX_USER_INPUT_TEXT_CHARS` permits one mebibyte of characters. Eight mebibytes leaves room
// for four-byte UTF-8 plus JSON escaping and record metadata without admitting an unbounded line.
pub(super) const MAX_INTERACTIVE_MODEL_CONTEXT_RECORD_BYTES: usize =
    codex_protocol::user_input::MAX_USER_INPUT_TEXT_CHARS * 8;
const MODEL_CONTEXT_SCAN_LIMIT_EXCEEDED: &str =
    "latest model context exceeds the bounded active scan limit";

#[cfg(test)]
#[path = "model_context_tests.rs"]
mod tests;

/// Loads rollout items needed to reconstruct the latest model-visible context.
///
/// Paginated JSONL rollouts use a reverse scan. When it finds both a usable replacement-
/// history checkpoint and the completed user-turn context needed for resume metadata, the returned
/// replay starts with the canonical head `SessionMeta` followed by that newest suffix. When no
/// bounded cutoff is available, the scan continues to the beginning and returns the complete
/// replay it already accumulated.
///
/// Compressed segments are decoded before applying their original JSONL offsets.
/// Indexed segmented legacy rollouts with complete compaction checkpoints use the same bounded
/// active scan. Unindexed or inherited rollouts still replay all canonical history.
pub(super) async fn load_latest_model_context(
    store: &LocalThreadStore,
    params: LoadThreadHistoryParams,
) -> ThreadStoreResult<StoredModelContext> {
    let resolved = if params.include_archived {
        super::thread_rollout_resolver::resolve_current_including_archived(store, params.thread_id)
            .await?
    } else {
        super::thread_rollout_resolver::resolve_current(store, params.thread_id).await?
    }
    .ok_or_else(|| ThreadStoreError::InvalidRequest {
        message: format!("no rollout found for thread id {}", params.thread_id),
    })?;
    let mut path = resolved.path;
    let rollout_id = resolved.rollout_id;

    let mut history_access =
        super::goal_supervisor_runtime_repair::repair_selected_history_before_access(
            store,
            params.thread_id,
            path.as_path(),
            super::goal_supervisor_runtime_repair::RepairAccess::ActiveOnly,
        )
        .await?;
    path = codex_rollout::existing_rollout_path(path.as_path())
        .await
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!(
                "rollout {} disappeared after history repair",
                path.display()
            ),
        })?;
    let certified_snapshot = history_access
        .take_certified_active_snapshot()
        .filter(|snapshot| snapshot.rollout_path == path);
    let (session_meta, projected_after_repair) = if let Some(snapshot) = certified_snapshot {
        (snapshot.head.session_meta, Some(snapshot.scan.items))
    } else {
        drop(history_access);
        history_access =
            super::goal_supervisor_runtime_repair::repair_selected_history_before_access(
                store,
                params.thread_id,
                path.as_path(),
                super::goal_supervisor_runtime_repair::RepairAccess::Compatibility,
            )
            .await?;
        path = codex_rollout::existing_rollout_path(path.as_path())
            .await
            .ok_or_else(|| ThreadStoreError::Internal {
                message: format!(
                    "rollout {} disappeared after compatibility repair",
                    path.display()
                ),
            })?;
        let session_meta = codex_rollout::read_session_meta_line(path.as_path())
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to read session metadata {}: {err}", path.display()),
            })?;
        let projected =
            scan_projected_active_model_context(store, rollout_id, &path, &session_meta).await?;
        (session_meta, projected)
    };
    if session_meta.meta.id != params.thread_id {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "rollout at {} belongs to thread {}, not {}",
                path.display(),
                session_meta.meta.id,
                params.thread_id
            ),
        });
    }
    let _history_access = history_access;

    let items = if let Some(items) = projected_after_repair {
        items
    } else if matches!(session_meta.meta.history_mode, ThreadHistoryMode::Paginated)
        || (matches!(session_meta.meta.history_mode, ThreadHistoryMode::Legacy)
            && session_meta.meta.segment_id.is_some())
    {
        if matches!(session_meta.meta.history_mode, ThreadHistoryMode::Legacy) {
            read_thread::load_history_items(store.config.codex_home.as_path(), path.as_path())
                .await?
        } else {
            let lineage = store.resolve_rollout_lineage(params.thread_id).await?;
            scan_model_context_from_lineage(lineage, session_meta).await?
        }
    } else {
        read_thread::load_history_items(store.config.codex_home.as_path(), path.as_path()).await?
    };

    Ok(StoredModelContext {
        thread_id: params.thread_id,
        items,
    })
}

/// Uses an indexed active checkpoint without traversing immutable same-thread predecessors.
///
/// The SQLite projection proves that the active file was fully indexed. A completed
/// `ModelContextScan` then proves the active segment contains all checkpoint and turn metadata
/// needed for resume. Any unprojected, inherited, incomplete, or concurrently replaced rollout
/// preserves the existing complete-lineage implementation.
pub(super) async fn scan_projected_active_model_context(
    _store: &LocalThreadStore,
    _rollout_id: codex_protocol::RolloutId,
    path: &Path,
    session_meta: &SessionMetaLine,
) -> ThreadStoreResult<Option<Vec<RolloutItem>>> {
    let compressed_active = path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".jsonl.zst"));
    if path.extension().and_then(|extension| extension.to_str()) != Some("jsonl")
        && !compressed_active
    {
        return Ok(None);
    }

    let before = tokio::fs::metadata(path)
        .await
        .map_err(thread_store_io_error)?;
    let active_scan = if compressed_active {
        let path_for_scan = path.to_path_buf();
        let meta_for_scan = session_meta.clone();
        let result = tokio::task::spawn_blocking(move || {
            scan_compressed_active_model_context_blocking(&path_for_scan, meta_for_scan)
        })
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to join compressed active model context scan: {err}"),
        })?;
        active_model_context_scan_or_fallback(result)?
    } else {
        let path_for_scan = path.to_path_buf();
        let meta_for_scan = session_meta.clone();
        let result = tokio::task::spawn_blocking(move || {
            scan_projected_active_model_context_blocking(&path_for_scan, meta_for_scan)
        })
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to join active model context scan: {err}"),
        })?;
        active_model_context_scan_or_fallback(result)?
    };
    let Some(active_scan) = active_scan else {
        return Ok(None);
    };
    if !active_scan.segment_checkpoint {
        return Ok(None);
    }

    let after = tokio::fs::metadata(path)
        .await
        .map_err(thread_store_io_error)?;
    if !unchanged_active_rollout(&before, &after).map_err(thread_store_io_error)? {
        return Ok(None);
    }

    tracing::debug!(
        outcome = "active_checkpoint_hit",
        active_segments_opened = 1_u64,
        referenced_segments_opened = 0_u64,
        active_segment_bytes = before.len(),
        records_scanned = active_scan.records_scanned,
        compressed_active,
        "loaded latest model context from the active rollout segment"
    );

    Ok(Some(active_scan.items))
}

pub(super) struct ActiveModelContextScan {
    pub(super) items: Vec<RolloutItem>,
    pub(super) suffix_lines: Vec<RolloutLine>,
    pub(super) segment_checkpoint: bool,
    pub(super) records_scanned: u64,
    pub(super) latest_ordinal: Option<u64>,
}

fn scan_loaded_active_model_context(
    lines: Vec<RolloutLine>,
    session_meta: SessionMetaLine,
) -> Option<ActiveModelContextScan> {
    let mut scan = ModelContextScan::default();
    let mut suffix_lines = Vec::new();
    let mut latest_ordinal = None;
    for (index, line) in lines.into_iter().rev().enumerate() {
        let records_scanned = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
        latest_ordinal = latest_ordinal.max(line.ordinal);
        if matches!(
            line.item,
            RolloutItem::SessionMeta(_) | RolloutItem::RolloutReference(_)
        ) {
            continue;
        }
        let complete = scan.push(line.item.clone()).is_complete();
        suffix_lines.push(line);
        if complete {
            let segment_checkpoint = scan.completed_at_segment_checkpoint();
            suffix_lines.reverse();
            return Some(ActiveModelContextScan {
                items: scan.finish(session_meta),
                suffix_lines,
                segment_checkpoint,
                records_scanned,
                latest_ordinal,
            });
        }
    }
    None
}

fn scan_projected_active_model_context_blocking(
    path: &Path,
    session_meta: SessionMetaLine,
) -> io::Result<Option<ActiveModelContextScan>> {
    scan_projected_active_model_context_blocking_with_limit(
        path,
        session_meta,
        MAX_INTERACTIVE_MODEL_CONTEXT_SCAN_BYTES,
    )
}

fn scan_projected_active_model_context_blocking_with_limit(
    path: &Path,
    session_meta: SessionMetaLine,
    max_scan_bytes: u64,
) -> io::Result<Option<ActiveModelContextScan>> {
    let file = File::open(path)?;
    let mut scanner = ReverseJsonlScanner::new(file)?
        .with_max_record_bytes(MAX_INTERACTIVE_MODEL_CONTEXT_RECORD_BYTES);
    let mut scan = ModelContextScan::default();
    let mut suffix_lines = Vec::new();
    let mut records_scanned = 0_u64;
    let mut latest_ordinal = None;
    loop {
        let outcome = scanner.scan_next::<serde_json::Value>()?;
        if scanner.oversized_records_skipped() != 0 {
            return Err(interactive_model_context_too_large());
        }
        if scanner.bytes_scanned() > max_scan_bytes {
            return Err(interactive_model_context_too_large());
        }
        let Some(outcome) = outcome else {
            break;
        };
        let value = match outcome {
            ScanOutcome::Parsed(value) => value,
            ScanOutcome::Rejected(err) => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, err));
            }
        };
        let Some(line) = RolloutRecorder::parse_rollout_line_value(value)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
        else {
            continue;
        };
        records_scanned = records_scanned.saturating_add(1);
        latest_ordinal = latest_ordinal.max(line.ordinal);
        if matches!(
            line.item,
            RolloutItem::SessionMeta(_) | RolloutItem::RolloutReference(_)
        ) {
            continue;
        }
        let complete = scan.push(line.item.clone()).is_complete();
        suffix_lines.push(line);
        if complete {
            let segment_checkpoint = scan.completed_at_segment_checkpoint();
            suffix_lines.reverse();
            return Ok(Some(ActiveModelContextScan {
                items: scan.finish(session_meta),
                suffix_lines,
                segment_checkpoint,
                records_scanned,
                latest_ordinal,
            }));
        }
    }
    Ok(None)
}

fn scan_compressed_active_model_context_blocking(
    path: &Path,
    session_meta: SessionMetaLine,
) -> io::Result<Option<ActiveModelContextScan>> {
    scan_compressed_active_model_context_blocking_with_limit(
        path,
        session_meta,
        MAX_INTERACTIVE_MODEL_CONTEXT_SCAN_BYTES,
        MAX_INTERACTIVE_MODEL_CONTEXT_RECORD_BYTES,
    )
}

fn scan_compressed_active_model_context_blocking_with_limit(
    path: &Path,
    session_meta: SessionMetaLine,
    max_scan_bytes: u64,
    max_record_bytes: usize,
) -> io::Result<Option<ActiveModelContextScan>> {
    let input = File::open(path)?;
    let decoder = zstd::stream::read::Decoder::new(input)?;
    let mut reader = BufReader::new(decoder);
    let mut lines = Vec::new();
    let mut decoded_bytes = 0_u64;
    loop {
        let mut record = Vec::new();
        let read = reader
            .by_ref()
            .take(max_record_bytes.saturating_add(1) as u64)
            .read_until(b'\n', &mut record)?;
        if read == 0 {
            break;
        }
        if read > max_record_bytes {
            return Err(interactive_model_context_too_large());
        }
        decoded_bytes = decoded_bytes.saturating_add(read as u64);
        if decoded_bytes > max_scan_bytes {
            return Err(interactive_model_context_too_large());
        }
        if record.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let value = serde_json::from_slice(record.as_slice())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if let Some(line) = RolloutRecorder::parse_rollout_line_value(value)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
        {
            lines.push(line);
        }
    }
    Ok(scan_loaded_active_model_context(lines, session_meta))
}

fn interactive_model_context_too_large() -> io::Error {
    io::Error::new(
        io::ErrorKind::FileTooLarge,
        MODEL_CONTEXT_SCAN_LIMIT_EXCEEDED,
    )
}

fn active_model_context_scan_or_fallback(
    result: io::Result<Option<ActiveModelContextScan>>,
) -> ThreadStoreResult<Option<ActiveModelContextScan>> {
    match result {
        Err(error) if error.kind() == io::ErrorKind::FileTooLarge => {
            tracing::debug!(
                outcome = "active_checkpoint_scan_limit",
                "bounded active model-context scan yielded to complete compatibility reconstruction"
            );
            Ok(None)
        }
        Ok(scan) => Ok(scan),
        Err(error) => Err(thread_store_io_error(error)),
    }
}

pub(super) fn interactive_model_context_scan_error(error: io::Error) -> ThreadStoreError {
    if error.kind() == io::ErrorKind::FileTooLarge {
        ThreadStoreError::InvalidRequest {
            message: MODEL_CONTEXT_SCAN_LIMIT_EXCEEDED.to_string(),
        }
    } else {
        thread_store_io_error(error)
    }
}

/// Reads only the certified suffix of a plain active rollout.
///
/// Indexed latest-fork preparation uses the same reverse scan as resume. Keeping this helper in
/// the model-context module prevents fork preparation from buffering the complete active JSONL.
pub(super) async fn scan_plain_active_model_context_snapshot(
    path: &Path,
    session_meta: SessionMetaLine,
) -> ThreadStoreResult<Option<ActiveModelContextScan>> {
    let path = path.to_path_buf();
    let result = tokio::task::spawn_blocking(move || {
        scan_projected_active_model_context_blocking(path.as_path(), session_meta)
    })
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to join active model context scan: {err}"),
    })?;
    active_model_context_scan_or_fallback(result)
}

fn unchanged_active_rollout(before: &Metadata, after: &Metadata) -> io::Result<bool> {
    if before.len() != after.len() || before.modified()? != after.modified()? {
        return Ok(false);
    }
    #[cfg(unix)]
    if before.dev() != after.dev()
        || before.ino() != after.ino()
        || before.ctime() != after.ctime()
        || before.ctime_nsec() != after.ctime_nsec()
    {
        return Ok(false);
    }
    Ok(true)
}

/// Loads startup context from a fork's frozen inherited prefix.
pub(super) async fn load_for_fork(
    lineage: RolloutLineage,
    history_base: Option<HistoryPosition>,
) -> ThreadStoreResult<Vec<RolloutItem>> {
    let source_path = lineage
        .segments()
        .last()
        .map(|segment| segment.rollout_path.as_path())
        .ok_or_else(|| ThreadStoreError::Internal {
            message: "fork lineage has no source segment".to_string(),
        })?;
    let session_meta = codex_rollout::read_session_meta_line(source_path)
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!(
                "failed to read session metadata {}: {err}",
                source_path.display()
            ),
        })?;
    match history_base {
        Some(history_base) => {
            let lineage = lineage.truncate_at(history_base).await?;
            scan_model_context_from_lineage(lineage, session_meta).await
        }
        None => Ok(vec![RolloutItem::SessionMeta(session_meta)]),
    }
}

/// Loads a fork boundary only when its selected physical segment contains a certified checkpoint.
///
/// This is the proof required by bounded explicit-fork preparation: a single physical segment is
/// sufficient only when its checkpoint replaces all older model-visible context.
pub(super) fn load_certified_prefix_for_fork(
    lines: &[RolloutLine],
) -> ThreadStoreResult<Option<Vec<RolloutItem>>> {
    let Some(RolloutItem::SessionMeta(session_meta)) = lines.first().map(|line| &line.item) else {
        return Err(ThreadStoreError::Internal {
            message: "bounded fork prefix does not start with session metadata".to_string(),
        });
    };
    let Some(scan) = scan_loaded_active_model_context(lines.to_vec(), session_meta.clone()) else {
        return Ok(None);
    };
    let items = scan.items;
    let certified = items.iter().enumerate().any(|(index, item)| {
        let RolloutItem::Compacted(compacted) = item else {
            return false;
        };
        codex_rollout::validated_segment_state_checkpoint(compacted, &items[index + 1..]).is_some()
    });
    Ok(certified.then_some(items))
}

/// Loads the complete logical prefix selected for a prepared fork.
///
/// Unlike [`load_for_fork`], this is response hydration rather than model input, so it must not
/// stop at a replacement-history checkpoint.
pub(super) async fn load_full_for_fork(
    lineage: RolloutLineage,
    history_base: Option<HistoryPosition>,
) -> ThreadStoreResult<Vec<RolloutItem>> {
    let source_path = lineage
        .segments()
        .last()
        .map(|segment| segment.rollout_path.as_path())
        .ok_or_else(|| ThreadStoreError::Internal {
            message: "fork lineage has no source segment".to_string(),
        })?;
    let session_meta = codex_rollout::read_session_meta_line(source_path)
        .await
        .map_err(thread_store_io_error)?;
    load_full_for_fork_with_session_meta(lineage, history_base, session_meta).await
}

/// Loads complete fork history when the caller already authenticated the source metadata.
pub(super) async fn load_full_for_fork_with_session_meta(
    lineage: RolloutLineage,
    history_base: Option<HistoryPosition>,
    session_meta: SessionMetaLine,
) -> ThreadStoreResult<Vec<RolloutItem>> {
    let Some(history_base) = history_base else {
        return Ok(vec![RolloutItem::SessionMeta(session_meta)]);
    };
    let lineage = lineage.truncate_at(history_base).await?;
    let mut items = vec![RolloutItem::SessionMeta(session_meta)];
    for segment in lineage.segments() {
        let (lines, _, parse_errors) =
            codex_rollout::RolloutRecorder::load_rollout_lines(segment.rollout_path.as_path())
                .await
                .map_err(thread_store_io_error)?;
        if parse_errors != 0 {
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "failed to load prepared fork history: {} contains {parse_errors} invalid record(s)",
                    segment.rollout_path.display()
                ),
            });
        }
        for line in lines {
            let Some(ordinal) = line.ordinal else {
                continue;
            };
            if ordinal < segment.start_ordinal
                || segment
                    .end_ordinal_exclusive
                    .is_some_and(|end| ordinal >= end)
            {
                continue;
            }
            let mut item = line.item;
            if matches!(
                item,
                RolloutItem::SessionMeta(_) | RolloutItem::RolloutReference(_)
            ) || !segment.filter_rollout_item(&mut item)
            {
                continue;
            }
            items.push(item);
        }
    }
    Ok(items)
}

async fn scan_model_context_from_lineage(
    lineage: RolloutLineage,
    session_meta: SessionMetaLine,
) -> ThreadStoreResult<Vec<RolloutItem>> {
    let scan = tokio::task::spawn_blocking(move || {
        scan_model_context_from_lineage_blocking(&lineage, session_meta)
    })
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to join model context scan: {err}"),
    })?;
    match scan {
        Ok(items) => Ok(items),
        Err(err) => Err(ThreadStoreError::Internal {
            message: format!("failed to scan paginated model context lineage: {err}"),
        }),
    }
}

fn scan_model_context_from_lineage_blocking(
    lineage: &RolloutLineage,
    session_meta: SessionMetaLine,
) -> io::Result<Vec<RolloutItem>> {
    let mut scan = ModelContextScan::default();
    'segments: for segment in lineage.segments().iter().rev() {
        let file = codex_rollout::open_rollout_seekable_reader(segment.rollout_path.as_path())?;
        let mut scanner = match segment.end_byte_offset {
            Some(end_byte_offset) => ReverseJsonlScanner::new_at(file, end_byte_offset)?,
            None => ReverseJsonlScanner::new(file)?,
        };
        while let Some(outcome) = scanner.scan_next::<serde_json::Value>()? {
            let ScanOutcome::Parsed(value) = outcome else {
                continue;
            };
            let Ok(Some(line)) = RolloutRecorder::parse_rollout_line_value(value) else {
                continue;
            };
            if let Some(ordinal) = line.ordinal
                && (ordinal < segment.start_ordinal
                    || segment
                        .end_ordinal_exclusive
                        .is_some_and(|end| ordinal >= end))
            {
                continue;
            }
            // Each physical segment contributes only its local delta. Its head metadata is
            // replaced with the requested thread's canonical SessionMeta after replay.
            let mut item = line.item;
            if matches!(&item, RolloutItem::SessionMeta(_)) {
                break;
            }
            if matches!(&item, RolloutItem::RolloutReference(_))
                || !segment.filter_rollout_item(&mut item)
            {
                continue;
            }
            match scan.push(item) {
                ModelContextScanProgress::Continue => {}
                ModelContextScanProgress::Complete => break 'segments,
            }
        }
    }

    let canonical_meta = session_meta.clone();
    let mut items = scan.finish(session_meta);
    if !matches!(items.first(), Some(RolloutItem::SessionMeta(_))) {
        items.insert(0, RolloutItem::SessionMeta(canonical_meta));
    }
    Ok(items)
}

fn thread_store_io_error(err: io::Error) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("failed to scan paginated model context lineage: {err}"),
    }
}
