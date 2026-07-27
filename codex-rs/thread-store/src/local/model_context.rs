use std::fs::File;
use std::fs::Metadata;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::ModelContextScan;
use codex_rollout::ModelContextScanProgress;
use codex_rollout::ReverseJsonlScanner;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_rollout::ScanOutcome;

use super::LocalThreadStore;
use super::read_thread;
use super::rollout_lineage::RolloutLineage;
use crate::LoadThreadHistoryParams;
use crate::StoredModelContext;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

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
    let path = resolved.path;
    let rollout_id = resolved.rollout_id;

    let session_meta = codex_rollout::read_session_meta_line(path.as_path())
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to read session metadata {}: {err}", path.display()),
        })?;
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

    let items = if matches!(session_meta.meta.history_mode, ThreadHistoryMode::Paginated)
        || (matches!(session_meta.meta.history_mode, ThreadHistoryMode::Legacy)
            && session_meta.meta.segment_id.is_some())
    {
        if let Some(items) =
            scan_projected_active_model_context(store, rollout_id, &path, &session_meta).await?
        {
            items
        } else if matches!(session_meta.meta.history_mode, ThreadHistoryMode::Legacy) {
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
    store: &LocalThreadStore,
    rollout_id: codex_protocol::RolloutId,
    path: &Path,
    session_meta: &SessionMetaLine,
) -> ThreadStoreResult<Option<Vec<RolloutItem>>> {
    if (matches!(session_meta.meta.history_mode, ThreadHistoryMode::Paginated)
        && session_meta.meta.forked_from_id.is_some())
        || session_meta.meta.history_base.is_some()
        || session_meta.meta.subagent_history_start_ordinal.is_some()
        || path.extension().and_then(|extension| extension.to_str()) != Some("jsonl")
    {
        return Ok(None);
    }

    let before = tokio::fs::metadata(path)
        .await
        .map_err(thread_store_io_error)?;
    let Some(projection_state) = super::thread_history::projection_state(store, rollout_id).await?
    else {
        return Ok(None);
    };
    if projection_state.next_byte_offset != before.len() {
        return Ok(None);
    }

    let path_for_scan = path.to_path_buf();
    let meta_for_scan = session_meta.clone();
    let (items, reference) = tokio::task::spawn_blocking(move || {
        scan_projected_active_model_context_blocking(&path_for_scan, meta_for_scan)
    })
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to join indexed model context scan: {err}"),
    })?
    .map_err(thread_store_io_error)?;
    let Some(items) = items else {
        return Ok(None);
    };

    if let Some(reference) = reference {
        if reference.thread_id != Some(session_meta.meta.id)
            || reference.nth_user_message.is_some()
            || reference
                .compacted_replacement_history_filter_texts
                .is_some()
        {
            return Ok(None);
        }
        codex_rollout::resolve_rollout_reference_path(
            store.config.codex_home.as_path(),
            &reference,
        )
        .await
        .map_err(thread_store_io_error)?;
    }

    let after = tokio::fs::metadata(path)
        .await
        .map_err(thread_store_io_error)?;
    if !unchanged_active_rollout(&before, &after).map_err(thread_store_io_error)? {
        return Ok(None);
    }

    Ok(Some(items))
}

fn scan_projected_active_model_context_blocking(
    path: &Path,
    session_meta: SessionMetaLine,
) -> io::Result<(Option<Vec<RolloutItem>>, Option<RolloutReferenceItem>)> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    line.clear();
    let reference = if reader.read_line(&mut line)? == 0 {
        None
    } else {
        let item = serde_json::from_str::<RolloutLine>(&line)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
            .item;
        match item {
            RolloutItem::RolloutReference(reference) => Some(reference),
            _ => None,
        }
    };

    let mut scanner = ReverseJsonlScanner::new(reader.into_inner())?;
    let mut scan = ModelContextScan::default();
    while let Some(outcome) = scanner.scan_next::<RolloutLine>()? {
        let ScanOutcome::Parsed(line) = outcome else {
            continue;
        };
        if matches!(
            line.item,
            RolloutItem::SessionMeta(_) | RolloutItem::RolloutReference(_)
        ) {
            continue;
        }
        if matches!(scan.push(line.item), ModelContextScanProgress::Complete) {
            return Ok((Some(scan.finish(session_meta)), reference));
        }
    }
    Ok((None, reference))
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
        while let Some(outcome) = scanner.scan_next::<RolloutLine>()? {
            let ScanOutcome::Parsed(line) = outcome else {
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
