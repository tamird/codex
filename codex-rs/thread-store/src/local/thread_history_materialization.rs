use std::io::SeekFrom;
use std::path::Path;

use chrono::DateTime;
use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_app_server_protocol::ThreadHistoryChangeSet;
use codex_app_server_protocol::project_rollout_line;
use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::BufReader;
use tracing::warn;

use super::LocalThreadStore;
use super::thread_history::ProjectedRolloutLine;
use super::thread_history::RolloutProjectionStep;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

pub(super) async fn materialize_to_sqlite(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &Path,
) -> ThreadStoreResult<()> {
    const MAX_PROJECTION_BATCH_RECORDS: usize = 256;
    const MAX_PROJECTION_BATCH_BYTES: u64 = 4 * 1024 * 1024;

    if store.state_db.is_none() {
        return Ok(());
    }
    let projection_state = super::thread_history::projection_state(store, thread_id).await?;
    let mut start_offset = projection_state
        .as_ref()
        .map_or(0, |state| state.next_byte_offset);
    if let Some(state) = projection_state.as_ref()
        && start_offset != 0
        && first_rollout_ordinal(rollout_path).await? == Some(state.next_ordinal)
    {
        // The stable rollout was replaced after its immutable prefix was projected. Recover the
        // physical cursor without changing the lineage ordinal or deleting the existing rows.
        super::thread_history::reset_projection_for_replacement(
            store,
            thread_id,
            state.next_ordinal,
        )
        .await?;
        start_offset = 0;
    }
    if projection_state.is_none()
        && codex_rollout::existing_rollout_path(rollout_path)
            .await
            .is_none()
    {
        return Ok(());
    }
    let session_meta = codex_rollout::read_session_meta_line(rollout_path)
        .await
        .map_err(thread_store_io_error)?
        .meta;
    let initial_ordinal = match session_meta.history_base {
        Some(base) => base.end_ordinal_exclusive,
        None => first_rollout_ordinal(rollout_path).await?.unwrap_or(0),
    };
    if projection_state.is_none()
        && (session_meta.history_base.is_some()
            || super::rollout_lineage::leading_same_thread_reference_ordinal(
                rollout_path,
                session_meta.id,
            )
            .await?
            .is_some())
    {
        super::thread_history::begin_incomplete_paginated_projection(
            store,
            thread_id,
            initial_ordinal,
        )
        .await?;
    }
    let subagent_history_start_ordinal = session_meta.subagent_history_start_ordinal;
    let expected_ordinal = projection_state
        .as_ref()
        .map_or(initial_ordinal, |state| state.next_ordinal);
    let path = rollout_path.to_path_buf();
    let file =
        tokio::task::spawn_blocking(move || codex_rollout::open_rollout_seekable_reader(&path))
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to join rollout projection read: {err}"),
            })?
            .map_err(thread_store_io_error)?;
    let mut file = tokio::fs::File::from_std(file);
    let end_offset = file.metadata().await.map_err(thread_store_io_error)?.len();
    let byte_count =
        end_offset
            .checked_sub(start_offset)
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "durable rollout shrank before projection".to_string(),
            })?;
    if byte_count == 0 {
        return Ok(());
    }

    file.seek(SeekFrom::Start(start_offset))
        .await
        .map_err(thread_store_io_error)?;
    let mut reader = BufReader::new(file.take(byte_count));
    let mut line_bytes = Vec::new();
    let mut batch_start_offset = start_offset;
    let mut next_offset = start_offset;
    let mut line_start_offset = start_offset;
    let mut next_ordinal = expected_ordinal;
    let mut pending_rejected_line_count = 0_u64;
    let mut projections = Vec::with_capacity(MAX_PROJECTION_BATCH_RECORDS);

    loop {
        line_bytes.clear();
        let bytes_read = reader
            .read_until(b'\n', &mut line_bytes)
            .await
            .map_err(thread_store_io_error)?;
        if bytes_read == 0 || !line_bytes.ends_with(b"\n") {
            break;
        }
        let line_end_offset = line_start_offset
            .checked_add(
                u64::try_from(bytes_read).map_err(|_| ThreadStoreError::Internal {
                    message: "durable rollout line exceeds addressable range".to_string(),
                })?,
            )
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "durable rollout byte offset overflow".to_string(),
            })?;

        if line_bytes.iter().all(u8::is_ascii_whitespace) {
            if pending_rejected_line_count == 0 {
                next_offset = line_end_offset;
            }
            line_start_offset = line_end_offset;
            if pending_rejected_line_count == 0
                && next_offset.saturating_sub(batch_start_offset) >= MAX_PROJECTION_BATCH_BYTES
            {
                apply_paginated_projection_batch(
                    store,
                    thread_id,
                    &mut batch_start_offset,
                    next_offset,
                    initial_ordinal,
                    &mut projections,
                )
                .await?;
            }
            continue;
        }

        let value = match serde_json::from_slice::<serde_json::Value>(&line_bytes) {
            Ok(value) => value,
            Err(err) => {
                if pending_rejected_line_count == 0 {
                    apply_paginated_projection_batch(
                        store,
                        thread_id,
                        &mut batch_start_offset,
                        next_offset,
                        initial_ordinal,
                        &mut projections,
                    )
                    .await?;
                }
                warn!(
                    %thread_id,
                    rollout_path = %rollout_path.display(),
                    line_start_byte_offset = line_start_offset,
                    line_end_byte_offset = line_end_offset,
                    expected_ordinal = next_ordinal,
                    error = %err,
                    "deferring rejected rollout line until a later ordinal resolves it"
                );
                pending_rejected_line_count += 1;
                line_start_offset = line_end_offset;
                continue;
            }
        };
        let value_ordinal = value.get("ordinal").and_then(serde_json::Value::as_u64);
        let line = match serde_json::from_value::<RolloutLine>(value) {
            Ok(line) => Some(line),
            Err(err) => {
                warn!(
                    %thread_id,
                    rollout_path = %rollout_path.display(),
                    line_start_byte_offset = line_start_offset,
                    line_end_byte_offset = line_end_offset,
                    expected_ordinal = next_ordinal,
                    line_ordinal = ?value_ordinal,
                    error = %err,
                    "deferring unknown rollout line until a later ordinal resolves it"
                );
                None
            }
        };
        let ordinal = match line
            .as_ref()
            .and_then(|line| line.ordinal)
            .or(value_ordinal)
        {
            Some(ordinal) => ordinal,
            None if line.is_none() => {
                if pending_rejected_line_count == 0 {
                    apply_paginated_projection_batch(
                        store,
                        thread_id,
                        &mut batch_start_offset,
                        next_offset,
                        initial_ordinal,
                        &mut projections,
                    )
                    .await?;
                }
                pending_rejected_line_count += 1;
                line_start_offset = line_end_offset;
                continue;
            }
            None => {
                return Err(ThreadStoreError::Internal {
                    message: format!(
                        "paginated rollout line for {thread_id} is missing an ordinal"
                    ),
                });
            }
        };
        if ordinal < next_ordinal {
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "thread history projection for {thread_id} expected ordinal {next_ordinal}, got {ordinal}"
                ),
            });
        }
        let Some(line) = line else {
            if pending_rejected_line_count == 0 {
                apply_paginated_projection_batch(
                    store,
                    thread_id,
                    &mut batch_start_offset,
                    next_offset,
                    initial_ordinal,
                    &mut projections,
                )
                .await?;
            }
            pending_rejected_line_count += 1;
            line_start_offset = line_end_offset;
            continue;
        };
        let skipped_ordinal_count = ordinal - next_ordinal;
        if skipped_ordinal_count > pending_rejected_line_count {
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "thread history projection for {thread_id} expected ordinal {next_ordinal}, got {ordinal}; {pending_rejected_line_count} rejected rollout lines cannot cover that gap"
                ),
            });
        }
        let is_inherited_subagent_history =
            subagent_history_start_ordinal.is_some_and(|start| ordinal < start);
        let changes = if is_inherited_subagent_history {
            ThreadHistoryChangeSet::default()
        } else {
            project_rollout_line(&line)
        };
        let fallback_created_at_ms = if changes
            .changed_items
            .iter()
            .any(|item| item.started_at_ms.is_none())
            || (!is_inherited_subagent_history && matches!(line.item, RolloutItem::RealtimeItem(_)))
        {
            match DateTime::parse_from_rfc3339(line.timestamp.as_str()) {
                Ok(timestamp) => Some(timestamp.timestamp_millis()),
                Err(err) => {
                    if pending_rejected_line_count == 0 {
                        apply_paginated_projection_batch(
                            store,
                            thread_id,
                            &mut batch_start_offset,
                            next_offset,
                            initial_ordinal,
                            &mut projections,
                        )
                        .await?;
                    }
                    warn!(
                        %thread_id,
                        rollout_path = %rollout_path.display(),
                        line_start_byte_offset = line_start_offset,
                        line_end_byte_offset = line_end_offset,
                        expected_ordinal = next_ordinal,
                        line_ordinal = ordinal,
                        error = %err,
                        "deferring rollout line with invalid timestamp until a later ordinal resolves it"
                    );
                    pending_rejected_line_count += 1;
                    line_start_offset = line_end_offset;
                    continue;
                }
            }
        } else {
            None
        };
        if skipped_ordinal_count > 0 {
            warn!(
                %thread_id,
                rollout_path = %rollout_path.display(),
                line_start_byte_offset = line_start_offset,
                line_end_byte_offset = line_end_offset,
                expected_ordinal = next_ordinal,
                line_ordinal = ordinal,
                skipped_ordinal_start = next_ordinal,
                skipped_ordinal_end_exclusive = ordinal,
                "skipping rollout ordinal range after rejected lines"
            );
            projections.push(RolloutProjectionStep::SkippedOrdinalRange {
                start_ordinal: next_ordinal,
                end_ordinal_exclusive: ordinal,
            });
        }
        pending_rejected_line_count = 0;
        projections.push(RolloutProjectionStep::Line(Box::new(
            ProjectedRolloutLine {
                ordinal,
                start_byte_offset: line_start_offset,
                end_byte_offset: line_end_offset,
                fallback_created_at_ms,
                changes,
                realtime_item: if is_inherited_subagent_history {
                    None
                } else if let RolloutItem::RealtimeItem(item) = line.item {
                    Some(item)
                } else {
                    None
                },
            },
        )));
        next_ordinal = ordinal
            .checked_add(1)
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "rollout ordinal exceeds SQLite integer range".to_string(),
            })?;
        next_offset = line_end_offset;
        line_start_offset = line_end_offset;

        if projections.len() >= MAX_PROJECTION_BATCH_RECORDS
            || next_offset.saturating_sub(batch_start_offset) >= MAX_PROJECTION_BATCH_BYTES
        {
            apply_paginated_projection_batch(
                store,
                thread_id,
                &mut batch_start_offset,
                next_offset,
                initial_ordinal,
                &mut projections,
            )
            .await?;
        }
    }

    if pending_rejected_line_count == 0 {
        apply_paginated_projection_batch(
            store,
            thread_id,
            &mut batch_start_offset,
            next_offset,
            initial_ordinal,
            &mut projections,
        )
        .await?;
    }
    Ok(())
}

async fn apply_paginated_projection_batch(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    batch_start_offset: &mut u64,
    next_offset: u64,
    initial_ordinal: u64,
    projections: &mut Vec<RolloutProjectionStep>,
) -> ThreadStoreResult<()> {
    if *batch_start_offset == next_offset && projections.is_empty() {
        return Ok(());
    }
    super::thread_history::apply_projection(
        store,
        thread_id,
        *batch_start_offset,
        next_offset,
        initial_ordinal,
        std::mem::take(projections),
    )
    .await?;
    *batch_start_offset = next_offset;
    Ok(())
}

/// Project legacy-visible history without changing canonical rollout records or their ordinals.
pub(super) async fn materialize_legacy_to_sqlite(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &Path,
    builder: &mut ThreadHistoryBuilder,
) -> ThreadStoreResult<()> {
    materialize_legacy_to_sqlite_backfill(
        store,
        thread_id,
        rollout_path,
        builder,
        /*complete*/ true,
    )
    .await
}

pub(super) async fn materialize_legacy_to_sqlite_backfill(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &Path,
    builder: &mut ThreadHistoryBuilder,
    complete: bool,
) -> ThreadStoreResult<()> {
    let result =
        materialize_legacy_to_sqlite_inner(store, thread_id, rollout_path, builder, complete).await;
    if result.is_err() {
        builder.reset();
    }
    result
}

async fn materialize_legacy_to_sqlite_inner(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &Path,
    builder: &mut ThreadHistoryBuilder,
    complete: bool,
) -> ThreadStoreResult<()> {
    const MAX_PROJECTION_BATCH_RECORDS: usize = 256;
    const MAX_PROJECTION_BATCH_BYTES: u64 = 4 * 1024 * 1024;

    let projection_state = super::thread_history::projection_state(store, thread_id).await?;
    let mut batch_start_offset = projection_state.as_ref().map_or(0, |state| {
        if state.next_byte_offset
            == super::thread_history::INCOMPLETE_LEGACY_PROJECTION_BYTE_OFFSET.unsigned_abs()
        {
            0
        } else {
            state.next_byte_offset
        }
    });
    let mut next_rollout_ordinal = projection_state
        .as_ref()
        .map_or(0, |state| state.next_ordinal);
    let initial_ordinal = next_rollout_ordinal;
    let path = rollout_path.to_path_buf();
    let file =
        tokio::task::spawn_blocking(move || codex_rollout::open_rollout_seekable_reader(&path))
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to join legacy rollout projection read: {err}"),
            })?;
    let mut file = match file {
        Ok(file) => tokio::fs::File::from_std(file),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound && batch_start_offset == 0 => {
            return Ok(());
        }
        Err(err) => return Err(thread_store_io_error(err)),
    };
    let end_offset = file.metadata().await.map_err(thread_store_io_error)?.len();
    let byte_count =
        end_offset
            .checked_sub(batch_start_offset)
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "durable rollout shrank before legacy projection".to_string(),
            })?;
    if byte_count == 0 {
        return Ok(());
    }

    file.seek(SeekFrom::Start(batch_start_offset))
        .await
        .map_err(thread_store_io_error)?;
    let mut reader = BufReader::new(file.take(byte_count));
    let mut line_bytes = Vec::new();
    let mut next_offset = batch_start_offset;
    let mut projections = Vec::with_capacity(MAX_PROJECTION_BATCH_RECORDS);

    loop {
        line_bytes.clear();
        let bytes_read = reader
            .read_until(b'\n', &mut line_bytes)
            .await
            .map_err(thread_store_io_error)?;
        if bytes_read == 0 || !line_bytes.ends_with(b"\n") {
            break;
        }

        let start_byte_offset = next_offset;
        next_offset = next_offset
            .checked_add(
                u64::try_from(bytes_read).map_err(|_| ThreadStoreError::Internal {
                    message: "legacy rollout line exceeds addressable range".to_string(),
                })?,
            )
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "legacy rollout byte offset overflow".to_string(),
            })?;

        if !line_bytes.iter().all(u8::is_ascii_whitespace) {
            match serde_json::from_slice::<RolloutLine>(&line_bytes) {
                Ok(line) => {
                    let created_at_ms = DateTime::parse_from_rfc3339(line.timestamp.as_str())
                        .map(|timestamp| timestamp.timestamp_millis())
                        .map_err(thread_history_error)?;
                    let is_physical_rollout_record =
                        matches!(&line.item, RolloutItem::SessionMeta(_))
                            && next_rollout_ordinal != 0
                            || matches!(&line.item, RolloutItem::RolloutReference(_));
                    let changes = if !is_physical_rollout_record
                        && codex_rollout::is_persisted_rollout_item(
                            &line.item,
                            ThreadHistoryMode::Legacy,
                        ) {
                        builder.handle_rollout_item_with_changes(&line.item)
                    } else {
                        ThreadHistoryChangeSet::default()
                    };
                    projections.push(RolloutProjectionStep::Line(Box::new(
                        ProjectedRolloutLine {
                            ordinal: next_rollout_ordinal,
                            start_byte_offset,
                            end_byte_offset: next_offset,
                            fallback_created_at_ms: Some(created_at_ms),
                            changes,
                            realtime_item: match line.item {
                                RolloutItem::RealtimeItem(item) => Some(item),
                                _ => None,
                            },
                        },
                    )));
                    next_rollout_ordinal =
                        next_rollout_ordinal.checked_add(1).ok_or_else(|| {
                            ThreadStoreError::Internal {
                                message: "legacy rollout projection ordinal overflow".to_string(),
                            }
                        })?;
                }
                Err(err) => {
                    warn!(
                        "skipping rejected legacy rollout line while projecting {rollout_path:?}: {err}"
                    );
                }
            }
        }

        if projections.len() >= MAX_PROJECTION_BATCH_RECORDS
            || next_offset - batch_start_offset >= MAX_PROJECTION_BATCH_BYTES
        {
            let batch = std::mem::replace(
                &mut projections,
                Vec::with_capacity(MAX_PROJECTION_BATCH_RECORDS),
            );
            super::thread_history::apply_legacy_projection(
                store,
                thread_id,
                batch_start_offset,
                next_offset,
                initial_ordinal,
                batch,
                /*complete*/ false,
            )
            .await?;
            batch_start_offset = next_offset;
        }
    }

    if next_offset != batch_start_offset || complete {
        super::thread_history::apply_legacy_projection(
            store,
            thread_id,
            batch_start_offset,
            next_offset,
            initial_ordinal,
            projections,
            complete,
        )
        .await?;
    }

    Ok(())
}

async fn first_rollout_ordinal(rollout_path: &Path) -> ThreadStoreResult<Option<u64>> {
    let mut reader = codex_rollout::open_rollout_line_reader(rollout_path)
        .await
        .map_err(thread_store_io_error)?;
    while let Some(line) = reader.next_line().await.map_err(thread_store_io_error)? {
        if line.trim().is_empty() {
            continue;
        }
        let line =
            serde_json::from_str::<RolloutLine>(line.as_str()).map_err(thread_history_error)?;
        return Ok(line.ordinal);
    }
    Ok(None)
}

fn thread_history_error(err: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("failed to project thread history: {err}"),
    }
}

fn thread_store_io_error(err: std::io::Error) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: err.to_string(),
    }
}

#[cfg(test)]
#[path = "thread_history_materialization_tests.rs"]
mod tests;
