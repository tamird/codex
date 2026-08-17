use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_protocol::RolloutId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_rollout::RolloutConfig;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_rollout::RolloutRecorder;
use codex_rollout::RolloutRecorderParams;
use codex_rollout::is_persisted_rollout_item;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;
use tracing::warn;

use super::LocalThreadStore;
use super::create_thread;
use crate::AppendThreadItemsParams;
use crate::CreateThreadParams;
use crate::ReadThreadParams;
use crate::ResumeThreadParams;
use crate::ThreadPersistenceMode;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use crate::types::canonical_history_mode_from_rollout_items;

const ROLLOUT_SIZE_BYTES_METRIC: &str = "codex.rollout.size_bytes";

pub(super) async fn create_thread(
    store: &LocalThreadStore,
    params: CreateThreadParams,
) -> ThreadStoreResult<()> {
    let thread_id = params.thread_id;
    let _live_writer_guard = store.live_writer_locks.lock(thread_id).await;
    let history_mode = params.history_mode;
    let persistence_mode = params.persistence_mode;
    let inherited_legacy_history =
        matches!(history_mode, ThreadHistoryMode::Legacy) && params.forked_from_id.is_some();
    store.ensure_live_recorder_absent(thread_id).await?;
    let writer_lock = store.writer_lock_coordinator.acquire(thread_id)?;
    let recorder = create_thread::create_thread(store, params).await?;
    store
        .insert_live_recorder(
            thread_id,
            recorder,
            thread_id,
            history_mode,
            writer_lock,
            persistence_mode,
        )
        .await?;
    if inherited_legacy_history
        && let Some(entry) = store.live_recorders.lock().await.get_mut(&thread_id)
    {
        // A fork can retain parent-owned history after later rotations replace its leading
        // cross-thread reference with a same-thread reference.
        entry.legacy_history_projection_enabled = false;
        entry.legacy_history_builder_needs_rebuild = false;
    }
    Ok(())
}

pub(super) async fn resume_thread(
    store: &LocalThreadStore,
    params: ResumeThreadParams,
) -> ThreadStoreResult<()> {
    // Keep the duplicate-writer error ahead of history inspection.
    store.ensure_live_recorder_absent(params.thread_id).await?;
    if let Some(history) = params.history.as_deref() {
        super::goal_supervisor_runtime_repair::reject_malformed_supplied_history(history)?;
    }
    if let Some(requested_path) = params.rollout_path.as_deref()
        && let Some(selected_path) = selected_rollout_path(store, params.thread_id).await?
        && codex_rollout::plain_rollout_path(requested_path)
            != codex_rollout::plain_rollout_path(selected_path.as_path())
    {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "rollout path does not select the current rollout for thread {}",
                params.thread_id
            ),
        });
    }
    let has_supplied_history = params.history.is_some();
    let history_mode = if let Some(history) = params.history.as_deref() {
        canonical_history_mode_from_rollout_items(history)
    } else if let Some(rollout_path) = params.rollout_path.as_ref() {
        super::read_thread::read_thread_by_rollout_path(
            store,
            rollout_path.clone(),
            params.include_archived,
            /*include_history*/ false,
        )
        .await?
        .history_mode
    } else {
        super::read_thread::read_thread(
            store,
            ReadThreadParams {
                thread_id: params.thread_id,
                include_archived: params.include_archived,
                include_history: false,
            },
        )
        .await?
        .history_mode
    };
    let rollout_path = match (params.rollout_path, params.history) {
        (Some(rollout_path), _history) => rollout_path,
        (None, _history) => {
            let thread = super::read_thread::read_thread(
                store,
                ReadThreadParams {
                    thread_id: params.thread_id,
                    include_archived: params.include_archived,
                    // History is consumed only after the active checkpoint decision below. Path
                    // discovery must not traverse predecessors first.
                    include_history: false,
                },
            )
            .await?;
            thread
                .rollout_path
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: format!("thread {} does not have a rollout path", params.thread_id),
                })?
        }
    };
    let supplied_empty_placeholder = has_supplied_history
        && std::fs::metadata(rollout_path.as_path()).is_ok_and(|metadata| metadata.len() == 0);
    let mut history_access = if !supplied_empty_placeholder {
        let session_meta = codex_rollout::read_session_meta_line(rollout_path.as_path())
            .await
            .map_err(|error| ThreadStoreError::Internal {
                message: format!("failed to resume local thread recorder: {error}"),
            })?;
        let rollout_id =
            codex_rollout::rollout_id_from_path(rollout_path.as_path()).unwrap_or(params.thread_id);
        if super::model_context::scan_projected_active_model_context(
            store,
            rollout_id,
            rollout_path.as_path(),
            &session_meta,
        )
        .await?
        .is_some()
        {
            super::goal_supervisor_runtime_repair::repair_active_history_before_access(
                store,
                params.thread_id,
                rollout_path.as_path(),
            )
            .await?
        } else {
            super::goal_supervisor_runtime_repair::repair_compatibility_history_before_access(
                store,
                params.thread_id,
                rollout_path.as_path(),
            )
            .await?
        }
    } else {
        super::goal_supervisor_runtime_repair::GoalSupervisorHistoryAccess::clean()
    };
    let _live_writer_guard = if history_access.is_reserved() {
        None
    } else {
        Some(store.live_writer_locks.lock(params.thread_id).await)
    };
    store.ensure_live_recorder_absent(params.thread_id).await?;
    let writer_lock = if history_access.is_reserved() {
        history_access
            .take_writer_lock(params.thread_id)
            .ok_or_else(|| ThreadStoreError::Internal {
                message: format!(
                    "goal-supervisor repair did not retain writer ownership for thread {}",
                    params.thread_id
                ),
            })?
    } else {
        store.writer_lock_coordinator.acquire(params.thread_id)?
    };
    super::segment::cleanup_stale_staged_rollouts(rollout_path.as_path()).await?;
    let cwd = params
        .metadata
        .cwd
        .clone()
        .ok_or_else(|| ThreadStoreError::InvalidRequest {
            message: "local thread store requires a cwd".to_string(),
        })?;
    let config = RolloutConfig {
        codex_home: store.config.codex_home.clone(),
        sqlite: store.config.sqlite.clone(),
        cwd,
        model_provider_id: params.metadata.model_provider.clone(),
        generate_memories: matches!(params.metadata.memory_mode, ThreadMemoryMode::Enabled),
    };
    let recorder = RolloutRecorder::new(&config, RolloutRecorderParams::resume(rollout_path))
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to resume local thread recorder: {err}"),
        })?;
    let rollout_path = recorder.rollout_path().to_path_buf();
    let noncanonical_session_meta =
        if codex_rollout::rollout_id_from_path(rollout_path.as_path()).is_none() {
            Some(
                codex_rollout::read_session_meta_line(rollout_path.as_path())
                    .await
                    .map_err(thread_store_io_error)?,
            )
        } else {
            None
        };
    let segmented_rollout = matches!(history_mode, ThreadHistoryMode::Legacy)
        && match noncanonical_session_meta.as_ref() {
            Some(metadata) => metadata.meta.segment_id.is_some(),
            None => codex_rollout::read_session_meta_line(rollout_path.as_path())
                .await
                .is_ok_and(|metadata| metadata.meta.segment_id.is_some()),
        };
    let rollout_id = match codex_rollout::rollout_id_from_path(rollout_path.as_path()) {
        Some(rollout_id) => rollout_id,
        None => {
            let authenticated_thread_id = noncanonical_session_meta
                .as_ref()
                .map(|metadata| metadata.meta.id)
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: format!(
                        "noncanonical rollout metadata is missing for {}",
                        rollout_path.display()
                    ),
                })?;
            super::thread_rollout_resolver::rollout_id_from_path_or_authenticated_thread_id(
                rollout_path.as_path(),
                params.thread_id,
                authenticated_thread_id,
            )?
        }
    };
    let segmented_legacy_projection_complete = if segmented_rollout {
        Some(if store.state_db().await.is_none() {
            has_supplied_history
        } else {
            let rollout_len = tokio::fs::metadata(recorder.rollout_path())
                .await
                .map_err(thread_store_io_error)?
                .len();
            super::thread_history::projection_state(store, rollout_id)
                .await?
                .is_some_and(|state| state.next_byte_offset == rollout_len)
        })
    } else {
        None
    };
    store
        .insert_live_recorder(
            params.thread_id,
            recorder,
            rollout_id,
            history_mode,
            writer_lock,
            ThreadPersistenceMode::Durable,
        )
        .await?;
    if let Some(projection_complete) = segmented_legacy_projection_complete
        && let Some(entry) = store.live_recorders.lock().await.get_mut(&params.thread_id)
    {
        entry.legacy_history_projection_enabled = projection_complete;
        entry.legacy_history_builder_needs_rebuild = projection_complete;
    }
    Ok(())
}

/// Returns the authoritative selected path without requiring a paginated rollout to parse first.
async fn selected_rollout_path(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<Option<PathBuf>> {
    if let Some(state_db) = store.state_db().await.as_deref()
        && let Some(metadata) =
            state_db
                .get_thread(thread_id)
                .await
                .map_err(|error| ThreadStoreError::Internal {
                    message: format!("failed to read thread metadata for {thread_id}: {error}"),
                })?
        && metadata.history_mode == ThreadHistoryMode::Paginated
    {
        return Ok(Some(metadata.rollout_path));
    }
    Ok(
        super::thread_rollout_resolver::resolve_current_including_archived(store, thread_id)
            .await?
            .map(|selected| selected.path),
    )
}

pub(super) async fn restore_segmented_legacy_history_builder_if_needed(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &std::path::Path,
) -> ThreadStoreResult<()> {
    let needs_rebuild = store
        .live_recorders
        .lock()
        .await
        .get(&thread_id)
        .is_some_and(|entry| entry.legacy_history_builder_needs_rebuild);
    if !needs_rebuild {
        return Ok(());
    }
    match build_segmented_legacy_history_projection(store, thread_id, rollout_path).await {
        Ok(builder) => {
            if let Some(entry) = store.live_recorders.lock().await.get_mut(&thread_id) {
                entry.legacy_history_builder = Arc::new(tokio::sync::Mutex::new(builder));
                entry.legacy_history_projection_enabled = true;
                entry.legacy_history_builder_needs_rebuild = false;
            }
        }
        Err(error) => {
            warn!(
                "cannot restore complete segmented legacy history for thread {thread_id}; continuing without history projection: {error}"
            );
            if let Some(entry) = store.live_recorders.lock().await.get_mut(&thread_id) {
                entry.legacy_history_projection_enabled = false;
                entry.legacy_history_builder_needs_rebuild = true;
            }
        }
    }
    Ok(())
}

/// Build the existing visible-history index from complete same-thread rollout segments.
pub(super) async fn backfill_segmented_legacy_projection(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &std::path::Path,
) -> ThreadStoreResult<()> {
    let builder = build_segmented_legacy_history_projection(store, thread_id, rollout_path).await?;
    if let Some(entry) = store.live_recorders.lock().await.get_mut(&thread_id) {
        entry.legacy_history_builder = Arc::new(tokio::sync::Mutex::new(builder));
        entry.legacy_history_projection_enabled = true;
        entry.legacy_history_builder_needs_rebuild = false;
    }
    Ok(())
}

async fn build_segmented_legacy_history_projection(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &std::path::Path,
) -> ThreadStoreResult<ThreadHistoryBuilder> {
    let session_meta = codex_rollout::read_session_meta_line(rollout_path)
        .await
        .map_err(thread_store_io_error)?;
    if session_meta.meta.id != thread_id
        || session_meta.meta.history_mode != ThreadHistoryMode::Legacy
        || session_meta.meta.segment_id.is_none()
        || session_meta.meta.history_base.is_some()
    {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!("thread {thread_id} does not have segmented legacy history"),
        });
    }

    let lineage = segmented_legacy_rollout_paths(store, thread_id, rollout_path).await?;
    for segment in &lineage {
        if segment
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".jsonl.zst"))
        {
            return Err(ThreadStoreError::Unsupported {
                operation: "compressed_legacy_history_projection",
            });
        }
    }

    let rollout_id = super::thread_rollout_resolver::rollout_id_from_path_or_legacy_thread_id(
        rollout_path,
        thread_id,
        ThreadHistoryMode::Legacy,
    )?;
    let projection = super::thread_history::projection_state(store, rollout_id).await?;
    let active_rollout_len = tokio::fs::metadata(rollout_path)
        .await
        .map_err(thread_store_io_error)?
        .len();
    if projection
        .is_some_and(|state| state.next_ordinal > 0 && state.next_byte_offset == active_rollout_len)
    {
        return stream_segmented_legacy_history_builder(
            &lineage,
            RolloutItem::SessionMeta(session_meta),
        )
        .await;
    }

    super::thread_history::delete_thread(store, rollout_id).await?;
    super::thread_history::begin_legacy_projection_backfill(store, rollout_id).await?;
    let mut builder = ThreadHistoryBuilder::new();
    let result: ThreadStoreResult<()> = async {
        for (index, segment) in lineage.iter().enumerate() {
            if index != 0 {
                let next_ordinal = super::thread_history::projection_state(store, rollout_id)
                    .await?
                    .ok_or_else(|| ThreadStoreError::Internal {
                        message: format!(
                            "segmented legacy history projection for {thread_id} has no checkpoint"
                        ),
                    })?
                    .next_ordinal;
                super::thread_history::reset_projection_for_replacement(
                    store,
                    rollout_id,
                    next_ordinal,
                )
                .await?;
            }
            super::thread_history_materialization::materialize_legacy_to_sqlite_backfill(
                store,
                rollout_id,
                segment.as_path(),
                &mut builder,
                index + 1 == lineage.len(),
            )
            .await?;
        }
        let projection = super::thread_history::projection_state(store, rollout_id)
            .await?
            .ok_or_else(|| ThreadStoreError::Internal {
                message: format!(
                    "segmented legacy history projection for {thread_id} has no checkpoint"
                ),
            })?;
        if projection.next_ordinal == 0 || projection.next_byte_offset != active_rollout_len {
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "segmented legacy history projection for {thread_id} does not include the complete active rollout"
                ),
            });
        }
        Ok(())
    }
    .await;
    if let Err(error) = result {
        if let Err(delete_error) = super::thread_history::delete_thread(store, rollout_id).await {
            warn!(
                "failed to discard an incomplete segmented legacy projection for {thread_id}: {delete_error}"
            );
        }
        return Err(error);
    }
    Ok(builder)
}

async fn segmented_legacy_rollout_paths(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &std::path::Path,
) -> ThreadStoreResult<Vec<PathBuf>> {
    let mut paths = Vec::new();
    let mut visited_paths = HashSet::new();
    let mut visited_segment_ids = HashSet::new();
    let mut current_path = rollout_path.to_path_buf();

    loop {
        current_path = codex_rollout::existing_rollout_path(current_path.as_path())
            .await
            .ok_or_else(|| ThreadStoreError::InvalidRequest {
                message: format!("segmented legacy history for {thread_id} has a missing rollout"),
            })?;
        if !visited_paths.insert(current_path.clone()) {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!("segmented legacy history for {thread_id} contains a cycle"),
            });
        }

        let session_meta = codex_rollout::read_session_meta_line(current_path.as_path())
            .await
            .map_err(thread_store_io_error)?;
        if session_meta.meta.id != thread_id
            || session_meta.meta.history_mode != ThreadHistoryMode::Legacy
            || session_meta.meta.history_base.is_some()
        {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!(
                    "segmented legacy history for {thread_id} contains another thread or history mode"
                ),
            });
        }
        if !visited_segment_ids.insert((session_meta.meta.id, session_meta.meta.segment_id)) {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!("segmented legacy history for {thread_id} contains a cycle"),
            });
        }

        let mut reader = codex_rollout::open_rollout_line_reader(current_path.as_path())
            .await
            .map_err(thread_store_io_error)?;
        let mut seen_session_meta = false;
        let mut seen_non_metadata_record = false;
        let mut leading_reference = None;
        let is_active_rollout = paths.is_empty();
        while let Some(line) = reader.next_line().await.map_err(thread_store_io_error)? {
            if line.trim().is_empty() {
                continue;
            }
            let line = match serde_json::from_str::<RolloutLine>(line.as_str()) {
                Ok(line) => line,
                Err(_error)
                    if is_active_rollout && seen_session_meta && seen_non_metadata_record =>
                {
                    continue;
                }
                Err(error) => {
                    return Err(ThreadStoreError::Internal {
                        message: format!(
                            "failed to decode segmented legacy rollout {}: {error}",
                            current_path.display()
                        ),
                    });
                }
            };
            if !seen_session_meta {
                if !matches!(line.item, RolloutItem::SessionMeta(_)) {
                    return Err(ThreadStoreError::InvalidRequest {
                        message: format!(
                            "segmented legacy rollout {} does not start with session metadata",
                            current_path.display()
                        ),
                    });
                }
                seen_session_meta = true;
                continue;
            }
            if let RolloutItem::RolloutReference(reference) = line.item {
                if seen_non_metadata_record {
                    return Err(ThreadStoreError::Unsupported {
                        operation: "non_leading_legacy_history_reference_projection",
                    });
                }
                leading_reference = Some(reference);
            }
            seen_non_metadata_record = true;
        }
        paths.push(current_path);

        let Some(reference) = leading_reference else {
            break;
        };
        if reference.thread_id != Some(thread_id)
            || reference.nth_user_message.is_some()
            || reference
                .compacted_replacement_history_filter_texts
                .as_ref()
                .is_some_and(|filters| !filters.is_empty())
        {
            return Err(ThreadStoreError::Unsupported {
                operation: "inherited_legacy_history_projection",
            });
        }
        current_path = codex_rollout::resolve_rollout_reference_path(
            store.config.codex_home.as_path(),
            &reference,
        )
        .await
        .map_err(thread_store_io_error)?;
    }

    paths.reverse();
    Ok(paths)
}

async fn stream_segmented_legacy_history_builder(
    lineage: &[PathBuf],
    canonical_session_meta: RolloutItem,
) -> ThreadStoreResult<ThreadHistoryBuilder> {
    let mut builder = ThreadHistoryBuilder::new();
    builder.handle_rollout_item_with_changes(&canonical_session_meta);
    for rollout_path in lineage {
        let file = tokio::fs::File::open(rollout_path)
            .await
            .map_err(thread_store_io_error)?;
        let read_limit = file.metadata().await.map_err(thread_store_io_error)?.len();
        let mut reader = BufReader::new(file.take(read_limit));
        let mut line_bytes = Vec::new();
        loop {
            line_bytes.clear();
            let bytes_read = reader
                .read_until(b'\n', &mut line_bytes)
                .await
                .map_err(thread_store_io_error)?;
            if bytes_read == 0 || !line_bytes.ends_with(b"\n") {
                break;
            }
            if line_bytes.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let line = serde_json::from_slice::<RolloutLine>(&line_bytes).map_err(|error| {
                ThreadStoreError::Internal {
                    message: format!(
                        "failed to decode segmented legacy rollout {}: {error}",
                        rollout_path.display()
                    ),
                }
            })?;
            if !matches!(
                line.item,
                RolloutItem::SessionMeta(_) | RolloutItem::RolloutReference(_)
            ) && codex_rollout::is_persisted_rollout_item(&line.item, ThreadHistoryMode::Legacy)
            {
                builder.handle_rollout_item_with_changes(&line.item);
            }
        }
    }
    Ok(builder)
}

#[tracing::instrument(
    level = "trace",
    skip_all,
    fields(item_count = params.items.len())
)]
pub(super) async fn append_items(
    store: &LocalThreadStore,
    params: AppendThreadItemsParams,
) -> ThreadStoreResult<()> {
    write_and_project(
        store,
        params.thread_id,
        RolloutWriteOp::AppendItems(params.items),
    )
    .await
}

pub(super) async fn persist_thread(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    write_and_project(store, thread_id, RolloutWriteOp::Persist).await
}

pub(super) async fn persist_thread_reserved(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    write_and_project_reserved(store, thread_id, RolloutWriteOp::Persist).await
}

pub(super) async fn flush_thread(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    write_and_project(store, thread_id, RolloutWriteOp::Flush).await
}

pub(super) async fn shutdown_thread(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    let mut pending_metadata = store.pending_thread_metadata.lock(thread_id).await;
    let _live_writer_guard = store.live_writer_locks.lock(thread_id).await;
    let (recorder, rollout_id, history_mode, persistence_mode) =
        live_writer_parts(store, thread_id).await?;
    if matches!(persistence_mode, ThreadPersistenceMode::Deferred) {
        store.live_recorders.lock().await.remove(&thread_id);
        return Ok(());
    }
    let rollout_path = recorder.rollout_path().to_path_buf();
    if matches!(history_mode, ThreadHistoryMode::Legacy) {
        recorder.shutdown().await.map_err(thread_store_io_error)?;
        if let Err(error) = project_segmented_legacy_rollout(store, thread_id, &recorder).await {
            warn!(
                "failed to project segmented legacy history during shutdown for {thread_id}: {error}"
            );
        }
    } else {
        recorder.shutdown().await.map_err(thread_store_io_error)?;
        if let Err(err) = super::thread_history_materialization::materialize_to_sqlite(
            store,
            rollout_id,
            rollout_path.as_path(),
        )
        .await
        {
            warn!("failed to project durable rollout during shutdown for {thread_id}: {err}");
        }
    }
    sync_materialized_rollout_path(store, thread_id, rollout_path.as_path()).await?;
    if let Some(metrics) = codex_otel::global()
        && let Ok(metadata) = tokio::fs::metadata(&rollout_path).await
    {
        let size_bytes = i64::try_from(metadata.len()).unwrap_or(i64::MAX);
        let _ = metrics.histogram(ROLLOUT_SIZE_BYTES_METRIC, size_bytes, &[]);
    }
    let rollout_exists = match tokio::fs::try_exists(&rollout_path).await {
        Ok(rollout_exists) => rollout_exists,
        Err(err) => {
            warn!(
                "failed to check rollout path after shutdown for {thread_id}; preserving pending metadata: {err}"
            );
            true
        }
    };
    store.live_recorders.lock().await.remove(&thread_id);
    drop(_live_writer_guard);
    if !rollout_exists && pending_metadata.take().is_some() {
        store.pending_thread_metadata.remove(thread_id).await;
    }
    Ok(())
}

pub(super) async fn discard_thread(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    let mut pending_metadata = store.pending_thread_metadata.lock(thread_id).await;
    let _live_writer_guard = store.live_writer_locks.lock(thread_id).await;
    if pending_metadata.take().is_some() {
        store.pending_thread_metadata.remove(thread_id).await;
    }
    store
        .live_recorders
        .lock()
        .await
        .remove(&thread_id)
        .map(|_| ())
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })
}

pub(super) async fn rollout_path(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<PathBuf> {
    let live_recorders = store.live_recorders.lock().await;
    let entry = live_recorders
        .get(&thread_id)
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
    Ok(entry
        .recovery
        .as_ref()
        .map(|recovery| recovery.rollout_path.clone())
        .unwrap_or_else(|| entry.recorder.rollout_path().to_path_buf()))
}

pub(super) async fn sync_materialized_rollout_path(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &std::path::Path,
) -> ThreadStoreResult<()> {
    if codex_rollout::existing_rollout_path(rollout_path)
        .await
        .is_none()
    {
        return Ok(());
    }
    let Some(state_db) = store.state_db().await else {
        return Ok(());
    };
    let result: ThreadStoreResult<()> = async {
        let Some(mut metadata) =
            state_db
                .get_thread(thread_id)
                .await
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!("failed to read thread metadata for {thread_id}: {err}"),
                })?
        else {
            return Ok(());
        };
        if metadata.rollout_path != rollout_path {
            metadata.rollout_path = rollout_path.to_path_buf();
            state_db
                .upsert_thread(&metadata)
                .await
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!("failed to update thread metadata for {thread_id}: {err}"),
                })?;
        }
        Ok(())
    }
    .await;
    result
}

fn thread_store_io_error(err: std::io::Error) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: err.to_string(),
    }
}

/// The rollout writer has three distinct lifecycle moments:
/// - `AppendItems` is normal turn/event persistence and adds new rollout records.
/// - `Persist` makes the thread durable before any turn items exist; locally this can write the
///   initial `SessionMeta`.
/// - `Flush` writes any rollout records already queued in the recorder and ensures they are
///   durably persisted.
///
/// Each can advance the rollout JSONL file on disk, so we need to make sure we materialize the
/// new data into the SQLite history tables (turns and items) as necessary.
enum RolloutWriteOp {
    AppendItems(Vec<RolloutItem>),
    Persist,
    Flush,
}

pub(super) async fn live_writer_parts(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<(
    RolloutRecorder,
    RolloutId,
    ThreadHistoryMode,
    ThreadPersistenceMode,
)> {
    let recovery = {
        let live_recorders = store.live_recorders.lock().await;
        let entry = live_recorders
            .get(&thread_id)
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        match entry.recovery.clone() {
            Some(recovery) => recovery,
            None => {
                return Ok((
                    entry.recorder.clone(),
                    entry.rollout_id,
                    entry.history_mode,
                    entry.persistence_mode,
                ));
            }
        }
    };

    let recorder = RolloutRecorder::new(
        &recovery.config,
        RolloutRecorderParams::resume(recovery.rollout_path),
    )
    .await
    .map_err(thread_store_io_error)?;
    let mut live_recorders = store.live_recorders.lock().await;
    let entry = live_recorders
        .get_mut(&thread_id)
        .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
    entry.recorder = recorder.clone();
    entry.recovery = None;
    Ok((
        recorder,
        entry.rollout_id,
        entry.history_mode,
        entry.persistence_mode,
    ))
}

async fn write_and_project(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    write_op: RolloutWriteOp,
) -> ThreadStoreResult<()> {
    // Every live write should have a recorder: create/resume installs one, while
    // shutdown/discard/delete removes it. Keep the lookup defensive so late writes fail after
    // teardown.
    let _live_writer_guard = store.live_writer_locks.lock(thread_id).await;
    write_and_project_reserved(store, thread_id, write_op).await
}

async fn write_and_project_reserved(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    write_op: RolloutWriteOp,
) -> ThreadStoreResult<()> {
    let (recorder, rollout_id, history_mode, persistence_mode) =
        live_writer_parts(store, thread_id).await?;
    let sync_rollout_path = matches!(&write_op, RolloutWriteOp::Persist | RolloutWriteOp::Flush);
    let write_op = match write_op {
        RolloutWriteOp::AppendItems(mut items) => {
            items.retain(|item| is_persisted_rollout_item(item, history_mode));
            if items.is_empty() {
                return Ok(());
            }
            RolloutWriteOp::AppendItems(items)
        }
        RolloutWriteOp::Persist => RolloutWriteOp::Persist,
        RolloutWriteOp::Flush => RolloutWriteOp::Flush,
    };
    let inherited_legacy_reference = matches!(history_mode, ThreadHistoryMode::Legacy)
        && matches!(&write_op, RolloutWriteOp::AppendItems(items) if items.iter().any(|item| {
            matches!(item, RolloutItem::RolloutReference(reference)
                if reference.thread_id != Some(thread_id)
                    || reference.nth_user_message.is_some()
                    || reference
                        .compacted_replacement_history_filter_texts
                        .as_ref()
                        .is_some_and(|filters| !filters.is_empty()))
        }));
    if matches!(persistence_mode, ThreadPersistenceMode::Deferred) {
        match write_op {
            RolloutWriteOp::AppendItems(items) => {
                recorder
                    .record_canonical_items(items.as_slice())
                    .await
                    .map_err(thread_store_io_error)?;
                if inherited_legacy_reference {
                    disable_inherited_legacy_history_projection(store, thread_id, rollout_id)
                        .await?;
                }
                return Ok(());
            }
            RolloutWriteOp::Flush => return Ok(()),
            RolloutWriteOp::Persist => {
                recorder.persist().await.map_err(thread_store_io_error)?;
                let mut live_recorders = store.live_recorders.lock().await;
                let entry = live_recorders
                    .get_mut(&thread_id)
                    .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
                entry.persistence_mode = ThreadPersistenceMode::Durable;
            }
        }
    } else {
        if matches!(history_mode, ThreadHistoryMode::Legacy)
            && matches!(&write_op, RolloutWriteOp::AppendItems(_))
        {
            restore_segmented_legacy_history_builder_if_needed(
                store,
                thread_id,
                recorder.rollout_path(),
            )
            .await?;
        }
        durable_write(&recorder, write_op).await?;
    }
    if inherited_legacy_reference {
        disable_inherited_legacy_history_projection(store, thread_id, rollout_id).await?;
    }
    if matches!(history_mode, ThreadHistoryMode::Legacy) {
        if let Err(error) = project_segmented_legacy_rollout(store, thread_id, &recorder).await {
            warn!("failed to project segmented legacy history for {thread_id}: {error}");
        }
    } else {
        let rollout_path = recorder.rollout_path();
        // SQLite is a rebuildable view. The flush barrier must win before projection starts so it
        // can lag JSONL after failure, but can never get ahead of canonical history.
        if let Err(err) = super::thread_history_materialization::materialize_to_sqlite(
            store,
            rollout_id,
            rollout_path,
        )
        .await
        {
            warn!("failed to project durable rollout for {thread_id}: {err}");
        }
    }
    if sync_rollout_path {
        sync_materialized_rollout_path(store, thread_id, recorder.rollout_path()).await?;
    }
    Ok(())
}

async fn disable_inherited_legacy_history_projection(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_id: RolloutId,
) -> ThreadStoreResult<()> {
    if let Some(entry) = store.live_recorders.lock().await.get_mut(&thread_id) {
        // Parent-owned history stays inherited after later same-thread segment rotations.
        entry.legacy_history_projection_enabled = false;
        entry.legacy_history_builder_needs_rebuild = false;
    }
    super::thread_history::delete_thread(store, rollout_id).await
}

pub(super) async fn project_segmented_legacy_rollout(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    recorder: &RolloutRecorder,
) -> ThreadStoreResult<()> {
    let (builder, rollout_id) = {
        let live_recorders = store.live_recorders.lock().await;
        let entry = live_recorders
            .get(&thread_id)
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        if !entry.legacy_history_projection_enabled || entry.legacy_history_builder_needs_rebuild {
            return Ok(());
        }
        (Arc::clone(&entry.legacy_history_builder), entry.rollout_id)
    };
    let rollout_path = recorder.rollout_path();
    let session_meta = codex_rollout::read_session_meta_line(rollout_path)
        .await
        .map_err(thread_store_io_error)?;
    if session_meta.meta.segment_id.is_none() {
        return Ok(());
    }

    let mut builder = builder.lock_owned().await;
    if let Err(error) = super::thread_history_materialization::materialize_legacy_to_sqlite(
        store,
        rollout_id,
        rollout_path,
        &mut builder,
    )
    .await
    {
        builder.reset();
        drop(builder);
        invalidate_segmented_legacy_projection(store, thread_id, rollout_id).await;
        return Err(error);
    }
    Ok(())
}

pub(super) async fn invalidate_segmented_legacy_projection(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_id: RolloutId,
) {
    if let Err(error) = super::thread_history::delete_thread(store, rollout_id).await {
        warn!(
            "failed to discard an incomplete segmented legacy projection for {thread_id}: {error}"
        );
    }
    if let Some(entry) = store.live_recorders.lock().await.get_mut(&thread_id) {
        entry.legacy_history_projection_enabled = false;
        entry.legacy_history_builder_needs_rebuild = true;
    }
}

async fn durable_write(recorder: &RolloutRecorder, write: RolloutWriteOp) -> ThreadStoreResult<()> {
    match write {
        RolloutWriteOp::AppendItems(items) => {
            recorder
                .record_canonical_items(items.as_slice())
                .await
                .map_err(thread_store_io_error)?;
            recorder.flush().await.map_err(thread_store_io_error)
        }
        RolloutWriteOp::Persist => recorder.persist().await.map_err(thread_store_io_error),
        RolloutWriteOp::Flush => recorder.flush().await.map_err(thread_store_io_error),
    }
}
