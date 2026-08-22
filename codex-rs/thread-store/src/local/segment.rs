#[cfg(test)]
use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(test)]
use std::sync::LazyLock;
#[cfg(test)]
use std::sync::Mutex as StdMutex;

use codex_protocol::RolloutId;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH;
use codex_rollout::RolloutConfig;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_rollout::RolloutRecorder;
use codex_rollout::RolloutRecorderParams;
use futures::FutureExt;
use serde_json::Value;
use sha2::Digest as _;
use sha2::Sha256;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
#[cfg(test)]
use tokio::sync::Notify;
use tracing::warn;

use super::LiveRecorderRecovery;
use super::LocalThreadStore;
use super::RolloutWriterReservation;
use crate::FreezeRolloutSegmentParams;
use crate::FrozenRolloutSegment;
use crate::SegmentCheckpointPersistenceOutcome;
use crate::ThreadPersistenceMode;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

pub(crate) mod confined_publication;
pub(super) mod history_repair_publication;

#[cfg(test)]
static SEGMENT_REOPEN_FAILURES: LazyLock<StdMutex<HashSet<ThreadId>>> =
    LazyLock::new(|| StdMutex::new(HashSet::new()));
#[cfg(test)]
static SEGMENT_DURABILITY_FAILURES: LazyLock<StdMutex<HashSet<ThreadId>>> =
    LazyLock::new(|| StdMutex::new(HashSet::new()));
#[cfg(test)]
static SEGMENT_PRECOMMIT_FAILURES: LazyLock<StdMutex<HashSet<ThreadId>>> =
    LazyLock::new(|| StdMutex::new(HashSet::new()));
#[cfg(test)]
static CHECKPOINT_PERSISTENCE_PAUSES: LazyLock<
    StdMutex<HashMap<ThreadId, Arc<CheckpointPersistencePause>>>,
> = LazyLock::new(|| StdMutex::new(HashMap::new()));

#[cfg(test)]
pub(super) const SEGMENT_ROTATION_CRASH_BOUNDARY_ENV: &str =
    "FRODEX_SEGMENT_ROTATION_CRASH_BOUNDARY";
#[cfg(test)]
pub(super) const SEGMENT_ROTATION_CRASH_THREAD_ENV: &str = "FRODEX_SEGMENT_ROTATION_CRASH_THREAD";
#[cfg(test)]
pub(super) const SEGMENT_ROTATION_CRASH_EXIT_CODE: i32 = 87;

#[cfg(test)]
pub(super) struct CheckpointPersistencePause {
    pub(super) entered: Notify,
    pub(super) release: Notify,
}

#[cfg(test)]
pub(super) fn inject_next_segment_reopen_failure(thread_id: ThreadId) {
    SEGMENT_REOPEN_FAILURES
        .lock()
        .expect("segment reopen failure mutex")
        .insert(thread_id);
}

#[cfg(test)]
pub(super) fn inject_next_segment_durability_failure(thread_id: ThreadId) {
    SEGMENT_DURABILITY_FAILURES
        .lock()
        .expect("segment durability failure mutex")
        .insert(thread_id);
}

#[cfg(test)]
pub(super) fn inject_next_segment_precommit_failure(thread_id: ThreadId) {
    SEGMENT_PRECOMMIT_FAILURES
        .lock()
        .expect("segment precommit failure mutex")
        .insert(thread_id);
}

#[cfg(test)]
pub(super) fn inject_checkpoint_persistence_pause(
    thread_id: ThreadId,
) -> Arc<CheckpointPersistencePause> {
    let pause = Arc::new(CheckpointPersistencePause {
        entered: Notify::new(),
        release: Notify::new(),
    });
    CHECKPOINT_PERSISTENCE_PAUSES
        .lock()
        .expect("checkpoint persistence pause mutex")
        .insert(thread_id, Arc::clone(&pause));
    pause
}

#[cfg(test)]
fn take_segment_reopen_failure(thread_id: ThreadId) -> bool {
    SEGMENT_REOPEN_FAILURES
        .lock()
        .expect("segment reopen failure mutex")
        .remove(&thread_id)
}

#[cfg(test)]
fn take_segment_durability_failure(thread_id: ThreadId) -> bool {
    SEGMENT_DURABILITY_FAILURES
        .lock()
        .expect("segment durability failure mutex")
        .remove(&thread_id)
}

#[cfg(test)]
fn take_segment_precommit_failure(thread_id: ThreadId) -> bool {
    SEGMENT_PRECOMMIT_FAILURES
        .lock()
        .expect("segment precommit failure mutex")
        .remove(&thread_id)
}

#[cfg(test)]
fn crash_segment_rotation_at(thread_id: ThreadId, boundary: &str) {
    let Some(configured_thread_id) = std::env::var_os(SEGMENT_ROTATION_CRASH_THREAD_ENV) else {
        return;
    };
    if configured_thread_id.to_string_lossy() != thread_id.to_string() {
        return;
    }
    if std::env::var_os(SEGMENT_ROTATION_CRASH_BOUNDARY_ENV).as_deref()
        != Some(std::ffi::OsStr::new(boundary))
    {
        return;
    }

    // The parent test verifies the files left by a process that ran no Rust destructors.
    std::process::exit(SEGMENT_ROTATION_CRASH_EXIT_CODE);
}

#[cfg(not(test))]
fn crash_segment_rotation_at(_thread_id: ThreadId, _boundary: &str) {}

/// Whether a segment freeze replaced the mutable active rollout.
enum FrozenSegmentPublication {
    ActiveRolloutUnchanged,
    ActiveRolloutReplaced(StableRolloutPublication),
}

/// Internal freeze result retaining the active-rollout commit classification.
struct FrozenRolloutSegmentResult {
    frozen: FrozenRolloutSegment,
    publication: FrozenSegmentPublication,
}

/// Durability acknowledgement for a visible active-rollout replacement.
enum StableRolloutPublication {
    Durable,
    DurabilityUnknown { error: ThreadStoreError },
}

/// Publication result for the atomic checkpoint-append fallback.
enum AtomicCheckpointAppend {
    Committed,
    Indeterminate { error: ThreadStoreError },
}

pub(super) async fn freeze_thread_segment(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    params: FreezeRolloutSegmentParams,
) -> ThreadStoreResult<FrozenRolloutSegment> {
    freeze_thread_segment_for_rollout(store, thread_id, params, /*expected_rollout_id*/ None).await
}

pub(super) async fn freeze_thread_segment_for_rollout(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    params: FreezeRolloutSegmentParams,
    expected_rollout_id: Option<RolloutId>,
) -> ThreadStoreResult<FrozenRolloutSegment> {
    let store_for_freeze = store.clone();
    let freeze = tokio::spawn(async move {
        let reservation = reserve_segment_writers(&store_for_freeze, thread_id).await?;
        freeze_thread_segment_reserved_with_publication(
            &store_for_freeze,
            thread_id,
            params,
            expected_rollout_id,
            &reservation,
        )
        .await
    });
    let result = match freeze.await {
        Ok(result) => result?,
        Err(error) => {
            store.live_recorders.lock().await.remove(&thread_id);
            return Err(ThreadStoreError::Conflict {
                message: format!(
                    "segment freeze task for thread {thread_id} failed at an indeterminate commit point: {error}"
                ),
            });
        }
    };
    match result.publication {
        FrozenSegmentPublication::ActiveRolloutUnchanged
        | FrozenSegmentPublication::ActiveRolloutReplaced(StableRolloutPublication::Durable) => {
            Ok(result.frozen)
        }
        FrozenSegmentPublication::ActiveRolloutReplaced(
            StableRolloutPublication::DurabilityUnknown { error },
        ) => {
            store.live_recorders.lock().await.remove(&thread_id);
            Err(ThreadStoreError::Conflict {
                message: format!(
                    "segment rotation for thread {thread_id} committed without a durability acknowledgement; restart before continuing: {error}"
                ),
            })
        }
    }
}

/// Reserves every mutable rollout owner reachable from the source before freezing any bytes.
///
/// Discovery is repeated after newly found owners are reserved. The final pass therefore reads
/// every mutable reference while its stable thread ID has both in-process and cross-process writer
/// ownership. Content-addressed immutable files are scanned for nested references but do not need
/// a writer reservation themselves.
async fn reserve_segment_writers(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<RolloutWriterReservation> {
    let mut thread_ids = vec![thread_id];
    loop {
        let reservation = store.reserve_rollout_writers(thread_ids.as_slice()).await?;
        let live_source = {
            let live_recorders = store.live_recorders.lock().await;
            live_recorders.get(&thread_id).map(|entry| {
                (
                    entry
                        .recovery
                        .as_ref()
                        .map(|recovery| recovery.rollout_path.clone())
                        .unwrap_or_else(|| entry.recorder.rollout_path().to_path_buf()),
                    entry.recorder.clone(),
                    entry.persistence_mode == ThreadPersistenceMode::Deferred,
                )
            })
        };
        let (source_path, live_recorder, allow_missing_source) = match live_source {
            Some((source_path, recorder, allow_missing_source)) => {
                // Durable creation still defers the first file write. Materialize that header
                // before discovering references; deferred threads remain buffered until freeze.
                if !allow_missing_source {
                    recorder.persist().await.map_err(thread_store_io_error)?;
                }
                (source_path, Some(recorder), allow_missing_source)
            }
            None => {
                let source_path =
                    super::thread_rollout_resolver::resolve_current_including_archived(
                        store, thread_id,
                    )
                    .await?
                    .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?
                    .path;
                (source_path, None, false)
            }
        };
        let buffered_items = match live_recorder {
            Some(recorder) => recorder
                .buffered_canonical_items()
                .await
                .map_err(thread_store_io_error)?,
            None => Vec::new(),
        };
        let mut discovered = discover_mutable_reference_owners(
            store,
            source_path.as_path(),
            allow_missing_source,
            buffered_items.as_slice(),
            &reservation,
        )
        .await?;
        discovered.push(thread_id);
        discovered.sort_unstable_by_key(ThreadId::to_string);
        discovered.dedup();
        if discovered
            .iter()
            .all(|thread_id| reservation.contains(*thread_id))
        {
            return Ok(reservation);
        }
        thread_ids = discovered;
    }
}

async fn discover_mutable_reference_owners(
    store: &LocalThreadStore,
    source_path: &Path,
    allow_missing_source: bool,
    buffered_items: &[RolloutItem],
    reservation: &RolloutWriterReservation,
) -> ThreadStoreResult<Vec<ThreadId>> {
    let canonical_home = fs::canonicalize(store.config.codex_home.as_path())
        .await
        .ok();
    let mut owners = Vec::new();
    let mut pending = vec![source_path.to_path_buf()];
    let mut pending_references = buffered_items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::RolloutReference(reference) => Some(reference.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut visited = HashSet::new();
    loop {
        if let Some(path) = pending.pop() {
            let Some(path) = codex_rollout::existing_rollout_path(path.as_path()).await else {
                if allow_missing_source && path == source_path {
                    continue;
                }
                return Err(ThreadStoreError::Internal {
                    message: format!("referenced rollout {} does not exist", path.display()),
                });
            };
            if !visited.insert(path.clone()) {
                continue;
            }
            let (lines, _thread_id, _parse_errors) =
                RolloutRecorder::load_rollout_lines(path.as_path())
                    .await
                    .map_err(thread_store_io_error)?;
            pending_references.extend(lines.into_iter().filter_map(|line| match line.item {
                RolloutItem::RolloutReference(reference) => Some(reference),
                _ => None,
            }));
            continue;
        }
        let Some(reference) = pending_references.pop() else {
            break;
        };
        let referenced_thread_id =
            reference
                .thread_id
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: format!(
                        "rollout reference {} is missing thread_id",
                        reference.rollout_path.display()
                    ),
                })?;
        let recorded_immutable = reference_has_valid_recorded_immutable_candidate(
            store,
            canonical_home.as_deref(),
            &reference,
            referenced_thread_id,
        )
        .await;
        if !recorded_immutable && !reservation.contains(referenced_thread_id) {
            owners.push(referenced_thread_id);
            continue;
        }
        let resolved = codex_rollout::resolve_rollout_reference_path(
            store.config.codex_home.as_path(),
            &reference,
        )
        .await
        .map_err(thread_store_io_error)?;
        let immutable = is_immutable_segment_path(
            store.config.codex_home.as_path(),
            canonical_home.as_deref(),
            resolved.as_path(),
            referenced_thread_id,
            reference.segment_id,
        );
        if !immutable {
            owners.push(referenced_thread_id);
            // The next discovery pass owns this predecessor before reading it and revalidates the
            // complete graph under the expanded reservation.
            if !reservation.contains(referenced_thread_id) {
                continue;
            }
        }
        pending.push(resolved);
    }
    Ok(owners)
}

async fn reference_has_valid_recorded_immutable_candidate(
    store: &LocalThreadStore,
    canonical_home: Option<&Path>,
    reference: &RolloutReferenceItem,
    thread_id: ThreadId,
) -> bool {
    let Some(canonical_home) = canonical_home else {
        return false;
    };
    let Some(candidate) =
        codex_rollout::existing_rollout_path(reference.rollout_path.as_path()).await
    else {
        return false;
    };
    if !is_immutable_segment_path(
        store.config.codex_home.as_path(),
        Some(canonical_home),
        candidate.as_path(),
        thread_id,
        reference.segment_id,
    ) {
        return false;
    }
    let Ok(canonical_candidate) = fs::canonicalize(candidate.as_path()).await else {
        return false;
    };
    let expected_directory = canonical_home
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(
            reference
                .segment_id
                .map(|segment_id| segment_id.to_string())
                .unwrap_or_else(|| "initial".to_string()),
        );
    if !canonical_candidate.starts_with(expected_directory) {
        return false;
    }
    let Ok(metadata) = fs::symlink_metadata(candidate.as_path()).await else {
        return false;
    };
    if !metadata.file_type().is_file() {
        return false;
    }
    let Ok(session_meta) = codex_rollout::read_session_meta_line(candidate.as_path()).await else {
        return false;
    };
    session_meta.meta.id == thread_id
        && session_meta.meta.segment_id == reference.segment_id
        && reference.rollout_id.is_none_or(|rollout_id| {
            codex_rollout::rollout_id_from_path(
                codex_rollout::plain_rollout_path(candidate.as_path()).as_path(),
            ) == Some(rollout_id)
        })
}

pub(super) async fn freeze_thread_segment_reserved(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    params: FreezeRolloutSegmentParams,
    expected_rollout_id: Option<RolloutId>,
    reservation: &RolloutWriterReservation,
) -> ThreadStoreResult<FrozenRolloutSegment> {
    let result = freeze_thread_segment_reserved_with_publication(
        store,
        thread_id,
        params,
        expected_rollout_id,
        reservation,
    )
    .await?;
    match result.publication {
        FrozenSegmentPublication::ActiveRolloutUnchanged
        | FrozenSegmentPublication::ActiveRolloutReplaced(StableRolloutPublication::Durable) => {
            Ok(result.frozen)
        }
        FrozenSegmentPublication::ActiveRolloutReplaced(
            StableRolloutPublication::DurabilityUnknown { error },
        ) => Err(ThreadStoreError::Conflict {
            message: format!(
                "segment rotation for thread {thread_id} committed without a durability acknowledgement; restart before continuing: {error}"
            ),
        }),
    }
}

/// Persists a rotation checkpoint without treating a post-publication failure as unpublished.
///
/// The spawned owner keeps the commit classification alive if its caller is cancelled. Any panic
/// is indeterminate because it may have happened immediately after the atomic replacement.
pub(super) async fn persist_segment_checkpoint(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    params: FreezeRolloutSegmentParams,
) -> SegmentCheckpointPersistenceOutcome {
    if params.is_snapshot() {
        return SegmentCheckpointPersistenceOutcome::NotCommitted {
            error: ThreadStoreError::InvalidRequest {
                message: "a segment-state checkpoint must replace the active rollout".to_string(),
            },
        };
    }
    if let Err(error) = params.validate_checkpoint() {
        return SegmentCheckpointPersistenceOutcome::NotCommitted {
            error: ThreadStoreError::InvalidRequest {
                message: error.to_string(),
            },
        };
    }
    let store_for_persistence = store.clone();
    let result = tokio::spawn(async move {
        let reservation = match reserve_segment_writers(&store_for_persistence, thread_id).await {
            Ok(reservation) => reservation,
            Err(error) => {
                return SegmentCheckpointPersistenceOutcome::NotCommitted { error };
            }
        };
        #[cfg(test)]
        let pause = {
            CHECKPOINT_PERSISTENCE_PAUSES
                .lock()
                .expect("checkpoint persistence pause mutex")
                .remove(&thread_id)
        };
        #[cfg(test)]
        if let Some(pause) = pause {
            pause.entered.notify_one();
            pause.release.notified().await;
        }
        let result = AssertUnwindSafe(freeze_thread_segment_reserved_with_publication(
            &store_for_persistence,
            thread_id,
            params.clone(),
            /*expected_rollout_id*/ None,
            &reservation,
        ))
        .catch_unwind()
        .await;
        match result {
            Ok(Ok(result)) => match result.publication {
                FrozenSegmentPublication::ActiveRolloutReplaced(
                    StableRolloutPublication::Durable,
                ) => SegmentCheckpointPersistenceOutcome::Committed,
                FrozenSegmentPublication::ActiveRolloutReplaced(
                    StableRolloutPublication::DurabilityUnknown { error },
                ) => SegmentCheckpointPersistenceOutcome::Indeterminate { error },
                FrozenSegmentPublication::ActiveRolloutUnchanged => {
                    SegmentCheckpointPersistenceOutcome::NotCommitted {
                        error: ThreadStoreError::Internal {
                            message: "checkpoint rotation left the active rollout unchanged"
                                .to_string(),
                        },
                    }
                }
            },
            Ok(Err(rotation_error)) => {
                warn!(%thread_id, %rotation_error, "segment rotation failed before commit; atomically appending the checkpoint");
                match append_checkpoint_atomically_reserved(
                    &store_for_persistence,
                    thread_id,
                    &params,
                )
                .await
                {
                    Ok(AtomicCheckpointAppend::Committed) => {
                        SegmentCheckpointPersistenceOutcome::Committed
                    }
                    Ok(AtomicCheckpointAppend::Indeterminate { error }) => {
                        SegmentCheckpointPersistenceOutcome::Indeterminate {
                            error: ThreadStoreError::Internal {
                                message: format!(
                                    "segment rotation failed: {rotation_error}; atomic checkpoint append committed but durability is indeterminate: {error}"
                                ),
                            },
                        }
                    }
                    Err(append_error) => SegmentCheckpointPersistenceOutcome::NotCommitted {
                        error: ThreadStoreError::Internal {
                            message: format!(
                                "segment rotation failed: {rotation_error}; atomic checkpoint append failed without changing the active rollout: {append_error}"
                            ),
                        },
                    },
                }
            }
            Err(_) => SegmentCheckpointPersistenceOutcome::Indeterminate {
                error: ThreadStoreError::Internal {
                    message: format!(
                        "checkpoint persistence for thread {thread_id} panicked at an indeterminate commit point"
                    ),
                },
            },
        }
    })
    .await;

    match result {
        Ok(outcome) => outcome,
        Err(error) => SegmentCheckpointPersistenceOutcome::Indeterminate {
            error: ThreadStoreError::Internal {
                message: format!(
                    "checkpoint persistence task for thread {thread_id} failed at an indeterminate commit point: {error}"
                ),
            },
        },
    }
}

async fn append_checkpoint_atomically_reserved(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    params: &FreezeRolloutSegmentParams,
) -> ThreadStoreResult<AtomicCheckpointAppend> {
    let (recorder, rollout_id, history_mode, _persistence_mode) =
        super::live_writer::live_writer_parts(store, thread_id).await?;
    recorder.persist().await.map_err(thread_store_io_error)?;
    {
        let mut live_recorders = store.live_recorders.lock().await;
        let entry = live_recorders
            .get_mut(&thread_id)
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        entry.persistence_mode = ThreadPersistenceMode::Durable;
    }
    recorder.flush().await.map_err(thread_store_io_error)?;

    let stable_path = codex_rollout::plain_rollout_path(recorder.rollout_path());
    let source_path = codex_rollout::existing_rollout_path(stable_path.as_path())
        .await
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!("thread {thread_id} does not have a readable active rollout"),
        })?;
    if source_path != stable_path {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "atomic checkpoint append requires a materialized active rollout for thread {thread_id}"
            ),
        });
    }

    cleanup_stale_staged_rollouts(stable_path.as_path()).await?;
    let staged_path = staged_rollout_path(stable_path.as_path());
    if let Err(error) = copy_active_rollout(source_path.as_path(), staged_path.as_path()).await {
        let _ = fs::remove_file(staged_path.as_path()).await;
        return Err(error);
    }
    let staged_meta = match codex_rollout::read_session_meta_line(staged_path.as_path()).await {
        Ok(meta) => meta,
        Err(error) => {
            let _ = fs::remove_file(staged_path.as_path()).await;
            return Err(thread_store_io_error(error));
        }
    };
    let config = rollout_config(store, &staged_meta.meta);
    let staged_recorder =
        match RolloutRecorder::new(&config, RolloutRecorderParams::resume(staged_path.clone()))
            .await
        {
            Ok(recorder) => recorder,
            Err(error) => {
                let _ = fs::remove_file(staged_path.as_path()).await;
                return Err(thread_store_io_error(error));
            }
        };
    let items = codex_rollout::persisted_rollout_items(params.initial_items(), history_mode);
    let stage_result = async {
        staged_recorder
            .record_canonical_items(items.as_slice())
            .await
            .map_err(thread_store_io_error)?;
        staged_recorder
            .flush()
            .await
            .map_err(thread_store_io_error)?;
        staged_recorder
            .shutdown()
            .await
            .map_err(thread_store_io_error)
    }
    .await;
    if let Err(error) = stage_result {
        let _ = staged_recorder.shutdown().await;
        let _ = fs::remove_file(staged_path.as_path()).await;
        return Err(error);
    }

    {
        let mut live_recorders = store.live_recorders.lock().await;
        let entry = live_recorders
            .get_mut(&thread_id)
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        entry.recovery = Some(LiveRecorderRecovery {
            config: config.clone(),
            rollout_path: stable_path.clone(),
        });
    }
    if let Err(error) = recorder.shutdown().await {
        let _ = fs::remove_file(staged_path.as_path()).await;
        return Err(thread_store_io_error(error));
    }
    let publication = match replace_stable_rollout(staged_path.clone(), stable_path.clone()).await {
        Ok(publication) => publication,
        Err(error) => {
            let _ = fs::remove_file(staged_path.as_path()).await;
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "failed to atomically replace rollout {} with staged checkpoint {}: {error}",
                    stable_path.display(),
                    staged_path.display()
                ),
            });
        }
    };
    if let StableRolloutPublication::DurabilityUnknown { error } = publication {
        return Ok(AtomicCheckpointAppend::Indeterminate { error });
    }

    if let Some(entry) = store.live_recorders.lock().await.get_mut(&thread_id) {
        entry.history_mode = history_mode;
        entry.persistence_mode = ThreadPersistenceMode::Durable;
    }
    if let Err(error) = super::live_writer::live_writer_parts(store, thread_id).await {
        warn!(%thread_id, %error, "checkpoint append committed; live writer will reopen on its next operation");
    }
    if let Err(error) =
        super::live_writer::sync_materialized_rollout_path(store, thread_id, stable_path.as_path())
            .await
    {
        warn!(%thread_id, %error, "checkpoint append committed but rollout-path synchronization failed");
    }
    match history_mode {
        ThreadHistoryMode::Paginated => {
            if let Err(error) = super::thread_history_materialization::materialize_to_sqlite(
                store,
                rollout_id,
                stable_path.as_path(),
            )
            .await
            {
                warn!(%thread_id, %error, "checkpoint append committed but paginated projection repair failed");
            }
        }
        ThreadHistoryMode::Legacy => {
            if let Ok((reopened, _, _, _)) =
                super::live_writer::live_writer_parts(store, thread_id).await
                && let Err(error) = super::live_writer::project_segmented_legacy_rollout(
                    store, thread_id, &reopened,
                )
                .await
            {
                warn!(%thread_id, %error, "checkpoint append committed but legacy projection repair failed");
            }
        }
    }
    Ok(AtomicCheckpointAppend::Committed)
}

async fn copy_active_rollout(source_path: &Path, staged_path: &Path) -> ThreadStoreResult<()> {
    let mut source = fs::File::open(source_path)
        .await
        .map_err(thread_store_io_error)?;
    let mut staged = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(staged_path)
        .await
        .map_err(thread_store_io_error)?;
    tokio::io::copy(&mut source, &mut staged)
        .await
        .map_err(thread_store_io_error)?;
    staged.sync_all().await.map_err(thread_store_io_error)
}

async fn freeze_thread_segment_reserved_with_publication(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    params: FreezeRolloutSegmentParams,
    expected_rollout_id: Option<RolloutId>,
    reservation: &RolloutWriterReservation,
) -> ThreadStoreResult<FrozenRolloutSegmentResult> {
    debug_assert!(reservation.contains(thread_id));
    let has_live_entry = store.live_recorders.lock().await.contains_key(&thread_id);
    let live_entry = if has_live_entry {
        let (recorder, rollout_id, history_mode, _persistence_mode) =
            super::live_writer::live_writer_parts(store, thread_id).await?;
        Some((recorder, rollout_id, history_mode))
    } else {
        None
    };
    if let Some((recorder, _rollout_id, _history_mode)) = live_entry.as_ref() {
        recorder.persist().await.map_err(thread_store_io_error)?;
        crash_segment_rotation_at(thread_id, "source_persisted_before_flush");
        {
            let mut live_recorders = store.live_recorders.lock().await;
            let entry = live_recorders
                .get_mut(&thread_id)
                .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
            entry.persistence_mode = ThreadPersistenceMode::Durable;
        }
        recorder.flush().await.map_err(thread_store_io_error)?;
        crash_segment_rotation_at(thread_id, "source_flushed_before_seal");
    }
    #[cfg(test)]
    if take_segment_precommit_failure(thread_id) {
        return Err(ThreadStoreError::Internal {
            message: "injected segment precommit failure".to_string(),
        });
    }

    let (source_rollout_id, recorded_path) = match live_entry.as_ref() {
        Some((recorder, rollout_id, _history_mode)) => {
            (*rollout_id, recorder.rollout_path().to_path_buf())
        }
        None => {
            let resolved = super::thread_rollout_resolver::resolve_current_including_archived(
                store, thread_id,
            )
            .await?
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
            (resolved.rollout_id, resolved.path)
        }
    };
    if expected_rollout_id.is_some_and(|expected| expected != source_rollout_id) {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "rollout path does not select the current rollout for thread {thread_id}"
            ),
        });
    }
    let source_path = codex_rollout::existing_rollout_path(recorded_path.as_path())
        .await
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!("thread {thread_id} does not have a readable rollout"),
        })?;
    let stable_path = codex_rollout::plain_rollout_path(recorded_path.as_path());
    let (source_meta, next_rollout_ordinal, existing_reference, source_lines, skipped_records) =
        validate_source_rollout(source_path.as_path(), thread_id).await?;
    let history_mode = source_meta.meta.history_mode;
    if let Some((_recorder, _rollout_id, live_history_mode)) = live_entry.as_ref()
        && *live_history_mode != history_mode
    {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "live writer history mode does not match rollout metadata for thread {thread_id}"
            ),
        });
    }
    if params.is_snapshot()
        && let Some(reference) = existing_reference
    {
        let reference = stabilize_rollout_reference(
            store,
            reference,
            &mut HashSet::new(),
            /*depth*/ 0,
            reservation,
        )
        .await?;
        return Ok(FrozenRolloutSegmentResult {
            frozen: FrozenRolloutSegment {
                reference,
                history_base: None,
                source_session_meta: source_meta,
                history_mode,
                next_rollout_ordinal,
            },
            publication: FrozenSegmentPublication::ActiveRolloutUnchanged,
        });
    }

    if params.is_snapshot() {
        let segment_id = if matches!(history_mode, ThreadHistoryMode::Legacy) {
            Some(snapshot_segment_id(source_lines.as_slice())?)
        } else {
            None
        };
        let snapshot_rollout_id = if matches!(history_mode, ThreadHistoryMode::Paginated) {
            ThreadId::new()
        } else {
            source_rollout_id
        };
        let immutable_path = if matches!(history_mode, ThreadHistoryMode::Paginated) {
            native_history_segment_path(
                store.config.codex_home.as_path(),
                codex_rollout::plain_rollout_path(source_path.as_path()).as_path(),
                Some(snapshot_rollout_id),
            )?
        } else {
            immutable_segment_path(
                store.config.codex_home.as_path(),
                thread_id,
                segment_id,
                codex_rollout::plain_rollout_path(source_path.as_path()).as_path(),
            )?
        };
        install_snapshot_segment(
            source_lines.as_slice(),
            immutable_path.as_path(),
            segment_id,
        )
        .await?;
        return Ok(FrozenRolloutSegmentResult {
            frozen: FrozenRolloutSegment {
                reference: RolloutReferenceItem {
                    rollout_id: Some(snapshot_rollout_id),
                    rollout_path: immutable_path.clone(),
                    thread_id: Some(thread_id),
                    rollout_timestamp: rollout_timestamp_from_path(stable_path.as_path()),
                    segment_id,
                    max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
                    nth_user_message: None,
                    compacted_replacement_history_filter_texts: None,
                },
                history_base: if matches!(history_mode, ThreadHistoryMode::Paginated) {
                    Some(HistoryPosition {
                        thread_id: snapshot_rollout_id,
                        end_ordinal_exclusive: next_rollout_ordinal.ok_or_else(|| {
                            ThreadStoreError::Internal {
                                message: "paginated rollout snapshot has no terminal ordinal"
                                    .to_string(),
                            }
                        })?,
                        end_byte_offset: fs::metadata(immutable_path.as_path())
                            .await
                            .map_err(thread_store_io_error)?
                            .len(),
                    })
                } else {
                    None
                },
                source_session_meta: source_meta,
                history_mode,
                next_rollout_ordinal,
            },
            publication: FrozenSegmentPublication::ActiveRolloutUnchanged,
        });
    }

    if matches!(history_mode, ThreadHistoryMode::Legacy) && live_entry.is_some() {
        super::live_writer::restore_segmented_legacy_history_builder_if_needed(
            store,
            thread_id,
            source_path.as_path(),
        )
        .await?;
    }

    let legacy_projection_builder =
        if matches!(history_mode, ThreadHistoryMode::Legacy) && live_entry.is_some() {
            let builder = store
                .live_recorders
                .lock()
                .await
                .get(&thread_id)
                .filter(|entry| entry.legacy_history_projection_enabled)
                .map(|entry| Arc::clone(&entry.legacy_history_builder));
            if let Some(builder) = builder {
                let mut builder_guard = Arc::clone(&builder).lock_owned().await;
                let result = super::thread_history_materialization::materialize_legacy_to_sqlite(
                    store,
                    source_rollout_id,
                    source_path.as_path(),
                    &mut builder_guard,
                )
                .await;
                if let Err(error) = result {
                    builder_guard.reset();
                    drop(builder_guard);
                    super::live_writer::invalidate_segmented_legacy_projection(
                        store,
                        thread_id,
                        source_rollout_id,
                    )
                    .await;
                    return Err(error);
                }
                drop(builder_guard);
                Some(builder)
            } else {
                None
            }
        } else {
            if matches!(history_mode, ThreadHistoryMode::Paginated)
                && live_entry.is_some()
                && !skipped_records
            {
                super::thread_history_materialization::materialize_to_sqlite(
                    store,
                    source_rollout_id,
                    source_path.as_path(),
                )
                .await?;
            }
            None
        };
    let legacy_next_projection_ordinal = if legacy_projection_builder.is_some() {
        Some(
            super::thread_history::projection_state(store, source_rollout_id)
                .await?
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: format!(
                        "segmented legacy history projection for {thread_id} has no checkpoint"
                    ),
                })?
                .next_ordinal,
        )
    } else {
        None
    };

    let segment_id = source_meta.meta.segment_id;
    let native_segment_rollout_id =
        matches!(history_mode, ThreadHistoryMode::Paginated).then(ThreadId::new);
    let immutable_path = if matches!(history_mode, ThreadHistoryMode::Paginated) {
        native_history_segment_path(
            store.config.codex_home.as_path(),
            source_path.as_path(),
            native_segment_rollout_id,
        )?
    } else {
        immutable_segment_path(
            store.config.codex_home.as_path(),
            thread_id,
            segment_id,
            source_path.as_path(),
        )?
    };
    if skipped_records {
        install_snapshot_segment(
            source_lines.as_slice(),
            immutable_path.as_path(),
            segment_id,
        )
        .await?;
    } else {
        install_immutable_segment(source_path.as_path(), immutable_path.as_path()).await?;
    }
    crash_segment_rotation_at(thread_id, "immutable_sealed_before_reference");

    let reference = RolloutReferenceItem {
        rollout_id: native_segment_rollout_id.or(Some(source_rollout_id)),
        rollout_path: immutable_path.clone(),
        thread_id: Some(thread_id),
        rollout_timestamp: rollout_timestamp_from_path(stable_path.as_path()),
        segment_id,
        max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };
    let config = rollout_config(store, &source_meta.meta);
    let initial_rollout_ordinal = next_rollout_ordinal.unwrap_or(0);
    let native_history_base = if let Some(native_segment_rollout_id) = native_segment_rollout_id {
        Some(HistoryPosition {
            thread_id: native_segment_rollout_id,
            end_ordinal_exclusive: initial_rollout_ordinal,
            end_byte_offset: fs::metadata(immutable_path.as_path())
                .await
                .map_err(thread_store_io_error)?
                .len(),
        })
    } else {
        None
    };
    if matches!(history_mode, ThreadHistoryMode::Paginated) {
        let history_base = native_history_base.ok_or_else(|| ThreadStoreError::Internal {
            message: "paginated segment rotation did not derive history_base".to_string(),
        })?;
        let active_path = stable_path.clone();
        cleanup_stale_staged_rollouts(active_path.as_path()).await?;
        let staged_path = staged_rollout_path(active_path.as_path());
        let staged_recorder = create_paginated_continuation_recorder(
            &config,
            &source_meta.meta,
            staged_path.clone(),
            history_base,
            initial_rollout_ordinal,
        )
        .await
        .map_err(|error| ThreadStoreError::Internal {
            message: format!(
                "failed to create paginated continuation {}: {error}",
                staged_path.display()
            ),
        })?;
        if let Err(err) = staged_recorder
            .record_canonical_items(params.initial_items())
            .await
        {
            let _ = staged_recorder.shutdown().await;
            let _ = fs::remove_file(staged_path.as_path()).await;
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "failed to record paginated continuation {}: {err}",
                    staged_path.display()
                ),
            });
        }
        crash_segment_rotation_at(thread_id, "reference_recorded_before_checkpoint");
        if let Err(err) = staged_recorder.persist().await {
            let _ = staged_recorder.shutdown().await;
            let _ = fs::remove_file(staged_path.as_path()).await;
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "failed to materialize paginated continuation {}: {err}",
                    staged_path.display()
                ),
            });
        }
        if let Err(err) = staged_recorder.flush().await {
            let _ = staged_recorder.shutdown().await;
            let _ = fs::remove_file(staged_path.as_path()).await;
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "failed to flush paginated continuation {}: {err}",
                    staged_path.display()
                ),
            });
        }
        crash_segment_rotation_at(thread_id, "checkpoint_recorded_before_flush");
        staged_recorder
            .shutdown()
            .await
            .map_err(|error| ThreadStoreError::Internal {
                message: format!(
                    "failed to close paginated continuation {}: {error}",
                    staged_path.display()
                ),
            })?;
        crash_segment_rotation_at(thread_id, "staged_rollout_durable_before_publication");

        fs::metadata(staged_path.as_path())
            .await
            .map_err(|error| ThreadStoreError::Internal {
                message: format!(
                    "paginated continuation {} disappeared before publication: {error}",
                    staged_path.display()
                ),
            })?;

        if let Some((recorder, _rollout_id, _history_mode)) = live_entry.as_ref() {
            {
                let mut live_recorders = store.live_recorders.lock().await;
                let entry = live_recorders
                    .get_mut(&thread_id)
                    .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
                entry.recovery = Some(LiveRecorderRecovery {
                    config: config.clone(),
                    rollout_path: active_path.clone(),
                });
            }
            recorder
                .shutdown()
                .await
                .map_err(|error| ThreadStoreError::Internal {
                    message: format!(
                        "failed to close previous paginated rollout {}: {error}",
                        source_path.display()
                    ),
                })?;
        }

        let publication = replace_stable_rollout(staged_path.clone(), active_path.clone())
            .await
            .map_err(|error| ThreadStoreError::Internal {
                message: format!(
                    "failed to publish paginated continuation {} from {}: {error}",
                    active_path.display(),
                    staged_path.display()
                ),
            })?;
        #[cfg(test)]
        let publication = if take_segment_durability_failure(thread_id) {
            StableRolloutPublication::DurabilityUnknown {
                error: ThreadStoreError::Internal {
                    message: "injected segment durability failure".to_string(),
                },
            }
        } else {
            publication
        };
        if live_entry.is_some() {
            let mut live_recorders = store.live_recorders.lock().await;
            let entry = live_recorders
                .get_mut(&thread_id)
                .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
            entry.history_mode = history_mode;
            entry.persistence_mode = ThreadPersistenceMode::Durable;
        }
        if live_entry.is_some() {
            #[cfg(test)]
            let injected_reopen_failure = take_segment_reopen_failure(thread_id);
            #[cfg(not(test))]
            let injected_reopen_failure = false;
            let reopen_result = if injected_reopen_failure {
                Err(ThreadStoreError::Internal {
                    message: "injected segment recorder reopen failure".to_string(),
                })
            } else {
                super::live_writer::live_writer_parts(store, thread_id)
                    .await
                    .map(|_| ())
            };
            if let Err(error) = reopen_result {
                warn!(%thread_id, %error, "native segment rotation committed; live writer will reopen on its next operation");
            }
        }
        crash_segment_rotation_at(thread_id, "stable_rollout_published_before_projection");
        let projection_result = async {
            super::thread_history::reset_projection_for_replacement(
                store,
                source_rollout_id,
                initial_rollout_ordinal,
            )
            .await?;
            super::thread_history_materialization::materialize_to_sqlite(
                store,
                source_rollout_id,
                active_path.as_path(),
            )
            .await
        }
        .await;
        if let Err(err) = projection_result {
            warn!(%thread_id, %err, "native segment rotation committed but paginated projection repair failed");
        }
        return Ok(FrozenRolloutSegmentResult {
            frozen: FrozenRolloutSegment {
                reference,
                history_base: native_history_base,
                source_session_meta: source_meta,
                history_mode,
                next_rollout_ordinal,
            },
            publication: FrozenSegmentPublication::ActiveRolloutReplaced(publication),
        });
    }

    cleanup_stale_staged_rollouts(stable_path.as_path()).await?;
    let staged_path = staged_rollout_path(stable_path.as_path());
    let staged_recorder = RolloutRecorder::new(
        &config,
        RolloutRecorderParams::CreateAtPath {
            path: staged_path.clone(),
            session_meta: Box::new(source_meta.meta.clone()),
            base_instructions: source_meta
                .meta
                .base_instructions
                .clone()
                .unwrap_or_default(),
            dynamic_tools: source_meta.meta.dynamic_tools.clone().unwrap_or_default(),
            initial_rollout_ordinal,
        },
    )
    .await
    .map_err(thread_store_io_error)?;
    if let Err(err) = staged_recorder
        .record_canonical_items(&[RolloutItem::RolloutReference(reference.clone())])
        .await
    {
        let _ = staged_recorder.shutdown().await;
        let _ = fs::remove_file(staged_path.as_path()).await;
        return Err(thread_store_io_error(err));
    }
    crash_segment_rotation_at(thread_id, "reference_recorded_before_checkpoint");
    if let Err(err) = staged_recorder
        .record_canonical_items(params.initial_items())
        .await
    {
        let _ = staged_recorder.shutdown().await;
        let _ = fs::remove_file(staged_path.as_path()).await;
        return Err(thread_store_io_error(err));
    }
    crash_segment_rotation_at(thread_id, "checkpoint_recorded_before_flush");
    if let Err(err) = staged_recorder.flush().await {
        let _ = staged_recorder.shutdown().await;
        let _ = fs::remove_file(staged_path.as_path()).await;
        return Err(thread_store_io_error(err));
    }
    staged_recorder
        .shutdown()
        .await
        .map_err(thread_store_io_error)?;
    crash_segment_rotation_at(thread_id, "staged_rollout_durable_before_publication");

    if let Some((recorder, _rollout_id, _history_mode)) = live_entry.as_ref() {
        {
            let mut live_recorders = store.live_recorders.lock().await;
            let entry = live_recorders
                .get_mut(&thread_id)
                .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
            entry.recovery = Some(LiveRecorderRecovery {
                config: config.clone(),
                rollout_path: stable_path.clone(),
            });
        }
        recorder.shutdown().await.map_err(thread_store_io_error)?;
    }
    let publication = match replace_stable_rollout(staged_path.clone(), stable_path.clone()).await {
        Ok(publication) => publication,
        Err(err) => {
            let _ = fs::remove_file(staged_path.as_path()).await;
            if live_entry.is_some()
                && let Err(reopen_error) =
                    super::live_writer::live_writer_parts(store, thread_id).await
            {
                warn!(%thread_id, %reopen_error, "failed to recover live writer after unpublished segment rotation");
            }
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "failed to atomically replace rollout {} with {}: {err}",
                    stable_path.display(),
                    staged_path.display()
                ),
            });
        }
    };
    crash_segment_rotation_at(thread_id, "stable_rollout_published_before_projection");
    #[cfg(test)]
    let publication = if take_segment_durability_failure(thread_id) {
        StableRolloutPublication::DurabilityUnknown {
            error: ThreadStoreError::Internal {
                message: "injected segment durability failure".to_string(),
            },
        }
    } else {
        publication
    };
    if matches!(&publication, StableRolloutPublication::Durable) && source_path != stable_path {
        match fs::remove_file(source_path.as_path()).await {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => {
                warn!(%thread_id, %err, "segment rotation committed but old compressed rollout remains")
            }
        }
    }

    if live_entry.is_some() {
        if let Some(entry) = store.live_recorders.lock().await.get_mut(&thread_id) {
            entry.history_mode = history_mode;
            entry.persistence_mode = ThreadPersistenceMode::Durable;
        }
        #[cfg(test)]
        let injected_reopen_failure = take_segment_reopen_failure(thread_id);
        #[cfg(not(test))]
        let injected_reopen_failure = false;
        let reopen_result = if injected_reopen_failure {
            Err(ThreadStoreError::Internal {
                message: "injected segment recorder reopen failure".to_string(),
            })
        } else {
            super::live_writer::live_writer_parts(store, thread_id)
                .await
                .map(|_| ())
        };
        if let Err(err) = reopen_result {
            warn!(%thread_id, %err, "segment rotation committed; live writer will reopen on its next operation");
        }
    }

    if let Err(err) =
        super::live_writer::sync_materialized_rollout_path(store, thread_id, stable_path.as_path())
            .await
    {
        warn!(%thread_id, %err, "segment rotation committed but rollout-path synchronization failed");
    }

    if matches!(history_mode, ThreadHistoryMode::Paginated) {
        let projection_result = async {
            super::thread_history::reset_projection_for_replacement(
                store,
                source_rollout_id,
                initial_rollout_ordinal,
            )
            .await?;
            super::thread_history_materialization::materialize_to_sqlite(
                store,
                source_rollout_id,
                stable_path.as_path(),
            )
            .await
        }
        .await;
        if let Err(err) = projection_result {
            warn!(%thread_id, %err, "segment rotation committed but paginated projection repair failed");
        }
    } else if let (Some(builder), Some(next_projection_ordinal)) =
        (legacy_projection_builder, legacy_next_projection_ordinal)
    {
        let reset_result = super::thread_history::reset_projection_for_replacement(
            store,
            source_rollout_id,
            next_projection_ordinal,
        )
        .await;
        let mut builder = builder.lock_owned().await;
        let result = match reset_result {
            Ok(()) => {
                super::thread_history_materialization::materialize_legacy_to_sqlite(
                    store,
                    source_rollout_id,
                    stable_path.as_path(),
                    &mut builder,
                )
                .await
            }
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            builder.reset();
            drop(builder);
            super::live_writer::invalidate_segmented_legacy_projection(
                store,
                thread_id,
                source_rollout_id,
            )
            .await;
            warn!(%thread_id, %error, "segment rotation committed but legacy projection repair failed");
        }
    }

    Ok(FrozenRolloutSegmentResult {
        frozen: FrozenRolloutSegment {
            reference,
            history_base: None,
            source_session_meta: source_meta,
            history_mode,
            next_rollout_ordinal,
        },
        publication: FrozenSegmentPublication::ActiveRolloutReplaced(publication),
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "the persisted fork boundary and its combined writer reservation are explicit"
)]
pub(super) async fn freeze_paginated_prefix_reserved(
    store: &LocalThreadStore,
    source_thread_id: ThreadId,
    source_rollout_path: &Path,
    prefix_thread_id: ThreadId,
    prefix_rollout_id: RolloutId,
    prefix_rollout_path: &Path,
    end_ordinal_exclusive: u64,
    end_byte_offset: u64,
    reservation: &RolloutWriterReservation,
) -> ThreadStoreResult<FrozenRolloutSegment> {
    freeze_paginated_prefix_reserved_inner(
        store,
        source_thread_id,
        source_rollout_path,
        prefix_thread_id,
        prefix_rollout_id,
        prefix_rollout_path,
        end_ordinal_exclusive,
        end_byte_offset,
        reservation,
        /*preserve_certified_immutable_reference*/ false,
    )
    .await
}

/// Freezes a certified same-thread prefix without rewalking its immutable predecessor chain.
#[expect(
    clippy::too_many_arguments,
    reason = "the certified fork boundary and its combined writer reservation are explicit"
)]
pub(super) async fn freeze_certified_paginated_prefix_reserved(
    store: &LocalThreadStore,
    source_thread_id: ThreadId,
    prefix_thread_id: ThreadId,
    prefix_rollout_id: RolloutId,
    prefix_rollout_path: &Path,
    end_ordinal_exclusive: u64,
    source_session_meta: SessionMetaLine,
    prefix_lines: Vec<RolloutLine>,
    reservation: &RolloutWriterReservation,
) -> ThreadStoreResult<FrozenRolloutSegment> {
    freeze_prepared_paginated_prefix_reserved_inner(
        store,
        source_thread_id,
        prefix_thread_id,
        prefix_rollout_id,
        prefix_rollout_path,
        end_ordinal_exclusive,
        source_session_meta,
        prefix_lines,
        reservation,
    )
    .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "the authenticated prefix identity and combined writer reservation are explicit"
)]
async fn freeze_prepared_paginated_prefix_reserved_inner(
    store: &LocalThreadStore,
    source_thread_id: ThreadId,
    prefix_thread_id: ThreadId,
    _prefix_rollout_id: RolloutId,
    prefix_rollout_path: &Path,
    end_ordinal_exclusive: u64,
    source_session_meta: SessionMetaLine,
    mut prefix_lines: Vec<RolloutLine>,
    reservation: &RolloutWriterReservation,
) -> ThreadStoreResult<FrozenRolloutSegment> {
    debug_assert!(reservation.contains(source_thread_id));
    debug_assert!(reservation.contains(prefix_thread_id));
    if source_session_meta.meta.id != source_thread_id {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "prepared fork source metadata does not belong to thread {source_thread_id}"
            ),
        });
    }
    let history_mode = source_session_meta.meta.history_mode;
    if prefix_rollout_path != codex_rollout::plain_rollout_path(prefix_rollout_path) {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "prepared fork prefix {} was not materialized before freezing",
                prefix_rollout_path.display()
            ),
        });
    }
    match prefix_lines.first().map(|line| &line.item) {
        Some(RolloutItem::SessionMeta(meta)) if meta.meta.id == prefix_thread_id => {}
        Some(RolloutItem::SessionMeta(_)) => {
            return Err(ThreadStoreError::Conflict {
                message: format!(
                    "prepared rollout prefix {} does not belong to thread {prefix_thread_id}",
                    prefix_rollout_path.display()
                ),
            });
        }
        _ => {
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "prepared rollout prefix {} does not start with session metadata",
                    prefix_rollout_path.display()
                ),
            });
        }
    }
    let actual_end_ordinal =
        validate_ordinals(prefix_lines.as_slice(), history_mode)?.ok_or_else(|| {
            ThreadStoreError::Internal {
                message: format!(
                    "prepared rollout prefix for {prefix_thread_id} has no terminal ordinal"
                ),
            }
        })?;
    if actual_end_ordinal != end_ordinal_exclusive {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "prepared rollout prefix for {prefix_thread_id} ended at ordinal \
                 {actual_end_ordinal}, expected {end_ordinal_exclusive}"
            ),
        });
    }
    let canonical_home = fs::canonicalize(store.config.codex_home.as_path())
        .await
        .ok();
    for line in prefix_lines.iter_mut().skip(1) {
        let RolloutItem::RolloutReference(reference) = &mut line.item else {
            continue;
        };
        if reference.thread_id == Some(prefix_thread_id)
            && reference.nth_user_message.is_none()
            && reference
                .compacted_replacement_history_filter_texts
                .is_none()
            && reference_has_valid_recorded_immutable_candidate(
                store,
                canonical_home.as_deref(),
                reference,
                prefix_thread_id,
            )
            .await
        {
            continue;
        }
        *reference = stabilize_rollout_reference(
            store,
            reference.clone(),
            &mut HashSet::new(),
            /*depth*/ 0,
            reservation,
        )
        .await?;
    }
    let snapshot_rollout_id = ThreadId::new();
    let immutable_path = native_history_segment_path(
        store.config.codex_home.as_path(),
        prefix_rollout_path,
        Some(snapshot_rollout_id),
    )?;
    install_snapshot_segment(
        prefix_lines.as_slice(),
        immutable_path.as_path(),
        /*segment_id*/ None,
    )
    .await?;
    Ok(FrozenRolloutSegment {
        reference: RolloutReferenceItem {
            rollout_id: Some(snapshot_rollout_id),
            rollout_path: immutable_path.clone(),
            thread_id: Some(prefix_thread_id),
            rollout_timestamp: rollout_timestamp_from_path(prefix_rollout_path),
            segment_id: None,
            max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        },
        history_base: Some(HistoryPosition {
            thread_id: snapshot_rollout_id,
            end_ordinal_exclusive,
            end_byte_offset: fs::metadata(immutable_path.as_path())
                .await
                .map_err(thread_store_io_error)?
                .len(),
        }),
        source_session_meta,
        history_mode,
        next_rollout_ordinal: Some(end_ordinal_exclusive),
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "the persisted fork boundary and immutable-reference policy are explicit"
)]
async fn freeze_paginated_prefix_reserved_inner(
    store: &LocalThreadStore,
    source_thread_id: ThreadId,
    source_rollout_path: &Path,
    prefix_thread_id: ThreadId,
    _prefix_rollout_id: RolloutId,
    prefix_rollout_path: &Path,
    end_ordinal_exclusive: u64,
    end_byte_offset: u64,
    reservation: &RolloutWriterReservation,
    preserve_certified_immutable_reference: bool,
) -> ThreadStoreResult<FrozenRolloutSegment> {
    debug_assert!(reservation.contains(source_thread_id));
    debug_assert!(reservation.contains(prefix_thread_id));
    let (source_session_meta, _, _, _, _) =
        validate_source_rollout(source_rollout_path, source_thread_id).await?;
    let history_mode = source_session_meta.meta.history_mode;
    if prefix_rollout_path != codex_rollout::plain_rollout_path(prefix_rollout_path) {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "prepared fork prefix {} was not materialized before freezing",
                prefix_rollout_path.display()
            ),
        });
    }
    let prefix_bytes = fs::read(prefix_rollout_path)
        .await
        .map_err(thread_store_io_error)?;
    let end_byte_offset =
        usize::try_from(end_byte_offset).map_err(|_| ThreadStoreError::Internal {
            message: format!("fork byte offset for {prefix_thread_id} exceeds addressable memory"),
        })?;
    let prefix =
        prefix_bytes
            .get(..end_byte_offset)
            .ok_or_else(|| ThreadStoreError::InvalidRequest {
                message: "fork boundary exceeds inherited source history".to_string(),
            })?;
    if !prefix.ends_with(b"\n") {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "fork boundary for {prefix_thread_id} is not a complete rollout record"
            ),
        });
    }
    let mut prefix_lines = prefix
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
        .filter_map(
            |line| match RolloutRecorder::parse_rollout_line_bytes(line) {
                Ok(Some(line)) => Some(Ok(line)),
                Ok(None) => None,
                Err(err) => Some(Err(ThreadStoreError::Internal {
                    message: format!(
                        "failed to read prepared rollout prefix for {prefix_thread_id}: {err}"
                    ),
                })),
            },
        )
        .collect::<ThreadStoreResult<Vec<_>>>()?;
    match prefix_lines.first().map(|line| &line.item) {
        Some(RolloutItem::SessionMeta(meta)) if meta.meta.id == prefix_thread_id => {}
        Some(RolloutItem::SessionMeta(_)) => {
            return Err(ThreadStoreError::Conflict {
                message: format!(
                    "prepared rollout prefix {} does not belong to thread {prefix_thread_id}",
                    prefix_rollout_path.display()
                ),
            });
        }
        _ => {
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "prepared rollout prefix {} does not start with session metadata",
                    prefix_rollout_path.display()
                ),
            });
        }
    }
    let actual_end_ordinal = prefix_lines
        .last()
        .and_then(|line| line.ordinal)
        .and_then(|ordinal| ordinal.checked_add(1))
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!(
                "prepared rollout prefix for {prefix_thread_id} has no terminal ordinal"
            ),
        })?;
    if actual_end_ordinal != end_ordinal_exclusive {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "prepared rollout prefix for {prefix_thread_id} ended at ordinal \
                 {actual_end_ordinal}, expected {end_ordinal_exclusive}"
            ),
        });
    }
    let canonical_home = fs::canonicalize(store.config.codex_home.as_path())
        .await
        .ok();
    for line in prefix_lines.iter_mut().skip(1) {
        let RolloutItem::RolloutReference(reference) = &mut line.item else {
            continue;
        };
        if preserve_certified_immutable_reference
            && reference.thread_id == Some(prefix_thread_id)
            && reference.nth_user_message.is_none()
            && reference
                .compacted_replacement_history_filter_texts
                .is_none()
            && reference_has_valid_recorded_immutable_candidate(
                store,
                canonical_home.as_deref(),
                reference,
                prefix_thread_id,
            )
            .await
        {
            continue;
        }
        *reference = stabilize_rollout_reference(
            store,
            reference.clone(),
            &mut HashSet::new(),
            /*depth*/ 0,
            reservation,
        )
        .await?;
    }
    let snapshot_rollout_id = ThreadId::new();
    let immutable_path = native_history_segment_path(
        store.config.codex_home.as_path(),
        prefix_rollout_path,
        Some(snapshot_rollout_id),
    )?;
    install_snapshot_segment(
        prefix_lines.as_slice(),
        immutable_path.as_path(),
        /*segment_id*/ None,
    )
    .await?;
    Ok(FrozenRolloutSegment {
        reference: RolloutReferenceItem {
            rollout_id: Some(snapshot_rollout_id),
            rollout_path: immutable_path.clone(),
            thread_id: Some(prefix_thread_id),
            rollout_timestamp: rollout_timestamp_from_path(prefix_rollout_path),
            segment_id: None,
            max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        },
        history_base: Some(HistoryPosition {
            thread_id: snapshot_rollout_id,
            end_ordinal_exclusive,
            end_byte_offset: fs::metadata(immutable_path.as_path())
                .await
                .map_err(thread_store_io_error)?
                .len(),
        }),
        source_session_meta,
        history_mode,
        next_rollout_ordinal: Some(end_ordinal_exclusive),
    })
}

/// Retains a referenced immutable snapshot while nested references are stabilized oldest first.
struct StabilizationFrame {
    reference: RolloutReferenceItem,
    thread_id: ThreadId,
    identity: (ThreadId, RolloutId, Option<SegmentId>),
    resolved_path: PathBuf,
    lines: Vec<RolloutLine>,
    next_line: usize,
    nested_reference_changed: bool,
    graph_depth: usize,
}

async fn stabilize_rollout_reference(
    store: &LocalThreadStore,
    reference: RolloutReferenceItem,
    active_references: &mut HashSet<(ThreadId, RolloutId, Option<SegmentId>)>,
    depth: usize,
    reservation: &RolloutWriterReservation,
) -> ThreadStoreResult<RolloutReferenceItem> {
    let mut inserted_references = Vec::new();
    let result = stabilize_rollout_reference_iteratively(
        store,
        reference,
        active_references,
        &mut inserted_references,
        depth,
        reservation,
    )
    .await;
    for identity in inserted_references {
        active_references.remove(&identity);
    }
    result
}

async fn stabilize_rollout_reference_iteratively(
    store: &LocalThreadStore,
    reference: RolloutReferenceItem,
    active_references: &mut HashSet<(ThreadId, RolloutId, Option<SegmentId>)>,
    inserted_references: &mut Vec<(ThreadId, RolloutId, Option<SegmentId>)>,
    depth: usize,
    reservation: &RolloutWriterReservation,
) -> ThreadStoreResult<RolloutReferenceItem> {
    let canonical_home = fs::canonicalize(store.config.codex_home.as_path())
        .await
        .ok();
    let mut frames = vec![
        load_stabilization_frame(
            store,
            canonical_home.as_deref(),
            reference,
            active_references,
            inserted_references,
            depth,
            reservation,
        )
        .await?,
    ];

    while let Some(frame) = frames.last_mut() {
        let mut nested_reference = None;
        while let Some(line) = frame.lines.get(frame.next_line) {
            frame.next_line += 1;
            if let RolloutItem::RolloutReference(reference) = &line.item {
                nested_reference = Some(reference.clone());
                break;
            }
        }

        if let Some(nested_reference) = nested_reference {
            let referenced_thread_id =
                nested_reference
                    .thread_id
                    .ok_or_else(|| ThreadStoreError::Internal {
                        message: format!(
                            "rollout reference {} is missing thread_id",
                            nested_reference.rollout_path.display()
                        ),
                    })?;
            let fork_boundary = referenced_thread_id != frame.thread_id
                || nested_reference.nth_user_message.is_some();
            let graph_depth = frame.graph_depth + usize::from(fork_boundary);
            frames.push(
                load_stabilization_frame(
                    store,
                    canonical_home.as_deref(),
                    nested_reference,
                    active_references,
                    inserted_references,
                    graph_depth,
                    reservation,
                )
                .await?,
            );
            continue;
        }

        let Some(completed) = frames.pop() else {
            return Err(ThreadStoreError::Internal {
                message: "rollout reference stabilization stack is empty".to_string(),
            });
        };
        let stabilized = if !completed.nested_reference_changed
            && is_immutable_segment_path(
                &store.config.codex_home,
                canonical_home.as_deref(),
                completed.resolved_path.as_path(),
                completed.thread_id,
                completed.reference.segment_id,
            ) {
            completed.reference
        } else {
            let segment_id = snapshot_segment_id(completed.lines.as_slice())?;
            let immutable_path = immutable_segment_path(
                store.config.codex_home.as_path(),
                completed.thread_id,
                Some(segment_id),
                codex_rollout::plain_rollout_path(completed.resolved_path.as_path()).as_path(),
            )?;
            install_snapshot_segment(
                completed.lines.as_slice(),
                immutable_path.as_path(),
                Some(segment_id),
            )
            .await?;
            RolloutReferenceItem {
                rollout_id: completed.reference.rollout_id,
                rollout_path: immutable_path,
                thread_id: Some(completed.thread_id),
                rollout_timestamp: completed.reference.rollout_timestamp,
                segment_id: Some(segment_id),
                max_depth: completed.reference.max_depth,
                nth_user_message: completed.reference.nth_user_message,
                compacted_replacement_history_filter_texts: completed
                    .reference
                    .compacted_replacement_history_filter_texts,
            }
        };
        active_references.remove(&completed.identity);

        let Some(parent) = frames.last_mut() else {
            return Ok(stabilized);
        };
        let Some(RolloutItem::RolloutReference(parent_reference)) = parent
            .lines
            .get_mut(parent.next_line.saturating_sub(1))
            .map(|line| &mut line.item)
        else {
            return Err(ThreadStoreError::Internal {
                message: "stabilized rollout reference has no parent reference".to_string(),
            });
        };
        parent.nested_reference_changed |= !rollout_references_equal(parent_reference, &stabilized);
        *parent_reference = stabilized;
    }

    Err(ThreadStoreError::Internal {
        message: "rollout reference stabilization completed without a result".to_string(),
    })
}

async fn load_stabilization_frame(
    store: &LocalThreadStore,
    canonical_home: Option<&Path>,
    reference: RolloutReferenceItem,
    active_references: &mut HashSet<(ThreadId, RolloutId, Option<SegmentId>)>,
    inserted_references: &mut Vec<(ThreadId, RolloutId, Option<SegmentId>)>,
    graph_depth: usize,
    reservation: &RolloutWriterReservation,
) -> ThreadStoreResult<StabilizationFrame> {
    if graph_depth >= MAX_ROLLOUT_REFERENCE_DEPTH {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "rollout reference graph exceeds maximum depth of {MAX_ROLLOUT_REFERENCE_DEPTH}"
            ),
        });
    }
    let thread_id = reference
        .thread_id
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!(
                "rollout reference {} is missing thread_id",
                reference.rollout_path.display()
            ),
        })?;
    let rollout_id = reference.rollout_id.unwrap_or(thread_id);
    let identity = (thread_id, rollout_id, reference.segment_id);
    if !active_references.insert(identity) {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "rollout reference cycle detected at {thread_id}/{}",
                reference
                    .segment_id
                    .map(|segment_id| segment_id.to_string())
                    .unwrap_or_else(|| "initial".to_string())
            ),
        });
    }
    inserted_references.push(identity);

    let resolved_path =
        codex_rollout::resolve_rollout_reference_path(&store.config.codex_home, &reference)
            .await
            .map_err(thread_store_io_error)?;
    if !is_immutable_segment_path(
        store.config.codex_home.as_path(),
        canonical_home,
        resolved_path.as_path(),
        thread_id,
        reference.segment_id,
    ) && !reservation.contains(thread_id)
    {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "mutable rollout predecessor for thread {thread_id} is outside the writer reservation"
            ),
        });
    }
    let (lines, loaded_thread_id, parse_errors) =
        RolloutRecorder::load_rollout_lines(resolved_path.as_path())
            .await
            .map_err(thread_store_io_error)?;
    if parse_errors != 0 {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "rollout {} contains {parse_errors} invalid record(s)",
                resolved_path.display()
            ),
        });
    }
    if loaded_thread_id != Some(thread_id) {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "rollout {} does not belong to thread {thread_id}",
                resolved_path.display()
            ),
        });
    }

    Ok(StabilizationFrame {
        reference,
        thread_id,
        identity,
        resolved_path,
        lines,
        next_line: 1,
        nested_reference_changed: false,
        graph_depth,
    })
}

fn is_immutable_segment_path(
    codex_home: &Path,
    canonical_home: Option<&Path>,
    path: &Path,
    thread_id: ThreadId,
    segment_id: Option<SegmentId>,
) -> bool {
    let native_directory =
        Path::new(codex_rollout::SESSIONS_SUBDIR).join(codex_rollout::ROLLOUT_SEGMENTS_SUBDIR);
    let rotated_directory = Path::new(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(
            segment_id
                .map(|segment_id| segment_id.to_string())
                .unwrap_or_else(|| "initial".to_string()),
        );
    let is_immutable = |home: &Path| {
        path.starts_with(home.join(&native_directory))
            || path.starts_with(home.join(&rotated_directory))
    };
    is_immutable(codex_home) || canonical_home.is_some_and(is_immutable)
}

fn rollout_references_equal(left: &RolloutReferenceItem, right: &RolloutReferenceItem) -> bool {
    left.rollout_path == right.rollout_path
        && left.thread_id == right.thread_id
        && left.rollout_id == right.rollout_id
        && left.rollout_timestamp == right.rollout_timestamp
        && left.segment_id == right.segment_id
        && left.max_depth == right.max_depth
        && left.nth_user_message == right.nth_user_message
        && left.compacted_replacement_history_filter_texts
            == right.compacted_replacement_history_filter_texts
}

async fn validate_source_rollout(
    path: &Path,
    thread_id: ThreadId,
) -> ThreadStoreResult<(
    SessionMetaLine,
    Option<u64>,
    Option<RolloutReferenceItem>,
    Vec<RolloutLine>,
    bool,
)> {
    let (mut lines, loaded_thread_id, parse_errors) = RolloutRecorder::load_rollout_lines(path)
        .await
        .map_err(thread_store_io_error)?;
    if loaded_thread_id != Some(thread_id) {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "rollout {} does not belong to thread {thread_id}",
                path.display()
            ),
        });
    }
    let source_meta = match lines.first().map(|line| &line.item) {
        Some(RolloutItem::SessionMeta(meta)) => meta.clone(),
        _ => {
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "rollout {} does not start with session metadata",
                    path.display()
                ),
            });
        }
    };
    if parse_errors != 0 && source_meta.meta.history_mode != ThreadHistoryMode::Legacy {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "rollout {} contains {parse_errors} invalid record(s)",
                path.display()
            ),
        });
    }
    let (next_rollout_ordinal, repaired_ordinals) =
        repair_source_ordinals(lines.as_mut_slice(), source_meta.meta.history_mode)?;
    let existing_reference = match lines.as_slice() {
        [
            RolloutLine {
                item: RolloutItem::SessionMeta(_),
                ..
            },
            RolloutLine {
                item: RolloutItem::RolloutReference(reference),
                ..
            },
        ] => Some(reference.clone()),
        _ => None,
    };
    Ok((
        source_meta,
        next_rollout_ordinal,
        existing_reference,
        lines,
        parse_errors != 0 || repaired_ordinals,
    ))
}

/// Restores the physical-record ordinal invariant before a segment is frozen.
///
/// Older checkpoint publication reopened a recorder at the final checkpoint ordinal, so the
/// first subsequent record could repeat that ordinal. Segment boundaries are defined by physical
/// record order; assigning contiguous ordinals from the authenticated first record preserves that
/// order when the canonical snapshot writer installs the repaired segment.
fn repair_source_ordinals(
    lines: &mut [RolloutLine],
    history_mode: ThreadHistoryMode,
) -> ThreadStoreResult<(Option<u64>, bool)> {
    if matches!(history_mode, ThreadHistoryMode::Legacy) {
        return Ok((validate_ordinals(lines, history_mode)?, false));
    }
    let Some(first) = lines.first().and_then(|line| line.ordinal) else {
        return Err(ThreadStoreError::Internal {
            message: "paginated rollout is empty or starts without an ordinal".to_string(),
        });
    };
    let mut expected = first;
    let mut repaired = false;
    for line in lines {
        if line.ordinal != Some(expected) {
            line.ordinal = Some(expected);
            repaired = true;
        }
        expected = expected
            .checked_add(1)
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "paginated rollout ordinal overflow".to_string(),
            })?;
    }
    Ok((Some(expected), repaired))
}

// Full-history forks freeze the parent's current rollout into an immutable segment and store a
// RolloutReference instead of copying inherited events. The content-derived ID lets unchanged
// snapshots share that segment while later parent appends produce a new fork boundary. This
// segmentation is what deduplicates fork history; it is not legacy compatibility machinery.
// The hash preimage omits SessionMeta.segment_id because the installed metadata stores the
// resulting ID; canonical object ordering makes the preimage stable across process reloads.
fn snapshot_segment_id(lines: &[RolloutLine]) -> ThreadStoreResult<SegmentId> {
    let mut hasher = Sha256::new();
    for encoded in canonical_snapshot_lines(lines, /*segment_id*/ None)? {
        hasher.update(encoded);
        hasher.update(b"\n");
    }
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Ok(SegmentId::from_bytes(bytes))
}

async fn install_snapshot_segment(
    lines: &[RolloutLine],
    destination: &Path,
    segment_id: Option<SegmentId>,
) -> ThreadStoreResult<()> {
    let parent = immutable_segment_parent(destination)?;
    fs::create_dir_all(parent.as_path())
        .await
        .map_err(thread_store_io_error)?;
    let temporary_path = immutable_segment_temporary_path(destination);
    let write_result = async {
        let mut temporary_file = create_immutable_segment_file(temporary_path.as_path()).await?;
        for encoded in canonical_snapshot_lines(lines, segment_id).map_err(io::Error::other)? {
            temporary_file.write_all(encoded.as_slice()).await?;
            temporary_file.write_all(b"\n").await?;
        }
        temporary_file.sync_all().await?;
        Ok::<(), io::Error>(())
    }
    .await;
    if let Err(err) = write_result {
        let _ = fs::remove_file(temporary_path.as_path()).await;
        return Err(thread_store_io_error(err));
    }
    commit_immutable_segment(temporary_path, destination, parent).await
}

fn canonical_snapshot_lines(
    lines: &[RolloutLine],
    segment_id: Option<SegmentId>,
) -> ThreadStoreResult<Vec<Vec<u8>>> {
    lines
        .iter()
        .enumerate()
        .map(|(index, line)| {
            let mut line = line.clone();
            if index == 0
                && let RolloutItem::SessionMeta(meta) = &mut line.item
            {
                meta.meta.segment_id = segment_id;
            }
            let value = serde_json::to_value(line).map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to serialize rollout snapshot: {err}"),
            })?;
            serde_json::to_vec(&canonicalize_json(&value)).map_err(|err| {
                ThreadStoreError::Internal {
                    message: format!("failed to encode canonical rollout snapshot: {err}"),
                }
            })
        })
        .collect()
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(canonicalize_json).collect()),
        Value::Object(map) => {
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| *key);
            let mut sorted = serde_json::Map::with_capacity(map.len());
            for (key, value) in entries {
                sorted.insert(key.clone(), canonicalize_json(value));
            }
            Value::Object(sorted)
        }
        _ => value.clone(),
    }
}

fn validate_ordinals(
    lines: &[RolloutLine],
    history_mode: ThreadHistoryMode,
) -> ThreadStoreResult<Option<u64>> {
    match history_mode {
        ThreadHistoryMode::Legacy => {
            if lines.iter().any(|line| line.ordinal.is_some()) {
                return Err(ThreadStoreError::Internal {
                    message: "legacy rollout contains a paginated ordinal".to_string(),
                });
            }
            Ok(None)
        }
        ThreadHistoryMode::Paginated => {
            let mut ordinals = lines.iter().map(|line| {
                line.ordinal.ok_or_else(|| ThreadStoreError::Internal {
                    message: "paginated rollout line is missing an ordinal".to_string(),
                })
            });
            let Some(first) = ordinals.next() else {
                return Err(ThreadStoreError::Internal {
                    message: "paginated rollout is empty".to_string(),
                });
            };
            let mut expected = first?;
            for ordinal in ordinals {
                expected = expected
                    .checked_add(1)
                    .ok_or_else(|| ThreadStoreError::Internal {
                        message: "paginated rollout ordinal overflow".to_string(),
                    })?;
                if ordinal? != expected {
                    return Err(ThreadStoreError::Internal {
                        message: format!("paginated rollout expected ordinal {expected}"),
                    });
                }
            }
            expected
                .checked_add(1)
                .map(Some)
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: "paginated rollout ordinal overflow".to_string(),
                })
        }
    }
}

fn rollout_config(store: &LocalThreadStore, meta: &SessionMeta) -> RolloutConfig {
    RolloutConfig {
        codex_home: store.config.codex_home.clone(),
        sqlite: store.config.sqlite.clone(),
        cwd: meta.cwd.clone(),
        model_provider_id: meta
            .model_provider
            .clone()
            .unwrap_or_else(|| store.config.default_model_provider_id.clone()),
        generate_memories: meta.memory_mode.as_deref() != Some("disabled"),
    }
}

async fn create_paginated_continuation_recorder(
    config: &RolloutConfig,
    source_meta: &SessionMeta,
    path: PathBuf,
    history_base: HistoryPosition,
    initial_rollout_ordinal: u64,
) -> ThreadStoreResult<RolloutRecorder> {
    let mut continuation_meta = source_meta.clone();
    continuation_meta.segment_id = None;
    continuation_meta.history_base = Some(history_base);
    RolloutRecorder::new(
        config,
        RolloutRecorderParams::CreateAtPath {
            path,
            session_meta: Box::new(continuation_meta),
            base_instructions: source_meta.base_instructions.clone().unwrap_or_default(),
            dynamic_tools: source_meta.dynamic_tools.clone().unwrap_or_default(),
            initial_rollout_ordinal,
        },
    )
    .await
    .map_err(thread_store_io_error)
}

/// Places native `history_base` predecessors below `sessions/` without exposing them as threads.
///
/// Upstream resolves a physical rollout ID recursively below `sessions/`, while thread listing
/// only enters date directories. Keeping the canonical filename makes the predecessor readable by
/// unmodified upstream Codex and keeps it out of the desktop thread list.
fn native_history_segment_path(
    codex_home: &Path,
    source_path: &Path,
    rollout_id: Option<RolloutId>,
) -> ThreadStoreResult<PathBuf> {
    let source_path = match rollout_id {
        Some(rollout_id) => {
            codex_rollout::history_rollout_path_with_rollout_id(source_path, rollout_id)
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: format!(
                        "rollout {} does not have a canonical filename",
                        source_path.display()
                    ),
                })?
        }
        None => source_path.to_path_buf(),
    };
    let file_name = source_path
        .file_name()
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!(
                "rollout {} does not have a file name",
                source_path.display()
            ),
        })?;
    let (year, month, day) =
        codex_rollout::rollout_date_parts(file_name).ok_or_else(|| ThreadStoreError::Internal {
            message: format!(
                "rollout {} does not have a canonical dated filename",
                source_path.display()
            ),
        })?;
    Ok(codex_home
        .join(codex_rollout::SESSIONS_SUBDIR)
        .join(codex_rollout::ROLLOUT_SEGMENTS_SUBDIR)
        .join(year)
        .join(month)
        .join(day)
        .join(file_name))
}

fn immutable_segment_path(
    codex_home: &Path,
    thread_id: ThreadId,
    segment_id: Option<SegmentId>,
    source_path: &Path,
) -> ThreadStoreResult<PathBuf> {
    let file_name = source_path
        .file_name()
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!(
                "rollout {} does not have a file name",
                source_path.display()
            ),
        })?;
    Ok(codex_home
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(
            segment_id
                .map(|segment_id| segment_id.to_string())
                .unwrap_or_else(|| "initial".to_string()),
        )
        .join(file_name))
}

async fn install_immutable_segment(source: &Path, destination: &Path) -> ThreadStoreResult<()> {
    let parent = immutable_segment_parent(destination)?;
    fs::create_dir_all(parent.as_path())
        .await
        .map_err(thread_store_io_error)?;
    // Copy and flush before installing the destination name. Linking the live source directly
    // would let later appends mutate a segment that references already treat as immutable.
    let temporary_path = immutable_segment_temporary_path(destination);
    let copy_result = async {
        let mut source_file = fs::File::open(source).await?;
        let mut temporary_file = create_immutable_segment_file(temporary_path.as_path()).await?;
        tokio::io::copy(&mut source_file, &mut temporary_file).await?;
        temporary_file.sync_all().await
    }
    .await;
    if let Err(err) = copy_result {
        let _ = fs::remove_file(temporary_path.as_path()).await;
        return Err(thread_store_io_error(err));
    }
    commit_immutable_segment(temporary_path, destination, parent).await
}

fn immutable_segment_parent(destination: &Path) -> ThreadStoreResult<PathBuf> {
    destination
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!(
                "immutable rollout segment {} does not have a parent",
                destination.display()
            ),
        })
}

fn immutable_segment_temporary_path(destination: &Path) -> PathBuf {
    let mut temporary_path = destination.as_os_str().to_os_string();
    temporary_path.push(format!(".install-{}.tmp", SegmentId::new()));
    PathBuf::from(temporary_path)
}

async fn commit_immutable_segment(
    temporary_path: PathBuf,
    destination: &Path,
    parent: PathBuf,
) -> ThreadStoreResult<()> {
    let result = async {
        match fs::hard_link(temporary_path.as_path(), destination).await {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                let destination_metadata = fs::symlink_metadata(destination)
                    .await
                    .map_err(thread_store_io_error)?;
                if !destination_metadata.file_type().is_file() {
                    return Err(ThreadStoreError::Conflict {
                        message: format!(
                            "immutable rollout segment {} already exists but is not a regular file",
                            destination.display()
                        ),
                    });
                }
                if !files_equal_and_sync_destination(
                    temporary_path.as_path(),
                    destination,
                    destination_metadata,
                )
                .await?
                {
                    // The existing segment may already be referenced. Never replace its contents
                    // while recovering an interrupted rotation; fail closed instead.
                    return Err(ThreadStoreError::Conflict {
                        message: format!(
                            "immutable rollout segment {} already exists with different contents",
                            destination.display()
                        ),
                    });
                }
            }
            Err(err) => {
                return Err(ThreadStoreError::Internal {
                    message: format!(
                        "failed to install immutable rollout segment {}: {err}",
                        destination.display()
                    ),
                });
            }
        }
        let destination_metadata = fs::symlink_metadata(destination)
            .await
            .map_err(thread_store_io_error)?;
        if !destination_metadata.file_type().is_file() {
            return Err(ThreadStoreError::Conflict {
                message: format!(
                    "immutable rollout segment {} changed before synchronization",
                    destination.display()
                ),
            });
        }
        // A new destination is the already-mode-0600, already-synchronized temporary inode;
        // hard-linking it does not require reopening and synchronizing the same inode again. The
        // pre-existing branch compared and synchronized through its verified descriptor above.
        #[cfg(unix)]
        tokio::task::spawn_blocking(move || std::fs::File::open(parent)?.sync_all())
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to join immutable segment directory sync: {err}"),
            })?
            .map_err(thread_store_io_error)?;
        #[cfg(not(unix))]
        let _ = parent;
        Ok(())
    }
    .await;
    let _ = fs::remove_file(temporary_path.as_path()).await;
    result
}

async fn create_immutable_segment_file(path: &Path) -> io::Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    let file = options.open(path).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .await?;
    }
    Ok(file)
}

async fn files_equal_and_sync_destination(
    left: &Path,
    right_path: &Path,
    expected_metadata: std::fs::Metadata,
) -> ThreadStoreResult<bool> {
    let left_len = fs::metadata(left)
        .await
        .map_err(thread_store_io_error)?
        .len();
    let right_len = expected_metadata.len();
    if left_len != right_len {
        return Ok(false);
    }
    let mut left = fs::File::open(left).await.map_err(thread_store_io_error)?;
    let mut options = fs::OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut right = options
        .open(right_path)
        .await
        .map_err(thread_store_io_error)?;
    let opened_metadata = right.metadata().await.map_err(thread_store_io_error)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if opened_metadata.dev() != expected_metadata.dev()
            || opened_metadata.ino() != expected_metadata.ino()
        {
            return Err(ThreadStoreError::Conflict {
                message: format!(
                    "immutable rollout segment {} changed before synchronization",
                    right_path.display()
                ),
            });
        }
    }
    #[cfg(not(unix))]
    if !opened_metadata.file_type().is_file() {
        return Ok(false);
    }
    let mut left_buffer = vec![0; 64 * 1024];
    let mut right_buffer = vec![0; 64 * 1024];
    loop {
        let left_count = left
            .read(left_buffer.as_mut_slice())
            .await
            .map_err(thread_store_io_error)?;
        let right_count = right
            .read(right_buffer.as_mut_slice())
            .await
            .map_err(thread_store_io_error)?;
        if left_count != right_count || left_buffer[..left_count] != right_buffer[..right_count] {
            return Ok(false);
        }
        if left_count == 0 {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                right
                    .set_permissions(std::fs::Permissions::from_mode(0o600))
                    .await
                    .map_err(thread_store_io_error)?;
            }
            right.sync_all().await.map_err(thread_store_io_error)?;
            return Ok(true);
        }
    }
}

async fn replace_stable_rollout(
    staged: PathBuf,
    stable: PathBuf,
) -> io::Result<StableRolloutPublication> {
    let staged_for_write = staged.clone();
    let stable_for_write = stable.clone();
    let replace_result = tokio::task::spawn_blocking(move || {
        let contents = std::fs::read_to_string(staged_for_write.as_path())?;
        codex_utils_path::write_atomically(stable_for_write.as_path(), contents.as_str())
    })
    .await;
    match replace_result {
        Ok(result) => result?,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => panic!("rollout replacement task was cancelled: {error}"),
    }
    let _ = fs::remove_file(staged).await;
    let publication = match sync_stable_rollout_publication(stable.as_path()).await {
        Ok(()) => StableRolloutPublication::Durable,
        Err(error) => StableRolloutPublication::DurabilityUnknown { error },
    };
    Ok(publication)
}

async fn sync_stable_rollout_publication(stable: &Path) -> ThreadStoreResult<()> {
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(stable)
        .await
        .map_err(thread_store_io_error)?
        .sync_all()
        .await
        .map_err(thread_store_io_error)?;
    let parent =
        stable
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| ThreadStoreError::Internal {
                message: format!("rollout {} does not have a parent", stable.display()),
            })?;
    #[cfg(unix)]
    tokio::task::spawn_blocking(move || std::fs::File::open(parent)?.sync_all())
        .await
        .map_err(|error| ThreadStoreError::Internal {
            message: format!("failed to join rollout directory sync: {error}"),
        })?
        .map_err(thread_store_io_error)?;
    #[cfg(not(unix))]
    let _ = parent;
    Ok(())
}

fn staged_rollout_path(stable_path: &Path) -> PathBuf {
    let mut staged = stable_path.as_os_str().to_os_string();
    staged.push(format!(".staged-{}.tmp", SegmentId::new()));
    PathBuf::from(staged)
}

/// Removes segment-rotation files left by a process that died before publication.
///
/// Callers hold the rollout writer reservation for `stable_path`, so a matching staged file
/// cannot belong to another live rotation.
pub(super) async fn cleanup_stale_staged_rollouts(stable_path: &Path) -> ThreadStoreResult<()> {
    let parent = stable_path
        .parent()
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!("rollout {} does not have a parent", stable_path.display()),
        })?;
    let stable_name = stable_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!(
                "rollout {} does not have a UTF-8 file name",
                stable_path.display()
            ),
        })?;
    let staged_prefix = format!("{stable_name}.staged-");
    let mut entries = match fs::read_dir(parent).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(thread_store_io_error(error)),
    };
    let mut removed = false;
    while let Some(entry) = entries.next_entry().await.map_err(thread_store_io_error)? {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with(staged_prefix.as_str()) && name.ends_with(".tmp") {
            match fs::remove_file(entry.path()).await {
                Ok(()) => removed = true,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(thread_store_io_error(error)),
            }
        }
    }
    if removed {
        sync_stable_rollout_publication(stable_path).await?;
    }
    Ok(())
}

fn rollout_timestamp_from_path(path: &Path) -> Option<String> {
    let file_name = path.file_name()?.to_str()?;
    codex_rollout::rollout_id_from_path(path)?;
    let timestamp = file_name.strip_prefix("rollout-")?.get(..19)?;
    Some(timestamp.to_string())
}

fn thread_store_io_error(err: io::Error) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: err.to_string(),
    }
}

#[cfg(test)]
#[path = "segment_tests.rs"]
mod tests;
