use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(test)]
use std::sync::LazyLock;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ThreadHistoryMode;
use tempfile::NamedTempFile;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::BufReader;
#[cfg(test)]
use tokio::sync::Notify;
use tracing::warn;

use super::LocalThreadStore;
use super::live_writer;
use super::ordinal_recovery;
use super::thread_history;
use super::thread_history_materialization;
use super::thread_rollout_resolver;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

#[cfg(not(test))]
const BACKGROUND_REBUILD_QUIET_PERIOD: Duration = Duration::from_secs(5);
#[cfg(test)]
const BACKGROUND_REBUILD_QUIET_PERIOD: Duration = Duration::from_millis(25);
const MAX_REBUILD_ATTEMPTS: usize = 3;
const STAGING_DIRECTORY: &str = "projection-rebuilds";

#[cfg(test)]
pub(super) const PROJECTION_REBUILD_CRASH_THREAD_ENV: &str =
    "FRODEX_PROJECTION_REBUILD_CRASH_THREAD";
#[cfg(test)]
pub(super) const PROJECTION_REBUILD_CRASH_BOUNDARY_ENV: &str =
    "FRODEX_PROJECTION_REBUILD_CRASH_BOUNDARY";
#[cfg(test)]
pub(super) const PROJECTION_REBUILD_CRASH_EXIT_CODE: i32 = 88;

#[cfg(test)]
static PROJECTION_REBUILD_PAUSES: LazyLock<
    StdMutex<HashMap<ThreadId, Arc<ProjectionRebuildPause>>>,
> = LazyLock::new(|| StdMutex::new(HashMap::new()));

#[cfg(test)]
pub(super) struct ProjectionRebuildPause {
    pub(super) entered: Notify,
    pub(super) release: Notify,
}

#[cfg(test)]
pub(super) fn inject_projection_rebuild_pause(thread_id: ThreadId) -> Arc<ProjectionRebuildPause> {
    let pause = Arc::new(ProjectionRebuildPause {
        entered: Notify::new(),
        release: Notify::new(),
    });
    PROJECTION_REBUILD_PAUSES
        .lock()
        .expect("projection rebuild pause mutex")
        .insert(thread_id, Arc::clone(&pause));
    pause
}

/// A delayed rebuild selected by the most recent unprojected read.
///
/// A newer read resets the quiet period before work begins. Once the rebuild starts, later reads
/// leave it running so a frequently opened thread cannot starve its projection repair.
pub(super) struct ScheduledProjectionRebuild {
    generation: u64,
    state: ScheduledProjectionRebuildState,
}

enum ScheduledProjectionRebuildState {
    Waiting(Option<tokio::task::AbortHandle>),
    Rebuilding,
}

pub(super) type ScheduledProjectionRebuilds = HashMap<ThreadId, ScheduledProjectionRebuild>;

/// Schedules one rebuild after the thread has had a quiet period.
pub(super) async fn schedule(store: LocalThreadStore, thread_id: ThreadId) {
    let schedules = Arc::clone(&store.projection_rebuild_schedules);
    let (generation, previous_abort) = {
        let mut schedules = schedules
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if schedules.get(&thread_id).is_some_and(|scheduled| {
            matches!(
                scheduled.state,
                ScheduledProjectionRebuildState::Rebuilding
                    | ScheduledProjectionRebuildState::Waiting(None)
            )
        }) {
            return;
        }
        let previous = schedules.remove(&thread_id);
        let generation = previous
            .as_ref()
            .map_or(0, |scheduled| scheduled.generation.wrapping_add(1));
        let previous_abort = previous.and_then(|scheduled| match scheduled.state {
            ScheduledProjectionRebuildState::Waiting(abort) => abort,
            ScheduledProjectionRebuildState::Rebuilding => None,
        });
        schedules.insert(
            thread_id,
            ScheduledProjectionRebuild {
                generation,
                state: ScheduledProjectionRebuildState::Waiting(None),
            },
        );
        (generation, previous_abort)
    };

    if let Some(abort) = previous_abort {
        abort.abort();
    }

    let task_schedules = Arc::clone(&schedules);
    let task = tokio::spawn(async move {
        tokio::time::sleep(BACKGROUND_REBUILD_QUIET_PERIOD).await;
        {
            let mut schedules = task_schedules
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(scheduled) = schedules
                .get_mut(&thread_id)
                .filter(|scheduled| scheduled.generation == generation)
            else {
                return;
            };
            scheduled.state = ScheduledProjectionRebuildState::Rebuilding;
        }
        if let Err(error) = rebuild(&store, thread_id).await {
            warn!(%thread_id, %error, "background Paginated history projection rebuild failed");
        }
        let mut schedules = task_schedules
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if schedules
            .get(&thread_id)
            .is_some_and(|scheduled| scheduled.generation == generation)
        {
            schedules.remove(&thread_id);
        }
    });
    let abort = task.abort_handle();
    let mut schedules = schedules
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match schedules.get_mut(&thread_id) {
        Some(scheduled)
            if scheduled.generation == generation
                && matches!(
                    scheduled.state,
                    ScheduledProjectionRebuildState::Waiting(None)
                ) =>
        {
            scheduled.state = ScheduledProjectionRebuildState::Waiting(Some(abort));
        }
        Some(scheduled)
            if scheduled.generation == generation
                && matches!(scheduled.state, ScheduledProjectionRebuildState::Rebuilding) => {}
        _ => abort.abort(),
    }
}

pub(super) async fn rebuild(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<bool> {
    let Some(_registration) = register(store, thread_id) else {
        return Ok(false);
    };
    rebuild_registered(store, thread_id).await
}

pub(super) async fn rebuild_waiting(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<bool> {
    loop {
        if let Some(_registration) = register(store, thread_id) {
            return rebuild_registered(store, thread_id).await;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

struct ProjectionRebuildRegistration {
    active: Arc<StdMutex<HashSet<ThreadId>>>,
    thread_id: ThreadId,
}

impl Drop for ProjectionRebuildRegistration {
    fn drop(&mut self) {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.thread_id);
    }
}

fn register(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> Option<ProjectionRebuildRegistration> {
    let active = Arc::clone(&store.projection_rebuilds);
    if !active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(thread_id)
    {
        return None;
    }
    Some(ProjectionRebuildRegistration { active, thread_id })
}

#[expect(
    clippy::await_holding_invalid_type,
    reason = "staging cleanup and publication must exclude other projection rebuilds in this store"
)]
async fn rebuild_registered(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<bool> {
    if store.state_db.is_none() {
        return Ok(false);
    }
    let _rebuild_gate = store.projection_rebuild_gate.lock().await;
    cleanup_stale_staging(store).await?;
    if store.has_history_projection(thread_id).await? {
        return Ok(true);
    }
    for _ in 0..MAX_REBUILD_ATTEMPTS {
        let Some(selected) =
            thread_rollout_resolver::resolve_current_including_archived(store, thread_id).await?
        else {
            return Ok(false);
        };
        let session_meta = codex_rollout::read_session_meta_line(selected.path.as_path())
            .await
            .map_err(projection_io_error)?;
        if session_meta.meta.history_mode != ThreadHistoryMode::Paginated {
            return Ok(false);
        }
        let lineage = store.resolve_rollout_lineage(thread_id).await?;
        if lineage.root_rollout_id != selected.rollout_id
            || lineage
                .segments
                .iter()
                .any(|segment| !segment.filter_texts.is_empty())
        {
            return Ok(false);
        }
        let ordinal_recovery = ordinal_recovery::prepare(store, &selected, &session_meta).await?;
        let projection_lineage = match ordinal_recovery.as_ref() {
            Some(recovery) => {
                store
                    .resolve_rollout_lineage_from_path(thread_id, &recovery.rollout_path)
                    .await?
            }
            None => lineage.clone(),
        };

        let staging_thread_id = ThreadId::new();
        let staging_guard =
            ProjectionStagingGuard::create(store, thread_id, staging_thread_id).await?;
        let staged_result = stage_lineage(store, staging_thread_id, &projection_lineage).await;
        if let Err(error) = staged_result {
            discard_staging(store, staging_thread_id, staging_guard).await?;
            return Err(error);
        }
        #[cfg(test)]
        crash_at_boundary(thread_id, "after_staging");
        #[cfg(test)]
        pause_after_staging(thread_id).await;

        // Ordinal recovery already holds both reservations through corrected-file publication.
        let _lifecycle = if ordinal_recovery.is_none() {
            Some(store.live_writer_locks.reserve_lifecycle(thread_id).await)
        } else {
            None
        };
        let _writers = if ordinal_recovery.is_none() {
            Some(store.reserve_rollout_writers(&[thread_id]).await?)
        } else {
            None
        };
        match live_writer::persist_thread_reserved(store, thread_id).await {
            Ok(()) | Err(ThreadStoreError::ThreadNotFound { .. }) => {}
            Err(error) => {
                discard_staging(store, staging_thread_id, staging_guard).await?;
                return Err(error);
            }
        }
        let current = store.resolve_rollout_lineage(thread_id).await?;
        if current != lineage {
            discard_staging(store, staging_thread_id, staging_guard).await?;
            continue;
        }
        let active = projection_lineage
            .segments
            .last()
            .ok_or_else(|| projection_error("selected lineage has no active segment"))?;
        let active_path = codex_rollout::existing_rollout_path(active.rollout_path())
            .await
            .ok_or_else(|| projection_error("selected active rollout is unavailable"))?;
        let active_is_compressed =
            active_path.extension().and_then(|value| value.to_str()) == Some("zst");
        if active_is_compressed {
            let archived = store
                .state_db
                .as_ref()
                .ok_or_else(|| projection_error("projection rebuild requires SQLite metadata"))?
                .get_thread(thread_id)
                .await
                .map_err(|error| {
                    projection_error(format!("failed to read archived thread metadata: {error}"))
                })?
                .is_some_and(|metadata| metadata.archived_at.is_some());
            if !archived {
                discard_staging(store, staging_thread_id, staging_guard).await?;
                return Err(projection_error("selected active rollout is compressed"));
            }
        } else {
            // An unarchived active rollout may have appended after the lineage snapshot. Catch its
            // projection up while the writer is reserved before publishing the staged rows.
            thread_history_materialization::materialize_to_sqlite(
                store,
                staging_thread_id,
                active_path.as_path(),
            )
            .await?;
        }
        let staged_state = thread_history::projection_state(store, staging_thread_id)
            .await?
            .ok_or_else(|| projection_error("staged projection has no checkpoint"))?;
        if !active_is_compressed {
            let active_len = tokio::fs::metadata(active_path.as_path())
                .await
                .map_err(projection_io_error)?
                .len();
            if staged_state.next_byte_offset != active_len {
                discard_staging(store, staging_thread_id, staging_guard).await?;
                continue;
            }
        }
        thread_history::publish_staged_projection(
            store,
            projection_lineage.root_rollout_id,
            staging_thread_id,
        )
        .await?;
        if let Some(recovery) = ordinal_recovery {
            recovery.select(store, thread_id).await?;
        }
        staging_guard.remove().await?;
        return Ok(true);
    }
    Err(ThreadStoreError::Conflict {
        message: format!(
            "thread {thread_id} changed during {MAX_REBUILD_ATTEMPTS} projection rebuild attempts"
        ),
    })
}

#[cfg(test)]
pub(super) fn crash_at_boundary(thread_id: ThreadId, boundary: &str) {
    if std::env::var(PROJECTION_REBUILD_CRASH_THREAD_ENV)
        .ok()
        .as_deref()
        == Some(thread_id.to_string().as_str())
        && std::env::var(PROJECTION_REBUILD_CRASH_BOUNDARY_ENV)
            .ok()
            .as_deref()
            == Some(boundary)
    {
        std::process::exit(PROJECTION_REBUILD_CRASH_EXIT_CODE);
    }
}

/// A durable marker whose advisory lock distinguishes active staging from crash residue.
struct ProjectionStagingGuard {
    path: PathBuf,
    directory: PathBuf,
    file: File,
}

impl ProjectionStagingGuard {
    async fn create(
        store: &LocalThreadStore,
        source_thread_id: ThreadId,
        staging_thread_id: ThreadId,
    ) -> ThreadStoreResult<Self> {
        let directory = store.config.codex_home.join(".tmp").join(STAGING_DIRECTORY);
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(directory.as_path()).map_err(projection_io_error)?;
            let path = directory.join(format!("{staging_thread_id}.lock"));
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(path.as_path())
                .map_err(projection_io_error)?;
            file.lock().map_err(projection_io_error)?;
            writeln!(file, "{source_thread_id}").map_err(projection_io_error)?;
            file.sync_all().map_err(projection_io_error)?;
            sync_directory(directory.as_path())?;
            Ok(Self {
                path,
                directory,
                file,
            })
        })
        .await
        .map_err(|error| projection_error(format!("projection staging task failed: {error}")))?
    }

    async fn remove(self) -> ThreadStoreResult<()> {
        tokio::task::spawn_blocking(move || {
            let Self {
                path,
                directory,
                file,
            } = self;
            drop(file);
            match std::fs::remove_file(path.as_path()) {
                Ok(()) => sync_directory(directory.as_path()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(projection_io_error(error)),
            }
        })
        .await
        .map_err(|error| projection_error(format!("projection cleanup task failed: {error}")))?
    }
}

async fn discard_staging(
    store: &LocalThreadStore,
    staging_thread_id: ThreadId,
    guard: ProjectionStagingGuard,
) -> ThreadStoreResult<()> {
    thread_history::delete_thread(store, staging_thread_id).await?;
    guard.remove().await
}

async fn cleanup_stale_staging(store: &LocalThreadStore) -> ThreadStoreResult<()> {
    let directory = store.config.codex_home.join(".tmp").join(STAGING_DIRECTORY);
    let stale = tokio::task::spawn_blocking(
        move || -> ThreadStoreResult<Vec<(ThreadId, ProjectionStagingGuard)>> {
            let entries = match std::fs::read_dir(directory.as_path()) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(error) => return Err(projection_io_error(error)),
            };
            let mut stale = Vec::new();
            for entry in entries {
                let entry = entry.map_err(projection_io_error)?;
                let path = entry.path();
                let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                    continue;
                };
                let Some(staging_id) = name.strip_suffix(".lock") else {
                    continue;
                };
                let Ok(staging_id) = ThreadId::from_string(staging_id) else {
                    continue;
                };
                let file = match OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(path.as_path())
                {
                    Ok(file) => file,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(projection_io_error(error)),
                };
                match file.try_lock() {
                    Ok(()) => {
                        stale.push((
                            staging_id,
                            ProjectionStagingGuard {
                                path,
                                directory: directory.clone(),
                                file,
                            },
                        ));
                    }
                    Err(std::fs::TryLockError::WouldBlock) => {}
                    Err(std::fs::TryLockError::Error(error)) => {
                        return Err(projection_io_error(error));
                    }
                }
            }
            Ok(stale)
        },
    )
    .await
    .map_err(|error| projection_error(format!("projection cleanup task failed: {error}")))??;
    for (staging_id, guard) in stale {
        thread_history::delete_thread(store, staging_id).await?;
        guard.remove().await?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> ThreadStoreResult<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(projection_io_error)
}

#[cfg(test)]
async fn pause_after_staging(thread_id: ThreadId) {
    let pause = PROJECTION_REBUILD_PAUSES
        .lock()
        .expect("projection rebuild pause mutex")
        .remove(&thread_id);
    if let Some(pause) = pause {
        pause.entered.notify_one();
        pause.release.notified().await;
    }
}

async fn stage_lineage(
    store: &LocalThreadStore,
    staging_thread_id: ThreadId,
    lineage: &super::rollout_lineage::RolloutLineage,
) -> ThreadStoreResult<()> {
    for segment in &lineage.segments {
        let existing = codex_rollout::existing_rollout_path(segment.rollout_path())
            .await
            .ok_or_else(|| {
                projection_error(format!(
                    "lineage segment {} is unavailable",
                    segment.rollout_path().display()
                ))
            })?;
        let prepared = ProjectionInput::prepare(store, existing).await?;
        start_segment(store, staging_thread_id, prepared.path(), segment).await?;
        if let Some(end_ordinal_exclusive) = segment.end_ordinal_exclusive {
            let end_byte_offset = match segment.jsonl_end_byte_offset() {
                Some(offset) => offset,
                None => super::rollout_lineage::byte_offset_for_ordinal(
                    prepared.path(),
                    end_ordinal_exclusive,
                )
                .await?
                .ok_or_else(|| projection_error("decoded lineage segment has no byte boundary"))?,
            };
            thread_history_materialization::materialize_prefix_to_sqlite(
                store,
                staging_thread_id,
                prepared.path(),
                HistoryPosition {
                    thread_id: segment.rollout_id,
                    end_ordinal_exclusive,
                    end_byte_offset,
                },
            )
            .await?;
        } else {
            // The active segment has no fixed ordinal cutoff. Its writer may still append;
            // the reserved-writer catch-up below verifies the final publication boundary.
            thread_history_materialization::materialize_to_sqlite(
                store,
                staging_thread_id,
                prepared.path(),
            )
            .await?;
        }
    }
    Ok(())
}

/// Starts each physical file at its own metadata boundary while retaining the selected history.
async fn start_segment(
    store: &LocalThreadStore,
    staging_thread_id: ThreadId,
    path: &Path,
    segment: &super::rollout_lineage::RolloutLineageSegment,
) -> ThreadStoreResult<()> {
    let head = super::rollout_lineage::read_rollout_head(path).await?;
    let metadata_ordinal = head
        .session_meta_ordinal
        .ok_or_else(|| projection_error("lineage metadata has no ordinal"))?;
    let state = thread_history::projection_state(store, staging_thread_id).await?;
    let next_ordinal = match state {
        Some(state) => state.next_ordinal,
        None if head.leading_reference.is_none()
            && head.session_meta.meta.history_base.is_none() =>
        {
            metadata_ordinal
        }
        None => {
            return Err(projection_error(
                "lineage segment has no projected predecessor",
            ));
        }
    };
    let expected_start = if let Some((reference_ordinal, reference)) = &head.leading_reference {
        let predecessor_end = reference_ordinal
            .checked_sub(1)
            .ok_or_else(|| projection_error("lineage reference precedes its metadata"))?;
        // Older compatibility files repeat metadata at ordinal zero. A user-message cutoff can
        // also omit part of the referenced ordinal range; no other reference authorizes that gap.
        if (metadata_ordinal != predecessor_end && metadata_ordinal != 0)
            || next_ordinal > predecessor_end
            || (next_ordinal != predecessor_end && reference.nth_user_message.is_none())
        {
            return Err(projection_error(format!(
                "lineage segment {} has metadata ordinal {metadata_ordinal} and reference ordinal \
                 {reference_ordinal}, after projected ordinal {next_ordinal}",
                segment.rollout_path().display()
            )));
        }
        *reference_ordinal
    } else {
        if next_ordinal != metadata_ordinal
            || head
                .session_meta
                .meta
                .history_base
                .is_some_and(|base| base.end_ordinal_exclusive != metadata_ordinal)
        {
            return Err(projection_error(format!(
                "lineage segment {} starts at metadata ordinal {metadata_ordinal}, \
                 after projected ordinal {next_ordinal}",
                segment.rollout_path().display()
            )));
        }
        metadata_ordinal
            .checked_add(1)
            .ok_or_else(|| projection_error("lineage metadata ordinal overflow"))?
    };
    if head.session_meta.meta.id != segment.thread_id
        || head.session_meta.meta.history_mode != ThreadHistoryMode::Paginated
        || head.first_local_ordinal != expected_start
        || segment.start_ordinal != expected_start
    {
        return Err(projection_error(format!(
            "lineage segment {} does not start at expected local ordinal {expected_start}",
            segment.rollout_path().display()
        )));
    }

    let file = tokio::fs::File::open(path)
        .await
        .map_err(projection_io_error)?;
    let mut reader = BufReader::new(file);
    let mut bytes = Vec::new();
    loop {
        bytes.clear();
        let count = reader
            .read_until(b'\n', &mut bytes)
            .await
            .map_err(projection_io_error)?;
        if count == 0 || !bytes.ends_with(b"\n") {
            return Err(projection_error(
                "lineage metadata is not a complete JSONL record",
            ));
        }
        if !bytes.iter().all(u8::is_ascii_whitespace) {
            break;
        }
    }
    let metadata_end = reader
        .stream_position()
        .await
        .map_err(projection_io_error)?;

    thread_history::reset_projection_for_replacement(store, staging_thread_id, next_ordinal)
        .await?;
    thread_history::apply_projection(
        store,
        staging_thread_id,
        /*start_offset*/ 0,
        metadata_end,
        next_ordinal,
        vec![thread_history::RolloutProjectionStep::SkippedOrdinalRange {
            start_ordinal: next_ordinal,
            end_ordinal_exclusive: expected_start,
        }],
    )
    .await
}

/// Plain path retained with an optional temporary decompression owner.
struct ProjectionInput {
    path: PathBuf,
    _temporary: Option<NamedTempFile>,
}

impl ProjectionInput {
    async fn prepare(store: &LocalThreadStore, path: PathBuf) -> ThreadStoreResult<Self> {
        if path.extension().and_then(|value| value.to_str()) != Some("zst") {
            return Ok(Self {
                path,
                _temporary: None,
            });
        }
        let temp_dir = store.config.codex_home.join(".tmp");
        tokio::fs::create_dir_all(temp_dir.as_path())
            .await
            .map_err(projection_io_error)?;
        let temporary = tokio::task::spawn_blocking(move || -> std::io::Result<NamedTempFile> {
            let mut temporary = tempfile::Builder::new()
                .prefix("projection-rebuild-")
                .suffix(".jsonl")
                .tempfile_in(temp_dir)?;
            let source = File::open(path)?;
            let mut decoder = zstd::stream::read::Decoder::new(source)?;
            std::io::copy(&mut decoder, temporary.as_file_mut())?;
            temporary.as_file_mut().flush()?;
            Ok(temporary)
        })
        .await
        .map_err(|error| {
            projection_error(format!("projection decompression task failed: {error}"))
        })?
        .map_err(projection_io_error)?;
        Ok(Self {
            path: temporary.path().to_path_buf(),
            _temporary: Some(temporary),
        })
    }

    fn path(&self) -> &Path {
        self.path.as_path()
    }
}

fn projection_error(message: impl Into<String>) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: message.into(),
    }
}

fn projection_io_error(error: impl std::fmt::Display) -> ThreadStoreError {
    projection_error(format!(
        "failed to rebuild Paginated history projection: {error}"
    ))
}
