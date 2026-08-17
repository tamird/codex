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
use codex_protocol::protocol::ThreadHistoryMode;
use tempfile::NamedTempFile;
#[cfg(test)]
use tokio::sync::Notify;
use tracing::warn;

use super::LocalThreadStore;
use super::live_writer;
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
/// A newer read aborts the previous task, including an in-progress rebuild. Dropping the rebuild
/// future rolls back its SQLite transaction and leaves its staging marker for the next rebuild to
/// remove. This keeps complete-lineage maintenance out of interactive request measurements and
/// prevents a user request from competing with projection repair for memory and I/O.
pub(super) struct ScheduledProjectionRebuild {
    generation: u64,
    abort: Option<tokio::task::AbortHandle>,
}

pub(super) type ScheduledProjectionRebuilds = HashMap<ThreadId, ScheduledProjectionRebuild>;

/// Schedules one rebuild after the thread has had a quiet period.
pub(super) async fn schedule(store: LocalThreadStore, thread_id: ThreadId) {
    let schedules = Arc::clone(&store.projection_rebuild_schedules);
    let (generation, previous_abort) = {
        let mut schedules = schedules
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = schedules.remove(&thread_id);
        let generation = previous
            .as_ref()
            .map_or(0, |scheduled| scheduled.generation.wrapping_add(1));
        let previous_abort = previous.and_then(|scheduled| scheduled.abort);
        schedules.insert(
            thread_id,
            ScheduledProjectionRebuild {
                generation,
                abort: None,
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
        Some(scheduled) if scheduled.generation == generation => {
            scheduled.abort = Some(abort);
        }
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

async fn rebuild_registered(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<bool> {
    if store.state_db.is_none() {
        return Ok(false);
    }
    cleanup_stale_staging(store).await?;
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

        let staging_thread_id = ThreadId::new();
        let staging_guard =
            ProjectionStagingGuard::create(store, thread_id, staging_thread_id).await?;
        let staged_result = stage_lineage(store, staging_thread_id, &lineage).await;
        if let Err(error) = staged_result {
            discard_staging(store, staging_thread_id, staging_guard).await?;
            return Err(error);
        }
        #[cfg(test)]
        crash_at_boundary(thread_id, "after_staging");
        #[cfg(test)]
        pause_after_staging(thread_id).await;

        let _lifecycle = store.live_writer_locks.reserve_lifecycle(thread_id).await;
        let _writers = store.reserve_rollout_writers(&[thread_id]).await?;
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
        let active = current
            .segments
            .last()
            .ok_or_else(|| projection_error("selected lineage has no active segment"))?;
        let active_path = codex_rollout::existing_rollout_path(active.rollout_path())
            .await
            .ok_or_else(|| projection_error("selected active rollout is unavailable"))?;
        if active_path.extension().and_then(|value| value.to_str()) == Some("zst") {
            discard_staging(store, staging_thread_id, staging_guard).await?;
            return Err(projection_error("selected active rollout is compressed"));
        }
        thread_history_materialization::materialize_to_sqlite(
            store,
            staging_thread_id,
            active_path.as_path(),
        )
        .await?;
        let staged_state = thread_history::projection_state(store, staging_thread_id)
            .await?
            .ok_or_else(|| projection_error("staged projection has no checkpoint"))?;
        let active_len = tokio::fs::metadata(active_path.as_path())
            .await
            .map_err(projection_io_error)?
            .len();
        if staged_state.next_byte_offset != active_len {
            discard_staging(store, staging_thread_id, staging_guard).await?;
            continue;
        }
        thread_history::publish_staged_projection(store, selected.rollout_id, staging_thread_id)
            .await?;
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
        let expected_end = segment.jsonl_end_byte_offset();
        let actual_len = tokio::fs::metadata(prepared.path())
            .await
            .map_err(projection_io_error)?
            .len();
        if expected_end.is_some_and(|end| end != actual_len) {
            return Err(projection_error(format!(
                "lineage segment {} selects {expected_end:?} of {actual_len} decoded bytes",
                segment.rollout_path().display()
            )));
        }
        thread_history_materialization::materialize_to_sqlite(
            store,
            staging_thread_id,
            prepared.path(),
        )
        .await?;
    }
    Ok(())
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
