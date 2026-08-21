//! Coordinates automatic rollout migration with interactive thread loads.
//!
//! App-server startup enables migration without inventorying rollout files. A request that loads
//! one Legacy or `RolloutReference`-backed Paginated thread migrates that thread and waits for the
//! shared attempt. Native thread loads, list operations, and search operations do not start
//! unrelated migration work.
//!
//! The older creation-ordered Legacy cursor remains below for compatibility with the explicit
//! startup migration entry point and its persisted skip fingerprints. Automatic native
//! `history_base` conversion does not trust that cursor because it may predate Paginated reference
//! conversion.
//!
//! Rollouts that background migration cannot finish are remembered so they do not hold the cursor
//! back forever. Ordinary failures stay skipped until a manual migration retries them; busy
//! rollouts are retried on later startups because the writer may have gone away.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::ffi::OsString;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::StateDbHandle;
use codex_state::RolloutMigrationCursor;
use codex_state::RolloutMigrationSkippedRollout;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::watch;
use tracing::warn;

use super::LocalThreadStore;
use super::RolloutMigrationMode;
use super::RolloutMigrationOptions;
use super::RolloutMigrationReport;
use super::RolloutMigrationStatus;
use super::dependencies::MigrationAdmission;
use super::dependencies::discover_dependencies;
use super::find_all_rollout_paths;
use super::lineage::contains_convertible_rollout_reference;
use super::lineage::has_leading_filtered_rollout_reference;
use super::migration_error;
use super::publish::migration_journal_path;
use super::publish::pending_migration_thread_ids;
use super::telemetry::RolloutMigrationTrigger;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use crate::local::live_writer;
use crate::local::thread_rollout_resolver;

const LEGACY_TO_PAGINATED_MIGRATION_ID: &str = "legacy_to_paginated_v1";
const NATIVE_HISTORY_BASE_MIGRATION_ID: &str = "native_history_base_v2_mixed_rollback";
const EMPTY_SKIP_REASON: &str = "empty";
const FAILED_SKIP_REASON: &str = "failed";
const MALFORMED_SESSION_META_SKIP_REASON: &str = "malformed_session_meta";
const BUSY_SKIP_REASON: &str = "busy";
const NATIVE_OR_COMPATIBLE_REASON: &str = "native_or_compatible";
const CURSOR_LOOKBACK_SECONDS: i64 = 48 * 60 * 60;
const MAINTENANCE_RETRY_DELAY: Duration = Duration::from_secs(1);

#[path = "recent_failures.rs"]
mod recent_failures;

/// Bounds migration workers and joins concurrent loads of the same thread.
#[derive(Default)]
pub(crate) struct StartupMigrationCoordinator {
    enabled: AtomicBool,
    state: Mutex<CoordinatorState>,
    ready: Notify,
    #[cfg(test)]
    processed: Mutex<Vec<ThreadId>>,
    /// Tests pause after durable journal creation without holding a coordinator mutex.
    #[cfg(test)]
    pub(super) journal_barriers: Mutex<HashMap<ThreadId, Arc<tokio::sync::Barrier>>>,
}

#[derive(Default)]
struct CoordinatorState {
    workers: usize,
    /// Retain compression priority even while every conflicting attempt is waiting to retry.
    foreground: Option<Arc<codex_rollout::RolloutMaintenanceIntentGuard>>,
    priority: VecDeque<ThreadId>,
    entries: HashMap<ThreadId, MigrationEntry>,
    recent_failures: recent_failures::RecentFailures,
}

struct MigrationEntry {
    path: PathBuf,
    rollout_id: codex_protocol::RolloutId,
    source_fingerprint: RolloutFingerprint,
    modified_at: SystemTime,
    status: MigrationEntryStatus,
    completion: watch::Sender<MigrationCompletion>,
    admission: MigrationAdmission,
    not_before: tokio::time::Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MigrationEntryStatus {
    Pending,
    Running,
    Complete,
}

#[derive(Clone, Debug)]
enum MigrationCompletion {
    Pending,
    Complete(Result<(), String>),
}

struct MigrationWork {
    thread_id: ThreadId,
    path: PathBuf,
    rollout_id: codex_protocol::RolloutId,
    source_fingerprint: RolloutFingerprint,
    admission: MigrationAdmission,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RolloutFingerprint {
    size_bytes: i64,
    modified_at_ns: i64,
}

enum StartupInspection {
    Paginated,
    Compatible,
    Legacy,
    NeedsMigration,
    ReferenceBacked,
    Skipped,
    Unresolved,
}

pub(super) fn start_automatic_rollout_migration(store: LocalThreadStore) {
    store
        .rollout_migration_coordinator
        .enabled
        .store(true, Ordering::Release);
}

pub(super) async fn await_thread_migration(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    if live_writer::rollout_path(store, thread_id).await.is_ok() {
        return Ok(());
    }
    let enabled = store
        .rollout_migration_coordinator
        .enabled
        .load(Ordering::Acquire);
    if !enabled {
        return Ok(());
    }

    let existing_receiver = {
        let mut state = store.rollout_migration_coordinator.state.lock().await;
        subscribe_to_existing(&mut state, thread_id)
    };
    let mut receiver = if let Some(receiver) = existing_receiver {
        receiver
    } else {
        if recent_failures::should_defer_retry(store, thread_id).await {
            return Ok(());
        }
        let Some(mut resolved) =
            thread_rollout_resolver::resolve_current_including_archived(store, thread_id).await?
        else {
            return Ok(());
        };
        let journal = migration_journal_path(&store.config.codex_home, thread_id);
        let mut before = rollout_fingerprint(&resolved.path).await?;
        // A complete native root projection already covers its immutable predecessors. Walking
        // those predecessors again made every nonresident page inventory the entire home.
        // Leading RolloutReference roots still require classification and automatic conversion.
        let complete_native_root =
            resolved
                .authenticated_session_meta
                .as_ref()
                .is_some_and(|metadata| {
                    metadata.history_mode == ThreadHistoryMode::Paginated
                        && metadata.history_base.is_some()
                })
                && crate::local::thread_history::has_complete_root_projection_for_resolved(
                    store,
                    thread_id,
                    resolved.clone(),
                )
                .await?;
        let inspection = if complete_native_root {
            Ok(StartupInspection::Paginated)
        } else {
            inspect_rollout_path(store, &resolved.path).await
        };
        let inspection = match inspection {
            Ok(inspection) => inspection,
            Err(error) => {
                if tokio::fs::try_exists(&journal)
                    .await
                    .map_err(migration_error)?
                {
                    warn!(
                        thread_id = %thread_id,
                        path = %resolved.path.display(),
                        "automatic rollout migration inspection failed with a pending journal; attempting recovery: {error}"
                    );
                    StartupInspection::Unresolved
                } else {
                    warn!(
                        thread_id = %thread_id,
                        path = %resolved.path.display(),
                        "automatic rollout migration inspection failed; using the selected rollout's supported reader: {error}"
                    );
                    return Ok(());
                }
            }
        };
        if matches!(
            inspection,
            StartupInspection::Legacy | StartupInspection::ReferenceBacked
        ) && let Some(_migration_guard) =
            codex_rollout::try_acquire_rollout_migration_dependency_lock(
                &store.config.codex_home,
                &[thread_id],
            )
            .map_err(migration_error)?
        {
            // With migration excluded for this task, a busy writer belongs to an active session,
            // not a competing converter. Report that conflict instead of retrying indefinitely.
            drop(store.writer_lock_coordinator.acquire(thread_id)?);
        }
        let mut native_history_ready = match inspection {
            StartupInspection::Paginated => {
                complete_native_root || store.has_history_projection(thread_id).await?
            }
            StartupInspection::Compatible => true,
            StartupInspection::Legacy
            | StartupInspection::NeedsMigration
            | StartupInspection::ReferenceBacked
            | StartupInspection::Skipped
            | StartupInspection::Unresolved => false,
        };
        if matches!(inspection, StartupInspection::Paginated)
            && !native_history_ready
            && !tokio::fs::try_exists(&journal)
                .await
                .map_err(migration_error)?
        {
            // Projection repair has its own writer reservations. It must not queue behind a
            // different thread's Legacy conversion or acquire that conversion's home-wide job.
            match store.try_complete_history_projection(thread_id).await {
                Ok(true) => {
                    let Some(current) =
                        thread_rollout_resolver::resolve_current_including_archived(
                            store, thread_id,
                        )
                        .await?
                    else {
                        return Ok(());
                    };
                    // Ordinal recovery can select a corrected sibling during projection repair.
                    resolved = current;
                    before = rollout_fingerprint(&resolved.path).await?;
                    native_history_ready = store.has_history_projection(thread_id).await?;
                }
                result => {
                    native_history_ready = retained_selected_source_is_unchanged(
                        store,
                        thread_id,
                        &resolved.path,
                        resolved.rollout_id,
                        before,
                    )
                    .await?;
                    if native_history_ready && let Err(error) = result {
                        warn!(
                            thread_id = %thread_id,
                            path = %resolved.path.display(),
                            "native projection repair retained the selected source; using its supported reader: {error}"
                        );
                    }
                }
            }
        }
        let already_native = !tokio::fs::try_exists(&journal)
            .await
            .map_err(migration_error)?
            && native_history_ready
            && rollout_fingerprint(&resolved.path).await? == before
            && !tokio::fs::try_exists(&journal)
                .await
                .map_err(migration_error)?
            && thread_rollout_resolver::resolve_current_including_archived(store, thread_id)
                .await?
                .is_some_and(|current| {
                    current.rollout_id == resolved.rollout_id && current.path == resolved.path
                });
        let modified_at = rollout_modified_at(resolved.path.as_path()).await?;
        let admission = if already_native {
            MigrationAdmission::Exclusive
        } else {
            match discover_dependencies(&store.config.codex_home, &resolved.path).await {
                Ok(Some(dependencies)) => MigrationAdmission::Shared(Arc::new(dependencies)),
                Ok(None) | Err(_) => MigrationAdmission::Exclusive,
            }
        };
        let mut state = store.rollout_migration_coordinator.state.lock().await;
        if let Some(receiver) = subscribe_to_existing(&mut state, thread_id) {
            receiver
        } else if already_native {
            // Native reads must not join a worker that is converting an unrelated Legacy thread.
            return Ok(());
        } else {
            let (completion, receiver) = watch::channel(MigrationCompletion::Pending);
            state.entries.insert(
                thread_id,
                MigrationEntry {
                    path: resolved.path,
                    rollout_id: resolved.rollout_id,
                    source_fingerprint: before,
                    modified_at,
                    status: MigrationEntryStatus::Pending,
                    completion,
                    admission,
                    not_before: tokio::time::Instant::now(),
                },
            );
            state.priority.push_back(thread_id);
            receiver
        }
    };
    ensure_worker(store).await?;

    loop {
        let completion = receiver.borrow().clone();
        match completion {
            MigrationCompletion::Pending => {
                receiver.changed().await.map_err(|_| {
                    migration_error(format!(
                        "automatic rollout migration stopped before thread {thread_id} completed"
                    ))
                })?;
            }
            MigrationCompletion::Complete(Ok(())) => return Ok(()),
            MigrationCompletion::Complete(Err(message)) => {
                return Err(migration_error(message));
            }
        }
    }
}

fn subscribe_to_existing(
    state: &mut CoordinatorState,
    thread_id: ThreadId,
) -> Option<watch::Receiver<MigrationCompletion>> {
    let entry = state.entries.get_mut(&thread_id)?;
    let pending = entry.status == MigrationEntryStatus::Pending;
    let receiver = entry.completion.subscribe();
    if pending && !state.priority.contains(&thread_id) {
        state.priority.push_back(thread_id);
    }
    Some(receiver)
}

async fn rollout_modified_at(path: &Path) -> ThreadStoreResult<SystemTime> {
    tokio::fs::metadata(path)
        .await
        .and_then(|metadata| metadata.modified())
        .map_err(migration_error)
}

async fn ensure_worker(store: &LocalThreadStore) -> ThreadStoreResult<()> {
    let foreground = Arc::new(
        codex_rollout::acquire_rollout_maintenance_intent(&store.config.codex_home)
            .await
            .map_err(migration_error)?,
    );
    let count = {
        let mut state = store.rollout_migration_coordinator.state.lock().await;
        state.foreground.get_or_insert(foreground);
        let pending = state
            .entries
            .values()
            .filter(|entry| entry.status == MigrationEntryStatus::Pending)
            .count();
        let count = pending.min(codex_rollout::MAX_CONCURRENT_ROLLOUT_MIGRATIONS - state.workers);
        state.workers += count;
        if state.workers == 0 {
            state.foreground = None;
        }
        count
    };
    store.rollout_migration_coordinator.ready.notify_one();
    for _ in 0..count {
        let store = store.clone();
        tokio::spawn(async move { run_worker(store).await });
    }
    Ok(())
}

async fn run_worker(store: LocalThreadStore) {
    loop {
        let Some(mut work) = next_work(&store).await else {
            return;
        };
        #[cfg(test)]
        store
            .rollout_migration_coordinator
            .processed
            .lock()
            .await
            .push(work.thread_id);
        let result = store
            .migrate_rollout_path_on_demand(work.thread_id, work.path.clone(), &mut work.admission)
            .await;
        let result = match result {
            Ok(Some(outcome)) if outcome.status == RolloutMigrationStatus::Failed => {
                let message = outcome.message.clone().unwrap_or_else(|| {
                    format!(
                        "automatic rollout migration failed for {}",
                        work.path.display()
                    )
                });
                match retained_selected_source_is_unchanged(
                    &store,
                    work.thread_id,
                    &work.path,
                    work.rollout_id,
                    work.source_fingerprint,
                )
                .await
                {
                    Ok(true) => {
                        warn!(
                            thread_id = %work.thread_id,
                            path = %work.path.display(),
                            message = %message,
                            "automatic rollout migration left the selected source unchanged; using its supported reader"
                        );
                        Ok(Some(outcome))
                    }
                    Ok(false) => Err(migration_error(format!(
                        "{message}; automatic rollout migration did not retain an unchanged selected source"
                    ))),
                    Err(error) => Err(error),
                }
            }
            result => result,
        };
        if let Ok(Some(outcome)) = &result
            && matches!(
                outcome.status,
                RolloutMigrationStatus::Migrated | RolloutMigrationStatus::AlreadyPaginated
            )
            && let Ok(fingerprint) = rollout_fingerprint(outcome.rollout_path.as_path()).await
            && let Err(error) =
                record_terminal_inspection(&store, outcome.rollout_path.as_path(), fingerprint)
                    .await
        {
            warn!(
                thread_id = %work.thread_id,
                path = %outcome.rollout_path.display(),
                "failed to record automatic rollout migration fingerprint: {error}"
            );
        }
        {
            let mut state = store.rollout_migration_coordinator.state.lock().await;
            if matches!(&result, Ok(Some(outcome)) if outcome.status == RolloutMigrationStatus::Failed)
            {
                state.recent_failures.record(&work);
            }
            let Some(entry) = state.entries.get_mut(&work.thread_id) else {
                continue;
            };
            entry.admission = work.admission;
            match result {
                Ok(Some(outcome)) if outcome.status == RolloutMigrationStatus::SkippedBusy => {
                    entry.status = MigrationEntryStatus::Pending;
                    entry.not_before =
                        tokio::time::Instant::now() + std::time::Duration::from_millis(50);
                    state.priority.push_back(work.thread_id);
                }
                Ok(Some(_)) => {
                    finish_entry(entry, Ok(()));
                }
                Ok(None) => {
                    finish_entry(entry, Ok(()));
                }
                Err(crate::ThreadStoreError::Conflict { .. }) => {
                    entry.status = MigrationEntryStatus::Pending;
                    entry.not_before =
                        tokio::time::Instant::now() + std::time::Duration::from_millis(250);
                    state.priority.push_back(work.thread_id);
                }
                Err(error) => {
                    finish_entry(entry, Err(error.to_string()));
                }
            }
            // Receivers retain the terminal watch value. Do not cache completed attempts forever:
            // a repaired or replaced source must be inspected on its next request.
            if state
                .entries
                .get(&work.thread_id)
                .is_some_and(|entry| entry.status == MigrationEntryStatus::Complete)
            {
                state.entries.remove(&work.thread_id);
            }
        }
        store.rollout_migration_coordinator.ready.notify_one();
    }
}

async fn next_work(store: &LocalThreadStore) -> Option<MigrationWork> {
    loop {
        let retry_at = {
            let mut state = store.rollout_migration_coordinator.state.lock().await;
            let mut priority_thread = None;
            for _ in 0..state.priority.len() {
                let thread_id = state.priority.pop_front()?;
                if state.entries.get(&thread_id).is_some_and(|entry| {
                    entry.status == MigrationEntryStatus::Pending
                        && entry.not_before <= tokio::time::Instant::now()
                }) {
                    priority_thread = Some(thread_id);
                    break;
                }
                state.priority.push_back(thread_id);
            }
            let thread_id = priority_thread.or_else(|| {
                state
                    .entries
                    .iter()
                    .filter(|(_, entry)| {
                        entry.status == MigrationEntryStatus::Pending
                            && entry.not_before <= tokio::time::Instant::now()
                    })
                    .max_by(|left, right| {
                        left.1
                            .modified_at
                            .cmp(&right.1.modified_at)
                            .then_with(|| right.1.path.cmp(&left.1.path))
                    })
                    .map(|(thread_id, _)| *thread_id)
            });
            if let Some(thread_id) = thread_id {
                let entry = state.entries.get_mut(&thread_id)?;
                entry.status = MigrationEntryStatus::Running;
                return Some(MigrationWork {
                    thread_id,
                    path: entry.path.clone(),
                    rollout_id: entry.rollout_id,
                    source_fingerprint: entry.source_fingerprint,
                    admission: entry.admission.clone(),
                });
            }
            let next = state
                .entries
                .values()
                .filter(|entry| entry.status == MigrationEntryStatus::Pending)
                .map(|entry| entry.not_before)
                .min();
            if let Some(next) = next {
                next
            } else {
                state.workers -= 1;
                if state.workers == 0 {
                    state.foreground = None;
                }
                return None;
            }
        };
        tokio::select! {
            _ = tokio::time::sleep_until(retry_at) => {},
            _ = store.rollout_migration_coordinator.ready.notified() => {},
        }
    }
}

/// A failed conversion may fall back only when no publication or recovery state escaped.
async fn retained_selected_source_is_unchanged(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    path: &Path,
    rollout_id: codex_protocol::RolloutId,
    source_fingerprint: RolloutFingerprint,
) -> ThreadStoreResult<bool> {
    let journal = migration_journal_path(&store.config.codex_home, thread_id);
    if tokio::fs::try_exists(&journal)
        .await
        .map_err(migration_error)?
    {
        return Ok(false);
    }
    if rollout_fingerprint(path).await? != source_fingerprint {
        return Ok(false);
    }
    let selected =
        thread_rollout_resolver::resolve_current_including_archived(store, thread_id).await?;
    if !selected.is_some_and(|current| current.rollout_id == rollout_id && current.path == path) {
        return Ok(false);
    }
    Ok(rollout_fingerprint(path).await? == source_fingerprint
        && !tokio::fs::try_exists(&journal)
            .await
            .map_err(migration_error)?)
}

fn finish_entry(entry: &mut MigrationEntry, result: Result<(), String>) {
    entry.status = MigrationEntryStatus::Complete;
    entry
        .completion
        .send_replace(MigrationCompletion::Complete(result));
}

async fn record_terminal_inspection(
    store: &LocalThreadStore,
    path: &Path,
    fingerprint: RolloutFingerprint,
) -> ThreadStoreResult<()> {
    startup_state_db(store)?
        .record_rollout_migration_skip(
            NATIVE_HISTORY_BASE_MIGRATION_ID,
            &RolloutMigrationSkippedRollout {
                rollout_path: relative_rollout_path(store, path),
                rollout_size_bytes: fingerprint.size_bytes,
                rollout_modified_at_ns: fingerprint.modified_at_ns,
                skip_reason: NATIVE_OR_COMPATIBLE_REASON.to_string(),
            },
        )
        .await
        .map_err(migration_error)
}

#[cfg(test)]
pub(super) async fn processed_thread_ids(store: &LocalThreadStore) -> Vec<ThreadId> {
    store
        .rollout_migration_coordinator
        .processed
        .lock()
        .await
        .clone()
}

#[cfg(test)]
pub(super) async fn automatic_migration_idle(store: &LocalThreadStore) -> bool {
    let state = store.rollout_migration_coordinator.state.lock().await;
    state.workers == 0
        && state.entries.values().all(|entry| {
            !matches!(
                entry.status,
                MigrationEntryStatus::Pending | MigrationEntryStatus::Running
            )
        })
}

pub(super) async fn migrate_rollouts_on_startup(store: &LocalThreadStore) -> ThreadStoreResult<()> {
    let Some(state_db) = store.state_db.as_ref() else {
        return Ok(());
    };
    let paths = find_all_rollout_paths(&store.config.codex_home).await?;
    let mut skipped_rollouts = state_db
        .list_rollout_migration_skipped_rollouts(LEGACY_TO_PAGINATED_MIGRATION_ID)
        .await
        .map_err(migration_error)?;
    retry_busy_rollouts(store, skipped_rollouts.as_slice(), paths.as_slice()).await?;
    skipped_rollouts = state_db
        .list_rollout_migration_skipped_rollouts(LEGACY_TO_PAGINATED_MIGRATION_ID)
        .await
        .map_err(migration_error)?;
    if !pending_migration_thread_ids(&store.config.codex_home)
        .await?
        .is_empty()
    {
        return migrate_all_rollouts(store, paths, skipped_rollouts.as_slice()).await;
    }
    let skipped_file_names = skipped_rollout_file_names(store, skipped_rollouts.as_slice());
    let state = state_db
        .get_rollout_migration_state(LEGACY_TO_PAGINATED_MIGRATION_ID)
        .await
        .map_err(migration_error)?;

    if state.is_none() {
        return migrate_all_rollouts(store, paths, skipped_rollouts.as_slice()).await;
    }

    let last_checked_thread = state.and_then(|state| state.last_checked_thread);
    let lookback_created_at = last_checked_thread.as_ref().map(|cursor| {
        cursor
            .thread_created_at
            .saturating_sub(CURSOR_LOOKBACK_SECONDS)
    });
    let candidates = paths
        .iter()
        .filter(|path| {
            !plain_rollout_file_name(path)
                .is_some_and(|file_name| skipped_file_names.contains(&file_name))
                && thread_creation_cursor(path).is_none_or(|cursor| {
                    lookback_created_at.is_none_or(|lookback_created_at| {
                        cursor.thread_created_at >= lookback_created_at
                    })
                })
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(());
    }

    let mut unresolved = false;
    for path in candidates {
        match inspect_rollout_path(store, path).await? {
            StartupInspection::Paginated
            | StartupInspection::Compatible
            | StartupInspection::Skipped => {}
            StartupInspection::Legacy
            | StartupInspection::ReferenceBacked
            | StartupInspection::NeedsMigration => {
                return migrate_all_rollouts(store, paths, skipped_rollouts.as_slice()).await;
            }
            StartupInspection::Unresolved => unresolved = true,
        }
    }
    if unresolved {
        return Ok(());
    }

    advance_last_checked_thread(store, paths.as_slice()).await
}

async fn migrate_all_rollouts(
    store: &LocalThreadStore,
    paths_before_migration: Vec<PathBuf>,
    existing_skips: &[RolloutMigrationSkippedRollout],
) -> ThreadStoreResult<()> {
    let skipped_file_names = skipped_rollout_file_names(store, existing_skips);
    let pending_thread_ids = pending_migration_thread_ids(&store.config.codex_home).await?;
    let paths_to_migrate = paths_before_migration
        .iter()
        .filter(|path| {
            !plain_rollout_file_name(path)
                .is_some_and(|file_name| skipped_file_names.contains(&file_name))
                || codex_rollout::thread_id_from_path(path)
                    .is_some_and(|thread_id| pending_thread_ids.contains(&thread_id))
        })
        .cloned()
        .collect();
    let report = run_startup_migration(store, paths_to_migrate).await?;
    for outcome in &report.outcomes {
        update_skip_after_outcome(store, outcome).await?;
    }
    // Only mark the pre-migration snapshot; newer rollouts wait for the next startup check.
    advance_last_checked_thread(store, paths_before_migration.as_slice()).await
}

async fn retry_busy_rollouts(
    store: &LocalThreadStore,
    skipped_rollouts: &[RolloutMigrationSkippedRollout],
    discovered_paths: &[PathBuf],
) -> ThreadStoreResult<()> {
    let mut paths = Vec::new();
    let mut moved_skip_paths = Vec::new();
    for skipped_rollout in skipped_rollouts
        .iter()
        .filter(|skipped_rollout| skipped_rollout.skip_reason == BUSY_SKIP_REASON)
    {
        let stored_path = store.config.codex_home.join(&skipped_rollout.rollout_path);
        let path = if tokio::fs::try_exists(&stored_path)
            .await
            .map_err(migration_error)?
        {
            Some(stored_path.clone())
        } else {
            // Archive/unarchive moves one rollout between roots, while compression swaps between
            // its plain and compressed filenames. Match the plain basename across both.
            let file_name = plain_rollout_file_name(&stored_path);
            discovered_paths
                .iter()
                .find(|path| plain_rollout_file_name(path) == file_name)
                .cloned()
        };
        let Some(path) = path else {
            continue;
        };
        if path != stored_path {
            moved_skip_paths.push(skipped_rollout.rollout_path.as_str());
        }
        paths.push(path);
    }
    if paths.is_empty() {
        return Ok(());
    }
    let report = run_startup_migration(store, paths).await?;
    for moved_skip_path in moved_skip_paths {
        remove_skip(store, moved_skip_path).await?;
    }
    for outcome in &report.outcomes {
        update_skip_after_outcome(store, outcome).await?;
    }
    Ok(())
}

async fn run_startup_migration(
    store: &LocalThreadStore,
    paths: Vec<PathBuf>,
) -> ThreadStoreResult<RolloutMigrationReport> {
    loop {
        let Some(maintenance_guard) =
            codex_rollout::try_acquire_rollout_maintenance_lock(&store.config.codex_home)
                .map_err(migration_error)?
        else {
            tokio::time::sleep(MAINTENANCE_RETRY_DELAY).await;
            continue;
        };
        // Avoid counting expected compression contention as a failed migration run. The migration
        // path takes the real lock below, so retry if another maintainer wins this small gap.
        drop(maintenance_guard);
        match store
            .migrate_rollouts_with_progress_for_trigger(
                RolloutMigrationOptions {
                    mode: RolloutMigrationMode::Apply,
                    thread_ids: Vec::new(),
                    max_mib_per_second: None,
                },
                |_| {},
                RolloutMigrationTrigger::Startup,
                super::RolloutMigrationPaths::Known(paths.clone()),
            )
            .await
        {
            Err(ThreadStoreError::Conflict { .. }) => continue,
            result => return result,
        }
    }
}

async fn update_skip_after_outcome(
    store: &LocalThreadStore,
    outcome: &super::RolloutMigrationOutcome,
) -> ThreadStoreResult<()> {
    let relative_path = relative_rollout_path(store, &outcome.rollout_path);
    match outcome.status {
        RolloutMigrationStatus::Migrated | RolloutMigrationStatus::AlreadyPaginated => {
            remove_skip(store, relative_path.as_str()).await
        }
        RolloutMigrationStatus::SkippedEmpty => {
            record_current_skip(store, &outcome.rollout_path, EMPTY_SKIP_REASON).await
        }
        RolloutMigrationStatus::SkippedBusy => {
            record_current_skip(store, &outcome.rollout_path, BUSY_SKIP_REASON).await
        }
        RolloutMigrationStatus::Failed => {
            if outcome.thread_id.is_some_and(|thread_id| {
                migration_journal_path(&store.config.codex_home, thread_id).exists()
            }) {
                return Ok(());
            }
            record_current_skip(store, &outcome.rollout_path, FAILED_SKIP_REASON).await
        }
        RolloutMigrationStatus::Eligible => Ok(()),
    }
}

async fn inspect_rollout_path(
    store: &LocalThreadStore,
    path: &Path,
) -> ThreadStoreResult<StartupInspection> {
    let before = rollout_fingerprint(path).await?;
    match codex_rollout::read_session_meta_line(path).await {
        Ok(metadata) if metadata.meta.history_mode == ThreadHistoryMode::Legacy => {
            Ok(StartupInspection::Legacy)
        }
        Ok(_) => {
            if contains_convertible_rollout_reference(store.config.codex_home.as_path(), path)
                .await?
            {
                Ok(StartupInspection::ReferenceBacked)
            } else if has_leading_filtered_rollout_reference(path).await? {
                Ok(StartupInspection::Compatible)
            } else {
                Ok(StartupInspection::Paginated)
            }
        }
        Err(_) => {
            let after = rollout_fingerprint(path).await?;
            if before != after {
                return Ok(StartupInspection::Unresolved);
            }
            // The migration path re-reads empty files under the writer lock before deciding
            // whether they are terminally empty or just waiting for SessionMeta.
            if before.size_bytes == 0 {
                return Ok(StartupInspection::NeedsMigration);
            }
            record_skip(store, path, before, MALFORMED_SESSION_META_SKIP_REASON).await?;
            Ok(StartupInspection::Skipped)
        }
    }
}

async fn record_current_skip(
    store: &LocalThreadStore,
    path: &Path,
    skip_reason: &str,
) -> ThreadStoreResult<()> {
    // These fields remain in the generic schema, but background skips are permanent now. Keep
    // recording the best available fingerprint for humans inspecting SQLite.
    let fingerprint = rollout_fingerprint(path).await.unwrap_or_default();
    record_skip(store, path, fingerprint, skip_reason).await
}

async fn record_skip(
    store: &LocalThreadStore,
    path: &Path,
    fingerprint: RolloutFingerprint,
    skip_reason: &str,
) -> ThreadStoreResult<()> {
    let state_db = startup_state_db(store)?;
    let skipped_rollout = RolloutMigrationSkippedRollout {
        rollout_path: relative_rollout_path(store, path),
        rollout_size_bytes: fingerprint.size_bytes,
        rollout_modified_at_ns: fingerprint.modified_at_ns,
        skip_reason: skip_reason.to_string(),
    };
    state_db
        .record_rollout_migration_skip(LEGACY_TO_PAGINATED_MIGRATION_ID, &skipped_rollout)
        .await
        .map_err(migration_error)
}

async fn remove_skip(store: &LocalThreadStore, rollout_path: &str) -> ThreadStoreResult<()> {
    startup_state_db(store)?
        .remove_rollout_migration_skip(LEGACY_TO_PAGINATED_MIGRATION_ID, rollout_path)
        .await
        .map_err(migration_error)
}

async fn advance_last_checked_thread(
    store: &LocalThreadStore,
    paths: &[PathBuf],
) -> ThreadStoreResult<()> {
    let last_checked_thread = paths
        .iter()
        .filter_map(|path| thread_creation_cursor(path))
        .max();
    startup_state_db(store)?
        .advance_rollout_migration_state(
            LEGACY_TO_PAGINATED_MIGRATION_ID,
            last_checked_thread.as_ref(),
        )
        .await
        .map_err(migration_error)
}

fn startup_state_db(store: &LocalThreadStore) -> ThreadStoreResult<&StateDbHandle> {
    store
        .state_db
        .as_ref()
        .ok_or_else(|| migration_error("startup migration requires state db"))
}

fn thread_creation_cursor(path: &Path) -> Option<RolloutMigrationCursor> {
    let name = codex_rollout::RolloutFileName::parse(path.file_name()?.to_str()?)?;
    Some(RolloutMigrationCursor {
        thread_created_at: name.timestamp().unix_timestamp(),
        thread_id: name.thread_id().to_string(),
    })
}

fn relative_rollout_path(store: &LocalThreadStore, path: &Path) -> String {
    path.strip_prefix(&store.config.codex_home)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn skipped_rollout_file_names(
    store: &LocalThreadStore,
    skipped_rollouts: &[RolloutMigrationSkippedRollout],
) -> HashSet<OsString> {
    skipped_rollouts
        .iter()
        .filter_map(|skipped_rollout| {
            plain_rollout_file_name(&store.config.codex_home.join(&skipped_rollout.rollout_path))
        })
        .collect()
}

fn plain_rollout_file_name(path: &Path) -> Option<OsString> {
    codex_rollout::plain_rollout_path(path)
        .file_name()
        .map(std::ffi::OsStr::to_os_string)
}

async fn rollout_fingerprint(path: &Path) -> ThreadStoreResult<RolloutFingerprint> {
    let metadata = tokio::fs::metadata(path).await.map_err(migration_error)?;
    let size_bytes = i64::try_from(metadata.len()).map_err(migration_error)?;
    let modified_at_ns = metadata
        .modified()
        .map_err(migration_error)?
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(migration_error)?
        .as_nanos();
    let modified_at_ns = i64::try_from(modified_at_ns).map_err(migration_error)?;
    Ok(RolloutFingerprint {
        size_bytes,
        modified_at_ns,
    })
}

#[cfg(test)]
#[path = "startup_tests.rs"]
mod tests;
