mod archive_thread;
mod create_thread;
mod delete_thread;
mod fork_handoff;
mod goal_supervisor_history_repair;
mod goal_supervisor_runtime_repair;
#[cfg(test)]
mod goal_supervisor_runtime_repair_tests;
mod helpers;
mod list_threads;
mod live_writer;
mod model_context;
mod move_thread_to_section;
mod ordinal_recovery;
mod paginated_fork;
mod pending_thread_metadata;
mod projection_rebuild;
mod projects;
mod read_thread;
mod revert_thread;
mod rollout_migration;
// This lands before the reader PRs that consume the shared lineage resolver.
#[allow(dead_code)]
mod rollout_lineage;
mod search_threads;
mod segment;
mod thread_history;
mod thread_history_materialization;
mod thread_rollout_resolver;
mod thread_sections;
mod unarchive_thread;
mod update_thread_metadata;
mod writer_lock;

#[cfg(test)]
#[path = "pending_thread_metadata_tests.rs"]
mod pending_thread_metadata_tests;
#[cfg(test)]
mod test_support;

use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_protocol::RolloutId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::ResponseItemEnvelope;
use codex_rollout::RolloutRecorder;
use codex_rollout::StateDbHandle;
use codex_state::SqliteConfig;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio::sync::Mutex;
use tokio::sync::OnceCell;
use tokio::sync::OwnedMutexGuard;
use tokio::sync::OwnedRwLockReadGuard;
use tokio::sync::OwnedRwLockWriteGuard;
use tokio::sync::RwLock;

use crate::AppendThreadItemsParams;
use crate::ArchiveThreadParams;
use crate::ArchiveThreadsParams;
use crate::CreateProjectParams;
use crate::CreateThreadParams;
use crate::CreateThreadSectionParams;
use crate::CreatedProject;
use crate::DeleteThreadParams;
use crate::DeleteThreadSectionParams;
use crate::DeleteThreadsOutcome;
use crate::DeleteThreadsParams;
use crate::DeletedProject;
use crate::FreezeRolloutSegmentParams;
use crate::FrozenRolloutSegment;
use crate::ItemPage;
use crate::ListItemsParams;
use crate::ListProjectsParams;
use crate::ListThreadSectionsParams;
use crate::ListThreadsParams;
use crate::ListTimelineParams;
use crate::ListTurnsParams;
use crate::LoadThreadHistoryParams;
use crate::MoveProjectParams;
use crate::MoveThreadToSectionParams;
use crate::PersistContext;
use crate::PrepareForkParams;
use crate::PreparedFork;
use crate::ProjectMoveOutcome;
use crate::ReadThreadByRolloutPathParams;
use crate::ReadThreadParams;
use crate::ReadThreadsParams;
use crate::RenameThreadSectionParams;
use crate::ResumeThreadParams;
use crate::RevertThreadParams;
use crate::SearchThreadOccurrencesParams;
use crate::SearchThreadsParams;
use crate::SegmentCheckpointPersistenceOutcome;
use crate::StoredModelContext;
use crate::StoredProject;
use crate::StoredProjectsPage;
use crate::StoredThread;
use crate::StoredThreadHistory;
use crate::StoredThreadSection;
use crate::StoredThreadSectionsPage;
use crate::ThreadMetadataPatch;
use crate::ThreadOccurrenceSearchPage;
use crate::ThreadPage;
use crate::ThreadPersistenceMode;
use crate::ThreadSearchPage;
use crate::ThreadStore;
use crate::ThreadStoreError;
use crate::ThreadStoreFuture;
use crate::ThreadStoreResult;
use crate::TimelinePage;
use crate::TouchRootThreadRecencyParams;
use crate::TurnPage;
use crate::UpdateProjectParams;
use crate::UpdateThreadMetadataParams;
use crate::UpdatedProject;
use crate::local::writer_lock::WriterLockCoordinator;
use crate::local::writer_lock::WriterLockGuard;

pub use rollout_migration::RolloutMigrationAdditionalFreeSpace;
pub use rollout_migration::RolloutMigrationFailureReason;
pub use rollout_migration::RolloutMigrationHistoryBaseDependency;
pub use rollout_migration::RolloutMigrationLineagePredecessor;
pub use rollout_migration::RolloutMigrationLineageSource;
pub use rollout_migration::RolloutMigrationLineageTarget;
pub use rollout_migration::RolloutMigrationManifest;
pub use rollout_migration::RolloutMigrationManifestKind;
pub use rollout_migration::RolloutMigrationMode;
pub use rollout_migration::RolloutMigrationOptions;
pub use rollout_migration::RolloutMigrationOutcome;
pub use rollout_migration::RolloutMigrationProgress;
pub use rollout_migration::RolloutMigrationPublicationPhase;
pub use rollout_migration::RolloutMigrationReferenceBoundary;
pub use rollout_migration::RolloutMigrationReferenceDependency;
pub use rollout_migration::RolloutMigrationReport;
pub use rollout_migration::RolloutMigrationStatus;

/// Local filesystem/SQLite-backed implementation of [`ThreadStore`].
///
/// Local storage has two compatibility surfaces. Rollout JSONL files are the
/// durable replay format and remain readable without SQLite, including older
/// files that encode metadata in `SessionMeta` items and name-index entries.
/// The SQLite state DB, when available, is the queryable metadata index used by
/// list/read paths for fast lookup.
///
/// Live appends still write canonical JSONL history, but append-derived
/// metadata is observed above the store and applied through
/// [`ThreadStore::update_thread_metadata`]. This implementation applies that
/// patch literally to SQLite while keeping the JSONL/name-index compatibility
/// behavior needed for SQLite-less reads, repair, and old local rollout files.
#[derive(Clone)]
pub struct LocalThreadStore {
    pub(super) config: LocalThreadStoreConfig,
    live_recorders: Arc<Mutex<HashMap<ThreadId, LiveRecorderEntry>>>,
    pending_thread_metadata: pending_thread_metadata::PendingThreadMetadataRegistry,
    live_writer_locks: Arc<LiveWriterLocks>,
    writer_lock_coordinator: Arc<WriterLockCoordinator>,
    state_db: Option<StateDbHandle>,
    thread_history_db: Arc<OnceCell<sqlx::SqlitePool>>,
    projection_rebuilds: Arc<StdMutex<HashSet<ThreadId>>>,
    projection_rebuild_schedules: Arc<StdMutex<projection_rebuild::ScheduledProjectionRebuilds>>,
    /// Bounds complete-lineage projection repair to one disk- and memory-intensive scan.
    projection_rebuild_gate: Arc<Mutex<()>>,
    /// Coordinates first-request rollout conversion with thread-specific reads.
    rollout_migration_coordinator: Arc<rollout_migration::StartupMigrationCoordinator>,
}

struct LiveRecorderEntry {
    recorder: RolloutRecorder,
    /// Reopens the committed stable rollout before the next live operation.
    ///
    /// Segment rotation installs this before shutting down the previous recorder so cancellation
    /// or a transient post-commit reopen failure cannot reuse a recorder whose task has exited.
    recovery: Option<LiveRecorderRecovery>,
    // Rollout projection rows are keyed by immutable rollout ID, not the stable thread ID used
    // to find this live writer.
    rollout_id: ThreadId,
    // Local rollout files are materialized lazily, but metadata updates can arrive before the
    // canonical SessionMeta is durable. Retain the mode captured when live persistence was opened
    // so missing SQLite rows can still be seeded.
    history_mode: ThreadHistoryMode,
    writer_lock: WriterLockGuard,
    /// Whether the recorder may materialize its canonical in-memory queue.
    persistence_mode: ThreadPersistenceMode,
    // Legacy rollout records do not contain canonical projected items or ordinals.
    // Keep the reducer across appends and physical rotation so indexed visible
    // history retains the same generated item and turn identities as JSONL replay.
    legacy_history_builder: Arc<Mutex<ThreadHistoryBuilder>>,
    // Resuming a recorder does not restore the legacy reducer. Do not extend an
    // existing projection with generated item identities from a fresh reducer.
    legacy_history_projection_enabled: bool,
    // A resumed legacy writer rebuilds reducer state from complete canonical
    // lineage before its first append, not from bounded model context.
    legacy_history_builder_needs_rebuild: bool,
}

/// Information needed to reopen a live recorder after active-rollout replacement.
#[derive(Clone)]
struct LiveRecorderRecovery {
    config: codex_rollout::RolloutConfig,
    rollout_path: PathBuf,
}

/// Exclusive ownership of the mutable rollout files for a set of stable thread IDs.
///
/// Callers acquire the in-process mutexes first and the cross-process writer locks second, both in
/// stable thread-ID order. A same-store live recorder already owns its cross-process lock, so its
/// entry supplies that half of the reservation until the in-process mutex is released.
struct RolloutWriterReservation {
    store_identity: usize,
    thread_ids: Vec<ThreadId>,
    _in_process_guards: Vec<(ThreadId, OwnedMutexGuard<()>)>,
    cross_process_guards: Vec<(ThreadId, WriterLockGuard)>,
}

impl RolloutWriterReservation {
    fn contains(&self, thread_id: ThreadId) -> bool {
        self.thread_ids.contains(&thread_id)
    }

    fn take_cross_process_guard(&mut self, thread_id: ThreadId) -> Option<WriterLockGuard> {
        let index = self
            .cross_process_guards
            .iter()
            .position(|(candidate, _)| *candidate == thread_id)?;
        Some(self.cross_process_guards.swap_remove(index).1)
    }
}

#[derive(Default)]
struct LiveWriterLocks {
    // Keep per-thread locks after a writer goes idle. Removing one while another caller is about
    // to acquire it could let two operations for the same thread run at once.
    by_thread: Mutex<HashMap<ThreadId, Arc<ThreadCoordination>>>,
}

#[derive(Default)]
struct ThreadCoordination {
    // Serialize writes and capture consistent fork snapshots.
    writer: Arc<Mutex<()>>,
    // Forks hold a shared lease until their child reference is durable; deletion, archive, and
    // unarchive require exclusive access. Keeping this separate from `writer` lets the source
    // accept writes during child initialization, including MCP startup that can take 30 seconds.
    // Operations that need both locks must acquire `lifecycle` before `writer`.
    lifecycle: Arc<RwLock<()>>,
}

impl LiveWriterLocks {
    async fn coordination(&self, thread_id: ThreadId) -> Arc<ThreadCoordination> {
        self.by_thread
            .lock()
            .await
            .entry(thread_id)
            .or_default()
            .clone()
    }

    async fn lock(&self, thread_id: ThreadId) -> OwnedMutexGuard<()> {
        self.coordination(thread_id)
            .await
            .writer
            .clone()
            .lock_owned()
            .await
    }

    async fn try_lock(&self, thread_id: ThreadId) -> ThreadStoreResult<OwnedMutexGuard<()>> {
        self.coordination(thread_id)
            .await
            .writer
            .clone()
            .try_lock_owned()
            .map_err(|_| ThreadStoreError::Conflict {
                message: format!("thread {thread_id} has an active rollout operation"),
            })
    }

    async fn reserve_lifecycle(&self, thread_id: ThreadId) -> OwnedRwLockReadGuard<()> {
        self.coordination(thread_id)
            .await
            .lifecycle
            .clone()
            .read_owned()
            .await
    }

    async fn lock_lifecycle(&self, thread_id: ThreadId) -> OwnedRwLockWriteGuard<()> {
        self.coordination(thread_id)
            .await
            .lifecycle
            .clone()
            .write_owned()
            .await
    }
}

/// Process-scoped configuration for local thread storage.
///
/// This describes where local storage lives. New-thread rollout metadata such
/// as cwd, provider, and memory mode is supplied when live persistence is opened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalThreadStoreConfig {
    pub codex_home: PathBuf,
    pub sqlite: SqliteConfig,
    /// Provider used only when older local metadata does not contain one.
    pub default_model_provider_id: String,
}

impl LocalThreadStoreConfig {
    pub fn from_config(config: &impl codex_rollout::RolloutConfigView) -> Self {
        Self {
            codex_home: config.codex_home().to_path_buf(),
            sqlite: config.sqlite_config().clone(),
            default_model_provider_id: config.model_provider_id().to_string(),
        }
    }
}

impl std::fmt::Debug for LocalThreadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalThreadStore")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl LocalThreadStore {
    /// Create a local store using an already initialized state DB handle.
    pub fn new(config: LocalThreadStoreConfig, state_db: Option<StateDbHandle>) -> Self {
        let writer_lock_coordinator = Arc::new(WriterLockCoordinator::new(&config.codex_home));
        Self {
            config,
            live_recorders: Arc::new(Mutex::new(HashMap::new())),
            pending_thread_metadata:
                pending_thread_metadata::PendingThreadMetadataRegistry::default(),
            live_writer_locks: Arc::new(LiveWriterLocks::default()),
            writer_lock_coordinator,
            state_db,
            thread_history_db: Arc::new(OnceCell::new()),
            projection_rebuilds: Arc::new(StdMutex::new(HashSet::new())),
            projection_rebuild_schedules: Arc::new(StdMutex::new(HashMap::new())),
            projection_rebuild_gate: Arc::new(Mutex::new(())),
            rollout_migration_coordinator: Arc::new(
                rollout_migration::StartupMigrationCoordinator::default(),
            ),
        }
    }

    /// Return the state DB handle used by local rollout writers.
    pub async fn state_db(&self) -> Option<StateDbHandle> {
        self.state_db.clone()
    }

    /// Returns whether the projected visible history can satisfy paginated reads.
    pub async fn has_history_projection(&self, thread_id: ThreadId) -> ThreadStoreResult<bool> {
        let Some(resolved) =
            thread_rollout_resolver::resolve_current_including_archived(self, thread_id).await?
        else {
            return Ok(false);
        };
        if thread_history::has_complete_root_projection_for_resolved(
            self,
            thread_id,
            resolved.clone(),
        )
        .await?
        {
            return Ok(true);
        }
        let physical_history_mode = match resolved.authenticated_session_meta.as_ref() {
            Some(session_meta) => session_meta.history_mode,
            None => {
                codex_rollout::read_session_meta_line(resolved.path.as_path())
                    .await
                    .map_err(|error| ThreadStoreError::Internal {
                        message: format!(
                            "failed to read selected rollout metadata {}: {error}",
                            resolved.path.display()
                        ),
                    })?
                    .meta
                    .history_mode
            }
        };
        if physical_history_mode != ThreadHistoryMode::Paginated {
            return Ok(false);
        }
        let Some(projection_state) =
            thread_history::projection_state(self, resolved.rollout_id).await?
        else {
            return Ok(false);
        };
        if !projection_state.lineage_complete {
            return Ok(false);
        }
        if resolved.path.extension().and_then(|value| value.to_str()) == Some("jsonl")
            && tokio::fs::metadata(resolved.path.as_path())
                .await
                .map_err(|error| ThreadStoreError::Internal {
                    message: format!(
                        "failed to read selected rollout metadata {}: {error}",
                        resolved.path.display()
                    ),
                })?
                .len()
                != projection_state.next_byte_offset
        {
            return Ok(false);
        }
        // A completed or interrupted turn can legitimately have no visible items. The
        // checkpoint, not the newest turn's presentation, certifies projection completeness.
        Ok(true)
    }

    /// Returns whether the already-validated Paginated rollout has a complete projection at the
    /// supplied durable byte boundary.
    pub(super) async fn has_complete_history_projection_at(
        &self,
        rollout_id: RolloutId,
        end_byte_offset: u64,
    ) -> ThreadStoreResult<bool> {
        let Some(projection_state) = thread_history::projection_state(self, rollout_id).await?
        else {
            return Ok(false);
        };
        Ok(projection_state.lineage_complete
            && projection_state.next_byte_offset == end_byte_offset)
    }

    /// Rebuilds one complete same-thread Paginated projection and publishes it atomically.
    ///
    /// Callers that serve interactive requests should use
    /// [`Self::schedule_history_projection_rebuild`] instead. This method waits for the complete
    /// lineage scan and exists for startup maintenance, explicit repair, and deterministic tests.
    pub async fn rebuild_history_projection(&self, thread_id: ThreadId) -> ThreadStoreResult<bool> {
        projection_rebuild::rebuild_waiting(self, thread_id).await
    }

    /// Starts a single delayed projection rebuild without blocking the current request.
    pub async fn schedule_history_projection_rebuild(&self, thread_id: ThreadId) {
        projection_rebuild::schedule(self.clone(), thread_id).await;
    }

    /// Returns the exact durable ordinal and byte offset of a projected thread.
    pub async fn projected_history_position(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<Option<HistoryPosition>> {
        let Some(resolved) =
            thread_rollout_resolver::resolve_current_including_archived(self, thread_id).await?
        else {
            return Ok(None);
        };
        Ok(thread_history::projection_state(self, resolved.rollout_id)
            .await?
            .map(|state| HistoryPosition {
                thread_id: resolved.rollout_id,
                end_ordinal_exclusive: state.next_ordinal,
                end_byte_offset: state.next_byte_offset,
            }))
    }

    async fn thread_history_db(&self) -> ThreadStoreResult<&sqlx::SqlitePool> {
        if self.state_db.is_none() {
            return Err(ThreadStoreError::Unsupported {
                operation: "paginated_history",
            });
        }
        self.thread_history_db
            .get_or_try_init(|| async {
                let pool = codex_state::open_thread_history_db(&self.config.sqlite)
                    .await
                    .map_err(|err| ThreadStoreError::Internal {
                        message: format!("failed to open thread history database: {err}"),
                    })?;
                thread_history::ensure_projection_integrity_triggers(&pool).await?;
                Ok(pool)
            })
            .await
    }

    /// Read a local rollout-backed thread by path.
    pub async fn read_thread_by_rollout_path(
        &self,
        rollout_path: PathBuf,
        include_archived: bool,
        include_history: bool,
    ) -> ThreadStoreResult<StoredThread> {
        read_thread::read_thread_by_rollout_path(
            self,
            rollout_path,
            include_archived,
            include_history,
        )
        .await
    }

    /// Return the live local rollout path for legacy local-only code paths.
    pub async fn live_rollout_path(&self, thread_id: ThreadId) -> ThreadStoreResult<PathBuf> {
        live_writer::rollout_path(self, thread_id).await
    }

    /// Freezes the thread's current prefix and installs a reference-backed continuation.
    pub async fn freeze_thread_segment(
        &self,
        thread_id: ThreadId,
        params: FreezeRolloutSegmentParams,
    ) -> ThreadStoreResult<FrozenRolloutSegment> {
        let Some(source) =
            thread_rollout_resolver::resolve_current_including_archived(self, thread_id).await?
        else {
            // A deferred live recorder is intentionally pathless until the freeze operation makes
            // it durable. The segment implementation already owns that transition; there is no
            // persisted history to inspect for the compatibility repair beforehand.
            return segment::freeze_thread_segment(self, thread_id, params).await;
        };
        // Rotation stabilizes the active rollout's bounded reference window. History-base
        // ancestors are repaired by operations that replay them; traversing the complete lineage
        // here makes repeated rotations rescan every older immutable segment.
        let history_access = goal_supervisor_runtime_repair::repair_selected_history_before_access(
            self,
            thread_id,
            source.path.as_path(),
            goal_supervisor_runtime_repair::RepairAccess::Recent,
        )
        .await?;
        match history_access.writer_reservation() {
            Some(reservation) => {
                segment::freeze_thread_segment_reserved(
                    self,
                    thread_id,
                    params,
                    /*expected_rollout_id*/ None,
                    reservation,
                )
                .await
            }
            None => segment::freeze_thread_segment(self, thread_id, params).await,
        }
    }

    /// Freezes a thread only while the expected physical rollout remains selected.
    pub async fn freeze_thread_segment_for_rollout(
        &self,
        thread_id: ThreadId,
        expected_rollout_id: codex_protocol::RolloutId,
        params: FreezeRolloutSegmentParams,
    ) -> ThreadStoreResult<FrozenRolloutSegment> {
        let source = thread_rollout_resolver::resolve_current_including_archived(self, thread_id)
            .await?
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        if source.rollout_id != expected_rollout_id {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!(
                    "rollout path does not select the current rollout for thread {thread_id}"
                ),
            });
        }
        let history_access = goal_supervisor_runtime_repair::repair_selected_history_before_access(
            self,
            thread_id,
            source.path.as_path(),
            goal_supervisor_runtime_repair::RepairAccess::Recent,
        )
        .await?;
        match history_access.writer_reservation() {
            Some(reservation) => {
                segment::freeze_thread_segment_reserved(
                    self,
                    thread_id,
                    params,
                    Some(expected_rollout_id),
                    reservation,
                )
                .await
            }
            None => {
                segment::freeze_thread_segment_for_rollout(
                    self,
                    thread_id,
                    params,
                    Some(expected_rollout_id),
                )
                .await
            }
        }
    }

    /// Freezes a paginated fork without reading history excluded from its response.
    pub async fn prepare_fork_without_response_history(
        &self,
        params: PrepareForkParams,
    ) -> ThreadStoreResult<PreparedFork> {
        self.await_automatic_rollout_migration(params.thread_id)
            .await?;
        paginated_fork::prepare_without_response_history(self, params).await
    }

    /// Prepares the selected paginated fork without reading history excluded from its response.
    /// Ephemeral context-only forks do not freeze the source rollout.
    pub async fn prepare_fork_without_response_history_for_rollout(
        &self,
        params: PrepareForkParams,
        expected_rollout_id: codex_protocol::RolloutId,
        ephemeral_context_only: bool,
    ) -> ThreadStoreResult<PreparedFork> {
        self.await_automatic_rollout_migration(params.thread_id)
            .await?;
        paginated_fork::prepare_without_response_history_for_rollout(
            self,
            params,
            expected_rollout_id,
            ephemeral_context_only,
        )
        .await
    }

    /// Prepares a latest fork using a verified complete, in-memory source model context.
    pub async fn prepare_fork_with_model_context(
        &self,
        params: PrepareForkParams,
        model_context: Arc<Vec<ResponseItemEnvelope>>,
        expected_position: HistoryPosition,
    ) -> ThreadStoreResult<PreparedFork> {
        self.await_automatic_rollout_migration(params.thread_id)
            .await?;
        paginated_fork::prepare_with_model_context(self, params, model_context, expected_position)
            .await
    }

    /// Prepares the selected latest fork using a verified in-memory source model context.
    pub async fn prepare_fork_with_model_context_for_rollout(
        &self,
        params: PrepareForkParams,
        model_context: Arc<Vec<ResponseItemEnvelope>>,
        expected_position: HistoryPosition,
        expected_rollout_id: codex_protocol::RolloutId,
    ) -> ThreadStoreResult<PreparedFork> {
        self.await_automatic_rollout_migration(params.thread_id)
            .await?;
        paginated_fork::prepare_with_model_context_for_rollout(
            self,
            params,
            model_context,
            expected_position,
            expected_rollout_id,
        )
        .await
    }

    /// Prepares a latest side fork without materializing projected response turns.
    pub async fn prepare_fork_without_response_history_with_model_context(
        &self,
        params: PrepareForkParams,
        model_context: Arc<Vec<ResponseItemEnvelope>>,
        expected_position: HistoryPosition,
    ) -> ThreadStoreResult<PreparedFork> {
        self.await_automatic_rollout_migration(params.thread_id)
            .await?;
        paginated_fork::prepare_without_response_history_with_model_context(
            self,
            params,
            model_context,
            expected_position,
        )
        .await
    }

    /// Prepares the selected latest side fork without materializing projected response turns.
    /// Ephemeral context-only forks do not freeze the source rollout.
    pub async fn prepare_fork_without_response_history_with_model_context_for_rollout(
        &self,
        params: PrepareForkParams,
        model_context: Arc<Vec<ResponseItemEnvelope>>,
        expected_position: HistoryPosition,
        expected_rollout_id: codex_protocol::RolloutId,
        ephemeral_context_only: bool,
    ) -> ThreadStoreResult<PreparedFork> {
        self.await_automatic_rollout_migration(params.thread_id)
            .await?;
        paginated_fork::prepare_without_response_history_with_model_context_for_rollout(
            self,
            params,
            model_context,
            expected_position,
            expected_rollout_id,
            ephemeral_context_only,
        )
        .await
    }

    /// Discard a derived legacy projection without modifying canonical rollout history.
    pub async fn discard_segmented_legacy_projection(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<()> {
        let resolved = thread_rollout_resolver::resolve_current_including_archived(self, thread_id)
            .await?
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        thread_history::delete_thread(self, resolved.rollout_id).await
    }

    /// Returns the selected physical rollout ID while callers hold any required lifecycle guard.
    pub async fn current_rollout_id(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<Option<codex_protocol::RolloutId>> {
        Ok(
            thread_rollout_resolver::resolve_current_including_archived(self, thread_id)
                .await?
                .map(|resolved| resolved.rollout_id),
        )
    }

    /// Prepares a fork only if `expected_rollout_id` remains selected under the lifecycle guard.
    pub async fn prepare_fork_for_rollout(
        &self,
        params: PrepareForkParams,
        expected_rollout_id: codex_protocol::RolloutId,
    ) -> ThreadStoreResult<PreparedFork> {
        self.await_automatic_rollout_migration(params.thread_id)
            .await?;
        paginated_fork::prepare_for_rollout(self, params, expected_rollout_id).await
    }

    /// Prevents deletion, archive, and unarchive while a new child reference is initialized.
    pub async fn reserve_thread_lifecycle(
        &self,
        thread_id: ThreadId,
    ) -> crate::ThreadLifecycleReservation {
        crate::ThreadLifecycleReservation::new(
            self.live_writer_locks.reserve_lifecycle(thread_id).await,
        )
    }

    /// Keeps a prepared source alive across processes without transferring write authority.
    /// The caller must already retain its `PreparedFork` lifecycle reservation. Only the process
    /// owning the live source may admit fork readers; ordinary writers still require exclusivity.
    pub async fn reserve_exported_fork(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<impl std::fmt::Debug + Send + 'static> {
        let _writer = self.live_writer_locks.lock(thread_id).await;
        let (recorder, _, _, _) = live_writer::live_writer_parts(self, thread_id).await?;
        // Drain even commands left behind by a cancelled append before a fallible conversion.
        recorder
            .flush()
            .await
            .map_err(|error| ThreadStoreError::Internal {
                message: format!(
                    "failed to flush fork source before sharing its reservation: {error}"
                ),
            })?;
        let mut recorders = self.live_recorders.lock().await;
        let entry = recorders
            .get_mut(&thread_id)
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
        let result = entry.writer_lock.share_for_fork();
        if !entry.writer_lock.is_reserved() {
            // A failed lock conversion cannot leave an active recorder authorized to write.
            recorders.remove(&thread_id);
        }
        result?;
        self.writer_lock_coordinator.acquire_fork_reader(thread_id)
    }

    /// Acquires independent deletion protection before validating an imported immutable snapshot.
    /// The returned lease must survive until the new child's history reference has been flushed.
    pub async fn reserve_imported_fork(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<impl std::fmt::Debug + Send + 'static> {
        let lifecycle = self.reserve_thread_lifecycle(thread_id).await;
        let reader = self
            .writer_lock_coordinator
            .acquire_fork_reader(thread_id)?;
        Ok((lifecycle, reader))
    }

    pub(crate) async fn live_persistence_mode(
        &self,
        thread_id: ThreadId,
    ) -> Option<ThreadPersistenceMode> {
        self.live_recorders
            .lock()
            .await
            .get(&thread_id)
            .map(|entry| entry.persistence_mode)
    }

    pub(super) async fn ensure_live_recorder_absent(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<()> {
        if self.live_recorders.lock().await.contains_key(&thread_id) {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!("thread {thread_id} already has a live local writer"),
            });
        }
        Ok(())
    }

    async fn acquire_writer_locks(
        &self,
        thread_ids: &[ThreadId],
    ) -> ThreadStoreResult<Vec<WriterLockGuard>> {
        let mut writer_locks = Vec::with_capacity(thread_ids.len());
        for &thread_id in thread_ids {
            if self.live_recorders.lock().await.contains_key(&thread_id) {
                continue;
            }
            writer_locks.push(self.writer_lock_coordinator.acquire(thread_id)?);
        }
        Ok(writer_locks)
    }

    async fn acquire_destructive_writer_locks(
        &self,
        thread_ids: &[ThreadId],
    ) -> ThreadStoreResult<Vec<WriterLockGuard>> {
        for &thread_id in thread_ids {
            let recorder = match live_writer::live_writer_parts(self, thread_id).await {
                Ok((recorder, _, _, _)) => recorder,
                Err(ThreadStoreError::ThreadNotFound { thread_id: _ }) => continue,
                Err(error) => return Err(error),
            };
            recorder
                .flush()
                .await
                .map_err(|error| ThreadStoreError::Internal {
                    message: format!("failed to flush thread before exclusive deletion: {error}"),
                })?;
            let mut recorders = self.live_recorders.lock().await;
            let entry = recorders
                .get_mut(&thread_id)
                .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
            let result = entry.writer_lock.require_exclusive();
            if !entry.writer_lock.is_reserved() {
                recorders.remove(&thread_id);
            }
            result?;
        }
        self.acquire_writer_locks(thread_ids).await
    }

    async fn reserve_rollout_writers(
        &self,
        thread_ids: &[ThreadId],
    ) -> ThreadStoreResult<RolloutWriterReservation> {
        let mut thread_ids = thread_ids.to_vec();
        thread_ids.sort_unstable_by_key(ThreadId::to_string);
        thread_ids.dedup();
        let mut in_process_guards = Vec::with_capacity(thread_ids.len());
        for &thread_id in &thread_ids {
            in_process_guards.push((thread_id, self.live_writer_locks.lock(thread_id).await));
        }
        let mut cross_process_guards = Vec::with_capacity(thread_ids.len());
        for &thread_id in &thread_ids {
            if self.live_recorders.lock().await.contains_key(&thread_id) {
                continue;
            }
            cross_process_guards
                .push((thread_id, self.writer_lock_coordinator.acquire(thread_id)?));
        }
        Ok(RolloutWriterReservation {
            store_identity: Arc::as_ptr(&self.live_writer_locks) as usize,
            thread_ids,
            _in_process_guards: in_process_guards,
            cross_process_guards,
        })
    }

    /// Reserve newly discovered ancestors without waiting while another thread is already locked.
    async fn try_reserve_rollout_writers(
        &self,
        thread_ids: &[ThreadId],
    ) -> ThreadStoreResult<RolloutWriterReservation> {
        let mut thread_ids = thread_ids.to_vec();
        thread_ids.sort_unstable_by_key(ThreadId::to_string);
        thread_ids.dedup();
        let mut in_process_guards = Vec::with_capacity(thread_ids.len());
        for &thread_id in &thread_ids {
            in_process_guards.push((thread_id, self.live_writer_locks.try_lock(thread_id).await?));
        }
        let mut cross_process_guards = Vec::with_capacity(thread_ids.len());
        for &thread_id in &thread_ids {
            let recorder = self
                .live_recorders
                .lock()
                .await
                .get(&thread_id)
                .map(|entry| entry.recorder.clone());
            if let Some(recorder) = recorder {
                recorder
                    .flush()
                    .await
                    .map_err(|error| ThreadStoreError::Internal {
                        message: format!("failed to flush reserved rollout: {error}"),
                    })?;
                continue;
            }
            cross_process_guards
                .push((thread_id, self.writer_lock_coordinator.acquire(thread_id)?));
        }
        Ok(RolloutWriterReservation {
            store_identity: Arc::as_ptr(&self.live_writer_locks) as usize,
            thread_ids,
            _in_process_guards: in_process_guards,
            cross_process_guards,
        })
    }

    async fn insert_live_recorder(
        &self,
        thread_id: ThreadId,
        recorder: RolloutRecorder,
        rollout_id: ThreadId,
        history_mode: ThreadHistoryMode,
        writer_lock: WriterLockGuard,
        persistence_mode: ThreadPersistenceMode,
    ) -> ThreadStoreResult<()> {
        match self.live_recorders.lock().await.entry(thread_id) {
            Entry::Occupied(entry) => Err(ThreadStoreError::InvalidRequest {
                message: format!("thread {} already has a live local writer", entry.key()),
            }),
            Entry::Vacant(entry) => {
                entry.insert(LiveRecorderEntry {
                    recorder,
                    recovery: None,
                    rollout_id,
                    history_mode,
                    writer_lock,
                    persistence_mode,
                    legacy_history_builder: Arc::new(Mutex::new(ThreadHistoryBuilder::new())),
                    legacy_history_projection_enabled: true,
                    legacy_history_builder_needs_rebuild: false,
                });
                Ok(())
            }
        }
    }

    async fn load_history(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreResult<StoredThreadHistory> {
        if let Ok(rollout_path) = live_writer::rollout_path(self, params.thread_id).await {
            if !params.include_archived
                && helpers::rollout_path_is_archived(
                    self.config.codex_home.as_path(),
                    rollout_path.as_path(),
                )
            {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("thread {} is archived", params.thread_id),
                });
            }
            return read_thread::read_thread_by_rollout_path(
                self,
                rollout_path,
                /*include_archived*/ true,
                /*include_history*/ true,
            )
            .await?
            .history
            .ok_or_else(|| ThreadStoreError::Internal {
                message: format!("failed to load history for thread {}", params.thread_id),
            });
        }

        read_thread::read_thread(
            self,
            ReadThreadParams {
                thread_id: params.thread_id,
                include_archived: params.include_archived,
                include_history: true,
            },
        )
        .await?
        .history
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!("failed to load history for thread {}", params.thread_id),
        })
    }

    async fn read_thread_by_rollout_path_params(
        &self,
        params: ReadThreadByRolloutPathParams,
    ) -> ThreadStoreResult<StoredThread> {
        if params.include_history {
            let metadata = read_thread::read_thread_by_rollout_path(
                self,
                params.rollout_path.clone(),
                params.include_archived,
                /*include_history*/ false,
            )
            .await?;
            if let Some(selected) = thread_rollout_resolver::resolve_current_including_archived(
                self,
                metadata.thread_id,
            )
            .await?
                && let Some(requested_path) = metadata.rollout_path.as_deref()
                && codex_rollout::rollout_paths_match(requested_path, &selected.path).await
            {
                // Migration can change both the selected filename and its history format. Read
                // the resume snapshot after conversion, not before opening the migrated writer.
                self.await_automatic_rollout_migration(metadata.thread_id)
                    .await?;
                return read_thread::read_thread(
                    self,
                    ReadThreadParams {
                        thread_id: metadata.thread_id,
                        include_archived: params.include_archived,
                        include_history: true,
                    },
                )
                .await;
            }
        }
        read_thread::read_thread_by_rollout_path(
            self,
            params.rollout_path,
            params.include_archived,
            params.include_history,
        )
        .await
    }

    /// Lists projection-backed turns without enabling app-server routing yet.
    pub async fn list_turns(&self, params: ListTurnsParams) -> ThreadStoreResult<TurnPage> {
        self.await_automatic_rollout_migration(params.thread_id)
            .await?;
        thread_history::list_turns(self, params).await
    }

    /// Read indexed segmented legacy turns without changing the legacy cursor.
    pub async fn list_segmented_legacy_turns(
        &self,
        params: ListTurnsParams,
    ) -> ThreadStoreResult<Option<TurnPage>> {
        self.await_automatic_rollout_migration(params.thread_id)
            .await?;
        thread_history::list_segmented_legacy_turns(self, params).await
    }

    /// Read an existing legacy index without backfilling history during initial navigation.
    pub async fn list_existing_segmented_legacy_turns(
        &self,
        params: ListTurnsParams,
    ) -> ThreadStoreResult<Option<TurnPage>> {
        self.await_automatic_rollout_migration(params.thread_id)
            .await?;
        thread_history::list_existing_segmented_legacy_turns(self, params).await
    }

    /// Reports whether a complete legacy index exists without triggering a history backfill.
    pub async fn has_complete_segmented_legacy_projection(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<bool> {
        thread_history::has_complete_segmented_legacy_projection(self, thread_id).await
    }

    /// Lists projection-backed items without enabling app-server routing yet.
    pub async fn list_items(&self, params: ListItemsParams) -> ThreadStoreResult<ItemPage> {
        self.await_automatic_rollout_migration(params.thread_id)
            .await?;
        thread_history::list_items(self, params).await
    }

    /// Lists bounded ordinary and realtime items from the rollout projection.
    pub async fn list_timeline(
        &self,
        params: ListTimelineParams,
    ) -> ThreadStoreResult<TimelinePage> {
        thread_history::list_timeline(self, params).await
    }

    /// Hydrate indexed segmented legacy items without exposing paginated history.
    pub async fn list_segmented_legacy_items(
        &self,
        params: ListItemsParams,
    ) -> ThreadStoreResult<Option<ItemPage>> {
        self.await_automatic_rollout_migration(params.thread_id)
            .await?;
        thread_history::list_segmented_legacy_items(self, params).await
    }

    /// Searches projection-backed visible messages within one paginated thread.
    pub async fn search_thread_occurrences(
        &self,
        params: SearchThreadOccurrencesParams,
    ) -> ThreadStoreResult<ThreadOccurrenceSearchPage> {
        thread_history::search_thread_occurrences(self, params).await
    }
}

impl ThreadStore for LocalThreadStore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn create_thread(&self, params: CreateThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { live_writer::create_thread(self, params).await })
    }

    fn stage_pending_thread_metadata(
        &self,
        thread_id: ThreadId,
        patch: ThreadMetadataPatch,
    ) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            if self.state_db.is_none() {
                return Err(ThreadStoreError::InvalidRequest {
                    message: "pending thread metadata requires a state db".to_string(),
                });
            }
            if patch.rollout_path.is_some() {
                return Err(ThreadStoreError::InvalidRequest {
                    message: "pending thread metadata cannot set rollout_path".to_string(),
                });
            }
            self.pending_thread_metadata.stage(thread_id, patch).await
        })
    }

    fn remove_pending_thread_metadata(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            self.pending_thread_metadata.remove(thread_id).await;
            Ok(())
        })
    }

    fn resume_thread(&self, params: ResumeThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            self.await_automatic_rollout_migration(params.thread_id)
                .await?;
            live_writer::resume_thread(self, params).await
        })
    }

    fn append_items(&self, params: AppendThreadItemsParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { live_writer::append_items(self, params).await })
    }

    fn persist_segment_checkpoint(
        &self,
        thread_id: ThreadId,
        params: FreezeRolloutSegmentParams,
    ) -> Pin<Box<dyn Future<Output = SegmentCheckpointPersistenceOutcome> + Send + '_>> {
        Box::pin(async move { segment::persist_segment_checkpoint(self, thread_id, params).await })
    }

    fn persist_thread(
        &self,
        thread_id: ThreadId,
        _context: PersistContext,
    ) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { live_writer::persist_thread(self, thread_id).await })
    }

    fn flush_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { live_writer::flush_thread(self, thread_id).await })
    }

    fn shutdown_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { live_writer::shutdown_thread(self, thread_id).await })
    }

    fn discard_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { live_writer::discard_thread(self, thread_id).await })
    }

    fn load_history(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredThreadHistory> {
        Box::pin(async move {
            self.await_automatic_rollout_migration(params.thread_id)
                .await?;
            LocalThreadStore::load_history(self, params).await
        })
    }

    fn load_latest_model_context(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredModelContext> {
        Box::pin(async move {
            self.await_automatic_rollout_migration(params.thread_id)
                .await?;
            model_context::load_latest_model_context(self, params).await
        })
    }

    fn prepare_fork(&self, params: PrepareForkParams) -> ThreadStoreFuture<'_, PreparedFork> {
        Box::pin(async move {
            self.await_automatic_rollout_migration(params.thread_id)
                .await?;
            paginated_fork::prepare(self, params).await
        })
    }

    fn revert_thread(&self, params: RevertThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            self.await_automatic_rollout_migration(params.thread_id)
                .await?;
            revert_thread::revert(self, params).await
        })
    }

    fn read_thread(&self, params: ReadThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async move {
            self.await_automatic_rollout_migration(params.thread_id)
                .await?;
            read_thread::read_thread(self, params).await
        })
    }

    fn read_threads(&self, params: ReadThreadsParams) -> ThreadStoreFuture<'_, Vec<StoredThread>> {
        Box::pin(async move { read_thread::read_threads(self, params).await })
    }

    fn read_thread_by_rollout_path(
        &self,
        params: ReadThreadByRolloutPathParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(LocalThreadStore::read_thread_by_rollout_path_params(
            self, params,
        ))
    }

    fn list_threads(&self, params: ListThreadsParams) -> ThreadStoreFuture<'_, ThreadPage> {
        Box::pin(async move { list_threads::list_threads(self, params).await })
    }

    fn supports_thread_sections(&self) -> bool {
        self.state_db.is_some()
    }

    fn list_thread_sections(
        &self,
        params: ListThreadSectionsParams,
    ) -> ThreadStoreFuture<'_, StoredThreadSectionsPage> {
        Box::pin(async move { thread_sections::list_thread_sections(self, params).await })
    }

    fn create_thread_section(
        &self,
        params: CreateThreadSectionParams,
    ) -> ThreadStoreFuture<'_, StoredThreadSection> {
        Box::pin(async move { thread_sections::create_thread_section(self, params).await })
    }

    fn rename_thread_section(
        &self,
        params: RenameThreadSectionParams,
    ) -> ThreadStoreFuture<'_, Option<StoredThreadSection>> {
        Box::pin(async move { thread_sections::rename_thread_section(self, params).await })
    }

    fn delete_thread_section(
        &self,
        params: DeleteThreadSectionParams,
    ) -> ThreadStoreFuture<'_, bool> {
        Box::pin(async move { thread_sections::delete_thread_section(self, params).await })
    }

    fn supports_projects(&self) -> bool {
        self.state_db.is_some()
    }

    fn list_projects(
        &self,
        params: ListProjectsParams,
    ) -> ThreadStoreFuture<'_, StoredProjectsPage> {
        Box::pin(async move { projects::list_projects(self, params).await })
    }

    fn read_project(&self, project_id: String) -> ThreadStoreFuture<'_, Option<StoredProject>> {
        Box::pin(async move { projects::read_project(self, project_id).await })
    }

    fn create_project(&self, params: CreateProjectParams) -> ThreadStoreFuture<'_, CreatedProject> {
        Box::pin(async move { projects::create_project(self, params).await })
    }

    fn update_project(
        &self,
        params: UpdateProjectParams,
    ) -> ThreadStoreFuture<'_, Option<UpdatedProject>> {
        Box::pin(async move { projects::update_project(self, params).await })
    }

    fn move_project(
        &self,
        params: MoveProjectParams,
    ) -> ThreadStoreFuture<'_, Option<ProjectMoveOutcome>> {
        Box::pin(async move { projects::move_project(self, params).await })
    }

    fn delete_project(&self, project_id: String) -> ThreadStoreFuture<'_, Option<DeletedProject>> {
        Box::pin(async move { projects::delete_project(self, project_id).await })
    }

    fn supports_paginated_history_lists(&self) -> bool {
        self.state_db.is_some()
    }

    fn list_turns(&self, params: ListTurnsParams) -> ThreadStoreFuture<'_, TurnPage> {
        Box::pin(LocalThreadStore::list_turns(self, params))
    }

    fn list_items(&self, params: ListItemsParams) -> ThreadStoreFuture<'_, ItemPage> {
        Box::pin(LocalThreadStore::list_items(self, params))
    }

    fn list_timeline(&self, params: ListTimelineParams) -> ThreadStoreFuture<'_, TimelinePage> {
        Box::pin(LocalThreadStore::list_timeline(self, params))
    }

    fn search_threads(
        &self,
        params: SearchThreadsParams,
    ) -> ThreadStoreFuture<'_, ThreadSearchPage> {
        Box::pin(async move { search_threads::search_threads(self, params).await })
    }

    fn search_thread_occurrences(
        &self,
        params: SearchThreadOccurrencesParams,
    ) -> ThreadStoreFuture<'_, ThreadOccurrenceSearchPage> {
        Box::pin(LocalThreadStore::search_thread_occurrences(self, params))
    }

    fn update_thread_metadata(
        &self,
        params: UpdateThreadMetadataParams,
    ) -> ThreadStoreFuture<'_, Option<StoredThread>> {
        Box::pin(async move {
            update_thread_metadata::update_thread_metadata(self, params)
                .await
                .map(Some)
        })
    }

    fn touch_root_thread_recency(
        &self,
        params: TouchRootThreadRecencyParams,
    ) -> ThreadStoreFuture<'_, Option<std::time::Duration>> {
        Box::pin(async move {
            let Some(state_db) = self.state_db().await else {
                return Ok(None);
            };
            state_db
                .touch_root_thread_recency_for_descendant(
                    params.descendant_thread_id,
                    params.activity_at,
                    params.minimum_interval,
                )
                .await
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!("failed to advance root thread recency: {err}"),
                })
        })
    }

    fn move_thread_to_section(
        &self,
        params: MoveThreadToSectionParams,
    ) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { move_thread_to_section::move_thread_to_section(self, params).await })
    }

    fn archive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            self.await_automatic_rollout_migration(params.thread_id)
                .await?;
            archive_thread::archive_threads(
                self,
                ArchiveThreadsParams {
                    thread_ids: vec![params.thread_id],
                    writer_lock_thread_ids: Vec::new(),
                },
            )
            .await
            .map(|_| ())
        })
    }

    fn archive_threads(
        &self,
        params: ArchiveThreadsParams,
    ) -> ThreadStoreFuture<'_, Vec<ThreadId>> {
        Box::pin(async move { archive_thread::archive_threads(self, params).await })
    }

    fn unarchive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async move {
            self.await_automatic_rollout_migration(params.thread_id)
                .await?;
            unarchive_thread::unarchive_thread(self, params).await
        })
    }

    fn delete_thread(&self, params: DeleteThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { delete_thread::delete_thread(self, params).await })
    }

    fn delete_threads(&self, params: DeleteThreadsParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { delete_thread::delete_threads(self, params).await })
    }

    fn delete_threads_with_outcome(
        &self,
        params: DeleteThreadsParams,
    ) -> ThreadStoreFuture<'_, DeleteThreadsOutcome> {
        Box::pin(async move { delete_thread::delete_threads_with_outcome(self, params).await })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use codex_protocol::ThreadId;
    use codex_protocol::config_types::ReasoningSummary;
    use codex_protocol::items::TurnItem;
    use codex_protocol::items::UserMessageItem;
    use codex_protocol::models::BaseInstructions;
    use codex_protocol::models::ContentItem;
    use codex_protocol::models::FunctionCallOutputPayload;
    use codex_protocol::models::MessagePhase;
    use codex_protocol::models::ResponseItem;
    use codex_protocol::protocol::AgentMessageEvent;
    use codex_protocol::protocol::AskForApproval;
    use codex_protocol::protocol::EventMsg;
    use codex_protocol::protocol::ItemCompletedEvent;
    use codex_protocol::protocol::SandboxPolicy;
    use codex_protocol::protocol::SessionSource;
    use codex_protocol::protocol::ThreadHistoryMode;
    use codex_protocol::protocol::ThreadMemoryMode;
    use codex_protocol::protocol::TurnCompleteEvent;
    use codex_protocol::protocol::TurnContextItem;
    use codex_protocol::protocol::TurnStartedEvent;
    use codex_protocol::protocol::UserMessageEvent;
    use codex_rollout::RolloutItem;
    use tempfile::TempDir;

    use super::*;
    use crate::LiveThread;
    use crate::ThreadPersistenceMetadata;
    use crate::local::test_support::test_config;
    use crate::local::test_support::write_archived_session_file;
    use crate::local::test_support::write_session_file;
    use crate::local::test_support::write_session_file_with_history_mode;

    #[tokio::test]
    async fn live_writer_lifecycle_writes_and_closes() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let thread_id = ThreadId::default();

        store
            .create_thread(create_thread_params(thread_id))
            .await
            .expect("create live thread");
        let rollout_path = store
            .live_rollout_path(thread_id)
            .await
            .expect("load rollout path");

        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("first live write")],
            })
            .await
            .expect("append live item");
        store
            .persist_thread(thread_id, PersistContext::Standard)
            .await
            .expect("persist live thread");
        store
            .flush_thread(thread_id)
            .await
            .expect("flush live thread");

        assert_rollout_contains_message(rollout_path.as_path(), "first live write").await;

        store
            .shutdown_thread(thread_id)
            .await
            .expect("shutdown live thread");
        let err = store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("write after shutdown")],
            })
            .await
            .expect_err("shutdown should remove the live thread writer");
        assert!(
            matches!(err, ThreadStoreError::ThreadNotFound { thread_id: missing } if missing == thread_id)
        );
    }

    #[tokio::test]
    async fn raw_append_items_does_not_update_sqlite_metadata() {
        // This pins the ThreadStore contract: raw appends are history-only. Callers that need
        // metadata updates must use LiveThread or call update_thread_metadata explicitly.
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = LocalThreadStore::new(config, Some(runtime.clone()));
        let thread_id = ThreadId::default();

        store
            .create_thread(create_thread_params(thread_id))
            .await
            .expect("create live thread");
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("raw append")],
            })
            .await
            .expect("append raw item");
        store.flush_thread(thread_id).await.expect("flush thread");

        assert_eq!(
            runtime
                .get_thread(thread_id)
                .await
                .expect("sqlite metadata read"),
            None
        );
    }

    #[tokio::test]
    async fn live_thread_observes_appended_items_into_sqlite_metadata() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let thread_id = ThreadId::default();
        let live_thread = LiveThread::create(store.clone(), create_thread_params(thread_id))
            .await
            .expect("create live thread");

        live_thread
            .append_items(&[user_message_item("observed append")])
            .await
            .expect("append observed item");
        live_thread.flush().await.expect("flush thread");

        let metadata = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");
        assert_eq!(
            metadata.first_user_message.as_deref(),
            Some("observed append")
        );
        assert_eq!(metadata.preview.as_deref(), Some("observed append"));
        assert_eq!(metadata.title, "observed append");
    }

    #[tokio::test]
    async fn descendant_activity_advances_root_recency_with_persisted_debounce() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let root_id = ThreadId::new();
        let child_id = ThreadId::new();
        let root = LiveThread::create(store.clone(), create_thread_params(root_id))
            .await
            .expect("create root thread");
        let mut child_params = create_thread_params(child_id);
        child_params.parent_thread_id = Some(root_id);
        child_params.history_mode = ThreadHistoryMode::Paginated;
        let child = LiveThread::create(store.clone(), child_params)
            .await
            .expect("create child thread");
        let initial_recency_at =
            chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0).expect("timestamp");

        for (thread_id, rollout_path) in [
            (
                root_id,
                root.local_rollout_path()
                    .await
                    .expect("root rollout path")
                    .expect("local root rollout"),
            ),
            (
                child_id,
                child
                    .local_rollout_path()
                    .await
                    .expect("child rollout path")
                    .expect("local child rollout"),
            ),
        ] {
            let mut builder = codex_state::ThreadMetadataBuilder::new(
                thread_id,
                rollout_path,
                initial_recency_at,
                SessionSource::Exec,
            );
            builder.updated_at = Some(initial_recency_at);
            builder.recency_at = Some(initial_recency_at);
            builder.cwd = home.path().to_path_buf();
            runtime
                .upsert_thread(&builder.build("test-provider"))
                .await
                .expect("thread metadata should persist");
        }
        runtime
            .upsert_thread_spawn_edge(
                root_id,
                child_id,
                codex_state::DirectionalThreadSpawnEdgeStatus::Open,
            )
            .await
            .expect("spawn edge should persist");

        child
            .append_items(&[response_user_message_item("background child output")])
            .await
            .expect("child activity should persist");
        let first_root = runtime
            .get_thread(root_id)
            .await
            .expect("root should load")
            .expect("root should exist");
        assert!(first_root.recency_at > initial_recency_at);
        assert_eq!(first_root.updated_at, initial_recency_at);

        child.shutdown().await.expect("shutdown child thread");
        let resumed_child = LiveThread::resume(
            store,
            ThreadHistoryMode::Paginated,
            ResumeThreadParams {
                thread_id: child_id,
                rollout_path: None,
                history: Some(Arc::new(vec![response_user_message_item(
                    "bounded context without session metadata",
                )])),
                include_archived: false,
                metadata: thread_metadata(),
            },
        )
        .await
        .expect("resume child thread");
        resumed_child
            .append_items(&[response_user_message_item("more background child output")])
            .await
            .expect("resumed child activity should persist");
        assert_eq!(
            runtime
                .get_thread(root_id)
                .await
                .expect("root should load")
                .expect("root should exist")
                .recency_at,
            first_root.recency_at
        );
    }

    #[tokio::test]
    async fn paginated_resume_rejects_explicit_rollout_path_over_stale_sqlite_path() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let uuid = uuid::Uuid::from_u128(228);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path = write_session_file_with_history_mode(
            home.path(),
            "2025-01-03T12-00-00",
            uuid,
            ThreadHistoryMode::Paginated,
        )
        .expect("paginated session file");
        let stale_rollout_path = home.path().join("stale-rollout.jsonl");
        tokio::fs::write(&stale_rollout_path, "malformed session metadata\n")
            .await
            .expect("write stale rollout");
        let mut builder = codex_state::ThreadMetadataBuilder::new(
            thread_id,
            stale_rollout_path,
            chrono::Utc::now(),
            SessionSource::Cli,
        );
        builder.history_mode = ThreadHistoryMode::Paginated;
        builder.cwd = home.path().to_path_buf();
        let mut metadata = builder.build("test-provider");
        metadata.preview = Some("original user message".to_string());
        metadata.first_user_message = Some("original user message".to_string());
        metadata.title = "original user message".to_string();
        runtime
            .upsert_thread(&metadata)
            .await
            .expect("update stale sqlite rollout path");

        let error = match LiveThread::resume(
            store,
            ThreadHistoryMode::Paginated,
            ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path.clone()),
                history: Some(Arc::new(vec![user_message_item("bounded suffix")])),
                include_archived: false,
                metadata: ThreadPersistenceMetadata {
                    cwd: Some(home.path().to_path_buf()),
                    model_provider: "test-provider".to_string(),
                    memory_mode: ThreadMemoryMode::Enabled,
                },
            },
        )
        .await
        {
            Ok(_) => panic!("requested rollout that is not selected by sqlite must fail"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("rollout path does not select the current rollout")
        );

        let metadata = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");
        assert_eq!(
            (
                metadata.preview.as_deref(),
                metadata.title.as_str(),
                metadata.first_user_message.as_deref(),
            ),
            (
                Some("original user message"),
                "original user message",
                Some("original user message"),
            )
        );
    }

    #[tokio::test]
    async fn live_thread_does_not_derive_metadata_from_inherited_items() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let thread_id = ThreadId::default();
        let mut params = create_thread_params(thread_id);
        params.history_mode = ThreadHistoryMode::Paginated;
        let cwd = std::env::current_dir().expect("current directory");
        let turn_context = |model: &str, approval_policy| {
            RolloutItem::TurnContext(TurnContextItem {
                turn_id: Some("turn-1".to_string()),
                cwd: serde_json::from_value(serde_json::json!(cwd)).expect("absolute cwd"),
                workspace_roots: None,
                current_date: None,
                timezone: None,
                approval_policy,
                approvals_reviewer: None,
                sandbox_policy: SandboxPolicy::DangerFullAccess,
                permission_profile: None,
                active_permission_profile: None,
                network: None,
                file_system_sandbox_policy: None,
                model: model.to_string(),
                comp_hash: None,
                personality: None,
                collaboration_mode: None,
                multi_agent_version: None,
                multi_agent_mode: None,
                realtime_active: None,
                cyber_access_program: None,
                effort: None,
                service_tier: None,
                model_profile: None,
                summary: ReasoningSummary::Auto,
            })
        };

        let live_thread = LiveThread::create_with_inherited_model_context(
            store,
            params,
            &[turn_context("parent-model", AskForApproval::Never)],
        )
        .await
        .expect("create live thread with inherited context");
        live_thread
            .persist(PersistContext::Standard)
            .await
            .expect("persist thread");
        let inherited_metadata = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");
        assert_eq!(inherited_metadata.model, None);

        live_thread
            .append_items(&[turn_context("child-model", AskForApproval::OnRequest)])
            .await
            .expect("append child context");
        let child_metadata = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");
        assert_eq!(child_metadata.model.as_deref(), Some("child-model"));
        assert_eq!(child_metadata.approval_mode, "on-request");
    }

    #[tokio::test]
    async fn inherited_model_context_boundary_uses_absolute_rollout_ordinals() {
        let home = TempDir::new().expect("temp dir");
        let store = Arc::new(LocalThreadStore::new(
            test_config(home.path()),
            /*state_db*/ None,
        ));
        let thread_id = ThreadId::default();
        let mut params = create_thread_params(thread_id);
        params.history_mode = ThreadHistoryMode::Paginated;
        params.initial_rollout_ordinal = 41;
        let message = |text: &str| {
            RolloutItem::ResponseItem(
                ResponseItem::Message {
                    id: None,
                    role: "developer".to_string(),
                    content: vec![ContentItem::InputText {
                        text: text.to_string(),
                    }],
                    phase: None,
                    internal_chat_message_metadata_passthrough: None,
                }
                .into(),
            )
        };

        let live_thread = LiveThread::create_with_inherited_model_context(
            store.clone(),
            params,
            &[message("inherited")],
        )
        .await
        .expect("create live thread with inherited context");
        live_thread
            .append_items(&[message("child")])
            .await
            .expect("append child context");
        live_thread.flush().await.expect("flush thread");
        let rollout_path = store
            .live_rollout_path(thread_id)
            .await
            .expect("live rollout path");
        let (lines, _, _) = RolloutRecorder::load_rollout_lines(&rollout_path)
            .await
            .expect("load rollout lines");

        let RolloutItem::SessionMeta(meta_line) = &lines[0].item else {
            panic!("rollout should start with session metadata");
        };
        assert_eq!(meta_line.meta.subagent_history_start_ordinal, Some(43));
        assert_eq!(
            lines
                .iter()
                .map(|line| line.ordinal.expect("line should have an ordinal"))
                .collect::<Vec<_>>(),
            vec![41, 42, 43]
        );
    }

    #[tokio::test]
    async fn live_thread_output_advances_updated_at_but_not_recency_at() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let thread_id = ThreadId::default();
        let live_thread = LiveThread::create(store, create_thread_params(thread_id))
            .await
            .expect("create live thread");

        live_thread
            .append_items(&[user_message_item("start thread")])
            .await
            .expect("append initial user message");
        live_thread.flush().await.expect("flush thread");
        let before_turn_start = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");

        live_thread
            .append_items(&[RolloutItem::EventMsg(EventMsg::TurnStarted(
                TurnStartedEvent {
                    turn_id: "turn-1".to_string(),
                    trace_id: None,
                    started_at: None,
                    model_context_window: None,
                    collaboration_mode_kind: Default::default(),
                },
            ))])
            .await
            .expect("append turn start");
        live_thread.flush().await.expect("flush thread");
        let after_turn_start = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");
        assert!(after_turn_start.recency_at > before_turn_start.recency_at);

        live_thread
            .append_items(&[
                RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
                    message: "commentary".to_string(),
                    phase: Some(MessagePhase::Commentary),
                    memory_citation: None,
                    delivery: None,
                })),
                RolloutItem::ResponseItem(
                    ResponseItem::FunctionCallOutput {
                        id: None,
                        call_id: Some("call-1".to_string()),
                        name: None,
                        namespace: None,
                        output: FunctionCallOutputPayload::from_text("tool output".to_string()),
                        internal_chat_message_metadata_passthrough: None,
                    }
                    .into(),
                ),
                RolloutItem::EventMsg(EventMsg::TokenCount(
                    codex_protocol::protocol::TokenCountEvent {
                        info: None,
                        rate_limits: None,
                    },
                )),
                RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
                    turn_id: "turn-1".to_string(),
                    started_at: None,
                    last_agent_message: None,
                    error: None,
                    completed_at: None,
                    duration_ms: None,
                    time_to_first_token_ms: None,
                })),
            ])
            .await
            .expect("append post-start items");
        live_thread.flush().await.expect("flush thread");
        let completed = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");

        assert!(completed.updated_at > after_turn_start.updated_at);
        assert_eq!(completed.recency_at, after_turn_start.recency_at);
    }

    #[tokio::test]
    async fn live_thread_shutdown_does_not_materialize_empty_thread_metadata() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let thread_id = ThreadId::default();
        let live_thread = LiveThread::create(store.clone(), create_thread_params(thread_id))
            .await
            .expect("create live thread");
        let rollout_path = store
            .live_rollout_path(thread_id)
            .await
            .expect("live rollout path");

        live_thread.shutdown().await.expect("shutdown thread");

        assert!(
            !tokio::fs::try_exists(rollout_path.as_path())
                .await
                .expect("rollout path should be checkable")
        );
        assert_eq!(
            runtime
                .get_thread(thread_id)
                .await
                .expect("sqlite metadata read"),
            None
        );
    }

    #[tokio::test]
    async fn live_thread_memory_mode_update_before_rollout_materializes_keeps_history_mode() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let thread_id = ThreadId::default();
        let live_thread = LiveThread::create(store.clone(), create_thread_params(thread_id))
            .await
            .expect("create live thread");

        live_thread
            .update_memory_mode(ThreadMemoryMode::Disabled, /*include_archived*/ false)
            .await
            .expect("update memory mode");

        assert_eq!(
            runtime
                .get_thread(thread_id)
                .await
                .expect("sqlite metadata read")
                .expect("sqlite metadata")
                .history_mode,
            ThreadHistoryMode::Legacy
        );
        assert_eq!(
            runtime
                .get_thread_memory_mode(thread_id)
                .await
                .expect("thread memory mode should be readable")
                .as_deref(),
            Some("disabled")
        );
    }

    #[tokio::test]
    async fn live_thread_shutdown_with_buffered_items_materializes_before_metadata_read() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let thread_id = ThreadId::default();
        let live_thread = LiveThread::create(store.clone(), create_thread_params(thread_id))
            .await
            .expect("create live thread");
        let rollout_path = store
            .live_rollout_path(thread_id)
            .await
            .expect("live rollout path");

        live_thread
            .append_items(&[RolloutItem::EventMsg(EventMsg::TokenCount(
                codex_protocol::protocol::TokenCountEvent {
                    info: None,
                    rate_limits: None,
                },
            ))])
            .await
            .expect("append metadata-only item");
        live_thread.shutdown().await.expect("shutdown thread");

        assert!(
            tokio::fs::try_exists(rollout_path.as_path())
                .await
                .expect("rollout path should be checkable")
        );
        let metadata = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");
        assert_eq!(metadata.rollout_path, rollout_path);
    }

    #[tokio::test]
    async fn live_thread_resume_loads_history_before_observing_metadata() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let uuid = uuid::Uuid::from_u128(401);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path =
            write_session_file(home.path(), "2025-01-03T17-00-00", uuid).expect("session file");
        let live_thread = LiveThread::resume(
            store,
            ThreadHistoryMode::Legacy,
            ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path),
                history: None,
                include_archived: false,
                metadata: ThreadPersistenceMetadata {
                    cwd: Some(home.path().to_path_buf()),
                    model_provider: "different-provider".to_string(),
                    memory_mode: ThreadMemoryMode::Enabled,
                },
            },
        )
        .await
        .expect("resume live thread");

        live_thread
            .append_items(&[user_message_item("new live append")])
            .await
            .expect("append after resume");

        let metadata = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");
        assert_eq!(
            metadata.created_at.to_rfc3339(),
            "2025-01-03T17:00:00+00:00"
        );
        assert_eq!(metadata.model_provider, "test-provider");
        assert_eq!(
            metadata.first_user_message.as_deref(),
            Some("Hello from user")
        );
    }

    #[tokio::test]
    async fn live_thread_resume_loads_history_from_explicit_external_rollout_path() {
        let home = TempDir::new().expect("temp dir");
        let external_home = TempDir::new().expect("external temp dir");
        let config = test_config(home.path());
        let runtime = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
        let uuid = uuid::Uuid::from_u128(402);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path = write_session_file(external_home.path(), "2025-01-03T17-30-00", uuid)
            .expect("external session file");
        let live_thread = LiveThread::resume(
            store,
            ThreadHistoryMode::Legacy,
            ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path),
                history: None,
                include_archived: false,
                metadata: ThreadPersistenceMetadata {
                    cwd: Some(home.path().to_path_buf()),
                    model_provider: "different-provider".to_string(),
                    memory_mode: ThreadMemoryMode::Enabled,
                },
            },
        )
        .await
        .expect("resume external live thread");

        live_thread
            .append_items(&[user_message_item("new external append")])
            .await
            .expect("append after external resume");

        let metadata = runtime
            .get_thread(thread_id)
            .await
            .expect("sqlite metadata read")
            .expect("sqlite metadata");
        assert_eq!(
            metadata.created_at.to_rfc3339(),
            "2025-01-03T17:30:00+00:00"
        );
        assert_eq!(metadata.model_provider, "test-provider");
        assert_eq!(
            metadata.first_user_message.as_deref(),
            Some("Hello from user")
        );
    }

    #[tokio::test]
    async fn create_thread_rejects_missing_cwd() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let competing_store =
            LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        for history_mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
            let thread_id = ThreadId::default();
            let mut params = create_thread_params(thread_id);
            params.history_mode = history_mode;
            params.metadata.cwd = None;

            let err = store
                .create_thread(params)
                .await
                .expect_err("local thread store should require cwd");

            assert!(matches!(
                err,
                ThreadStoreError::InvalidRequest { message }
                    if message == "local thread store requires a cwd"
            ));

            let mut valid_params = create_thread_params(thread_id);
            valid_params.history_mode = history_mode;
            competing_store
                .create_thread(valid_params)
                .await
                .expect("failed initialization should release cross-process writer ownership");
        }
    }

    #[tokio::test]
    async fn discard_thread_drops_unmaterialized_live_writer() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        for history_mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
            let thread_id = ThreadId::default();
            let mut params = create_thread_params(thread_id);
            params.history_mode = history_mode;

            store
                .create_thread(params)
                .await
                .expect("create live thread");
            let rollout_path = store
                .live_rollout_path(thread_id)
                .await
                .expect("load rollout path");
            assert!(!rollout_path.exists());

            let lock_path = home
                .path()
                .join("thread-writer-locks")
                .join(format!("{thread_id}.lock"));
            assert!(lock_path.exists());
            store
                .discard_thread(thread_id)
                .await
                .expect("discard live thread");

            assert!(!rollout_path.exists());
            assert!(!lock_path.exists());
            let err = store
                .append_items(AppendThreadItemsParams {
                    thread_id,
                    items: vec![user_message_item("write after discard")],
                })
                .await
                .expect_err("discard should remove the live thread writer");
            assert!(
                matches!(err, ThreadStoreError::ThreadNotFound { thread_id: missing } if missing == thread_id)
            );
        }
    }

    #[tokio::test]
    async fn resume_thread_reopens_live_writer_and_appends() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let thread_id = ThreadId::default();

        let first_store = LocalThreadStore::new(config.clone(), /*state_db*/ None);
        first_store
            .create_thread(create_thread_params(thread_id))
            .await
            .expect("create initial thread");
        first_store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("before resume")],
            })
            .await
            .expect("append initial item");
        first_store
            .persist_thread(thread_id, PersistContext::Standard)
            .await
            .expect("persist initial thread");
        first_store
            .flush_thread(thread_id)
            .await
            .expect("flush initial thread");
        let rollout_path = first_store
            .live_rollout_path(thread_id)
            .await
            .expect("load rollout path");
        first_store
            .shutdown_thread(thread_id)
            .await
            .expect("shutdown initial writer");

        let resumed_store = LocalThreadStore::new(config, /*state_db*/ None);
        resumed_store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: None,
                history: None,
                include_archived: true,
                metadata: thread_metadata(),
            })
            .await
            .expect("resume live thread");
        resumed_store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("after resume")],
            })
            .await
            .expect("append resumed item");
        resumed_store
            .flush_thread(thread_id)
            .await
            .expect("flush resumed thread");

        assert_rollout_contains_message(rollout_path.as_path(), "before resume").await;
        assert_rollout_contains_message(rollout_path.as_path(), "after resume").await;
    }

    #[tokio::test]
    async fn resume_thread_rejects_supplied_history_mode_that_disagrees_with_rollout() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let uuid = uuid::Uuid::from_u128(410);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let legacy_path = write_session_file_with_history_mode(
            home.path(),
            "2025-01-04T12-00-00",
            uuid,
            ThreadHistoryMode::Legacy,
        )
        .expect("legacy session file");
        let paginated_path = write_session_file_with_history_mode(
            home.path(),
            "2025-01-04T12-01-00",
            uuid,
            ThreadHistoryMode::Paginated,
        )
        .expect("paginated session file");
        let history = Arc::new(
            RolloutRecorder::load_rollout_items(legacy_path.as_path())
                .await
                .expect("legacy supplied history")
                .0,
        );

        let original = std::fs::read(&paginated_path).expect("original native file");
        let error = store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(paginated_path.clone()),
                history: Some(history),
                include_archived: true,
                metadata: thread_metadata(),
            })
            .await
            .expect_err("supplied legacy history must not configure a native writer");
        assert!(matches!(error, ThreadStoreError::InvalidRequest { message }
            if message.contains("history format changed before resume")));
        assert_eq!(
            std::fs::read(paginated_path).expect("unchanged native file"),
            original
        );
    }

    #[tokio::test]
    async fn resume_thread_supplied_history_does_not_mask_malformed_rollout() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let uuid = uuid::Uuid::from_u128(411);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let history_uuid = uuid::Uuid::from_u128(412);
        let legacy_path = write_session_file_with_history_mode(
            home.path(),
            "2025-01-04T12-02-00",
            history_uuid,
            ThreadHistoryMode::Legacy,
        )
        .expect("legacy session file");
        let history = Arc::new(
            RolloutRecorder::load_rollout_items(legacy_path.as_path())
                .await
                .expect("legacy supplied history")
                .0,
        );
        let malformed_path = home.path().join("malformed-rollout.jsonl");
        std::fs::write(&malformed_path, "not a rollout line\n").expect("malformed rollout");

        let error = store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(malformed_path),
                history: Some(history),
                include_archived: true,
                metadata: thread_metadata(),
            })
            .await
            .expect_err("malformed nonempty rollout should fail");

        assert!(
            error
                .to_string()
                .contains("failed to resume local thread recorder")
        );
    }

    #[tokio::test]
    async fn live_writers_reject_cross_process_create_and_resume() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let primary = LocalThreadStore::new(config.clone(), /*state_db*/ None);
        let secondary = LocalThreadStore::new(config, /*state_db*/ None);

        for history_mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
            let thread_id = ThreadId::default();
            let mut create_params = create_thread_params(thread_id);
            create_params.history_mode = history_mode;

            primary
                .create_thread(create_params.clone())
                .await
                .expect("create live thread");
            primary
                .persist_thread(thread_id, PersistContext::Standard)
                .await
                .expect("persist thread for resume");
            let rollout_path = primary
                .live_rollout_path(thread_id)
                .await
                .expect("load rollout path");
            let resume_params = ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path),
                history: None,
                include_archived: true,
                metadata: thread_metadata(),
            };

            let error = secondary
                .create_thread(create_params)
                .await
                .expect_err("competing create should fail");
            assert!(matches!(error, ThreadStoreError::Conflict { .. }));

            let error = secondary
                .resume_thread(resume_params.clone())
                .await
                .expect_err("competing resume should fail");
            assert!(matches!(error, ThreadStoreError::Conflict { .. }));

            primary
                .shutdown_thread(thread_id)
                .await
                .expect("shutdown should release writer ownership");
            secondary
                .resume_thread(resume_params)
                .await
                .expect("resume after shutdown should acquire writer ownership");
        }
    }

    #[tokio::test]
    async fn create_thread_rejects_duplicate_live_writer() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let thread_id = ThreadId::default();

        store
            .create_thread(create_thread_params(thread_id))
            .await
            .expect("create live thread");

        let err = store
            .create_thread(create_thread_params(thread_id))
            .await
            .expect_err("duplicate live writer should fail");

        assert!(matches!(err, ThreadStoreError::InvalidRequest { .. }));
        assert!(err.to_string().contains("already has a live local writer"));
    }

    #[tokio::test]
    async fn resume_thread_rejects_duplicate_live_writer() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let thread_id = ThreadId::default();

        store
            .create_thread(create_thread_params(thread_id))
            .await
            .expect("create live thread");
        let rollout_path = store
            .live_rollout_path(thread_id)
            .await
            .expect("live rollout path");
        let err = store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path),
                history: None,
                include_archived: true,
                metadata: thread_metadata(),
            })
            .await
            .expect_err("duplicate live resume should fail");
        assert!(matches!(err, ThreadStoreError::InvalidRequest { .. }));
        assert!(err.to_string().contains("already has a live local writer"));
    }

    #[tokio::test]
    async fn resume_thread_rejects_missing_cwd() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let competing_store =
            LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let uuid = uuid::Uuid::from_u128(408);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path = write_session_file_with_history_mode(
            home.path(),
            "2025-01-04T11-30-00",
            uuid,
            ThreadHistoryMode::Paginated,
        )
        .expect("session file");
        let err = store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path.clone()),
                history: None,
                include_archived: true,
                metadata: ThreadPersistenceMetadata {
                    cwd: None,
                    model_provider: "test-provider".to_string(),
                    memory_mode: ThreadMemoryMode::Enabled,
                },
            })
            .await
            .expect_err("missing cwd should fail");

        assert!(matches!(err, ThreadStoreError::InvalidRequest { .. }));
        assert!(err.to_string().contains("requires a cwd"));

        competing_store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path),
                history: None,
                include_archived: true,
                metadata: thread_metadata(),
            })
            .await
            .expect("failed initialization should release cross-process writer ownership");
    }

    #[tokio::test]
    async fn load_history_uses_live_writer_rollout_path() {
        let home = TempDir::new().expect("temp dir");
        let external_home = TempDir::new().expect("external temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let uuid = uuid::Uuid::from_u128(404);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path = write_session_file(external_home.path(), "2025-01-04T10-00-00", uuid)
            .expect("external session file");

        store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path),
                history: None,
                include_archived: true,
                metadata: thread_metadata(),
            })
            .await
            .expect("resume live thread");
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("external history item")],
            })
            .await
            .expect("append live item");
        store
            .flush_thread(thread_id)
            .await
            .expect("flush live thread");

        let history = store
            .load_history(LoadThreadHistoryParams {
                thread_id,
                include_archived: false,
            })
            .await
            .expect("load external live history");

        assert!(history.items.iter().any(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::UserMessage(event)) if event.message == "external history item"
            )
        }));
    }

    #[tokio::test]
    async fn read_thread_uses_live_writer_rollout_path_for_external_resume() {
        let home = TempDir::new().expect("temp dir");
        let external_home = TempDir::new().expect("external temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let uuid = uuid::Uuid::from_u128(406);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path = write_session_file(external_home.path(), "2025-01-04T11-00-00", uuid)
            .expect("external session file");

        store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path.clone()),
                history: None,
                include_archived: true,
                metadata: thread_metadata(),
            })
            .await
            .expect("resume live thread");

        let thread = store
            .read_thread(ReadThreadParams {
                thread_id,
                include_archived: false,
                include_history: true,
            })
            .await
            .expect("read external live thread");

        assert_eq!(thread.rollout_path, Some(rollout_path));
        assert!(thread.history.expect("history").items.iter().any(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::UserMessage(event)) if event.message == "Hello from user"
            )
        }));

        let error = store
            .prepare_fork(PrepareForkParams {
                thread_id,
                boundary: crate::ForkBoundary::Latest,
            })
            .await
            .expect_err("external rollouts cannot be referenced by thread id");
        assert!(
            error.to_string().contains("must be in Codex home"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn load_history_uses_live_writer_rollout_path_for_archived_source() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let uuid = uuid::Uuid::from_u128(405);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path = write_archived_session_file(home.path(), "2025-01-04T10-30-00", uuid)
            .expect("archived session file");

        store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path),
                history: None,
                include_archived: true,
                metadata: thread_metadata(),
            })
            .await
            .expect("resume live archived thread");
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("archived live history item")],
            })
            .await
            .expect("append live item");
        store
            .flush_thread(thread_id)
            .await
            .expect("flush live thread");

        let err = store
            .read_thread(ReadThreadParams {
                thread_id,
                include_archived: false,
                include_history: false,
            })
            .await
            .expect_err("active-only read should reject archived live thread");
        assert!(matches!(err, ThreadStoreError::InvalidRequest { .. }));

        let err = store
            .load_history(LoadThreadHistoryParams {
                thread_id,
                include_archived: false,
            })
            .await
            .expect_err("active-only history should reject archived live thread");
        assert!(matches!(err, ThreadStoreError::InvalidRequest { .. }));
        assert!(err.to_string().contains("archived"));

        let history = store
            .load_history(LoadThreadHistoryParams {
                thread_id,
                include_archived: true,
            })
            .await
            .expect("load archived live history");

        assert!(history.items.iter().any(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::UserMessage(event)) if event.message == "archived live history item"
            )
        }));
    }

    #[tokio::test]
    async fn read_thread_by_rollout_path_includes_history() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let thread_id = ThreadId::default();

        store
            .create_thread(create_thread_params(thread_id))
            .await
            .expect("create thread");
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![user_message_item("path read")],
            })
            .await
            .expect("append item");
        store.flush_thread(thread_id).await.expect("flush thread");
        let rollout_path = store
            .live_rollout_path(thread_id)
            .await
            .expect("load rollout path");

        let thread = store
            .read_thread_by_rollout_path(
                rollout_path,
                /*include_archived*/ true,
                /*include_history*/ true,
            )
            .await
            .expect("read thread by rollout path");

        assert_eq!(thread.thread_id, thread_id);
        assert_eq!(thread.history_mode, ThreadHistoryMode::Legacy);
        assert_eq!(
            thread
                .history
                .as_ref()
                .expect("history")
                .items
                .iter()
                .filter(|item| matches!(item, RolloutItem::EventMsg(EventMsg::UserMessage(_))))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn paginated_threads_allow_history_reads_and_resume() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let uuid = uuid::Uuid::from_u128(408);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path = write_session_file_with_history_mode(
            home.path(),
            "2025-01-04T12-00-00",
            uuid,
            ThreadHistoryMode::Paginated,
        )
        .expect("session file");

        let thread = store
            .read_thread(ReadThreadParams {
                thread_id,
                include_archived: false,
                include_history: false,
            })
            .await
            .expect("metadata read");
        assert_eq!(thread.history_mode, ThreadHistoryMode::Paginated);
        assert!(thread.history.is_none());

        let thread = store
            .read_thread_by_rollout_path(
                rollout_path.clone(),
                /*include_archived*/ true,
                /*include_history*/ false,
            )
            .await
            .expect("metadata path read");
        assert_eq!(thread.history_mode, ThreadHistoryMode::Paginated);
        assert!(thread.history.is_none());

        let thread = store
            .read_thread(ReadThreadParams {
                thread_id,
                include_archived: false,
                include_history: true,
            })
            .await
            .expect("full history read");
        assert_eq!(thread.history_mode, ThreadHistoryMode::Paginated);
        assert!(thread.history.is_some());

        let thread = store
            .read_thread_by_rollout_path(
                rollout_path.clone(),
                /*include_archived*/ true,
                /*include_history*/ true,
            )
            .await
            .expect("full history path read");
        assert_eq!(thread.history_mode, ThreadHistoryMode::Paginated);
        assert!(thread.history.is_some());

        let history = store
            .load_history(LoadThreadHistoryParams {
                thread_id,
                include_archived: false,
            })
            .await
            .expect("history load");
        assert_eq!(history.thread_id, thread_id);

        store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path),
                history: None,
                include_archived: false,
                metadata: thread_metadata(),
            })
            .await
            .expect("resume should succeed");
    }

    #[tokio::test]
    async fn paginated_live_appends_use_paginated_history_mode() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let thread_id = ThreadId::default();
        let mut create_params = create_thread_params(thread_id);
        create_params.history_mode = ThreadHistoryMode::Paginated;
        store
            .create_thread(create_params)
            .await
            .expect("create paginated thread");
        let paginated_item = RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
            thread_id,
            turn_id: "turn-1".to_string(),
            item: TurnItem::UserMessage(UserMessageItem {
                id: "item-1".to_string(),
                client_id: None,
                content: Vec::new(),
            }),
            started_at_ms: Some(0),
            completed_at_ms: 1,
        }));
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![
                    user_message_item("legacy event should not persist"),
                    paginated_item,
                ],
            })
            .await
            .expect("append paginated item");
        let rollout_path = store
            .live_rollout_path(thread_id)
            .await
            .expect("paginated rollout path");
        let (items, _, _) = RolloutRecorder::load_rollout_items(rollout_path.as_path())
            .await
            .expect("load paginated rollout");
        assert!(items.iter().any(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::ItemCompleted(event))
                    if event.turn_id == "turn-1"
            )
        }));
        assert!(!items.iter().any(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::UserMessage(event))
                    if event.message == "legacy event should not persist"
            )
        }));
        store
            .shutdown_thread(thread_id)
            .await
            .expect("shutdown paginated thread");
        store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path),
                history: None,
                include_archived: false,
                metadata: thread_metadata(),
            })
            .await
            .expect("resume paginated thread");
    }

    fn create_thread_params(thread_id: ThreadId) -> CreateThreadParams {
        CreateThreadParams {
            session_id: thread_id.into(),
            thread_id,
            extra_config: None,
            forked_from_id: None,
            forked_from_ordinal_exclusive: None,
            parent_thread_id: None,
            source: SessionSource::Exec,
            thread_source: None,
            originator: "test_originator".to_string(),
            base_instructions: BaseInstructions::default(),
            dynamic_tools: Vec::new(),
            selected_capability_roots: Vec::new(),
            multi_agent_version: None,
            history_mode: ThreadHistoryMode::Legacy,
            history_base: None,
            subagent_history_start_ordinal: None,
            persistence_mode: ThreadPersistenceMode::Durable,
            initial_rollout_ordinal: 0,
            initial_window_id: uuid::Uuid::now_v7().to_string(),
            metadata: thread_metadata(),
        }
    }

    fn thread_metadata() -> ThreadPersistenceMetadata {
        ThreadPersistenceMetadata {
            cwd: Some(std::env::current_dir().expect("cwd")),
            model_provider: "test-provider".to_string(),
            memory_mode: ThreadMemoryMode::Enabled,
        }
    }

    fn user_message_item(message: &str) -> RolloutItem {
        RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
            client_id: None,
            message: message.to_string(),
            images: None,
            local_images: Vec::new(),
            text_elements: Vec::new(),
            ..Default::default()
        }))
    }

    fn response_user_message_item(message: &str) -> RolloutItem {
        RolloutItem::ResponseItem(
            ResponseItem::Message {
                id: None,
                role: "user".to_string(),
                content: vec![ContentItem::InputText {
                    text: message.to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }
            .into(),
        )
    }

    async fn assert_rollout_contains_message(path: &std::path::Path, expected: &str) {
        let (items, _, _) = RolloutRecorder::load_rollout_items(path)
            .await
            .expect("load rollout items");
        assert!(items.iter().any(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::UserMessage(event)) if event.message == expected
            )
        }));
    }
}
