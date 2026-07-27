use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutPersistenceTelemetry;
use codex_rollout::measure_and_filter_rollout_items;
use codex_rollout::persisted_rollout_items;
use futures::FutureExt;
use tokio::sync::Mutex;
use tracing::warn;

use crate::AppendThreadItemsParams;
use crate::CreateThreadParams;
use crate::FreezeRolloutSegmentParams;
use crate::FrozenRolloutSegment;
use crate::LoadThreadHistoryParams;
use crate::LocalThreadStore;
use crate::PersistContext;
use crate::ReadThreadParams;
use crate::ResumeThreadParams;
use crate::SegmentCheckpointPersistenceOutcome;
use crate::StoredThread;
use crate::StoredThreadHistory;
use crate::ThreadMetadataPatch;
use crate::ThreadPersistenceMode;
use crate::ThreadStore;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use crate::UpdateThreadMetadataParams;
use crate::thread_metadata_sync::ThreadMetadataSync;

/// Handle for an active thread's persistence lifecycle.
///
/// `LiveThread` keeps lifecycle decisions with the caller while delegating storage details to
/// [`ThreadStore`]. Local stores may use a rollout file internally and remote stores may use a
/// service, but session code should only need this handle for the active thread.
#[derive(Clone)]
pub struct LiveThread {
    thread_id: ThreadId,
    history_mode: ThreadHistoryMode,
    thread_store: Arc<dyn ThreadStore>,
    metadata_sync: Arc<Mutex<ThreadMetadataSync>>,
    persistence_mode: Arc<Mutex<ThreadPersistenceMode>>,
    /// Rejects later persistence after a checkpoint may have committed ambiguously.
    persistence_restart_required: Arc<AtomicBool>,
    persistence_telemetry: RolloutPersistenceTelemetry,
}

/// Arms the restart fence until checkpoint persistence returns a classified outcome.
struct CheckpointPersistenceRestartGuard {
    restart_required: Arc<AtomicBool>,
    armed: bool,
}

impl CheckpointPersistenceRestartGuard {
    fn new(restart_required: Arc<AtomicBool>) -> Self {
        Self {
            restart_required,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CheckpointPersistenceRestartGuard {
    fn drop(&mut self) {
        if self.armed {
            self.restart_required.store(true, Ordering::Release);
        }
    }
}

/// Owns a live thread while session initialization is still fallible.
///
/// If initialization returns early after persistence has been opened, dropping this guard discards
/// the live writer without forcing lazy in-memory state to become durable. Call [`commit`] once the
/// session owns the live thread for normal operation.
pub struct LiveThreadInitGuard {
    live_thread: Option<LiveThread>,
}

impl LiveThreadInitGuard {
    pub fn new(live_thread: Option<LiveThread>) -> Self {
        Self { live_thread }
    }

    pub fn as_ref(&self) -> Option<&LiveThread> {
        self.live_thread.as_ref()
    }

    pub fn commit(&mut self) {
        self.live_thread = None;
    }

    pub async fn discard(&mut self) {
        let Some(live_thread) = self.live_thread.take() else {
            return;
        };
        if let Err(err) = live_thread.discard().await {
            warn!("failed to discard thread persistence for failed session init: {err}");
        }
    }
}

impl Drop for LiveThreadInitGuard {
    fn drop(&mut self) {
        let Some(live_thread) = self.live_thread.take() else {
            return;
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            warn!("failed to discard thread persistence for failed session init: no Tokio runtime");
            return;
        };
        handle.spawn(async move {
            if let Err(err) = live_thread.discard().await {
                warn!("failed to discard thread persistence for failed session init: {err}");
            }
        });
    }
}

impl LiveThread {
    pub async fn create(
        thread_store: Arc<dyn ThreadStore>,
        params: CreateThreadParams,
    ) -> ThreadStoreResult<Self> {
        let thread_id = params.thread_id;
        let history_mode = params.history_mode;
        let persistence_mode = params.persistence_mode;
        let metadata_sync = ThreadMetadataSync::for_create(&params).await;
        thread_store.create_thread(params).await?;
        Ok(Self {
            thread_id,
            history_mode,
            thread_store,
            metadata_sync: Arc::new(Mutex::new(metadata_sync)),
            persistence_mode: Arc::new(Mutex::new(persistence_mode)),
            persistence_restart_required: Arc::new(AtomicBool::new(false)),
            persistence_telemetry: RolloutPersistenceTelemetry::new(thread_id),
        })
    }

    /// Create a child thread with inherited model context already durable.
    ///
    /// The boundary belongs in session metadata before the copied prefix is written so history
    /// projection can distinguish inherited context from the child's own records immediately.
    pub async fn create_with_inherited_model_context(
        thread_store: Arc<dyn ThreadStore>,
        mut params: CreateThreadParams,
        inherited_model_context: &[RolloutItem],
    ) -> ThreadStoreResult<Self> {
        let inherited_model_context = inherited_model_context
            .iter()
            .filter(|item| !matches!(item, RolloutItem::SessionMeta(_)))
            .cloned()
            .collect::<Vec<_>>();
        let persisted_prefix_item_count =
            persisted_rollout_items(&inherited_model_context, params.history_mode).len();
        let persisted_prefix_item_count =
            u64::try_from(persisted_prefix_item_count).map_err(|_| ThreadStoreError::Internal {
                message: "inherited model context is too large".to_string(),
            })?;
        params.subagent_history_start_ordinal = Some(
            params
                .initial_rollout_ordinal
                .checked_add(1)
                .and_then(|ordinal| ordinal.checked_add(persisted_prefix_item_count))
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: "inherited model context is too large".to_string(),
                })?,
        );
        let live_thread = Self::create(thread_store, params).await?;
        if let Err(err) = live_thread
            .persist_appended_items(&inherited_model_context)
            .await
        {
            if let Err(discard_err) = live_thread.discard().await {
                warn!(
                    "failed to discard thread persistence after inherited context append failed: {discard_err}"
                );
            }
            return Err(err);
        }
        Ok(live_thread)
    }

    pub async fn resume(
        thread_store: Arc<dyn ThreadStore>,
        history_mode: ThreadHistoryMode,
        params: ResumeThreadParams,
    ) -> ThreadStoreResult<Self> {
        let thread_id = params.thread_id;
        let should_load_history = params.history.is_none();
        let include_archived = params.include_archived;
        let metadata = if history_mode == ThreadHistoryMode::Paginated
            && let Some(local_store) = thread_store.as_any().downcast_ref::<LocalThreadStore>()
            && let Some(state_db) = local_store.state_db().await
        {
            state_db
                .get_thread(thread_id)
                .await
                .map_err(|err| ThreadStoreError::Internal {
                    message: format!("failed to read thread metadata for {thread_id}: {err}"),
                })?
        } else {
            None
        };
        let mut metadata_sync = ThreadMetadataSync::for_resume(&params, metadata.as_ref());
        thread_store.resume_thread(params).await?;
        if should_load_history {
            match thread_store
                .load_history(LoadThreadHistoryParams {
                    thread_id,
                    include_archived,
                })
                .await
            {
                Ok(history) => metadata_sync.record_resume_history(&history.items),
                Err(err) => {
                    if let Err(discard_err) = thread_store.discard_thread(thread_id).await {
                        warn!(
                            "failed to discard thread persistence after resume history load failed: {discard_err}"
                        );
                    }
                    return Err(err);
                }
            }
        }
        Ok(Self {
            thread_id,
            history_mode,
            thread_store,
            metadata_sync: Arc::new(Mutex::new(metadata_sync)),
            persistence_mode: Arc::new(Mutex::new(ThreadPersistenceMode::Durable)),
            persistence_restart_required: Arc::new(AtomicBool::new(false)),
            persistence_telemetry: RolloutPersistenceTelemetry::new(thread_id),
        })
    }

    #[tracing::instrument(
        level = "trace",
        skip_all,
        fields(item_count = raw_items.len())
    )]
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "item persistence and metadata publication must observe one persistence mode"
    )]
    pub async fn append_items(&self, raw_items: &[RolloutItem]) -> ThreadStoreResult<()> {
        let persistence_mode = self.persistence_mode.lock().await;
        self.ensure_persistence_available("append rollout items")?;
        let items = self.persist_appended_items(raw_items).await?;
        if items.is_empty() {
            return Ok(());
        }
        let update = self
            .metadata_sync
            .lock()
            .await
            .observe_appended_items(items.as_slice());
        if matches!(*persistence_mode, ThreadPersistenceMode::Deferred) {
            return Ok(());
        }
        if let Some(update) = update {
            self.thread_store
                .update_thread_metadata(UpdateThreadMetadataParams {
                    thread_id: self.thread_id,
                    patch: update.patch.clone(),
                    include_archived: true,
                })
                .await?;
            self.metadata_sync
                .lock()
                .await
                .mark_pending_update_applied(&update);
        }
        Ok(())
    }

    async fn persist_appended_items(
        &self,
        raw_items: &[RolloutItem],
    ) -> ThreadStoreResult<Vec<RolloutItem>> {
        // Empty appends are intentionally ignored rather than represented as zero-sized batches.
        if raw_items.is_empty() {
            return Ok(Vec::new());
        }
        let (items, measurement) = if self.persistence_telemetry.is_enabled() {
            let (items, measurement) =
                measure_and_filter_rollout_items(raw_items, self.history_mode);
            (items, Some(measurement))
        } else {
            (persisted_rollout_items(raw_items, self.history_mode), None)
        };
        self.thread_store
            .append_items(AppendThreadItemsParams {
                thread_id: self.thread_id,
                items: raw_items.to_vec(),
            })
            .await?;
        if let Some(measurement) = measurement.as_ref() {
            self.persistence_telemetry
                .record_batch(raw_items, measurement);
        }
        Ok(items)
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "the durable transition must serialize with other persistence-mode operations"
    )]
    pub async fn persist(&self, context: PersistContext) -> ThreadStoreResult<()> {
        let mut persistence_mode = self.persistence_mode.lock().await;
        self.ensure_persistence_available("persist the thread")?;
        if context == PersistContext::TurnStart {
            self.flush_pending_metadata_update_for_existing_history()
                .await?;
        }
        self.thread_store
            .persist_thread(self.thread_id, context)
            .await?;
        *persistence_mode = ThreadPersistenceMode::Durable;
        drop(persistence_mode);
        self.flush_pending_metadata_update().await
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "flush and metadata publication must observe one persistence mode"
    )]
    pub async fn flush(&self) -> ThreadStoreResult<()> {
        let persistence_mode = self.persistence_mode.lock().await;
        self.ensure_persistence_available("flush the thread")?;
        self.thread_store.flush_thread(self.thread_id).await?;
        if matches!(*persistence_mode, ThreadPersistenceMode::Deferred) {
            return Ok(());
        }
        drop(persistence_mode);
        self.flush_pending_metadata_update_for_existing_history()
            .await
    }

    /// Returns whether this thread should remain memory-only until explicitly persisted.
    pub async fn is_persistence_deferred(&self) -> bool {
        matches!(
            *self.persistence_mode.lock().await,
            ThreadPersistenceMode::Deferred
        )
    }

    /// Freezes the current local prefix for compaction or a full-history fork.
    ///
    /// Remote stores do not expose local rollout segments and return `Ok(None)`.
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "segment freezing must serialize with persistence-mode transitions"
    )]
    pub async fn freeze_local_segment(
        &self,
        params: FreezeRolloutSegmentParams,
    ) -> ThreadStoreResult<Option<FrozenRolloutSegment>> {
        let mut persistence_mode = self.persistence_mode.lock().await;
        self.ensure_persistence_available("freeze a rollout segment")?;
        let Some(local_store) = self
            .thread_store
            .as_any()
            .downcast_ref::<LocalThreadStore>()
        else {
            return Ok(None);
        };
        let freeze_result = local_store
            .freeze_thread_segment(self.thread_id, params)
            .await;
        if matches!(
            local_store.live_persistence_mode(self.thread_id).await,
            Some(ThreadPersistenceMode::Durable)
        ) {
            *persistence_mode = ThreadPersistenceMode::Durable;
        }
        let frozen = freeze_result.map(Some)?;
        drop(persistence_mode);
        self.flush_pending_metadata_update().await?;
        Ok(frozen)
    }

    /// Persists a complete segment-state checkpoint without interpreting an ordinary write error
    /// as proof that the checkpoint was not published.
    pub async fn persist_segment_checkpoint(
        &self,
        params: FreezeRolloutSegmentParams,
    ) -> SegmentCheckpointPersistenceOutcome {
        let live_thread = self.clone();
        let checkpoint_owner = tokio::spawn(async move {
            let result = AssertUnwindSafe(live_thread.persist_segment_checkpoint_inner(params))
                .catch_unwind()
                .await;
            match result {
                Ok(outcome) => outcome,
                Err(_) => {
                    let _ = live_thread.require_restart_and_discard().await;
                    SegmentCheckpointPersistenceOutcome::Indeterminate {
                        error: ThreadStoreError::Internal {
                            message: format!(
                                "checkpoint persistence owner for thread {} panicked at an indeterminate commit point",
                                live_thread.thread_id
                            ),
                        },
                    }
                }
            }
        });
        match checkpoint_owner.await {
            Ok(outcome) => outcome,
            Err(error) => {
                let _ = self.require_restart_and_discard().await;
                SegmentCheckpointPersistenceOutcome::Indeterminate {
                    error: ThreadStoreError::Internal {
                        message: format!(
                            "checkpoint persistence owner for thread {} failed: {error}",
                            self.thread_id
                        ),
                    },
                }
            }
        }
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "checkpoint persistence must serialize with persistence-mode transitions"
    )]
    async fn persist_segment_checkpoint_inner(
        &self,
        params: FreezeRolloutSegmentParams,
    ) -> SegmentCheckpointPersistenceOutcome {
        let mut persistence_mode = self.persistence_mode.lock().await;
        if let Err(error) = self.ensure_persistence_available("persist a segment checkpoint") {
            return SegmentCheckpointPersistenceOutcome::Indeterminate { error };
        }
        let mut restart_guard =
            CheckpointPersistenceRestartGuard::new(Arc::clone(&self.persistence_restart_required));
        let raw_items = params.initial_items().to_vec();
        let outcome = self
            .thread_store
            .persist_segment_checkpoint(self.thread_id, params)
            .await;
        let persisted_items = if matches!(outcome, SegmentCheckpointPersistenceOutcome::Committed) {
            let (items, measurement) = if self.persistence_telemetry.is_enabled() {
                let (items, measurement) =
                    measure_and_filter_rollout_items(raw_items.as_slice(), self.history_mode);
                (items, Some(measurement))
            } else {
                (
                    persisted_rollout_items(raw_items.as_slice(), self.history_mode),
                    None,
                )
            };
            if let Some(measurement) = measurement.as_ref() {
                self.persistence_telemetry
                    .record_batch(raw_items.as_slice(), measurement);
            }
            Some(items)
        } else {
            None
        };
        if let Some(local_store) = self
            .thread_store
            .as_any()
            .downcast_ref::<LocalThreadStore>()
            && matches!(
                local_store.live_persistence_mode(self.thread_id).await,
                Some(ThreadPersistenceMode::Durable)
            )
        {
            *persistence_mode = ThreadPersistenceMode::Durable;
        }
        match &outcome {
            SegmentCheckpointPersistenceOutcome::Indeterminate { .. } => {
                self.persistence_restart_required
                    .store(true, Ordering::Release);
                let _ = self.thread_store.discard_thread(self.thread_id).await;
            }
            SegmentCheckpointPersistenceOutcome::NotCommitted { .. } => restart_guard.disarm(),
            SegmentCheckpointPersistenceOutcome::Committed => {
                if let Some(items) = persisted_items
                    && !items.is_empty()
                {
                    self.metadata_sync
                        .lock()
                        .await
                        .observe_appended_items(items.as_slice());
                }
                if let Err(error) = self.flush_pending_metadata_update().await {
                    warn!(%error, "segment-state checkpoint committed but metadata publication failed");
                }
                restart_guard.disarm();
            }
        }
        outcome
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "shutdown must serialize its metadata decision with persistence-mode transitions"
    )]
    pub async fn shutdown(&self) -> ThreadStoreResult<()> {
        let persistence_mode = self.persistence_mode.lock().await;
        self.ensure_persistence_available("shut down thread persistence")?;
        let metadata_result = if matches!(*persistence_mode, ThreadPersistenceMode::Durable) {
            self.flush_pending_metadata_update_for_existing_history()
                .await
        } else {
            Ok(())
        };
        let shutdown_result = self.thread_store.shutdown_thread(self.thread_id).await;
        match (metadata_result, shutdown_result) {
            (Err(metadata_error), Err(shutdown_error)) => Err(ThreadStoreError::Internal {
                message: format!(
                    "thread metadata update failed: {metadata_error}; thread shutdown failed: {shutdown_error}"
                ),
            }),
            (Err(metadata_error), Ok(())) => Err(metadata_error),
            (Ok(()), result) => result,
        }
    }

    pub async fn discard(&self) -> ThreadStoreResult<()> {
        self.thread_store.discard_thread(self.thread_id).await
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "restart fencing and writer discard must exclude concurrent persistence"
    )]
    async fn require_restart_and_discard(&self) -> ThreadStoreResult<()> {
        self.persistence_restart_required
            .store(true, Ordering::Release);
        let _persistence_mode = self.persistence_mode.lock().await;
        self.thread_store.discard_thread(self.thread_id).await
    }

    fn ensure_persistence_available(&self, operation: &str) -> ThreadStoreResult<()> {
        if self.persistence_restart_required.load(Ordering::Acquire) {
            return Err(ThreadStoreError::Conflict {
                message: format!(
                    "cannot {operation} for thread {}; checkpoint persistence is indeterminate and the thread must be restarted",
                    self.thread_id
                ),
            });
        }
        Ok(())
    }

    /// Returns whether checkpoint publication lost a durability acknowledgement.
    ///
    /// Session code uses the same fence before starting a new turn so cancellation of the
    /// compaction caller cannot hide an indeterminate background publication.
    pub fn persistence_restart_required(&self) -> bool {
        self.persistence_restart_required.load(Ordering::Acquire)
    }

    pub async fn load_history(
        &self,
        include_archived: bool,
    ) -> ThreadStoreResult<StoredThreadHistory> {
        self.thread_store
            .load_history(LoadThreadHistoryParams {
                thread_id: self.thread_id,
                include_archived,
            })
            .await
    }

    pub async fn read_thread(
        &self,
        include_archived: bool,
        include_history: bool,
    ) -> ThreadStoreResult<StoredThread> {
        self.thread_store
            .read_thread(ReadThreadParams {
                thread_id: self.thread_id,
                include_archived,
                include_history,
            })
            .await
    }

    pub async fn update_memory_mode(
        &self,
        mode: ThreadMemoryMode,
        include_archived: bool,
    ) -> ThreadStoreResult<()> {
        self.flush_pending_metadata_update().await?;
        self.thread_store
            .update_thread_metadata(UpdateThreadMetadataParams {
                thread_id: self.thread_id,
                patch: ThreadMetadataPatch {
                    memory_mode: Some(mode),
                    ..Default::default()
                },
                include_archived,
            })
            .await?;
        Ok(())
    }

    /// Updates metadata while preserving this API's materialized-thread contract.
    ///
    /// Stores may successfully return no thread for a no-op update, so this reads the thread as a
    /// fallback in that case.
    pub async fn update_metadata(
        &self,
        patch: ThreadMetadataPatch,
        include_archived: bool,
    ) -> ThreadStoreResult<StoredThread> {
        self.flush_pending_metadata_update().await?;
        let updated = self
            .thread_store
            .update_thread_metadata(UpdateThreadMetadataParams {
                thread_id: self.thread_id,
                patch,
                include_archived,
            })
            .await?;
        match updated {
            Some(thread) => Ok(thread),
            None => {
                self.read_thread(include_archived, /*include_history*/ false)
                    .await
            }
        }
    }

    /// Returns the live local rollout path for legacy local-only callers.
    ///
    /// Remote stores do not expose rollout files, so they return `Ok(None)`.
    pub async fn local_rollout_path(&self) -> ThreadStoreResult<Option<PathBuf>> {
        let Some(local_store) = self
            .thread_store
            .as_any()
            .downcast_ref::<LocalThreadStore>()
        else {
            return Ok(None);
        };
        local_store
            .live_rollout_path(self.thread_id)
            .await
            .map(Some)
    }

    async fn flush_pending_metadata_update(&self) -> ThreadStoreResult<()> {
        let update = self.metadata_sync.lock().await.take_pending_update();
        self.apply_pending_metadata_update(update).await
    }

    async fn flush_pending_metadata_update_for_existing_history(&self) -> ThreadStoreResult<()> {
        let update = self
            .metadata_sync
            .lock()
            .await
            .take_pending_update_for_existing_history();
        self.apply_pending_metadata_update(update).await
    }

    async fn apply_pending_metadata_update(
        &self,
        update: Option<crate::thread_metadata_sync::PendingThreadMetadataPatch>,
    ) -> ThreadStoreResult<()> {
        let Some(update) = update else {
            return Ok(());
        };
        self.thread_store
            .update_thread_metadata(UpdateThreadMetadataParams {
                thread_id: self.thread_id,
                patch: update.patch.clone(),
                include_archived: true,
            })
            .await?;
        self.metadata_sync
            .lock()
            .await
            .mark_pending_update_applied(&update);
        Ok(())
    }
}
