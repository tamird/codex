//! Repairs the closed alpha6 goal-supervisor persistence defect before history replay.
//!
//! Repair publishes only ordinary rollout files. Changed immutable segments are installed from
//! leaves toward their mutable root, and the root is replaced last. The transform is
//! deterministic, so a crash leaves either the old graph or a graph that a later scan can finish;
//! no repair journal or additional database is required.

use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;

use super::LocalThreadStore;
use super::RolloutWriterReservation;
use super::goal_supervisor_history_repair::GoalSupervisorLineageProvenance;
use super::goal_supervisor_history_repair::SELECTED_MISSING_MESSAGE_ID;
use super::goal_supervisor_history_repair::reject_malformed_goal_supervisor_supplied_history;
use super::goal_supervisor_history_repair::repair_legacy_goal_supervisor_jsonl_lines_selected_with_provenance;
use super::goal_supervisor_history_repair::repair_legacy_goal_supervisor_jsonl_lines_with_provenance;
use super::goal_supervisor_history_repair::repair_legacy_goal_supervisor_lines_with_provenance;
use super::goal_supervisor_history_repair::rewrite_rollout_jsonl_same_length;
use super::goal_supervisor_history_repair::selected_goal_supervisor_candidate_ids_from_jsonl;
use super::segment::history_repair_publication::HistoryRepairLifecycleLease;
use super::segment::history_repair_publication::HistoryRepairMaintenanceLease;
use super::segment::history_repair_publication::HistoryRepairPublication;
use super::segment::history_repair_publication::HistoryRepairWriterToken;
use super::segment::history_repair_publication::acquire_history_repair_maintenance;
use super::segment::history_repair_publication::authorize_history_repair_writer;
use super::segment::history_repair_publication::clear_history_repair_segment_id;
use super::segment::history_repair_publication::history_repair_publication_needs_exclusive;
use super::segment::history_repair_publication::history_repair_segment_id;
use super::segment::history_repair_publication::install_existing_identity_history_repair_backup;
use super::segment::history_repair_publication::install_history_repair_segment;
use super::segment::history_repair_publication::publish_compressed_history_repair_replacement;
use super::segment::history_repair_publication::publish_history_repair_replacement;
use super::segment::history_repair_publication::recover_history_repair_publication;
use super::segment::history_repair_publication::replace_history_repair_segment_id;
use super::segment::history_repair_publication::reserve_history_repair_lifecycle;
use super::segment::history_repair_publication::validate_legacy_initial_repair_path;
use super::writer_lock::WriterLockGuard;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use sha2::Digest as _;
use sha2::Sha256;

/// Process-wide restart quarantine keyed by canonical `CODEX_HOME`.
///
/// A replacement with unknown directory durability can disappear after a crash. Every store in
/// this process must therefore reject that rollout until the process restarts.
static INDETERMINATE_HISTORY_REPAIRS: LazyLock<Mutex<HashMap<PathBuf, HashSet<ThreadId>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The result of one repair publication.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct GoalSupervisorHistoryRepairOutcome {
    pub(super) repaired_messages: usize,
    pub(super) installed_segments: usize,
    pub(super) active_rollout_replaced: bool,
}

/// Ownership retained from a dirty repair until the caller consumes or opens the rollout.
///
/// Clean and dirty histories keep every mutable owner stable after the locked rescan so a cached
/// affected process cannot append the malformed representation between validation and access.
/// The existing rollout-maintenance lock also prevents compression from replacing the selected
/// physical file before the caller opens it.
pub(super) struct GoalSupervisorHistoryAccess {
    lifecycle: Vec<HistoryRepairLifecycleLease>,
    maintenance: Option<HistoryRepairMaintenanceLease>,
    /// Excludes legacy maintenance and compression without authorizing repair publication.
    read_maintenance: Option<codex_rollout::RolloutMaintenanceReadGuard>,
    reservation: Option<RolloutWriterReservation>,
    certified_active_snapshot: Option<CertifiedActiveHistorySnapshot>,
}

pub(super) struct CertifiedActiveHistorySnapshot {
    pub(super) rollout_path: PathBuf,
    pub(super) end_byte_offset: u64,
    pub(super) head: super::rollout_lineage::RolloutHead,
    pub(super) scan: super::model_context::ActiveModelContextScan,
}

impl std::fmt::Debug for GoalSupervisorHistoryAccess {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GoalSupervisorHistoryAccess")
            .field("lifecycle_thread_count", &self.lifecycle.len())
            .field(
                "holds_maintenance",
                &(self.maintenance.is_some() || self.read_maintenance.is_some()),
            )
            .field("holds_writer_reservation", &self.reservation.is_some())
            .field(
                "has_certified_active_snapshot",
                &self.certified_active_snapshot.is_some(),
            )
            .finish()
    }
}

impl GoalSupervisorHistoryAccess {
    pub(super) fn clean() -> Self {
        Self {
            lifecycle: Vec::new(),
            maintenance: None,
            read_maintenance: None,
            reservation: None,
            certified_active_snapshot: None,
        }
    }

    pub(super) fn writer_reservation(&self) -> Option<&RolloutWriterReservation> {
        self.reservation.as_ref()
    }

    pub(super) fn take_certified_active_snapshot(
        &mut self,
    ) -> Option<CertifiedActiveHistorySnapshot> {
        self.certified_active_snapshot.take()
    }

    pub(super) fn take_writer_lock(&mut self, thread_id: ThreadId) -> Option<WriterLockGuard> {
        self.reservation
            .as_mut()
            .and_then(|reservation| reservation.take_cross_process_guard(thread_id))
    }

    pub(super) fn take_lifecycle(
        &mut self,
        thread_id: ThreadId,
    ) -> Option<tokio::sync::OwnedRwLockReadGuard<()>> {
        let index = self
            .lifecycle
            .iter()
            .position(|lease| lease.thread_id() == thread_id)?;
        Some(self.lifecycle.swap_remove(index).into_guard())
    }

    pub(super) fn is_reserved(&self) -> bool {
        self.reservation.is_some()
    }

    pub(super) async fn writer_token(
        &self,
        store: &LocalThreadStore,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<HistoryRepairWriterToken<'_>> {
        let maintenance = self
            .maintenance
            .as_ref()
            .ok_or_else(|| ThreadStoreError::Internal {
                message: format!(
                    "goal-supervisor publication is missing maintenance ownership for thread {thread_id}"
                ),
            })?;
        let Some(lifecycle) = self
            .lifecycle
            .iter()
            .find(|lease| lease.thread_id() == thread_id)
        else {
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "goal-supervisor publication is missing lifecycle ownership for thread {thread_id}"
                ),
            });
        };
        let reservation = self
            .reservation
            .as_ref()
            .filter(|reservation| reservation.contains(thread_id))
            .ok_or_else(|| ThreadStoreError::Internal {
                message: format!(
                    "goal-supervisor publication is missing writer ownership for thread {thread_id}"
                ),
            })?;
        authorize_history_repair_writer(store, thread_id, maintenance, lifecycle, reservation).await
    }
}

/// One mutable rollout root consumed by an operation.
#[derive(Clone, Debug, Eq, PartialEq)]
struct RepairRoot {
    thread_id: ThreadId,
    path: PathBuf,
    end_byte_offset: Option<u64>,
    source_sha256: String,
    source_was_compressed: bool,
    include_references: bool,
    reference_policy: ReferencePolicy,
    needs_repair: bool,
    lock_thread_ids: Vec<ThreadId>,
}

/// The mutable roots and writer owners that must remain stable during compatibility repair.
struct RepairScope {
    roots: Vec<RepairRoot>,
    certified_active_snapshot: Option<CertifiedActiveHistorySnapshot>,
}

impl RepairScope {
    fn needs_repair(&self) -> bool {
        self.roots.iter().any(|root| root.needs_repair)
    }

    fn lock_thread_ids(&self) -> Vec<ThreadId> {
        let mut ids = self
            .roots
            .iter()
            .flat_map(|root| root.lock_thread_ids.iter().copied())
            .collect::<Vec<_>>();
        ids.sort_unstable_by_key(ThreadId::to_string);
        ids.dedup();
        ids
    }
}

#[derive(Clone, Debug, Default)]
struct ReferenceScan {
    needs_repair: bool,
    lock_thread_ids: Vec<ThreadId>,
    selected_candidate_ids: HashSet<String>,
}

#[derive(Debug)]
struct ReferenceRepair {
    reference: RolloutReferenceItem,
    repaired_messages: usize,
    installed_segments: usize,
}

struct RepairFrame {
    reference: RolloutReferenceItem,
    thread_id: ThreadId,
    resolved_path: PathBuf,
    source: Vec<u8>,
    lines: Vec<RolloutLine>,
    next_line: usize,
    direct_repaired_messages: usize,
    repaired_messages: usize,
    installed_segments: usize,
    nested_reference_changed: bool,
    graph_depth: usize,
    ordinary_reference_depth: usize,
    reference_policy: ReferencePolicy,
    provenance: GoalSupervisorLineageProvenance,
    selected_message_ids: Option<Arc<HashSet<String>>>,
    mutable_fallback: bool,
}

/// Rejects poisoned caller-supplied history because it has no durable source to repair.
pub(super) fn reject_malformed_supplied_history(items: &[RolloutItem]) -> ThreadStoreResult<()> {
    reject_malformed_goal_supervisor_supplied_history(items)
}

/// Repairs every mutable root consumed by compatibility replay.
#[cfg(test)]
pub(super) async fn repair_compatibility_history_before_access(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &Path,
) -> ThreadStoreResult<GoalSupervisorHistoryAccess> {
    repair_before_access(
        store,
        thread_id,
        rollout_path,
        RepairAccess::Compatibility,
        HistorySelection::Exact,
    )
    .await
}

/// Repairs only the bounded reference window used by ordinary thread history reads.
pub(super) async fn repair_recent_history_before_access(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &Path,
) -> ThreadStoreResult<GoalSupervisorHistoryAccess> {
    repair_before_access(
        store,
        thread_id,
        rollout_path,
        RepairAccess::Recent,
        HistorySelection::Exact,
    )
    .await
}

/// Repairs only the active root for an authoritative active checkpoint.
#[cfg(test)]
pub(super) async fn repair_active_history_before_access(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &Path,
) -> ThreadStoreResult<GoalSupervisorHistoryAccess> {
    repair_before_access(
        store,
        thread_id,
        rollout_path,
        RepairAccess::ActiveOnly,
        HistorySelection::Exact,
    )
    .await
}

/// Reject a replaced selection instead of reading or repairing an older retained rollout.
pub(super) async fn repair_selected_history_before_access(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &Path,
    access: RepairAccess,
) -> ThreadStoreResult<GoalSupervisorHistoryAccess> {
    repair_before_access(
        store,
        thread_id,
        rollout_path,
        access,
        HistorySelection::Current,
    )
    .await
}

/// Determines which records the caller will consume after repair.
#[derive(Clone, Copy)]
pub(super) enum RepairAccess {
    ActiveOnly,
    Compatibility,
    Recent,
}

/// Explicit historical reads must not follow a thread's newer selected rollout.
#[derive(Clone, Copy)]
enum HistorySelection {
    Current,
    Exact,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReferencePolicy {
    Complete,
    Recent,
}

async fn repair_before_access(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &Path,
    access: RepairAccess,
    selection: HistorySelection,
) -> ThreadStoreResult<GoalSupervisorHistoryAccess> {
    let home_key = canonical_home_key(store)?;
    if quarantined_thread(home_key.as_path(), thread_id) {
        return Err(indeterminate_repair_error(thread_id));
    }
    if let Err(confinement_error) = confined_rollout_path(store, rollout_path).await {
        return clean_unconfined_access_or_error(thread_id, rollout_path, confinement_error).await;
    }
    let preflight_reference_policy = if matches!(access, RepairAccess::Recent) {
        ReferencePolicy::Recent
    } else {
        ReferencePolicy::Complete
    };
    if !matches!(access, RepairAccess::ActiveOnly) {
        let (preflight, _) = scan_root_for_access(
            store,
            thread_id,
            rollout_path,
            access,
            preflight_reference_policy,
        )
        .await?;
        if preflight.needs_repair && is_immutable_segment(store, rollout_path).await? {
            return Err(ThreadStoreError::Conflict {
                message: format!(
                    "immutable rollout root {} requires repair through its mutable parent",
                    rollout_path.display()
                ),
            });
        }
    }
    let mut lock_ids = vec![thread_id];
    let mut exclusive = false;
    let mut scope_changes = 0;

    // Lifecycle ownership precedes maintenance. The first discovery runs while the selected
    // thread is stable. A cross-thread lineage expands the lock set and is then rediscovered with
    // every owner stable; a same-thread rotation lineage needs only the one locked scan.
    loop {
        let mut lifecycle = Vec::with_capacity(lock_ids.len());
        for &id in &lock_ids {
            lifecycle.push(reserve_history_repair_lifecycle(store, id).await);
        }
        let (maintenance, read_maintenance) = if exclusive {
            (Some(acquire_maintenance(store).await?), None)
        } else {
            (
                None,
                Some(
                    codex_rollout::acquire_rollout_maintenance_read_lock(
                        store.config.codex_home.as_path(),
                    )
                    .await
                    .map_err(thread_store_io_error)?,
                ),
            )
        };
        let reservation = store.reserve_rollout_writers(lock_ids.as_slice()).await?;
        reject_quarantined_scope(home_key.as_path(), lock_ids.as_slice())?;
        let mut access_token = GoalSupervisorHistoryAccess {
            lifecycle,
            maintenance,
            read_maintenance,
            reservation: Some(reservation),
            certified_active_snapshot: None,
        };
        if matches!(selection, HistorySelection::Current) {
            super::live_writer::require_selected_rollout_path(store, thread_id, rollout_path)
                .await?;
        }
        let locked_rollout_path = if matches!(selection, HistorySelection::Exact)
            || codex_rollout::existing_rollout_path(rollout_path)
                .await
                .is_some()
        {
            rollout_path.to_path_buf()
        } else {
            super::thread_rollout_resolver::resolve_current_including_archived(store, thread_id)
                .await?
                .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?
                .path
        };
        let locked_scope =
            discover_scope(store, thread_id, locked_rollout_path.as_path(), access).await?;
        let discovered_lock_ids = locked_scope.lock_thread_ids();
        if discovered_lock_ids != lock_ids {
            drop(access_token);
            scope_changes += 1;
            if scope_changes >= 3 {
                return Err(ThreadStoreError::Conflict {
                    message: format!(
                        "goal-supervisor history for thread {thread_id} changed while repair locks were acquired"
                    ),
                });
            }
            lock_ids = discovered_lock_ids;
            continue;
        }
        reject_quarantined_scope(home_key.as_path(), lock_ids.as_slice())?;
        if !exclusive
            && (locked_scope.needs_repair()
                || scope_needs_exclusive_recovery(store, &locked_scope).await?)
        {
            // Never upgrade while retaining a writer that another shared reader may await.
            drop(access_token);
            exclusive = true;
            continue;
        }
        if exclusive {
            recover_interrupted_publications(store, &locked_scope, &access_token).await?;
        }

        if !locked_scope.needs_repair() {
            access_token.certified_active_snapshot = locked_scope.certified_active_snapshot;
            return Ok(access_token);
        }
        for &id in &lock_ids {
            store.ensure_live_recorder_absent(id).await?;
        }

        // Publication uses blocking descriptor operations. A detached owner retains every lock
        // and records any durability quarantine even if the requesting task is cancelled.
        let owned_store = store.clone();
        return tokio::spawn(async move {
            repair_scope_locked(&owned_store, thread_id, &locked_scope, &access_token).await?;
            Ok(access_token)
        })
        .await
        .map_err(|error| ThreadStoreError::Internal {
            message: format!("failed to join goal-supervisor history repair: {error}"),
        })?;
    }
}

async fn scope_needs_exclusive_recovery(
    store: &LocalThreadStore,
    scope: &RepairScope,
) -> ThreadStoreResult<bool> {
    for root in &scope.roots {
        let physical = codex_rollout::existing_rollout_path(&root.path)
            .await
            .ok_or_else(|| ThreadStoreError::Conflict {
                message: format!("rollout {} changed before repair", root.path.display()),
            })?;
        if !is_immutable_segment(store, &physical).await?
            && history_repair_publication_needs_exclusive(&store.config.codex_home, &physical)
                .await?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn clean_unconfined_access_or_error(
    thread_id: ThreadId,
    rollout_path: &Path,
    confinement_error: ThreadStoreError,
) -> ThreadStoreResult<GoalSupervisorHistoryAccess> {
    let source = read_plain_source(rollout_path).await?;
    let (lines, count) = repair_legacy_goal_supervisor_jsonl_lines_with_provenance(
        source.as_slice(),
        GoalSupervisorLineageProvenance::Untrusted,
    )
    .map_err(|error| annotate_path(error, rollout_path))?;
    validate_root_thread_id(lines.as_slice(), thread_id, rollout_path)?;
    let depends_on_other_rollouts = lines.iter().any(|line| {
        matches!(line.item, RolloutItem::RolloutReference(_))
            || matches!(
                &line.item,
                RolloutItem::SessionMeta(meta) if meta.meta.history_base.is_some()
            )
    });
    if count.total() != 0 || depends_on_other_rollouts {
        return Err(confinement_error);
    }
    // Existing explicit-path APIs support clean noncanonical rollouts. They remain readable, but
    // any candidate requiring mutation or lineage traversal fails before a repair lock or artifact
    // is created because descriptor-confined publication is unavailable outside canonical roots.
    Ok(GoalSupervisorHistoryAccess::clean())
}

async fn recover_interrupted_publications(
    store: &LocalThreadStore,
    scope: &RepairScope,
    access: &GoalSupervisorHistoryAccess,
) -> ThreadStoreResult<()> {
    for root in &scope.roots {
        let physical = codex_rollout::existing_rollout_path(root.path.as_path())
            .await
            .ok_or_else(|| ThreadStoreError::Conflict {
                message: format!("rollout {} changed before repair", root.path.display()),
            })?;
        if is_immutable_segment(store, physical.as_path()).await? {
            continue;
        }
        let writer = access.writer_token(store, root.thread_id).await?;
        recover_history_repair_publication(
            &writer,
            store.config.codex_home.as_path(),
            root.thread_id,
            physical.as_path(),
        )
        .await?;
    }
    Ok(())
}

fn indeterminate_repair_error(thread_id: ThreadId) -> ThreadStoreError {
    ThreadStoreError::Conflict {
        message: format!(
            "goal-supervisor history repair for thread {thread_id} has indeterminate durability; restart before continuing"
        ),
    }
}

async fn acquire_maintenance(
    store: &LocalThreadStore,
) -> ThreadStoreResult<HistoryRepairMaintenanceLease> {
    acquire_history_repair_maintenance(store).await
}

async fn discover_scope(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &Path,
    access: RepairAccess,
) -> ThreadStoreResult<RepairScope> {
    let rollout_path = confined_rollout_path(store, rollout_path).await?;
    let reference_policy = if matches!(access, RepairAccess::Recent) {
        ReferencePolicy::Recent
    } else {
        ReferencePolicy::Complete
    };
    let (root, certified_active_snapshot) = scan_root_for_access(
        store,
        thread_id,
        rollout_path.as_path(),
        access,
        reference_policy,
    )
    .await?;
    if root.needs_repair && is_immutable_segment(store, rollout_path.as_path()).await? {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "immutable rollout root {} requires repair through its mutable parent",
                rollout_path.display()
            ),
        });
    }
    if matches!(access, RepairAccess::ActiveOnly) {
        return Ok(RepairScope {
            roots: vec![root],
            certified_active_snapshot,
        });
    }

    if matches!(access, RepairAccess::Recent) {
        return Ok(RepairScope {
            roots: vec![root],
            certified_active_snapshot: None,
        });
    }
    let session_meta = codex_rollout::read_session_meta_line(rollout_path.as_path())
        .await
        .map_err(thread_store_io_error)?;
    if session_meta.meta.history_mode != codex_protocol::protocol::ThreadHistoryMode::Paginated {
        return Ok(RepairScope {
            roots: vec![root],
            certified_active_snapshot: None,
        });
    }
    let lineage = store
        .resolve_rollout_lineage_from_path(thread_id, rollout_path.as_path())
        .await?;
    let mut lineage_thread_ids = lineage
        .segments()
        .iter()
        .map(super::rollout_lineage::RolloutLineageSegment::thread_id)
        .collect::<Vec<_>>();
    lineage_thread_ids.push(thread_id);
    lineage_thread_ids.sort_unstable_by_key(ThreadId::to_string);
    lineage_thread_ids.dedup();
    let mut roots = Vec::new();
    let mut seen = HashSet::new();
    for segment in lineage.segments() {
        let physical_path = segment.rollout_path();
        let path = codex_rollout::plain_rollout_path(physical_path);
        if is_immutable_segment(store, physical_path).await? || !seen.insert(path.clone()) {
            continue;
        }
        let end_byte_offset = segment.jsonl_end_byte_offset();
        roots.push(
            scan_root(
                store,
                segment.thread_id(),
                physical_path,
                end_byte_offset,
                /*include_references*/ true,
                reference_policy,
            )
            .await?,
        );
    }
    if roots.is_empty() || !roots.iter().any(|candidate| candidate.path == root.path) {
        roots.push(root);
    }
    // Fork preparation has always reserved every stable thread identity in the consumed lineage,
    // including identities represented only by immutable segments. Preserve that reservation
    // while history repair owns the writer locks so decompression and reference materialization
    // cannot discover an owner outside the locked set.
    roots[0].lock_thread_ids.extend(lineage_thread_ids);
    roots[0]
        .lock_thread_ids
        .sort_unstable_by_key(ThreadId::to_string);
    roots[0].lock_thread_ids.dedup();
    Ok(RepairScope {
        roots,
        certified_active_snapshot: None,
    })
}

async fn scan_root_for_access(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: &Path,
    access: RepairAccess,
    reference_policy: ReferencePolicy,
) -> ThreadStoreResult<(RepairRoot, Option<CertifiedActiveHistorySnapshot>)> {
    if matches!(access, RepairAccess::ActiveOnly)
        && let Some((root, snapshot)) =
            scan_clean_certified_active_root(thread_id, rollout_path, reference_policy).await?
    {
        return Ok((root, Some(snapshot)));
    }
    let root = scan_root(
        store,
        thread_id,
        rollout_path,
        /*end_byte_offset*/ None,
        /*include_references*/ !matches!(access, RepairAccess::ActiveOnly),
        reference_policy,
    )
    .await?;
    Ok((root, None))
}

/// Proves that the model-visible active suffix contains no affected Goal-supervisor envelope.
///
/// A certified segment checkpoint makes older active-file records irrelevant to latest-state
/// reconstruction. Inspecting only that suffix avoids buffering a multi-gigabyte active JSONL on
/// every resume or latest fork. An invalid or absent checkpoint, a parse rejection, or detected
/// legacy damage falls back to the complete physical repair scan.
async fn scan_clean_certified_active_root(
    thread_id: ThreadId,
    rollout_path: &Path,
    reference_policy: ReferencePolicy,
) -> ThreadStoreResult<Option<(RepairRoot, CertifiedActiveHistorySnapshot)>> {
    let physical_path = codex_rollout::existing_rollout_path(rollout_path)
        .await
        .ok_or_else(|| ThreadStoreError::InvalidRequest {
            message: format!("rollout {} does not exist", rollout_path.display()),
        })?;
    if physical_path.extension().and_then(|value| value.to_str()) != Some("jsonl") {
        return Ok(None);
    }
    let head = super::rollout_lineage::read_rollout_head(physical_path.as_path()).await?;
    let session_meta = head.session_meta.clone();
    let Some(scan) = super::model_context::scan_plain_active_model_context_snapshot(
        physical_path.as_path(),
        session_meta.clone(),
    )
    .await?
    else {
        return Ok(None);
    };
    if !scan.segment_checkpoint {
        return Ok(None);
    }
    let end_byte_offset = tokio::fs::metadata(physical_path.as_path())
        .await
        .map_err(thread_store_io_error)?
        .len();
    let mut bounded_lines = scan.suffix_lines.clone();
    if session_meta.meta.id != thread_id {
        return Ok(None);
    }
    bounded_lines.insert(
        0,
        RolloutLine {
            timestamp: String::new(),
            ordinal: None,
            item: RolloutItem::SessionMeta(session_meta),
        },
    );
    let repair_count = repair_legacy_goal_supervisor_lines_with_provenance(
        bounded_lines.as_mut_slice(),
        GoalSupervisorLineageProvenance::Untrusted,
    )
    .map_err(|error| annotate_path(error, rollout_path))?;
    if repair_count.total() != 0 {
        return Ok(None);
    }
    let root = RepairRoot {
        thread_id,
        path: codex_rollout::plain_rollout_path(rollout_path),
        end_byte_offset: None,
        source_sha256: String::new(),
        source_was_compressed: false,
        include_references: false,
        reference_policy,
        needs_repair: false,
        lock_thread_ids: vec![thread_id],
    };
    Ok(Some((
        root,
        CertifiedActiveHistorySnapshot {
            rollout_path: physical_path,
            end_byte_offset,
            head,
            scan,
        },
    )))
}

async fn scan_root(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    path: &Path,
    end_byte_offset: Option<u64>,
    include_references: bool,
    reference_policy: ReferencePolicy,
) -> ThreadStoreResult<RepairRoot> {
    let physical_path = codex_rollout::existing_rollout_path(path)
        .await
        .ok_or_else(|| ThreadStoreError::InvalidRequest {
            message: format!("rollout {} does not exist", path.display()),
        })?;
    let source = read_plain_source(physical_path.as_path()).await?;
    let consumed = consumed_source(source.as_slice(), end_byte_offset, path)?;
    let (lines, count) = repair_legacy_goal_supervisor_jsonl_lines_with_provenance(
        consumed,
        GoalSupervisorLineageProvenance::Untrusted,
    )
    .map_err(|error| annotate_path(error, path))?;
    validate_root_thread_id(lines.as_slice(), thread_id, path)?;
    let mut reference_scan = ReferenceScan::default();
    if include_references {
        let provenance = GoalSupervisorLineageProvenance::Untrusted.continued_through(&lines);
        let references = lines.iter().filter_map(|line| match &line.item {
            RolloutItem::RolloutReference(reference) => Some(reference),
            _ => None,
        });
        for reference in references {
            let boundary = reference_thread_id(reference)? != thread_id
                || reference.nth_user_message.is_some();
            if reference_policy == ReferencePolicy::Recent
                && !boundary
                && reference.max_depth.min(DEFAULT_ROLLOUT_REFERENCE_DEPTH) == 0
            {
                continue;
            }
            merge_reference_scan(
                &mut reference_scan,
                scan_reference(
                    store,
                    thread_id,
                    reference,
                    provenance.continued_through_reference(reference),
                    reference_policy,
                    selected_message_ids_for_reference(
                        store,
                        lines.as_slice(),
                        reference,
                        reference_policy,
                        /*inherited_selection*/ None,
                    )
                    .await?,
                )
                .await?,
            )?;
        }
    }
    let needs_repair = count.total() != 0 || reference_scan.needs_repair;
    if needs_repair && root_segment_id(&lines)?.is_none() {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "cannot repair segmentless rollout {} without a byte-exact standard backup",
                path.display()
            ),
        });
    }
    let mut lock_thread_ids = reference_scan.lock_thread_ids;
    lock_thread_ids.push(thread_id);
    lock_thread_ids.sort_unstable_by_key(ThreadId::to_string);
    lock_thread_ids.dedup();
    Ok(RepairRoot {
        thread_id,
        path: codex_rollout::plain_rollout_path(path),
        end_byte_offset,
        source_sha256: sha256(source.as_slice()),
        source_was_compressed: physical_path != codex_rollout::plain_rollout_path(&physical_path),
        include_references,
        reference_policy,
        needs_repair,
        lock_thread_ids,
    })
}

async fn scan_reference(
    store: &LocalThreadStore,
    parent_thread_id: ThreadId,
    reference: &RolloutReferenceItem,
    inherited: GoalSupervisorLineageProvenance,
    reference_policy: ReferencePolicy,
    selected_message_ids: Option<Arc<HashSet<String>>>,
) -> ThreadStoreResult<ReferenceScan> {
    struct ScanFrame {
        reference: RolloutReferenceItem,
        thread_id: ThreadId,
        path: PathBuf,
        lines: Vec<RolloutLine>,
        next_line: usize,
        direct_repair: bool,
        nested_repair: bool,
        graph_depth: usize,
        ordinary_reference_depth: usize,
        reference_policy: ReferencePolicy,
        provenance: GoalSupervisorLineageProvenance,
        selected_message_ids: Option<Arc<HashSet<String>>>,
        lock_thread_ids: Vec<ThreadId>,
        selected_candidate_ids: HashSet<String>,
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the iterative lineage scan carries each bounded traversal constraint explicitly"
    )]
    async fn load_scan_frame(
        store: &LocalThreadStore,
        reference: RolloutReferenceItem,
        inherited: GoalSupervisorLineageProvenance,
        graph_depth: usize,
        ordinary_reference_depth: usize,
        reference_policy: ReferencePolicy,
        selected_message_ids: Option<Arc<HashSet<String>>>,
        active: &mut HashSet<PathBuf>,
    ) -> ThreadStoreResult<Option<ScanFrame>> {
        let thread_id = reference_thread_id(&reference)?;
        let path = match codex_rollout::resolve_rollout_reference_path(
            store.config.codex_home.as_path(),
            &reference,
        )
        .await
        {
            Ok(path) => path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(thread_store_io_error(error)),
        };
        // Immutable bytes can be authenticated without blocking the owner's live writer. If this
        // scan finds a repair, completion adds `thread_id` and the outer discovery loop repeats
        // with that writer reserved before publishing any replacement.
        let lock_thread_ids = if is_immutable_segment(store, path.as_path()).await? {
            Vec::new()
        } else {
            vec![thread_id]
        };
        if !active.insert(path.clone()) {
            return Err(cycle_error(path.as_path()));
        }
        // Native immutable rollouts use a distinct physical rollout ID without a legacy
        // segment ID. Only a legacy initial reference is restricted to the initial directory.
        if reference.segment_id.is_none()
            && reference
                .rollout_id
                .is_none_or(|rollout_id| rollout_id == thread_id)
        {
            validate_legacy_initial_repair_path(
                store.config.codex_home.as_path(),
                thread_id,
                path.as_path(),
            )
            .await?;
        }
        let source = read_plain_source(path.as_path()).await?;
        let selected_candidate_ids = selected_message_ids
            .as_deref()
            .map(|selected| {
                selected_goal_supervisor_candidate_ids_from_jsonl(source.as_slice(), selected)
            })
            .transpose()?
            .unwrap_or_default();
        let (lines, count) = match selected_message_ids.as_deref() {
            Some(selected) => repair_legacy_goal_supervisor_jsonl_lines_selected_with_provenance(
                source.as_slice(),
                inherited,
                selected,
            ),
            None => repair_legacy_goal_supervisor_jsonl_lines_with_provenance(
                source.as_slice(),
                inherited,
            ),
        }
        .map_err(|error| annotate_path(error, path.as_path()))?;
        let provenance = inherited.continued_through(&lines);
        Ok(Some(ScanFrame {
            reference,
            thread_id,
            path,
            lines,
            next_line: 0,
            direct_repair: count.total() != 0,
            nested_repair: false,
            graph_depth,
            ordinary_reference_depth,
            reference_policy,
            provenance,
            selected_message_ids,
            lock_thread_ids,
            selected_candidate_ids,
        }))
    }

    let root_thread_id = reference_thread_id(reference)?;
    let root_boundary = root_thread_id != parent_thread_id || reference.nth_user_message.is_some();
    let mut active = HashSet::new();
    let Some(root_frame) = load_scan_frame(
        store,
        reference.clone(),
        inherited,
        usize::from(root_boundary),
        usize::from(!root_boundary),
        reference_policy,
        selected_message_ids,
        &mut active,
    )
    .await?
    else {
        return Ok(ReferenceScan::default());
    };
    let mut frames = vec![root_frame];
    let mut result = ReferenceScan::default();
    loop {
        let Some(frame) = frames.last_mut() else {
            return Err(repair_error("reference scan stack is empty"));
        };
        let nested = frame.lines[frame.next_line..]
            .iter()
            .position(|line| matches!(line.item, RolloutItem::RolloutReference(_)))
            .map(|offset| frame.next_line + offset);
        if let Some(index) = nested {
            frame.next_line = index + 1;
            let RolloutItem::RolloutReference(reference) = &frame.lines[index].item else {
                unreachable!();
            };
            let reference = reference.clone();
            let nested_thread = reference_thread_id(&reference)?;
            let boundary = nested_thread != frame.thread_id || reference.nth_user_message.is_some();
            if frame.reference_policy == ReferencePolicy::Recent
                && !boundary
                && frame.ordinary_reference_depth
                    >= reference.max_depth.min(DEFAULT_ROLLOUT_REFERENCE_DEPTH)
            {
                continue;
            }
            if boundary && frame.graph_depth >= MAX_ROLLOUT_REFERENCE_DEPTH {
                return Err(depth_error());
            }
            let inherited = frame.provenance.continued_through_reference(&reference);
            let graph_depth = frame.graph_depth + usize::from(boundary);
            let ordinary_reference_depth = frame.ordinary_reference_depth + usize::from(!boundary);
            let reference_policy = frame.reference_policy;
            let selected_message_ids = selected_message_ids_for_reference(
                store,
                frame.lines.as_slice(),
                &reference,
                reference_policy,
                frame.selected_message_ids.as_ref(),
            )
            .await?;
            if let Some(nested_frame) = load_scan_frame(
                store,
                reference,
                inherited,
                graph_depth,
                ordinary_reference_depth,
                reference_policy,
                selected_message_ids,
                &mut active,
            )
            .await?
            {
                frames.push(nested_frame);
            }
            continue;
        }

        let mut completed = frames
            .pop()
            .ok_or_else(|| repair_error("reference scan stack is empty"))?;
        let changed = completed.direct_repair || completed.nested_repair;
        if changed && !completed.lock_thread_ids.contains(&completed.thread_id) {
            completed.lock_thread_ids.push(completed.thread_id);
        }
        active.remove(&completed.path);
        if completed.reference.segment_id.is_none() && changed {
            return Err(ThreadStoreError::Conflict {
                message: format!(
                    "cannot repair legacy initial {} without a byte-exact standard backup",
                    completed.path.display()
                ),
            });
        }
        if let Some(parent) = frames.last_mut() {
            reject_cross_segment_selected_duplicates(
                &parent.selected_candidate_ids,
                &completed.selected_candidate_ids,
            )?;
            parent.nested_repair |= changed;
            parent
                .lock_thread_ids
                .extend(completed.lock_thread_ids.iter().copied());
            parent
                .selected_candidate_ids
                .extend(completed.selected_candidate_ids.iter().cloned());
        } else {
            result.needs_repair = changed;
            result.lock_thread_ids = completed.lock_thread_ids;
            result
                .lock_thread_ids
                .sort_unstable_by_key(ThreadId::to_string);
            result.lock_thread_ids.dedup();
            result.selected_candidate_ids = completed.selected_candidate_ids;
            return Ok(result);
        }
    }
}

fn merge_reference_scan(
    target: &mut ReferenceScan,
    source: ReferenceScan,
) -> ThreadStoreResult<()> {
    reject_cross_segment_selected_duplicates(
        &target.selected_candidate_ids,
        &source.selected_candidate_ids,
    )?;
    target.needs_repair |= source.needs_repair;
    target.lock_thread_ids.extend(source.lock_thread_ids);
    target
        .selected_candidate_ids
        .extend(source.selected_candidate_ids);
    Ok(())
}

fn reject_cross_segment_selected_duplicates(
    left: &HashSet<String>,
    right: &HashSet<String>,
) -> ThreadStoreResult<()> {
    if let Some(id) = left.intersection(right).next() {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "selected goal-supervisor message ID {id:?} occurs in more than one rollout segment"
            ),
        });
    }
    Ok(())
}

async fn repair_scope_locked(
    store: &LocalThreadStore,
    requested_thread_id: ThreadId,
    scope: &RepairScope,
    access: &GoalSupervisorHistoryAccess,
) -> ThreadStoreResult<GoalSupervisorHistoryRepairOutcome> {
    let mut outcome = GoalSupervisorHistoryRepairOutcome::default();
    // RolloutLineage is oldest-to-newest. A history_base child is published only after the root it
    // consumes, so an interrupted run remains replayable and a retry finishes the suffix.
    for root in &scope.roots {
        if !root.needs_repair {
            continue;
        }
        let repaired = repair_root_locked(store, requested_thread_id, root, access).await?;
        outcome.repaired_messages += repaired.repaired_messages;
        outcome.installed_segments += repaired.installed_segments;
        outcome.active_rollout_replaced |= repaired.active_rollout_replaced;
    }
    Ok(outcome)
}

async fn repair_root_locked(
    store: &LocalThreadStore,
    requested_thread_id: ThreadId,
    root: &RepairRoot,
    access: &GoalSupervisorHistoryAccess,
) -> ThreadStoreResult<GoalSupervisorHistoryRepairOutcome> {
    let writer = access.writer_token(store, root.thread_id).await?;
    let existing = codex_rollout::existing_rollout_path(root.path.as_path())
        .await
        .ok_or_else(|| ThreadStoreError::Conflict {
            message: format!("rollout {} changed before repair", root.path.display()),
        })?;
    let logical_path = codex_rollout::plain_rollout_path(existing.as_path());
    let source = read_plain_source(existing.as_path()).await?;
    if sha256(source.as_slice()) != root.source_sha256 {
        return Err(ThreadStoreError::Conflict {
            message: format!("rollout {} changed before repair", root.path.display()),
        });
    }
    let consumed_len = root.end_byte_offset.map_or(source.len(), |offset| {
        usize::try_from(offset).unwrap_or(usize::MAX)
    });
    let consumed = consumed_source(
        source.as_slice(),
        root.end_byte_offset,
        logical_path.as_path(),
    )?;
    let (mut lines, count) = repair_legacy_goal_supervisor_jsonl_lines_with_provenance(
        consumed,
        GoalSupervisorLineageProvenance::Untrusted,
    )
    .map_err(|error| annotate_path(error, logical_path.as_path()))?;
    let provenance = GoalSupervisorLineageProvenance::Untrusted.continued_through(&lines);
    let mut repaired_messages = count.total();
    let mut installed_segments = 0;
    let mut reference_changed = false;
    if root.include_references {
        let selection_parent_lines = lines.clone();
        for line in &mut lines {
            let RolloutItem::RolloutReference(reference) = &mut line.item else {
                continue;
            };
            let repaired = repair_reference(
                store,
                root.thread_id,
                reference.clone(),
                provenance.continued_through_reference(reference),
                root.reference_policy,
                selected_message_ids_for_reference(
                    store,
                    selection_parent_lines.as_slice(),
                    reference,
                    root.reference_policy,
                    /*inherited_selection*/ None,
                )
                .await?,
                access,
            )
            .await?;
            repaired_messages += repaired.repaired_messages;
            installed_segments += repaired.installed_segments;
            if !same_reference(&repaired.reference, reference)? {
                *reference = repaired.reference;
                reference_changed = true;
            }
        }
    }
    if repaired_messages == 0 && !reference_changed {
        return Ok(GoalSupervisorHistoryRepairOutcome::default());
    }

    let old_segment_id = root_segment_id(&lines)?.ok_or_else(|| ThreadStoreError::Conflict {
        message: format!(
            "cannot repair segmentless rollout {} without a byte-exact standard backup",
            logical_path.display()
        ),
    })?;
    install_existing_identity_history_repair_backup(
        &writer,
        store.config.codex_home.as_path(),
        root.thread_id,
        old_segment_id,
        logical_path.as_path(),
        source.as_slice(),
    )
    .await?;
    let mut repaired = rewrite_rollout_jsonl_same_length(consumed, &lines)?
        .ok_or_else(|| repair_error("changed root produced no replacement"))?;
    preserve_session_meta_record(consumed, repaired.as_mut_slice())?;
    repaired.extend_from_slice(&source[consumed_len..]);
    let identity_cleared = clear_history_repair_segment_id(repaired.as_slice(), old_segment_id)?;
    let new_segment_id = history_repair_segment_id(identity_cleared.as_slice());
    let replacement =
        replace_history_repair_segment_id(repaired.as_slice(), old_segment_id, new_segment_id)?;
    let publication = if root.source_was_compressed {
        publish_compressed_history_repair_replacement(
            &writer,
            store.config.codex_home.as_path(),
            existing.as_path(),
            replacement.as_slice(),
        )
        .await?
    } else {
        publish_history_repair_replacement(
            &writer,
            store.config.codex_home.as_path(),
            logical_path.as_path(),
            replacement.as_slice(),
        )
        .await?
    };
    match publication {
        HistoryRepairPublication::Durable => Ok(GoalSupervisorHistoryRepairOutcome {
            repaired_messages,
            installed_segments,
            active_rollout_replaced: true,
        }),
        HistoryRepairPublication::DurabilityUnknown { error } => {
            quarantine_threads(
                canonical_home_key(store)?.as_path(),
                [root.thread_id, requested_thread_id],
            );
            Err(ThreadStoreError::Conflict {
                message: format!(
                    "goal-supervisor repair for {} committed without a durability acknowledgement; restart before continuing: {error}",
                    logical_path.display()
                ),
            })
        }
    }
}

fn canonical_home_key(store: &LocalThreadStore) -> ThreadStoreResult<PathBuf> {
    std::fs::canonicalize(store.config.codex_home.as_path()).map_err(thread_store_io_error)
}

fn quarantined_thread(home: &Path, thread_id: ThreadId) -> bool {
    INDETERMINATE_HISTORY_REPAIRS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(home)
        .is_some_and(|threads| threads.contains(&thread_id))
}

fn reject_quarantined_scope(home: &Path, thread_ids: &[ThreadId]) -> ThreadStoreResult<()> {
    let quarantined = INDETERMINATE_HISTORY_REPAIRS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(thread_id) = quarantined
        .get(home)
        .and_then(|threads| thread_ids.iter().find(|id| threads.contains(id)))
    {
        return Err(indeterminate_repair_error(*thread_id));
    }
    Ok(())
}

fn quarantine_threads(home: &Path, thread_ids: impl IntoIterator<Item = ThreadId>) {
    INDETERMINATE_HISTORY_REPAIRS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .entry(home.to_path_buf())
        .or_default()
        .extend(thread_ids);
}

#[cfg(test)]
pub(super) fn clear_quarantine_for_test(home: &Path) {
    if let Ok(home) = std::fs::canonicalize(home) {
        INDETERMINATE_HISTORY_REPAIRS
            .lock()
            .expect("history repair quarantine mutex")
            .remove(&home);
    }
}

async fn selected_message_ids_for_reference(
    store: &LocalThreadStore,
    parent_lines: &[RolloutLine],
    reference: &RolloutReferenceItem,
    reference_policy: ReferencePolicy,
    inherited_selection: Option<&Arc<HashSet<String>>>,
) -> ThreadStoreResult<Option<Arc<HashSet<String>>>> {
    if reference.nth_user_message.is_none() {
        return Ok(inherited_selection.cloned());
    }
    let session_meta = parent_lines
        .iter()
        .find(|line| matches!(line.item, RolloutItem::SessionMeta(_)))
        .cloned()
        .ok_or_else(|| repair_error("reference parent does not contain SessionMeta"))?;
    let reference_line = parent_lines
        .iter()
        .find(|line| {
            matches!(
                &line.item,
                RolloutItem::RolloutReference(candidate)
                    if same_reference(candidate, reference).unwrap_or(false)
            )
        })
        .cloned()
        .ok_or_else(|| repair_error("reference parent does not contain inbound reference"))?;
    let materialized = match reference_policy {
        ReferencePolicy::Complete => {
            codex_rollout::materialize_rollout_lines_from(
                store.config.codex_home.as_path(),
                vec![session_meta, reference_line],
            )
            .await
        }
        ReferencePolicy::Recent => {
            codex_rollout::materialize_recent_rollout_lines_from(
                store.config.codex_home.as_path(),
                vec![session_meta, reference_line],
            )
            .await
        }
    }
    .map_err(thread_store_io_error)?;
    let mut selected = HashSet::new();
    for line in &materialized {
        collect_agent_message_ids(&line.item, &mut selected);
    }
    if let Some(inherited) = inherited_selection {
        selected.retain(|id| inherited.contains(id));
    }
    Ok(Some(Arc::new(selected)))
}

fn collect_agent_message_ids(item: &RolloutItem, selected: &mut HashSet<String>) {
    match item {
        RolloutItem::ResponseItem(item) => {
            if let codex_protocol::models::ResponseItem::AgentMessage { id, .. } = &**item {
                selected.insert(
                    id.as_ref()
                        .map(|id| id.as_str().to_string())
                        .unwrap_or_else(|| SELECTED_MISSING_MESSAGE_ID.to_string()),
                );
            }
        }
        RolloutItem::Compacted(compacted) => {
            for item in compacted.replacement_history.as_deref().unwrap_or_default() {
                if let codex_protocol::models::ResponseItem::AgentMessage { id, .. } = &**item {
                    selected.insert(
                        id.as_ref()
                            .map(|id| id.as_str().to_string())
                            .unwrap_or_else(|| SELECTED_MISSING_MESSAGE_ID.to_string()),
                    );
                }
            }
        }
        _ => {}
    }
}

fn root_segment_id(lines: &[RolloutLine]) -> ThreadStoreResult<Option<SegmentId>> {
    let Some(RolloutItem::SessionMeta(meta)) = lines.first().map(|line| &line.item) else {
        return Err(repair_error("rollout does not start with SessionMeta"));
    };
    Ok(meta.meta.segment_id)
}

async fn repair_reference(
    store: &LocalThreadStore,
    parent_thread_id: ThreadId,
    reference: RolloutReferenceItem,
    inherited: GoalSupervisorLineageProvenance,
    reference_policy: ReferencePolicy,
    selected_message_ids: Option<Arc<HashSet<String>>>,
    access: &GoalSupervisorHistoryAccess,
) -> ThreadStoreResult<ReferenceRepair> {
    let root_thread_id = reference_thread_id(&reference)?;
    let root_boundary = root_thread_id != parent_thread_id || reference.nth_user_message.is_some();
    let mut active = HashSet::new();
    let mut frames = vec![
        load_frame(
            store,
            reference,
            inherited,
            usize::from(root_boundary),
            usize::from(!root_boundary),
            reference_policy,
            selected_message_ids,
            &mut active,
        )
        .await?,
    ];
    loop {
        let Some(frame) = frames.last_mut() else {
            return Err(repair_error("reference repair stack is empty"));
        };
        let nested = frame.lines[frame.next_line..]
            .iter()
            .position(|line| matches!(line.item, RolloutItem::RolloutReference(_)))
            .map(|offset| frame.next_line + offset);
        if let Some(index) = nested {
            frame.next_line = index + 1;
            let RolloutItem::RolloutReference(reference) = &frame.lines[index].item else {
                unreachable!();
            };
            let reference = reference.clone();
            let nested_thread = reference_thread_id(&reference)?;
            let boundary = nested_thread != frame.thread_id || reference.nth_user_message.is_some();
            if frame.reference_policy == ReferencePolicy::Recent
                && !boundary
                && frame.ordinary_reference_depth
                    >= reference.max_depth.min(DEFAULT_ROLLOUT_REFERENCE_DEPTH)
            {
                continue;
            }
            if boundary && frame.graph_depth >= MAX_ROLLOUT_REFERENCE_DEPTH {
                return Err(depth_error());
            }
            let inherited = frame.provenance.continued_through_reference(&reference);
            let graph_depth = frame.graph_depth + usize::from(boundary);
            let ordinary_reference_depth = frame.ordinary_reference_depth + usize::from(!boundary);
            let reference_policy = frame.reference_policy;
            let selected_message_ids = selected_message_ids_for_reference(
                store,
                frame.lines.as_slice(),
                &reference,
                reference_policy,
                frame.selected_message_ids.as_ref(),
            )
            .await?;
            frames.push(
                load_frame(
                    store,
                    reference,
                    inherited,
                    graph_depth,
                    ordinary_reference_depth,
                    reference_policy,
                    selected_message_ids,
                    &mut active,
                )
                .await?,
            );
            continue;
        }

        let mut completed = frames
            .pop()
            .ok_or_else(|| repair_error("reference repair stack is empty"))?;
        let changed = completed.direct_repaired_messages != 0 || completed.nested_reference_changed;
        let repaired_reference = if !changed {
            completed.reference.clone()
        } else if completed.reference.segment_id.is_none() {
            return Err(ThreadStoreError::Conflict {
                message: format!(
                    "cannot repair legacy initial {} without a byte-exact standard backup",
                    completed.resolved_path.display()
                ),
            });
        } else if completed.mutable_fallback {
            let writer = access.writer_token(store, completed.thread_id).await?;
            let old_segment_id = completed.reference.segment_id.ok_or_else(|| {
                repair_error("mutable reference fallback is missing its segment identity")
            })?;
            let identity_cleared =
                replacement_with_segment_id(&completed, /*segment_id*/ None)?;
            let segment_id = history_repair_segment_id(identity_cleared.as_slice());
            let replacement = replacement_with_segment_id(&completed, Some(segment_id))?;
            install_existing_identity_history_repair_backup(
                &writer,
                store.config.codex_home.as_path(),
                completed.thread_id,
                old_segment_id,
                completed.resolved_path.as_path(),
                completed.source.as_slice(),
            )
            .await?;
            let publication = if is_compressed_path(completed.resolved_path.as_path()) {
                publish_compressed_history_repair_replacement(
                    &writer,
                    store.config.codex_home.as_path(),
                    completed.resolved_path.as_path(),
                    replacement.as_slice(),
                )
                .await?
            } else {
                publish_history_repair_replacement(
                    &writer,
                    store.config.codex_home.as_path(),
                    completed.resolved_path.as_path(),
                    replacement.as_slice(),
                )
                .await?
            };
            if let HistoryRepairPublication::DurabilityUnknown { error } = publication {
                quarantine_threads(
                    canonical_home_key(store)?.as_path(),
                    [completed.thread_id, parent_thread_id],
                );
                return Err(ThreadStoreError::Conflict {
                    message: format!(
                        "goal-supervisor repair for {} committed without a durability acknowledgement; restart before continuing: {error}",
                        completed.resolved_path.display()
                    ),
                });
            }
            completed.installed_segments += 1;
            RolloutReferenceItem {
                segment_id: Some(segment_id),
                ..completed.reference.clone()
            }
        } else {
            let writer = access.writer_token(store, completed.thread_id).await?;
            let identity_cleared =
                replacement_with_segment_id(&completed, /*segment_id*/ None)?;
            let segment_id = history_repair_segment_id(identity_cleared.as_slice());
            let replacement = replacement_with_segment_id(&completed, Some(segment_id))?;
            let installed_path = install_history_repair_segment(
                &writer,
                store.config.codex_home.as_path(),
                completed.thread_id,
                segment_id,
                completed.resolved_path.as_path(),
                identity_cleared.as_slice(),
                replacement.as_slice(),
            )
            .await?;
            completed.installed_segments += 1;
            RolloutReferenceItem {
                rollout_path: installed_path,
                segment_id: Some(segment_id),
                ..completed.reference.clone()
            }
        };
        active.remove(&completed.resolved_path);
        if let Some(parent) = frames.last_mut() {
            let index = parent.next_line.saturating_sub(1);
            let Some(RolloutItem::RolloutReference(parent_reference)) =
                parent.lines.get_mut(index).map(|line| &mut line.item)
            else {
                return Err(repair_error("completed reference has no parent"));
            };
            if !same_reference(parent_reference, &repaired_reference)? {
                *parent_reference = repaired_reference;
                parent.nested_reference_changed = true;
            }
            parent.repaired_messages += completed.repaired_messages;
            parent.installed_segments += completed.installed_segments;
            continue;
        }
        return Ok(ReferenceRepair {
            reference: repaired_reference,
            repaired_messages: completed.repaired_messages,
            installed_segments: completed.installed_segments,
        });
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the iterative repair traversal carries each bounded lineage constraint explicitly"
)]
async fn load_frame(
    store: &LocalThreadStore,
    reference: RolloutReferenceItem,
    inherited: GoalSupervisorLineageProvenance,
    graph_depth: usize,
    ordinary_reference_depth: usize,
    reference_policy: ReferencePolicy,
    selected_message_ids: Option<Arc<HashSet<String>>>,
    active: &mut HashSet<PathBuf>,
) -> ThreadStoreResult<RepairFrame> {
    let thread_id = reference_thread_id(&reference)?;
    let resolved_path = codex_rollout::resolve_rollout_reference_path(
        store.config.codex_home.as_path(),
        &reference,
    )
    .await
    .map_err(thread_store_io_error)?;
    if !active.insert(resolved_path.clone()) {
        return Err(cycle_error(resolved_path.as_path()));
    }
    let mutable_fallback = if reference.segment_id.is_none()
        && reference
            .rollout_id
            .is_none_or(|rollout_id| rollout_id == thread_id)
    {
        validate_legacy_initial_repair_path(
            store.config.codex_home.as_path(),
            thread_id,
            resolved_path.as_path(),
        )
        .await?;
        false
    } else {
        !is_immutable_segment(store, resolved_path.as_path()).await?
    };
    let source = read_plain_source(resolved_path.as_path()).await?;
    let (lines, count) = match selected_message_ids.as_deref() {
        Some(selected) => repair_legacy_goal_supervisor_jsonl_lines_selected_with_provenance(
            source.as_slice(),
            inherited,
            selected,
        ),
        None => {
            repair_legacy_goal_supervisor_jsonl_lines_with_provenance(source.as_slice(), inherited)
        }
    }
    .map_err(|error| annotate_path(error, resolved_path.as_path()))?;
    let provenance = inherited.continued_through(&lines);
    Ok(RepairFrame {
        reference,
        thread_id,
        resolved_path,
        source,
        lines,
        next_line: 0,
        direct_repaired_messages: count.total(),
        repaired_messages: count.total(),
        installed_segments: 0,
        nested_reference_changed: false,
        graph_depth,
        ordinary_reference_depth,
        reference_policy,
        provenance,
        selected_message_ids,
        mutable_fallback,
    })
}

fn replacement_with_segment_id(
    frame: &RepairFrame,
    segment_id: Option<SegmentId>,
) -> ThreadStoreResult<Vec<u8>> {
    let Some(RolloutItem::SessionMeta(meta)) = frame.lines.first().map(|line| &line.item) else {
        return Err(repair_error(
            "referenced rollout does not start with SessionMeta",
        ));
    };
    let old_segment_id = meta
        .meta
        .segment_id
        .ok_or_else(|| repair_error("referenced rollout is missing its segment identity"))?;
    let repaired =
        rewrite_rollout_jsonl_same_length(frame.source.as_slice(), frame.lines.as_slice())?
            .ok_or_else(|| repair_error("segment repair produced no replacement"))?;
    let mut repaired = repaired;
    preserve_session_meta_record(frame.source.as_slice(), repaired.as_mut_slice())?;
    match segment_id {
        Some(segment_id) => {
            replace_history_repair_segment_id(repaired.as_slice(), old_segment_id, segment_id)
        }
        None => clear_history_repair_segment_id(repaired.as_slice(), old_segment_id),
    }
}

fn preserve_session_meta_record(source: &[u8], replacement: &mut [u8]) -> ThreadStoreResult<()> {
    if source.len() != replacement.len() {
        return Err(repair_error(
            "history repair changed the rollout length before identity replacement",
        ));
    }
    let mut offset = 0;
    for record in source.split_inclusive(|byte| *byte == b'\n') {
        let end = offset + record.len();
        if serde_json::from_slice::<RolloutLine>(record)
            .is_ok_and(|line| matches!(line.item, RolloutItem::SessionMeta(_)))
        {
            replacement[offset..end].copy_from_slice(record);
            return Ok(());
        }
        offset = end;
    }
    Err(repair_error(
        "history repair source has no SessionMeta record",
    ))
}

async fn read_plain_source(path: &Path) -> ThreadStoreResult<Vec<u8>> {
    let physical = codex_rollout::existing_rollout_path(path)
        .await
        .ok_or_else(|| ThreadStoreError::InvalidRequest {
            message: format!("rollout {} does not exist", path.display()),
        })?;
    if is_compressed_path(physical.as_path()) {
        let physical = physical.clone();
        return tokio::task::spawn_blocking(move || {
            let file = std::fs::File::open(physical)?;
            zstd::stream::decode_all(file)
        })
        .await
        .map_err(|error| repair_error(error.to_string()))?
        .map_err(thread_store_io_error);
    }
    tokio::fs::read(physical)
        .await
        .map_err(thread_store_io_error)
}

fn is_compressed_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".jsonl.zst"))
}

fn consumed_source<'a>(
    source: &'a [u8],
    end_byte_offset: Option<u64>,
    path: &Path,
) -> ThreadStoreResult<&'a [u8]> {
    let end = end_byte_offset
        .map(|offset| usize::try_from(offset).unwrap_or(usize::MAX))
        .unwrap_or(source.len());
    let consumed = source
        .get(..end)
        .ok_or_else(|| ThreadStoreError::InvalidRequest {
            message: format!("history boundary is past rollout {}", path.display()),
        })?;
    if end != source.len() && end != 0 && consumed.last() != Some(&b'\n') {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "history boundary is not a record boundary in {}",
                path.display()
            ),
        });
    }
    Ok(consumed)
}

async fn confined_rollout_path(
    store: &LocalThreadStore,
    path: &Path,
) -> ThreadStoreResult<PathBuf> {
    let existing = codex_rollout::existing_rollout_path(path)
        .await
        .ok_or_else(|| ThreadStoreError::InvalidRequest {
            message: format!("rollout {} does not exist", path.display()),
        })?;
    for subdirectory in [
        codex_rollout::SESSIONS_SUBDIR,
        codex_rollout::ARCHIVED_SESSIONS_SUBDIR,
        codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR,
    ] {
        let root = store.config.codex_home.join(subdirectory);
        if !root.exists() {
            continue;
        }
        if let Ok(path) = super::helpers::scoped_rollout_path(
            root,
            existing.as_path(),
            "canonical rollout directory",
        ) {
            return Ok(path);
        }
    }
    Err(ThreadStoreError::InvalidRequest {
        message: format!(
            "rollout {} is not inside a canonical rollout directory",
            path.display()
        ),
    })
}

fn validate_root_thread_id(
    lines: &[RolloutLine],
    expected_thread_id: ThreadId,
    path: &Path,
) -> ThreadStoreResult<()> {
    let Some(RolloutItem::SessionMeta(meta)) = lines.first().map(|line| &line.item) else {
        return Err(repair_error(format!(
            "rollout {} does not start with SessionMeta",
            path.display()
        )));
    };
    if meta.meta.id != expected_thread_id {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "rollout {} belongs to thread {}, not {}",
                path.display(),
                meta.meta.id,
                expected_thread_id
            ),
        });
    }
    Ok(())
}

async fn is_immutable_segment(store: &LocalThreadStore, path: &Path) -> ThreadStoreResult<bool> {
    let root = store
        .config
        .codex_home
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR);
    let canonical_root = match tokio::fs::canonicalize(root).await {
        Ok(path) => path,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(thread_store_io_error(error)),
    };
    let canonical_path = tokio::fs::canonicalize(path)
        .await
        .map_err(thread_store_io_error)?;
    Ok(canonical_path.starts_with(canonical_root))
}

fn reference_thread_id(reference: &RolloutReferenceItem) -> ThreadStoreResult<ThreadId> {
    reference
        .thread_id
        .ok_or_else(|| ThreadStoreError::InvalidRequest {
            message: format!(
                "rollout reference {} is missing thread_id",
                reference.rollout_path.display()
            ),
        })
}

fn same_reference(
    left: &RolloutReferenceItem,
    right: &RolloutReferenceItem,
) -> ThreadStoreResult<bool> {
    Ok(
        serde_json::to_value(left).map_err(|error| repair_error(error.to_string()))?
            == serde_json::to_value(right).map_err(|error| repair_error(error.to_string()))?,
    )
}

fn sha256(source: &[u8]) -> String {
    format!("{:x}", Sha256::digest(source))
}

fn cycle_error(path: &Path) -> ThreadStoreError {
    repair_error(format!(
        "rollout reference cycle detected at {}",
        path.display()
    ))
}

fn depth_error() -> ThreadStoreError {
    repair_error(format!(
        "rollout reference graph exceeds maximum depth of {MAX_ROLLOUT_REFERENCE_DEPTH}"
    ))
}

fn annotate_path(error: ThreadStoreError, path: &Path) -> ThreadStoreError {
    match error {
        ThreadStoreError::InvalidRequest { message } => ThreadStoreError::InvalidRequest {
            message: format!("{message} in rollout {}", path.display()),
        },
        ThreadStoreError::Conflict { message } => ThreadStoreError::Conflict {
            message: format!("{message} in rollout {}", path.display()),
        },
        other => other,
    }
}

fn repair_error(message: impl Into<String>) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: message.into(),
    }
}

fn thread_store_io_error(error: io::Error) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: error.to_string(),
    }
}
