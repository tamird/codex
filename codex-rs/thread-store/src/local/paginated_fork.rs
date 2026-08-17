use codex_protocol::RolloutId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::ResponseItemEnvelope;
use codex_rollout::ReverseJsonlScanner;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_rollout::ScanOutcome;
use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::Arc;
#[cfg(test)]
use std::sync::LazyLock;
#[cfg(test)]
use std::sync::Mutex as StdMutex;
#[cfg(test)]
use tokio::sync::Notify;
use tokio::sync::OwnedRwLockReadGuard;

use super::LocalThreadStore;
use super::goal_supervisor_runtime_repair::GoalSupervisorHistoryAccess;
use super::live_writer;
use super::model_context;
use super::thread_history::find_source_turn;
use super::thread_history::find_visible_turn;
use crate::ForkBoundary;
use crate::FreezeRolloutSegmentParams;
use crate::ItemSortKey;
use crate::ListItemsParams;
use crate::ListTurnsParams;
use crate::PrepareForkParams;
use crate::PreparedFork;
use crate::SortDirection;
use crate::StoredTurn;
use crate::StoredTurnItemsView;
use crate::StoredTurnStatus;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

#[derive(Clone, Copy)]
enum ForkResponseHistory {
    Full,
    ModelContext,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ForkPersistence {
    /// Publish an immutable source segment for a durable child rollout reference.
    ReferenceBacked,
    /// Retain only bounded startup context for a child with deferred persistence.
    EphemeralContext,
}

#[derive(Debug)]
struct ForkHistoryReservation {
    _lifecycle: OwnedRwLockReadGuard<()>,
    history_access: GoalSupervisorHistoryAccess,
}

enum IndexedForkAttempt {
    Prepared(Box<PreparedFork>),
    Fallback(Box<ForkHistoryReservation>),
}

struct ActiveForkSuffix {
    lines: Vec<RolloutLine>,
    latest_ordinal: Option<u64>,
}

#[cfg(test)]
static LINEAGE_PERSISTENCE_PAUSES: LazyLock<
    StdMutex<HashMap<codex_protocol::ThreadId, Arc<LineagePersistencePause>>>,
> = LazyLock::new(|| StdMutex::new(HashMap::new()));

#[cfg(test)]
pub(super) struct LineagePersistencePause {
    pub(super) entered: Notify,
    pub(super) release: Notify,
}

#[cfg(test)]
pub(super) fn inject_lineage_persistence_pause(
    thread_id: codex_protocol::ThreadId,
) -> Arc<LineagePersistencePause> {
    let pause = Arc::new(LineagePersistencePause {
        entered: Notify::new(),
        release: Notify::new(),
    });
    LINEAGE_PERSISTENCE_PAUSES
        .lock()
        .expect("lineage persistence pause mutex")
        .insert(thread_id, Arc::clone(&pause));
    pause
}

pub(super) async fn prepare(
    store: &LocalThreadStore,
    params: PrepareForkParams,
) -> ThreadStoreResult<PreparedFork> {
    prepare_with_response_history(
        store,
        params,
        ForkResponseHistory::Full,
        /*supplied_context*/ None,
        /*expected_rollout_id*/ None,
        ForkPersistence::ReferenceBacked,
    )
    .await
}

pub(super) async fn prepare_for_rollout(
    store: &LocalThreadStore,
    params: PrepareForkParams,
    expected_rollout_id: RolloutId,
) -> ThreadStoreResult<PreparedFork> {
    prepare_with_response_history(
        store,
        params,
        ForkResponseHistory::Full,
        /*supplied_context*/ None,
        Some(expected_rollout_id),
        ForkPersistence::ReferenceBacked,
    )
    .await
}

pub(super) async fn prepare_without_response_history(
    store: &LocalThreadStore,
    params: PrepareForkParams,
) -> ThreadStoreResult<PreparedFork> {
    prepare_with_response_history(
        store,
        params,
        ForkResponseHistory::ModelContext,
        /*supplied_context*/ None,
        /*expected_rollout_id*/ None,
        ForkPersistence::ReferenceBacked,
    )
    .await
}

pub(super) async fn prepare_without_response_history_for_rollout(
    store: &LocalThreadStore,
    params: PrepareForkParams,
    expected_rollout_id: RolloutId,
    ephemeral_context_only: bool,
) -> ThreadStoreResult<PreparedFork> {
    prepare_with_response_history(
        store,
        params,
        ForkResponseHistory::ModelContext,
        /*supplied_context*/ None,
        Some(expected_rollout_id),
        if ephemeral_context_only {
            ForkPersistence::EphemeralContext
        } else {
            ForkPersistence::ReferenceBacked
        },
    )
    .await
}

pub(super) async fn prepare_with_model_context(
    store: &LocalThreadStore,
    params: PrepareForkParams,
    model_context: Arc<Vec<ResponseItemEnvelope>>,
    expected_position: HistoryPosition,
) -> ThreadStoreResult<PreparedFork> {
    prepare_with_response_history(
        store,
        params,
        ForkResponseHistory::Full,
        Some((model_context, expected_position)),
        /*expected_rollout_id*/ None,
        ForkPersistence::ReferenceBacked,
    )
    .await
}

pub(super) async fn prepare_with_model_context_for_rollout(
    store: &LocalThreadStore,
    params: PrepareForkParams,
    model_context: Arc<Vec<ResponseItemEnvelope>>,
    expected_position: HistoryPosition,
    expected_rollout_id: RolloutId,
) -> ThreadStoreResult<PreparedFork> {
    prepare_with_response_history(
        store,
        params,
        ForkResponseHistory::Full,
        Some((model_context, expected_position)),
        Some(expected_rollout_id),
        ForkPersistence::ReferenceBacked,
    )
    .await
}

pub(super) async fn prepare_without_response_history_with_model_context(
    store: &LocalThreadStore,
    params: PrepareForkParams,
    model_context: Arc<Vec<ResponseItemEnvelope>>,
    expected_position: HistoryPosition,
) -> ThreadStoreResult<PreparedFork> {
    prepare_with_response_history(
        store,
        params,
        ForkResponseHistory::ModelContext,
        Some((model_context, expected_position)),
        /*expected_rollout_id*/ None,
        ForkPersistence::ReferenceBacked,
    )
    .await
}

pub(super) async fn prepare_without_response_history_with_model_context_for_rollout(
    store: &LocalThreadStore,
    params: PrepareForkParams,
    model_context: Arc<Vec<ResponseItemEnvelope>>,
    expected_position: HistoryPosition,
    expected_rollout_id: RolloutId,
    ephemeral_context_only: bool,
) -> ThreadStoreResult<PreparedFork> {
    prepare_with_response_history(
        store,
        params,
        ForkResponseHistory::ModelContext,
        Some((model_context, expected_position)),
        Some(expected_rollout_id),
        if ephemeral_context_only {
            ForkPersistence::EphemeralContext
        } else {
            ForkPersistence::ReferenceBacked
        },
    )
    .await
}

async fn prepare_with_response_history(
    store: &LocalThreadStore,
    params: PrepareForkParams,
    response_history: ForkResponseHistory,
    supplied_context: Option<(Arc<Vec<ResponseItemEnvelope>>, HistoryPosition)>,
    expected_rollout_id: Option<RolloutId>,
    persistence: ForkPersistence,
) -> ThreadStoreResult<PreparedFork> {
    if let Some((items, _)) = supplied_context.as_ref() {
        let supplied = items
            .iter()
            .cloned()
            .map(RolloutItem::ResponseItem)
            .collect::<Vec<_>>();
        super::goal_supervisor_runtime_repair::reject_malformed_supplied_history(
            supplied.as_slice(),
        )?;
    }
    let PrepareForkParams {
        thread_id,
        boundary,
    } = params;
    let interrupt_if_open = matches!(&boundary, ForkBoundary::Latest);
    let mut indexed_fallback_reservation = None;
    if !matches!(boundary, ForkBoundary::Latest)
        && matches!(response_history, ForkResponseHistory::ModelContext)
    {
        let source = resolve_fork_source(store, thread_id).await?;
        let mut history_access =
            super::goal_supervisor_runtime_repair::repair_selected_history_before_access(
                store,
                thread_id,
                source.path.as_path(),
                super::goal_supervisor_runtime_repair::RepairAccess::ActiveOnly,
            )
            .await?;
        let lifecycle = history_access
            .take_lifecycle(thread_id)
            .ok_or_else(missing_source_reservation)?;
        let reservation = ForkHistoryReservation {
            _lifecycle: lifecycle,
            history_access,
        };
        match try_prepare_indexed_explicit_model_context_fork(
            store,
            thread_id,
            &boundary,
            supplied_context.clone(),
            expected_rollout_id,
            reservation,
            persistence,
        )
        .await?
        {
            IndexedForkAttempt::Prepared(prepared) => return Ok(*prepared),
            IndexedForkAttempt::Fallback(reservation) => {
                let active_head =
                    super::rollout_lineage::read_rollout_head(source.path.as_path()).await?;
                if active_head
                    .leading_reference
                    .as_ref()
                    .is_some_and(|(_, reference)| {
                        super::rollout_lineage::canonical_same_thread_reference(
                            store.config.codex_home.as_path(),
                            source.path.as_path(),
                            thread_id,
                            source.rollout_id,
                            reference,
                        )
                    })
                    || super::rollout_lineage::has_same_thread_history_base(
                        store,
                        &active_head,
                        thread_id,
                    )
                    .await?
                {
                    return Err(ThreadStoreError::InvalidRequest {
                        message: "the segmented rollout cannot reconstruct the requested fork \
                                  boundary within the recent checkpoint window; migrate the \
                                  history before forking at that boundary"
                            .to_string(),
                    });
                }
                indexed_fallback_reservation = Some(reservation);
            }
        }
    }
    if matches!(boundary, ForkBoundary::Latest) {
        let source = resolve_fork_source(store, thread_id).await?;
        trace_fork_stage("resolved_source_for_indexed_attempt");
        let mut history_access =
            super::goal_supervisor_runtime_repair::repair_selected_history_before_access(
                store,
                thread_id,
                source.path.as_path(),
                super::goal_supervisor_runtime_repair::RepairAccess::ActiveOnly,
            )
            .await?;
        trace_fork_stage("repaired_active_history");
        let lifecycle = history_access
            .take_lifecycle(thread_id)
            .ok_or_else(missing_source_reservation)?;
        let repair_reservation = ForkHistoryReservation {
            _lifecycle: lifecycle,
            history_access,
        };
        let indexed_store = store.clone();
        let attempt = tokio::spawn(async move {
            try_prepare_indexed_latest_fork(
                &indexed_store,
                thread_id,
                response_history,
                supplied_context,
                expected_rollout_id,
                repair_reservation,
                persistence,
            )
            .await
        })
        .await
        .map_err(|error| ThreadStoreError::Internal {
            message: format!("failed to prepare indexed fork: {error}"),
        })??;
        match attempt {
            IndexedForkAttempt::Prepared(prepared) => return Ok(*prepared),
            IndexedForkAttempt::Fallback(reservation) => {
                if matches!(response_history, ForkResponseHistory::ModelContext) {
                    match try_prepare_certified_latest_model_context_fork(
                        store,
                        thread_id,
                        expected_rollout_id,
                        *reservation,
                        persistence,
                    )
                    .await?
                    {
                        IndexedForkAttempt::Prepared(prepared) => return Ok(*prepared),
                        IndexedForkAttempt::Fallback(reservation) => {
                            indexed_fallback_reservation = Some(reservation);
                        }
                    }
                } else {
                    indexed_fallback_reservation = Some(reservation);
                }
            }
        }
        trace_fork_stage("indexed_attempt_fell_back");
    }
    let mut prepared_same_thread_model_context = None;
    let mut prepared_same_thread_full_history = None;
    let mut prepared_same_thread_session_meta = None;
    let fast_lineage = if let Some(reservation) = indexed_fallback_reservation {
        let writer_reservation =
            reservation
                .history_access
                .writer_reservation()
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: "history repair did not retain fork writer ownership".to_string(),
                })?;
        let prepared = store
            .try_prepare_same_thread_fork_lineage_reserved(
                thread_id,
                expected_rollout_id,
                writer_reservation,
                matches!(response_history, ForkResponseHistory::Full),
            )
            .await?;
        prepared.map(|prepared| (reservation, prepared))
    } else {
        None
    };
    let (mut lineage, history_access, source_reservation, mut source_projection_was_missing) =
        if let Some((reservation, prepared)) = fast_lineage {
            let ForkHistoryReservation {
                _lifecycle: source_reservation,
                history_access,
            } = *reservation;
            prepared_same_thread_model_context = Some(Arc::new(prepared.model_context));
            prepared_same_thread_full_history = prepared.full_history.map(Arc::new);
            prepared_same_thread_session_meta = Some(prepared.session_meta);
            trace_fork_stage("prepared_clean_same_thread_lineage");
            (
                prepared.lineage,
                history_access,
                source_reservation,
                prepared.source_projection_was_missing,
            )
        } else {
            resolve_compatibility_fork_lineage(store, thread_id, expected_rollout_id).await?
        };
    trace_fork_stage("resolved_reference_lineage");
    let mut frozen_before_projection = None;
    if matches!(boundary, ForkBoundary::Latest) && persistence == ForkPersistence::ReferenceBacked {
        let writer_reservation =
            history_access
                .writer_reservation()
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: "history repair did not retain fork writer ownership".to_string(),
                })?;
        let frozen = super::segment::freeze_thread_segment_reserved(
            store,
            thread_id,
            FreezeRolloutSegmentParams::snapshot(),
            expected_rollout_id,
            writer_reservation,
        )
        .await?;
        if let Some(history_base) = frozen.history_base {
            lineage = store
                .resolve_rollout_lineage_from_path(
                    thread_id,
                    frozen.reference.rollout_path.as_path(),
                )
                .await?;
            source_projection_was_missing = true;
            debug_assert_eq!(
                lineage
                    .segments()
                    .last()
                    .map(super::rollout_lineage::RolloutLineageSegment::rollout_id),
                Some(history_base.thread_id)
            );
        }
        frozen_before_projection = Some(frozen);
        trace_fork_stage("froze_latest_prefix_before_projection");
    }
    let source_segment = lineage
        .segments()
        .last()
        .ok_or_else(|| ThreadStoreError::Internal {
            message: "fork lineage has no source segment".to_string(),
        })?;
    if store.state_db.is_none() {
        return Err(ThreadStoreError::Unsupported {
            operation: "prepare_fork",
        });
    }
    if !matches!(boundary, ForkBoundary::Latest) || frozen_before_projection.is_some() {
        if source_projection_was_missing {
            super::thread_history::clear_projection_cursor(store, source_segment.rollout_id())
                .await?;
        }
        for segment in lineage
            .segments()
            .iter()
            .take(lineage.segments().len().saturating_sub(1))
        {
            if super::thread_history::projection_state(store, segment.rollout_id())
                .await?
                .is_some_and(|state| {
                    segment
                        .end_ordinal()
                        .is_some_and(|end| state.next_ordinal >= end)
                })
            {
                continue;
            }
            super::thread_history_materialization::materialize_to_sqlite(
                store,
                segment.rollout_id(),
                segment.rollout_path.as_path(),
            )
            .await?;
        }
    }
    super::thread_history_materialization::materialize_to_sqlite(
        store,
        source_segment.rollout_id(),
        source_segment.rollout_path.as_path(),
    )
    .await?;

    let latest_projection_state =
        super::thread_history::projection_state(store, source_segment.rollout_id())
            .await?
            .ok_or_else(|| ThreadStoreError::Internal {
                message: format!("missing projection state for paginated thread {thread_id}"),
            })?;
    let latest_position = HistoryPosition {
        thread_id: source_segment.rollout_id(),
        end_ordinal_exclusive: latest_projection_state.next_ordinal,
        end_byte_offset: latest_projection_state.next_byte_offset,
    };
    let pool = store.thread_history_db().await?;
    let position = match boundary {
        ForkBoundary::Latest => latest_position,
        ForkBoundary::ThroughTurn(turn_id) => {
            let row = find_visible_turn(pool, &lineage, turn_id.as_str()).await?;
            if row.status == "inProgress" {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("lastTurnId '{turn_id}' identifies an in-progress turn"),
                });
            }
            let rollout_end_ordinal = row
                .rollout_end_ordinal
                .ok_or_else(|| missing_turn_position(turn_id.as_str()))?;
            let rollout_end_byte_offset = row
                .rollout_end_byte_offset
                .ok_or_else(|| missing_turn_position(turn_id.as_str()))?;
            HistoryPosition {
                thread_id: row.rollout_id,
                end_ordinal_exclusive: u64::try_from(rollout_end_ordinal)
                    .map_err(|_| invalid_turn_position(turn_id.as_str()))?
                    .checked_add(1)
                    .ok_or_else(|| invalid_turn_position(turn_id.as_str()))?,
                end_byte_offset: u64::try_from(rollout_end_byte_offset)
                    .map_err(|_| invalid_turn_position(turn_id.as_str()))?,
            }
        }
        ForkBoundary::BeforeTurn(turn_id) => {
            let row = find_source_turn(pool, &lineage, turn_id.as_str()).await?;
            if row.rollout_end_ordinal == Some(row.rollout_ordinal) {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("turn {turn_id} does not have a persisted start boundary"),
                });
            }
            let rollout_byte_offset = row
                .rollout_byte_offset
                .ok_or_else(|| missing_turn_position(turn_id.as_str()))?;
            HistoryPosition {
                thread_id: row.rollout_id,
                end_ordinal_exclusive: u64::try_from(row.rollout_ordinal)
                    .map_err(|_| invalid_turn_position(turn_id.as_str()))?,
                end_byte_offset: u64::try_from(rollout_byte_offset)
                    .map_err(|_| invalid_turn_position(turn_id.as_str()))?,
            }
        }
    };
    let segment_index = lineage.segments().iter().position(|segment| {
        segment.rollout_id() == position.thread_id
            && position.end_ordinal_exclusive >= segment.start_ordinal()
            && segment
                .end_ordinal()
                .is_none_or(|end| position.end_ordinal_exclusive <= end)
    });
    let Some(segment_index) = segment_index else {
        return Err(ThreadStoreError::InvalidRequest {
            message: "fork boundary exceeds inherited source history".to_string(),
        });
    };
    let segment = &lineage.segments()[segment_index];
    if segment
        .end_ordinal()
        .is_some_and(|end| position.end_ordinal_exclusive > end)
        || segment
            .end_byte_offset
            .is_some_and(|end| position.end_byte_offset > end)
    {
        return Err(ThreadStoreError::InvalidRequest {
            message: "fork boundary exceeds inherited source history".to_string(),
        });
    }
    let history_base = if position.end_ordinal_exclusive == segment.start_ordinal() {
        segment_index.checked_sub(1).and_then(|index| {
            let previous = &lineage.segments()[index];
            Some(HistoryPosition {
                thread_id: previous.rollout_id(),
                end_ordinal_exclusive: previous.end_ordinal()?,
                end_byte_offset: previous.end_byte_offset?,
            })
        })
    } else {
        Some(position)
    };
    let source_rollout_path = source_segment.rollout_path.clone();
    let indexed_root_latest = interrupt_if_open
        && !source_projection_was_missing
        && history_base == Some(latest_position)
        && indexed_root_fallback_is_current(store, thread_id, &lineage, latest_position).await?;
    let prefix_end = history_base.unwrap_or(position);
    let prefix_lineage = lineage.clone().truncate_at(prefix_end).await?;
    trace_fork_stage("truncated_prefix_lineage");
    let prefix_segment =
        prefix_lineage
            .segments()
            .last()
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "normalized fork prefix is outside the source lineage".to_string(),
            })?;
    let prefix_thread_id = prefix_segment.thread_id();
    let prefix_rollout_id = prefix_segment.rollout_id();
    let prefix_rollout_path = prefix_segment.rollout_path.clone();
    let end_byte_offset =
        prefix_segment
            .end_byte_offset
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "prepared fork prefix is missing its byte boundary".to_string(),
            })?;
    // The detached owner retains every writer reservation until immutable publication finishes,
    // even if the request that initiated fork preparation is cancelled.
    let publication_store = store.clone();
    let (frozen_segment, history_access) = if let Some(frozen) = frozen_before_projection {
        (Some(frozen), history_access)
    } else if persistence == ForkPersistence::ReferenceBacked {
        let (frozen_segment, history_access) = tokio::spawn(async move {
            let writer_reservation =
                history_access
                    .writer_reservation()
                    .ok_or_else(|| ThreadStoreError::Internal {
                        message: "history repair did not retain fork writer ownership".to_string(),
                    })?;
            let frozen_segment = if indexed_root_latest {
                super::segment::freeze_thread_segment_reserved(
                    &publication_store,
                    thread_id,
                    FreezeRolloutSegmentParams::snapshot(),
                    expected_rollout_id,
                    writer_reservation,
                )
                .await?
            } else {
                super::segment::freeze_paginated_prefix_reserved(
                    &publication_store,
                    thread_id,
                    source_rollout_path.as_path(),
                    prefix_thread_id,
                    prefix_rollout_id,
                    prefix_rollout_path.as_path(),
                    prefix_end.end_ordinal_exclusive,
                    end_byte_offset,
                    writer_reservation,
                )
                .await?
            };
            Ok::<_, ThreadStoreError>((frozen_segment, history_access))
        })
        .await
        .map_err(|error| ThreadStoreError::Internal {
            message: format!("failed to publish fork segment: {error}"),
        })??;
        (Some(frozen_segment), history_access)
    } else {
        (None, history_access)
    };
    trace_fork_stage("published_frozen_prefix");
    let latest_model_context = if let Some(model_context) = prepared_same_thread_model_context {
        model_context
    } else {
        Arc::new(model_context::load_for_fork(lineage.clone(), Some(latest_position)).await?)
    };
    trace_fork_stage("loaded_latest_model_context");
    let model_context = if history_base == Some(latest_position) {
        Arc::clone(&latest_model_context)
    } else {
        Arc::new(model_context::load_for_fork(lineage.clone(), history_base).await?)
    };
    // `HistoryPosition` cannot encode content filtering. Retain the complete persistence prefix
    // only for compatibility lineages that apply filters; ordinary native history_base ancestry
    // remains zero-copy across both same-thread rotations and cross-thread forks.
    let copied_history = if lineage.requires_copied_history() {
        Some(Arc::new(
            model_context::load_full_for_fork(lineage.clone(), history_base).await?,
        ))
    } else {
        None
    };
    trace_fork_stage("classified_copied_history");
    let (response_history, projected_response_turns) = match (
        response_history,
        copied_history.as_ref(),
        prepared_same_thread_full_history,
    ) {
        (ForkResponseHistory::Full, Some(copied_history), _) => (Arc::clone(copied_history), None),
        (ForkResponseHistory::Full, None, Some(prepared_full_history)) => {
            (prepared_full_history, None)
        }
        (ForkResponseHistory::Full, None, None) if indexed_root_latest => (
            Arc::clone(&model_context),
            Some(Arc::new(
                load_projected_response_turns(store, thread_id).await?,
            )),
        ),
        (ForkResponseHistory::Full, None, None) => (
            Arc::new(
                if let Some(session_meta) = prepared_same_thread_session_meta {
                    model_context::load_full_for_fork_with_session_meta(
                        lineage,
                        history_base,
                        session_meta,
                    )
                    .await?
                } else {
                    model_context::load_full_for_fork(lineage, history_base).await?
                },
            ),
            None,
        ),
        (ForkResponseHistory::ModelContext, _, _) => (Arc::clone(&model_context), None),
    };
    trace_fork_stage("loaded_response_history");
    let mut prepared = PreparedFork::new(
        thread_id,
        history_base,
        frozen_segment,
        model_context,
        latest_model_context,
        response_history,
        interrupt_if_open,
        {
            // Once the immutable prefix is durable, later child persistence no longer consumes
            // the mutable source. Retain only the lifecycle lease that blocks source deletion;
            // keeping repair maintenance and writer ownership here would block valid source
            // appends and unrelated fork preparation for the lifetime of PreparedFork.
            drop(history_access);
            crate::ThreadLifecycleReservation::new(source_reservation)
        },
    );
    prepared.copied_history = copied_history;
    prepared.projected_response_turns = projected_response_turns;
    Ok(prepared)
}

/// Prepares an explicit model-context fork by reading only the physical segment containing the
/// projected turn boundary.
async fn try_prepare_indexed_explicit_model_context_fork(
    store: &LocalThreadStore,
    thread_id: codex_protocol::ThreadId,
    boundary: &ForkBoundary,
    supplied_context: Option<(Arc<Vec<ResponseItemEnvelope>>, HistoryPosition)>,
    expected_rollout_id: Option<RolloutId>,
    mut repair_reservation: ForkHistoryReservation,
    persistence: ForkPersistence,
) -> ThreadStoreResult<IndexedForkAttempt> {
    macro_rules! fallback {
        ($reason:literal) => {{
            tracing::event!(
                target: "codex_history_io",
                tracing::Level::TRACE,
                event.name = "codex.history.fork.indexed_explicit_fallback",
                reason = $reason,
                "indexed explicit-fork preparation fell back"
            );
            return Ok(IndexedForkAttempt::Fallback(Box::new(repair_reservation)));
        }};
    }
    if store.state_db.is_none() {
        fallback!("state_db_unavailable");
    }
    let Some(resolved) = super::thread_rollout_resolver::resolve_current(store, thread_id).await?
    else {
        fallback!("current_rollout_unresolved");
    };
    if expected_rollout_id.is_some_and(|expected| expected != resolved.rollout_id) {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "rollout path does not select the current rollout for thread {thread_id}"
            ),
        });
    }
    if super::thread_history::projection_state(store, resolved.rollout_id)
        .await?
        .is_none()
    {
        fallback!("projection_state_missing");
    }
    match live_writer::persist_thread_reserved(store, thread_id).await {
        Ok(()) | Err(ThreadStoreError::ThreadNotFound { .. }) => {}
        Err(error) => return Err(error),
    }
    let Some(state_db) = store.state_db().await else {
        fallback!("state_db_runtime_unavailable");
    };
    let Some(metadata) =
        state_db
            .get_thread(thread_id)
            .await
            .map_err(|error| ThreadStoreError::Internal {
                message: format!("failed to read indexed fork source metadata: {error}"),
            })?
    else {
        fallback!("thread_metadata_missing");
    };
    if metadata.archived_at.is_some()
        || metadata
            .rollout_path
            .extension()
            .and_then(|extension| extension.to_str())
            != Some("jsonl")
    {
        fallback!("thread_archived_or_active_not_plain_jsonl");
    }
    if super::helpers::scoped_rollout_path(
        store.config.codex_home.clone(),
        metadata.rollout_path.as_path(),
        "CODEX_HOME",
    )
    .is_err()
    {
        fallback!("active_rollout_outside_codex_home");
    }
    let Some(latest_position) = store.projected_history_position(thread_id).await? else {
        fallback!("projected_history_position_missing");
    };
    let Ok(file_metadata) = tokio::fs::metadata(metadata.rollout_path.as_path()).await else {
        fallback!("active_rollout_metadata_unavailable");
    };
    if file_metadata.len() != latest_position.end_byte_offset
        || supplied_context
            .as_ref()
            .is_some_and(|(_, expected)| expected != &latest_position)
    {
        fallback!("active_boundary_or_supplied_context_mismatch");
    }
    let certified_snapshot = repair_reservation
        .history_access
        .take_certified_active_snapshot()
        .filter(|snapshot| {
            snapshot.rollout_path == metadata.rollout_path
                && snapshot.end_byte_offset == file_metadata.len()
        });
    let (active_head, active_scan) = if let Some(snapshot) = certified_snapshot {
        (snapshot.head, snapshot.scan)
    } else {
        let Ok(active_head) =
            super::rollout_lineage::read_rollout_head(metadata.rollout_path.as_path()).await
        else {
            fallback!("active_session_metadata_unreadable");
        };
        let Ok(Some(active_scan)) = model_context::scan_plain_active_model_context_snapshot(
            metadata.rollout_path.as_path(),
            active_head.session_meta.clone(),
        )
        .await
        else {
            fallback!("certified_active_model_context_missing");
        };
        (active_head, active_scan)
    };
    let active_session_meta = active_head.session_meta.clone();
    let same_thread_history_base =
        history_base_belongs_to_thread(store, &active_session_meta, thread_id).await?;
    if active_session_meta.meta.id != thread_id
        || active_session_meta.meta.history_mode != ThreadHistoryMode::Paginated
        || active_session_meta.meta.forked_from_id.is_some()
        || !same_thread_history_base
        || active_session_meta
            .meta
            .subagent_history_start_ordinal
            .is_some()
    {
        fallback!("active_session_metadata_ineligible");
    }
    if !active_scan.segment_checkpoint
        || !store
            .has_complete_history_projection_at(resolved.rollout_id, file_metadata.len())
            .await?
    {
        fallback!("certified_context_or_complete_projection_missing");
    }

    let turn_id = match boundary {
        ForkBoundary::ThroughTurn(turn_id) | ForkBoundary::BeforeTurn(turn_id) => turn_id,
        ForkBoundary::Latest => fallback!("latest_boundary"),
    };
    let pool = store.thread_history_db().await?;
    let row =
        super::thread_history::find_projected_turn(pool, resolved.rollout_id, turn_id.as_str())
            .await?;
    let position = match boundary {
        ForkBoundary::ThroughTurn(_) => {
            if row.status == "inProgress" {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("lastTurnId '{turn_id}' identifies an in-progress turn"),
                });
            }
            let rollout_end_ordinal = row
                .rollout_end_ordinal
                .ok_or_else(|| missing_turn_position(turn_id.as_str()))?;
            let rollout_end_byte_offset = row
                .rollout_end_byte_offset
                .ok_or_else(|| missing_turn_position(turn_id.as_str()))?;
            HistoryPosition {
                thread_id: row.rollout_id,
                end_ordinal_exclusive: u64::try_from(rollout_end_ordinal)
                    .map_err(|_| invalid_turn_position(turn_id.as_str()))?
                    .checked_add(1)
                    .ok_or_else(|| invalid_turn_position(turn_id.as_str()))?,
                end_byte_offset: u64::try_from(rollout_end_byte_offset)
                    .map_err(|_| invalid_turn_position(turn_id.as_str()))?,
            }
        }
        ForkBoundary::BeforeTurn(_) => {
            if row.rollout_end_ordinal == Some(row.rollout_ordinal) {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("turn {turn_id} does not have a persisted start boundary"),
                });
            }
            let rollout_byte_offset = row
                .rollout_byte_offset
                .ok_or_else(|| missing_turn_position(turn_id.as_str()))?;
            HistoryPosition {
                thread_id: row.rollout_id,
                end_ordinal_exclusive: u64::try_from(row.rollout_ordinal)
                    .map_err(|_| invalid_turn_position(turn_id.as_str()))?,
                end_byte_offset: u64::try_from(rollout_byte_offset)
                    .map_err(|_| invalid_turn_position(turn_id.as_str()))?,
            }
        }
        ForkBoundary::Latest => fallback!("latest_boundary"),
    };
    if position.end_ordinal_exclusive > latest_position.end_ordinal_exclusive {
        fallback!("explicit_boundary_exceeds_projection");
    }
    let Some(prefix) = store
        .resolve_bounded_same_thread_prefix_at(
            thread_id,
            resolved.rollout_id,
            metadata.rollout_path.as_path(),
            active_head,
            position,
        )
        .await?
    else {
        fallback!("bounded_same_thread_lineage_unavailable");
    };
    let prefix_thread_id = prefix.segment.thread_id();
    let prefix_rollout_id = prefix.segment.rollout_id();
    let prefix_rollout_path = prefix.segment.rollout_path.clone();
    let Some(model_context) = model_context::load_certified_prefix_for_fork(&prefix.lines)? else {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "the rollout segment containing turn '{turn_id}' has no certified state \
                 checkpoint; migrate the history before forking at that boundary"
            ),
        });
    };
    let model_context = Arc::new(model_context);
    let latest_model_context = Arc::new(active_scan.items);
    let frozen_segment = if persistence == ForkPersistence::ReferenceBacked {
        let writer_reservation = repair_reservation
            .history_access
            .writer_reservation()
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "history repair did not retain indexed-fork writer ownership".to_string(),
            })?;
        let frozen = super::segment::freeze_certified_paginated_prefix_reserved(
            store,
            thread_id,
            prefix_thread_id,
            prefix_rollout_id,
            prefix_rollout_path.as_path(),
            position.end_ordinal_exclusive,
            active_session_meta.clone(),
            prefix.lines,
            writer_reservation,
        )
        .await?;
        if frozen.next_rollout_ordinal != Some(position.end_ordinal_exclusive) {
            fallback!("frozen_segment_ordinal_mismatch");
        }
        Some(frozen)
    } else {
        None
    };
    let ForkHistoryReservation {
        _lifecycle: source_reservation,
        history_access,
    } = repair_reservation;
    drop(history_access);
    Ok(IndexedForkAttempt::Prepared(Box::new(PreparedFork::new(
        thread_id,
        Some(position),
        frozen_segment,
        Arc::clone(&model_context),
        latest_model_context,
        model_context,
        /*interrupt_if_open*/ false,
        crate::ThreadLifecycleReservation::new(source_reservation),
    ))))
}

async fn resolve_compatibility_fork_lineage(
    store: &LocalThreadStore,
    thread_id: codex_protocol::ThreadId,
    expected_rollout_id: Option<RolloutId>,
) -> ThreadStoreResult<(
    super::rollout_lineage::RolloutLineage,
    GoalSupervisorHistoryAccess,
    OwnedRwLockReadGuard<()>,
    bool,
)> {
    let source = resolve_fork_source(store, thread_id).await?;
    trace_fork_stage("resolved_source_for_compatibility");
    let mut history_access =
        super::goal_supervisor_runtime_repair::repair_selected_history_before_access(
            store,
            thread_id,
            source.path.as_path(),
            super::goal_supervisor_runtime_repair::RepairAccess::Compatibility,
        )
        .await?;
    trace_fork_stage("repaired_compatibility_history");
    let source_reservation = history_access
        .take_lifecycle(thread_id)
        .ok_or_else(missing_source_reservation)?;
    // Keep the source reserved until persistence and lineage materialization finish, even if the
    // caller cancels fork preparation.
    let lineage_store = store.clone();
    tokio::spawn(async move {
        let writer_reservation =
            history_access
                .writer_reservation()
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: "history repair did not retain fork writer ownership".to_string(),
                })?;
        let (lineage, source_projection_was_missing) = lineage_store
            .resolve_rollout_lineage_for_reference_reserved(
                thread_id,
                expected_rollout_id,
                writer_reservation,
            )
            .await?;
        #[cfg(test)]
        let pause = {
            LINEAGE_PERSISTENCE_PAUSES
                .lock()
                .expect("lineage persistence pause mutex")
                .remove(&thread_id)
        };
        #[cfg(test)]
        if let Some(pause) = pause {
            pause.entered.notify_one();
            pause.release.notified().await;
        }
        Ok::<_, ThreadStoreError>((
            lineage,
            history_access,
            source_reservation,
            source_projection_was_missing,
        ))
    })
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to resolve fork lineage: {err}"),
    })?
}

fn trace_fork_stage(stage: &'static str) {
    tracing::event!(
        target: "codex_history_io",
        tracing::Level::TRACE,
        event.name = "codex.history.fork.stage",
        stage,
        "completed fork preparation stage"
    );
}

/// Confirms that an active native history boundary does not cross a logical thread.
async fn history_base_belongs_to_thread(
    store: &LocalThreadStore,
    session_meta: &SessionMetaLine,
    thread_id: codex_protocol::ThreadId,
) -> ThreadStoreResult<bool> {
    let Some(history_base) = session_meta.meta.history_base else {
        return Ok(true);
    };
    let Some(path) = codex_rollout::find_rollout_path_by_rollout_id(
        store.config.codex_home.as_path(),
        history_base.thread_id,
    )
    .await
    .map_err(|error| ThreadStoreError::Internal {
        message: format!(
            "failed to resolve history_base rollout {}: {error}",
            history_base.thread_id
        ),
    })?
    else {
        return Ok(false);
    };
    let predecessor = codex_rollout::read_session_meta_line(path.as_path())
        .await
        .map_err(|error| ThreadStoreError::Internal {
            message: format!(
                "failed to read history_base rollout {}: {error}",
                path.display()
            ),
        })?;
    Ok(predecessor.meta.id == thread_id)
}

/// Resolves a source that can be safely referenced by a child rollout.
///
/// Deferred live threads have no file until fork preparation explicitly persists them. Existing
/// explicit-path APIs can read external rollouts, but a child reference must remain confined to
/// the store's `CODEX_HOME`.
async fn resolve_fork_source(
    store: &LocalThreadStore,
    thread_id: codex_protocol::ThreadId,
) -> ThreadStoreResult<super::thread_rollout_resolver::ResolvedThreadRollout> {
    let mut source =
        super::thread_rollout_resolver::resolve_current_including_archived(store, thread_id)
            .await?;
    if source.is_none() {
        live_writer::persist_thread(store, thread_id).await?;
        source =
            super::thread_rollout_resolver::resolve_current_including_archived(store, thread_id)
                .await?;
    }
    let mut source = source.ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
    source.path = super::helpers::scoped_rollout_path(
        store.config.codex_home.clone(),
        source.path.as_path(),
        "Codex home",
    )?;
    Ok(source)
}

/// Confirms that a completely validated lineage can reuse its source-owned indexed projection.
async fn indexed_root_fallback_is_current(
    store: &LocalThreadStore,
    thread_id: codex_protocol::ThreadId,
    lineage: &super::rollout_lineage::RolloutLineage,
    expected_position: HistoryPosition,
) -> ThreadStoreResult<bool> {
    if lineage
        .segments()
        .iter()
        .any(|segment| segment.thread_id() != thread_id || segment.filters_items())
        || lineage
            .segments()
            .first()
            .is_none_or(|segment| segment.start_ordinal() != 1)
        || lineage.segments().windows(2).any(|segments| {
            segments[0]
                .end_ordinal()
                .and_then(|ordinal| ordinal.checked_add(1))
                != Some(segments[1].start_ordinal())
        })
    {
        return Ok(false);
    }
    let Some(state_db) = store.state_db().await else {
        return Ok(false);
    };
    let Some(metadata) =
        state_db
            .get_thread(thread_id)
            .await
            .map_err(|error| ThreadStoreError::Internal {
                message: format!("failed to read indexed fork source metadata: {error}"),
            })?
    else {
        return Ok(false);
    };
    if metadata.archived_at.is_some()
        || metadata
            .rollout_path
            .extension()
            .and_then(|extension| extension.to_str())
            != Some("jsonl")
        || !tokio::fs::metadata(metadata.rollout_path.as_path())
            .await
            .is_ok_and(|metadata| metadata.len() == expected_position.end_byte_offset)
    {
        return Ok(false);
    }
    let Ok(session_meta) =
        codex_rollout::read_session_meta_line(metadata.rollout_path.as_path()).await
    else {
        return Ok(false);
    };
    let same_thread_history_base =
        history_base_belongs_to_thread(store, &session_meta, thread_id).await?;
    if session_meta.meta.id != thread_id
        || session_meta.meta.history_mode != ThreadHistoryMode::Paginated
        || session_meta.meta.forked_from_id.is_some()
        || !same_thread_history_base
        || session_meta.meta.subagent_history_start_ordinal.is_some()
        || !store.has_history_projection(thread_id).await?
    {
        return Ok(false);
    }
    let Ok((active_lines, loaded_thread_id, parse_errors)) =
        codex_rollout::RolloutRecorder::load_rollout_lines(metadata.rollout_path.as_path()).await
    else {
        return Ok(false);
    };
    if loaded_thread_id != Some(thread_id)
        || parse_errors != 0
        || active_lines.iter().enumerate().any(|(index, line)| {
            matches!(
                &line.item,
                RolloutItem::RolloutReference(reference)
                    if index != 1
                        || !super::rollout_lineage::canonical_same_thread_reference(
                            store.config.codex_home.as_path(),
                            metadata.rollout_path.as_path(),
                            thread_id,
                            expected_position.thread_id,
                            reference,
                        )
            )
        })
    {
        return Ok(false);
    }
    let latest_turn = super::thread_history::list_turns(
        store,
        ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: None,
            page_size: 1,
            sort_direction: SortDirection::Desc,
            items_view: StoredTurnItemsView::NotLoaded,
        },
    )
    .await?;
    Ok(!latest_turn
        .turns
        .first()
        .is_some_and(|turn| matches!(turn.status, StoredTurnStatus::InProgress)))
}

/// Reuses a complete projected root only when its exact model context is already authoritative.
///
/// Indexed UI rows omit model response items, settings, and compaction records. A complete live
/// snapshot or a valid replacement-history checkpoint is therefore required before immutable
/// predecessors can be trusted without replaying their JSONL files.
async fn try_prepare_indexed_latest_fork(
    store: &LocalThreadStore,
    thread_id: codex_protocol::ThreadId,
    response_history: ForkResponseHistory,
    supplied_context: Option<(Arc<Vec<ResponseItemEnvelope>>, HistoryPosition)>,
    expected_rollout_id: Option<RolloutId>,
    repair_reservation: ForkHistoryReservation,
    persistence: ForkPersistence,
) -> ThreadStoreResult<IndexedForkAttempt> {
    macro_rules! fallback {
        ($reason:literal) => {{
            tracing::event!(
                target: "codex_history_io",
                tracing::Level::TRACE,
                event.name = "codex.history.fork.indexed_fallback",
                reason = $reason,
                "indexed latest-fork preparation fell back"
            );
            return Ok(IndexedForkAttempt::Fallback(Box::new(repair_reservation)));
        }};
    }
    if store.state_db.is_none() {
        fallback!("state_db_unavailable");
    }
    let Some(resolved) = super::thread_rollout_resolver::resolve_current(store, thread_id).await?
    else {
        fallback!("current_rollout_unresolved");
    };
    if expected_rollout_id.is_some_and(|expected| expected != resolved.rollout_id) {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "rollout path does not select the current rollout for thread {thread_id}"
            ),
        });
    }
    if super::thread_history::projection_state(store, resolved.rollout_id)
        .await?
        .is_none()
    {
        fallback!("projection_state_missing");
    }
    match live_writer::persist_thread_reserved(store, thread_id).await {
        Ok(()) | Err(ThreadStoreError::ThreadNotFound { .. }) => {}
        Err(error) => return Err(error),
    }
    let Some(state_db) = store.state_db().await else {
        fallback!("state_db_runtime_unavailable");
    };
    let Some(metadata) =
        state_db
            .get_thread(thread_id)
            .await
            .map_err(|error| ThreadStoreError::Internal {
                message: format!("failed to read indexed fork source metadata: {error}"),
            })?
    else {
        fallback!("thread_metadata_missing");
    };
    if metadata.archived_at.is_some()
        || metadata
            .rollout_path
            .extension()
            .and_then(|extension| extension.to_str())
            != Some("jsonl")
    {
        fallback!("thread_archived_or_active_not_plain_jsonl");
    }

    let Some(position) = store.projected_history_position(thread_id).await? else {
        fallback!("projected_history_position_missing");
    };
    let Ok(file_metadata) = tokio::fs::metadata(metadata.rollout_path.as_path()).await else {
        fallback!("active_rollout_metadata_unavailable");
    };
    if file_metadata.len() != position.end_byte_offset
        || supplied_context
            .as_ref()
            .is_some_and(|(_, expected)| expected != &position)
    {
        fallback!("active_boundary_or_supplied_context_mismatch");
    }
    if super::helpers::scoped_rollout_path(
        store.config.codex_home.clone(),
        metadata.rollout_path.as_path(),
        "CODEX_HOME",
    )
    .is_err()
    {
        fallback!("active_rollout_outside_codex_home");
    }

    let active_head =
        match super::rollout_lineage::read_rollout_head(metadata.rollout_path.as_path()).await {
            Ok(active_head) => active_head,
            Err(_) => fallback!("active_rollout_header_unreadable"),
        };
    let session_meta = active_head.session_meta.clone();
    if session_meta.meta.id != thread_id {
        fallback!("active_rollout_identity_mismatch");
    }
    let (active_suffix, projected_model_context) = if supplied_context.is_none() {
        let Ok(Some(active_scan)) = model_context::scan_plain_active_model_context_snapshot(
            metadata.rollout_path.as_path(),
            session_meta.clone(),
        )
        .await
        else {
            fallback!("certified_active_model_context_missing");
        };
        if !active_scan.segment_checkpoint {
            fallback!("certified_active_model_context_missing");
        }
        (
            ActiveForkSuffix {
                lines: active_scan.suffix_lines,
                latest_ordinal: active_scan.latest_ordinal,
            },
            Some(Arc::new(active_scan.items)),
        )
    } else {
        let active_suffix =
            match scan_plain_active_authoritative_suffix(metadata.rollout_path.as_path()).await {
                Ok(active_suffix) => active_suffix,
                Err(error) if error.kind() == io::ErrorKind::FileTooLarge => {
                    return Err(model_context::interactive_model_context_scan_error(error));
                }
                Err(_) => fallback!("active_authoritative_suffix_unreadable"),
            };
        (active_suffix, None)
    };
    if active_suffix
        .latest_ordinal
        .and_then(|ordinal| ordinal.checked_add(1))
        != Some(position.end_ordinal_exclusive)
    {
        fallback!("active_rollout_ordinal_mismatch");
    }
    let same_thread_history_base =
        history_base_belongs_to_thread(store, &session_meta, thread_id).await?;
    if session_meta.meta.id != thread_id
        || session_meta.meta.history_mode != ThreadHistoryMode::Paginated
        || session_meta.meta.forked_from_id.is_some()
        || !same_thread_history_base
        || session_meta.meta.subagent_history_start_ordinal.is_some()
    {
        fallback!("active_session_metadata_ineligible");
    }

    if active_head.leading_reference.is_some() && !active_head.has_local_history
        || active_head
            .leading_reference
            .as_ref()
            .is_some_and(|(_, reference)| {
                !super::rollout_lineage::canonical_same_thread_reference(
                    store.config.codex_home.as_path(),
                    metadata.rollout_path.as_path(),
                    thread_id,
                    position.thread_id,
                    reference,
                )
            })
        || active_suffix
            .lines
            .iter()
            .any(|line| matches!(line.item, RolloutItem::RolloutReference(_)))
    {
        fallback!("active_rollout_reference_layout_ineligible");
    }
    if let Some((_, reference)) = active_head.leading_reference.as_ref() {
        if codex_rollout::existing_rollout_path(reference.rollout_path.as_path()).await
            != Some(reference.rollout_path.clone())
        {
            fallback!("referenced_rollout_unavailable");
        }
        if super::helpers::scoped_rollout_path(
            store.config.codex_home.clone(),
            reference.rollout_path.as_path(),
            "CODEX_HOME",
        )
        .is_err()
        {
            fallback!("referenced_rollout_outside_codex_home");
        }
        let Ok(reference_meta) =
            codex_rollout::read_session_meta_line(reference.rollout_path.as_path()).await
        else {
            fallback!("referenced_session_metadata_unreadable");
        };
        if reference_meta.meta.id != thread_id
            || reference_meta.meta.segment_id != reference.segment_id
            || reference_meta.meta.history_mode != ThreadHistoryMode::Paginated
            || reference_meta.meta.forked_from_id.is_some()
            || reference_meta.meta.history_base.is_some()
            || reference_meta.meta.subagent_history_start_ordinal.is_some()
        {
            fallback!("referenced_session_metadata_ineligible");
        }
    }

    if !store.has_history_projection(thread_id).await? {
        fallback!("complete_history_projection_unavailable");
    }

    let (model_context, shared_model_response_items) = if let Some((items, _)) = supplied_context {
        if active_suffix.lines.iter().any(|line| {
            matches!(
                line.item,
                RolloutItem::Compacted(_)
                    | RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ThreadRolledBack(
                        _
                    ))
            )
        }) {
            fallback!("supplied_context_conflicts_with_active_history");
        }
        (
            authoritative_model_metadata(&session_meta, active_suffix.lines.as_slice()),
            Some(items),
        )
    } else {
        let Some(model_context) = projected_model_context else {
            fallback!("projected_model_context_missing");
        };
        (model_context, None)
    };
    let projected_response_turns = if matches!(response_history, ForkResponseHistory::Full) {
        let turns = load_projected_response_turns(store, thread_id).await?;
        if turns
            .last()
            .is_some_and(|turn| matches!(turn.status, StoredTurnStatus::InProgress))
        {
            fallback!("projected_full_history_has_active_turn");
        }
        Some(Arc::new(turns))
    } else {
        let latest = super::thread_history::list_turns(
            store,
            ListTurnsParams {
                thread_id,
                include_archived: false,
                cursor: None,
                page_size: 1,
                sort_direction: SortDirection::Desc,
                items_view: StoredTurnItemsView::NotLoaded,
            },
        )
        .await?;
        if latest
            .turns
            .first()
            .is_some_and(|turn| matches!(turn.status, StoredTurnStatus::InProgress))
        {
            fallback!("projected_latest_history_has_active_turn");
        }
        None
    };

    let frozen_segment = if persistence == ForkPersistence::ReferenceBacked {
        let writer_reservation = repair_reservation
            .history_access
            .writer_reservation()
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "history repair did not retain indexed-fork writer ownership".to_string(),
            })?;
        let frozen_segment = super::segment::freeze_thread_segment_reserved(
            store,
            thread_id,
            FreezeRolloutSegmentParams::snapshot(),
            expected_rollout_id,
            writer_reservation,
        )
        .await?;
        if frozen_segment.next_rollout_ordinal != Some(position.end_ordinal_exclusive) {
            fallback!("frozen_segment_ordinal_mismatch");
        }
        Some(frozen_segment)
    } else {
        None
    };
    let ForkHistoryReservation {
        _lifecycle: source_reservation,
        history_access,
    } = repair_reservation;
    // The durable immutable snapshot no longer depends on the mutable source. Keep only the
    // lifecycle lease required to prevent deletion until the child reference is persisted.
    drop(history_access);
    let mut prepared = PreparedFork::new(
        thread_id,
        Some(position),
        frozen_segment,
        Arc::clone(&model_context),
        Arc::clone(&model_context),
        Arc::clone(&model_context),
        /*interrupt_if_open*/ true,
        crate::ThreadLifecycleReservation::new(source_reservation),
    );
    prepared.projected_response_turns = projected_response_turns;
    prepared.shared_model_response_items = shared_model_response_items;
    Ok(IndexedForkAttempt::Prepared(Box::new(prepared)))
}

/// Prepares `/side` and `excludeTurns` from a certified active suffix when SQLite is absent.
///
/// The immutable snapshot retains the complete lineage. Only model-visible startup context is
/// hydrated, so a missing projection must not force a complete predecessor traversal.
async fn try_prepare_certified_latest_model_context_fork(
    store: &LocalThreadStore,
    thread_id: codex_protocol::ThreadId,
    expected_rollout_id: Option<RolloutId>,
    repair_reservation: ForkHistoryReservation,
    persistence: ForkPersistence,
) -> ThreadStoreResult<IndexedForkAttempt> {
    macro_rules! fallback {
        ($reason:literal) => {{
            tracing::event!(
                target: "codex_history_io",
                tracing::Level::TRACE,
                event.name = "codex.history.fork.certified_context_fallback",
                reason = $reason,
                "certified latest model-context preparation fell back"
            );
            return Ok(IndexedForkAttempt::Fallback(Box::new(repair_reservation)));
        }};
    }

    let Some(resolved) = super::thread_rollout_resolver::resolve_current(store, thread_id).await?
    else {
        fallback!("current_rollout_unresolved");
    };
    if expected_rollout_id.is_some_and(|expected| expected != resolved.rollout_id) {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "rollout path does not select the current rollout for thread {thread_id}"
            ),
        });
    }
    match live_writer::persist_thread_reserved(store, thread_id).await {
        Ok(()) | Err(ThreadStoreError::ThreadNotFound { .. }) => {}
        Err(error) => return Err(error),
    }
    let path = super::helpers::scoped_rollout_path(
        store.config.codex_home.clone(),
        resolved.path.as_path(),
        "CODEX_HOME",
    )?;
    if path.extension().and_then(|extension| extension.to_str()) != Some("jsonl") {
        fallback!("active_rollout_not_plain");
    }
    let Ok(session_meta) = codex_rollout::read_session_meta_line(path.as_path()).await else {
        fallback!("session_metadata_unreadable");
    };
    let same_thread_history_base =
        history_base_belongs_to_thread(store, &session_meta, thread_id).await?;
    if session_meta.meta.id != thread_id
        || session_meta.meta.history_mode != ThreadHistoryMode::Paginated
        || session_meta.meta.forked_from_id.is_some()
        || !same_thread_history_base
        || session_meta.meta.subagent_history_start_ordinal.is_some()
    {
        fallback!("session_metadata_ineligible");
    }
    let Some(active_scan) =
        model_context::scan_plain_active_model_context_snapshot(path.as_path(), session_meta)
            .await?
    else {
        fallback!("active_model_context_missing");
    };
    if !active_scan.segment_checkpoint {
        fallback!("active_checkpoint_missing");
    }
    let Some(end_ordinal_exclusive) = active_scan
        .latest_ordinal
        .and_then(|ordinal| ordinal.checked_add(1))
    else {
        fallback!("active_ordinal_missing");
    };
    let end_byte_offset = tokio::fs::metadata(path.as_path())
        .await
        .map_err(|error| ThreadStoreError::Internal {
            message: format!("failed to read active rollout metadata: {error}"),
        })?
        .len();
    let position = HistoryPosition {
        thread_id: resolved.rollout_id,
        end_ordinal_exclusive,
        end_byte_offset,
    };
    let frozen_segment = if persistence == ForkPersistence::ReferenceBacked {
        let writer_reservation = repair_reservation
            .history_access
            .writer_reservation()
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "history repair did not retain latest-fork writer ownership".to_string(),
            })?;
        let frozen_segment = super::segment::freeze_thread_segment_reserved(
            store,
            thread_id,
            FreezeRolloutSegmentParams::snapshot(),
            expected_rollout_id,
            writer_reservation,
        )
        .await?;
        if frozen_segment.next_rollout_ordinal != Some(end_ordinal_exclusive) {
            fallback!("frozen_ordinal_mismatch");
        }
        Some(frozen_segment)
    } else {
        None
    };
    let ForkHistoryReservation {
        _lifecycle: source_reservation,
        history_access,
    } = repair_reservation;
    drop(history_access);
    let model_context = Arc::new(active_scan.items);
    Ok(IndexedForkAttempt::Prepared(Box::new(PreparedFork::new(
        thread_id,
        Some(position),
        frozen_segment,
        Arc::clone(&model_context),
        Arc::clone(&model_context),
        model_context,
        /*interrupt_if_open*/ true,
        crate::ThreadLifecycleReservation::new(source_reservation),
    ))))
}

async fn scan_plain_active_authoritative_suffix(path: &Path) -> io::Result<ActiveForkSuffix> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut scanner = ReverseJsonlScanner::new(std::fs::File::open(path)?)?
            .with_max_record_bytes(model_context::MAX_INTERACTIVE_MODEL_CONTEXT_RECORD_BYTES);
        let mut lines = Vec::new();
        let mut latest_ordinal = None;
        let mut saw_latest_user = false;
        loop {
            let outcome = scanner.scan_next::<serde_json::Value>()?;
            if scanner.oversized_records_skipped() != 0
                || scanner.bytes_scanned()
                    > model_context::MAX_INTERACTIVE_MODEL_CONTEXT_SCAN_BYTES
            {
                return Err(io::Error::new(
                    io::ErrorKind::FileTooLarge,
                    "latest active turn exceeds the interactive scan limit; run `codex migrate-rollouts`",
                ));
            }
            let Some(outcome) = outcome else {
                break;
            };
            let value = match outcome {
                ScanOutcome::Parsed(value) => value,
                ScanOutcome::Rejected(error) => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, error));
                }
            };
            let Some(line) = codex_rollout::RolloutRecorder::parse_rollout_line_value(value)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
            else {
                continue;
            };
            latest_ordinal = latest_ordinal.max(line.ordinal);
            if matches!(line.item, RolloutItem::SessionMeta(_)) {
                break;
            }
            if matches!(line.item, RolloutItem::RolloutReference(_)) {
                continue;
            }
            let previous_turn_complete = saw_latest_user
                && matches!(
                    line.item,
                    RolloutItem::EventMsg(
                        codex_protocol::protocol::EventMsg::TurnComplete(_)
                    )
                );
            if previous_turn_complete {
                break;
            }
            saw_latest_user |= matches!(
                &line.item,
                RolloutItem::ResponseItem(envelope)
                    if matches!(
                        &envelope.item,
                        codex_protocol::models::ResponseItem::Message { role, .. }
                            if role == "user"
                    )
            );
            lines.push(line);
        }
        lines.reverse();
        Ok(ActiveForkSuffix {
            lines,
            latest_ordinal,
        })
    })
    .await
    .map_err(|error| io::Error::other(format!("failed to join active suffix scan: {error}")))?
}

fn authoritative_model_metadata(
    session_meta: &SessionMetaLine,
    active_lines: &[RolloutLine],
) -> Arc<Vec<RolloutItem>> {
    let mut items = Vec::with_capacity(active_lines.len() + 1);
    items.push(RolloutItem::SessionMeta(session_meta.clone()));
    let latest_user_ordinal = active_lines.iter().rev().find_map(|line| match &line.item {
        RolloutItem::ResponseItem(envelope)
            if matches!(&envelope.item, codex_protocol::models::ResponseItem::Message { role, .. } if role == "user") => {
            line.ordinal
        }
        _ => None,
    });
    items.extend(active_lines.iter().filter_map(|line| match &line.item {
        RolloutItem::ResponseItem(envelope)
            if matches!(&envelope.item, codex_protocol::models::ResponseItem::Message { role, .. } if role == "user")
                && line.ordinal == latest_user_ordinal => {
            Some(line.item.clone())
        }
        RolloutItem::TurnContext(_)
        | RolloutItem::WorldState(_)
        | RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::TokenCount(_))
        | RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::UserMessage(_))
        | RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::TurnStarted(_))
        | RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::TurnComplete(_))
        | RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ItemCompleted(_))
        | RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::ThreadSettingsApplied(_)) => {
            Some(line.item.clone())
        }
        _ => None,
    }));
    Arc::new(items)
}

async fn load_projected_response_turns(
    store: &LocalThreadStore,
    thread_id: codex_protocol::ThreadId,
) -> ThreadStoreResult<Vec<StoredTurn>> {
    let page_size = usize::try_from(i64::MAX - 1).map_err(|_| ThreadStoreError::Internal {
        message: "projected fork history exceeds the supported page size".to_string(),
    })?;
    let mut turns = super::thread_history::list_turns(
        store,
        ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: None,
            page_size,
            sort_direction: SortDirection::Asc,
            items_view: StoredTurnItemsView::NotLoaded,
        },
    )
    .await?
    .turns;
    let items = super::thread_history::list_items(
        store,
        ListItemsParams {
            thread_id,
            turn_id: None,
            include_archived: false,
            cursor: None,
            page_size,
            sort_direction: SortDirection::Asc,
            sort_key: ItemSortKey::CreatedAtOrdinal,
            after_updated_at_ordinal: None,
        },
    )
    .await?
    .items;
    let mut items_by_turn = HashMap::new();
    for item in items {
        items_by_turn
            .entry(item.turn_id.clone())
            .or_insert_with(Vec::new)
            .push(item);
    }
    for turn in &mut turns {
        turn.items = items_by_turn
            .remove(turn.turn_id.as_str())
            .unwrap_or_default();
    }
    Ok(turns)
}

fn missing_turn_position(turn_id: &str) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: format!("turn {turn_id} does not have persisted rollout positions"),
    }
}

pub(super) async fn history_base_at_boundary(
    store: &LocalThreadStore,
    thread_id: codex_protocol::ThreadId,
    boundary: ForkBoundary,
    lineage: &super::rollout_lineage::RolloutLineage,
) -> ThreadStoreResult<Option<HistoryPosition>> {
    let source_segment = lineage
        .segments()
        .last()
        .ok_or_else(|| ThreadStoreError::Internal {
            message: "fork lineage has no source segment".to_string(),
        })?;
    let latest_projection_state =
        super::thread_history::projection_state(store, source_segment.rollout_id())
            .await?
            .ok_or_else(|| ThreadStoreError::Internal {
                message: format!("missing projection state for paginated thread {thread_id}"),
            })?;
    let latest_position = HistoryPosition {
        thread_id: source_segment.rollout_id(),
        end_ordinal_exclusive: latest_projection_state.next_ordinal,
        end_byte_offset: latest_projection_state.next_byte_offset,
    };
    let pool = store.thread_history_db().await?;
    let position = match boundary {
        ForkBoundary::Latest => latest_position,
        ForkBoundary::ThroughTurn(turn_id) => {
            let row = find_visible_turn(pool, lineage, turn_id.as_str()).await?;
            if row.status == "inProgress" {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("lastTurnId '{turn_id}' identifies an in-progress turn"),
                });
            }
            let rollout_end_ordinal = row
                .rollout_end_ordinal
                .ok_or_else(|| missing_turn_position(turn_id.as_str()))?;
            let rollout_end_byte_offset = row
                .rollout_end_byte_offset
                .ok_or_else(|| missing_turn_position(turn_id.as_str()))?;
            HistoryPosition {
                thread_id: row.rollout_id,
                end_ordinal_exclusive: u64::try_from(rollout_end_ordinal)
                    .map_err(|_| invalid_turn_position(turn_id.as_str()))?
                    .checked_add(1)
                    .ok_or_else(|| invalid_turn_position(turn_id.as_str()))?,
                end_byte_offset: u64::try_from(rollout_end_byte_offset)
                    .map_err(|_| invalid_turn_position(turn_id.as_str()))?,
            }
        }
        ForkBoundary::BeforeTurn(turn_id) => {
            let row = find_source_turn(pool, lineage, turn_id.as_str()).await?;
            if row.rollout_end_ordinal == Some(row.rollout_ordinal) {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("turn {turn_id} does not have a persisted start boundary"),
                });
            }
            let rollout_byte_offset = row
                .rollout_byte_offset
                .ok_or_else(|| missing_turn_position(turn_id.as_str()))?;
            HistoryPosition {
                thread_id: row.rollout_id,
                end_ordinal_exclusive: u64::try_from(row.rollout_ordinal)
                    .map_err(|_| invalid_turn_position(turn_id.as_str()))?,
                end_byte_offset: u64::try_from(rollout_byte_offset)
                    .map_err(|_| invalid_turn_position(turn_id.as_str()))?,
            }
        }
    };
    let segment_index = lineage.segments().iter().position(|segment| {
        segment.rollout_id() == position.thread_id
            && position.end_ordinal_exclusive >= segment.start_ordinal()
            && segment
                .end_ordinal()
                .is_none_or(|end| position.end_ordinal_exclusive <= end)
    });
    let Some(segment_index) = segment_index else {
        return Err(ThreadStoreError::InvalidRequest {
            message: "fork boundary exceeds inherited source history".to_string(),
        });
    };
    let segment = &lineage.segments()[segment_index];
    if segment
        .end_ordinal()
        .is_some_and(|end| position.end_ordinal_exclusive > end)
        || segment
            .end_byte_offset
            .is_some_and(|end| position.end_byte_offset > end)
    {
        return Err(ThreadStoreError::InvalidRequest {
            message: "fork boundary exceeds inherited source history".to_string(),
        });
    }
    if position.end_ordinal_exclusive == segment.start_ordinal() {
        Ok(segment_index.checked_sub(1).and_then(|index| {
            let previous = &lineage.segments()[index];
            Some(HistoryPosition {
                thread_id: previous.rollout_id(),
                end_ordinal_exclusive: previous.end_ordinal()?,
                end_byte_offset: previous.end_byte_offset?,
            })
        }))
    } else {
        Ok(Some(position))
    }
}

fn invalid_turn_position(turn_id: &str) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("invalid rollout position for turn {turn_id}"),
    }
}

fn missing_source_reservation() -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: "fork preparation lost its source lifecycle reservation".to_string(),
    }
}
