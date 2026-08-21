use codex_protocol::RolloutId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use serde::Deserialize;
use serde::Serialize;
use sqlx::Row;

use super::super::LocalThreadStore;
use super::super::rollout_lineage::RolloutLineage;
use super::super::rollout_lineage::RolloutLineageSegment;
use super::super::thread_rollout_resolver::ResolvedThreadRollout;
use super::segment_paging::page_indexed_item_rows;
use super::segment_paging::page_indexed_turn_rows;
use super::segment_paging::page_item_rows;
use super::segment_paging::page_turn_rows;
use super::segment_paging::validate_page_size;
use super::sqlite_integer;
use super::turn_lookup::find_source_turn;
use crate::ItemPage;
use crate::ListItemsParams;
use crate::ListTurnsParams;
use crate::StoredThreadItem;
use crate::StoredTurn;
use crate::StoredTurnError;
use crate::StoredTurnItemsView;
use crate::StoredTurnStatus;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use crate::TurnPage;

#[cfg(test)]
#[path = "read_tests.rs"]
mod tests;

#[derive(Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub(super) enum CursorScope {
    Turns,
    ItemsByCreatedAtOrdinal,
    ItemsByUpdatedAtOrdinal,
}

/// A history position bound to one logical thread and its selected physical rollout.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct HistoryCursor {
    pub requested_thread_id: ThreadId,
    pub root_rollout_id: RolloutId,
    pub rollout_ordinal: u64,
    pub include_anchor: bool,
    pub scope: CursorScope,
}

#[derive(Clone, Copy)]
pub(super) struct PhysicalHistoryPosition {
    pub rollout_ordinal: i64,
}

pub(super) struct StoredTurnRow {
    pub position: PhysicalHistoryPosition,
    pub turn_id: String,
    pub status: StoredTurnStatus,
    pub error: Option<StoredTurnError>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub duration_ms: Option<i64>,
    pub first_user_item_id: Option<String>,
    pub final_agent_item_id: Option<String>,
    pub summary_items: Vec<StoredThreadItem>,
}

#[derive(sqlx::FromRow)]
pub(super) struct StoredSummaryColumns {
    summary_first_user_turn_id: Option<String>,
    summary_first_user_item_id: Option<String>,
    summary_first_user_rollout_ordinal: Option<i64>,
    summary_first_user_updated_at_ordinal: Option<i64>,
    summary_first_user_created_at_ms: Option<i64>,
    summary_first_user_item_json: Option<String>,
    summary_final_agent_turn_id: Option<String>,
    summary_final_agent_item_id: Option<String>,
    summary_final_agent_rollout_ordinal: Option<i64>,
    summary_final_agent_updated_at_ordinal: Option<i64>,
    summary_final_agent_created_at_ms: Option<i64>,
    summary_final_agent_item_json: Option<String>,
}

struct StoredSummaryItemColumns {
    turn_id: Option<String>,
    item_id: Option<String>,
    rollout_ordinal: Option<i64>,
    updated_at_ordinal: Option<i64>,
    created_at_ms: Option<i64>,
    item_json: Option<String>,
}

pub(super) struct StoredThreadItemRow {
    pub position: PhysicalHistoryPosition,
    pub item: StoredThreadItem,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct LegacyProjectionCursor {
    turn_id: String,
    include_anchor: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LegacyProjectionLookup {
    ExistingOnly,
    Backfill,
}

pub(in crate::local) async fn list_turns(
    store: &LocalThreadStore,
    params: ListTurnsParams,
) -> ThreadStoreResult<TurnPage> {
    validate_thread_for_paginated_reads(
        store,
        params.thread_id,
        params.include_archived,
        "list_turns",
    )
    .await?;
    validate_page_size(params.page_size)?;
    let lineage = match indexed_same_thread_lineage(store, params.thread_id).await? {
        Some(lineage) => lineage,
        None => store.resolve_rollout_lineage(params.thread_id).await?,
    };
    list_turns_from_lineage(store, params, &lineage).await
}

async fn list_turns_from_lineage(
    store: &LocalThreadStore,
    params: ListTurnsParams,
    lineage: &RolloutLineage,
) -> ThreadStoreResult<TurnPage> {
    let pool = store.thread_history_db().await?;
    let page = page_turn_rows(
        pool,
        params.thread_id,
        lineage,
        params.cursor.as_deref(),
        params.page_size,
        params.sort_direction,
        params.items_view,
    )
    .await?;
    let mut turns = Vec::with_capacity(page.rows.len());
    for turn in page.rows {
        let items = match params.items_view {
            StoredTurnItemsView::NotLoaded => Vec::new(),
            StoredTurnItemsView::Summary
                if matches!(turn.status, StoredTurnStatus::Interrupted)
                    && turn.first_user_item_id.is_none()
                    && turn.final_agent_item_id.is_none() =>
            {
                // Synthetic fork-boundary rows are interrupted without local summary IDs.
                // Load their summary from the earliest visible source turn.
                load_inherited_summary_items(pool, lineage, &turn).await?
            }
            StoredTurnItemsView::Summary => turn.summary_items,
        };
        turns.push(StoredTurn {
            turn_id: turn.turn_id,
            items,
            items_view: params.items_view,
            status: turn.status,
            error: turn.error,
            started_at: turn.started_at,
            completed_at: turn.completed_at,
            duration_ms: turn.duration_ms,
        });
    }

    Ok(TurnPage {
        turns,
        next_cursor: page.next_cursor,
        backwards_cursor: page.backwards_cursor,
    })
}

pub(in crate::local) async fn list_items(
    store: &LocalThreadStore,
    params: ListItemsParams,
) -> ThreadStoreResult<ItemPage> {
    validate_thread_for_paginated_reads(
        store,
        params.thread_id,
        params.include_archived,
        "list_items",
    )
    .await?;
    validate_page_size(params.page_size)?;
    let lineage = match indexed_same_thread_lineage(store, params.thread_id).await? {
        Some(lineage) => lineage,
        None => store.resolve_rollout_lineage(params.thread_id).await?,
    };
    let pool = store.thread_history_db().await?;
    let page = page_item_rows(pool, &lineage, &params).await?;

    Ok(ItemPage {
        items: page.rows.into_iter().map(|row| row.item).collect(),
        next_cursor: page.next_cursor,
        backwards_cursor: page.backwards_cursor,
    })
}

/// Treats a fully projected root thread as one logical ordinal range instead of its segments.
///
/// Immutable predecessors were validated when their SQLite rows were projected. Revalidating
/// every predecessor during an indexed page would make the request depend on thread length.
pub(in crate::local) async fn indexed_same_thread_lineage(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<Option<RolloutLineage>> {
    let Some(resolved) =
        super::super::thread_rollout_resolver::resolve_current_including_archived(store, thread_id)
            .await?
    else {
        return Ok(None);
    };
    indexed_same_thread_lineage_from_resolved(store, thread_id, resolved).await
}

async fn indexed_same_thread_lineage_from_resolved(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    resolved: ResolvedThreadRollout,
) -> ThreadStoreResult<Option<RolloutLineage>> {
    let rollout_id = resolved.rollout_id;
    let rollout_path = resolved.path;
    let authenticated_session_meta = resolved.authenticated_session_meta;
    let Some(projection) = super::projection_state(store, rollout_id).await? else {
        return Ok(None);
    };
    if !projection.lineage_complete {
        return Ok(None);
    }
    let Some(state_db) = store.state_db().await else {
        return Ok(None);
    };
    let Some(metadata) =
        state_db
            .get_thread(thread_id)
            .await
            .map_err(|error| ThreadStoreError::Internal {
                message: format!("failed to read indexed thread metadata: {error}"),
            })?
    else {
        return Ok(None);
    };
    if metadata.archived_at.is_some() {
        return Ok(None);
    }

    let Ok(file_metadata) = tokio::fs::metadata(rollout_path.as_path()).await else {
        return Ok(None);
    };
    if file_metadata.len() != projection.next_byte_offset {
        return Ok(None);
    }

    let session_meta = match authenticated_session_meta {
        Some(session_meta) => session_meta,
        None => {
            let Ok(session_meta) =
                codex_rollout::read_session_meta_line(rollout_path.as_path()).await
            else {
                return Ok(None);
            };
            session_meta.meta
        }
    };
    if session_meta.id != thread_id
        || session_meta.history_mode != ThreadHistoryMode::Paginated
        || (session_meta.forked_from_id.is_some() && session_meta.history_base.is_none())
        || (session_meta.subagent_history_start_ordinal.is_some()
            && session_meta.history_base.is_none())
    {
        return Ok(None);
    }

    Ok(Some(RolloutLineage {
        root_rollout_id: rollout_id,
        segments: vec![RolloutLineageSegment {
            thread_id,
            rollout_id,
            rollout_path,
            start_ordinal: 0,
            end_ordinal_exclusive: None,
            jsonl_end_byte_offset: None,
            end_byte_offset: None,
            filter_texts: Vec::new(),
            goal_supervisor_provenance: Default::default(),
            uses_history_base: false,
            uses_fork_boundary: false,
        }],
    }))
}

/// Checks the complete root checkpoint without resolving the selected rollout a second time.
///
/// Forked histories retain the caller's complete lineage check. A complete same-thread native
/// `history_base` projection is already one logical ordinal range. Empty turns are valid in any
/// status; out-of-band projection mutations invalidate the checkpoint through SQLite triggers.
pub(in crate::local) async fn has_complete_root_projection_for_resolved(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    resolved: ResolvedThreadRollout,
) -> ThreadStoreResult<bool> {
    Ok(
        indexed_same_thread_lineage_from_resolved(store, thread_id, resolved)
            .await?
            .is_some(),
    )
}

/// Read an existing segmented legacy projection without exposing indexed cursors.
pub(in crate::local) async fn list_segmented_legacy_turns(
    store: &LocalThreadStore,
    params: ListTurnsParams,
) -> ThreadStoreResult<Option<TurnPage>> {
    list_segmented_legacy_turns_with_lookup(store, params, LegacyProjectionLookup::Backfill).await
}

/// Reads a complete legacy projection without creating or rebuilding its history database.
pub(in crate::local) async fn list_existing_segmented_legacy_turns(
    store: &LocalThreadStore,
    params: ListTurnsParams,
) -> ThreadStoreResult<Option<TurnPage>> {
    list_segmented_legacy_turns_with_lookup(store, params, LegacyProjectionLookup::ExistingOnly)
        .await
}

/// Checks legacy projection completeness without starting an initial history backfill.
pub(in crate::local) async fn has_complete_segmented_legacy_projection(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<bool> {
    Ok(
        existing_legacy_projection(store, thread_id, LegacyProjectionLookup::ExistingOnly)
            .await?
            .is_some(),
    )
}

async fn list_segmented_legacy_turns_with_lookup(
    store: &LocalThreadStore,
    params: ListTurnsParams,
    lookup: LegacyProjectionLookup,
) -> ThreadStoreResult<Option<TurnPage>> {
    validate_page_size(params.page_size)?;
    let _live_writer_guard = store.live_writer_locks.lock(params.thread_id).await;
    let Some((pool, rollout_id)) =
        existing_legacy_projection(store, params.thread_id, lookup).await?
    else {
        return Ok(None);
    };
    let indexed_cursor = match params.cursor.as_deref() {
        Some(cursor) => Some(legacy_turn_cursor_to_index(pool, rollout_id, cursor).await?),
        None => None,
    };
    let page = page_indexed_turn_rows(
        pool,
        rollout_id,
        indexed_cursor.as_deref(),
        params.page_size,
        params.sort_direction,
    )
    .await?;
    let mut turns = Vec::with_capacity(page.rows.len());
    for turn in page.rows {
        let items = match params.items_view {
            StoredTurnItemsView::NotLoaded => Vec::new(),
            StoredTurnItemsView::Summary => {
                load_legacy_summary_items(pool, rollout_id, &turn).await?
            }
        };
        turns.push(StoredTurn {
            turn_id: turn.turn_id,
            items,
            items_view: params.items_view,
            status: turn.status,
            error: turn.error,
            started_at: turn.started_at,
            completed_at: turn.completed_at,
            duration_ms: turn.duration_ms,
        });
    }
    Ok(Some(TurnPage {
        turns,
        next_cursor: indexed_turn_cursor_to_legacy(pool, rollout_id, page.next_cursor.as_deref())
            .await?,
        backwards_cursor: indexed_turn_cursor_to_legacy(
            pool,
            rollout_id,
            page.backwards_cursor.as_deref(),
        )
        .await?,
    }))
}

/// Hydrate indexed items without changing the legacy public items API.
pub(in crate::local) async fn list_segmented_legacy_items(
    store: &LocalThreadStore,
    params: ListItemsParams,
) -> ThreadStoreResult<Option<ItemPage>> {
    validate_page_size(params.page_size)?;
    let _live_writer_guard = store.live_writer_locks.lock(params.thread_id).await;
    let Some((pool, rollout_id)) =
        existing_legacy_projection(store, params.thread_id, LegacyProjectionLookup::Backfill)
            .await?
    else {
        return Ok(None);
    };
    let page = page_indexed_item_rows(
        pool,
        rollout_id,
        params.turn_id.as_deref(),
        params.cursor.as_deref(),
        params.page_size,
        params.sort_direction,
    )
    .await?;
    Ok(Some(ItemPage {
        items: page.rows.into_iter().map(|row| row.item).collect(),
        next_cursor: page.next_cursor,
        backwards_cursor: page.backwards_cursor,
    }))
}

async fn existing_legacy_projection(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    lookup: LegacyProjectionLookup,
) -> ThreadStoreResult<Option<(&sqlx::SqlitePool, RolloutId)>> {
    let Some(state_db) = store.state_db().await else {
        return Ok(None);
    };
    let Some(metadata) = state_db
        .get_thread(thread_id)
        .await
        .map_err(super::thread_history_error)?
    else {
        return Ok(None);
    };
    if metadata.history_mode != ThreadHistoryMode::Legacy {
        return Ok(None);
    }
    let rollout_id =
        super::super::thread_rollout_resolver::rollout_id_from_path_or_legacy_thread_id(
            metadata.rollout_path.as_path(),
            thread_id,
            ThreadHistoryMode::Legacy,
        )?;

    let session_meta = match codex_rollout::read_session_meta_line(&metadata.rollout_path).await {
        Ok(session_meta) => session_meta.meta,
        Err(_) => return Ok(None),
    };
    if session_meta.id != thread_id
        || session_meta.history_mode != ThreadHistoryMode::Legacy
        || session_meta.segment_id.is_none()
        || session_meta.subagent_history_start_ordinal.is_some()
        || session_meta.history_base.is_some()
    {
        return Ok(None);
    }

    let mut reader = match codex_rollout::open_rollout_line_reader(&metadata.rollout_path).await {
        Ok(reader) => reader,
        Err(_) => return Ok(None),
    };
    let mut saw_session_meta = false;
    while let Some(line) = reader
        .next_line()
        .await
        .map_err(super::thread_history_error)?
    {
        if line.trim().is_empty() {
            continue;
        }
        let line = match codex_rollout::RolloutRecorder::parse_rollout_line_bytes(line.as_bytes()) {
            Ok(Some(line)) => line,
            Ok(None) => continue,
            Err(_) => return Ok(None),
        };
        if !saw_session_meta {
            if !matches!(line.item, RolloutItem::SessionMeta(_)) {
                return Ok(None);
            }
            saw_session_meta = true;
            continue;
        }
        if let RolloutItem::RolloutReference(reference) = line.item
            && (reference.thread_id != Some(thread_id)
                || reference.nth_user_message.is_some()
                || reference
                    .compacted_replacement_history_filter_texts
                    .as_ref()
                    .is_some_and(|filters| !filters.is_empty()))
        {
            return Ok(None);
        }
        break;
    }

    let rollout_len = match tokio::fs::metadata(&metadata.rollout_path).await {
        Ok(metadata) => metadata.len(),
        Err(_) => return Ok(None),
    };

    let db_path = store.config.sqlite.thread_history_db_path();
    let db_exists = tokio::fs::try_exists(db_path)
        .await
        .map_err(super::thread_history_error)?;
    if !db_exists && lookup == LegacyProjectionLookup::ExistingOnly {
        return Ok(None);
    }
    if !db_exists
        && let Err(error) = super::super::live_writer::backfill_segmented_legacy_projection(
            store,
            thread_id,
            metadata.rollout_path.as_path(),
        )
        .await
    {
        tracing::warn!("failed to backfill segmented legacy history for {thread_id}: {error}");
        return Ok(None);
    }

    let pool = store.thread_history_db().await?;
    let mut projected_len = sqlx::query_scalar::<_, i64>(
        "SELECT next_rollout_byte_offset FROM thread_history_projection_state WHERE thread_id = ? LIMIT 1",
    )
    .bind(rollout_id.to_string())
    .fetch_optional(pool)
    .await
    .map_err(super::thread_history_error)?;
    let projection_is_current = projected_len
        .and_then(|offset| u64::try_from(offset).ok())
        .is_some_and(|offset| offset == rollout_len);
    if !projection_is_current && lookup == LegacyProjectionLookup::Backfill {
        if let Err(error) = super::super::live_writer::backfill_segmented_legacy_projection(
            store,
            thread_id,
            metadata.rollout_path.as_path(),
        )
        .await
        {
            tracing::warn!("failed to backfill segmented legacy history for {thread_id}: {error}");
            return Ok(None);
        }
        projected_len = sqlx::query_scalar::<_, i64>(
            "SELECT next_rollout_byte_offset FROM thread_history_projection_state WHERE thread_id = ? LIMIT 1",
        )
        .bind(rollout_id.to_string())
        .fetch_optional(pool)
        .await
        .map_err(super::thread_history_error)?;
    }
    Ok(projected_len
        .and_then(|offset| u64::try_from(offset).ok())
        .filter(|offset| *offset == rollout_len)
        .map(|_| (pool, rollout_id)))
}

async fn legacy_turn_cursor_to_index(
    pool: &sqlx::SqlitePool,
    thread_id: RolloutId,
    cursor: &str,
) -> ThreadStoreResult<String> {
    let legacy: LegacyProjectionCursor =
        serde_json::from_str(cursor).map_err(|_| invalid_cursor(cursor))?;
    let ordinal = sqlx::query_scalar::<_, i64>(
        "SELECT rollout_ordinal FROM thread_turns WHERE thread_id = ? AND turn_id = ? LIMIT 1",
    )
    .bind(thread_id.to_string())
    .bind(legacy.turn_id)
    .fetch_optional(pool)
    .await
    .map_err(super::thread_history_error)?
    .ok_or_else(|| invalid_cursor(cursor))?;
    serialize_cursor(
        thread_id,
        thread_id,
        CursorScope::Turns,
        ordinal,
        legacy.include_anchor,
    )
}

async fn indexed_turn_cursor_to_legacy(
    pool: &sqlx::SqlitePool,
    thread_id: ThreadId,
    cursor: Option<&str>,
) -> ThreadStoreResult<Option<String>> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let indexed = parse_cursor(Some(cursor), thread_id, thread_id, CursorScope::Turns)?
        .ok_or_else(|| invalid_cursor(cursor))?;
    let ordinal = i64::try_from(indexed.rollout_ordinal).map_err(|_| invalid_cursor(cursor))?;
    let turn_id = sqlx::query_scalar::<_, String>(
        "SELECT turn_id FROM thread_turns WHERE thread_id = ? AND rollout_ordinal = ? LIMIT 1",
    )
    .bind(thread_id.to_string())
    .bind(ordinal)
    .fetch_optional(pool)
    .await
    .map_err(super::thread_history_error)?
    .ok_or_else(|| invalid_cursor(cursor))?;
    serde_json::to_string(&LegacyProjectionCursor {
        turn_id,
        include_anchor: indexed.include_anchor,
    })
    .map(Some)
    .map_err(super::thread_history_error)
}

pub(in crate::local) async fn validate_thread_for_paginated_reads(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    include_archived: bool,
    operation: &'static str,
) -> ThreadStoreResult<()> {
    let Some(state_db) = store.state_db().await else {
        return Err(ThreadStoreError::Unsupported { operation });
    };
    let Some(metadata) =
        state_db
            .get_thread(thread_id)
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to read thread metadata: {err}"),
            })?
    else {
        return Err(ThreadStoreError::Unsupported { operation });
    };
    if metadata.archived_at.is_some() && !include_archived {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!("thread {thread_id} is archived"),
        });
    }
    match metadata.history_mode {
        ThreadHistoryMode::Legacy => Err(ThreadStoreError::Unsupported { operation }),
        ThreadHistoryMode::Paginated => Ok(()),
    }
}

async fn load_inherited_summary_items(
    pool: &sqlx::SqlitePool,
    lineage: &RolloutLineage,
    turn: &StoredTurnRow,
) -> ThreadStoreResult<Vec<StoredThreadItem>> {
    let source = find_source_turn(pool, lineage, turn.turn_id.as_str()).await?;
    let segment = lineage.segment_for_position(
        source.rollout_id,
        u64::try_from(source.rollout_ordinal)
            .map_err(|_| invalid_cursor("negative rollout ordinal"))?,
    )?;
    let start_ordinal = sqlite_integer(segment.start_ordinal(), "rollout ordinal")?;
    let end_ordinal = segment
        .end_ordinal()
        .map(|ordinal| sqlite_integer(ordinal, "rollout ordinal"))
        .transpose()?;
    let rows = sqlx::query(
        r#"
SELECT turn_id, item_id, updated_at_ordinal, created_at_ms, item_json
FROM thread_items
WHERE thread_id = ?
  AND turn_id = ?
  AND rollout_ordinal >= ?
  AND (? IS NULL OR rollout_ordinal < ?)
  AND (item_id = ? OR item_id = ?)
ORDER BY rollout_ordinal ASC
        "#,
    )
    .bind(source.rollout_id.to_string())
    .bind(turn.turn_id.as_str())
    .bind(start_ordinal)
    .bind(end_ordinal)
    .bind(end_ordinal)
    .bind(source.first_user_item_id)
    .bind(source.final_agent_item_id)
    .fetch_all(pool)
    .await
    .map_err(super::thread_history_error)?;
    let items = rows
        .into_iter()
        .map(stored_thread_item)
        .collect::<ThreadStoreResult<Vec<_>>>()?;
    items
        .into_iter()
        .filter_map(|item| match segment.allows_stored_item(&item) {
            Ok(true) => Some(Ok(item)),
            Ok(false) => None,
            Err(err) => Some(Err(err)),
        })
        .collect()
}

/// Preserve legacy summaries, including commentary-only implicit turns.
async fn load_legacy_summary_items(
    pool: &sqlx::SqlitePool,
    thread_id: ThreadId,
    turn: &StoredTurnRow,
) -> ThreadStoreResult<Vec<StoredThreadItem>> {
    let thread_id = thread_id.to_string();
    let rows = sqlx::query(
        r#"
SELECT turn_id, item_id, updated_at_ordinal, created_at_ms, item_json
FROM thread_items
WHERE thread_id = ?
  AND turn_id = ?
  AND (
    item_id = ?
    OR item_id = (
        SELECT item_id
        FROM thread_items
        WHERE thread_id = ?
          AND turn_id = ?
          AND item_type = 'userMessage'
        ORDER BY rollout_ordinal ASC
        LIMIT 1
    )
    OR item_id = (
        SELECT item_id
        FROM thread_items
        WHERE thread_id = ?
          AND turn_id = ?
          AND item_type = 'agentMessage'
        ORDER BY rollout_ordinal DESC
        LIMIT 1
    )
  )
ORDER BY rollout_ordinal ASC
        "#,
    )
    .bind(thread_id.as_str())
    .bind(turn.turn_id.as_str())
    .bind(turn.first_user_item_id.as_deref())
    .bind(thread_id.as_str())
    .bind(turn.turn_id.as_str())
    .bind(thread_id.as_str())
    .bind(turn.turn_id.as_str())
    .fetch_all(pool)
    .await
    .map_err(super::thread_history_error)?;
    rows.into_iter().map(stored_thread_item).collect()
}

pub(super) fn parse_cursor(
    cursor: Option<&str>,
    requested_thread_id: ThreadId,
    root_rollout_id: RolloutId,
    scope: CursorScope,
) -> ThreadStoreResult<Option<HistoryCursor>> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let cursor_value: HistoryCursor =
        serde_json::from_str(cursor).map_err(|_| invalid_cursor(cursor))?;
    if cursor_value.requested_thread_id != requested_thread_id
        || cursor_value.root_rollout_id != root_rollout_id
        || cursor_value.scope != scope
    {
        return Err(invalid_cursor(cursor));
    }
    Ok(Some(cursor_value))
}

pub(super) fn serialize_cursor(
    requested_thread_id: ThreadId,
    root_rollout_id: RolloutId,
    scope: CursorScope,
    rollout_ordinal: i64,
    include_anchor: bool,
) -> ThreadStoreResult<String> {
    let rollout_ordinal =
        u64::try_from(rollout_ordinal).map_err(|_| invalid_cursor("negative rollout ordinal"))?;
    serde_json::to_string(&HistoryCursor {
        requested_thread_id,
        root_rollout_id,
        rollout_ordinal,
        include_anchor,
        scope,
    })
    .map_err(super::thread_history_error)
}

pub(super) fn stored_turn_row(row: sqlx::sqlite::SqliteRow) -> ThreadStoreResult<StoredTurnRow> {
    let status = match row.try_get::<String, _>("status")?.as_str() {
        "completed" => StoredTurnStatus::Completed,
        "interrupted" => StoredTurnStatus::Interrupted,
        "failed" => StoredTurnStatus::Failed,
        "inProgress" => StoredTurnStatus::InProgress,
        status => {
            return Err(ThreadStoreError::Internal {
                message: format!("unknown stored turn status: {status}"),
            });
        }
    };
    let error_json = row.try_get::<Option<String>, _>("error_json")?;
    let error = error_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(super::thread_history_error)?;
    Ok(StoredTurnRow {
        position: PhysicalHistoryPosition {
            rollout_ordinal: row.try_get("rollout_ordinal")?,
        },
        turn_id: row.try_get("turn_id")?,
        status,
        error,
        started_at: row.try_get("started_at")?,
        completed_at: row.try_get("completed_at")?,
        duration_ms: row.try_get("duration_ms")?,
        first_user_item_id: row.try_get("first_user_item_id")?,
        final_agent_item_id: row.try_get("final_agent_item_id")?,
        summary_items: Vec::new(),
    })
}

impl StoredSummaryColumns {
    pub(super) fn into_stored_items(self) -> ThreadStoreResult<Vec<StoredThreadItem>> {
        let mut summary_items = [
            StoredSummaryItemColumns {
                turn_id: self.summary_first_user_turn_id,
                item_id: self.summary_first_user_item_id,
                rollout_ordinal: self.summary_first_user_rollout_ordinal,
                updated_at_ordinal: self.summary_first_user_updated_at_ordinal,
                created_at_ms: self.summary_first_user_created_at_ms,
                item_json: self.summary_first_user_item_json,
            }
            .into_stored_item()?,
            StoredSummaryItemColumns {
                turn_id: self.summary_final_agent_turn_id,
                item_id: self.summary_final_agent_item_id,
                rollout_ordinal: self.summary_final_agent_rollout_ordinal,
                updated_at_ordinal: self.summary_final_agent_updated_at_ordinal,
                created_at_ms: self.summary_final_agent_created_at_ms,
                item_json: self.summary_final_agent_item_json,
            }
            .into_stored_item()?,
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        summary_items.sort_by_key(|(rollout_ordinal, _)| *rollout_ordinal);
        Ok(summary_items.into_iter().map(|(_, item)| item).collect())
    }
}

impl StoredSummaryItemColumns {
    fn into_stored_item(self) -> ThreadStoreResult<Option<(i64, StoredThreadItem)>> {
        let Some(item_id) = self.item_id else {
            return Ok(None);
        };
        let (
            Some(turn_id),
            Some(rollout_ordinal),
            Some(updated_at_ordinal),
            Some(created_at_ms),
            Some(item_json),
        ) = (
            self.turn_id,
            self.rollout_ordinal,
            self.updated_at_ordinal,
            self.created_at_ms,
            self.item_json,
        )
        else {
            return Err(ThreadStoreError::Internal {
                message: "stored summary item is missing joined columns".to_string(),
            });
        };
        Ok(Some((
            rollout_ordinal,
            StoredThreadItem {
                turn_id,
                item_id,
                updated_at_ordinal: stored_updated_at_ordinal(updated_at_ordinal)?,
                created_at_ms,
                item_json: item_json.into_bytes(),
            },
        )))
    }
}

pub(super) fn stored_thread_item_row(
    row: sqlx::sqlite::SqliteRow,
) -> ThreadStoreResult<StoredThreadItemRow> {
    let rollout_ordinal = row.try_get::<i64, _>("rollout_ordinal")?;
    if rollout_ordinal < 0 {
        return Err(ThreadStoreError::Internal {
            message: format!("invalid stored item rollout ordinal: {rollout_ordinal}"),
        });
    }
    Ok(StoredThreadItemRow {
        position: PhysicalHistoryPosition { rollout_ordinal },
        item: stored_thread_item(row)?,
    })
}

fn stored_thread_item(row: sqlx::sqlite::SqliteRow) -> ThreadStoreResult<StoredThreadItem> {
    let updated_at_ordinal = stored_updated_at_ordinal(row.try_get("updated_at_ordinal")?)?;
    Ok(StoredThreadItem {
        turn_id: row.try_get("turn_id")?,
        item_id: row.try_get("item_id")?,
        updated_at_ordinal,
        created_at_ms: row.try_get("created_at_ms")?,
        item_json: row.try_get::<String, _>("item_json")?.into_bytes(),
    })
}

fn stored_updated_at_ordinal(updated_at_ordinal: i64) -> ThreadStoreResult<u64> {
    u64::try_from(updated_at_ordinal).map_err(|_| ThreadStoreError::Internal {
        message: format!("invalid stored item updated-at ordinal: {updated_at_ordinal}"),
    })
}

pub(super) fn invalid_cursor(cursor: &str) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: format!("invalid cursor: {cursor}"),
    }
}
