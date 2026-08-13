use codex_protocol::RolloutId;
use codex_protocol::ThreadId;
use sqlx::FromRow;
use sqlx::QueryBuilder;
use sqlx::Sqlite;

use super::super::rollout_lineage::RolloutLineage;
use super::super::rollout_lineage::RolloutLineageSegment;
use super::read::CursorScope;
use super::read::HistoryCursor;
use super::read::PhysicalHistoryPosition;
use super::read::StoredSummaryColumns;
use super::read::StoredThreadItemRow;
use super::read::StoredTurnRow;
use super::read::invalid_cursor;
use super::read::parse_cursor;
use super::read::serialize_cursor;
use super::read::stored_thread_item_row;
use super::read::stored_turn_row;
use super::thread_history_error;
use crate::ItemSortKey;
use crate::ListItemsParams;
use crate::SortDirection;
use crate::StoredTurnItemsView;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

pub(super) struct SegmentPage<T> {
    pub rows: Vec<T>,
    pub next_cursor: Option<String>,
    pub backwards_cursor: Option<String>,
}

pub(super) fn validate_page_size(page_size: usize) -> ThreadStoreResult<()> {
    if page_size == 0 {
        return Err(ThreadStoreError::InvalidRequest {
            message: "page size must be positive".to_string(),
        });
    }
    let limit = page_size.checked_add(1).ok_or_else(page_size_too_large)?;
    i64::try_from(limit).map_err(|_| page_size_too_large())?;
    Ok(())
}

pub(super) async fn page_turn_rows(
    pool: &sqlx::SqlitePool,
    requested_thread_id: ThreadId,
    lineage: &RolloutLineage,
    cursor: Option<&str>,
    page_size: usize,
    direction: SortDirection,
    items_view: StoredTurnItemsView,
) -> ThreadStoreResult<SegmentPage<StoredTurnRow>> {
    let root_rollout_id = lineage.root_rollout_id();
    let cursor = parse_cursor(
        cursor,
        requested_thread_id,
        root_rollout_id,
        CursorScope::Turns,
    )?;
    let mut rows = Vec::new();
    for (segment_index, segment, segment_cursor) in
        segments_from_cursor(lineage, direction, cursor.as_ref())?
    {
        let remaining = remaining_limit(page_size, rows.len())?;
        if remaining == 0 {
            break;
        }
        let mut query =
            QueryBuilder::<Sqlite>::new(if matches!(items_view, StoredTurnItemsView::Summary) {
                r#"
WITH page_turns AS (
SELECT
    turn_id,
    rollout_ordinal,
    status,
    error_json,
    started_at,
    completed_at,
    duration_ms,
    first_user_item_id,
    final_agent_item_id
FROM thread_turns
WHERE thread_id =
            "#
            } else {
                r#"
SELECT
    turn_id,
    rollout_ordinal,
    status,
    error_json,
    started_at,
    completed_at,
    duration_ms,
    first_user_item_id,
    final_agent_item_id
FROM thread_turns
WHERE thread_id =
            "#
            });
        query.push_bind(segment.rollout_id().to_string());
        push_segment_range(&mut query, segment)?;
        push_newer_turn_shadow_predicates(
            &mut query,
            segment,
            &lineage.segments()[segment_index + 1..],
        )?;
        push_cursor_clause(&mut query, direction, segment_cursor)?;
        push_order_and_limit(&mut query, direction, remaining);
        if matches!(items_view, StoredTurnItemsView::Summary) {
            query.push(
                r#"
)
SELECT
    page_turns.*,
    first_user.turn_id AS summary_first_user_turn_id,
    first_user.item_id AS summary_first_user_item_id,
    first_user.rollout_ordinal AS summary_first_user_rollout_ordinal,
    first_user.updated_at_ordinal AS summary_first_user_updated_at_ordinal,
    first_user.created_at_ms AS summary_first_user_created_at_ms,
    first_user.item_json AS summary_first_user_item_json,
    final_agent.turn_id AS summary_final_agent_turn_id,
    final_agent.item_id AS summary_final_agent_item_id,
    final_agent.rollout_ordinal AS summary_final_agent_rollout_ordinal,
    final_agent.updated_at_ordinal AS summary_final_agent_updated_at_ordinal,
    final_agent.created_at_ms AS summary_final_agent_created_at_ms,
    final_agent.item_json AS summary_final_agent_item_json
FROM page_turns
LEFT JOIN thread_items AS first_user
  ON first_user.thread_id =
                "#,
            );
            push_summary_item_join(&mut query, segment, "first_user", "first_user_item_id")?;
            query.push(
                r#"
LEFT JOIN thread_items AS final_agent
  ON final_agent.thread_id =
                "#,
            );
            push_summary_item_join(&mut query, segment, "final_agent", "final_agent_item_id")?;
            let order = match direction {
                SortDirection::Asc => "ASC",
                SortDirection::Desc => "DESC",
            };
            query
                .push(" ORDER BY page_turns.rollout_ordinal ")
                .push(order);
        }
        rows.extend(
            query
                .build()
                .fetch_all(pool)
                .await
                .map_err(thread_history_error)?
                .into_iter()
                .map(|row| {
                    let summary_items = if matches!(items_view, StoredTurnItemsView::Summary) {
                        StoredSummaryColumns::from_row(&row)?.into_stored_items()?
                    } else {
                        Vec::new()
                    };
                    let mut turn = stored_turn_row(row)?;
                    turn.summary_items = summary_items
                        .into_iter()
                        .filter_map(|item| match segment.allows_stored_item(&item) {
                            Ok(true) => Some(Ok(item)),
                            Ok(false) => None,
                            Err(err) => Some(Err(err)),
                        })
                        .collect::<ThreadStoreResult<Vec<_>>>()?;
                    Ok(turn)
                })
                .collect::<ThreadStoreResult<Vec<_>>>()?,
        );
    }
    finish_page(
        requested_thread_id,
        root_rollout_id,
        CursorScope::Turns,
        rows,
        page_size,
    )
}

/// Reads a complete legacy projection without resolving its immutable predecessors.
pub(super) async fn page_indexed_turn_rows(
    pool: &sqlx::SqlitePool,
    requested_thread_id: ThreadId,
    cursor: Option<&str>,
    page_size: usize,
    direction: SortDirection,
) -> ThreadStoreResult<SegmentPage<StoredTurnRow>> {
    let cursor = parse_cursor(
        cursor,
        requested_thread_id,
        requested_thread_id,
        CursorScope::Turns,
    )?;
    if !cursor_belongs_to_requested_projection(
        pool,
        requested_thread_id,
        CursorScope::Turns,
        cursor.as_ref(),
    )
    .await?
    {
        return Err(invalid_cursor("unknown projected turn"));
    }

    let mut query = QueryBuilder::<Sqlite>::new(
        r#"
SELECT
    turn_id,
    rollout_ordinal,
    status,
    error_json,
    started_at,
    completed_at,
    duration_ms,
    first_user_item_id,
    final_agent_item_id
FROM thread_turns
WHERE thread_id =
        "#,
    );
    query.push_bind(requested_thread_id.to_string());
    query.push(
        r#"
 AND (
    turn_id NOT LIKE 'rollout-%'
    OR rollout_end_ordinal IS NOT NULL
    OR EXISTS (
        SELECT 1
        FROM thread_items
        WHERE thread_items.thread_id = thread_turns.thread_id
          AND thread_items.turn_id = thread_turns.turn_id
    )
 )
        "#,
    );
    push_cursor_clause(&mut query, direction, cursor.as_ref())?;
    push_order_and_limit(
        &mut query,
        direction,
        remaining_limit(page_size, /*row_count*/ 0)?,
    );
    let rows = query
        .build()
        .fetch_all(pool)
        .await
        .map_err(thread_history_error)?
        .into_iter()
        .map(stored_turn_row)
        .collect::<ThreadStoreResult<Vec<_>>>()?;
    finish_page(
        requested_thread_id,
        requested_thread_id,
        CursorScope::Turns,
        rows,
        page_size,
    )
}

/// Reads items belonging to a complete legacy projection without expanding its lineage.
pub(super) async fn page_indexed_item_rows(
    pool: &sqlx::SqlitePool,
    requested_thread_id: ThreadId,
    turn_id: Option<&str>,
    cursor: Option<&str>,
    page_size: usize,
    direction: SortDirection,
) -> ThreadStoreResult<SegmentPage<StoredThreadItemRow>> {
    let scope = CursorScope::ItemsByCreatedAtOrdinal;
    let cursor = parse_cursor(
        cursor,
        requested_thread_id,
        requested_thread_id,
        scope.clone(),
    )?;
    if !cursor_belongs_to_requested_projection(pool, requested_thread_id, scope, cursor.as_ref())
        .await?
    {
        return Err(invalid_cursor("unknown projected item"));
    }

    let mut query = QueryBuilder::<Sqlite>::new(
        r#"
SELECT turn_id, item_id, rollout_ordinal, updated_at_ordinal, created_at_ms, item_json
FROM thread_items
WHERE thread_id =
        "#,
    );
    query.push_bind(requested_thread_id.to_string());
    if let Some(turn_id) = turn_id {
        query.push(" AND turn_id = ").push_bind(turn_id);
    }
    push_cursor_clause(&mut query, direction, cursor.as_ref())?;
    push_order_and_limit(
        &mut query,
        direction,
        remaining_limit(page_size, /*row_count*/ 0)?,
    );
    let rows = query
        .build()
        .fetch_all(pool)
        .await
        .map_err(thread_history_error)?
        .into_iter()
        .map(stored_thread_item_row)
        .collect::<ThreadStoreResult<Vec<_>>>()?;
    finish_page(
        requested_thread_id,
        requested_thread_id,
        CursorScope::ItemsByCreatedAtOrdinal,
        rows,
        page_size,
    )
}

async fn cursor_belongs_to_requested_projection(
    pool: &sqlx::SqlitePool,
    requested_thread_id: ThreadId,
    scope: CursorScope,
    cursor: Option<&HistoryCursor>,
) -> ThreadStoreResult<bool> {
    let Some(cursor) = cursor else {
        return Ok(true);
    };
    let query = match scope {
        CursorScope::Turns => {
            "SELECT 1 FROM thread_turns WHERE thread_id = ? AND rollout_ordinal = ? LIMIT 1"
        }
        CursorScope::ItemsByCreatedAtOrdinal => {
            "SELECT 1 FROM thread_items WHERE thread_id = ? AND rollout_ordinal = ? LIMIT 1"
        }
        CursorScope::ItemsByUpdatedAtOrdinal => {
            "SELECT 1 FROM thread_items WHERE thread_id = ? AND updated_at_ordinal = ? LIMIT 1"
        }
    };
    Ok(sqlx::query_scalar::<_, i64>(query)
        .bind(requested_thread_id.to_string())
        .bind(sqlite_integer(cursor.rollout_ordinal)?)
        .fetch_optional(pool)
        .await
        .map_err(thread_history_error)?
        .is_some())
}

fn push_newer_turn_shadow_predicates(
    query: &mut QueryBuilder<Sqlite>,
    segment: &RolloutLineageSegment,
    newer_segments: &[RolloutLineageSegment],
) -> ThreadStoreResult<()> {
    for newer_segment in newer_segments {
        // One thread cannot shadow itself because `(thread_id, turn_id)` is unique in SQLite.
        if newer_segment.rollout_id() == segment.rollout_id() {
            continue;
        }
        query
            .push(" AND NOT EXISTS (SELECT 1 FROM thread_turns AS newer_turn WHERE newer_turn.thread_id = ")
            .push_bind(newer_segment.rollout_id().to_string())
            .push(" AND newer_turn.turn_id = thread_turns.turn_id AND newer_turn.rollout_ordinal >= ")
            .push_bind(sqlite_integer(newer_segment.start_ordinal())?);
        if let Some(end_ordinal) = newer_segment.end_ordinal() {
            query
                .push(" AND newer_turn.rollout_ordinal < ")
                .push_bind(sqlite_integer(end_ordinal)?);
        }
        query.push(")");
    }
    Ok(())
}

fn push_summary_item_join(
    query: &mut QueryBuilder<Sqlite>,
    segment: &RolloutLineageSegment,
    alias: &str,
    item_id_column: &str,
) -> ThreadStoreResult<()> {
    query
        .push_bind(segment.rollout_id().to_string())
        .push(" AND ")
        .push(alias)
        .push(".turn_id = page_turns.turn_id AND ")
        .push(alias)
        .push(".item_id = page_turns.")
        .push(item_id_column)
        .push(" AND ")
        .push(alias)
        .push(".rollout_ordinal >= ")
        .push_bind(sqlite_integer(segment.start_ordinal())?);
    if let Some(end_ordinal) = segment.end_ordinal() {
        query
            .push(" AND ")
            .push(alias)
            .push(".rollout_ordinal < ")
            .push_bind(sqlite_integer(end_ordinal)?);
    }
    Ok(())
}

pub(super) async fn page_item_rows(
    pool: &sqlx::SqlitePool,
    lineage: &RolloutLineage,
    params: &ListItemsParams,
) -> ThreadStoreResult<SegmentPage<StoredThreadItemRow>> {
    // Update ordinals are local to a physical rollout. Forked lineages need a structured
    // watermark before incremental replay can safely span their segments.
    if params.after_updated_at_ordinal.is_some() && lineage.segments().len() > 1 {
        return Err(ThreadStoreError::InvalidRequest {
            message: "incremental item replay is not supported for forked threads".to_string(),
        });
    }
    if matches!(params.sort_key, ItemSortKey::UpdatedAtOrdinal) {
        let Some(after_updated_at_ordinal) = params.after_updated_at_ordinal else {
            return Err(ThreadStoreError::InvalidRequest {
                message: "update-ordinal item sorting requires an update watermark".to_string(),
            });
        };
        let [segment] = lineage.segments() else {
            return Err(ThreadStoreError::Internal {
                message: "update-ordinal item paging requires one rollout segment".to_string(),
            });
        };
        return page_updated_item_rows(
            pool,
            segment.rollout_id(),
            lineage.root_rollout_id(),
            params,
            after_updated_at_ordinal,
        )
        .await;
    }
    let cursor = parse_cursor(
        params.cursor.as_deref(),
        params.thread_id,
        lineage.root_rollout_id(),
        CursorScope::ItemsByCreatedAtOrdinal,
    )?;
    let mut rows = Vec::new();
    for (_, segment, segment_cursor) in
        segments_from_cursor(lineage, params.sort_direction, cursor.as_ref())?
    {
        let remaining = remaining_limit(params.page_size, rows.len())?;
        if remaining == 0 {
            break;
        }
        let mut query = QueryBuilder::<Sqlite>::new(
            r#"
SELECT turn_id, item_id, rollout_ordinal, updated_at_ordinal, created_at_ms, item_json
FROM thread_items
WHERE thread_id =
            "#,
        );
        query.push_bind(segment.rollout_id().to_string());
        push_segment_range(&mut query, segment)?;
        if let Some(after_updated_at_ordinal) = params.after_updated_at_ordinal {
            query
                .push(" AND updated_at_ordinal > ")
                .push_bind(sqlite_integer(after_updated_at_ordinal)?);
        }
        if let Some(turn_id) = params.turn_id.as_deref() {
            query.push(" AND turn_id = ").push_bind(turn_id);
        }
        push_cursor_clause(&mut query, params.sort_direction, segment_cursor)?;
        push_order_and_limit(
            &mut query,
            params.sort_direction,
            if segment.filters_items() {
                i64::MAX
            } else {
                remaining
            },
        );
        let segment_rows = query
            .build()
            .fetch_all(pool)
            .await
            .map_err(thread_history_error)?
            .into_iter()
            .map(stored_thread_item_row)
            .collect::<ThreadStoreResult<Vec<_>>>()?;
        for row in segment_rows {
            if segment.allows_stored_item(&row.item)? {
                rows.push(row);
                if remaining_limit(params.page_size, rows.len())? == 0 {
                    break;
                }
            }
        }
    }
    finish_page(
        params.thread_id,
        lineage.root_rollout_id(),
        CursorScope::ItemsByCreatedAtOrdinal,
        rows,
        params.page_size,
    )
}

async fn page_updated_item_rows(
    pool: &sqlx::SqlitePool,
    rollout_id: ThreadId,
    root_rollout_id: RolloutId,
    params: &ListItemsParams,
    after_updated_at_ordinal: u64,
) -> ThreadStoreResult<SegmentPage<StoredThreadItemRow>> {
    let cursor = parse_cursor(
        params.cursor.as_deref(),
        params.thread_id,
        root_rollout_id,
        CursorScope::ItemsByUpdatedAtOrdinal,
    )?;
    let mut query = QueryBuilder::<Sqlite>::new(
        r#"
SELECT turn_id, item_id, updated_at_ordinal AS rollout_ordinal, updated_at_ordinal, created_at_ms, item_json
FROM thread_items
WHERE thread_id =
        "#,
    );
    query
        .push_bind(rollout_id.to_string())
        .push(" AND updated_at_ordinal > ")
        .push_bind(sqlite_integer(after_updated_at_ordinal)?);
    if let Some(turn_id) = params.turn_id.as_deref() {
        query.push(" AND turn_id = ").push_bind(turn_id);
    }
    if let Some(cursor) = cursor {
        let comparator = match (params.sort_direction, cursor.include_anchor) {
            (SortDirection::Asc, true) => ">=",
            (SortDirection::Asc, false) => ">",
            (SortDirection::Desc, true) => "<=",
            (SortDirection::Desc, false) => "<",
        };
        query
            .push(" AND updated_at_ordinal ")
            .push(comparator)
            .push(" ")
            .push_bind(sqlite_integer(cursor.rollout_ordinal)?);
    }
    let order = match params.sort_direction {
        SortDirection::Asc => "ASC",
        SortDirection::Desc => "DESC",
    };
    query
        .push(" ORDER BY updated_at_ordinal ")
        .push(order)
        .push(" LIMIT ")
        .push_bind(remaining_limit(params.page_size, /*row_count*/ 0)?);
    let rows = query
        .build()
        .fetch_all(pool)
        .await
        .map_err(thread_history_error)?
        .into_iter()
        .map(stored_thread_item_row)
        .collect::<ThreadStoreResult<Vec<_>>>()?;
    finish_page(
        params.thread_id,
        root_rollout_id,
        CursorScope::ItemsByUpdatedAtOrdinal,
        rows,
        params.page_size,
    )
}

fn segments_from_cursor<'a>(
    lineage: &'a RolloutLineage,
    direction: SortDirection,
    cursor: Option<&'a HistoryCursor>,
) -> ThreadStoreResult<Vec<(usize, &'a RolloutLineageSegment, Option<&'a HistoryCursor>)>> {
    let segments = lineage.segments();
    let cursor_index = cursor
        .map(|cursor| {
            let mut matching_indexes =
                segments.iter().enumerate().filter_map(|(index, segment)| {
                    segment
                        .contains_ordinal(cursor.rollout_ordinal)
                        .then_some(index)
                });
            let Some(index) = matching_indexes.next() else {
                return Err(invalid_cursor("position outside thread lineage"));
            };
            if matching_indexes.next().is_some() {
                return Err(invalid_cursor("ambiguous physical segment"));
            }
            Ok(index)
        })
        .transpose()?;
    let indexes: Vec<usize> = match direction {
        SortDirection::Asc => (cursor_index.unwrap_or(0)..segments.len()).collect(),
        SortDirection::Desc => {
            let end = cursor_index.unwrap_or_else(|| segments.len().saturating_sub(1));
            (0..=end).rev().collect()
        }
    };
    Ok(indexes
        .into_iter()
        .map(|index| {
            let segment_cursor = if Some(index) == cursor_index {
                cursor
            } else {
                None
            };
            (index, &segments[index], segment_cursor)
        })
        .collect())
}

fn push_segment_range(
    query: &mut QueryBuilder<Sqlite>,
    segment: &RolloutLineageSegment,
) -> ThreadStoreResult<()> {
    query
        .push(" AND rollout_ordinal >= ")
        .push_bind(sqlite_integer(segment.start_ordinal())?);
    if let Some(end_ordinal) = segment.end_ordinal() {
        query
            .push(" AND rollout_ordinal < ")
            .push_bind(sqlite_integer(end_ordinal)?);
    }
    Ok(())
}

fn push_cursor_clause(
    query: &mut QueryBuilder<Sqlite>,
    direction: SortDirection,
    cursor: Option<&HistoryCursor>,
) -> ThreadStoreResult<()> {
    if let Some(cursor) = cursor {
        let comparator = match (direction, cursor.include_anchor) {
            (SortDirection::Asc, true) => ">=",
            (SortDirection::Asc, false) => ">",
            (SortDirection::Desc, true) => "<=",
            (SortDirection::Desc, false) => "<",
        };
        query
            .push(" AND rollout_ordinal ")
            .push(comparator)
            .push(" ")
            .push_bind(sqlite_integer(cursor.rollout_ordinal)?);
    }
    Ok(())
}

fn push_order_and_limit(query: &mut QueryBuilder<Sqlite>, direction: SortDirection, limit: i64) {
    let order = match direction {
        SortDirection::Asc => "ASC",
        SortDirection::Desc => "DESC",
    };
    query
        .push(" ORDER BY rollout_ordinal ")
        .push(order)
        .push(" LIMIT ")
        .push_bind(limit);
}

fn remaining_limit(page_size: usize, row_count: usize) -> ThreadStoreResult<i64> {
    let limit = page_size
        .checked_add(1)
        .and_then(|limit| limit.checked_sub(row_count))
        .ok_or_else(page_size_too_large)?;
    i64::try_from(limit).map_err(|_| page_size_too_large())
}

fn finish_page<T: HasPosition>(
    requested_thread_id: ThreadId,
    root_rollout_id: RolloutId,
    scope: CursorScope,
    mut rows: Vec<T>,
    page_size: usize,
) -> ThreadStoreResult<SegmentPage<T>> {
    let has_more = rows.len() > page_size;
    rows.truncate(page_size);
    let backwards_cursor = rows
        .first()
        .map(|row| {
            serialize_cursor(
                requested_thread_id,
                root_rollout_id,
                scope.clone(),
                row.position().rollout_ordinal,
                /*include_anchor*/ true,
            )
        })
        .transpose()?;
    let next_cursor = if has_more {
        rows.last()
            .map(|row| {
                serialize_cursor(
                    requested_thread_id,
                    root_rollout_id,
                    scope,
                    row.position().rollout_ordinal,
                    /*include_anchor*/ false,
                )
            })
            .transpose()?
    } else {
        None
    };
    Ok(SegmentPage {
        rows,
        next_cursor,
        backwards_cursor,
    })
}

trait HasPosition {
    fn position(&self) -> PhysicalHistoryPosition;
}

impl HasPosition for StoredTurnRow {
    fn position(&self) -> PhysicalHistoryPosition {
        self.position
    }
}

impl HasPosition for StoredThreadItemRow {
    fn position(&self) -> PhysicalHistoryPosition {
        self.position
    }
}

fn sqlite_integer(value: u64) -> ThreadStoreResult<i64> {
    i64::try_from(value).map_err(|_| ThreadStoreError::InvalidRequest {
        message: "rollout ordinal exceeds SQLite integer range".to_string(),
    })
}

fn page_size_too_large() -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: "page size is too large".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn same_thread_segments_do_not_add_turn_shadow_queries() {
        let thread_id = ThreadId::new();
        let oldest = segment(thread_id, "oldest", /*start_ordinal*/ 1, Some(10));
        let newer = [
            segment(thread_id, "middle", /*start_ordinal*/ 10, Some(20)),
            segment(thread_id, "newest", /*start_ordinal*/ 20, None),
        ];
        let mut query = QueryBuilder::<Sqlite>::new("SELECT * FROM thread_turns WHERE 1 = 1");

        push_newer_turn_shadow_predicates(&mut query, &oldest, &newer)
            .expect("same-thread shadow predicates");

        assert_eq!(
            query.sql().as_str(),
            "SELECT * FROM thread_turns WHERE 1 = 1"
        );
    }

    #[test]
    fn forked_thread_segments_retain_turn_shadow_queries() {
        let parent_thread_id = ThreadId::new();
        let child_thread_id = ThreadId::new();
        let parent = segment(
            parent_thread_id,
            "parent",
            /*start_ordinal*/ 1,
            Some(10),
        );
        let newer = [
            segment(
                parent_thread_id,
                "parent-rotation",
                /*start_ordinal*/ 10,
                Some(20),
            ),
            segment(child_thread_id, "child", /*start_ordinal*/ 20, None),
        ];
        let mut query = QueryBuilder::<Sqlite>::new("SELECT * FROM thread_turns WHERE 1 = 1");

        push_newer_turn_shadow_predicates(&mut query, &parent, &newer)
            .expect("forked-thread shadow predicates");

        assert_eq!(query.sql().as_str().matches("NOT EXISTS").count(), 1);
        assert!(
            query
                .sql()
                .as_str()
                .contains("newer_turn.turn_id = thread_turns.turn_id")
        );
    }

    #[test]
    fn cursor_selects_same_thread_segment_by_ordinal_range() {
        let thread_id = ThreadId::default();
        let lineage = RolloutLineage {
            root_rollout_id: thread_id,
            segments: vec![
                segment(thread_id, "first", /*start_ordinal*/ 1, Some(10)),
                segment(thread_id, "second", /*start_ordinal*/ 10, Some(20)),
            ],
        };
        let cursor = HistoryCursor {
            requested_thread_id: thread_id,
            root_rollout_id: thread_id,
            rollout_ordinal: 12,
            include_anchor: false,
            scope: CursorScope::ItemsByCreatedAtOrdinal,
        };

        let segments =
            segments_from_cursor(&lineage, SortDirection::Asc, Some(&cursor)).expect("segments");

        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].1.rollout_path, PathBuf::from("second"));
        assert!(segments[0].2.is_some());
    }

    #[test]
    fn cursor_rejects_overlapping_same_thread_segments() {
        let thread_id = ThreadId::default();
        let lineage = RolloutLineage {
            root_rollout_id: thread_id,
            segments: vec![
                segment(thread_id, "first", /*start_ordinal*/ 1, Some(20)),
                segment(thread_id, "second", /*start_ordinal*/ 10, Some(30)),
            ],
        };
        let cursor = HistoryCursor {
            requested_thread_id: thread_id,
            root_rollout_id: thread_id,
            rollout_ordinal: 12,
            include_anchor: false,
            scope: CursorScope::ItemsByCreatedAtOrdinal,
        };

        let error = match segments_from_cursor(&lineage, SortDirection::Asc, Some(&cursor)) {
            Ok(_) => panic!("overlapping segments must be ambiguous"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("ambiguous physical segment"));
    }

    fn segment(
        thread_id: ThreadId,
        path: &str,
        start_ordinal: u64,
        end_ordinal_exclusive: Option<u64>,
    ) -> RolloutLineageSegment {
        RolloutLineageSegment {
            thread_id,
            rollout_id: thread_id,
            rollout_path: PathBuf::from(path),
            start_ordinal,
            end_ordinal_exclusive,
            jsonl_end_byte_offset: None,
            end_byte_offset: None,
            filter_texts: Vec::new(),
        }
    }
}
