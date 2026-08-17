use codex_protocol::ThreadId;
use sqlx::Row;

use super::super::rollout_lineage::RolloutLineage;
use super::super::rollout_lineage::RolloutLineageSegment;
use super::sqlite_integer;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

pub(in crate::local) struct TurnRow {
    pub rollout_id: ThreadId,
    pub rollout_ordinal: i64,
    pub rollout_byte_offset: Option<i64>,
    pub rollout_end_ordinal: Option<i64>,
    pub rollout_end_byte_offset: Option<i64>,
    pub status: String,
    pub first_user_item_id: Option<String>,
    pub final_agent_item_id: Option<String>,
}

pub(in crate::local) async fn find_source_turn(
    pool: &sqlx::SqlitePool,
    lineage: &RolloutLineage,
    turn_id: &str,
) -> ThreadStoreResult<TurnRow> {
    find_turn(pool, lineage.segments().iter(), turn_id).await
}

pub(in crate::local) async fn find_visible_turn(
    pool: &sqlx::SqlitePool,
    lineage: &RolloutLineage,
    turn_id: &str,
) -> ThreadStoreResult<TurnRow> {
    let root_rollout_id = lineage.root_rollout_id();
    let complete_root_projection = sqlx::query_scalar::<_, i64>(
        "SELECT next_rollout_byte_offset FROM thread_history_projection_state WHERE thread_id = ?",
    )
    .bind(root_rollout_id.to_string())
    .fetch_optional(pool)
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to inspect projected turn: {err}"),
    })?
    .is_some_and(|encoded_offset| encoded_offset >= 0);
    if complete_root_projection
        && let Some(mut row) = query_projected_turn(pool, root_rollout_id, turn_id).await?
        && let Ok(ordinal) = u64::try_from(row.rollout_end_ordinal.unwrap_or(row.rollout_ordinal))
        && let Some(segment_index) = lineage.segment_index_for_ordinal(ordinal)
    {
        row.rollout_id = lineage.segments()[segment_index].rollout_id();
        return Ok(row);
    }
    find_turn(pool, lineage.segments().iter().rev(), turn_id).await
}

/// Finds a turn in a complete root projection without resolving its physical segment lineage.
///
/// Callers must first prove that `rollout_id` names a clean same-thread projection. The newest row
/// wins so a repaired projection cannot expose a superseded duplicate turn identifier.
pub(in crate::local) async fn find_projected_turn(
    pool: &sqlx::SqlitePool,
    rollout_id: ThreadId,
    turn_id: &str,
) -> ThreadStoreResult<TurnRow> {
    query_projected_turn(pool, rollout_id, turn_id)
        .await?
        .ok_or_else(|| ThreadStoreError::InvalidRequest {
            message: format!("turn not found: {turn_id}"),
        })
}

async fn query_projected_turn(
    pool: &sqlx::SqlitePool,
    rollout_id: ThreadId,
    turn_id: &str,
) -> ThreadStoreResult<Option<TurnRow>> {
    Ok(sqlx::query(
        r#"
SELECT
    rollout_ordinal,
    rollout_byte_offset,
    rollout_end_ordinal,
    rollout_end_byte_offset,
    status,
    first_user_item_id,
    final_agent_item_id
FROM thread_turns
WHERE thread_id = ?
  AND turn_id = ?
ORDER BY rollout_ordinal DESC
LIMIT 1
        "#,
    )
    .bind(rollout_id.to_string())
    .bind(turn_id)
    .fetch_optional(pool)
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to resolve projected turn: {err}"),
    })?
    .map(|row| TurnRow {
        rollout_id,
        rollout_ordinal: row.get("rollout_ordinal"),
        rollout_byte_offset: row.get("rollout_byte_offset"),
        rollout_end_ordinal: row.get("rollout_end_ordinal"),
        rollout_end_byte_offset: row.get("rollout_end_byte_offset"),
        status: row.get("status"),
        first_user_item_id: row.get("first_user_item_id"),
        final_agent_item_id: row.get("final_agent_item_id"),
    }))
}

async fn find_turn<'a>(
    pool: &sqlx::SqlitePool,
    segments: impl Iterator<Item = &'a RolloutLineageSegment>,
    turn_id: &str,
) -> ThreadStoreResult<TurnRow> {
    for segment in segments {
        if let Some(row) = query_turn_row(pool, segment, turn_id).await? {
            return Ok(row);
        }
    }
    Err(ThreadStoreError::InvalidRequest {
        message: format!("turn not found: {turn_id}"),
    })
}

async fn query_turn_row(
    pool: &sqlx::SqlitePool,
    segment: &RolloutLineageSegment,
    turn_id: &str,
) -> ThreadStoreResult<Option<TurnRow>> {
    let end_ordinal = segment
        .end_ordinal()
        .map(|ordinal| sqlite_integer(ordinal, "rollout ordinal"))
        .transpose()?;
    sqlx::query(
        r#"
SELECT
    rollout_ordinal,
    rollout_byte_offset,
    rollout_end_ordinal,
    rollout_end_byte_offset,
    status,
    first_user_item_id,
    final_agent_item_id
FROM thread_turns
WHERE thread_id = ?
  AND turn_id = ?
  AND rollout_ordinal >= ?
  AND (? IS NULL OR rollout_ordinal < ?)
        "#,
    )
    .bind(segment.rollout_id().to_string())
    .bind(turn_id)
    .bind(sqlite_integer(segment.start_ordinal(), "rollout ordinal")?)
    .bind(end_ordinal)
    .bind(end_ordinal)
    .fetch_optional(pool)
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to resolve logical turn: {err}"),
    })
    .map(|row| {
        row.map(|row| TurnRow {
            rollout_id: segment.rollout_id(),
            rollout_ordinal: row.get("rollout_ordinal"),
            rollout_byte_offset: row.get("rollout_byte_offset"),
            rollout_end_ordinal: row.get("rollout_end_ordinal"),
            rollout_end_byte_offset: row.get("rollout_end_byte_offset"),
            status: row.get("status"),
            first_user_item_id: row.get("first_user_item_id"),
            final_agent_item_id: row.get("final_agent_item_id"),
        })
    })
}
