use codex_app_server_protocol::ThreadHistoryChangeSet;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::TurnStatus;
use codex_protocol::ThreadId;
use codex_protocol::models::MessagePhase;
use codex_protocol::realtime::RealtimeItem;

use super::LocalThreadStore;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

mod bulk_projection;
mod read;
mod realtime;
pub(super) use bulk_projection::BulkProjection;
mod search;
mod segment_paging;
mod turn_lookup;

pub(super) use read::has_complete_root_projection_for_resolved;
pub(super) use read::has_complete_segmented_legacy_projection;
pub(super) use read::list_existing_segmented_legacy_turns;
pub(super) use read::list_items;
pub(super) use read::list_segmented_legacy_items;
pub(super) use read::list_segmented_legacy_turns;
pub(super) use read::list_turns;
pub(super) use read::validate_thread_for_paginated_reads;
pub(super) use realtime::list_timeline;
pub(super) use search::search_thread_occurrences;
pub(super) use turn_lookup::find_projected_turn;
pub(super) use turn_lookup::find_source_turn;
pub(super) use turn_lookup::find_visible_turn;

/// A valid complete rollout line with its absolute byte span in durable JSONL.
///
/// `start_byte_offset..end_byte_offset` includes the terminating newline.
#[derive(Clone)]
pub(super) struct ProjectedRolloutLine {
    pub ordinal: u64,
    pub start_byte_offset: u64,
    pub end_byte_offset: u64,
    pub fallback_created_at_ms: Option<i64>,
    pub changes: ThreadHistoryChangeSet,
    pub realtime_item: Option<RealtimeItem>,
}

/// One ordered update to apply while advancing a rollout projection checkpoint.
///
/// Skipped ordinal ranges keep the byte and ordinal checkpoints describing the same durable
/// prefix even when a complete rollout line cannot be projected.
#[derive(Clone)]
pub(super) enum RolloutProjectionStep {
    Line(Box<ProjectedRolloutLine>),
    SkippedOrdinalRange {
        start_ordinal: u64,
        end_ordinal_exclusive: u64,
    },
}

pub(super) struct RolloutProjectionState {
    pub next_byte_offset: u64,
    pub next_ordinal: u64,
    /// Whether this projection includes every selected same-thread predecessor.
    pub lineage_complete: bool,
}

/// An existing SQLite offset reserved until every legacy predecessor has been indexed.
///
/// A crash during a predecessor transaction must not make an incomplete projection appear current
/// merely because that predecessor has the same byte length as the active rollout.
pub(super) const INCOMPLETE_LEGACY_PROJECTION_BYTE_OFFSET: i64 = i64::MAX;

/// Installs guards that invalidate a published projection when rows change outside its writer.
///
/// Projection writers update the rows and checkpoint in one transaction, so their final
/// checkpoint restores the intended completeness marker. An out-of-band row mutation has no
/// matching checkpoint update and leaves the reversible negative marker for canonical fallback
/// and rebuild. The triggers live in the rebuildable history database rather than a numbered
/// state migration.
pub(super) async fn ensure_projection_integrity_triggers(
    pool: &sqlx::SqlitePool,
) -> ThreadStoreResult<()> {
    for statement in [
        r#"
CREATE TRIGGER IF NOT EXISTS frodex_thread_turns_projection_insert
AFTER INSERT ON thread_turns
BEGIN
    UPDATE thread_history_projection_state
    SET next_rollout_byte_offset = CASE
        WHEN next_rollout_byte_offset >= 0
             AND next_rollout_byte_offset < 9223372036854775807
            THEN -1 - next_rollout_byte_offset
        ELSE next_rollout_byte_offset
    END
    WHERE thread_id = NEW.thread_id;
END
        "#,
        r#"
CREATE TRIGGER IF NOT EXISTS frodex_thread_turns_projection_update
AFTER UPDATE ON thread_turns
WHEN OLD.thread_id = NEW.thread_id
BEGIN
    UPDATE thread_history_projection_state
    SET next_rollout_byte_offset = CASE
        WHEN next_rollout_byte_offset >= 0
             AND next_rollout_byte_offset < 9223372036854775807
            THEN -1 - next_rollout_byte_offset
        ELSE next_rollout_byte_offset
    END
    WHERE thread_id = NEW.thread_id;
END
        "#,
        r#"
CREATE TRIGGER IF NOT EXISTS frodex_thread_turns_projection_delete
AFTER DELETE ON thread_turns
BEGIN
    UPDATE thread_history_projection_state
    SET next_rollout_byte_offset = CASE
        WHEN next_rollout_byte_offset >= 0
             AND next_rollout_byte_offset < 9223372036854775807
            THEN -1 - next_rollout_byte_offset
        ELSE next_rollout_byte_offset
    END
    WHERE thread_id = OLD.thread_id;
END
        "#,
        r#"
CREATE TRIGGER IF NOT EXISTS frodex_thread_items_projection_insert
AFTER INSERT ON thread_items
BEGIN
    UPDATE thread_history_projection_state
    SET next_rollout_byte_offset = CASE
        WHEN next_rollout_byte_offset >= 0
             AND next_rollout_byte_offset < 9223372036854775807
            THEN -1 - next_rollout_byte_offset
        ELSE next_rollout_byte_offset
    END
    WHERE thread_id = NEW.thread_id;
END
        "#,
        r#"
CREATE TRIGGER IF NOT EXISTS frodex_thread_items_projection_update
AFTER UPDATE ON thread_items
WHEN OLD.thread_id = NEW.thread_id
BEGIN
    UPDATE thread_history_projection_state
    SET next_rollout_byte_offset = CASE
        WHEN next_rollout_byte_offset >= 0
             AND next_rollout_byte_offset < 9223372036854775807
            THEN -1 - next_rollout_byte_offset
        ELSE next_rollout_byte_offset
    END
    WHERE thread_id = NEW.thread_id;
END
        "#,
        r#"
CREATE TRIGGER IF NOT EXISTS frodex_thread_items_projection_delete
AFTER DELETE ON thread_items
BEGIN
    UPDATE thread_history_projection_state
    SET next_rollout_byte_offset = CASE
        WHEN next_rollout_byte_offset >= 0
             AND next_rollout_byte_offset < 9223372036854775807
            THEN -1 - next_rollout_byte_offset
        ELSE next_rollout_byte_offset
    END
    WHERE thread_id = OLD.thread_id;
END
        "#,
    ] {
        sqlx::query(statement)
            .execute(pool)
            .await
            .map_err(thread_history_error)?;
    }
    Ok(())
}

pub(super) async fn projection_state(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<Option<RolloutProjectionState>> {
    if store.state_db.is_none() {
        return Ok(None);
    }
    let db_path = store.config.sqlite.thread_history_db_path();
    if !tokio::fs::try_exists(db_path.as_path())
        .await
        .map_err(thread_history_error)?
    {
        return Ok(None);
    }

    let pool = store.thread_history_db().await?;
    let state = sqlx::query_as::<_, (i64, i64)>(
        r#"
SELECT next_rollout_byte_offset, next_rollout_ordinal
FROM thread_history_projection_state
WHERE thread_id = ?
        "#,
    )
    .bind(thread_id.to_string())
    .fetch_optional(pool)
    .await
    .map_err(thread_history_error)?;
    state
        .map(|(next_byte_offset, next_ordinal)| {
            let (next_byte_offset, lineage_complete) =
                decode_paginated_projection_offset(next_byte_offset)?;
            Ok(RolloutProjectionState {
                next_byte_offset,
                next_ordinal: u64::try_from(next_ordinal).map_err(|_| {
                    ThreadStoreError::Internal {
                        message: format!(
                            "thread history projection for {thread_id} has a negative ordinal"
                        ),
                    }
                })?,
                lineage_complete,
            })
        })
        .transpose()
}

/// Marks a new active-only projection incomplete before any of its rows become visible.
pub(super) async fn begin_incomplete_paginated_projection(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    initial_ordinal: u64,
) -> ThreadStoreResult<()> {
    let pool = store.thread_history_db().await?;
    sqlx::query(
        r#"
INSERT INTO thread_history_projection_state (
    thread_id,
    next_rollout_byte_offset,
    next_rollout_ordinal
) VALUES (?, -1, ?)
ON CONFLICT(thread_id) DO NOTHING
        "#,
    )
    .bind(thread_id.to_string())
    .bind(sqlite_integer(initial_ordinal, "rollout ordinal")?)
    .execute(pool)
    .await
    .map_err(thread_history_error)?;
    Ok(())
}

pub(super) async fn reset_projection_for_replacement(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    next_rollout_ordinal: u64,
) -> ThreadStoreResult<()> {
    let pool = store.thread_history_db().await?;
    let thread_id = thread_id.to_string();
    let next_rollout_ordinal = sqlite_integer(next_rollout_ordinal, "rollout ordinal")?;
    let existing_state = sqlx::query_as::<_, (i64, i64)>(
        "SELECT next_rollout_byte_offset, next_rollout_ordinal FROM thread_history_projection_state WHERE thread_id = ?",
    )
    .bind(thread_id.as_str())
    .fetch_optional(pool)
    .await
    .map_err(thread_history_error)?;
    if existing_state
        .as_ref()
        .is_some_and(|(_, ordinal)| *ordinal != next_rollout_ordinal)
    {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "thread history projection for {thread_id} does not end at ordinal {next_rollout_ordinal}"
            ),
        });
    }
    let lineage_complete = existing_state
        .map(|(encoded_offset, _)| decode_paginated_projection_offset(encoded_offset))
        .transpose()?
        .is_none_or(|(_, lineage_complete)| lineage_complete);
    sqlx::query(
        r#"
INSERT INTO thread_history_projection_state (
    thread_id,
    next_rollout_byte_offset,
    next_rollout_ordinal
) VALUES (?, ?, ?)
ON CONFLICT(thread_id) DO UPDATE SET
    next_rollout_byte_offset = excluded.next_rollout_byte_offset,
    next_rollout_ordinal = excluded.next_rollout_ordinal
        "#,
    )
    .bind(thread_id)
    .bind(encode_paginated_projection_offset(
        /*offset*/ 0,
        lineage_complete,
    )?)
    .bind(next_rollout_ordinal)
    .execute(pool)
    .await
    .map_err(thread_history_error)?;
    Ok(())
}

pub(super) async fn clear_projection_cursor(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    let pool = store.thread_history_db().await?;
    sqlx::query("DELETE FROM thread_history_projection_state WHERE thread_id = ?")
        .bind(thread_id.to_string())
        .execute(pool)
        .await
        .map_err(thread_history_error)?;
    Ok(())
}

/// Atomically replaces a visible projection with rows prepared under a staging identity.
pub(super) async fn publish_staged_projection(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    staging_thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    if thread_id == staging_thread_id {
        return Err(ThreadStoreError::Internal {
            message: "staged history projection reuses the selected rollout identity".to_string(),
        });
    }
    let pool = store.thread_history_db().await?;
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(thread_history_error)?;
    #[cfg(test)]
    let selected_thread_id = thread_id;
    let thread_id = thread_id.to_string();
    let staging_thread_id = staging_thread_id.to_string();
    let staged_state_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM thread_history_projection_state WHERE thread_id = ?",
    )
    .bind(staging_thread_id.as_str())
    .fetch_one(&mut *transaction)
    .await
    .map_err(thread_history_error)?;
    if staged_state_count != 1 {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "staged history projection for {thread_id} has {staged_state_count} checkpoints"
            ),
        });
    }
    for statement in [
        "DELETE FROM thread_items WHERE thread_id = ?",
        "DELETE FROM thread_turns WHERE thread_id = ?",
        "DELETE FROM thread_history_projection_state WHERE thread_id = ?",
    ] {
        sqlx::query(statement)
            .bind(thread_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(thread_history_error)?;
    }
    for statement in [
        "UPDATE thread_turns SET thread_id = ? WHERE thread_id = ?",
        "UPDATE thread_items SET thread_id = ? WHERE thread_id = ?",
        "UPDATE thread_history_projection_state SET thread_id = ? WHERE thread_id = ?",
    ] {
        sqlx::query(statement)
            .bind(thread_id.as_str())
            .bind(staging_thread_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(thread_history_error)?;
    }
    #[cfg(test)]
    super::projection_rebuild::crash_at_boundary(selected_thread_id, "before_projection_commit");
    transaction.commit().await.map_err(thread_history_error)?;
    #[cfg(test)]
    super::projection_rebuild::crash_at_boundary(selected_thread_id, "after_projection_commit");
    Ok(())
}

pub(super) async fn begin_legacy_projection_backfill(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    let pool = store.thread_history_db().await?;
    sqlx::query(
        r#"
INSERT INTO thread_history_projection_state (
    thread_id,
    next_rollout_byte_offset,
    next_rollout_ordinal
) VALUES (?, ?, 0)
ON CONFLICT(thread_id) DO UPDATE SET
    next_rollout_byte_offset = excluded.next_rollout_byte_offset,
    next_rollout_ordinal = 0
        "#,
    )
    .bind(thread_id.to_string())
    .bind(INCOMPLETE_LEGACY_PROJECTION_BYTE_OFFSET)
    .execute(pool)
    .await
    .map_err(thread_history_error)?;
    Ok(())
}

pub(super) async fn apply_projection(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    start_offset: u64,
    next_offset: u64,
    initial_ordinal: u64,
    projections: Vec<RolloutProjectionStep>,
) -> ThreadStoreResult<()> {
    apply_projection_inner(
        store,
        thread_id,
        start_offset,
        next_offset,
        initial_ordinal,
        projections,
        /*legacy_backfill_complete*/ None,
    )
    .await
}

pub(super) async fn apply_legacy_projection(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    start_offset: u64,
    next_offset: u64,
    initial_ordinal: u64,
    projections: Vec<RolloutProjectionStep>,
    complete: bool,
) -> ThreadStoreResult<()> {
    apply_projection_inner(
        store,
        thread_id,
        start_offset,
        next_offset,
        initial_ordinal,
        projections,
        Some(complete),
    )
    .await
}

async fn apply_projection_inner(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    start_offset: u64,
    next_offset: u64,
    initial_ordinal: u64,
    projections: Vec<RolloutProjectionStep>,
    legacy_backfill_complete: Option<bool>,
) -> ThreadStoreResult<()> {
    let pool = store.thread_history_db().await?;
    // Write the projected rows and advance the JSONL offset and ordinal in one transaction. If
    // SQLite fails, it stays behind the durable rollout instead of claiming data it did not
    // materialize.
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(thread_history_error)?;
    let thread_id = thread_id.to_string();
    let projection_state = sqlx::query_as::<_, (i64, i64)>(
        r#"
SELECT next_rollout_byte_offset, next_rollout_ordinal
FROM thread_history_projection_state
WHERE thread_id = ?
        "#,
    )
    .bind(thread_id.as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(thread_history_error)?;
    let (expected_offset, mut next_ordinal, lineage_complete) = match projection_state {
        Some((encoded_offset, next_ordinal)) => {
            let (expected_offset, lineage_complete) =
                decode_paginated_projection_offset(encoded_offset)?;
            (
                sqlite_integer(expected_offset, "rollout byte offset")?,
                next_ordinal,
                lineage_complete,
            )
        }
        None => (0, sqlite_integer(initial_ordinal, "rollout ordinal")?, true),
    };
    let start_offset = sqlite_integer(start_offset, "rollout byte offset")?;
    if expected_offset != start_offset
        && !(legacy_backfill_complete.is_some()
            && expected_offset == INCOMPLETE_LEGACY_PROJECTION_BYTE_OFFSET)
    {
        return Err(ThreadStoreError::Internal {
            message: format!("thread history projection for {thread_id} is behind durable rollout"),
        });
    }

    for projection in projections {
        match projection {
            RolloutProjectionStep::Line(projection) => {
                let ordinal = sqlite_integer(projection.ordinal, "rollout ordinal")?;
                if ordinal != next_ordinal {
                    return Err(ThreadStoreError::Internal {
                        message: format!(
                            "thread history projection for {thread_id} expected ordinal {next_ordinal}, got {ordinal}"
                        ),
                    });
                }
                apply_change_set(
                    &mut transaction,
                    thread_id.as_str(),
                    ordinal,
                    sqlite_integer(projection.start_byte_offset, "rollout byte offset")?,
                    sqlite_integer(projection.end_byte_offset, "rollout byte offset")?,
                    projection.fallback_created_at_ms,
                    projection.changes,
                )
                .await?;
                if let Some(item) = projection.realtime_item {
                    let item_json = serde_json::to_string(&item).map_err(thread_history_error)?;
                    sqlx::query(
                        r#"
INSERT INTO thread_realtime_items (
    thread_id,
    item_id,
    rollout_ordinal,
    created_at_ms,
    item_type,
    item_json
) VALUES (?, ?, ?, ?, json_extract(?, '$.type'), ?)
ON CONFLICT(thread_id, item_id) DO NOTHING
                        "#,
                    )
                    .bind(thread_id.as_str())
                    .bind(item.id.as_str())
                    .bind(ordinal)
                    .bind(projection.fallback_created_at_ms.ok_or_else(|| {
                        ThreadStoreError::Internal {
                            message: "realtime rollout item is missing its timestamp".to_string(),
                        }
                    })?)
                    .bind(item_json.as_str())
                    .bind(item_json.as_str())
                    .execute(&mut *transaction)
                    .await
                    .map_err(thread_history_error)?;
                }
                next_ordinal =
                    next_ordinal
                        .checked_add(1)
                        .ok_or_else(|| ThreadStoreError::Internal {
                            message: "rollout ordinal exceeds SQLite integer range".to_string(),
                        })?;
            }
            RolloutProjectionStep::SkippedOrdinalRange {
                start_ordinal,
                end_ordinal_exclusive,
            } => {
                let start_ordinal = sqlite_integer(start_ordinal, "rollout ordinal")?;
                if start_ordinal != next_ordinal {
                    return Err(ThreadStoreError::Internal {
                        message: format!(
                            "thread history projection for {thread_id} expected ordinal {next_ordinal}, got {start_ordinal}"
                        ),
                    });
                }
                let end_ordinal_exclusive =
                    sqlite_integer(end_ordinal_exclusive, "rollout ordinal")?;
                if end_ordinal_exclusive <= start_ordinal {
                    return Err(ThreadStoreError::Internal {
                        message: format!(
                            "thread history projection for {thread_id} has an empty skipped ordinal range"
                        ),
                    });
                }
                next_ordinal = end_ordinal_exclusive;
            }
        }
    }

    sqlx::query(
        r#"
INSERT INTO thread_history_projection_state (
    thread_id,
    next_rollout_byte_offset,
    next_rollout_ordinal
) VALUES (?, ?, ?)
ON CONFLICT(thread_id) DO UPDATE SET
    next_rollout_byte_offset = excluded.next_rollout_byte_offset,
    next_rollout_ordinal = excluded.next_rollout_ordinal
        "#,
    )
    .bind(thread_id.as_str())
    .bind(match legacy_backfill_complete {
        Some(false) => INCOMPLETE_LEGACY_PROJECTION_BYTE_OFFSET,
        _ => encode_paginated_projection_offset(next_offset, lineage_complete)?,
    })
    .bind(next_ordinal)
    .execute(&mut *transaction)
    .await
    .map_err(thread_history_error)?;
    transaction.commit().await.map_err(thread_history_error)
}

pub(super) async fn delete_thread(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    delete_threads(store, &[thread_id]).await
}

/// Deletes every projection for the supplied physical rollout IDs in one transaction.
pub(super) async fn delete_threads(
    store: &LocalThreadStore,
    thread_ids: &[ThreadId],
) -> ThreadStoreResult<()> {
    if thread_ids.is_empty() {
        return Ok(());
    }
    let db_path = store.config.sqlite.thread_history_db_path();
    if !tokio::fs::try_exists(db_path.as_path())
        .await
        .map_err(thread_history_delete_error)?
    {
        return Ok(());
    }

    let pool = store.thread_history_db().await?;
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(thread_history_delete_error)?;
    for thread_id in thread_ids {
        let thread_id = thread_id.to_string();
        sqlx::query("DELETE FROM thread_items WHERE thread_id = ?")
            .bind(thread_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(thread_history_delete_error)?;
        sqlx::query("DELETE FROM thread_realtime_items WHERE thread_id = ?")
            .bind(thread_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(thread_history_delete_error)?;
        sqlx::query("DELETE FROM thread_turns WHERE thread_id = ?")
            .bind(thread_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(thread_history_delete_error)?;
        sqlx::query("DELETE FROM thread_history_projection_state WHERE thread_id = ?")
            .bind(thread_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(thread_history_delete_error)?;
    }
    transaction
        .commit()
        .await
        .map_err(thread_history_delete_error)
}

async fn apply_change_set(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    thread_id: &str,
    rollout_ordinal: i64,
    rollout_byte_offset: i64,
    rollout_end_byte_offset: i64,
    fallback_created_at_ms: Option<i64>,
    changes: ThreadHistoryChangeSet,
) -> ThreadStoreResult<()> {
    let ThreadHistoryChangeSet {
        changed_turns,
        changed_items,
        removed_turn_ids,
    } = changes;

    for turn_id in removed_turn_ids {
        sqlx::query("DELETE FROM thread_items WHERE thread_id = ? AND turn_id = ?")
            .bind(thread_id)
            .bind(turn_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(thread_history_error)?;
        sqlx::query("DELETE FROM thread_turns WHERE thread_id = ? AND turn_id = ?")
            .bind(thread_id)
            .bind(turn_id.as_str())
            .execute(&mut **transaction)
            .await
            .map_err(thread_history_error)?;
    }

    for turn in changed_turns {
        let turn_id = turn.turn_id;
        let error_json = turn
            .error
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(thread_history_error)?;
        let (terminal_ordinal, terminal_byte_offset) = match &turn.status {
            TurnStatus::Completed | TurnStatus::Interrupted | TurnStatus::Failed => {
                (Some(rollout_ordinal), Some(rollout_end_byte_offset))
            }
            TurnStatus::InProgress => (None, None),
        };
        // The same turn can appear again as it moves from started to completed. Update its latest
        // status, error, and timestamps, but keep the rollout ordinal from the first record that
        // created it.
        sqlx::query(
            r#"
INSERT INTO thread_turns (
    thread_id,
    turn_id,
    rollout_ordinal,
    rollout_byte_offset,
    rollout_end_ordinal,
    rollout_end_byte_offset,
    status,
    error_json,
    started_at,
    completed_at,
    duration_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(thread_id, turn_id) DO UPDATE SET
    rollout_end_ordinal = excluded.rollout_end_ordinal,
    rollout_end_byte_offset = excluded.rollout_end_byte_offset,
    status = excluded.status,
    error_json = excluded.error_json,
    started_at = excluded.started_at,
    completed_at = excluded.completed_at,
    duration_ms = excluded.duration_ms
WHERE thread_turns.rollout_end_ordinal IS NULL
  AND thread_turns.status = 'inProgress'
            "#,
        )
        .bind(thread_id)
        .bind(turn_id.as_str())
        .bind(rollout_ordinal)
        .bind(rollout_byte_offset)
        .bind(terminal_ordinal)
        .bind(terminal_byte_offset)
        .bind(turn_status(&turn.status))
        .bind(error_json)
        .bind(turn.started_at)
        .bind(turn.completed_at)
        .bind(turn.duration_ms)
        .execute(&mut **transaction)
        .await
        .map_err(thread_history_error)?;

        // Review turns can persist completed items before their turn lifecycle record. Fill the
        // summary IDs from those older item rows when the turn row finally arrives.
        sqlx::query(
            r#"
UPDATE thread_turns
SET
    first_user_item_id = COALESCE(
        first_user_item_id,
        (
            SELECT item_id
            FROM thread_items
            WHERE thread_id = ?
              AND turn_id = ?
              AND (
                item_type = 'userMessage'
                OR (item_type = '' AND json_extract(item_json, '$.type') = 'userMessage')
              )
            ORDER BY rollout_ordinal
            LIMIT 1
        )
    ),
    final_agent_item_id = COALESCE(
        (
            SELECT item_id
            FROM thread_items
            WHERE thread_id = ?
              AND turn_id = ?
              AND (
                item_type = 'agentMessage'
                OR (item_type = '' AND json_extract(item_json, '$.type') = 'agentMessage')
              )
              AND json_extract(item_json, '$.phase') = 'final_answer'
            ORDER BY rollout_ordinal DESC
            LIMIT 1
        ),
        CASE
            WHEN status IN ('completed', 'interrupted', 'failed') THEN (
                SELECT item_id
                FROM thread_items
                WHERE thread_id = ?
                  AND turn_id = ?
                  AND (
                    item_type = 'agentMessage'
                    OR (item_type = '' AND json_extract(item_json, '$.type') = 'agentMessage')
                  )
                  AND json_extract(item_json, '$.phase') IS NULL
                ORDER BY rollout_ordinal DESC
                LIMIT 1
            )
        END,
        final_agent_item_id
    )
WHERE thread_id = ?
  AND turn_id = ?
  AND (
    rollout_end_ordinal = ?
    OR status = 'inProgress'
  )
            "#,
        )
        .bind(thread_id)
        .bind(turn_id.as_str())
        .bind(thread_id)
        .bind(turn_id.as_str())
        .bind(thread_id)
        .bind(turn_id.as_str())
        .bind(thread_id)
        .bind(turn_id.as_str())
        .bind(rollout_ordinal)
        .execute(&mut **transaction)
        .await
        .map_err(thread_history_error)?;
    }

    for item in changed_items {
        let created_at_ms =
            item.started_at_ms
                .or(fallback_created_at_ms)
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: format!(
                        "thread history projection for {thread_id} is missing an item creation timestamp"
                    ),
                })?;
        let item_id = item.item.id().to_string();
        let item_json = serde_json::to_string(&item.item).map_err(thread_history_error)?;
        // Completed items are immutable: local producers emit ItemCompleted exactly once per
        // item. Tolerate an unexpected duplicate defensively so it cannot poison materialization,
        // preserving the original creation ordinal and timestamp while updating its snapshot.
        sqlx::query(
            r#"
INSERT INTO thread_items (
    thread_id,
    turn_id,
    item_id,
    rollout_ordinal,
    updated_at_ordinal,
    created_at_ms,
    item_type,
    item_json
) VALUES (?, ?, ?, ?, ?, ?, json_extract(?, '$.type'), ?)
ON CONFLICT(thread_id, turn_id, item_id) DO UPDATE SET
    updated_at_ordinal = excluded.updated_at_ordinal,
    item_type = excluded.item_type,
    item_json = excluded.item_json
            "#,
        )
        .bind(thread_id)
        .bind(item.turn_id.as_str())
        .bind(item_id.as_str())
        .bind(rollout_ordinal)
        .bind(rollout_ordinal)
        .bind(created_at_ms)
        .bind(item_json.as_str())
        .bind(item_json)
        .execute(&mut **transaction)
        .await
        .map_err(thread_history_error)?;

        // Keep summary item IDs on the turn row so reads do not need to scan every item in the
        // turn.
        match item.item {
            ThreadItem::UserMessage { .. } => {
                sqlx::query(
                    r#"
UPDATE thread_turns
SET first_user_item_id = COALESCE(first_user_item_id, ?)
WHERE thread_id = ?
  AND turn_id = ?
  AND (
    (rollout_end_ordinal IS NULL AND status = 'inProgress')
    OR (rollout_end_ordinal = rollout_ordinal AND status = 'completed')
  )
                    "#,
                )
                .bind(item_id.as_str())
                .bind(thread_id)
                .bind(item.turn_id.as_str())
                .execute(&mut **transaction)
                .await
                .map_err(thread_history_error)?;
            }
            ThreadItem::AgentMessage {
                phase: Some(MessagePhase::FinalAnswer),
                ..
            } => {
                sqlx::query(
                    r#"
UPDATE thread_turns
SET final_agent_item_id = ?
WHERE thread_id = ?
  AND turn_id = ?
  AND (
    (rollout_end_ordinal IS NULL AND status = 'inProgress')
    OR (rollout_end_ordinal = rollout_ordinal AND status = 'completed')
  )
                    "#,
                )
                .bind(item_id.as_str())
                .bind(thread_id)
                .bind(item.turn_id.as_str())
                .execute(&mut **transaction)
                .await
                .map_err(thread_history_error)?;
            }
            ThreadItem::AgentMessage {
                phase: Some(MessagePhase::Commentary) | None,
                ..
            }
            | ThreadItem::HookPrompt { .. }
            | ThreadItem::FunctionCallOutput { .. }
            | ThreadItem::InterAgentCommunication { .. }
            | ThreadItem::RawResponseItem { .. }
            | ThreadItem::Plan { .. }
            | ThreadItem::Reasoning { .. }
            | ThreadItem::CommandExecution { .. }
            | ThreadItem::FileChange { .. }
            | ThreadItem::McpToolCall { .. }
            | ThreadItem::DynamicToolCall { .. }
            | ThreadItem::CollabAgentToolCall { .. }
            | ThreadItem::SubAgentActivity { .. }
            | ThreadItem::WebSearch(_)
            | ThreadItem::ImageView { .. }
            | ThreadItem::Sleep(_)
            | ThreadItem::ImageGeneration(_)
            | ThreadItem::EnteredReviewMode { .. }
            | ThreadItem::ExitedReviewMode { .. }
            | ThreadItem::ContextCompaction { .. } => {}
        }
    }
    Ok(())
}

fn turn_status(status: &TurnStatus) -> &'static str {
    match status {
        TurnStatus::Completed => "completed",
        TurnStatus::Interrupted => "interrupted",
        TurnStatus::Failed => "failed",
        TurnStatus::InProgress => "inProgress",
    }
}

fn sqlite_integer(value: u64, field: &str) -> ThreadStoreResult<i64> {
    i64::try_from(value).map_err(|_| ThreadStoreError::Internal {
        message: format!("{field} exceeds SQLite integer range"),
    })
}

/// Stores active-only projection offsets as `-1 - offset` without changing the SQLite schema.
fn encode_paginated_projection_offset(
    offset: u64,
    lineage_complete: bool,
) -> ThreadStoreResult<i64> {
    let offset = sqlite_integer(offset, "rollout byte offset")?;
    if lineage_complete {
        Ok(offset)
    } else {
        Ok(-1 - offset)
    }
}

fn decode_paginated_projection_offset(encoded: i64) -> ThreadStoreResult<(u64, bool)> {
    let (offset, lineage_complete) = if encoded < 0 {
        (
            encoded
                .checked_neg()
                .and_then(|value| value.checked_sub(1))
                .ok_or_else(|| ThreadStoreError::Internal {
                    message: "thread history projection has an invalid incomplete byte offset"
                        .to_string(),
                })?,
            false,
        )
    } else {
        (encoded, true)
    };
    u64::try_from(offset)
        .map(|offset| (offset, lineage_complete))
        .map_err(|_| ThreadStoreError::Internal {
            message: "thread history projection has an invalid byte offset".to_string(),
        })
}

fn thread_history_error(err: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("failed to access thread history: {err}"),
    }
}

impl From<sqlx::Error> for ThreadStoreError {
    fn from(err: sqlx::Error) -> Self {
        thread_history_error(err)
    }
}

fn thread_history_delete_error(err: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("failed to delete thread history: {err}"),
    }
}
