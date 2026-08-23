//! Builds an unpublished migration projection before inserting its final rows.
//!
//! This reducer mirrors `apply_change_set`, including summary updates at the time of each event.
//! Reconstructing summaries from final item snapshots would change late and duplicate events.

use std::collections::BTreeMap;
use std::collections::HashMap;

use codex_app_server_protocol::ThreadHistoryTurnChange;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::TurnStatus;
use codex_protocol::ThreadId;
use codex_protocol::models::MessagePhase;

use super::ProjectedRolloutLine;
use super::encode_paginated_projection_offset;
use super::sqlite_integer;
use super::thread_history_error;
use super::turn_status;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use crate::local::LocalThreadStore;

/// Final SQLite rows for one unpublished physical or complete-root projection.
pub(in crate::local) struct BulkProjection {
    groups: BTreeMap<String, TurnGroup>,
    realtime_items: BTreeMap<String, RealtimeRow>,
    next_ordinal: u64,
    next_offset: u64,
}

/// Items can precede the first lifecycle record, so a group need not have a turn row yet.
#[derive(Default)]
struct TurnGroup {
    turn: Option<TurnRow>,
    items: BTreeMap<i64, ItemRow>,
    item_ordinals: HashMap<String, i64>,
}

/// The first coordinates survive lifecycle updates, and the first terminal update wins.
struct TurnRow {
    ordinal: i64,
    byte_offset: i64,
    end_ordinal: Option<i64>,
    end_byte_offset: Option<i64>,
    change: ThreadHistoryTurnChange,
    first_user_item_id: Option<String>,
    final_agent_item_id: Option<String>,
}

/// The first item position and creation time survive replacement of its snapshot.
struct ItemRow {
    id: String,
    ordinal: i64,
    updated_at_ordinal: i64,
    created_at_ms: i64,
    json: String,
    class: SummaryClass,
}

/// Realtime facts retain their first occurrence independently of ordinary turn updates.
struct RealtimeRow {
    ordinal: i64,
    created_at_ms: i64,
    json: String,
}

/// Only these item categories participate in the persisted turn summary.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SummaryClass {
    User,
    FinalAgent,
    UnphasedAgent,
    Other,
}

impl BulkProjection {
    pub(in crate::local) fn position(&self) -> (u64, u64) {
        (self.next_ordinal, self.next_offset)
    }

    pub(in crate::local) fn new(initial_ordinal: u64) -> Self {
        Self {
            groups: BTreeMap::new(),
            realtime_items: BTreeMap::new(),
            next_ordinal: initial_ordinal,
            next_offset: 0,
        }
    }

    /// A complete-root accumulator retains rows while byte coordinates restart in each file.
    pub(in crate::local) fn begin_segment(
        &mut self,
        initial_ordinal: u64,
    ) -> ThreadStoreResult<()> {
        if self.next_ordinal != initial_ordinal {
            return Err(invalid("bulk projection segment ordinal is not contiguous"));
        }
        self.next_offset = 0;
        Ok(())
    }

    pub(in crate::local) fn apply(&mut self, line: &ProjectedRolloutLine) -> ThreadStoreResult<()> {
        if line.ordinal != self.next_ordinal || line.start_byte_offset != self.next_offset {
            return Err(invalid(
                "bulk projection record coordinates are not contiguous",
            ));
        }
        let ordinal = sqlite_integer(line.ordinal, "rollout ordinal")?;
        let byte_offset = sqlite_integer(line.start_byte_offset, "rollout byte offset")?;
        let end_byte_offset = sqlite_integer(line.end_byte_offset, "rollout byte offset")?;
        for turn_id in &line.changes.removed_turn_ids {
            self.groups.remove(turn_id);
        }
        for change in &line.changes.changed_turns {
            let group = self.groups.entry(change.turn_id.clone()).or_default();
            let terminal = change.status != TurnStatus::InProgress;
            match &mut group.turn {
                Some(turn)
                    if turn.end_ordinal.is_none()
                        && turn.change.status == TurnStatus::InProgress =>
                {
                    turn.end_ordinal = terminal.then_some(ordinal);
                    turn.end_byte_offset = terminal.then_some(end_byte_offset);
                    // Older terminal events omit the timestamp recorded by TurnStarted.
                    let started_at = change.started_at.or(turn.change.started_at);
                    turn.change = change.clone();
                    turn.change.started_at = started_at;
                }
                Some(_) => {}
                None => {
                    group.turn = Some(TurnRow {
                        ordinal,
                        byte_offset,
                        end_ordinal: terminal.then_some(ordinal),
                        end_byte_offset: terminal.then_some(end_byte_offset),
                        change: change.clone(),
                        first_user_item_id: None,
                        final_agent_item_id: None,
                    })
                }
            }
            let turn = group
                .turn
                .as_mut()
                .ok_or_else(|| invalid("bulk projection turn was not inserted"))?;
            if turn.end_ordinal == Some(ordinal) || turn.change.status == TurnStatus::InProgress {
                if turn.first_user_item_id.is_none() {
                    turn.first_user_item_id = group
                        .items
                        .values()
                        .find(|item| item.class == SummaryClass::User)
                        .map(|item| item.id.clone());
                }
                let final_agent = group
                    .items
                    .values()
                    .rev()
                    .find(|item| item.class == SummaryClass::FinalAgent)
                    .or_else(|| {
                        (turn.change.status != TurnStatus::InProgress)
                            .then(|| {
                                group
                                    .items
                                    .values()
                                    .rev()
                                    .find(|item| item.class == SummaryClass::UnphasedAgent)
                            })
                            .flatten()
                    });
                if let Some(item) = final_agent {
                    turn.final_agent_item_id = Some(item.id.clone());
                }
            }
        }
        for change in &line.changes.changed_items {
            let created_at_ms = change
                .started_at_ms
                .or(line.fallback_created_at_ms)
                .ok_or_else(|| invalid("bulk projection item is missing its creation timestamp"))?;
            let id = change.item.id().to_string();
            let class = match &change.item {
                ThreadItem::UserMessage { .. } => SummaryClass::User,
                ThreadItem::AgentMessage {
                    phase: Some(MessagePhase::FinalAnswer),
                    ..
                } => SummaryClass::FinalAgent,
                ThreadItem::AgentMessage { phase: None, .. } => SummaryClass::UnphasedAgent,
                _ => SummaryClass::Other,
            };
            let json = serde_json::to_string(&change.item).map_err(thread_history_error)?;
            let group = self.groups.entry(change.turn_id.clone()).or_default();
            if let Some(previous_ordinal) = group.item_ordinals.get(&id) {
                let item = group
                    .items
                    .get_mut(previous_ordinal)
                    .ok_or_else(|| invalid("bulk projection item index is inconsistent"))?;
                item.updated_at_ordinal = ordinal;
                item.json = json;
                item.class = class;
            } else {
                if group.items.contains_key(&ordinal) {
                    return Err(invalid("bulk projection has two items at one ordinal"));
                }
                group.item_ordinals.insert(id.clone(), ordinal);
                group.items.insert(
                    ordinal,
                    ItemRow {
                        id: id.clone(),
                        ordinal,
                        updated_at_ordinal: ordinal,
                        created_at_ms,
                        json,
                        class,
                    },
                );
            }
            if let Some(turn) = &mut group.turn
                && ((turn.end_ordinal.is_none() && turn.change.status == TurnStatus::InProgress)
                    || (turn.end_ordinal == Some(turn.ordinal)
                        && turn.change.status == TurnStatus::Completed))
            {
                match class {
                    SummaryClass::User if turn.first_user_item_id.is_none() => {
                        turn.first_user_item_id = Some(id)
                    }
                    SummaryClass::FinalAgent => turn.final_agent_item_id = Some(id),
                    _ => {}
                }
            }
        }
        if let Some(item) = &line.realtime_item {
            let json = serde_json::to_string(item).map_err(thread_history_error)?;
            let created_at_ms = line
                .fallback_created_at_ms
                .ok_or_else(|| invalid("realtime rollout item is missing its timestamp"))?;
            self.realtime_items
                .entry(item.id.clone())
                .or_insert(RealtimeRow {
                    ordinal,
                    created_at_ms,
                    json,
                });
        }
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("bulk projection ordinal overflow"))?;
        self.next_offset = line.end_byte_offset;
        Ok(())
    }

    /// Replaces only an unpublished migration identity. The checkpoint commits after all rows.
    pub(in crate::local) async fn replace_unpublished(
        &self,
        store: &LocalThreadStore,
        thread_id: ThreadId,
        lineage_complete: bool,
    ) -> ThreadStoreResult<()> {
        let pool = store.thread_history_db().await?;
        let mut transaction = pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(thread_history_error)?;
        let thread_id = thread_id.to_string();
        for statement in [
            "DELETE FROM thread_items WHERE thread_id = ?",
            "DELETE FROM thread_turns WHERE thread_id = ?",
            "DELETE FROM thread_realtime_items WHERE thread_id = ?",
            "DELETE FROM thread_history_projection_state WHERE thread_id = ?",
        ] {
            sqlx::query(statement)
                .bind(&thread_id)
                .execute(&mut *transaction)
                .await
                .map_err(thread_history_error)?;
        }
        let turns = self
            .groups
            .iter()
            .filter_map(|(id, group)| group.turn.as_ref().map(|turn| (id, turn)))
            .map(|(id, turn)| {
                Ok((
                    id,
                    turn,
                    turn.change
                        .error
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()
                        .map_err(thread_history_error)?,
                ))
            })
            .collect::<ThreadStoreResult<Vec<_>>>()?;
        for rows in turns.chunks(64) {
            let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "INSERT INTO thread_turns (thread_id, turn_id, rollout_ordinal, rollout_byte_offset, rollout_end_ordinal, rollout_end_byte_offset, status, error_json, started_at, completed_at, duration_ms, first_user_item_id, final_agent_item_id) ",
            );
            query.push_values(rows, |mut row, (id, turn, error)| {
                row.push_bind(&thread_id)
                    .push_bind(*id)
                    .push_bind(turn.ordinal)
                    .push_bind(turn.byte_offset)
                    .push_bind(turn.end_ordinal)
                    .push_bind(turn.end_byte_offset)
                    .push_bind(turn_status(&turn.change.status))
                    .push_bind(error)
                    .push_bind(turn.change.started_at)
                    .push_bind(turn.change.completed_at)
                    .push_bind(turn.change.duration_ms)
                    .push_bind(&turn.first_user_item_id)
                    .push_bind(&turn.final_agent_item_id);
            });
            query
                .build()
                .execute(&mut *transaction)
                .await
                .map_err(thread_history_error)?;
        }
        let items = self
            .groups
            .iter()
            .flat_map(|(turn_id, group)| group.items.values().map(move |item| (turn_id, item)))
            .collect::<Vec<_>>();
        for rows in items.chunks(64) {
            let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "INSERT INTO thread_items (thread_id, turn_id, item_id, rollout_ordinal, updated_at_ordinal, created_at_ms, item_json, item_type) ",
            );
            query.push_values(rows, |mut row, (turn_id, item)| {
                row.push_bind(&thread_id)
                    .push_bind(*turn_id)
                    .push_bind(&item.id)
                    .push_bind(item.ordinal)
                    .push_bind(item.updated_at_ordinal)
                    .push_bind(item.created_at_ms)
                    .push_bind(&item.json)
                    .push("json_extract(")
                    .push_bind_unseparated(&item.json)
                    .push_unseparated(", '$.type')");
            });
            query
                .build()
                .execute(&mut *transaction)
                .await
                .map_err(thread_history_error)?;
        }
        let realtime_items = self.realtime_items.iter().collect::<Vec<_>>();
        for rows in realtime_items.chunks(64) {
            let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "INSERT INTO thread_realtime_items (thread_id, item_id, rollout_ordinal, created_at_ms, item_json, item_type) ",
            );
            query.push_values(rows, |mut row, (id, item)| {
                row.push_bind(&thread_id)
                    .push_bind(*id)
                    .push_bind(item.ordinal)
                    .push_bind(item.created_at_ms)
                    .push_bind(&item.json)
                    .push("json_extract(")
                    .push_bind_unseparated(&item.json)
                    .push_unseparated(", '$.type')");
            });
            query
                .build()
                .execute(&mut *transaction)
                .await
                .map_err(thread_history_error)?;
        }
        sqlx::query("INSERT INTO thread_history_projection_state (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, ?, ?)")
            .bind(&thread_id).bind(encode_paginated_projection_offset(self.next_offset, lineage_complete)?)
            .bind(sqlite_integer(self.next_ordinal, "rollout ordinal")?)
            .execute(&mut *transaction).await.map_err(thread_history_error)?;
        transaction.commit().await.map_err(thread_history_error)
    }
}

fn invalid(message: &str) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: message.to_string(),
    }
}

#[cfg(test)]
#[path = "bulk_projection_tests.rs"]
mod tests;
