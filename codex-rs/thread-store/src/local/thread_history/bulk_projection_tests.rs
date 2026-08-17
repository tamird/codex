use codex_app_server_protocol::ThreadHistoryChangeSet;
use codex_app_server_protocol::ThreadHistoryItemChange;
use codex_app_server_protocol::ThreadHistoryTurnChange;
use codex_app_server_protocol::TurnStatus;
use codex_protocol::ThreadId;
use codex_protocol::realtime::BemItemPresentation;
use codex_protocol::realtime::RealtimeItem;
use codex_protocol::realtime::RealtimeItemContent;
use codex_protocol::realtime::RealtimeSessionOutcome;
use codex_protocol::realtime::RealtimeTranscriptRole;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::BulkProjection;
use crate::local::LocalThreadStore;
use crate::local::test_support::test_config;
use crate::local::thread_history;
use crate::local::thread_history::ProjectedRolloutLine;
use crate::local::thread_history::RolloutProjectionStep;

fn turn(id: &str, status: TurnStatus) -> ThreadHistoryChangeSet {
    ThreadHistoryChangeSet {
        changed_turns: vec![ThreadHistoryTurnChange {
            turn_id: id.to_string(),
            status,
            error: None,
            started_at: Some(1),
            completed_at: Some(2),
            duration_ms: Some(1000),
        }],
        ..Default::default()
    }
}

fn item(turn_id: &str, id: &str, kind: &str, phase: Option<&str>) -> ThreadHistoryChangeSet {
    let value = if kind == "userMessage" {
        json!({"type":kind,"id":id,"clientId":null,"content":[]})
    } else {
        json!({"type":kind,"id":id,"text":"message","phase":phase,"memoryCitation":null})
    };
    ThreadHistoryChangeSet {
        changed_items: vec![ThreadHistoryItemChange {
            turn_id: turn_id.to_string(),
            item: serde_json::from_value(value).expect("test item"),
            started_at_ms: None,
            completed_at_ms: None,
        }],
        ..Default::default()
    }
}

async fn rows(store: &LocalThreadStore, id: ThreadId) -> Vec<Vec<String>> {
    let pool = store
        .thread_history_db()
        .await
        .expect("projection database");
    let mut result = Vec::new();
    for query in [
        "SELECT json_array(turn_id, rollout_ordinal, status, error_json, started_at, completed_at, duration_ms, first_user_item_id, final_agent_item_id, rollout_byte_offset, rollout_end_ordinal, rollout_end_byte_offset) FROM thread_turns WHERE thread_id = ? ORDER BY rollout_ordinal",
        "SELECT json_array(turn_id, item_id, rollout_ordinal, created_at_ms, item_json, item_type, updated_at_ordinal) FROM thread_items WHERE thread_id = ? ORDER BY rollout_ordinal",
        "SELECT json_array(item_id, rollout_ordinal, created_at_ms, item_type, item_json) FROM thread_realtime_items WHERE thread_id = ? ORDER BY rollout_ordinal",
        "SELECT json_array(next_rollout_byte_offset, next_rollout_ordinal) FROM thread_history_projection_state WHERE thread_id = ?",
    ] {
        result.push(
            sqlx::query_scalar::<_, String>(query)
                .bind(id.to_string())
                .fetch_all(pool)
                .await
                .expect("complete projection rows"),
        );
    }
    result
}

async fn projection_store(home: &std::path::Path) -> LocalThreadStore {
    let config = test_config(home);
    let rollout_config = codex_rollout::RolloutConfig {
        codex_home: config.codex_home.clone(),
        sqlite: config.sqlite.clone(),
        cwd: home.to_path_buf(),
        model_provider_id: config.default_model_provider_id.clone(),
        generate_memories: false,
    };
    let db = codex_rollout::state_db::try_init(&rollout_config)
        .await
        .expect("state database");
    LocalThreadStore::new(config, Some(db))
}

#[tokio::test]
async fn bulk_projection_matches_ordered_sql_for_late_and_duplicate_events() {
    let home = tempfile::tempdir().expect("Codex home");
    let store = projection_store(home.path()).await;
    let changes = vec![
        item("a", "user", "userMessage", /*phase*/ None),
        item("a", "unphased", "agentMessage", /*phase*/ None),
        turn("a", TurnStatus::InProgress),
        item("a", "final", "agentMessage", Some("final_answer")),
        turn("a", TurnStatus::Completed),
        item("a", "final", "userMessage", /*phase*/ None),
        item("a", "late", "agentMessage", Some("final_answer")),
        turn("a", TurnStatus::Failed),
        turn("b", TurnStatus::Completed),
        item("b", "late-user", "userMessage", /*phase*/ None),
        item("b", "late-final", "agentMessage", Some("final_answer")),
        turn("removed", TurnStatus::InProgress),
        item("removed", "discard", "userMessage", /*phase*/ None),
        ThreadHistoryChangeSet {
            removed_turn_ids: vec!["removed".to_string()],
            ..Default::default()
        },
        turn("removed", TurnStatus::Interrupted),
    ];
    let lines = changes
        .into_iter()
        .enumerate()
        .map(|(index, changes)| ProjectedRolloutLine {
            ordinal: index as u64,
            start_byte_offset: index as u64 * 100,
            end_byte_offset: (index as u64 + 1) * 100,
            fallback_created_at_ms: Some(index as i64),
            changes,
            realtime_item: None,
        })
        .collect::<Vec<_>>();
    for complete in [false, true] {
        let reference = ThreadId::new();
        let actual = ThreadId::new();
        if !complete {
            thread_history::begin_incomplete_paginated_projection(
                &store, reference, /*initial_ordinal*/ 0,
            )
            .await
            .expect("incomplete reference");
        }
        thread_history::apply_projection(
            &store,
            reference,
            /*start_offset*/ 0,
            lines.last().expect("last line").end_byte_offset,
            /*initial_ordinal*/ 0,
            lines
                .iter()
                .cloned()
                .map(Box::new)
                .map(RolloutProjectionStep::Line)
                .collect(),
        )
        .await
        .expect("ordered SQL reference");
        let mut bulk = BulkProjection::new(/*initial_ordinal*/ 0);
        bulk.begin_segment(/*initial_ordinal*/ 0)
            .expect("first segment");
        for line in &lines {
            bulk.apply(line).expect("reduce projection");
        }
        bulk.replace_unpublished(&store, actual, complete)
            .await
            .expect("bulk insert");
        assert_eq!(rows(&store, actual).await, rows(&store, reference).await);
    }
}

#[tokio::test]
async fn bulk_projection_preserves_realtime_identity_and_replacement_atomically() {
    let home = tempfile::tempdir().expect("Codex home");
    let store = projection_store(home.path()).await;
    let started = RealtimeItem {
        id: "z-started".to_string(),
        realtime_session_id: "voice".to_string(),
        content: RealtimeItemContent::RealtimeSessionStarted,
    };
    let transcript = RealtimeItem {
        id: "voice:transcript".to_string(),
        realtime_session_id: "voice".to_string(),
        content: RealtimeItemContent::TranscriptSegment {
            role: RealtimeTranscriptRole::Assistant,
            text: "Original spoken result".to_string(),
        },
    };
    let promotion = RealtimeItem {
        id: "b-promoted".to_string(),
        realtime_session_id: "voice".to_string(),
        content: RealtimeItemContent::BemItemPromoted {
            turn_id: "removed".to_string(),
            item_id: "artifact".to_string(),
            presentation: BemItemPresentation::InlineVisualization { index: 7 },
        },
    };
    let closed = RealtimeItem {
        id: "a-closed".to_string(),
        realtime_session_id: "voice".to_string(),
        content: RealtimeItemContent::RealtimeSessionClosed {
            outcome: RealtimeSessionOutcome::Failed,
        },
    };
    let events = [
        (ThreadHistoryChangeSet::default(), Some(started.clone())),
        (turn("removed", TurnStatus::InProgress), None),
        (ThreadHistoryChangeSet::default(), Some(transcript.clone())),
        (
            item("removed", "artifact", "userMessage", /*phase*/ None),
            None,
        ),
        (ThreadHistoryChangeSet::default(), Some(promotion.clone())),
        (
            ThreadHistoryChangeSet {
                removed_turn_ids: vec!["removed".to_string()],
                ..Default::default()
            },
            None,
        ),
        (
            ThreadHistoryChangeSet::default(),
            Some(RealtimeItem {
                id: transcript.id.clone(),
                realtime_session_id: "replacement-session".to_string(),
                content: RealtimeItemContent::TranscriptSegment {
                    role: RealtimeTranscriptRole::User,
                    text: "A duplicate must not replace the original".to_string(),
                },
            }),
        ),
        (ThreadHistoryChangeSet::default(), Some(closed.clone())),
    ];
    let lines = events
        .into_iter()
        .enumerate()
        .map(|(index, (changes, realtime_item))| ProjectedRolloutLine {
            ordinal: index as u64,
            start_byte_offset: index as u64 * 100,
            end_byte_offset: (index as u64 + 1) * 100,
            // Neither timestamp order nor lexicographic item IDs determine timeline order.
            fallback_created_at_ms: Some(10_000 - index as i64 * 123),
            changes,
            realtime_item,
        })
        .collect::<Vec<_>>();
    let reference = ThreadId::new();
    thread_history::apply_projection(
        &store,
        reference,
        /*start_offset*/ 0,
        /*next_offset*/ 800,
        /*initial_ordinal*/ 0,
        lines
            .iter()
            .cloned()
            .map(Box::new)
            .map(RolloutProjectionStep::Line)
            .collect(),
    )
    .await
    .expect("ordered SQL reference");
    let expected_realtime = [(0, started), (2, transcript), (4, promotion), (7, closed)]
        .into_iter()
        .map(|(ordinal, item)| {
            let value = serde_json::to_value(&item).expect("realtime payload");
            json!([
                item.id,
                ordinal,
                10_000 - ordinal * 123,
                value["type"],
                serde_json::to_string(&item).expect("serialize realtime payload"),
            ])
            .to_string()
        })
        .collect::<Vec<_>>();
    let expected = rows(&store, reference).await;
    assert_eq!(expected[2], expected_realtime);

    let actual = ThreadId::new();
    let mut stale = BulkProjection::new(/*initial_ordinal*/ 0);
    stale
        .apply(&ProjectedRolloutLine {
            ordinal: 0,
            start_byte_offset: 0,
            end_byte_offset: 50,
            fallback_created_at_ms: Some(-1),
            changes: ThreadHistoryChangeSet::default(),
            realtime_item: Some(RealtimeItem {
                id: "stale".to_string(),
                realtime_session_id: "old-session".to_string(),
                content: RealtimeItemContent::RealtimeSessionStarted,
            }),
        })
        .expect("stale projection");
    stale
        .replace_unpublished(&store, actual, /*lineage_complete*/ false)
        .await
        .expect("seed old projection through the producer");
    let before = rows(&store, actual).await;
    let mut bulk = BulkProjection::new(/*initial_ordinal*/ 0);
    for line in &lines {
        bulk.apply(line).expect("reduce interleaved projection");
    }
    let pool = store
        .thread_history_db()
        .await
        .expect("projection database");
    sqlx::query(
        "CREATE TRIGGER reject_realtime_replacement BEFORE INSERT ON thread_realtime_items WHEN NEW.item_id = 'voice:transcript' BEGIN SELECT RAISE(ABORT, 'test replacement failure'); END",
    ).execute(pool).await.expect("inject replacement failure");
    let error = bulk
        .replace_unpublished(&store, actual, /*lineage_complete*/ true)
        .await
        .expect_err("replacement must fail after deleting old rows");
    assert!(error.to_string().contains("test replacement failure"));
    assert_eq!(rows(&store, actual).await, before);
    sqlx::query("DROP TRIGGER reject_realtime_replacement")
        .execute(pool)
        .await
        .expect("remove failure injection");
    bulk.replace_unpublished(&store, actual, /*lineage_complete*/ true)
        .await
        .expect("replace stale rows and checkpoint atomically");
    assert_eq!(rows(&store, actual).await, expected);
}
