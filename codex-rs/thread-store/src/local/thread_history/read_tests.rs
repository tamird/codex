use std::fs;
use std::io::Write;
use std::time::Duration;

use chrono::Utc;
use codex_app_server_protocol::CodexErrorInfo;
use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_app_server_protocol::ThreadTimelineEntry;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use codex_protocol::protocol::UserMessageEvent;
use codex_protocol::realtime::BemItemPresentation;
use codex_protocol::realtime::RealtimeItem;
use codex_protocol::realtime::RealtimeItemContent;
use codex_protocol::realtime::RealtimeSessionOutcome;
use codex_protocol::realtime::RealtimeTranscriptRole;
use codex_rollout::CompactedItem;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::*;
use crate::ItemSortKey;
use crate::ListTimelineParams;
use crate::SearchThreadOccurrencesParams;
use crate::SortDirection;
use crate::StoredTurnError;
use crate::StoredTurnStatus;
use crate::local::test_support::test_config;

#[tokio::test]
async fn list_turns_pages_projected_rows_and_applies_item_views() {
    let (_home, store, thread_id) = store_with_mode(ThreadHistoryMode::Paginated).await;
    let db = history_db(&store).await;
    for (turn_id, ordinal, status, error, first_user, final_agent) in [
        (
            "turn-1",
            10,
            "completed",
            None,
            Some("user-1"),
            Some("agent-1"),
        ),
        (
            "turn-2",
            20,
            "failed",
            Some(
                r#"{"message":"turn failed","codexErrorInfo":"serverOverloaded","additionalDetails":"retry later"}"#,
            ),
            None,
            None,
        ),
        ("turn-3", 30, "inProgress", None, None, None),
    ] {
        insert_turn(
            db,
            thread_id,
            turn_id,
            ordinal,
            status,
            error,
            first_user,
            final_agent,
        )
        .await;
    }
    for (turn_id, item_id, ordinal) in [
        ("turn-1", "user-1", 11),
        ("turn-1", "middle-1", 12),
        ("turn-1", "agent-1", 13),
    ] {
        insert_item(db, thread_id, turn_id, item_id, ordinal).await;
    }

    let first_page = store
        .list_turns(turn_params(
            thread_id,
            /*cursor*/ None,
            /*page_size*/ 2,
            SortDirection::Asc,
            StoredTurnItemsView::Summary,
        ))
        .await
        .expect("first turns page");
    assert_eq!(turn_ids(&first_page), vec!["turn-1", "turn-2"]);
    assert_eq!(
        first_page.turns[0].items,
        vec![
            expected_item("turn-1", "user-1", /*rollout_ordinal*/ 11),
            expected_item("turn-1", "agent-1", /*rollout_ordinal*/ 13),
        ]
    );
    assert_eq!(
        first_page.turns[1].error,
        Some(StoredTurnError {
            message: "turn failed".to_string(),
            codex_error_info: Some(CodexErrorInfo::ServerOverloaded),
            additional_details: Some("retry later".to_string()),
        })
    );
    let second_page = store
        .list_turns(turn_params(
            thread_id,
            first_page.next_cursor,
            /*page_size*/ 2,
            SortDirection::Asc,
            StoredTurnItemsView::NotLoaded,
        ))
        .await
        .expect("second turns page");
    assert_eq!(turn_ids(&second_page), vec!["turn-3"]);
    assert_eq!(second_page.turns[0].items, Vec::new());
    assert_eq!(second_page.turns[0].status, StoredTurnStatus::InProgress);
    let backwards_page = store
        .list_turns(turn_params(
            thread_id,
            second_page.backwards_cursor,
            /*page_size*/ 2,
            SortDirection::Desc,
            StoredTurnItemsView::NotLoaded,
        ))
        .await
        .expect("backwards turns page");
    assert_eq!(turn_ids(&backwards_page), vec!["turn-3", "turn-2"]);
}

#[tokio::test]
async fn indexed_paginated_reads_do_not_traverse_same_thread_segments() {
    assert_indexed_paginated_segment_count(/*segment_count*/ 1_025).await;
}

#[tokio::test]
async fn indexed_paginated_reads_remain_bounded_across_ten_thousand_segments() {
    assert_indexed_paginated_segment_count(/*segment_count*/ 10_001).await;
}

#[tokio::test]
async fn indexed_paginated_reads_trust_current_projection_for_immutable_predecessors() {
    assert_indexed_paginated_segment_count_with_missing_predecessor(
        /*segment_count*/ 4, /*remove_projected_predecessor*/ true,
    )
    .await;
}

#[tokio::test]
async fn fork_marked_projection_requires_ancestry_without_history_base() {
    let (home, store, thread_id) = store_with_mode(ThreadHistoryMode::Paginated).await;
    let active_path =
        write_projected_same_thread_segments(home.path(), thread_id, /*segment_count*/ 2);
    let mut lines = codex_rollout::RolloutRecorder::load_rollout_lines(&active_path)
        .await
        .expect("read active segment")
        .0;
    let RolloutItem::SessionMeta(meta) = &mut lines[0].item else {
        panic!("active segment must start with metadata");
    };
    // Older fork headers lack history_base and must use the ancestry-validating fallback.
    meta.meta.forked_from_id = Some(ThreadId::new());
    meta.meta.history_base = None;
    let predecessor = match &lines[1].item {
        RolloutItem::RolloutReference(reference) => reference.rollout_path.clone(),
        _ => panic!("active segment must retain its predecessor reference"),
    };
    let mut encoded = Vec::new();
    for line in &lines {
        serde_json::to_writer(&mut encoded, line).expect("encode active record");
        encoded.push(b'\n');
    }
    fs::write(&active_path, &encoded).expect("write old fork header");
    sqlx::query(
        "INSERT INTO thread_history_projection_state (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, ?, ?)",
    )
    .bind(thread_id.to_string())
    .bind(i64::try_from(encoded.len()).expect("byte offset"))
    .bind(8_i64)
    .execute(history_db(&store).await)
    .await
    .expect("seed complete projection checkpoint");
    assert!(store.has_history_projection(thread_id).await.expect("valid ancestry"));

    fs::remove_file(predecessor).expect("remove required predecessor");
    store
        .has_history_projection(thread_id)
        .await
        .expect_err("a complete fallback checkpoint does not authenticate missing ancestry");
}

#[tokio::test]
async fn indexed_paginated_cursor_pages_never_open_projected_predecessors() {
    assert_indexed_cursor_pages_ignore_missing_predecessors(ThreadHistoryMode::Paginated).await;
}

#[tokio::test]
async fn indexed_legacy_cursor_pages_never_open_projected_predecessors() {
    assert_indexed_cursor_pages_ignore_missing_predecessors(ThreadHistoryMode::Legacy).await;
}

async fn assert_indexed_cursor_pages_ignore_missing_predecessors(history_mode: ThreadHistoryMode) {
    const SEGMENT_COUNT: usize = 65;
    const TURN_COUNT: usize = 6;

    let (home, store, thread_id) = store_with_mode(history_mode).await;
    let active_path = write_projected_same_thread_segments_with_mode(
        home.path(),
        thread_id,
        SEGMENT_COUNT,
        history_mode,
    );
    let db = history_db(&store).await;
    let next_ordinal = i64::try_from(SEGMENT_COUNT).expect("segment count fits ordinal") * 4;

    for index in 0..TURN_COUNT {
        let segment_index = SEGMENT_COUNT - TURN_COUNT + index;
        let ordinal = i64::try_from(segment_index).expect("turn segment fits ordinal") * 4;
        let turn_id = format!("turn-{index}");
        let user_id = format!("user-{index}");
        let agent_id = format!("agent-{index}");
        insert_turn(
            db,
            thread_id,
            turn_id.as_str(),
            ordinal,
            "completed",
            /*error_json*/ None,
            Some(user_id.as_str()),
            Some(agent_id.as_str()),
        )
        .await;
        insert_item(
            db,
            thread_id,
            turn_id.as_str(),
            user_id.as_str(),
            ordinal + 1,
        )
        .await;
        insert_item(
            db,
            thread_id,
            turn_id.as_str(),
            agent_id.as_str(),
            ordinal + 2,
        )
        .await;
    }
    sqlx::query(
        "INSERT INTO thread_history_projection_state (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, ?, ?)",
    )
    .bind(thread_id.to_string())
    .bind(
        i64::try_from(
            fs::metadata(active_path.as_path())
                .expect("active segment metadata")
                .len(),
        )
        .expect("active segment length fits SQLite integer"),
    )
    .bind(next_ordinal)
    .execute(db)
    .await
    .expect("publish current projected history after its rows");

    let immutable_directory = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string());
    let mut deleted_predecessors = 0;
    for segment in fs::read_dir(immutable_directory).expect("list immutable predecessor segments") {
        let segment = segment.expect("read immutable segment directory");
        fs::remove_file(segment.path().join("segment.jsonl"))
            .expect("remove already projected immutable predecessor");
        deleted_predecessors += 1;
    }
    assert_eq!(deleted_predecessors, SEGMENT_COUNT - 1);
    assert!(
        store.resolve_rollout_lineage(thread_id).await.is_err(),
        "the deleted predecessors must reject any complete physical lineage traversal"
    );

    let first_turn_page = indexed_projected_turn_page(
        &store,
        history_mode,
        turn_params(
            thread_id,
            /*cursor*/ None,
            /*page_size*/ 2,
            SortDirection::Desc,
            StoredTurnItemsView::Summary,
        ),
    )
    .await;
    assert_eq!(turn_ids(&first_turn_page), vec!["turn-5", "turn-4"]);
    assert_eq!(
        first_turn_page.turns[0].items,
        vec![
            expected_item("turn-5", "user-5", /*rollout_ordinal*/ 257),
            expected_item("turn-5", "agent-5", /*rollout_ordinal*/ 258),
        ]
    );

    let older_turn_page = indexed_projected_turn_page(
        &store,
        history_mode,
        turn_params(
            thread_id,
            first_turn_page.next_cursor,
            /*page_size*/ 2,
            SortDirection::Desc,
            StoredTurnItemsView::NotLoaded,
        ),
    )
    .await;
    assert_eq!(turn_ids(&older_turn_page), vec!["turn-3", "turn-2"]);
    assert!(older_turn_page.turns[0].items.is_empty());

    let oldest_turn_page = indexed_projected_turn_page(
        &store,
        history_mode,
        turn_params(
            thread_id,
            older_turn_page.next_cursor.clone(),
            /*page_size*/ 2,
            SortDirection::Desc,
            StoredTurnItemsView::Summary,
        ),
    )
    .await;
    assert_eq!(turn_ids(&oldest_turn_page), vec!["turn-1", "turn-0"]);
    assert!(oldest_turn_page.next_cursor.is_none());

    let backwards_turn_page = indexed_projected_turn_page(
        &store,
        history_mode,
        turn_params(
            thread_id,
            older_turn_page.backwards_cursor,
            /*page_size*/ 2,
            SortDirection::Asc,
            StoredTurnItemsView::NotLoaded,
        ),
    )
    .await;
    assert_eq!(turn_ids(&backwards_turn_page), vec!["turn-3", "turn-4"]);

    let first_item_page = indexed_projected_item_page(
        &store,
        history_mode,
        item_params(
            thread_id,
            /*turn_id*/ None,
            /*cursor*/ None,
            /*page_size*/ 3,
            SortDirection::Asc,
        ),
    )
    .await;
    assert_eq!(
        item_ids(&first_item_page),
        vec!["user-0", "agent-0", "user-1"]
    );

    let second_item_page = indexed_projected_item_page(
        &store,
        history_mode,
        item_params(
            thread_id,
            /*turn_id*/ None,
            first_item_page.next_cursor,
            /*page_size*/ 3,
            SortDirection::Asc,
        ),
    )
    .await;
    assert_eq!(
        item_ids(&second_item_page),
        vec!["agent-1", "user-2", "agent-2"]
    );

    let backwards_item_page = indexed_projected_item_page(
        &store,
        history_mode,
        item_params(
            thread_id,
            /*turn_id*/ None,
            second_item_page.backwards_cursor,
            /*page_size*/ 3,
            SortDirection::Desc,
        ),
    )
    .await;
    assert_eq!(
        item_ids(&backwards_item_page),
        vec!["agent-1", "user-1", "agent-0"]
    );

    let first_turn_items = indexed_projected_item_page(
        &store,
        history_mode,
        item_params(
            thread_id,
            Some("turn-4"),
            /*cursor*/ None,
            /*page_size*/ 1,
            SortDirection::Asc,
        ),
    )
    .await;
    assert_eq!(item_ids(&first_turn_items), vec!["user-4"]);

    let second_turn_items = indexed_projected_item_page(
        &store,
        history_mode,
        item_params(
            thread_id,
            Some("turn-4"),
            first_turn_items.next_cursor,
            /*page_size*/ 1,
            SortDirection::Asc,
        ),
    )
    .await;
    assert_eq!(item_ids(&second_turn_items), vec!["agent-4"]);
    assert!(second_turn_items.next_cursor.is_none());

    if history_mode == ThreadHistoryMode::Legacy {
        let latest_turn_page = list_segmented_legacy_turns(
            &store,
            turn_params(
                thread_id,
                /*cursor*/ None,
                /*page_size*/ 1,
                SortDirection::Desc,
                StoredTurnItemsView::Summary,
            ),
        )
        .await
        .expect("read current indexed legacy projection")
        .expect("complete legacy projection remains available");
        assert_eq!(turn_ids(&latest_turn_page), vec!["turn-5"]);
    }

    fs::OpenOptions::new()
        .append(true)
        .open(active_path.as_path())
        .expect("open active projected segment")
        .write_all(b"\n")
        .expect("invalidate active projection checkpoint");

    let params = turn_params(
        thread_id,
        /*cursor*/ None,
        /*page_size*/ 1,
        SortDirection::Desc,
        StoredTurnItemsView::Summary,
    );
    match history_mode {
        ThreadHistoryMode::Paginated => {
            let error = store
                .list_turns(params)
                .await
                .expect_err("stale paginated projection must inspect the missing predecessor");
            assert!(
                error.to_string().contains("rollout reference")
                    || error.to_string().contains("referenced rollout"),
                "stale paginated projection must reject missing immutable history: {error}"
            );
        }
        ThreadHistoryMode::Legacy => {
            let page = list_existing_segmented_legacy_turns(&store, params)
                .await
                .expect("inspect stale legacy projection");
            assert!(
                page.is_none(),
                "a stale legacy projection must not expose outdated indexed turns"
            );
        }
    }
}

async fn indexed_projected_turn_page(
    store: &LocalThreadStore,
    history_mode: ThreadHistoryMode,
    params: ListTurnsParams,
) -> TurnPage {
    match history_mode {
        ThreadHistoryMode::Paginated => store
            .list_turns(params)
            .await
            .expect("read projected paginated turn page"),
        ThreadHistoryMode::Legacy => list_existing_segmented_legacy_turns(store, params)
            .await
            .expect("read projected legacy turn page")
            .expect("complete legacy projection remains available"),
    }
}

async fn indexed_projected_item_page(
    store: &LocalThreadStore,
    history_mode: ThreadHistoryMode,
    params: ListItemsParams,
) -> ItemPage {
    match history_mode {
        ThreadHistoryMode::Paginated => store
            .list_items(params)
            .await
            .expect("read projected paginated item page"),
        ThreadHistoryMode::Legacy => list_segmented_legacy_items(store, params)
            .await
            .expect("read projected legacy item page")
            .expect("complete legacy projection remains available"),
    }
}

async fn assert_indexed_paginated_segment_count(segment_count: usize) {
    assert_indexed_paginated_segment_count_with_missing_predecessor(
        segment_count,
        /*remove_projected_predecessor*/ false,
    )
    .await;
}

async fn assert_indexed_paginated_segment_count_with_missing_predecessor(
    segment_count: usize,
    remove_projected_predecessor: bool,
) {
    let (home, store, thread_id) = store_with_mode(ThreadHistoryMode::Paginated).await;
    let active_path = write_projected_same_thread_segments(home.path(), thread_id, segment_count);
    let db = history_db(&store).await;
    let newest_ordinal = i64::try_from(segment_count).expect("segment count fits ordinal") * 4;

    insert_turn(
        db,
        thread_id,
        "newest-turn",
        newest_ordinal,
        "completed",
        /*error_json*/ None,
        Some("newest-user"),
        Some("newest-agent"),
    )
    .await;
    insert_item(
        db,
        thread_id,
        "newest-turn",
        "newest-user",
        newest_ordinal + 1,
    )
    .await;
    insert_item(
        db,
        thread_id,
        "newest-turn",
        "newest-agent",
        newest_ordinal + 2,
    )
    .await;
    sqlx::query(
        "INSERT INTO thread_history_projection_state (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, ?, ?)",
    )
    .bind(thread_id.to_string())
    .bind(
        i64::try_from(
            fs::metadata(active_path.as_path())
                .expect("active metadata")
                .len(),
        )
        .expect("byte offset"),
    )
    .bind(newest_ordinal + 3)
    .execute(db)
    .await
    .expect("seed current projection checkpoint");

    if remove_projected_predecessor {
        let segment_directory = fs::read_dir(
            home.path()
                .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
                .join(thread_id.to_string()),
        )
        .expect("list projected immutable segments")
        .next()
        .expect("projected immutable predecessor")
        .expect("read projected immutable predecessor")
        .path();
        fs::remove_file(segment_directory.join("segment.jsonl"))
            .expect("remove already projected immutable predecessor");
    }

    assert!(
        store
            .has_history_projection(thread_id)
            .await
            .expect("indexed same-thread history projection"),
        "an indexed projected thread must not resolve all immutable predecessors"
    );

    let turns = store
        .list_turns(turn_params(
            thread_id,
            /*cursor*/ None,
            /*page_size*/ 1,
            SortDirection::Desc,
            StoredTurnItemsView::Summary,
        ))
        .await
        .expect("indexed latest turn beyond the physical segment limit");
    assert_eq!(turn_ids(&turns), vec!["newest-turn"]);
    assert_eq!(turns.turns[0].items.len(), 2);

    let items = store
        .list_items(item_params(
            thread_id,
            Some("newest-turn"),
            /*cursor*/ None,
            /*page_size*/ 2,
            SortDirection::Asc,
        ))
        .await
        .expect("indexed latest items beyond the physical segment limit");
    assert_eq!(item_ids(&items), vec!["newest-user", "newest-agent"]);

    if remove_projected_predecessor {
        fs::OpenOptions::new()
            .append(true)
            .open(active_path.as_path())
            .expect("open active projected segment")
            .write_all(b"\n")
            .expect("invalidate active projection checkpoint");
        let error = store
            .list_turns(turn_params(
                thread_id,
                /*cursor*/ None,
                /*page_size*/ 1,
                SortDirection::Desc,
                StoredTurnItemsView::Summary,
            ))
            .await
            .expect_err("stale projection must validate the missing immutable predecessor");
        assert!(
            error.to_string().contains("rollout reference")
                || error.to_string().contains("referenced rollout"),
            "stale projected history must reject missing immutable predecessors: {error}"
        );
    }
}

#[tokio::test]
async fn list_items_pages_whole_thread_and_per_turn_rows() {
    let (_home, store, thread_id) = store_with_mode(ThreadHistoryMode::Paginated).await;
    let db = history_db(&store).await;
    for (turn_id, ordinal) in [("turn-1", 10), ("turn-2", 20)] {
        insert_turn(
            db,
            thread_id,
            turn_id,
            ordinal,
            "completed",
            /*error_json*/ None,
            /*first_user_item_id*/ None,
            /*final_agent_item_id*/ None,
        )
        .await;
    }
    for (turn_id, item_id, ordinal) in [
        ("turn-1", "item-1", 11),
        ("turn-1", "item-2", 12),
        ("turn-2", "item-3", 21),
        ("turn-2", "item-4", 22),
        ("turn-2", "item-5", 23),
    ] {
        insert_item(db, thread_id, turn_id, item_id, ordinal).await;
    }

    let first_page = store
        .list_items(item_params(
            thread_id,
            /*turn_id*/ None,
            /*cursor*/ None,
            /*page_size*/ 2,
            SortDirection::Asc,
        ))
        .await
        .expect("first item page");
    assert_eq!(
        first_page.items,
        vec![
            expected_item("turn-1", "item-1", /*rollout_ordinal*/ 11),
            expected_item("turn-1", "item-2", /*rollout_ordinal*/ 12),
        ]
    );
    let second_page = store
        .list_items(item_params(
            thread_id,
            /*turn_id*/ None,
            first_page.next_cursor,
            /*page_size*/ 2,
            SortDirection::Asc,
        ))
        .await
        .expect("second item page");
    assert_eq!(item_ids(&second_page), vec!["item-3", "item-4"]);
    let backwards_page = store
        .list_items(item_params(
            thread_id,
            /*turn_id*/ None,
            second_page.backwards_cursor,
            /*page_size*/ 2,
            SortDirection::Desc,
        ))
        .await
        .expect("backwards item page");
    assert_eq!(item_ids(&backwards_page), vec!["item-3", "item-2"]);

    let turn_page = store
        .list_items(item_params(
            thread_id,
            Some("turn-2"),
            /*cursor*/ None,
            /*page_size*/ 2,
            SortDirection::Desc,
        ))
        .await
        .expect("turn item page");
    assert_eq!(item_ids(&turn_page), vec!["item-5", "item-4"]);
    let whole_thread_from_turn_cursor = store
        .list_items(item_params(
            thread_id,
            /*turn_id*/ None,
            turn_page.backwards_cursor.clone(),
            /*page_size*/ 2,
            SortDirection::Desc,
        ))
        .await
        .expect("whole-thread page from turn cursor");
    assert_eq!(
        item_ids(&whole_thread_from_turn_cursor),
        vec!["item-5", "item-4"]
    );
    let next_turn_page = store
        .list_items(item_params(
            thread_id,
            Some("turn-2"),
            turn_page.next_cursor,
            /*page_size*/ 2,
            SortDirection::Desc,
        ))
        .await
        .expect("next turn item page");
    assert_eq!(item_ids(&next_turn_page), vec!["item-3"]);
}

#[tokio::test]
async fn timeline_interleaves_items_and_restores_page_boundary_session_state() {
    let (_home, store, thread_id) = store_with_mode(ThreadHistoryMode::Paginated).await;
    let db = history_db(&store).await;
    for (item_id, ordinal) in [
        ("before", 10),
        ("middle", 20),
        ("after", 30),
        ("closed", 40),
    ] {
        insert_item(db, thread_id, "turn-1", item_id, ordinal).await;
    }
    for (ordinal, item) in [
        (
            11,
            RealtimeItem {
                id: "voice-1:started".to_string(),
                realtime_session_id: "voice-1".to_string(),
                content: RealtimeItemContent::RealtimeSessionStarted,
            },
        ),
        (
            12,
            RealtimeItem {
                id: "voice-1:source:0".to_string(),
                realtime_session_id: "voice-1".to_string(),
                content: RealtimeItemContent::TranscriptSegment {
                    role: RealtimeTranscriptRole::Assistant,
                    text: "Before the artifact".to_string(),
                },
            },
        ),
        (
            21,
            RealtimeItem {
                id: "artifact".to_string(),
                realtime_session_id: "voice-1".to_string(),
                content: RealtimeItemContent::BemItemPromoted {
                    turn_id: "turn-1".to_string(),
                    item_id: "middle".to_string(),
                    presentation: BemItemPresentation::InlineMarkdown,
                },
            },
        ),
        (
            31,
            RealtimeItem {
                id: "voice-1:closed".to_string(),
                realtime_session_id: "voice-1".to_string(),
                content: RealtimeItemContent::RealtimeSessionClosed {
                    outcome: RealtimeSessionOutcome::Ended,
                },
            },
        ),
    ] {
        let item_json = serde_json::to_string(&item).expect("serialize realtime item");
        sqlx::query(
            r#"
INSERT INTO thread_realtime_items (
    thread_id, item_id, rollout_ordinal, created_at_ms, item_type, item_json
) VALUES (?, ?, ?, ?, json_extract(?, '$.type'), ?)
            "#,
        )
        .bind(thread_id.to_string())
        .bind(&item.id)
        .bind(ordinal)
        .bind(ordinal * 1_000)
        .bind(&item_json)
        .bind(&item_json)
        .execute(db)
        .await
        .expect("insert realtime item");
    }

    let latest = store
        .list_timeline(ListTimelineParams {
            thread_id,
            cursor: None,
            page_size: 3,
        })
        .await
        .expect("list latest timeline page");
    assert_eq!(
        latest
            .items
            .iter()
            .map(|item| match item {
                ThreadTimelineEntry::Item { position, .. }
                | ThreadTimelineEntry::Realtime { position, .. }
                | ThreadTimelineEntry::TurnStarted { position, .. }
                | ThreadTimelineEntry::TurnCompleted { position, .. } => *position,
            })
            .collect::<Vec<_>>(),
        vec![30, 31, 40]
    );
    assert_eq!(
        latest.active_realtime_session_at_page_start,
        Some("voice-1".to_string())
    );
    let older = store
        .list_timeline(ListTimelineParams {
            thread_id,
            cursor: latest.next_cursor,
            page_size: 3,
        })
        .await
        .expect("list older timeline page");
    assert_eq!(
        older
            .items
            .iter()
            .map(|item| match item {
                ThreadTimelineEntry::Item { position, .. }
                | ThreadTimelineEntry::Realtime { position, .. }
                | ThreadTimelineEntry::TurnStarted { position, .. }
                | ThreadTimelineEntry::TurnCompleted { position, .. } => *position,
            })
            .collect::<Vec<_>>(),
        vec![12, 20, 21]
    );
    assert_eq!(
        older.active_realtime_session_at_page_start,
        Some("voice-1".to_string())
    );
    let oldest = store
        .list_timeline(ListTimelineParams {
            thread_id,
            cursor: older.next_cursor,
            page_size: 3,
        })
        .await
        .expect("list oldest timeline page");
    assert_eq!(oldest.active_realtime_session_at_page_start, None);
    assert!(matches!(
        oldest.items.as_slice(),
        [
            ThreadTimelineEntry::Item { position: 10, .. },
            ThreadTimelineEntry::Realtime { position: 11, .. }
        ]
    ));

    let ordinary_items = store
        .list_items(item_params(
            thread_id,
            /*turn_id*/ None,
            /*cursor*/ None,
            /*page_size*/ 10,
            SortDirection::Asc,
        ))
        .await
        .expect("existing item API excludes realtime facts");
    assert_eq!(
        item_ids(&ordinary_items),
        vec!["before", "middle", "after", "closed"]
    );
}

#[tokio::test]
async fn timeline_turn_boundaries_page_through_shared_ordinals() {
    let (_home, store, thread_id) = store_with_mode(ThreadHistoryMode::Paginated).await;
    let db = history_db(&store).await;
    let error = r#"{"message":"failed","codexErrorInfo":null,"additionalDetails":null}"#;
    for (turn_id, start, end, status, error_json) in [
        ("complete", 10, Some(20), "completed", None),
        ("interrupt", 20, Some(30), "interrupted", None),
        ("failed", 30, Some(40), "failed", Some(error)),
        ("running", 40, None, "inProgress", None),
    ] {
        insert_turn(
            db, thread_id, turn_id, start, status, error_json, /*first_user_item_id*/ None,
            /*final_agent_item_id*/ None,
        )
        .await;
        sqlx::query("UPDATE thread_turns SET rollout_end_ordinal = ?, started_at = ?, completed_at = ?, duration_ms = ? WHERE thread_id = ? AND turn_id = ?")
            .bind(end)
            .bind(start)
            .bind(end)
            .bind(end.map(|end| (end - start) * 1000))
            .bind(thread_id.to_string())
            .bind(turn_id)
            .execute(db)
            .await
            .expect("set turn boundary");
    }
    insert_item(
        db, thread_id, "complete", "item", /*rollout_ordinal*/ 20,
    )
    .await;

    let all = store
        .list_timeline(ListTimelineParams {
            thread_id,
            cursor: None,
            page_size: 20,
        })
        .await
        .expect("full timeline");
    let mut cursor = None;
    let mut paged = Vec::new();
    loop {
        let page = store
            .list_timeline(ListTimelineParams {
                thread_id,
                cursor,
                page_size: 1,
            })
            .await
            .expect("single-entry timeline page");
        assert_eq!(page.items.len(), 1);
        paged.extend(page.items);
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    paged.reverse();
    assert_eq!(paged, all.items);
    assert_eq!(
        all.items
            .iter()
            .map(crate::local::thread_history::realtime::entry_key)
            .collect::<Vec<_>>(),
        vec![
            (10, 0, "complete"),
            (20, 0, "interrupt"),
            (20, 1, "item"),
            (20, 3, "complete"),
            (30, 0, "failed"),
            (30, 3, "interrupt"),
            (40, 0, "running"),
            (40, 3, "failed")
        ]
    );
    let completed = all
        .items
        .into_iter()
        .filter(|entry| matches!(entry, ThreadTimelineEntry::TurnCompleted { .. }))
        .collect::<Vec<_>>();
    let expected = [
        ("complete", 10, 20, "completed", serde_json::Value::Null),
        ("interrupt", 20, 30, "interrupted", serde_json::Value::Null),
        (
            "failed",
            30,
            40,
            "failed",
            serde_json::from_str(error).expect("error"),
        ),
    ]
    .into_iter()
    .map(|(id, start, end, status, error)| {
        serde_json::from_value(
            serde_json::json!({"type":"turnCompleted", "position":end, "turnId":id,
                "status":status, "error":error, "startedAt":start, "completedAt":end,
                "durationMs":10000}),
        )
        .expect("expected boundary")
    })
    .collect::<Vec<ThreadTimelineEntry>>();
    assert_eq!(completed, expected);
}

#[tokio::test]
async fn timeline_page_state_excludes_realtime_events_after_fork_cutoff() {
    let (home, store, child_id) = store_with_mode(ThreadHistoryMode::Paginated).await;
    let source_id = ThreadId::default();
    let source_path = rollout_path(home.path(), source_id);
    write_rollout_with_end(
        source_path.as_path(),
        source_id,
        /*history_base*/ None,
        /*next_ordinal*/ 5,
    );
    write_rollout_with_end(
        rollout_path(home.path(), child_id).as_path(),
        child_id,
        Some(history_position(
            source_path.as_path(),
            source_id,
            /*end_ordinal_exclusive*/ 3,
        )),
        /*next_ordinal*/ 2,
    );

    let db = history_db(&store).await;
    insert_turn(
        db,
        source_id,
        "source-turn",
        /*rollout_ordinal*/ 1,
        "completed",
        /*error_json*/ None,
        /*first_user_item_id*/ None,
        /*final_agent_item_id*/ None,
    )
    .await;
    sqlx::query("UPDATE thread_turns SET rollout_end_ordinal = 4 WHERE thread_id = ?")
        .bind(source_id.to_string())
        .execute(db)
        .await
        .expect("source turn end");
    insert_item(
        db,
        child_id,
        "child-turn",
        "child-item",
        /*rollout_ordinal*/ 4,
    )
    .await;
    for (ordinal, item) in [
        (
            1,
            RealtimeItem {
                id: "voice:started".to_string(),
                realtime_session_id: "voice".to_string(),
                content: RealtimeItemContent::RealtimeSessionStarted,
            },
        ),
        (
            3,
            RealtimeItem {
                id: "voice:closed".to_string(),
                realtime_session_id: "voice".to_string(),
                content: RealtimeItemContent::RealtimeSessionClosed {
                    outcome: RealtimeSessionOutcome::Ended,
                },
            },
        ),
    ] {
        let item_json = serde_json::to_string(&item).expect("serialize realtime item");
        sqlx::query(
            r#"
INSERT INTO thread_realtime_items (
    thread_id, item_id, rollout_ordinal, created_at_ms, item_type, item_json
) VALUES (?, ?, ?, ?, json_extract(?, '$.type'), ?)
            "#,
        )
        .bind(source_id.to_string())
        .bind(&item.id)
        .bind(ordinal)
        .bind(ordinal * 1_000)
        .bind(&item_json)
        .bind(&item_json)
        .execute(db)
        .await
        .expect("insert realtime boundary");
    }

    let page = store
        .list_timeline(ListTimelineParams {
            thread_id: child_id,
            cursor: None,
            page_size: 1,
        })
        .await
        .expect("list forked timeline page");

    assert_eq!(
        page.active_realtime_session_at_page_start,
        Some("voice".to_string())
    );
    let inherited = store
        .list_timeline(ListTimelineParams {
            thread_id: child_id,
            cursor: page.next_cursor,
            page_size: 10,
        })
        .await
        .expect("inherited timeline");
    assert_eq!(
        inherited
            .items
            .iter()
            .map(crate::local::thread_history::realtime::entry_key)
            .collect::<Vec<_>>(),
        vec![(1, 0, "source-turn"), (1, 2, "voice:started")]
    );
}

#[tokio::test]
async fn list_items_filters_exclusive_update_ordinals_across_pages_and_turns() {
    let (_home, store, thread_id) = store_with_mode(ThreadHistoryMode::Paginated).await;
    let db = history_db(&store).await;
    for (turn_id, item_id, ordinal) in [
        ("turn-1", "item-1", 1),
        ("turn-2", "item-2", 2),
        ("turn-2", "item-3", 3),
    ] {
        insert_item(db, thread_id, turn_id, item_id, ordinal).await;
    }
    sqlx::query(
        "UPDATE thread_items SET updated_at_ordinal = 4 WHERE thread_id = ? AND item_id = 'item-1'",
    )
    .bind(thread_id.to_string())
    .execute(db)
    .await
    .expect("advance first item update ordinal");

    let item_1 = StoredThreadItem {
        updated_at_ordinal: 4,
        ..expected_item("turn-1", "item-1", /*rollout_ordinal*/ 1)
    };
    let item_2 = expected_item("turn-2", "item-2", /*rollout_ordinal*/ 2);
    let item_3 = expected_item("turn-2", "item-3", /*rollout_ordinal*/ 3);
    let creation_page = store
        .list_items(item_params(
            thread_id,
            /*turn_id*/ None,
            /*cursor*/ None,
            /*page_size*/ 2,
            SortDirection::Asc,
        ))
        .await
        .expect("creation-ordered item page");
    assert_eq!(creation_page.items, vec![item_1.clone(), item_2.clone()]);
    for (sort_direction, expected) in [
        (SortDirection::Asc, vec![item_1.clone(), item_3.clone()]),
        (SortDirection::Desc, vec![item_3.clone(), item_1.clone()]),
    ] {
        let page = store
            .list_items(ListItemsParams {
                after_updated_at_ordinal: Some(2),
                ..item_params(
                    thread_id,
                    /*turn_id*/ None,
                    /*cursor*/ None,
                    /*page_size*/ 2,
                    sort_direction,
                )
            })
            .await
            .expect("creation-ordered filtered item page");
        assert_eq!(page.items, expected);
    }

    let first_page = store
        .list_items(updated_item_params(
            thread_id, /*after_updated_at_ordinal*/ 0,
        ))
        .await
        .expect("first filtered item page");
    assert_eq!(first_page.items, vec![item_2.clone(), item_3.clone()]);
    for params in [
        ListItemsParams {
            cursor: creation_page.next_cursor,
            ..updated_item_params(thread_id, /*after_updated_at_ordinal*/ 0)
        },
        item_params(
            thread_id,
            /*turn_id*/ None,
            first_page.next_cursor.clone(),
            /*page_size*/ 2,
            SortDirection::Asc,
        ),
    ] {
        let error = store
            .list_items(params)
            .await
            .expect_err("creation and update cursors should not be interchangeable");
        assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));
    }

    let second_page = store
        .list_items(ListItemsParams {
            cursor: first_page.next_cursor,
            ..updated_item_params(thread_id, /*after_updated_at_ordinal*/ 0)
        })
        .await
        .expect("second filtered item page");
    assert_eq!(second_page.items, vec![item_1.clone()]);
    assert!(second_page.next_cursor.is_none());

    let exclusive_page = store
        .list_items(ListItemsParams {
            turn_id: Some("turn-2".to_string()),
            ..updated_item_params(thread_id, /*after_updated_at_ordinal*/ 2)
        })
        .await
        .expect("exclusive filtered turn page");
    assert_eq!(exclusive_page.items, vec![item_3.clone()]);

    let descending_page = store
        .list_items(ListItemsParams {
            sort_direction: SortDirection::Desc,
            ..updated_item_params(thread_id, /*after_updated_at_ordinal*/ 0)
        })
        .await
        .expect("descending update-ordered item page");
    assert_eq!(descending_page.items, vec![item_1, item_3]);
    let descending_next_page = store
        .list_items(ListItemsParams {
            cursor: descending_page.next_cursor,
            sort_direction: SortDirection::Desc,
            ..updated_item_params(thread_id, /*after_updated_at_ordinal*/ 0)
        })
        .await
        .expect("next descending update-ordered item page");
    assert_eq!(descending_next_page.items, vec![item_2]);

    let error = store
        .list_items(ListItemsParams {
            sort_key: ItemSortKey::UpdatedAtOrdinal,
            ..item_params(
                thread_id,
                /*turn_id*/ None,
                /*cursor*/ None,
                /*page_size*/ 2,
                SortDirection::Asc,
            )
        })
        .await
        .expect_err("update-ordinal sorting should require a watermark");
    assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));
}

#[tokio::test]
async fn list_items_rejects_update_ordinals_outside_sqlite_integer_range() {
    let (_home, store, thread_id) = store_with_mode(ThreadHistoryMode::Paginated).await;

    for sort_key in [ItemSortKey::CreatedAtOrdinal, ItemSortKey::UpdatedAtOrdinal] {
        let error = store
            .list_items(ListItemsParams {
                sort_key,
                ..updated_item_params(thread_id, /*after_updated_at_ordinal*/ u64::MAX)
            })
            .await
            .expect_err("out-of-range SQLite update ordinal should fail");

        assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));
    }
}

#[tokio::test]
async fn list_items_update_ordinals_use_selected_rollout_id() {
    let (home, store, thread_id) = store_with_mode(ThreadHistoryMode::Paginated).await;
    let rollout_id = ThreadId::new();
    let selected_rollout_path = home.path().join(format!(
        "sessions/2026/07/16/rollout-2026-07-16T00-00-00-{thread_id}_{rollout_id}.jsonl"
    ));
    write_rollout(
        selected_rollout_path.as_path(),
        thread_id,
        /*history_base*/ None,
    );
    let state_db = store.state_db().await.expect("state runtime");
    let mut metadata = state_db
        .get_thread(thread_id)
        .await
        .expect("read metadata")
        .expect("thread metadata");
    metadata.rollout_path = selected_rollout_path;
    state_db
        .upsert_thread(&metadata)
        .await
        .expect("select replacement rollout");
    insert_item(
        history_db(&store).await,
        rollout_id,
        "turn-1",
        "item-1",
        /*rollout_ordinal*/ 1,
    )
    .await;

    let page = store
        .list_items(updated_item_params(
            thread_id, /*after_updated_at_ordinal*/ 0,
        ))
        .await
        .expect("update-ordered item page");

    assert_eq!(
        page.items,
        vec![expected_item(
            "turn-1", "item-1", /*rollout_ordinal*/ 1
        )]
    );
}

#[tokio::test]
async fn cursors_are_bound_to_the_selected_rollout_generation() {
    let (home, store, thread_id) = store_with_mode(ThreadHistoryMode::Paginated).await;
    let first_rollout_id = ThreadId::new();
    let second_rollout_id = ThreadId::new();
    let first_path = selected_rollout_path(home.path(), thread_id, first_rollout_id);
    let second_path = selected_rollout_path(home.path(), thread_id, second_rollout_id);
    write_rollout(first_path.as_path(), thread_id, /*history_base*/ None);
    write_rollout(second_path.as_path(), thread_id, /*history_base*/ None);
    select_rollout_path(&store, thread_id, first_path).await;

    let db = history_db(&store).await;
    for (rollout_id, prefix) in [(first_rollout_id, "first"), (second_rollout_id, "second")] {
        for (index, ordinal) in [(1, 10), (2, 20)] {
            let turn_id = format!("{prefix}-turn-{index}");
            let item_id = format!("{prefix}-item-{index}");
            insert_turn(
                db,
                rollout_id,
                turn_id.as_str(),
                ordinal,
                "completed",
                /*error_json*/ None,
                Some(item_id.as_str()),
                /*final_agent_item_id*/ None,
            )
            .await;
            insert_item(
                db,
                rollout_id,
                turn_id.as_str(),
                item_id.as_str(),
                ordinal + 1,
            )
            .await;
        }
    }

    let first_turn_page = store
        .list_turns(turn_params(
            thread_id,
            /*cursor*/ None,
            /*page_size*/ 1,
            SortDirection::Asc,
            StoredTurnItemsView::NotLoaded,
        ))
        .await
        .expect("first-generation turn page");
    let first_item_page = store
        .list_items(item_params(
            thread_id,
            /*turn_id*/ None,
            /*cursor*/ None,
            /*page_size*/ 1,
            SortDirection::Asc,
        ))
        .await
        .expect("first-generation item page");
    let first_updated_item_page = store
        .list_items(ListItemsParams {
            page_size: 1,
            ..updated_item_params(thread_id, /*after_updated_at_ordinal*/ 0)
        })
        .await
        .expect("first-generation updated-item page");
    let first_search_page = store
        .search_thread_occurrences(SearchThreadOccurrencesParams {
            thread_id,
            search_term: "item".to_string(),
            cursor: None,
            page_size: 1,
        })
        .await
        .expect("first-generation search page");

    let first_turn_next = first_turn_page
        .next_cursor
        .clone()
        .expect("first turn page should continue");
    let decoded: HistoryCursor =
        serde_json::from_str(first_turn_next.as_str()).expect("decode generated history cursor");
    assert_eq!(decoded.requested_thread_id, thread_id);
    assert_eq!(decoded.root_rollout_id, first_rollout_id);
    let first_search_next = first_search_page
        .next_cursor
        .clone()
        .expect("first search page should continue");
    let search_cursor_json = serde_json::from_str::<serde_json::Value>(first_search_next.as_str())
        .expect("decode generated search cursor");
    assert_eq!(
        search_cursor_json
            .get("threadId")
            .and_then(|value| value.as_str()),
        Some(thread_id.to_string().as_str())
    );
    assert_eq!(
        search_cursor_json
            .get("rootRolloutId")
            .and_then(|value| value.as_str()),
        Some(first_rollout_id.to_string().as_str())
    );

    // Appending under one selected rollout does not change its cursor generation.
    insert_turn(
        db,
        first_rollout_id,
        "first-turn-3",
        /*rollout_ordinal*/ 30,
        "completed",
        /*error_json*/ None,
        /*first_user_item_id*/ None,
        /*final_agent_item_id*/ None,
    )
    .await;
    let continued_first_generation = store
        .list_turns(turn_params(
            thread_id,
            Some(first_turn_next.clone()),
            /*page_size*/ 3,
            SortDirection::Asc,
            StoredTurnItemsView::NotLoaded,
        ))
        .await
        .expect("same-generation cursor should survive append");
    assert_eq!(
        turn_ids(&continued_first_generation),
        vec!["first-turn-2", "first-turn-3"]
    );

    let mut history_cursors = vec![
        first_turn_next,
        first_turn_page
            .backwards_cursor
            .expect("turn backwards cursor"),
        first_item_page.next_cursor.expect("item next cursor"),
        first_item_page
            .backwards_cursor
            .expect("item backwards cursor"),
        first_updated_item_page
            .next_cursor
            .expect("updated-item next cursor"),
        first_updated_item_page
            .backwards_cursor
            .expect("updated-item backwards cursor"),
        first_search_page.items[0].turn_cursor.clone(),
    ];
    let missing_generation_cursor = {
        let mut value = serde_json::to_value(&decoded).expect("encode history cursor");
        value
            .as_object_mut()
            .expect("history cursor object")
            .remove("rootRolloutId");
        serde_json::to_string(&value).expect("encode pre-generation cursor")
    };
    let error = store
        .list_turns(turn_params(
            thread_id,
            Some(missing_generation_cursor),
            /*page_size*/ 1,
            SortDirection::Asc,
            StoredTurnItemsView::NotLoaded,
        ))
        .await
        .expect_err("cursor without rollout generation must fail closed");
    assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));
    let missing_search_generation_cursor = {
        let mut value = search_cursor_json;
        value
            .as_object_mut()
            .expect("search cursor object")
            .remove("rootRolloutId");
        serde_json::to_string(&value).expect("encode pre-generation search cursor")
    };
    let error = store
        .search_thread_occurrences(SearchThreadOccurrencesParams {
            thread_id,
            search_term: "item".to_string(),
            cursor: Some(missing_search_generation_cursor),
            page_size: 1,
        })
        .await
        .expect_err("search cursor without rollout generation must fail closed");
    assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));

    select_rollout_path(&store, thread_id, second_path).await;

    for cursor in history_cursors.drain(..2) {
        let error = store
            .list_turns(turn_params(
                thread_id,
                Some(cursor),
                /*page_size*/ 1,
                SortDirection::Asc,
                StoredTurnItemsView::NotLoaded,
            ))
            .await
            .expect_err("turn cursor from replaced rollout must fail");
        assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));
    }
    for cursor in history_cursors.drain(..2) {
        let error = store
            .list_items(item_params(
                thread_id,
                /*turn_id*/ None,
                Some(cursor),
                /*page_size*/ 1,
                SortDirection::Asc,
            ))
            .await
            .expect_err("item cursor from replaced rollout must fail");
        assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));
    }
    for cursor in history_cursors.drain(..2) {
        let error = store
            .list_items(ListItemsParams {
                cursor: Some(cursor),
                page_size: 1,
                ..updated_item_params(thread_id, /*after_updated_at_ordinal*/ 0)
            })
            .await
            .expect_err("updated-item cursor from replaced rollout must fail");
        assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));
    }
    let error = store
        .list_turns(turn_params(
            thread_id,
            history_cursors.pop(),
            /*page_size*/ 1,
            SortDirection::Asc,
            StoredTurnItemsView::NotLoaded,
        ))
        .await
        .expect_err("search turn cursor from replaced rollout must fail");
    assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));
    let error = store
        .search_thread_occurrences(SearchThreadOccurrencesParams {
            thread_id,
            search_term: "item".to_string(),
            cursor: Some(first_search_next),
            page_size: 1,
        })
        .await
        .expect_err("search cursor from replaced rollout must fail");
    assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));
    let second_turn_page = store
        .list_turns(turn_params(
            thread_id,
            /*cursor*/ None,
            /*page_size*/ 1,
            SortDirection::Asc,
            StoredTurnItemsView::NotLoaded,
        ))
        .await
        .expect("fresh second-generation cursor");
    assert_eq!(turn_ids(&second_turn_page), vec!["second-turn-1"]);
    let second_cursor: HistoryCursor = serde_json::from_str(
        second_turn_page
            .next_cursor
            .as_deref()
            .expect("second generation should continue"),
    )
    .expect("decode second-generation cursor");
    assert_eq!(second_cursor.requested_thread_id, thread_id);
    assert_eq!(second_cursor.root_rollout_id, second_rollout_id);
}

#[tokio::test]
async fn list_history_keeps_legacy_threads_unsupported() {
    let (_home, store, thread_id) = store_with_mode(ThreadHistoryMode::Legacy).await;

    let error = store
        .list_turns(turn_params(
            thread_id,
            /*cursor*/ None,
            /*page_size*/ 1,
            SortDirection::Asc,
            StoredTurnItemsView::Summary,
        ))
        .await
        .expect_err("legacy turns remain unsupported");
    assert!(matches!(
        error,
        ThreadStoreError::Unsupported {
            operation: "list_turns"
        }
    ));

    let error = store
        .list_turns(turn_params(
            ThreadId::default(),
            /*cursor*/ None,
            /*page_size*/ 1,
            SortDirection::Asc,
            StoredTurnItemsView::Summary,
        ))
        .await
        .expect_err("unindexed threads remain unsupported");
    assert!(matches!(
        error,
        ThreadStoreError::Unsupported {
            operation: "list_turns"
        }
    ));
}

#[tokio::test]
async fn segmented_legacy_reads_without_projection_return_none() {
    let (_home, store, thread_id) = store_with_mode(ThreadHistoryMode::Legacy).await;

    let turns = list_segmented_legacy_turns(
        &store,
        turn_params(
            thread_id,
            /*cursor*/ None,
            /*page_size*/ 2,
            SortDirection::Desc,
            StoredTurnItemsView::Summary,
        ),
    )
    .await
    .expect("check for existing legacy turn projection");
    assert!(turns.is_none());

    let items = list_segmented_legacy_items(
        &store,
        item_params(
            thread_id,
            /*turn_id*/ None,
            /*cursor*/ None,
            /*page_size*/ 2,
            SortDirection::Desc,
        ),
    )
    .await
    .expect("check for existing legacy item projection");
    assert!(items.is_none());
    assert!(
        !tokio::fs::try_exists(store.config.sqlite.thread_history_db_path())
            .await
            .expect("inspect history database path"),
        "checking for a legacy projection must not create a history database"
    );
}

#[tokio::test]
async fn segmented_legacy_projection_preserves_legacy_turn_cursors() {
    let (home, store, thread_id) = store_with_mode(ThreadHistoryMode::Legacy).await;
    let rollout_len = write_segmented_legacy_rollout(home.path(), thread_id);
    let db = history_db(&store).await;
    for index in 1_i64..=5 {
        let turn_id = format!("turn-{index}");
        let user_id = format!("user-{index}");
        let agent_id = format!("agent-{index}");
        let ordinal = index * 10;
        insert_turn(
            db,
            thread_id,
            turn_id.as_str(),
            ordinal,
            "completed",
            /*error_json*/ None,
            Some(user_id.as_str()),
            Some(agent_id.as_str()),
        )
        .await;
        insert_item(
            db,
            thread_id,
            turn_id.as_str(),
            user_id.as_str(),
            ordinal + 1,
        )
        .await;
        insert_item(
            db,
            thread_id,
            turn_id.as_str(),
            agent_id.as_str(),
            ordinal + 2,
        )
        .await;
    }
    insert_turn(
        db,
        thread_id,
        "rollout-45",
        /*rollout_ordinal*/ 45,
        "completed",
        /*error_json*/ None,
        /*first_user_item_id*/ None,
        /*final_agent_item_id*/ None,
    )
    .await;
    sqlx::query(
        "INSERT INTO thread_history_projection_state (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, ?, ?)",
    )
    .bind(thread_id.to_string())
    .bind(i64::try_from(rollout_len).expect("rollout length fits SQLite integer"))
    .bind(100_i64)
    .execute(db)
    .await
    .expect("publish legacy projection state after its rows");

    let first_page = list_segmented_legacy_turns(
        &store,
        turn_params(
            thread_id,
            /*cursor*/ None,
            /*page_size*/ 2,
            SortDirection::Desc,
            StoredTurnItemsView::Summary,
        ),
    )
    .await
    .expect("read indexed legacy first page")
    .expect("legacy projection exists");
    assert_eq!(turn_ids(&first_page), vec!["turn-5", "turn-4"]);
    assert_eq!(
        first_page.turns[0].items,
        vec![
            expected_item("turn-5", "user-5", /*rollout_ordinal*/ 51),
            expected_item("turn-5", "agent-5", /*rollout_ordinal*/ 52),
        ]
    );

    let next_cursor = first_page
        .next_cursor
        .expect("indexed first page has older turns");
    let cursor = serde_json::from_str::<serde_json::Value>(&next_cursor)
        .expect("parse public legacy turn cursor");
    let fields = cursor.as_object().expect("legacy cursor is an object");
    assert_eq!(fields.len(), 2);
    assert!(fields.contains_key("turnId"));
    assert!(fields.contains_key("includeAnchor"));

    let second_page = list_segmented_legacy_turns(
        &store,
        turn_params(
            thread_id,
            Some(next_cursor),
            /*page_size*/ 2,
            SortDirection::Desc,
            StoredTurnItemsView::NotLoaded,
        ),
    )
    .await
    .expect("read indexed legacy second page")
    .expect("legacy projection exists");
    assert_eq!(turn_ids(&second_page), vec!["turn-3", "turn-2"]);
    assert!(second_page.turns[0].items.is_empty());

    let backwards_page = list_segmented_legacy_turns(
        &store,
        turn_params(
            thread_id,
            second_page.backwards_cursor,
            /*page_size*/ 2,
            SortDirection::Asc,
            StoredTurnItemsView::NotLoaded,
        ),
    )
    .await
    .expect("read indexed legacy backwards page")
    .expect("legacy projection exists");
    assert_eq!(turn_ids(&backwards_page), vec!["turn-3", "turn-4"]);
}

#[tokio::test]
async fn segmented_legacy_projection_pages_full_turn_items() {
    let (home, store, thread_id) = store_with_mode(ThreadHistoryMode::Legacy).await;
    let rollout_len = write_segmented_legacy_rollout(home.path(), thread_id);
    let db = history_db(&store).await;
    insert_turn(
        db,
        thread_id,
        "turn-1",
        /*rollout_ordinal*/ 10,
        "completed",
        /*error_json*/ None,
        /*first_user_item_id*/ None,
        /*final_agent_item_id*/ None,
    )
    .await;
    for (item_id, ordinal) in [("item-1", 11), ("item-2", 12), ("item-3", 13)] {
        insert_item(db, thread_id, "turn-1", item_id, ordinal).await;
    }
    sqlx::query(
        "INSERT INTO thread_history_projection_state (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, ?, ?)",
    )
    .bind(thread_id.to_string())
    .bind(i64::try_from(rollout_len).expect("rollout length fits SQLite integer"))
    .bind(100_i64)
    .execute(db)
    .await
    .expect("publish legacy projection state after its rows");

    let first_page = list_segmented_legacy_items(
        &store,
        item_params(
            thread_id,
            Some("turn-1"),
            /*cursor*/ None,
            /*page_size*/ 2,
            SortDirection::Asc,
        ),
    )
    .await
    .expect("read indexed legacy turn items")
    .expect("legacy projection exists");
    assert_eq!(item_ids(&first_page), vec!["item-1", "item-2"]);

    let second_page = list_segmented_legacy_items(
        &store,
        item_params(
            thread_id,
            Some("turn-1"),
            first_page.next_cursor,
            /*page_size*/ 2,
            SortDirection::Asc,
        ),
    )
    .await
    .expect("read indexed legacy remaining turn items")
    .expect("legacy projection exists");
    assert_eq!(item_ids(&second_page), vec!["item-3"]);
}

#[tokio::test]
async fn segmented_legacy_summary_uses_last_agent_message_after_final_answer() {
    let (home, store, thread_id) = store_with_mode(ThreadHistoryMode::Legacy).await;
    let rollout_len = write_segmented_legacy_rollout(home.path(), thread_id);
    let db = history_db(&store).await;
    insert_turn(
        db,
        thread_id,
        "turn-1",
        /*rollout_ordinal*/ 10,
        "completed",
        /*error_json*/ None,
        Some("user-1"),
        Some("agent-final"),
    )
    .await;
    for (item_id, ordinal) in [
        ("user-1", 11),
        ("agent-final", 12),
        ("agent-commentary", 13),
    ] {
        insert_item(db, thread_id, "turn-1", item_id, ordinal).await;
    }
    sqlx::query(
        "INSERT INTO thread_history_projection_state (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, ?, ?)",
    )
    .bind(thread_id.to_string())
    .bind(i64::try_from(rollout_len).expect("rollout length fits SQLite integer"))
    .bind(100_i64)
    .execute(db)
    .await
    .expect("publish current legacy projection state after its rows");

    let page = list_segmented_legacy_turns(
        &store,
        turn_params(
            thread_id,
            /*cursor*/ None,
            /*page_size*/ 1,
            SortDirection::Desc,
            StoredTurnItemsView::Summary,
        ),
    )
    .await
    .expect("read completed legacy turn summary")
    .expect("current legacy projection exists");
    let summary_ids = page.turns[0]
        .items
        .iter()
        .map(|item| item.item_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(summary_ids, vec!["user-1", "agent-commentary"]);
}

#[tokio::test]
async fn segmented_legacy_reads_reject_stale_projection_offsets() {
    let (home, store, thread_id) = store_with_mode(ThreadHistoryMode::Legacy).await;
    write_segmented_legacy_rollout(home.path(), thread_id);
    let rollout_len = append_segmented_legacy_turn(home.path(), thread_id, "turn-1");
    let db = history_db(&store).await;
    sqlx::query(
        "INSERT INTO thread_history_projection_state (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, ?, ?)",
    )
    .bind(thread_id.to_string())
    .bind(i64::try_from(rollout_len - 1).expect("rollout length fits SQLite integer"))
    .bind(100_i64)
    .execute(db)
    .await
    .expect("insert stale legacy projection state");
    insert_turn(
        db,
        thread_id,
        "turn-1",
        /*rollout_ordinal*/ 10,
        "completed",
        /*error_json*/ None,
        /*first_user_item_id*/ None,
        /*final_agent_item_id*/ None,
    )
    .await;

    let turns = list_existing_segmented_legacy_turns(
        &store,
        turn_params(
            thread_id,
            /*cursor*/ None,
            /*page_size*/ 1,
            SortDirection::Desc,
            StoredTurnItemsView::Summary,
        ),
    )
    .await
    .expect("inspect stale legacy projection");
    assert!(
        turns.is_none(),
        "a projection behind the active rollout must use canonical history"
    );

    let turns = list_segmented_legacy_turns(
        &store,
        turn_params(
            thread_id,
            /*cursor*/ None,
            /*page_size*/ 1,
            SortDirection::Desc,
            StoredTurnItemsView::Summary,
        ),
    )
    .await
    .expect("recover stale legacy projection")
    .expect("recovered legacy projection exists");
    assert_eq!(turn_ids(&turns), vec!["turn-1"]);
    let projected_offset = sqlx::query_scalar::<_, i64>(
        "SELECT next_rollout_byte_offset FROM thread_history_projection_state WHERE thread_id = ?",
    )
    .bind(thread_id.to_string())
    .fetch_one(db)
    .await
    .expect("read recovered legacy projection state");
    assert_eq!(projected_offset, i64::try_from(rollout_len).unwrap());
}

#[tokio::test]
async fn segmented_legacy_reads_reject_interrupted_backfill_until_projection_is_current() {
    let (home, store, thread_id) = store_with_mode(ThreadHistoryMode::Legacy).await;
    write_segmented_legacy_rollout(home.path(), thread_id);
    let rollout_len = append_segmented_legacy_turn(home.path(), thread_id, "turn-1");
    let db = history_db(&store).await;
    sqlx::query(
        "INSERT INTO thread_history_projection_state (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, ?, ?)",
    )
    .bind(thread_id.to_string())
    .bind(i64::MAX)
    .bind(100_i64)
    .execute(db)
    .await
    .expect("insert interrupted legacy projection state");
    insert_turn(
        db,
        thread_id,
        "turn-1",
        /*rollout_ordinal*/ 10,
        "completed",
        /*error_json*/ None,
        Some("item-1"),
        /*final_agent_item_id*/ None,
    )
    .await;
    insert_item(
        db, thread_id, "turn-1", "item-1", /*rollout_ordinal*/ 11,
    )
    .await;

    let turns = list_existing_segmented_legacy_turns(
        &store,
        turn_params(
            thread_id,
            /*cursor*/ None,
            /*page_size*/ 1,
            SortDirection::Desc,
            StoredTurnItemsView::Summary,
        ),
    )
    .await
    .expect("inspect interrupted legacy projection");
    assert!(
        turns.is_none(),
        "interrupted backfill must not expose partial turns"
    );

    let turns = list_segmented_legacy_turns(
        &store,
        turn_params(
            thread_id,
            /*cursor*/ None,
            /*page_size*/ 1,
            SortDirection::Desc,
            StoredTurnItemsView::Summary,
        ),
    )
    .await
    .expect("read completed legacy projection")
    .expect("completed projection must expose indexed turns");
    assert_eq!(turn_ids(&turns), vec!["turn-1"]);

    let items = list_segmented_legacy_items(
        &store,
        item_params(
            thread_id,
            Some("turn-1"),
            /*cursor*/ None,
            /*page_size*/ 1,
            SortDirection::Asc,
        ),
    )
    .await
    .expect("read recovered legacy item projection")
    .expect("recovered projection must expose indexed items");
    assert_eq!(item_ids(&items), vec!["item-1"]);

    let projected_offset = sqlx::query_scalar::<_, i64>(
        "SELECT next_rollout_byte_offset FROM thread_history_projection_state WHERE thread_id = ?",
    )
    .bind(thread_id.to_string())
    .fetch_one(db)
    .await
    .expect("read recovered legacy projection state");
    assert_eq!(projected_offset, i64::try_from(rollout_len).unwrap());
}

#[tokio::test]
async fn segmented_legacy_reads_accept_historical_fork_and_parent_metadata() {
    for (forked_from_id, parent_thread_id) in [
        (Some(ThreadId::new()), None),
        (None, Some(ThreadId::new())),
        (Some(ThreadId::new()), Some(ThreadId::new())),
    ] {
        let (home, store, thread_id) = store_with_mode(ThreadHistoryMode::Legacy).await;
        let rollout_len = write_segmented_legacy_rollout_with_origin(
            home.path(),
            thread_id,
            /*history_base*/ None,
            forked_from_id,
            parent_thread_id,
        );
        let db = history_db(&store).await;
        insert_turn(
            db,
            thread_id,
            "turn-1",
            /*rollout_ordinal*/ 10,
            "completed",
            /*error_json*/ None,
            Some("item-1"),
            /*final_agent_item_id*/ None,
        )
        .await;
        insert_item(
            db, thread_id, "turn-1", "item-1", /*rollout_ordinal*/ 11,
        )
        .await;
        sqlx::query(
            "INSERT INTO thread_history_projection_state (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, ?, ?)",
        )
        .bind(thread_id.to_string())
        .bind(i64::try_from(rollout_len).expect("rollout length fits SQLite integer"))
        .bind(100_i64)
        .execute(db)
        .await
        .expect("publish complete same-thread legacy projection after its rows");

        let turns = list_segmented_legacy_turns(
            &store,
            turn_params(
                thread_id,
                /*cursor*/ None,
                /*page_size*/ 1,
                SortDirection::Desc,
                StoredTurnItemsView::Summary,
            ),
        )
        .await
        .expect("read indexed history for a historically forked same-thread rollout")
        .expect("historical origin metadata must not exclude same-thread history");
        assert_eq!(turn_ids(&turns), vec!["turn-1"]);

        let items = list_segmented_legacy_items(
            &store,
            item_params(
                thread_id,
                Some("turn-1"),
                /*cursor*/ None,
                /*page_size*/ 1,
                SortDirection::Asc,
            ),
        )
        .await
        .expect("read items for a historically forked same-thread rollout")
        .expect("historical origin metadata must not exclude same-thread items");
        assert_eq!(item_ids(&items), vec!["item-1"]);
    }
}

#[tokio::test]
async fn segmented_legacy_index_preserves_implicit_compaction_only_turn() {
    let (home, store, thread_id) = store_with_mode(ThreadHistoryMode::Legacy).await;
    write_segmented_legacy_rollout(home.path(), thread_id);
    let path = rollout_path(home.path(), thread_id);
    let session_meta = codex_rollout::read_session_meta_line(path.as_path())
        .await
        .expect("read canonical legacy session metadata");
    let compacted = RolloutItem::Compacted(CompactedItem {
        message: String::new(),
        replacement_history: None,
        mcp_resource_origins: None,
        window_number: None,
        first_window_id: None,
        previous_window_id: None,
        window_id: None,
        segment_state_checkpoint: None,
    });
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(path.as_path())
        .expect("open segmented legacy rollout");
    writeln!(
        file,
        "{}",
        serde_json::to_string(&RolloutLine {
            timestamp: "2026-07-16T00:00:00.000Z".to_string(),
            ordinal: None,
            item: compacted.clone(),
        })
        .expect("serialize legacy compaction marker")
    )
    .expect("append legacy compaction marker");
    file.flush().expect("flush legacy compaction marker");

    let mut canonical = ThreadHistoryBuilder::new();
    canonical.handle_rollout_item_with_changes(&RolloutItem::SessionMeta(session_meta));
    canonical.handle_rollout_item_with_changes(&compacted);
    let canonical_turns = canonical.finish();
    assert_eq!(canonical_turns.len(), 1);
    assert_eq!(canonical_turns[0].id, "rollout-1");
    assert!(canonical_turns[0].items.is_empty());

    let indexed_turns = list_segmented_legacy_turns(
        &store,
        turn_params(
            thread_id,
            /*cursor*/ None,
            /*page_size*/ 1,
            SortDirection::Desc,
            StoredTurnItemsView::Summary,
        ),
    )
    .await
    .expect("read indexed compaction-only legacy history")
    .expect("compaction-only legacy history must have a complete projection");
    assert_eq!(
        turn_ids(&indexed_turns),
        vec![canonical_turns[0].id.as_str()]
    );
    assert!(indexed_turns.turns[0].items.is_empty());
}

#[tokio::test]
async fn segmented_legacy_reads_reject_active_cross_thread_rollout_references() {
    let (home, store, child_id) = store_with_mode(ThreadHistoryMode::Legacy).await;
    let parent_id = ThreadId::new();
    write_segmented_legacy_rollout_with_origin(
        home.path(),
        child_id,
        /*history_base*/ None,
        Some(parent_id),
        /*parent_thread_id*/ None,
    );
    let child_path = rollout_path(home.path(), child_id);
    let reference = RolloutLine {
        timestamp: "2026-07-16T00:00:01.000Z".to_string(),
        ordinal: None,
        item: RolloutItem::RolloutReference(RolloutReferenceItem {
            rollout_id: Some(parent_id),
            rollout_path: rollout_path(home.path(), parent_id),
            thread_id: Some(parent_id),
            rollout_timestamp: None,
            segment_id: None,
            max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        }),
    };
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(child_path.as_path())
        .expect("open forked legacy rollout");
    writeln!(
        file,
        "{}",
        serde_json::to_string(&reference).expect("serialize inherited rollout reference")
    )
    .expect("append inherited rollout reference");
    file.flush().expect("flush inherited rollout reference");
    let rollout_len = fs::metadata(child_path.as_path())
        .expect("read forked legacy rollout length")
        .len();

    let db = history_db(&store).await;
    sqlx::query(
        "INSERT INTO thread_history_projection_state (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, ?, ?)",
    )
    .bind(child_id.to_string())
    .bind(i64::try_from(rollout_len).expect("rollout length fits SQLite integer"))
    .bind(100_i64)
    .execute(db)
    .await
    .expect("insert complete child projection state");
    insert_turn(
        db,
        parent_id,
        "parent-turn",
        /*rollout_ordinal*/ 1,
        "completed",
        /*error_json*/ None,
        Some("parent-item"),
        /*final_agent_item_id*/ None,
    )
    .await;
    insert_item(
        db,
        parent_id,
        "parent-turn",
        "parent-item",
        /*rollout_ordinal*/ 2,
    )
    .await;

    let turns = list_existing_segmented_legacy_turns(
        &store,
        turn_params(
            child_id,
            /*cursor*/ None,
            /*page_size*/ 1,
            SortDirection::Desc,
            StoredTurnItemsView::Summary,
        ),
    )
    .await
    .expect("inspect inherited legacy turn projection");
    assert!(
        turns.is_none(),
        "child-only rows cannot replace an inherited parent rollout"
    );

    let items = list_segmented_legacy_items(
        &store,
        item_params(
            child_id,
            /*turn_id*/ None,
            /*cursor*/ None,
            /*page_size*/ 1,
            SortDirection::Desc,
        ),
    )
    .await
    .expect("inspect inherited legacy item projection");
    assert!(items.is_none(), "inherited items remain parent-owned");

    let owners = sqlx::query_scalar::<_, String>(
        "SELECT thread_id FROM thread_items WHERE item_id = ? ORDER BY thread_id",
    )
    .bind("parent-item")
    .fetch_all(db)
    .await
    .expect("inspect inherited item ownership");
    assert_eq!(owners, vec![parent_id.to_string()]);
}

#[tokio::test]
async fn segmented_legacy_reads_reject_cross_thread_history_bases() {
    let (home, store, thread_id) = store_with_mode(ThreadHistoryMode::Legacy).await;
    let parent_id = ThreadId::new();
    let rollout_len = write_segmented_legacy_rollout_with_history_base(
        home.path(),
        thread_id,
        Some(HistoryPosition {
            thread_id: parent_id,
            end_ordinal_exclusive: 5,
            end_byte_offset: 128,
        }),
    );
    let db = history_db(&store).await;
    insert_turn(
        db,
        parent_id,
        "parent-turn",
        /*rollout_ordinal*/ 1,
        "completed",
        /*error_json*/ None,
        Some("parent-item"),
        /*final_agent_item_id*/ None,
    )
    .await;
    insert_item(
        db,
        parent_id,
        "parent-turn",
        "parent-item",
        /*rollout_ordinal*/ 2,
    )
    .await;
    sqlx::query(
        "INSERT INTO thread_history_projection_state (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, ?, ?)",
    )
    .bind(thread_id.to_string())
    .bind(i64::try_from(rollout_len).expect("rollout length fits SQLite integer"))
    .bind(100_i64)
    .execute(db)
    .await
    .expect("insert cross-thread legacy projection state");
    insert_turn(
        db,
        thread_id,
        "child-turn",
        /*rollout_ordinal*/ 10,
        "completed",
        /*error_json*/ None,
        /*first_user_item_id*/ None,
        /*final_agent_item_id*/ None,
    )
    .await;

    let turns = list_segmented_legacy_turns(
        &store,
        turn_params(
            thread_id,
            /*cursor*/ None,
            /*page_size*/ 1,
            SortDirection::Desc,
            StoredTurnItemsView::Summary,
        ),
    )
    .await
    .expect("inspect inherited legacy projection");
    assert!(
        turns.is_none(),
        "child-only projected rows cannot replace inherited parent history"
    );

    let inherited_item_owners = sqlx::query_scalar::<_, String>(
        "SELECT thread_id FROM thread_items WHERE item_id = ? ORDER BY thread_id",
    )
    .bind("parent-item")
    .fetch_all(db)
    .await
    .expect("inspect physical ownership of inherited items");
    assert_eq!(
        inherited_item_owners,
        vec![parent_id.to_string()],
        "inherited parent items must not be copied into the child projection"
    );
}

#[tokio::test]
async fn segmented_legacy_reads_reject_same_thread_history_bases() {
    let (home, store, thread_id) = store_with_mode(ThreadHistoryMode::Legacy).await;
    let rollout_len = write_segmented_legacy_rollout_with_history_base(
        home.path(),
        thread_id,
        Some(HistoryPosition {
            thread_id,
            end_ordinal_exclusive: 5,
            end_byte_offset: 128,
        }),
    );
    let db = history_db(&store).await;
    sqlx::query(
        "INSERT INTO thread_history_projection_state (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, ?, ?)",
    )
    .bind(thread_id.to_string())
    .bind(i64::try_from(rollout_len).expect("rollout length fits SQLite integer"))
    .bind(100_i64)
    .execute(db)
    .await
    .expect("insert same-thread inherited legacy projection state");
    insert_turn(
        db,
        thread_id,
        "child-turn",
        /*rollout_ordinal*/ 10,
        "completed",
        /*error_json*/ None,
        /*first_user_item_id*/ None,
        /*final_agent_item_id*/ None,
    )
    .await;

    let turns = list_segmented_legacy_turns(
        &store,
        turn_params(
            thread_id,
            /*cursor*/ None,
            /*page_size*/ 1,
            SortDirection::Desc,
            StoredTurnItemsView::Summary,
        ),
    )
    .await
    .expect("inspect same-thread inherited legacy projection");
    assert!(
        turns.is_none(),
        "a frozen history-base cutoff cannot be replaced by unfiltered indexed rows"
    );
}

#[tokio::test]
async fn lineage_reads_page_across_parent_and_child_segments() {
    let (home, store, child_id) = store_with_mode(ThreadHistoryMode::Paginated).await;
    let root_id = ThreadId::default();
    let root_path = rollout_path(home.path(), root_id);
    write_rollout_with_end(
        root_path.as_path(),
        root_id,
        /*history_base*/ None,
        /*next_ordinal*/ 8,
    );
    write_rollout_with_end(
        rollout_path(home.path(), child_id).as_path(),
        child_id,
        Some(history_position(
            root_path.as_path(),
            root_id,
            /*end_ordinal_exclusive*/ 6,
        )),
        /*next_ordinal*/ 3,
    );
    let db = history_db(&store).await;
    for (thread_id, turn_id, ordinal, first_user, final_agent) in [
        (root_id, "root-1", 1, Some("root-user"), Some("root-agent")),
        (root_id, "root-2", 4, None, None),
        (root_id, "excluded-root", 6, None, None),
        (child_id, "child-1", 7, None, None),
    ] {
        insert_turn(
            db,
            thread_id,
            turn_id,
            ordinal,
            "completed",
            /*error_json*/ None,
            first_user,
            final_agent,
        )
        .await;
    }
    for (thread_id, turn_id, item_id, ordinal) in [
        (root_id, "root-1", "root-user", 2),
        (root_id, "root-1", "root-agent", 3),
        (root_id, "root-2", "root-2-item", 5),
        (root_id, "excluded-root", "excluded-item", 7),
        (child_id, "child-1", "child-item", 8),
    ] {
        insert_item(db, thread_id, turn_id, item_id, ordinal).await;
    }

    let first_turns = store
        .list_turns(turn_params(
            child_id,
            /*cursor*/ None,
            /*page_size*/ 2,
            SortDirection::Asc,
            StoredTurnItemsView::Summary,
        ))
        .await
        .expect("first lineage turns page");
    assert_eq!(turn_ids(&first_turns), vec!["root-1", "root-2"]);
    assert_eq!(
        first_turns.turns[0].items,
        vec![
            expected_item("root-1", "root-user", /*rollout_ordinal*/ 2),
            expected_item("root-1", "root-agent", /*rollout_ordinal*/ 3),
        ]
    );
    let second_turns = store
        .list_turns(turn_params(
            child_id,
            first_turns.next_cursor,
            /*page_size*/ 2,
            SortDirection::Asc,
            StoredTurnItemsView::NotLoaded,
        ))
        .await
        .expect("second lineage turns page");
    assert_eq!(turn_ids(&second_turns), vec!["child-1"]);
    let backwards_turns = store
        .list_turns(turn_params(
            child_id,
            second_turns.backwards_cursor,
            /*page_size*/ 2,
            SortDirection::Desc,
            StoredTurnItemsView::NotLoaded,
        ))
        .await
        .expect("backwards lineage turns page");
    assert_eq!(turn_ids(&backwards_turns), vec!["child-1", "root-2"]);

    let first_items = store
        .list_items(item_params(
            child_id,
            /*turn_id*/ None,
            /*cursor*/ None,
            /*page_size*/ 2,
            SortDirection::Asc,
        ))
        .await
        .expect("first lineage items page");
    assert_eq!(item_ids(&first_items), vec!["root-user", "root-agent"]);
    let second_items = store
        .list_items(item_params(
            child_id,
            /*turn_id*/ None,
            first_items.next_cursor,
            /*page_size*/ 2,
            SortDirection::Asc,
        ))
        .await
        .expect("second lineage items page");
    assert_eq!(item_ids(&second_items), vec!["root-2-item", "child-item"]);
    let descending_items = store
        .list_items(item_params(
            child_id,
            /*turn_id*/ None,
            /*cursor*/ None,
            /*page_size*/ 2,
            SortDirection::Desc,
        ))
        .await
        .expect("descending lineage items page");
    assert_eq!(
        item_ids(&descending_items),
        vec!["child-item", "root-2-item"]
    );
    let inherited_turn_items = store
        .list_items(item_params(
            child_id,
            Some("root-1"),
            /*cursor*/ None,
            /*page_size*/ 2,
            SortDirection::Asc,
        ))
        .await
        .expect("inherited turn item page");
    assert_eq!(
        item_ids(&inherited_turn_items),
        vec!["root-user", "root-agent"]
    );

    let owner_counts = sqlx::query_as::<_, (i64, i64, i64, i64)>(
        "SELECT (SELECT COUNT(*) FROM thread_turns WHERE thread_id = ?), (SELECT COUNT(*) FROM thread_items WHERE thread_id = ?), (SELECT COUNT(*) FROM thread_turns WHERE thread_id = ?), (SELECT COUNT(*) FROM thread_items WHERE thread_id = ?)",
    )
    .bind(root_id.to_string())
    .bind(root_id.to_string())
    .bind(child_id.to_string())
    .bind(child_id.to_string())
    .fetch_one(db)
    .await
    .expect("inspect parent and child history row owners");
    assert_eq!(
        owner_counts,
        (3, 4, 1, 1),
        "inherited pagination must leave parent turns and items parent-owned"
    );

    for sort_key in [ItemSortKey::CreatedAtOrdinal, ItemSortKey::UpdatedAtOrdinal] {
        let error = store
            .list_items(ListItemsParams {
                sort_key,
                ..updated_item_params(child_id, /*after_updated_at_ordinal*/ 0)
            })
            .await
            .expect_err("incremental replay should reject forked lineages");
        assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));
    }

    let first_occurrences = store
        .search_thread_occurrences(SearchThreadOccurrencesParams {
            thread_id: child_id,
            search_term: "item".to_string(),
            cursor: None,
            page_size: 2,
        })
        .await
        .expect("first inherited occurrence page");
    assert_eq!(
        first_occurrences
            .items
            .iter()
            .map(|item| item.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["root-agent", "root-2-item"]
    );
    let inherited_turn = store
        .list_turns(turn_params(
            child_id,
            Some(first_occurrences.items[0].turn_cursor.clone()),
            /*page_size*/ 1,
            SortDirection::Asc,
            StoredTurnItemsView::NotLoaded,
        ))
        .await
        .expect("navigate to inherited occurrence turn");
    assert_eq!(turn_ids(&inherited_turn), vec!["root-1"]);
    let child_occurrences = store
        .search_thread_occurrences(SearchThreadOccurrencesParams {
            thread_id: child_id,
            search_term: "item".to_string(),
            cursor: first_occurrences.next_cursor,
            page_size: 2,
        })
        .await
        .expect("continue inherited occurrence search");
    assert_eq!(child_occurrences.items[0].item_id, "child-item");
    assert_eq!(child_occurrences.next_cursor, None);

    let gap_cursor = serde_json::to_string(&HistoryCursor {
        requested_thread_id: child_id,
        root_rollout_id: child_id,
        rollout_ordinal: 6,
        include_anchor: true,
        scope: CursorScope::Turns,
    })
    .expect("serialize cursor in metadata gap");
    let error = store
        .list_turns(turn_params(
            child_id,
            Some(gap_cursor),
            /*page_size*/ 1,
            SortDirection::Asc,
            StoredTurnItemsView::NotLoaded,
        ))
        .await
        .expect_err("cursor cannot point to segment metadata");
    assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));

    let (_other_home, other_store, other_thread_id) =
        store_with_mode(ThreadHistoryMode::Paginated).await;
    let error = other_store
        .list_items(item_params(
            other_thread_id,
            /*turn_id*/ None,
            second_items.backwards_cursor,
            /*page_size*/ 2,
            SortDirection::Asc,
        ))
        .await
        .expect_err("lineage cursor belongs to requested thread");
    assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));
}

#[tokio::test]
async fn inherited_search_excludes_turns_created_after_the_fork() {
    let (home, store, child_id) = store_with_mode(ThreadHistoryMode::Paginated).await;
    let source_id = ThreadId::default();
    let source_path = rollout_path(home.path(), source_id);
    write_rollout_with_end(
        source_path.as_path(),
        source_id,
        /*history_base*/ None,
        /*next_ordinal*/ 5,
    );
    write_rollout_with_end(
        rollout_path(home.path(), child_id).as_path(),
        child_id,
        Some(history_position(
            source_path.as_path(),
            source_id,
            /*end_ordinal_exclusive*/ 3,
        )),
        /*next_ordinal*/ 2,
    );
    let db = history_db(&store).await;
    insert_turn(
        db,
        source_id,
        "hidden-turn",
        /*rollout_ordinal*/ 3,
        "completed",
        /*error_json*/ None,
        /*first_user_item_id*/ Some("hidden-item"),
        /*final_agent_item_id*/ None,
    )
    .await;
    insert_item(
        db,
        source_id,
        "hidden-turn",
        "hidden-item",
        /*rollout_ordinal*/ 2,
    )
    .await;

    let occurrences = store
        .search_thread_occurrences(SearchThreadOccurrencesParams {
            thread_id: child_id,
            search_term: "hidden".to_string(),
            cursor: None,
            page_size: 1,
        })
        .await
        .expect("search inherited history");

    assert!(occurrences.items.is_empty());
    assert_eq!(occurrences.next_cursor, None);
}

#[tokio::test]
async fn lineage_reads_nested_forks() {
    let (home, store, child_id) = store_with_mode(ThreadHistoryMode::Paginated).await;
    let root_id = ThreadId::default();
    let middle_id = ThreadId::default();
    let root_path = rollout_path(home.path(), root_id);
    write_rollout_with_end(
        root_path.as_path(),
        root_id,
        /*history_base*/ None,
        /*next_ordinal*/ 5,
    );
    let middle_path = rollout_path(home.path(), middle_id);
    write_rollout_with_end(
        middle_path.as_path(),
        middle_id,
        Some(history_position(
            root_path.as_path(),
            root_id,
            /*end_ordinal_exclusive*/ 4,
        )),
        /*next_ordinal*/ 3,
    );
    write_rollout_with_end(
        rollout_path(home.path(), child_id).as_path(),
        child_id,
        Some(history_position(
            middle_path.as_path(),
            middle_id,
            /*end_ordinal_exclusive*/ 7,
        )),
        /*next_ordinal*/ 2,
    );
    let db = history_db(&store).await;
    for (thread_id, turn_id, ordinal, status, first_user_item_id) in [
        (root_id, "root", 1, "completed", None),
        (root_id, "shared", 2, "completed", Some("before-fork")),
        (middle_id, "shared", 5, "interrupted", None),
        (middle_id, "middle", 6, "completed", None),
        (child_id, "child", 8, "completed", None),
    ] {
        insert_turn(
            db,
            thread_id,
            turn_id,
            ordinal,
            status,
            /*error_json*/ None,
            first_user_item_id,
            /*final_agent_item_id*/ None,
        )
        .await;
    }
    insert_item(
        db,
        root_id,
        "shared",
        "before-fork",
        /*rollout_ordinal*/ 3,
    )
    .await;
    insert_item(
        db,
        root_id,
        "shared",
        "after-fork",
        /*rollout_ordinal*/ 4,
    )
    .await;

    let first_descending_page = store
        .list_turns(turn_params(
            child_id,
            /*cursor*/ None,
            /*page_size*/ 2,
            SortDirection::Desc,
            StoredTurnItemsView::NotLoaded,
        ))
        .await
        .expect("first nested descending page");
    assert_eq!(turn_ids(&first_descending_page), vec!["child", "middle"]);
    let second_descending_page = store
        .list_turns(turn_params(
            child_id,
            first_descending_page.next_cursor,
            /*page_size*/ 2,
            SortDirection::Desc,
            StoredTurnItemsView::Summary,
        ))
        .await
        .expect("second nested descending page");
    assert_eq!(turn_ids(&second_descending_page), vec!["shared", "root"]);
    assert_eq!(
        second_descending_page.turns[0].status,
        StoredTurnStatus::Interrupted
    );
    assert_eq!(
        second_descending_page.turns[0].items,
        vec![expected_item(
            "shared",
            "before-fork",
            /*rollout_ordinal*/ 3
        )]
    );

    let ascending_page = store
        .list_turns(turn_params(
            child_id,
            /*cursor*/ None,
            /*page_size*/ 2,
            SortDirection::Asc,
            StoredTurnItemsView::Summary,
        ))
        .await
        .expect("first nested ascending page");
    assert_eq!(turn_ids(&ascending_page), vec!["root", "shared"]);

    let mut held_connections = Vec::new();
    for _ in 0..4 {
        held_connections.push(db.acquire().await.expect("hold history connection"));
    }
    let occurrences = tokio::time::timeout(
        Duration::from_secs(5),
        store.search_thread_occurrences(SearchThreadOccurrencesParams {
            thread_id: child_id,
            search_term: "o".to_string(),
            cursor: None,
            page_size: 1,
        }),
    )
    .await
    .expect("inherited search releases its row connection")
    .expect("search inherited occurrence");
    assert_eq!(occurrences.items[0].item_id, "before-fork");
    assert!(occurrences.next_cursor.is_some());

    let occurrence_turn = store
        .list_turns(turn_params(
            child_id,
            Some(occurrences.items[0].turn_cursor.clone()),
            /*page_size*/ 1,
            SortDirection::Asc,
            StoredTurnItemsView::NotLoaded,
        ))
        .await
        .expect("navigate to effective occurrence turn");
    assert_eq!(turn_ids(&occurrence_turn), vec!["shared"]);
}

async fn store_with_mode(history_mode: ThreadHistoryMode) -> (TempDir, LocalThreadStore, ThreadId) {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let thread_id = ThreadId::default();
    let rollout_path = rollout_path(home.path(), thread_id);
    if history_mode == ThreadHistoryMode::Paginated {
        write_rollout(
            rollout_path.as_path(),
            thread_id,
            /*history_base*/ None,
        );
    }
    let runtime = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("state runtime");
    let mut builder = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        rollout_path,
        Utc::now(),
        SessionSource::Cli,
    );
    builder.history_mode = history_mode;
    runtime
        .upsert_thread(&builder.build(config.default_model_provider_id.as_str()))
        .await
        .expect("seed thread metadata");
    let store = LocalThreadStore::new(config, Some(runtime));
    (home, store, thread_id)
}

fn selected_rollout_path(
    home: &std::path::Path,
    thread_id: ThreadId,
    rollout_id: ThreadId,
) -> std::path::PathBuf {
    home.join(format!(
        "sessions/2026/07/16/rollout-2026-07-16T00-00-00-{thread_id}_{rollout_id}.jsonl"
    ))
}

async fn select_rollout_path(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    rollout_path: std::path::PathBuf,
) {
    let state_db = store.state_db().await.expect("state runtime");
    let mut metadata = state_db
        .get_thread(thread_id)
        .await
        .expect("read metadata")
        .expect("thread metadata");
    metadata.rollout_path = rollout_path;
    state_db
        .upsert_thread(&metadata)
        .await
        .expect("select rollout generation");
}

fn write_rollout(
    path: &std::path::Path,
    thread_id: ThreadId,
    history_base: Option<HistoryPosition>,
) {
    write_rollout_with_end(path, thread_id, history_base, /*next_ordinal*/ 1);
}

fn write_segmented_legacy_rollout(home: &std::path::Path, thread_id: ThreadId) -> u64 {
    write_segmented_legacy_rollout_with_history_base(home, thread_id, /*history_base*/ None)
}

fn append_segmented_legacy_turn(home: &std::path::Path, thread_id: ThreadId, turn_id: &str) -> u64 {
    let path = rollout_path(home, thread_id);
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(path.as_path())
        .expect("open segmented legacy rollout");
    let items = [
        RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
            turn_id: turn_id.to_string(),
            trace_id: None,
            started_at: Some(10),
            model_context_window: None,
            collaboration_mode_kind: Default::default(),
        })),
        RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
            message: "indexed legacy message".to_string(),
            ..Default::default()
        })),
        RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
            turn_id: turn_id.to_string(),
            last_agent_message: None,
            error: None,
            started_at: Some(10),
            completed_at: Some(20),
            duration_ms: Some(10_000),
            time_to_first_token_ms: None,
        })),
    ];
    for item in items {
        let line = RolloutLine {
            timestamp: "2026-07-16T00:00:00.000Z".to_string(),
            ordinal: None,
            item,
        };
        writeln!(
            file,
            "{}",
            serde_json::to_string(&line).expect("serialize legacy rollout item")
        )
        .expect("append legacy rollout item");
    }
    file.flush().expect("flush segmented legacy rollout");
    fs::metadata(path)
        .expect("read segmented legacy rollout metadata")
        .len()
}

fn write_segmented_legacy_rollout_with_history_base(
    home: &std::path::Path,
    thread_id: ThreadId,
    history_base: Option<HistoryPosition>,
) -> u64 {
    write_segmented_legacy_rollout_with_origin(
        home,
        thread_id,
        history_base,
        /*forked_from_id*/ None,
        /*parent_thread_id*/ None,
    )
}

fn write_segmented_legacy_rollout_with_origin(
    home: &std::path::Path,
    thread_id: ThreadId,
    history_base: Option<HistoryPosition>,
    forked_from_id: Option<ThreadId>,
    parent_thread_id: Option<ThreadId>,
) -> u64 {
    let path = rollout_path(home, thread_id);
    fs::create_dir_all(path.parent().expect("rollout parent"))
        .expect("create legacy rollout parent");
    let line = RolloutLine {
        timestamp: "2026-07-16T00:00:00.000Z".to_string(),
        ordinal: None,
        item: RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                session_id: thread_id.into(),
                id: thread_id,
                segment_id: Some(SegmentId::new()),
                history_mode: ThreadHistoryMode::Legacy,
                history_base,
                forked_from_id,
                parent_thread_id,
                ..SessionMeta::default()
            },
            git: None,
        }),
    };
    fs::write(
        path.as_path(),
        format!(
            "{}\n",
            serde_json::to_string(&line).expect("serialize segmented legacy rollout")
        ),
    )
    .expect("write segmented legacy rollout");
    fs::metadata(path)
        .expect("read segmented legacy rollout metadata")
        .len()
}

fn write_rollout_with_end(
    path: &std::path::Path,
    thread_id: ThreadId,
    history_base: Option<HistoryPosition>,
    next_ordinal: u64,
) {
    fs::create_dir_all(path.parent().expect("rollout parent")).expect("create rollout parent");
    let initial_ordinal = history_base.map_or(0, |base| base.end_ordinal_exclusive);
    let mut lines = vec![RolloutLine {
        timestamp: "2026-07-16T00:00:00.000Z".to_string(),
        ordinal: Some(initial_ordinal),
        item: RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                session_id: thread_id.into(),
                id: thread_id,
                history_mode: ThreadHistoryMode::Paginated,
                history_base,
                ..SessionMeta::default()
            },
            git: None,
        }),
    }];
    for offset in 1..next_ordinal {
        let ordinal = initial_ordinal
            .checked_add(offset)
            .expect("fixture ordinal");
        lines.push(RolloutLine {
            timestamp: "2026-07-16T00:00:00.000Z".to_string(),
            ordinal: Some(ordinal),
            item: RolloutItem::EventMsg(EventMsg::ShutdownComplete),
        });
    }
    fs::write(
        path,
        format!(
            "{}\n",
            lines
                .iter()
                .map(|line| serde_json::to_string(line).expect("serialize rollout"))
                .collect::<Vec<_>>()
                .join("\n")
        ),
    )
    .expect("write rollout");
}

fn write_projected_same_thread_segments(
    home: &std::path::Path,
    thread_id: ThreadId,
    segment_count: usize,
) -> std::path::PathBuf {
    write_projected_same_thread_segments_with_mode(
        home,
        thread_id,
        segment_count,
        ThreadHistoryMode::Paginated,
    )
}

fn write_projected_same_thread_segments_with_mode(
    home: &std::path::Path,
    thread_id: ThreadId,
    segment_count: usize,
    history_mode: ThreadHistoryMode,
) -> std::path::PathBuf {
    let segment_ids = (0..segment_count)
        .map(|_| SegmentId::new())
        .collect::<Vec<_>>();
    let active_path = rollout_path(home, thread_id);
    let paths = segment_ids
        .iter()
        .enumerate()
        .map(|(index, segment_id)| {
            if index + 1 == segment_count {
                active_path.clone()
            } else {
                home.join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
                    .join(thread_id.to_string())
                    .join(segment_id.to_string())
                    .join("segment.jsonl")
            }
        })
        .collect::<Vec<_>>();

    for index in 0..segment_count {
        let ordinal = u64::try_from(index).expect("fixture ordinal") * 4;
        let mut lines = vec![RolloutLine {
            timestamp: "2026-07-16T00:00:00.000Z".to_string(),
            ordinal: Some(ordinal),
            item: RolloutItem::SessionMeta(SessionMetaLine {
                meta: SessionMeta {
                    session_id: thread_id.into(),
                    id: thread_id,
                    segment_id: Some(segment_ids[index]),
                    history_mode,
                    ..SessionMeta::default()
                },
                git: None,
            }),
        }];
        if let Some(previous_index) = index.checked_sub(1) {
            lines.push(RolloutLine {
                timestamp: "2026-07-16T00:00:00.000Z".to_string(),
                ordinal: Some(ordinal + 1),
                item: RolloutItem::RolloutReference(RolloutReferenceItem {
                    rollout_id: Some(thread_id),
                    rollout_path: paths[previous_index].clone(),
                    thread_id: Some(thread_id),
                    rollout_timestamp: None,
                    segment_id: Some(segment_ids[previous_index]),
                    max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
                    nth_user_message: None,
                    compacted_replacement_history_filter_texts: None,
                }),
            });
        }
        lines.push(RolloutLine {
            timestamp: "2026-07-16T00:00:00.000Z".to_string(),
            ordinal: Some(ordinal + 2),
            item: RolloutItem::EventMsg(EventMsg::ShutdownComplete),
        });
        let path = &paths[index];
        fs::create_dir_all(path.parent().expect("segment parent"))
            .expect("create segment directory");
        let encoded = lines
            .iter()
            .map(|line| serde_json::to_string(line).expect("serialize segment line"))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(path, format!("{encoded}\n")).expect("write physical segment");
    }

    active_path
}

fn rollout_path(home: &std::path::Path, thread_id: ThreadId) -> std::path::PathBuf {
    home.join(format!(
        "sessions/2026/07/16/rollout-2026-07-16T00-00-00-{thread_id}.jsonl"
    ))
}

fn history_position(
    path: &std::path::Path,
    thread_id: ThreadId,
    end_ordinal_exclusive: u64,
) -> HistoryPosition {
    HistoryPosition {
        thread_id,
        end_ordinal_exclusive,
        end_byte_offset: rollout_end_byte_offset(path, end_ordinal_exclusive),
    }
}

fn rollout_end_byte_offset(path: &std::path::Path, end_ordinal_exclusive: u64) -> u64 {
    let bytes = fs::read(path).expect("read rollout");
    let end_byte_offset = bytes
        .split_inclusive(|byte| *byte == b'\n')
        .take_while(|line| {
            serde_json::from_slice::<RolloutLine>(line)
                .expect("parse rollout fixture")
                .ordinal
                .expect("paginated rollout ordinal")
                < end_ordinal_exclusive
        })
        .map(<[u8]>::len)
        .sum::<usize>();
    u64::try_from(end_byte_offset).expect("rollout byte offset fits u64")
}

async fn history_db(store: &LocalThreadStore) -> &sqlx::SqlitePool {
    store
        .thread_history_db()
        .await
        .expect("open history fixture database")
}

#[allow(clippy::too_many_arguments)]
async fn insert_turn(
    db: &sqlx::SqlitePool,
    thread_id: ThreadId,
    turn_id: &str,
    rollout_ordinal: i64,
    status: &str,
    error_json: Option<&str>,
    first_user_item_id: Option<&str>,
    final_agent_item_id: Option<&str>,
) {
    sqlx::query(
        r#"
INSERT INTO thread_turns (
    thread_id,
    turn_id,
    rollout_ordinal,
    status,
    error_json,
    first_user_item_id,
    final_agent_item_id
) VALUES (?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(thread_id.to_string())
    .bind(turn_id)
    .bind(rollout_ordinal)
    .bind(status)
    .bind(error_json)
    .bind(first_user_item_id)
    .bind(final_agent_item_id)
    .execute(db)
    .await
    .expect("insert turn fixture");
}

async fn insert_item(
    db: &sqlx::SqlitePool,
    thread_id: ThreadId,
    turn_id: &str,
    item_id: &str,
    rollout_ordinal: i64,
) {
    let (item_type, item_json) = fixture_item(item_id);
    sqlx::query(
        "INSERT INTO thread_items (thread_id, turn_id, item_id, rollout_ordinal, updated_at_ordinal, created_at_ms, item_type, item_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(thread_id.to_string())
    .bind(turn_id)
    .bind(item_id)
    .bind(rollout_ordinal)
    .bind(rollout_ordinal)
    .bind(rollout_ordinal * 1_000)
    .bind(item_type)
    .bind(item_json)
    .execute(db)
    .await
    .expect("insert item fixture");
}

fn turn_params(
    thread_id: ThreadId,
    cursor: Option<String>,
    page_size: usize,
    sort_direction: SortDirection,
    items_view: StoredTurnItemsView,
) -> ListTurnsParams {
    ListTurnsParams {
        thread_id,
        include_archived: false,
        cursor,
        page_size,
        sort_direction,
        items_view,
    }
}

fn item_params(
    thread_id: ThreadId,
    turn_id: Option<&str>,
    cursor: Option<String>,
    page_size: usize,
    sort_direction: SortDirection,
) -> ListItemsParams {
    ListItemsParams {
        thread_id,
        turn_id: turn_id.map(str::to_owned),
        include_archived: false,
        cursor,
        page_size,
        sort_direction,
        sort_key: ItemSortKey::CreatedAtOrdinal,
        after_updated_at_ordinal: None,
    }
}

fn updated_item_params(thread_id: ThreadId, after_updated_at_ordinal: u64) -> ListItemsParams {
    ListItemsParams {
        sort_key: ItemSortKey::UpdatedAtOrdinal,
        after_updated_at_ordinal: Some(after_updated_at_ordinal),
        ..item_params(
            thread_id,
            /*turn_id*/ None,
            /*cursor*/ None,
            /*page_size*/ 2,
            SortDirection::Asc,
        )
    }
}

fn expected_item(turn_id: &str, item_id: &str, rollout_ordinal: u64) -> StoredThreadItem {
    StoredThreadItem {
        turn_id: turn_id.to_string(),
        item_id: item_id.to_string(),
        updated_at_ordinal: rollout_ordinal,
        created_at_ms: i64::try_from(rollout_ordinal).expect("fixture ordinal fits i64") * 1_000,
        item_json: fixture_item(item_id).1.into_bytes(),
    }
}

fn fixture_item(item_id: &str) -> (&'static str, String) {
    if item_id.contains("agent") {
        (
            "agentMessage",
            format!(r#"{{"type":"agentMessage","id":"{item_id}","text":"{item_id} item"}}"#),
        )
    } else {
        (
            "userMessage",
            format!(
                r#"{{"type":"userMessage","id":"{item_id}","content":[{{"type":"text","text":"{item_id}"}}]}}"#
            ),
        )
    }
}

fn turn_ids(page: &TurnPage) -> Vec<&str> {
    page.turns
        .iter()
        .map(|turn| turn.turn_id.as_str())
        .collect()
}

fn item_ids(page: &ItemPage) -> Vec<&str> {
    page.items
        .iter()
        .map(|item| item.item_id.as_str())
        .collect()
}
