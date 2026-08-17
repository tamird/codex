use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::config_types::ApprovalsReviewer;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::ReasoningSummary;
use codex_protocol::config_types::Settings;
use codex_protocol::config_types::WindowsSandboxLevel;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::models::AgentMessageInputContent;
use codex_protocol::models::ContentItem;
use codex_protocol::models::PermissionProfile;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SandboxPolicy;
use codex_protocol::protocol::SegmentPreviousTurnSettings;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::ThreadSettingsSnapshot;
use codex_protocol::protocol::TokenCountEvent;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnContextItem;
use codex_protocol::protocol::TurnEnvironmentSelections;
use codex_protocol::protocol::TurnStartedEvent;
use codex_protocol::user_input::UserInput;
use codex_rollout::CertifiedSegmentStateCheckpoint;
use codex_rollout::CompactedItem;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_utils_absolute_path::AbsolutePathBuf;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use crate::ThreadStore;
use crate::local::test_support::test_config;
use crate::local::test_support::write_session_file_with_fork;
use crate::local::test_support::write_session_file_with_history_mode;

#[tokio::test]
async fn active_model_context_scan_stops_at_the_interactive_byte_limit() {
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 2039);
    let path = write_session_file_with_history_mode(
        home.path(),
        "2025-01-03T13-39-00",
        uuid,
        ThreadHistoryMode::Paginated,
    )
    .expect("write active rollout");
    let session_meta = codex_rollout::read_session_meta_line(path.as_path())
        .await
        .expect("read session metadata");

    let error = match scan_projected_active_model_context_blocking_with_limit(
        path.as_path(),
        session_meta,
        /*max_scan_bytes*/ 1,
    ) {
        Ok(_) => panic!("oversized active scan must stop before complete replay"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), std::io::ErrorKind::FileTooLarge);
    assert!(error.to_string().contains("migrate-rollouts"));
}

#[tokio::test]
async fn compressed_active_model_context_scan_stops_at_the_interactive_byte_limit() {
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 2038);
    let path = write_session_file_with_history_mode(
        home.path(),
        "2025-01-03T13-38-00",
        uuid,
        ThreadHistoryMode::Paginated,
    )
    .expect("write active rollout");
    let session_meta = codex_rollout::read_session_meta_line(path.as_path())
        .await
        .expect("read session metadata");
    let compressed_path = path.with_extension("jsonl.zst");
    let source = std::fs::read(path.as_path()).expect("read active rollout");
    let compressed = zstd::stream::encode_all(source.as_slice(), 0).expect("compress rollout");
    std::fs::write(compressed_path.as_path(), compressed).expect("write compressed rollout");

    let error = match scan_compressed_active_model_context_blocking_with_limit(
        compressed_path.as_path(),
        session_meta,
        /*max_scan_bytes*/ 1,
        /*max_record_bytes*/ 1024,
    ) {
        Ok(_) => panic!("oversized compressed scan must stop before complete replay"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), std::io::ErrorKind::FileTooLarge);
    assert!(error.to_string().contains("migrate-rollouts"));
}

#[tokio::test]
async fn active_model_context_scan_rejects_an_oversized_record() {
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 2037);
    let path = write_session_file_with_history_mode(
        home.path(),
        "2025-01-03T13-37-00",
        uuid,
        ThreadHistoryMode::Paginated,
    )
    .expect("write active rollout");
    let session_meta = codex_rollout::read_session_meta_line(path.as_path())
        .await
        .expect("read session metadata");
    let mut file = OpenOptions::new()
        .append(true)
        .open(path.as_path())
        .expect("open active rollout");
    writeln!(
        file,
        "{{\"oversized\":\"{}\"}}",
        "x".repeat(MAX_INTERACTIVE_MODEL_CONTEXT_RECORD_BYTES)
    )
    .expect("append oversized record");

    let error = match scan_projected_active_model_context_blocking(path.as_path(), session_meta) {
        Ok(_) => panic!("oversized active record must require migration"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), std::io::ErrorKind::FileTooLarge);
    assert!(error.to_string().contains("migrate-rollouts"));
}

#[tokio::test]
async fn certified_active_checkpoint_does_not_open_missing_predecessor() {
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 2040);
    let thread_id = ThreadId::from_string(&uuid.to_string()).expect("thread id");
    let path = write_session_file_with_history_mode(
        home.path(),
        "2025-01-03T13-40-00",
        uuid,
        ThreadHistoryMode::Legacy,
    )
    .expect("write active rollout");
    let mut items = vec![RolloutItem::RolloutReference(RolloutReferenceItem {
        rollout_id: Some(thread_id),
        rollout_path: home.path().join("missing-predecessor.jsonl"),
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(SegmentId::new()),
        max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    })];
    items.extend(certified_cleared_checkpoint("active checkpoint").into_items());
    append_items(path.as_path(), items);
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let context = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect("load active checkpoint without predecessor");

    assert!(matches!(
        context.items.get(1),
        Some(RolloutItem::Compacted(compacted))
            if compacted.message == "active checkpoint"
                && compacted.segment_state_checkpoint.is_some()
    ));
    read_thread::load_history_items(home.path(), path.as_path())
        .await
        .expect_err("complete history still requires the predecessor");
}

#[tokio::test]
async fn certified_active_checkpoint_accepts_literal_decimal_rate_limits() {
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 2042);
    let thread_id = ThreadId::from_string(&uuid.to_string()).expect("thread id");
    let path = write_session_file_with_history_mode(
        home.path(),
        "2025-01-03T13-42-00",
        uuid,
        ThreadHistoryMode::Legacy,
    )
    .expect("write active rollout");
    let mut items = vec![RolloutItem::RolloutReference(RolloutReferenceItem {
        rollout_id: Some(thread_id),
        rollout_path: home.path().join("missing-predecessor.jsonl"),
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(SegmentId::new()),
        max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    })];
    items.extend(certified_cleared_checkpoint("decimal checkpoint").into_items());
    append_items(path.as_path(), items);
    let mut file = OpenOptions::new()
        .append(true)
        .open(path.as_path())
        .expect("open active rollout");
    writeln!(
        file,
        r#"{{"timestamp":"2026-08-14T00:00:00Z","type":"event_msg","payload":{{"type":"token_count","info":null,"rate_limits":{{"primary":{{"used_percent":5.0,"window_minutes":300,"resets_at":1786689000}},"secondary":{{"used_percent":12.5,"window_minutes":10080,"resets_at":1787292000}}}}}}}}"#,
    )
    .expect("append literal token-count record");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let context = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect("load active checkpoint with decimal rate limits");

    assert!(context.items.iter().any(|item| {
        matches!(
            item,
            RolloutItem::Compacted(compacted) if compacted.message == "decimal checkpoint"
        )
    }));
    assert!(context.items.iter().any(|item| {
        matches!(
            item,
            RolloutItem::EventMsg(EventMsg::TokenCount(event))
                if event
                    .rate_limits
                    .as_ref()
                    .and_then(|limits| limits.secondary.as_ref())
                    .is_some_and(|window| window.used_percent == 12.5)
        )
    }));
}

#[tokio::test]
async fn invalid_active_checkpoint_does_not_hide_missing_predecessor() {
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 2041);
    let thread_id = ThreadId::from_string(&uuid.to_string()).expect("thread id");
    let path = write_session_file_with_history_mode(
        home.path(),
        "2025-01-03T13-41-00",
        uuid,
        ThreadHistoryMode::Legacy,
    )
    .expect("write active rollout");
    let mut checkpoint = certified_cleared_checkpoint("invalid checkpoint").into_items();
    let Some(RolloutItem::Compacted(compacted)) = checkpoint.first_mut() else {
        panic!("checkpoint must start with compaction");
    };
    compacted
        .segment_state_checkpoint
        .as_mut()
        .expect("checkpoint descriptor")
        .version += 1;
    let mut items = vec![RolloutItem::RolloutReference(RolloutReferenceItem {
        rollout_id: Some(thread_id),
        rollout_path: home.path().join("missing-predecessor.jsonl"),
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(SegmentId::new()),
        max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    })];
    items.extend(checkpoint);
    append_items(path.as_path(), items);
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect_err("invalid checkpoint must not hide missing predecessor");
}

#[tokio::test]
async fn loads_latest_checkpoint_with_required_turn_metadata() {
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 1001);
    let thread_id = codex_protocol::ThreadId::from_string(&uuid.to_string()).expect("thread id");
    let mut latest_turn_context = turn_context(home.path(), "turn-2");
    let RolloutItem::TurnContext(latest_turn_context_item) = &mut latest_turn_context else {
        unreachable!("turn_context should return a turn context item");
    };
    latest_turn_context_item.service_tier = Some("priority".to_string());
    latest_turn_context_item.model_profile = Some("balanced".to_string());
    write_paginated_rollout(
        home.path(),
        "2025-01-03T13-00-00",
        uuid,
        [
            turn_started("turn-1"),
            user_message("older turn"),
            completed_user_message("turn-1", "older turn"),
            turn_context(home.path(), "turn-1"),
            compacted("older checkpoint", Some(Vec::new())),
            turn_complete("turn-1"),
            turn_started("turn-2"),
            user_message("latest turn"),
            completed_user_message("turn-2", "latest turn"),
            latest_turn_context,
            compacted("latest checkpoint", Some(Vec::new())),
            turn_complete("turn-2"),
        ],
    );
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let context = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect("load model context");

    assert!(matches!(
        context.items.first(),
        Some(RolloutItem::SessionMeta(_))
    ));
    assert!(context.items.iter().any(|item| {
        matches!(item, RolloutItem::Compacted(compacted) if compacted.message == "latest checkpoint")
    }));
    assert!(!context.items.iter().any(|item| {
        matches!(item, RolloutItem::Compacted(compacted) if compacted.message == "older checkpoint")
    }));
    assert!(context.items.iter().any(|item| {
        matches!(
            item,
            RolloutItem::TurnContext(context)
                if context.turn_id.as_deref() == Some("turn-2")
                    && context.service_tier.as_deref() == Some("priority")
                    && context.model_profile.as_deref() == Some("balanced")
        )
    }));
}

#[tokio::test]
async fn projected_checkpoint_resume_does_not_expand_513_older_segments() {
    const SEGMENT_COUNT: usize = 514;
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 2015);
    let thread_id = ThreadId::from_string(&uuid.to_string()).expect("thread id");
    let active_path = write_session_file_with_history_mode(
        home.path(),
        "2025-01-03T13-15-00",
        uuid,
        ThreadHistoryMode::Paginated,
    )
    .expect("write active rollout");
    let session_meta = codex_rollout::read_session_meta_line(active_path.as_path())
        .await
        .expect("read active metadata");
    let segment_ids = (0..SEGMENT_COUNT)
        .map(|_| SegmentId::new())
        .collect::<Vec<_>>();
    let paths = segment_ids
        .iter()
        .enumerate()
        .map(|(index, segment_id)| {
            if index + 1 == SEGMENT_COUNT {
                active_path.clone()
            } else {
                home.path()
                    .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
                    .join(thread_id.to_string())
                    .join(segment_id.to_string())
                    .join(active_path.file_name().expect("active rollout file name"))
            }
        })
        .collect::<Vec<_>>();

    for index in 0..SEGMENT_COUNT {
        let mut meta = session_meta.clone();
        meta.meta.segment_id = Some(segment_ids[index]);
        let mut items = vec![RolloutItem::SessionMeta(meta)];
        if let Some(previous_index) = index.checked_sub(1) {
            items.push(RolloutItem::RolloutReference(RolloutReferenceItem {
                rollout_id: Some(thread_id),
                rollout_path: paths[previous_index].clone(),
                thread_id: Some(thread_id),
                rollout_timestamp: None,
                segment_id: Some(segment_ids[previous_index]),
                max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
                nth_user_message: None,
                compacted_replacement_history_filter_texts: None,
            }));
        }
        if index + 1 == SEGMENT_COUNT {
            items.extend([
                turn_started("latest-turn"),
                user_message("latest user"),
                completed_user_message("latest-turn", "latest user"),
                turn_context(home.path(), "latest-turn"),
                compacted("latest checkpoint", Some(Vec::new())),
                turn_complete("latest-turn"),
            ]);
        }
        let start_ordinal = u64::try_from(index).expect("segment index") * 16;
        let lines = items
            .into_iter()
            .enumerate()
            .map(|(offset, item)| RolloutLine {
                timestamp: "2025-01-03T13:15:00Z".to_string(),
                ordinal: Some(start_ordinal + u64::try_from(offset).expect("line offset")),
                item,
            })
            .map(|line| serde_json::to_string(&line).expect("serialize rollout line"))
            .collect::<Vec<_>>();
        if let Some(parent) = paths[index].parent() {
            std::fs::create_dir_all(parent).expect("create segment directory");
        }
        std::fs::write(paths[index].as_path(), format!("{}\n", lines.join("\n")))
            .expect("write segment");
    }

    let active_len = std::fs::metadata(active_path.as_path())
        .expect("active metadata")
        .len();
    let store = projected_thread_store(
        home.path(),
        thread_id,
        active_len,
        u64::try_from(SEGMENT_COUNT * 16).expect("ordinal"),
    )
    .await;

    let context = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect("resume from current active checkpoint");

    assert!(context.items.iter().any(|item| {
        matches!(item, RolloutItem::Compacted(item) if item.message == "latest checkpoint")
    }));
    assert!(context.items.iter().any(|item| {
        matches!(item, RolloutItem::TurnContext(item) if item.turn_id.as_deref() == Some("latest-turn"))
    }));
}

#[tokio::test]
async fn projected_forked_legacy_checkpoint_does_not_expand_513_older_segments() {
    const SEGMENT_COUNT: usize = 514;
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 2018);
    let thread_id = ThreadId::from_string(&uuid.to_string()).expect("thread id");
    let active_path = write_session_file_with_fork(
        home.path(),
        home.path().join("sessions/2025/01/03"),
        "2025-01-03T13-18-00",
        uuid,
        "historical inherited request",
        Some("test-provider"),
        Some(Uuid::from_u128(/*v*/ 2019)),
        ThreadHistoryMode::Legacy,
    )
    .expect("write historically forked legacy rollout");
    let session_meta = codex_rollout::read_session_meta_line(&active_path)
        .await
        .expect("read active metadata");
    assert!(session_meta.meta.forked_from_id.is_some());
    let segment_ids = (0..SEGMENT_COUNT)
        .map(|_| SegmentId::new())
        .collect::<Vec<_>>();
    let paths = segment_ids
        .iter()
        .enumerate()
        .map(|(index, segment_id)| {
            if index + 1 == SEGMENT_COUNT {
                active_path.clone()
            } else {
                home.path()
                    .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
                    .join(thread_id.to_string())
                    .join(segment_id.to_string())
                    .join(active_path.file_name().expect("active rollout file name"))
            }
        })
        .collect::<Vec<_>>();

    for index in 0..SEGMENT_COUNT {
        let mut meta = session_meta.clone();
        meta.meta.segment_id = Some(segment_ids[index]);
        let mut items = vec![RolloutItem::SessionMeta(meta)];
        if let Some(previous_index) = index.checked_sub(1) {
            items.push(RolloutItem::RolloutReference(RolloutReferenceItem {
                rollout_id: Some(thread_id),
                rollout_path: paths[previous_index].clone(),
                thread_id: Some(thread_id),
                rollout_timestamp: None,
                segment_id: Some(segment_ids[previous_index]),
                max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
                nth_user_message: None,
                compacted_replacement_history_filter_texts: None,
            }));
        }
        if matches!(index, 1 | 2) {
            let mut historical_checkpoint =
                compacted("historical legacy checkpoint", Some(Vec::new()));
            if let RolloutItem::Compacted(compacted) = &mut historical_checkpoint {
                compacted.window_number = Some(u64::try_from(index).expect("window number"));
            }
            items.push(historical_checkpoint);
        }
        if index + 1 == SEGMENT_COUNT {
            let mut latest_checkpoint = compacted("latest legacy checkpoint", Some(Vec::new()));
            if let RolloutItem::Compacted(compacted) = &mut latest_checkpoint {
                compacted.window_number = Some(1);
            }
            items.extend([
                turn_started("latest-turn"),
                user_message("latest user"),
                completed_user_message("latest-turn", "latest user"),
                turn_context(home.path(), "latest-turn"),
                latest_checkpoint,
                turn_complete("latest-turn"),
            ]);
        }
        let lines = items
            .into_iter()
            .map(|item| RolloutLine {
                timestamp: "2025-01-03T13:18:00Z".to_string(),
                ordinal: None,
                item,
            })
            .map(|line| serde_json::to_string(&line).expect("serialize rollout line"))
            .collect::<Vec<_>>();
        if let Some(parent) = paths[index].parent() {
            std::fs::create_dir_all(parent).expect("create segment directory");
        }
        std::fs::write(&paths[index], format!("{}\n", lines.join("\n"))).expect("write segment");
    }

    let active_len = std::fs::metadata(&active_path)
        .expect("active metadata")
        .len();
    let store = projected_thread_store(
        home.path(),
        thread_id,
        active_len,
        u64::try_from(SEGMENT_COUNT * 16).expect("ordinal"),
    )
    .await;
    let canonical_compaction_count = read_thread::load_history_items(home.path(), &active_path)
        .await
        .expect("load complete canonical legacy history")
        .iter()
        .filter(|item| matches!(item, RolloutItem::Compacted(_)))
        .count();
    assert_eq!(canonical_compaction_count, 1);

    let context = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect("resume forked legacy history from its indexed active checkpoint");

    assert!(context.items.iter().any(|item| {
        matches!(
            item,
            RolloutItem::Compacted(item)
                if item.message == "latest legacy checkpoint" && item.window_number == Some(1)
        )
    }));
    assert!(context.items.iter().any(|item| {
        matches!(
            item,
            RolloutItem::TurnContext(item)
                if item.turn_id.as_deref() == Some("latest-turn")
        )
    }));
}

#[tokio::test]
async fn projected_legacy_checkpoint_without_window_replays_canonical_history() {
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 2021);
    let thread_id = ThreadId::from_string(&uuid.to_string()).expect("thread id");
    let path = write_session_file_with_fork(
        home.path(),
        home.path().join("sessions/2025/01/03"),
        "2025-01-03T13-21-00",
        uuid,
        "historical inherited request",
        Some("test-provider"),
        Some(Uuid::from_u128(/*v*/ 2022)),
        ThreadHistoryMode::Legacy,
    )
    .expect("write historically forked legacy rollout");
    let contents = std::fs::read_to_string(&path).expect("read active legacy rollout");
    let mut lines = contents.lines();
    let mut meta: serde_json::Value =
        serde_json::from_str(lines.next().expect("session metadata")).expect("parse metadata");
    meta["payload"]["segment_id"] =
        serde_json::to_value(SegmentId::new()).expect("serialize active legacy segment identity");
    let mut updated = serde_json::to_string(&meta).expect("serialize metadata");
    for line in lines {
        updated.push('\n');
        updated.push_str(line);
    }
    updated.push('\n');
    std::fs::write(&path, updated).expect("write active segment identity");
    append_items(
        &path,
        [
            turn_started("older-turn"),
            user_message("older request"),
            completed_user_message("older-turn", "older request"),
            turn_context(home.path(), "older-turn"),
            turn_complete("older-turn"),
            turn_started("latest-turn"),
            user_message("latest request"),
            completed_user_message("latest-turn", "latest request"),
            turn_context(home.path(), "latest-turn"),
            compacted_without_window("legacy checkpoint without window", Some(Vec::new())),
            turn_complete("latest-turn"),
        ],
    );

    let active_len = std::fs::metadata(&path)
        .expect("active legacy rollout metadata")
        .len();
    let store =
        projected_thread_store(home.path(), thread_id, active_len, /*next_ordinal*/ 32).await;
    let metadata = codex_rollout::read_session_meta_line(&path)
        .await
        .expect("read active legacy metadata");
    assert!(
        scan_projected_active_model_context(&store, thread_id, &path, &metadata)
            .await
            .expect("inspect unsupported legacy checkpoint")
            .is_none()
    );

    let expected = read_thread::load_history_items(home.path(), &path)
        .await
        .expect("load complete canonical legacy history");
    let actual = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect("fallback to complete canonical legacy history")
        .items;
    assert_eq!(
        serde_json::to_value(actual).expect("serialize actual legacy context"),
        serde_json::to_value(expected).expect("serialize canonical legacy context")
    );
}

#[tokio::test]
async fn unmarked_active_compaction_uses_compatibility_lineage() {
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 2016);
    let thread_id = ThreadId::from_string(&uuid.to_string()).expect("thread id");
    let path = write_ordinaled_paginated_rollout(
        home.path(),
        "2025-01-03T13-16-00",
        uuid,
        [
            turn_started("older-turn"),
            user_message("older user"),
            completed_user_message("older-turn", "older user"),
            turn_context(home.path(), "older-turn"),
            compacted("older checkpoint", Some(Vec::new())),
            turn_complete("older-turn"),
            turn_started("latest-turn"),
            user_message("latest user"),
            completed_user_message("latest-turn", "latest user"),
            turn_context(home.path(), "latest-turn"),
            compacted("latest checkpoint", Some(Vec::new())),
            turn_complete("latest-turn"),
        ],
    );
    let active_len = std::fs::metadata(&path).expect("rollout metadata").len();
    let store =
        projected_thread_store(home.path(), thread_id, active_len, /*next_ordinal*/ 32).await;
    let session_meta = codex_rollout::read_session_meta_line(&path)
        .await
        .expect("read active metadata");
    let expected = scan_model_context_from_lineage(
        store
            .resolve_rollout_lineage(thread_id)
            .await
            .expect("resolve complete lineage"),
        session_meta.clone(),
    )
    .await
    .expect("scan complete lineage");
    assert!(
        scan_projected_active_model_context(&store, thread_id, &path, &session_meta)
            .await
            .expect("scan active rollout")
            .is_none()
    );
    let actual = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect("load compatibility lineage")
        .items;

    assert_eq!(
        serde_json::to_value(actual).expect("serialize indexed context"),
        serde_json::to_value(expected).expect("serialize complete-lineage context")
    );
}

#[tokio::test]
async fn stale_projection_does_not_use_active_checkpoint() {
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 2017);
    let thread_id = ThreadId::from_string(&uuid.to_string()).expect("thread id");
    let path = write_ordinaled_paginated_rollout(
        home.path(),
        "2025-01-03T13-17-00",
        uuid,
        [
            turn_started("latest-turn"),
            user_message("latest user"),
            completed_user_message("latest-turn", "latest user"),
            turn_context(home.path(), "latest-turn"),
            compacted("latest checkpoint", Some(Vec::new())),
            turn_complete("latest-turn"),
        ],
    );
    let active_len = std::fs::metadata(&path).expect("rollout metadata").len();
    let store = projected_thread_store(
        home.path(),
        thread_id,
        active_len.saturating_sub(1),
        /*next_ordinal*/ 16,
    )
    .await;
    let session_meta = codex_rollout::read_session_meta_line(&path)
        .await
        .expect("read active metadata");

    assert!(
        scan_projected_active_model_context(&store, thread_id, &path, &session_meta)
            .await
            .expect("evaluate stale projection")
            .is_none()
    );
}

#[tokio::test]
async fn fork_context_excludes_items_after_frozen_cutoff() {
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 1007);
    let thread_id = ThreadId::from_string(&uuid.to_string()).expect("thread id");
    let path = write_ordinaled_paginated_rollout(
        home.path(),
        "2025-01-03T13-00-06",
        uuid,
        [turn_started("frozen-turn"), user_message("frozen message")],
    );
    let history_base =
        history_position(path.as_path(), thread_id, /*end_ordinal_exclusive*/ 3);
    append_items(path.as_path(), [user_message("later message")]);
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let lineage = store
        .resolve_rollout_lineage(thread_id)
        .await
        .expect("resolve source lineage");
    let session_meta = codex_rollout::read_session_meta_line(path.as_path())
        .await
        .expect("read source metadata");

    let context = load_for_fork(lineage, Some(history_base))
        .await
        .expect("load frozen fork context");

    let expected = vec![
        RolloutItem::SessionMeta(session_meta),
        turn_started("frozen-turn"),
        user_message("frozen message"),
    ];
    assert_eq!(
        serde_json::to_value(context).expect("serialize fork context"),
        serde_json::to_value(expected).expect("serialize expected fork context")
    );
}

#[tokio::test]
async fn loads_turn_metadata_across_an_older_checkpoint() {
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 1006);
    let thread_id = codex_protocol::ThreadId::from_string(&uuid.to_string()).expect("thread id");
    write_paginated_rollout(
        home.path(),
        "2025-01-03T13-00-05",
        uuid,
        [
            turn_started("turn-0"),
            user_message("oldest turn"),
            completed_user_message("turn-0", "oldest turn"),
            turn_context(home.path(), "turn-0"),
            turn_complete("turn-0"),
            turn_started("turn-1"),
            user_message("metadata turn"),
            completed_user_message("turn-1", "metadata turn"),
            turn_context(home.path(), "turn-1"),
            compacted("older checkpoint", Some(Vec::new())),
            turn_complete("turn-1"),
            turn_started("turn-2"),
            compacted("latest checkpoint", Some(Vec::new())),
            turn_complete("turn-2"),
        ],
    );
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let context = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect("load model context");

    assert!(context.items.iter().any(|item| {
        matches!(item, RolloutItem::Compacted(compacted) if compacted.message == "latest checkpoint")
    }));
    assert!(context.items.iter().any(|item| {
        matches!(item, RolloutItem::TurnContext(context) if context.turn_id.as_deref() == Some("turn-1"))
    }));
    assert!(!context.items.iter().any(|item| {
        matches!(item, RolloutItem::TurnContext(context) if context.turn_id.as_deref() == Some("turn-0"))
    }));
}

#[tokio::test]
async fn returns_scanned_full_history_for_unsupported_compaction() {
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 1002);
    let path = write_paginated_rollout(
        home.path(),
        "2025-01-03T13-00-01",
        uuid,
        [
            turn_started("turn-1"),
            user_message("turn"),
            completed_user_message("turn-1", "turn"),
            turn_context(home.path(), "turn-1"),
            compacted("usable checkpoint", Some(Vec::new())),
            compacted("legacy checkpoint", /*replacement_history*/ None),
            turn_complete("turn-1"),
        ],
    );

    assert_reverse_scan_matches_full_history(home.path(), path.as_path()).await;
}

#[tokio::test]
async fn paginated_model_context_without_compaction_window_scans_full_history() {
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 2020);
    let path = write_paginated_rollout(
        home.path(),
        "2025-01-03T13-20-00",
        uuid,
        [
            turn_started("older-turn"),
            user_message("older request"),
            completed_user_message("older-turn", "older request"),
            turn_context(home.path(), "older-turn"),
            turn_complete("older-turn"),
            turn_started("current-turn"),
            user_message("current request"),
            completed_user_message("current-turn", "current request"),
            turn_context(home.path(), "current-turn"),
            compacted_without_window("unsupported paginated checkpoint", Some(Vec::new())),
            turn_complete("current-turn"),
        ],
    );

    assert_reverse_scan_matches_full_history(home.path(), path.as_path()).await;
}

#[tokio::test]
async fn returns_scanned_full_history_at_bof_without_checkpoint() {
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 1003);
    let path = write_paginated_rollout(
        home.path(),
        "2025-01-03T13-00-02",
        uuid,
        [
            turn_started("turn-1"),
            user_message("turn"),
            completed_user_message("turn-1", "turn"),
            turn_context(home.path(), "turn-1"),
            turn_complete("turn-1"),
        ],
    );

    assert_reverse_scan_matches_full_history(home.path(), path.as_path()).await;
}

#[tokio::test]
async fn uses_agent_message_turn_context_without_scanning_older_turn() {
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 1004);
    let thread_id = codex_protocol::ThreadId::from_string(&uuid.to_string()).expect("thread id");
    write_paginated_rollout(
        home.path(),
        "2025-01-03T13-00-03",
        uuid,
        [
            turn_started("turn-1"),
            user_message("older turn"),
            completed_user_message("turn-1", "older turn"),
            turn_context(home.path(), "turn-1"),
            compacted("checkpoint", Some(Vec::new())),
            turn_complete("turn-1"),
            turn_started("turn-2"),
            turn_context(home.path(), "turn-2"),
            agent_message("child done"),
            turn_complete("turn-2"),
        ],
    );
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let context = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect("load model context");

    assert!(context.items.iter().any(|item| {
        matches!(item, RolloutItem::TurnContext(context) if context.turn_id.as_deref() == Some("turn-2"))
    }));
    assert!(!context.items.iter().any(|item| {
        matches!(item, RolloutItem::TurnContext(context) if context.turn_id.as_deref() == Some("turn-1"))
    }));
}

#[tokio::test]
async fn ignores_contextual_user_messages_when_selecting_turn_context() {
    let home = TempDir::new().expect("temp dir");
    let uuid = Uuid::from_u128(/*v*/ 1005);
    let thread_id = codex_protocol::ThreadId::from_string(&uuid.to_string()).expect("thread id");
    write_paginated_rollout(
        home.path(),
        "2025-01-03T13-00-04",
        uuid,
        [
            turn_started("turn-1"),
            user_message("real user turn"),
            completed_user_message("turn-1", "real user turn"),
            turn_context(home.path(), "turn-1"),
            compacted("checkpoint", Some(Vec::new())),
            turn_complete("turn-1"),
            turn_started("turn-2"),
            contextual_user_message(),
            turn_context(home.path(), "turn-2"),
            turn_complete("turn-2"),
        ],
    );
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let context = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect("load model context");

    assert!(context.items.iter().any(|item| {
        matches!(item, RolloutItem::TurnContext(context) if context.turn_id.as_deref() == Some("turn-1"))
    }));
}

#[tokio::test]
async fn replays_nested_archived_lineage_from_frozen_prefix() {
    let home = TempDir::new().expect("temp dir");
    let root_uuid = Uuid::from_u128(/*v*/ 2001);
    let root_id = ThreadId::from_string(&root_uuid.to_string()).expect("root id");
    let root_path = write_ordinaled_paginated_rollout(
        home.path(),
        "2025-01-03T13-01-00",
        root_uuid,
        [
            user_message("root before checkpoint"),
            compacted("root checkpoint", Some(Vec::new())),
            turn_started("root-excluded"),
            user_message("root after cutoff"),
        ],
    );
    let archived_root = home
        .path()
        .join("archived_sessions")
        .join(root_path.file_name().expect("root filename"));
    std::fs::create_dir_all(archived_root.parent().expect("archive parent"))
        .expect("create archive directory");
    std::fs::rename(root_path, &archived_root).expect("archive root rollout");

    let middle_uuid = Uuid::from_u128(/*v*/ 2002);
    let middle_id = ThreadId::from_string(&middle_uuid.to_string()).expect("middle id");
    let middle_path = write_ordinaled_paginated_rollout(
        home.path(),
        "2025-01-03T13-01-01",
        middle_uuid,
        [
            turn_started("middle-turn"),
            user_message("middle inherited"),
            completed_user_message("middle-turn", "middle inherited"),
            turn_context(home.path(), "middle-turn"),
            turn_complete("middle-turn"),
        ],
    );
    set_history_base(
        middle_path.as_path(),
        history_position(
            archived_root.as_path(),
            root_id,
            /*end_ordinal_exclusive*/ 3,
        ),
    );

    let child_uuid = Uuid::from_u128(/*v*/ 2003);
    let child_id = ThreadId::from_string(&child_uuid.to_string()).expect("child id");
    let child_path = write_ordinaled_paginated_rollout(
        home.path(),
        "2025-01-03T13-01-02",
        child_uuid,
        [
            turn_started("child-turn"),
            user_message("child local"),
            completed_user_message("child-turn", "child local"),
            turn_context(home.path(), "child-turn"),
            turn_complete("child-turn"),
        ],
    );
    set_history_base(
        child_path.as_path(),
        history_position(
            middle_path.as_path(),
            middle_id,
            /*end_ordinal_exclusive*/ 6,
        ),
    );
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let context = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id: child_id,
            include_archived: false,
        })
        .await
        .expect("load lineage model context");

    assert!(matches!(
        context.items.first(),
        Some(RolloutItem::SessionMeta(meta)) if meta.meta.id == child_id
    ));
    let child_meta = codex_rollout::read_session_meta_line(child_path.as_path())
        .await
        .expect("read child metadata");
    let expected = vec![
        RolloutItem::SessionMeta(child_meta),
        compacted("root checkpoint", Some(Vec::new())),
        turn_started("middle-turn"),
        user_message("middle inherited"),
        completed_user_message("middle-turn", "middle inherited"),
        turn_context(home.path(), "middle-turn"),
        turn_complete("middle-turn"),
        turn_started("child-turn"),
        user_message("child local"),
        completed_user_message("child-turn", "child local"),
        turn_context(home.path(), "child-turn"),
        turn_complete("child-turn"),
    ];
    assert_eq!(
        serde_json::to_value(&context.items).expect("serialize context"),
        serde_json::to_value(&expected).expect("serialize expected context")
    );
    // The same frozen lineage must replay from compressed files, without materializing or
    // accidentally including the archived root's records after the inherited cutoff.
    for path in [&archived_root, &middle_path, &child_path] {
        let input = std::fs::File::open(path).expect("open rollout");
        let output = std::fs::File::create(path.with_extension("jsonl.zst"))
            .expect("create compressed rollout");
        zstd::stream::copy_encode(input, output, /*level*/ 3).expect("compress rollout");
        std::fs::remove_file(path).expect("remove plain rollout");
    }
    let compressed_context = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id: child_id,
            include_archived: false,
        })
        .await
        .expect("load compressed lineage model context");
    assert_eq!(
        serde_json::to_value(compressed_context.items).expect("serialize compressed context"),
        serde_json::to_value(expected).expect("serialize expected context")
    );
    assert!(
        [archived_root, middle_path, child_path]
            .iter()
            .all(|path| !path.exists())
    );
}

fn write_paginated_rollout<const N: usize>(
    home: &Path,
    timestamp: &str,
    uuid: Uuid,
    items: [RolloutItem; N],
) -> PathBuf {
    let path =
        write_session_file_with_history_mode(home, timestamp, uuid, ThreadHistoryMode::Paginated)
            .expect("write session file");
    append_items(path.as_path(), items);
    path
}

async fn projected_thread_store(
    home: &Path,
    thread_id: ThreadId,
    next_byte_offset: u64,
    next_ordinal: u64,
) -> LocalThreadStore {
    let config = test_config(home);
    let state_db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("initialize state database");
    let store = LocalThreadStore::new(config, Some(state_db));
    let pool = store.thread_history_db().await.expect("thread history db");
    sqlx::query(
        "INSERT INTO thread_history_projection_state \
         (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, ?, ?)",
    )
    .bind(thread_id.to_string())
    .bind(i64::try_from(next_byte_offset).expect("active length"))
    .bind(i64::try_from(next_ordinal).expect("rollout ordinal"))
    .execute(pool)
    .await
    .expect("write projection state");
    store
}

fn write_ordinaled_paginated_rollout<const N: usize>(
    home: &Path,
    timestamp: &str,
    uuid: Uuid,
    items: [RolloutItem; N],
) -> PathBuf {
    let path =
        write_session_file_with_history_mode(home, timestamp, uuid, ThreadHistoryMode::Paginated)
            .expect("write session file");
    let mut file = OpenOptions::new()
        .append(true)
        .open(path.as_path())
        .expect("open session file");
    for (index, item) in items.into_iter().enumerate() {
        let line = RolloutLine {
            timestamp: "2025-01-03T13:00:01Z".to_string(),
            ordinal: Some(u64::try_from(index).expect("fixture index fits u64") + 1),
            item,
        };
        writeln!(
            file,
            "{}",
            serde_json::to_string(&line).expect("serialize line")
        )
        .expect("append rollout line");
    }
    path
}

fn set_history_base(path: &Path, history_base: HistoryPosition) {
    let contents = std::fs::read_to_string(path).expect("read rollout");
    let mut lines = contents.lines();
    let mut head: serde_json::Value =
        serde_json::from_str(lines.next().expect("session meta line")).expect("parse head");
    head["payload"]["history_base"] =
        serde_json::to_value(history_base).expect("serialize history base");
    let mut updated = serde_json::to_string(&head).expect("serialize head");
    for line in lines {
        updated.push('\n');
        updated.push_str(line);
    }
    updated.push('\n');
    std::fs::write(path, updated).expect("write history base");
}

fn history_position(
    path: &Path,
    thread_id: ThreadId,
    end_ordinal_exclusive: u64,
) -> HistoryPosition {
    HistoryPosition {
        thread_id,
        end_ordinal_exclusive,
        end_byte_offset: rollout_end_byte_offset(path, end_ordinal_exclusive),
    }
}

fn rollout_end_byte_offset(path: &Path, end_ordinal_exclusive: u64) -> u64 {
    let contents = std::fs::read(path).expect("read rollout");
    let mut byte_offset = 0_u64;
    for line in contents.split_inclusive(|byte| *byte == b'\n') {
        let parsed: RolloutLine =
            serde_json::from_slice(line).expect("parse rollout line for byte offset");
        if parsed.ordinal == Some(end_ordinal_exclusive) {
            return byte_offset;
        }
        byte_offset += u64::try_from(line.len()).expect("line length fits u64");
    }
    byte_offset
}

async fn assert_reverse_scan_matches_full_history(home: &Path, path: &Path) {
    let session_meta = codex_rollout::read_session_meta_line(path)
        .await
        .expect("read session metadata");
    let store = LocalThreadStore::new(test_config(home), /*state_db*/ None);
    let items = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id: session_meta.meta.id,
            include_archived: false,
        })
        .await
        .expect("scan model context")
        .items;
    let full_items = read_thread::load_history_items(home, path)
        .await
        .expect("load full history");

    assert_eq!(
        serde_json::to_value(items).expect("serialize scanned items"),
        serde_json::to_value(full_items).expect("serialize full items")
    );
}

fn append_items(path: &Path, items: impl IntoIterator<Item = RolloutItem>) {
    let mut file = OpenOptions::new()
        .append(true)
        .open(path)
        .expect("open session file");
    for item in items {
        let line = RolloutLine {
            timestamp: "2025-01-03T13:00:01Z".to_string(),
            ordinal: None,
            item,
        };
        writeln!(
            file,
            "{}",
            serde_json::to_string(&line).expect("serialize line")
        )
        .expect("append rollout line");
    }
}

fn turn_started(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_id: turn_id.to_string(),
        trace_id: None,
        started_at: None,
        model_context_window: Some(128_000),
        collaboration_mode_kind: Default::default(),
    }))
}

fn turn_complete(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
        turn_id: turn_id.to_string(),
        last_agent_message: None,
        error: None,
        started_at: None,
        completed_at: None,
        duration_ms: None,
        time_to_first_token_ms: None,
    }))
}

fn user_message(message: &str) -> RolloutItem {
    RolloutItem::ResponseItem(
        ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: message.to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        }
        .into(),
    )
}

fn contextual_user_message() -> RolloutItem {
    user_message("<environment_context>context only</environment_context>")
}

fn completed_user_message(turn_id: &str, message: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
        thread_id: codex_protocol::ThreadId::from_string("00000000-0000-0000-0000-000000000000")
            .expect("fixture thread id"),
        turn_id: turn_id.to_string(),
        item: TurnItem::UserMessage(UserMessageItem {
            id: format!("user-{turn_id}"),
            client_id: None,
            content: vec![UserInput::Text {
                text: message.to_string(),
                text_elements: Vec::new(),
            }],
        }),
        started_at_ms: Some(0),
        completed_at_ms: 0,
    }))
}

fn agent_message(message: &str) -> RolloutItem {
    RolloutItem::ResponseItem(
        ResponseItem::AgentMessage {
            id: None,
            author: "worker".to_string(),
            recipient: "root".to_string(),
            content: vec![AgentMessageInputContent::InputText {
                text: message.to_string(),
            }],
            internal_chat_message_metadata_passthrough: None,
        }
        .into(),
    )
}

fn turn_context(root: &Path, turn_id: &str) -> RolloutItem {
    RolloutItem::TurnContext(TurnContextItem {
        turn_id: Some(turn_id.to_string()),
        cwd: serde_json::from_value(serde_json::json!(root)).expect("absolute cwd"),
        workspace_roots: None,
        current_date: None,
        timezone: None,
        approval_policy: AskForApproval::Never,
        approvals_reviewer: None,
        sandbox_policy: SandboxPolicy::new_read_only_policy(),
        permission_profile: None,
        active_permission_profile: None,
        network: None,
        file_system_sandbox_policy: None,
        model: "test-model".to_string(),
        comp_hash: None,
        personality: None,
        collaboration_mode: None,
        multi_agent_version: None,
        multi_agent_mode: None,
        realtime_active: None,
        cyber_access_program: None,
        effort: None,
        service_tier: None,
        model_profile: None,
        summary: ReasoningSummary::Auto,
    })
}

fn compacted(message: &str, replacement_history: Option<Vec<ResponseItem>>) -> RolloutItem {
    RolloutItem::Compacted(CompactedItem {
        message: message.to_string(),
        replacement_history: replacement_history
            .map(|items| items.into_iter().map(Into::into).collect()),
        mcp_resource_origins: None,
        window_number: Some(1),
        first_window_id: None,
        previous_window_id: None,
        window_id: None,
        segment_state_checkpoint: None,
    })
}

fn compacted_without_window(
    message: &str,
    replacement_history: Option<Vec<ResponseItem>>,
) -> RolloutItem {
    RolloutItem::Compacted(CompactedItem {
        message: message.to_string(),
        replacement_history: replacement_history
            .map(|items| items.into_iter().map(Into::into).collect()),
        mcp_resource_origins: None,
        window_number: None,
        first_window_id: None,
        previous_window_id: None,
        window_id: None,
        segment_state_checkpoint: None,
    })
}

fn certified_cleared_checkpoint(message: &str) -> CertifiedSegmentStateCheckpoint {
    let window_id = Uuid::now_v7();
    let cwd: AbsolutePathBuf =
        serde_json::from_value(serde_json::json!("/tmp")).expect("absolute test cwd");
    CertifiedSegmentStateCheckpoint::new(
        CompactedItem {
            message: message.to_string(),
            replacement_history: Some(vec![
                ResponseItem::Message {
                    id: None,
                    role: "developer".to_string(),
                    content: vec![ContentItem::InputText {
                        text: "active checkpoint history".to_string(),
                    }],
                    phase: None,
                    internal_chat_message_metadata_passthrough: None,
                }
                .into(),
            ]),
            mcp_resource_origins: None,
            window_number: Some(4),
            first_window_id: Some(window_id.to_string()),
            previous_window_id: None,
            window_id: Some(window_id.to_string()),
            segment_state_checkpoint: None,
        },
        Some(SegmentPreviousTurnSettings {
            model: "test-model".to_string(),
            comp_hash: None,
            realtime_active: None,
        }),
        /*world_state*/ None,
        /*reference_context*/ None,
        ThreadSettingsAppliedEvent {
            thread_settings: ThreadSettingsSnapshot {
                model: "test-model".to_string(),
                model_provider_id: "test-provider".to_string(),
                service_tier: None,
                approval_policy: AskForApproval::Never,
                approvals_reviewer: ApprovalsReviewer::User,
                permission_profile: PermissionProfile::workspace_write(),
                active_permission_profile: None,
                cwd: cwd.clone(),
                environments: Some(TurnEnvironmentSelections::new(cwd, Vec::new())),
                workspace_roots: Some(Vec::new()),
                profile_workspace_roots: Some(Vec::new()),
                windows_sandbox_level: Some(WindowsSandboxLevel::Disabled),
                reasoning_effort: None,
                reasoning_summary: None,
                personality: None,
                collaboration_mode: CollaborationMode {
                    mode: ModeKind::Default,
                    settings: Settings {
                        model: "test-model".to_string(),
                        reasoning_effort: None,
                        developer_instructions: None,
                    },
                },
            },
        },
        TokenCountEvent {
            info: None,
            rate_limits: None,
        },
    )
    .expect("valid certified checkpoint")
}
