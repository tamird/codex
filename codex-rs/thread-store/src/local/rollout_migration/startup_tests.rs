use std::fs;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::UserMessageEvent;
use codex_protocol::user_input::UserInput;
use codex_rollout::RolloutConfig;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_rollout::RolloutRecorder;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::super::publish::migration_journal_path;
use super::super::publish::write_migration_journal;
use super::super::thread_history;
use super::LocalThreadStore;
use crate::ListThreadsParams;
use crate::ReadThreadParams;
use crate::SortDirection;
use crate::ThreadSortKey;
use crate::ThreadStore;
use crate::local::test_support::test_config;

const TIMESTAMP: &str = "2025-01-03T12:00:00Z";

#[path = "concurrency_tests.rs"]
mod concurrency_tests;

fn write_rollout(home: &Path, thread_id: ThreadId, history_mode: ThreadHistoryMode) -> PathBuf {
    let directory = home.join("sessions/2025/01/03");
    fs::create_dir_all(&directory).expect("create rollout directory");
    let path = directory.join(format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl"));
    let mut file = fs::File::create(&path).expect("create legacy rollout");
    let paginated = history_mode == ThreadHistoryMode::Paginated;
    let metadata = SessionMeta {
        session_id: thread_id.into(),
        id: thread_id,
        timestamp: TIMESTAMP.to_string(),
        cwd: home.to_path_buf(),
        originator: "test-originator".to_string(),
        cli_version: "0.0.0".to_string(),
        source: SessionSource::Cli,
        model_provider: Some("test-provider".to_string()),
        history_mode,
        ..SessionMeta::default()
    };
    let question = if paginated {
        EventMsg::ItemCompleted(ItemCompletedEvent {
            thread_id,
            turn_id: format!("turn-{thread_id}"),
            item: TurnItem::UserMessage(UserMessageItem {
                id: format!("user-{thread_id}"),
                client_id: None,
                content: vec![UserInput::Text {
                    text: "question".to_string(),
                    text_elements: Vec::new(),
                }],
            }),
            started_at_ms: None,
            completed_at_ms: 0,
        })
    } else {
        EventMsg::UserMessage(UserMessageEvent {
            message: "question".to_string(),
            ..UserMessageEvent::default()
        })
    };
    for (ordinal, item) in [
        RolloutItem::SessionMeta(SessionMetaLine {
            meta: metadata,
            git: None,
        }),
        RolloutItem::EventMsg(question),
    ]
    .into_iter()
    .enumerate()
    {
        let line = RolloutLine {
            timestamp: TIMESTAMP.to_string(),
            ordinal: paginated.then_some(ordinal as u64),
            item,
        };
        writeln!(
            file,
            "{}",
            serde_json::to_string(&line).expect("serialize legacy record")
        )
        .expect("write legacy record");
    }
    path
}

fn set_history_base(path: &Path, history_base: HistoryPosition) {
    let contents = fs::read_to_string(path).expect("read rollout");
    let mut lines = contents.lines();
    let mut head: serde_json::Value =
        serde_json::from_str(lines.next().expect("session metadata")).expect("parse metadata");
    head["payload"]["history_base"] =
        serde_json::to_value(history_base).expect("serialize history base");
    let mut updated = serde_json::to_string(&head).expect("serialize metadata");
    for line in lines {
        updated.push('\n');
        updated.push_str(line);
    }
    updated.push('\n');
    fs::write(path, updated).expect("write history base");
}

fn prepend_rollout_reference(path: &Path, reference: RolloutReferenceItem) {
    let original = fs::read_to_string(path).expect("read Paginated rollout");
    let mut original_lines = original.lines();
    let session_meta = original_lines.next().expect("session metadata");
    let reference = serde_json::to_string(&RolloutLine {
        timestamp: TIMESTAMP.to_string(),
        ordinal: Some(1),
        item: RolloutItem::RolloutReference(reference),
    })
    .expect("serialize rollout reference");
    let mut rewritten = format!("{session_meta}\n{reference}\n");
    for line in original_lines {
        let mut line: RolloutLine = serde_json::from_str(line).expect("parse local record");
        line.ordinal = line.ordinal.map(|ordinal| ordinal + 1);
        rewritten.push_str(&serde_json::to_string(&line).expect("serialize local record"));
        rewritten.push('\n');
    }
    fs::write(path, rewritten).expect("write leading rollout reference");
}

fn move_to_timestamp(
    home: &Path,
    path: PathBuf,
    session_day: &str,
    filename_timestamp: &str,
) -> PathBuf {
    let directory = home.join(format!("sessions/{session_day}"));
    fs::create_dir_all(&directory).expect("create rollout directory");
    let stem = path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_suffix(".jsonl"))
        .expect("rollout filename");
    let thread_id = stem
        .get(stem.len().checked_sub(36).expect("thread id offset")..)
        .expect("thread id in rollout filename");
    let moved_path = directory.join(format!("rollout-{filename_timestamp}-{thread_id}.jsonl"));
    fs::rename(path, &moved_path).expect("move rollout timestamp");
    moved_path
}

async fn indexed_store(home: &Path) -> LocalThreadStore {
    let config = test_config(home);
    let rollout_config = RolloutConfig {
        codex_home: config.codex_home.clone(),
        sqlite: config.sqlite.clone(),
        cwd: home.to_path_buf(),
        model_provider_id: config.default_model_provider_id.clone(),
        generate_memories: false,
    };
    let state_db = codex_rollout::state_db::try_init(&rollout_config)
        .await
        .expect("backfill legacy thread metadata");
    LocalThreadStore::new(config, Some(state_db))
}

#[tokio::test]
async fn records_and_advances_checked_thread() {
    let home = TempDir::new().expect("create Codex home");
    let legacy_thread_id = ThreadId::new();
    let legacy_path = write_rollout(home.path(), legacy_thread_id, ThreadHistoryMode::Legacy);
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts_on_startup()
        .await
        .expect("migrate startup rollouts");
    assert_eq!(
        codex_rollout::read_session_meta_line(&legacy_path)
            .await
            .expect("read migrated metadata")
            .meta
            .history_mode,
        ThreadHistoryMode::Paginated
    );

    let newer_thread_id = ThreadId::new();
    let newer_path = move_to_timestamp(
        home.path(),
        write_rollout(home.path(), newer_thread_id, ThreadHistoryMode::Paginated),
        "2025/01/04",
        "2025-01-04T12-00-00",
    );
    let physical_rollout_id = ThreadId::new();
    let reverted_path = newer_path.with_file_name(format!(
        "rollout-2025-01-04T12-00-00-{newer_thread_id}_{physical_rollout_id}.jsonl"
    ));
    fs::rename(newer_path, reverted_path).expect("write distinct physical rollout name");
    store
        .migrate_rollouts_on_startup()
        .await
        .expect("check newer paginated rollout");

    let state = store
        .state_db()
        .await
        .expect("state db")
        .get_rollout_migration_state(super::LEGACY_TO_PAGINATED_MIGRATION_ID)
        .await
        .expect("read migration state")
        .expect("migration state");
    assert_eq!(
        state.last_checked_thread,
        Some(codex_state::RolloutMigrationCursor {
            thread_created_at: chrono::DateTime::parse_from_rfc3339("2025-01-04T12:00:00Z")
                .expect("newer creation time")
                .timestamp(),
            thread_id: newer_thread_id.to_string(),
        })
    );
}

#[tokio::test]
async fn checks_rollouts_within_the_cursor_lookback() {
    let home = TempDir::new().expect("create Codex home");
    let older_thread_id = ThreadId::new();
    let older_path = move_to_timestamp(
        home.path(),
        write_rollout(home.path(), older_thread_id, ThreadHistoryMode::Legacy),
        "2025/01/02",
        "2025-01-02T12-00-00",
    );
    let newer_path = move_to_timestamp(
        home.path(),
        write_rollout(home.path(), ThreadId::new(), ThreadHistoryMode::Paginated),
        "2025/01/03",
        "2025-01-03T12-00-00",
    );
    let store = indexed_store(home.path()).await;
    let cursor = super::thread_creation_cursor(&newer_path).expect("newer rollout cursor");
    store
        .state_db()
        .await
        .expect("state db")
        .advance_rollout_migration_state(super::LEGACY_TO_PAGINATED_MIGRATION_ID, Some(&cursor))
        .await
        .expect("seed migration cursor");

    store
        .migrate_rollouts_on_startup()
        .await
        .expect("check rollout behind cursor");

    assert_eq!(
        codex_rollout::read_session_meta_line(&older_path)
            .await
            .expect("read migrated metadata")
            .meta
            .history_mode,
        ThreadHistoryMode::Paginated
    );
}

#[tokio::test]
async fn legacy_cursor_does_not_suppress_requested_automatic_migration() {
    let home = TempDir::new().expect("create Codex home");
    let legacy_thread_id = ThreadId::new();
    let legacy_path = move_to_timestamp(
        home.path(),
        write_rollout(home.path(), legacy_thread_id, ThreadHistoryMode::Legacy),
        "2025/01/02",
        "2025-01-02T12-00-00",
    );
    let newer_path = move_to_timestamp(
        home.path(),
        write_rollout(home.path(), ThreadId::new(), ThreadHistoryMode::Paginated),
        "2025/01/04",
        "2025-01-04T12-00-00",
    );
    let store = indexed_store(home.path()).await;
    let old_cursor = super::thread_creation_cursor(&newer_path).expect("newer rollout cursor");
    store
        .state_db()
        .await
        .expect("state db")
        .advance_rollout_migration_state(super::LEGACY_TO_PAGINATED_MIGRATION_ID, Some(&old_cursor))
        .await
        .expect("seed completed legacy migration cursor");

    store.start_automatic_rollout_migration();
    tokio::task::yield_now().await;
    assert_eq!(
        codex_rollout::read_session_meta_line(&legacy_path)
            .await
            .expect("read unrequested rollout")
            .meta
            .history_mode,
        ThreadHistoryMode::Legacy
    );
    tokio::time::timeout(
        Duration::from_secs(2),
        store.await_automatic_rollout_migration(legacy_thread_id),
    )
    .await
    .expect("requested migration ignores the completed legacy-only cursor")
    .expect("requested migration completes");
    assert_eq!(
        codex_rollout::read_session_meta_line(&legacy_path)
            .await
            .expect("read requested rollout")
            .meta
            .history_mode,
        ThreadHistoryMode::Paginated
    );
}

#[tokio::test]
async fn requested_paginated_thread_repairs_missing_projection_once() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let rollout_path = write_rollout(home.path(), thread_id, ThreadHistoryMode::Paginated);
    let store = indexed_store(home.path()).await;
    assert!(
        thread_history::projection_state(&store, thread_id)
            .await
            .expect("read missing projection")
            .is_none()
    );
    let _unrelated_job = codex_rollout::try_acquire_rollout_maintenance_job_lock(home.path())
        .expect("acquire unrelated migration job")
        .expect("unrelated migration job is free");

    store.start_automatic_rollout_migration();
    tokio::time::timeout(
        Duration::from_secs(2),
        store.await_automatic_rollout_migration(thread_id),
    )
    .await
    .expect("requested projection repair completes")
    .expect("requested projection repair succeeds");

    let rollout_len = fs::metadata(&rollout_path)
        .expect("read rollout metadata")
        .len();
    let projection = thread_history::projection_state(&store, thread_id)
        .await
        .expect("read repaired projection")
        .expect("repaired projection");
    assert!(projection.lineage_complete);
    assert_eq!(projection.next_byte_offset, rollout_len);
    assert!(super::processed_thread_ids(&store).await.is_empty());

    let restarted_store = indexed_store(home.path()).await;
    restarted_store.start_automatic_rollout_migration();
    restarted_store
        .await_automatic_rollout_migration(thread_id)
        .await
        .expect("completed projection remains ready after restart");
    assert!(
        super::processed_thread_ids(&restarted_store)
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn native_projection_failure_uses_retained_source_without_waiting_for_migration_job() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(home.path(), thread_id, ThreadHistoryMode::Paginated);
    let source = fs::read_to_string(&path).expect("read source rollout");
    let repeated_record = source.lines().last().expect("last source record");
    fs::write(&path, format!("{source}{repeated_record}\n"))
        .expect("append unsupported ordinal reuse");
    let original_bytes = fs::read(&path).expect("capture original rollout");
    let store = indexed_store(home.path()).await;
    let _unrelated_job = codex_rollout::try_acquire_rollout_maintenance_job_lock(home.path())
        .expect("acquire unrelated migration job")
        .expect("unrelated migration job is free");
    store.start_automatic_rollout_migration();
    tokio::time::timeout(
        Duration::from_secs(2),
        store.await_automatic_rollout_migration(thread_id),
    )
    .await
    .expect("unchanged native fallback does not wait for unrelated conversion")
    .expect("native fallback retains the selected source");
    assert_eq!(
        fs::read(&path).expect("read retained source"),
        original_bytes
    );
    assert!(super::processed_thread_ids(&store).await.is_empty());
    assert!(
        !store
            .has_history_projection(thread_id)
            .await
            .expect("no complete projection")
    );
}

#[tokio::test]
async fn requested_paginated_reference_still_migrates_with_local_projection() {
    let home = TempDir::new().expect("create Codex home");
    let parent_id = ThreadId::new();
    let parent_segment_id = SegmentId::new();
    let parent_path = write_rollout(home.path(), parent_id, ThreadHistoryMode::Legacy);
    let parent_contents = fs::read_to_string(&parent_path).expect("read parent rollout");
    let (head, suffix) = parent_contents
        .split_once('\n')
        .expect("parent metadata record");
    let mut head: serde_json::Value = serde_json::from_str(head).expect("parse parent metadata");
    head["payload"]["segment_id"] =
        serde_json::to_value(parent_segment_id).expect("encode immutable parent identity");
    fs::write(&parent_path, format!("{head}\n{suffix}")).expect("authenticate immutable parent");
    let thread_id = ThreadId::new();
    let path = write_rollout(home.path(), thread_id, ThreadHistoryMode::Paginated);
    prepend_rollout_reference(
        &path,
        RolloutReferenceItem {
            rollout_id: Some(parent_id),
            rollout_path: parent_path,
            thread_id: Some(parent_id),
            rollout_timestamp: None,
            segment_id: Some(parent_segment_id),
            max_depth: codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        },
    );
    let store = indexed_store(home.path()).await;
    crate::local::thread_history_materialization::materialize_to_sqlite(&store, thread_id, &path)
        .await
        .expect("seed local projection");
    store.start_automatic_rollout_migration();
    store
        .await_automatic_rollout_migration(thread_id)
        .await
        .expect("reference-backed root still migrates");
    assert_eq!(super::processed_thread_ids(&store).await, vec![thread_id]);
    let selected = store
        .read_thread(ReadThreadParams {
            thread_id,
            include_archived: false,
            include_history: false,
        })
        .await
        .expect("read migrated root");
    let metadata = codex_rollout::read_session_meta_line(
        selected.rollout_path.as_ref().expect("selected rollout"),
    )
    .await
    .expect("read migrated metadata");
    assert_eq!(metadata.meta.history_mode, ThreadHistoryMode::Paginated);
    assert!(
        !super::super::lineage::contains_convertible_rollout_reference(
            home.path(),
            selected.rollout_path.as_ref().expect("selected rollout"),
        )
        .await
        .expect("migrated lineage contains no convertible references")
    );
    assert!(
        store
            .has_history_projection(thread_id)
            .await
            .expect("complete projection")
    );
}

#[tokio::test]
async fn filtered_paginated_reference_does_not_start_eager_migration() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(home.path(), thread_id, ThreadHistoryMode::Paginated);
    prepend_rollout_reference(
        &path,
        RolloutReferenceItem {
            rollout_id: Some(ThreadId::new()),
            rollout_path: home.path().join("filtered-parent.jsonl"),
            thread_id: Some(ThreadId::new()),
            rollout_timestamp: None,
            segment_id: Some(SegmentId::new()),
            max_depth: codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: Some(1),
            compacted_replacement_history_filter_texts: Some(Vec::new()),
        },
    );

    let store = indexed_store(home.path()).await;
    assert!(matches!(
        super::inspect_rollout_path(&store, &path)
            .await
            .expect("inspect filtered reference"),
        super::StartupInspection::Compatible
    ));
    store.start_automatic_rollout_migration();
    tokio::task::yield_now().await;
    assert!(super::automatic_migration_idle(&store).await);
    assert!(super::processed_thread_ids(&store).await.is_empty());
    let terminal = store
        .state_db()
        .await
        .expect("state db")
        .list_rollout_migration_skipped_rollouts(super::NATIVE_HISTORY_BASE_MIGRATION_ID)
        .await
        .expect("read native migration fingerprints");
    assert!(terminal.is_empty());
    store
        .await_automatic_rollout_migration(thread_id)
        .await
        .expect("compatible thread remains readable on demand");
    assert!(super::processed_thread_ids(&store).await.is_empty());
}

#[tokio::test]
async fn reference_backed_legacy_rollout_records_terminal_startup_skip() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let parent_thread_id = ThreadId::new();
    let path = write_rollout(home.path(), thread_id, ThreadHistoryMode::Legacy);
    let store = indexed_store(home.path()).await;
    set_history_base(
        &path,
        HistoryPosition {
            thread_id: parent_thread_id,
            end_ordinal_exclusive: 7,
            end_byte_offset: 123,
        },
    );
    let original = fs::read(&path).expect("read reference-backed rollout");

    store
        .migrate_rollouts_on_startup()
        .await
        .expect("refuse reference-backed startup migration");

    assert_eq!(fs::read(&path).expect("reread rollout"), original);
    let state_db = store.state_db().await.expect("state db");
    let checked_thread = state_db
        .get_rollout_migration_state(super::LEGACY_TO_PAGINATED_MIGRATION_ID)
        .await
        .expect("read migration state")
        .expect("migration state")
        .last_checked_thread
        .expect("checked thread");
    let skip_reasons = state_db
        .list_rollout_migration_skipped_rollouts(super::LEGACY_TO_PAGINATED_MIGRATION_ID)
        .await
        .expect("read skipped rollouts")
        .into_iter()
        .map(|skipped_rollout| skipped_rollout.skip_reason)
        .collect::<Vec<_>>();
    assert_eq!(
        (checked_thread.thread_id, skip_reasons),
        (
            thread_id.to_string(),
            vec![super::FAILED_SKIP_REASON.to_string()]
        ),
    );
}

#[tokio::test]
async fn recovers_pending_migrations_after_retrying_busy_rollouts() {
    let home = TempDir::new().expect("create Codex home");
    let pending_thread_id = ThreadId::new();
    write_rollout(home.path(), pending_thread_id, ThreadHistoryMode::Legacy);
    let busy_thread_id = ThreadId::new();
    let busy_path = move_to_timestamp(
        home.path(),
        write_rollout(home.path(), busy_thread_id, ThreadHistoryMode::Legacy),
        "2025/01/04",
        "2025-01-04T12-00-00",
    );
    let store = indexed_store(home.path()).await;
    let state_db = store.state_db().await.expect("state db");
    let writer_guard = store
        .writer_lock_coordinator
        .acquire(busy_thread_id)
        .expect("hold cross-process writer lock");

    store
        .migrate_rollouts_on_startup()
        .await
        .expect("migrate and record busy rollout");
    drop(writer_guard);
    thread_history::delete_thread(&store, pending_thread_id)
        .await
        .expect("simulate missing projection");
    let journal_path = migration_journal_path(home.path(), pending_thread_id);
    write_migration_journal(&journal_path)
        .await
        .expect("simulate pending migration marker");

    store
        .migrate_rollouts_on_startup()
        .await
        .expect("retry busy rollout before recovery");

    assert_eq!(
        codex_rollout::read_session_meta_line(&busy_path)
            .await
            .expect("read migrated metadata")
            .meta
            .history_mode,
        ThreadHistoryMode::Paginated
    );
    assert!(!journal_path.exists());
    assert!(
        thread_history::projection_state(&store, pending_thread_id)
            .await
            .expect("read repaired projection")
            .is_some()
    );
    assert!(
        state_db
            .list_rollout_migration_skipped_rollouts(super::LEGACY_TO_PAGINATED_MIGRATION_ID)
            .await
            .expect("read skipped rollouts")
            .is_empty()
    );
}

#[tokio::test]
async fn waits_for_a_live_writer_before_migrating() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(home.path(), thread_id, ThreadHistoryMode::Legacy);
    let store = indexed_store(home.path()).await;
    let live_writer_guard = store.live_writer_locks.lock(thread_id).await;
    let migration_store = store.clone();
    let mut migration = tokio::spawn(async move {
        migration_store
            .migrate_rollouts_on_startup()
            .await
            .expect("migrate startup rollouts");
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut migration)
            .await
            .is_err(),
        "migration should wait for the live writer"
    );
    drop(live_writer_guard);
    migration.await.expect("join startup migration");

    assert_eq!(
        codex_rollout::read_session_meta_line(&path)
            .await
            .expect("read migrated metadata")
            .meta
            .history_mode,
        ThreadHistoryMode::Paginated
    );
}

#[tokio::test]
async fn waits_for_rollout_maintenance_before_migrating() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(home.path(), thread_id, ThreadHistoryMode::Legacy);
    let store = indexed_store(home.path()).await;
    let maintenance_guard = codex_rollout::try_acquire_rollout_maintenance_lock(home.path())
        .expect("acquire rollout maintenance lock")
        .expect("claim rollout maintenance lock");
    let migration_store = store.clone();
    let mut migration = tokio::spawn(async move {
        migration_store
            .migrate_rollouts_on_startup()
            .await
            .expect("migrate startup rollouts");
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut migration)
            .await
            .is_err(),
        "migration should wait for rollout maintenance"
    );
    drop(maintenance_guard);
    tokio::time::timeout(Duration::from_secs(2), migration)
        .await
        .expect("migration should retry rollout maintenance")
        .expect("join startup migration");

    assert_eq!(
        codex_rollout::read_session_meta_line(&path)
            .await
            .expect("read migrated metadata")
            .meta
            .history_mode,
        ThreadHistoryMode::Paginated
    );
}

#[tokio::test]
async fn permanently_skips_failed_rollouts_without_blocking_the_cursor() {
    let home = TempDir::new().expect("create Codex home");
    let failed_thread_id = ThreadId::new();
    let failed_path = write_rollout(home.path(), failed_thread_id, ThreadHistoryMode::Legacy);
    let store = indexed_store(home.path()).await;
    let state_db = store.state_db().await.expect("state db");
    let metadata = state_db
        .get_thread(failed_thread_id)
        .await
        .expect("read thread metadata")
        .expect("thread metadata");
    state_db
        .delete_thread(failed_thread_id)
        .await
        .expect("remove thread metadata");

    store
        .migrate_rollouts_on_startup()
        .await
        .expect("record failed rollout");
    assert_eq!(
        state_db
            .get_rollout_migration_state(super::LEGACY_TO_PAGINATED_MIGRATION_ID)
            .await
            .expect("read migration state")
            .expect("migration state")
            .last_checked_thread
            .expect("checked thread")
            .thread_id,
        failed_thread_id.to_string()
    );
    assert_eq!(
        state_db
            .list_rollout_migration_skipped_rollouts(super::LEGACY_TO_PAGINATED_MIGRATION_ID)
            .await
            .expect("read skipped rollouts")
            .into_iter()
            .map(|skipped_rollout| skipped_rollout.skip_reason)
            .collect::<Vec<_>>(),
        vec![super::FAILED_SKIP_REASON.to_string()]
    );

    state_db
        .insert_thread_if_absent(&metadata)
        .await
        .expect("restore thread metadata");
    let archived_directory = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir_all(&archived_directory).expect("create archived directory");
    let archived_path = archived_directory.join(failed_path.file_name().expect("rollout filename"));
    fs::rename(&failed_path, &archived_path).expect("archive failed rollout");

    store
        .migrate_rollouts_on_startup()
        .await
        .expect("skip failed rollout again");

    assert_eq!(
        codex_rollout::read_session_meta_line(&archived_path)
            .await
            .expect("read failed rollout metadata")
            .meta
            .history_mode,
        ThreadHistoryMode::Legacy
    );
}

#[tokio::test]
async fn retries_busy_rollouts_after_archive_and_compression_move() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = move_to_timestamp(
        home.path(),
        write_rollout(home.path(), thread_id, ThreadHistoryMode::Legacy),
        "2025/01/01",
        "2025-01-01T12-00-00",
    );
    let newest_thread_id = ThreadId::new();
    move_to_timestamp(
        home.path(),
        write_rollout(home.path(), newest_thread_id, ThreadHistoryMode::Paginated),
        "2025/01/04",
        "2025-01-04T12-00-00",
    );
    let store = indexed_store(home.path()).await;
    let state_db = store.state_db().await.expect("state db");
    let writer_guard = store
        .writer_lock_coordinator
        .acquire(thread_id)
        .expect("hold cross-process writer lock");

    store
        .migrate_rollouts_on_startup()
        .await
        .expect("record busy rollout");
    assert_eq!(
        state_db
            .get_rollout_migration_state(super::LEGACY_TO_PAGINATED_MIGRATION_ID)
            .await
            .expect("read migration state")
            .expect("migration state")
            .last_checked_thread
            .expect("checked thread")
            .thread_id,
        newest_thread_id.to_string()
    );
    assert_eq!(
        state_db
            .list_rollout_migration_skipped_rollouts(super::LEGACY_TO_PAGINATED_MIGRATION_ID)
            .await
            .expect("read skipped rollouts")
            .into_iter()
            .map(|skipped_rollout| skipped_rollout.skip_reason)
            .collect::<Vec<_>>(),
        vec![super::BUSY_SKIP_REASON.to_string()]
    );

    drop(writer_guard);
    let archived_directory = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir_all(&archived_directory).expect("create archived directory");
    let archived_path = archived_directory.join(path.file_name().expect("rollout filename"));
    fs::rename(&path, &archived_path).expect("archive busy rollout");
    let compressed_path = archived_path.with_extension("jsonl.zst");
    let mut input = fs::File::open(&archived_path).expect("open archived rollout");
    let output = fs::File::create(&compressed_path).expect("create compressed rollout");
    let mut encoder = zstd::stream::write::Encoder::new(output, 0).expect("create encoder");
    std::io::copy(&mut input, &mut encoder).expect("compress archived rollout");
    encoder.finish().expect("finish compressed rollout");
    fs::remove_file(&archived_path).expect("remove plain archived rollout");
    store
        .migrate_rollouts_on_startup()
        .await
        .expect("retry no-longer-busy rollout");

    assert_eq!(
        codex_rollout::read_session_meta_line(&compressed_path)
            .await
            .expect("read migrated metadata")
            .meta
            .history_mode,
        ThreadHistoryMode::Paginated
    );
    assert!(
        state_db
            .list_rollout_migration_skipped_rollouts(super::LEGACY_TO_PAGINATED_MIGRATION_ID)
            .await
            .expect("read skipped rollouts")
            .is_empty()
    );
}

#[tokio::test]
async fn treats_writer_owned_empty_rollouts_as_busy() {
    let home = TempDir::new().expect("create Codex home");
    write_rollout(home.path(), ThreadId::new(), ThreadHistoryMode::Paginated);
    let store = indexed_store(home.path()).await;
    store
        .migrate_rollouts_on_startup()
        .await
        .expect("seed startup cursor");

    let thread_id = ThreadId::new();
    let path = move_to_timestamp(
        home.path(),
        write_rollout(home.path(), thread_id, ThreadHistoryMode::Legacy),
        "2025/01/04",
        "2025-01-04T12-00-00",
    );
    let (items, _, _) = RolloutRecorder::load_rollout_items(&path)
        .await
        .expect("load rollout items");
    let metadata = codex_rollout::builder_from_items(items.as_slice(), &path)
        .expect("build thread metadata")
        .build("test-provider");
    let contents = fs::read(&path).expect("read rollout before emptying");
    fs::write(&path, []).expect("empty rollout");
    let state_db = store.state_db().await.expect("state db");
    state_db
        .upsert_thread(&metadata)
        .await
        .expect("seed thread metadata");
    let writer_guard = store
        .writer_lock_coordinator
        .acquire(thread_id)
        .expect("hold cross-process writer lock");

    store
        .migrate_rollouts_on_startup()
        .await
        .expect("record empty rollout as busy");
    assert_eq!(
        state_db
            .list_rollout_migration_skipped_rollouts(super::LEGACY_TO_PAGINATED_MIGRATION_ID)
            .await
            .expect("read skipped rollouts")
            .into_iter()
            .map(|skipped_rollout| skipped_rollout.skip_reason)
            .collect::<Vec<_>>(),
        vec![super::BUSY_SKIP_REASON.to_string()]
    );

    fs::write(&path, contents).expect("restore rollout");
    drop(writer_guard);
    store
        .migrate_rollouts_on_startup()
        .await
        .expect("retry no-longer-empty rollout");

    assert_eq!(
        codex_rollout::read_session_meta_line(&path)
            .await
            .expect("read migrated metadata")
            .meta
            .history_mode,
        ThreadHistoryMode::Paginated
    );
}

#[tokio::test]
async fn automatic_migration_runs_only_for_requested_thread_while_list_remains_nonblocking() {
    let home = TempDir::new().expect("create Codex home");
    let oldest_thread_id = ThreadId::new();
    let oldest_path = write_rollout(home.path(), oldest_thread_id, ThreadHistoryMode::Legacy);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let middle_thread_id = ThreadId::new();
    let middle_path = write_rollout(home.path(), middle_thread_id, ThreadHistoryMode::Legacy);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let newest_thread_id = ThreadId::new();
    let newest_path = write_rollout(home.path(), newest_thread_id, ThreadHistoryMode::Legacy);
    let store = indexed_store(home.path()).await;
    let oldest_guard = store.live_writer_locks.lock(oldest_thread_id).await;

    store.start_automatic_rollout_migration();
    tokio::task::yield_now().await;
    assert!(super::automatic_migration_idle(&store).await);
    assert!(super::processed_thread_ids(&store).await.is_empty());

    let listed = tokio::time::timeout(
        Duration::from_millis(100),
        ThreadStore::list_threads(
            &store,
            ListThreadsParams {
                page_size: 10,
                cursor: None,
                sort_key: ThreadSortKey::UpdatedAt,
                sort_direction: SortDirection::Desc,
                allowed_sources: Vec::new(),
                model_providers: None,
                cwd_filters: None,
                section: None,
                project_id: None,
                archived: false,
                search_term: None,
                relation_filter: None,
                use_state_db_only: true,
            },
        ),
    )
    .await
    .expect("thread list must not wait for migration")
    .expect("list threads");
    assert_eq!(listed.items.len(), 3);

    let load_store = store.clone();
    let mut load = tokio::spawn(async move {
        ThreadStore::read_thread(
            &load_store,
            ReadThreadParams {
                thread_id: oldest_thread_id,
                include_archived: false,
                include_history: true,
            },
        )
        .await
    });
    let joined_load_store = store.clone();
    let mut joined_load = tokio::spawn(async move {
        ThreadStore::read_thread(
            &joined_load_store,
            ReadThreadParams {
                thread_id: oldest_thread_id,
                include_archived: false,
                include_history: true,
            },
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut load)
            .await
            .is_err(),
        "specific thread load must wait for its migration"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut joined_load)
            .await
            .is_err(),
        "concurrent loads must join the same migration"
    );
    let attempts = super::processed_thread_ids(&store).await;
    assert!(!attempts.is_empty());
    assert!(attempts.iter().all(|id| *id == oldest_thread_id));

    drop(oldest_guard);
    load.await
        .expect("join thread load")
        .expect("load requested thread after migration");
    joined_load
        .await
        .expect("join second thread load")
        .expect("load requested thread from shared migration");
    assert!(
        super::processed_thread_ids(&store)
            .await
            .iter()
            .all(|id| *id == oldest_thread_id)
    );
    assert_eq!(
        codex_rollout::read_session_meta_line(&oldest_path)
            .await
            .expect("read requested rollout")
            .meta
            .history_mode,
        ThreadHistoryMode::Paginated
    );
    for path in [middle_path, newest_path] {
        assert_eq!(
            codex_rollout::read_session_meta_line(&path)
                .await
                .expect("read unrequested rollout")
                .meta
                .history_mode,
            ThreadHistoryMode::Legacy
        );
    }
}

#[tokio::test]
async fn maintenance_conflict_blocks_only_the_requested_thread_migration() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let rollout_path = write_rollout(home.path(), thread_id, ThreadHistoryMode::Legacy);
    let store = indexed_store(home.path()).await;
    let maintenance_guard = codex_rollout::try_acquire_rollout_maintenance_lock(home.path())
        .expect("acquire rollout maintenance lock")
        .expect("maintenance lock is available");

    store.start_automatic_rollout_migration();
    tokio::task::yield_now().await;
    assert!(super::automatic_migration_idle(&store).await);
    assert!(super::processed_thread_ids(&store).await.is_empty());
    assert_eq!(
        codex_rollout::read_session_meta_line(&rollout_path)
            .await
            .expect("read deferred rollout")
            .meta
            .history_mode,
        ThreadHistoryMode::Legacy
    );

    let load_store = store.clone();
    let mut load = tokio::spawn(async move {
        ThreadStore::read_thread(
            &load_store,
            ReadThreadParams {
                thread_id,
                include_archived: false,
                include_history: true,
            },
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut load)
            .await
            .is_err(),
        "requested thread waits while maintenance remains active"
    );
    assert_eq!(super::processed_thread_ids(&store).await, vec![thread_id]);
    drop(maintenance_guard);
    load.await
        .expect("join requested thread load")
        .expect("load thread after deferred migration");
    assert_eq!(
        codex_rollout::read_session_meta_line(&rollout_path)
            .await
            .expect("read migrated rollout")
            .meta
            .history_mode,
        ThreadHistoryMode::Paginated
    );
}

#[tokio::test]
async fn thread_load_uses_supported_reader_after_automatic_migration_failure() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let missing_thread_id = ThreadId::new();
    let rollout_path = write_rollout(home.path(), thread_id, ThreadHistoryMode::Paginated);
    prepend_rollout_reference(
        &rollout_path,
        RolloutReferenceItem {
            rollout_id: Some(missing_thread_id),
            rollout_path: home.path().join("missing-parent.jsonl"),
            thread_id: Some(missing_thread_id),
            rollout_timestamp: None,
            segment_id: Some(SegmentId::new()),
            max_depth: codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        },
    );
    let store = indexed_store(home.path()).await;
    store.start_automatic_rollout_migration();

    let thread = ThreadStore::read_thread(
        &store,
        ReadThreadParams {
            thread_id,
            include_archived: false,
            include_history: false,
        },
    )
    .await
    .expect("thread metadata should use the selected rollout after migration rejection");
    assert_eq!(thread.thread_id, thread_id);
    assert_eq!(thread.rollout_path, Some(rollout_path));
}

#[tokio::test]
async fn inspection_failure_with_pending_journal_blocks_thread_load() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let rollout_path = write_rollout(home.path(), thread_id, ThreadHistoryMode::Paginated);
    let store = indexed_store(home.path()).await;
    fs::OpenOptions::new()
        .append(true)
        .open(&rollout_path)
        .expect("open rollout")
        .write_all(b"{not json}\n")
        .expect("append invalid record");
    let journal_path = migration_journal_path(home.path(), thread_id);
    write_migration_journal(&journal_path)
        .await
        .expect("create pending migration journal");
    fs::write(&journal_path, b"{not json}").expect("corrupt pending migration journal");
    store.start_automatic_rollout_migration();

    let error = tokio::time::timeout(
        Duration::from_secs(2),
        store.await_automatic_rollout_migration(thread_id),
    )
    .await
    .expect("pending recovery must terminate")
    .expect_err("pending recovery failure must block the thread load");
    assert!(
        error
            .to_string()
            .contains("did not retain an unchanged selected source"),
        "unexpected error: {error}"
    );
    assert!(journal_path.exists());
}
