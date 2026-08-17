use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use codex_protocol::protocol::UserMessageEvent;
use codex_protocol::protocol::WorldStateItem;
use codex_protocol::user_input::UserInput;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_rollout::RolloutRecorder;
use pretty_assertions::assert_eq;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;

use super::super::LocalThreadStore;
use super::super::projection_rebuild::PROJECTION_REBUILD_CRASH_BOUNDARY_ENV;
use super::super::projection_rebuild::PROJECTION_REBUILD_CRASH_EXIT_CODE;
use super::super::projection_rebuild::PROJECTION_REBUILD_CRASH_THREAD_ENV;
use super::super::projection_rebuild::inject_projection_rebuild_pause;
use super::super::test_support::test_config;
use super::super::writer_lock::WriterLockCoordinator;
use super::SEGMENT_ROTATION_CRASH_BOUNDARY_ENV;
use super::SEGMENT_ROTATION_CRASH_EXIT_CODE;
use super::SEGMENT_ROTATION_CRASH_THREAD_ENV;
use super::inject_checkpoint_persistence_pause;
use super::inject_next_segment_durability_failure;
use super::inject_next_segment_precommit_failure;
use super::inject_next_segment_reopen_failure;
use super::install_immutable_segment;
use super::snapshot_segment_id;
use super::stabilize_rollout_reference;
use crate::AppendThreadItemsParams;
use crate::CreateThreadParams;
use crate::ForkBoundary;
use crate::FreezeRolloutSegmentParams;
use crate::LiveThread;
use crate::LoadThreadHistoryParams;
use crate::PersistContext;
use crate::PrepareForkParams;
use crate::ResumeThreadParams;
use crate::SegmentCheckpointPersistenceOutcome;
use crate::ThreadPersistenceMetadata;
use crate::ThreadPersistenceMode;
use crate::ThreadStore;
use crate::ThreadStoreError;

#[tokio::test]
async fn native_snapshot_reference_matches_the_persisted_immutable_identity() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::new();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create native thread");
    append_canonical_message(&store, thread_id, "native snapshot").await;
    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("freeze native snapshot");

    let resolved = codex_rollout::resolve_rollout_reference_path(home.path(), &frozen.reference)
        .await
        .expect("resolve the snapshot's compatibility reference");
    assert_eq!(
        resolved,
        std::fs::canonicalize(&frozen.reference.rollout_path).expect("canonical snapshot path"),
    );
    let metadata = codex_rollout::read_session_meta_line(&resolved)
        .await
        .expect("read metadata");
    assert_eq!(frozen.reference.segment_id, metadata.meta.segment_id);
    assert_eq!(
        frozen.reference.rollout_id,
        frozen.history_base.map(|base| base.thread_id)
    );
    let history = codex_rollout::materialize_rollout_items(home.path(), &resolved)
        .await
        .expect("materialize native snapshot");
    assert!(has_canonical_message(&history, "native snapshot"));
}

#[tokio::test]
async fn durable_live_thread_can_freeze_before_its_first_append() {
    let home = TempDir::new().expect("temp dir");
    let store = Arc::new(LocalThreadStore::new(
        test_config(home.path()),
        /*state_db*/ None,
    ));
    let thread_id = ThreadId::new();
    let live_thread = LiveThread::create(
        store.clone(),
        create_params(thread_id, ThreadHistoryMode::Legacy),
    )
    .await
    .expect("create durable thread");
    let path = live_thread
        .local_rollout_path()
        .await
        .expect("read path")
        .expect("local path");
    assert!(!path.exists(), "creation defers the initial file write");

    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("freeze before first append");
    let metadata = codex_rollout::read_session_meta_line(&frozen.reference.rollout_path)
        .await
        .expect("read frozen metadata");
    assert_eq!(metadata.meta.id, thread_id);

    tokio::fs::remove_file(&path)
        .await
        .expect("simulate lost active file");
    assert!(
        store
            .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::snapshot())
            .await
            .is_err(),
        "a previously materialized missing file must not be recreated"
    );
    assert!(!path.exists());
}

#[tokio::test]
async fn deferred_live_thread_stays_pathless_until_freeze_materializes_its_canonical_journal() {
    let home = TempDir::new().expect("temp dir");
    let config = test_config(home.path());
    let sqlite = config.sqlite.clone();
    let runtime =
        codex_state::StateRuntime::init(sqlite.clone(), config.default_model_provider_id.clone())
            .await
            .expect("initialize state db");
    let store = Arc::new(LocalThreadStore::new(config, Some(runtime.clone())));
    let thread_id = ThreadId::default();
    let mut params = create_params(thread_id, ThreadHistoryMode::Legacy);
    params.persistence_mode = ThreadPersistenceMode::Deferred;
    let live_thread = LiveThread::create(store.clone(), params)
        .await
        .expect("create deferred live thread");
    let clone = live_thread.clone();
    let stable_path = live_thread
        .local_rollout_path()
        .await
        .expect("read rollout path")
        .expect("local rollout path");

    live_thread
        .append_items(&[user_message_item("ephemeral prefix")])
        .await
        .expect("append deferred item");
    clone.flush().await.expect("flush deferred thread");
    assert!(
        !tokio::fs::try_exists(stable_path.as_path())
            .await
            .expect("check deferred rollout path"),
        "append and flush must leave a deferred rollout pathless"
    );
    assert_eq!(
        runtime
            .get_thread(thread_id)
            .await
            .expect("read deferred metadata"),
        None,
        "deferred append and flush must not write thread metadata"
    );
    let history_pool = codex_state::open_thread_history_db(&sqlite)
        .await
        .expect("open thread history db");
    let projection_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM thread_history_projection_state WHERE thread_id = ?",
    )
    .bind(thread_id.to_string())
    .fetch_one(&history_pool)
    .await
    .expect("count projection state");
    assert_eq!(projection_count, 0);

    let frozen = clone
        .freeze_local_segment(FreezeRolloutSegmentParams::rotate(vec![user_message_item(
            "replacement suffix",
        )]))
        .await
        .expect("freeze deferred thread")
        .expect("local frozen segment");
    assert!(
        tokio::fs::try_exists(stable_path.as_path())
            .await
            .expect("check stable rollout")
    );
    assert!(
        runtime
            .get_thread(thread_id)
            .await
            .expect("read materialized metadata")
            .is_some(),
        "freeze must make deferred metadata durable"
    );
    let immutable_items =
        RolloutRecorder::load_rollout_items(frozen.reference.rollout_path.as_path())
            .await
            .expect("load immutable segment")
            .0;
    assert!(matches!(
        immutable_items.first(),
        Some(RolloutItem::SessionMeta(_))
    ));
    assert!(has_message(&immutable_items, "ephemeral prefix"));
    let mut segment_entries = tokio::fs::read_dir(
        frozen
            .reference
            .rollout_path
            .parent()
            .expect("segment directory"),
    )
    .await
    .expect("read segment directory");
    let mut segment_file_count = 0;
    while segment_entries
        .next_entry()
        .await
        .expect("read segment entry")
        .is_some()
    {
        segment_file_count += 1;
    }
    assert_eq!(segment_file_count, 1);

    let replacement_items = RolloutRecorder::load_rollout_items(stable_path.as_path())
        .await
        .expect("load replacement rollout")
        .0;
    assert!(matches!(
        replacement_items.as_slice(),
        [
            RolloutItem::SessionMeta(_),
            RolloutItem::RolloutReference(_),
            RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent { message, .. }))
        ] if message == "replacement suffix"
    ));

    let logical_items =
        codex_rollout::materialize_rollout_items(home.path(), stable_path.as_path())
            .await
            .expect("materialize reference-backed rollout");
    assert!(has_message(&logical_items, "ephemeral prefix"));
    assert!(has_message(&logical_items, "replacement suffix"));

    live_thread
        .append_items(&[user_message_item("durable append")])
        .await
        .expect("append after freeze");
    live_thread.flush().await.expect("flush after freeze");
    let logical_items =
        codex_rollout::materialize_rollout_items(home.path(), stable_path.as_path())
            .await
            .expect("materialize durable continuation");
    assert!(has_message(&logical_items, "durable append"));
}

#[tokio::test]
async fn live_freeze_installs_immutable_prefix_and_isolates_later_appends() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist session metadata");
    append_message(&store, thread_id, "before freeze").await;
    store.flush_thread(thread_id).await.expect("flush thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    let source_bytes = tokio::fs::read(stable_path.as_path())
        .await
        .expect("read source prefix");

    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("freeze live segment");
    assert_eq!(frozen.history_mode, ThreadHistoryMode::Legacy);
    assert_eq!(frozen.next_rollout_ordinal, None);
    assert!(
        tokio::fs::try_exists(frozen.reference.rollout_path.as_path())
            .await
            .expect("check immutable segment")
    );
    assert_eq!(
        tokio::fs::read(frozen.reference.rollout_path.as_path())
            .await
            .expect("read immutable prefix"),
        source_bytes
    );

    append_message(&store, thread_id, "after freeze").await;
    store.flush_thread(thread_id).await.expect("flush thread");
    let immutable_items =
        RolloutRecorder::load_rollout_items(frozen.reference.rollout_path.as_path())
            .await
            .expect("read immutable segment")
            .0;
    assert!(has_message(&immutable_items, "before freeze"));
    assert!(!has_message(&immutable_items, "after freeze"));

    let live_path = store.live_rollout_path(thread_id).await.expect("live path");
    assert_eq!(live_path, stable_path);
    let replacement_meta = codex_rollout::read_session_meta_line(live_path.as_path())
        .await
        .expect("read replacement metadata");
    assert_eq!(replacement_meta.meta.id, thread_id);
    assert_ne!(
        replacement_meta.meta.segment_id,
        frozen.source_session_meta.meta.segment_id
    );
    let logical_items = codex_rollout::materialize_rollout_items(home.path(), live_path.as_path())
        .await
        .expect("materialize logical history");
    assert!(has_message(&logical_items, "before freeze"));
    assert!(has_message(&logical_items, "after freeze"));
}

#[tokio::test]
async fn committed_checkpoint_reopen_failure_recovers_without_duplicate_replacement() {
    let home = TempDir::new().expect("temp dir");
    let store = Arc::new(LocalThreadStore::new(
        test_config(home.path()),
        /*state_db*/ None,
    ));
    let thread_id = ThreadId::new();
    let live_thread = LiveThread::create(
        store.clone(),
        create_params(thread_id, ThreadHistoryMode::Legacy),
    )
    .await
    .expect("create live thread");
    live_thread
        .persist(PersistContext::Standard)
        .await
        .expect("persist live thread");
    live_thread
        .append_items(&[user_message_item("before checkpoint")])
        .await
        .expect("append source history");
    live_thread.flush().await.expect("flush source history");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");

    inject_next_segment_reopen_failure(thread_id);
    let outcome = live_thread
        .persist_segment_checkpoint(FreezeRolloutSegmentParams::rotate(vec![user_message_item(
            "checkpoint replacement",
        )]))
        .await;
    assert!(matches!(
        outcome,
        SegmentCheckpointPersistenceOutcome::Committed
    ));

    live_thread
        .append_items(&[user_message_item("after checkpoint")])
        .await
        .expect("lazy recorder recovery must permit the next append");
    live_thread.flush().await.expect("flush recovered writer");
    let items = RolloutRecorder::load_rollout_items(stable_path.as_path())
        .await
        .expect("read checkpoint rollout")
        .0;
    assert_eq!(message_count(&items, "checkpoint replacement"), 1);
    assert!(has_message(&items, "after checkpoint"));
}

#[tokio::test]
async fn precommit_rotation_failure_atomically_appends_the_checkpoint_once() {
    let home = TempDir::new().expect("temp dir");
    let store = Arc::new(LocalThreadStore::new(
        test_config(home.path()),
        /*state_db*/ None,
    ));
    let thread_id = ThreadId::new();
    let live_thread = LiveThread::create(
        store.clone(),
        create_params(thread_id, ThreadHistoryMode::Legacy),
    )
    .await
    .expect("create live thread");
    live_thread
        .persist(PersistContext::Standard)
        .await
        .expect("persist live thread");
    live_thread
        .append_items(&[user_message_item("before fallback")])
        .await
        .expect("append source history");
    live_thread.flush().await.expect("flush source history");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");

    inject_next_segment_precommit_failure(thread_id);
    let outcome = live_thread
        .persist_segment_checkpoint(FreezeRolloutSegmentParams::rotate(vec![user_message_item(
            "atomic fallback checkpoint",
        )]))
        .await;
    assert!(matches!(
        outcome,
        SegmentCheckpointPersistenceOutcome::Committed
    ));
    live_thread
        .append_items(&[user_message_item("after fallback")])
        .await
        .expect("append after atomic fallback");
    live_thread.flush().await.expect("flush recovered writer");
    let items = RolloutRecorder::load_rollout_items(stable_path.as_path())
        .await
        .expect("read fallback rollout")
        .0;
    assert_eq!(message_count(&items, "atomic fallback checkpoint"), 1);
    assert!(has_message(&items, "before fallback"));
    assert!(has_message(&items, "after fallback"));
    assert!(
        !items
            .iter()
            .any(|item| matches!(item, RolloutItem::RolloutReference(_))),
        "precommit fallback must leave the active rollout unsegmented"
    );
}

#[tokio::test]
async fn cancelling_checkpoint_caller_does_not_cancel_checkpoint_persistence() {
    let home = TempDir::new().expect("temp dir");
    let store = Arc::new(LocalThreadStore::new(
        test_config(home.path()),
        /*state_db*/ None,
    ));
    let thread_id = ThreadId::new();
    let live_thread = LiveThread::create(
        store.clone(),
        create_params(thread_id, ThreadHistoryMode::Legacy),
    )
    .await
    .expect("create live thread");
    live_thread
        .persist(PersistContext::Standard)
        .await
        .expect("persist live thread");
    live_thread
        .append_items(&[user_message_item("before cancelled caller")])
        .await
        .expect("append source history");
    live_thread.flush().await.expect("flush source history");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");

    let pause = inject_checkpoint_persistence_pause(thread_id);
    let checkpoint_owner = live_thread.clone();
    let caller = tokio::spawn(async move {
        checkpoint_owner
            .persist_segment_checkpoint(FreezeRolloutSegmentParams::rotate(vec![
                user_message_item("checkpoint after caller cancellation"),
            ]))
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), pause.entered.notified())
        .await
        .expect("checkpoint persistence owner must acquire its reservation");
    caller.abort();
    let _ = caller.await;
    pause.release.notify_one();

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let items = RolloutRecorder::load_rollout_items(stable_path.as_path())
                .await
                .expect("read active rollout while waiting for checkpoint")
                .0;
            if message_count(&items, "checkpoint after caller cancellation") == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached checkpoint owner must finish after caller cancellation");

    live_thread
        .append_items(&[user_message_item("after cancelled caller")])
        .await
        .expect("append after detached checkpoint owner completes");
    live_thread.flush().await.expect("flush recovered writer");
    let items = RolloutRecorder::load_rollout_items(stable_path.as_path())
        .await
        .expect("read checkpoint rollout")
        .0;
    assert_eq!(
        message_count(&items, "checkpoint after caller cancellation"),
        1
    );
    assert!(has_message(&items, "after cancelled caller"));
}

#[tokio::test]
async fn indeterminate_checkpoint_fences_later_persistence_without_duplicate_replacement() {
    let home = TempDir::new().expect("temp dir");
    let store = Arc::new(LocalThreadStore::new(
        test_config(home.path()),
        /*state_db*/ None,
    ));
    let thread_id = ThreadId::new();
    let live_thread = LiveThread::create(
        store.clone(),
        create_params(thread_id, ThreadHistoryMode::Legacy),
    )
    .await
    .expect("create live thread");
    live_thread
        .persist(PersistContext::Standard)
        .await
        .expect("persist live thread");
    live_thread
        .append_items(&[user_message_item("before indeterminate checkpoint")])
        .await
        .expect("append source history");
    live_thread.flush().await.expect("flush source history");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");

    inject_next_segment_durability_failure(thread_id);
    let outcome = live_thread
        .persist_segment_checkpoint(FreezeRolloutSegmentParams::rotate(vec![user_message_item(
            "indeterminate replacement",
        )]))
        .await;
    assert!(matches!(
        outcome,
        SegmentCheckpointPersistenceOutcome::Indeterminate { .. }
    ));
    let error = live_thread
        .append_items(&[user_message_item("must not append")])
        .await
        .expect_err("indeterminate checkpoint must fence later persistence");
    assert!(matches!(error, ThreadStoreError::Conflict { .. }));
    let items = RolloutRecorder::load_rollout_items(stable_path.as_path())
        .await
        .expect("read indeterminate checkpoint rollout")
        .0;
    assert_eq!(message_count(&items, "indeterminate replacement"), 1);
    assert!(!has_message(&items, "must not append"));
}

#[tokio::test]
async fn paginated_rotation_installs_the_exact_source_bytes() {
    let home = TempDir::new().expect("temp dir");
    let store = state_backed_store(home.path()).await;
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create paginated thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist paginated metadata");
    append_canonical_message(&store, thread_id, "paginated prefix").await;
    store.flush_thread(thread_id).await.expect("flush thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    let source_bytes = tokio::fs::read(stable_path.as_path())
        .await
        .expect("read paginated source");

    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("rotate paginated segment");

    assert_eq!(frozen.history_mode, ThreadHistoryMode::Paginated);
    let history_base = frozen
        .history_base
        .expect("paginated rotation must publish native history_base");
    assert_eq!(
        codex_rollout::rollout_id_from_path(frozen.reference.rollout_path.as_path()),
        Some(history_base.thread_id)
    );
    assert!(
        frozen.reference.rollout_path.starts_with(
            home.path()
                .join(codex_rollout::SESSIONS_SUBDIR)
                .join(codex_rollout::ROLLOUT_SEGMENTS_SUBDIR)
        )
    );
    assert_eq!(
        tokio::fs::read(frozen.reference.rollout_path.as_path())
            .await
            .expect("read immutable paginated segment"),
        source_bytes
    );
    let active_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("read native continuation path");
    assert_eq!(active_path, stable_path);
    let active_meta = codex_rollout::read_session_meta_line(active_path.as_path())
        .await
        .expect("read native continuation metadata");
    assert_eq!(active_meta.meta.id, thread_id);
    assert_eq!(active_meta.meta.history_base, Some(history_base));
    let active_items = RolloutRecorder::load_rollout_items(active_path.as_path())
        .await
        .expect("read native continuation")
        .0;
    assert!(
        active_items
            .iter()
            .all(|item| !matches!(item, RolloutItem::RolloutReference(_)))
    );

    append_canonical_message(&store, thread_id, "paginated suffix").await;
    store.flush_thread(thread_id).await.expect("flush suffix");
    let logical_items = store
        .load_history(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect("materialize native segmented history")
        .items;
    assert!(
        has_canonical_message(&logical_items, "paginated prefix"),
        "logical items: {logical_items:#?}"
    );
    assert!(has_canonical_message(&logical_items, "paginated suffix"));

    let sqlite_less_active = codex_rollout::find_thread_path_by_id_str(
        home.path(),
        thread_id.to_string().as_str(),
        /*state_db_ctx*/ None,
    )
    .await
    .expect("resolve active rollout without SQLite")
    .expect("active rollout without SQLite");
    assert_eq!(sqlite_less_active, active_path);
}

const SEGMENT_ROTATION_CRASH_HOME_ENV: &str = "FRODEX_SEGMENT_ROTATION_CRASH_HOME";
const PROJECTION_REBUILD_CRASH_HOME_ENV: &str = "FRODEX_PROJECTION_REBUILD_CRASH_HOME";

#[tokio::test]
#[ignore = "subprocess helper for segment_rotation_process_death_recovers_every_boundary"]
async fn segment_rotation_process_crash_child() {
    let Some(home) = std::env::var_os(SEGMENT_ROTATION_CRASH_HOME_ENV).map(PathBuf::from) else {
        return;
    };
    let thread_id = ThreadId::from_string(
        std::env::var(SEGMENT_ROTATION_CRASH_THREAD_ENV)
            .expect("segment crash thread id")
            .as_str(),
    )
    .expect("parse segment crash thread id");
    let store = state_backed_store(home.as_path()).await;
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create crash-test thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist crash-test thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("read crash-test rollout path");
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        stable_path,
        chrono::Utc::now(),
        SessionSource::Exec,
    );
    metadata.history_mode = ThreadHistoryMode::Paginated;
    metadata.cwd = home.clone();
    store
        .state_db()
        .await
        .expect("state runtime")
        .upsert_thread(&metadata.build(store.config.default_model_provider_id.as_str()))
        .await
        .expect("select crash-test rollout");
    append_turn(
        &store,
        thread_id,
        "before-crash-turn",
        "before-crash-item",
        "before segment process death",
    )
    .await;
    store
        .freeze_thread_segment(
            thread_id,
            FreezeRolloutSegmentParams::rotate(turn_items(
                thread_id,
                "checkpoint-turn",
                "checkpoint-item",
                "checkpoint after segment process death",
            )),
        )
        .await
        .expect("configured crash boundary must terminate the process");
    panic!("segment rotation completed without the configured process death");
}

#[tokio::test]
async fn segment_rotation_process_death_recovers_every_boundary() {
    let boundaries = [
        ("source_persisted_before_flush", false),
        ("source_flushed_before_seal", false),
        ("immutable_sealed_before_reference", false),
        ("reference_recorded_before_checkpoint", false),
        ("checkpoint_recorded_before_flush", false),
        ("staged_rollout_durable_before_publication", false),
        ("stable_rollout_published_before_projection", true),
    ];

    for (boundary, published) in boundaries {
        let home = TempDir::new().expect("create crash-test Codex home");
        let thread_id = ThreadId::new();
        let output = std::process::Command::new(
            std::env::current_exe().expect("current thread-store test executable"),
        )
        .arg("--exact")
        .arg("local::segment::tests::segment_rotation_process_crash_child")
        .arg("--ignored")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(SEGMENT_ROTATION_CRASH_HOME_ENV, home.path())
        .env(SEGMENT_ROTATION_CRASH_THREAD_ENV, thread_id.to_string())
        .env(SEGMENT_ROTATION_CRASH_BOUNDARY_ENV, boundary)
        .output()
        .expect("run segment crash subprocess");
        assert_eq!(
            output.status.code(),
            Some(SEGMENT_ROTATION_CRASH_EXIT_CODE),
            "boundary {boundary}; stdout: {}; stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let restarted = state_backed_store(home.path()).await;
        let metadata = restarted
            .state_db()
            .await
            .expect("state runtime")
            .get_thread(thread_id)
            .await
            .expect("read selected thread")
            .expect("selected thread metadata");
        let stable_path = metadata.rollout_path;
        let recovered =
            codex_rollout::materialize_rollout_items(home.path(), stable_path.as_path())
                .await
                .expect("materialize history after process death");
        assert_eq!(
            turn_message_count(&recovered, "before segment process death"),
            1,
            "boundary {boundary}"
        );
        assert_eq!(
            turn_message_count(&recovered, "checkpoint after segment process death"),
            usize::from(published),
            "boundary {boundary}"
        );

        restarted
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(stable_path.clone()),
                history: None,
                include_archived: true,
                metadata: create_params(thread_id, ThreadHistoryMode::Paginated).metadata,
            })
            .await
            .expect("resume after segment process death");
        assert_eq!(
            staged_rollout_file_count(stable_path.as_path()).await,
            0,
            "resume must remove stale staged files at boundary {boundary}"
        );
        if !published {
            restarted
                .freeze_thread_segment(
                    thread_id,
                    FreezeRolloutSegmentParams::rotate(turn_items(
                        thread_id,
                        "checkpoint-turn",
                        "checkpoint-item",
                        "checkpoint after segment process death",
                    )),
                )
                .await
                .expect("retry unpublished segment rotation");
        }
        append_turn(
            &restarted,
            thread_id,
            "after-crash-turn",
            "after-crash-item",
            "after segment process death",
        )
        .await;
        restarted
            .flush_thread(thread_id)
            .await
            .expect("flush after segment process death");

        let final_items =
            codex_rollout::materialize_rollout_items(home.path(), stable_path.as_path())
                .await
                .expect("materialize recovered segment lineage");
        for marker in [
            "before segment process death",
            "checkpoint after segment process death",
            "after segment process death",
        ] {
            assert_eq!(
                turn_message_count(&final_items, marker),
                1,
                "{boundary}: {marker}"
            );
        }
        let active_items = RolloutRecorder::load_rollout_items(stable_path.as_path())
            .await
            .expect("read recovered active rollout")
            .0;
        assert_eq!(
            active_items
                .iter()
                .filter(|item| matches!(item, RolloutItem::RolloutReference(_)))
                .count(),
            0,
            "boundary {boundary}"
        );
        assert!(
            codex_rollout::read_session_meta_line(stable_path.as_path())
                .await
                .expect("read recovered active metadata")
                .meta
                .history_base
                .is_some(),
            "boundary {boundary}"
        );
        let projected = restarted
            .projected_history_position(thread_id)
            .await
            .expect("read recovered projection")
            .expect("recovered projection position");
        assert_eq!(
            projected.end_byte_offset,
            tokio::fs::metadata(stable_path.as_path())
                .await
                .expect("read recovered rollout metadata")
                .len(),
            "boundary {boundary}"
        );
    }
}

#[tokio::test]
async fn fork_racing_checkpoint_rotation_observes_one_complete_boundary() {
    let home = TempDir::new().expect("create race-test Codex home");
    let store = state_backed_store(home.path()).await;
    let thread_id = ThreadId::new();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create race-test thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist race-test thread");
    append_turn(
        &store,
        thread_id,
        "before-race-turn",
        "before-race-item",
        "before checkpoint race",
    )
    .await;

    let pause = inject_checkpoint_persistence_pause(thread_id);
    let rotation_store = store.clone();
    let rotation = tokio::spawn(async move {
        rotation_store
            .persist_segment_checkpoint(
                thread_id,
                FreezeRolloutSegmentParams::rotate(turn_items(
                    thread_id,
                    "checkpoint-race-turn",
                    "checkpoint-race-item",
                    "checkpoint race replacement",
                )),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), pause.entered.notified())
        .await
        .expect("checkpoint rotation must hold its writer reservation");

    let fork_store = store.clone();
    let mut preparation = tokio::spawn(async move {
        fork_store
            .prepare_fork(PrepareForkParams {
                thread_id,
                boundary: ForkBoundary::Latest,
            })
            .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut preparation)
            .await
            .is_err(),
        "fork preparation must wait for the in-flight checkpoint publication"
    );

    pause.release.notify_one();
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(10), rotation)
            .await
            .expect("checkpoint rotation must not deadlock")
            .expect("checkpoint task must join"),
        SegmentCheckpointPersistenceOutcome::Committed
    ));
    let prepared = tokio::time::timeout(std::time::Duration::from_secs(10), preparation)
        .await
        .expect("fork preparation must not deadlock")
        .expect("fork task must join")
        .expect("prepare fork after checkpoint publication");
    assert_eq!(
        turn_message_count(prepared.model_context.as_slice(), "before checkpoint race"),
        1
    );
    assert_eq!(
        turn_message_count(
            prepared.model_context.as_slice(),
            "checkpoint race replacement"
        ),
        1
    );

    append_turn(
        &store,
        thread_id,
        "after-race-turn",
        "after-race-item",
        "after checkpoint race",
    )
    .await;
    let frozen_items = codex_rollout::materialize_rollout_items(
        home.path(),
        prepared
            .frozen_segment
            .as_ref()
            .expect("durable fork freezes its source")
            .reference
            .rollout_path
            .as_path(),
    )
    .await
    .expect("materialize prepared fork boundary");
    assert_eq!(
        turn_message_count(frozen_items.as_slice(), "before checkpoint race"),
        1
    );
    assert_eq!(
        turn_message_count(frozen_items.as_slice(), "checkpoint race replacement"),
        1
    );
    assert_eq!(
        turn_message_count(frozen_items.as_slice(), "after checkpoint race"),
        0
    );
}

#[tokio::test]
async fn concurrent_side_and_desktop_reads_rebuild_one_missing_projection() {
    let home = TempDir::new().expect("create projection-race Codex home");
    let store = state_backed_store(home.path()).await;
    let thread_id = ThreadId::new();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create projection-race thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist projection-race thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("projection-race rollout path");
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        stable_path.clone(),
        chrono::Utc::now(),
        SessionSource::Exec,
    );
    metadata.history_mode = ThreadHistoryMode::Paginated;
    metadata.cwd = home.path().to_path_buf();
    store
        .state_db()
        .await
        .expect("state runtime")
        .upsert_thread(&metadata.build(store.config.default_model_provider_id.as_str()))
        .await
        .expect("select projection-race rollout");

    for index in 0..3 {
        append_turn(
            &store,
            thread_id,
            format!("projection-race-turn-{index}").as_str(),
            format!("projection-race-item-{index}").as_str(),
            format!("projection race message {index}").as_str(),
        )
        .await;
        if index != 2 {
            store
                .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
                .await
                .expect("rotate projection-race segment");
        }
    }
    store
        .flush_thread(thread_id)
        .await
        .expect("flush projection-race history");
    let active_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("read rotated projection-race rollout path");
    let rollout_id = store
        .projected_history_position(thread_id)
        .await
        .expect("read initial projection position")
        .expect("initial projection position")
        .thread_id;
    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open projection-race history database");
    for statement in [
        "DELETE FROM thread_items WHERE thread_id = ?",
        "DELETE FROM thread_turns WHERE thread_id = ?",
        "DELETE FROM thread_history_projection_state WHERE thread_id = ?",
    ] {
        sqlx::query(statement)
            .bind(rollout_id.to_string())
            .execute(&pool)
            .await
            .expect("clear projection before race");
    }

    let mut reads = Vec::new();
    let mut sides = Vec::new();
    for _ in 0..8 {
        let read_store = store.clone();
        let read_path = active_path.clone();
        reads.push(tokio::spawn(async move {
            let history = read_store
                .read_thread_by_rollout_path(
                    read_path, /*include_archived*/ true, /*include_history*/ true,
                )
                .await
                .expect("concurrent Desktop read")
                .history
                .expect("Desktop history")
                .items;
            for index in 0..3 {
                assert_eq!(
                    turn_message_count(
                        history.as_slice(),
                        format!("projection race message {index}").as_str()
                    ),
                    1
                );
            }
        }));

        let side_store = store.clone();
        sides.push(tokio::spawn(async move {
            let prepared = side_store
                .prepare_fork_without_response_history(PrepareForkParams {
                    thread_id,
                    boundary: ForkBoundary::Latest,
                })
                .await
                .expect("concurrent side preparation");
            assert!(prepared.projected_response_turns.is_none());
            for index in 0..3 {
                assert_eq!(
                    turn_message_count(
                        prepared.model_context.as_slice(),
                        format!("projection race message {index}").as_str()
                    ),
                    1
                );
            }
        }));
    }

    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        for task in reads.into_iter().chain(sides) {
            task.await.expect("join projection-race task");
        }
    })
    .await
    .expect("concurrent side and Desktop reads must not deadlock");
    let projected = store
        .projected_history_position(thread_id)
        .await
        .expect("read rebuilt projection")
        .expect("rebuilt projection position");
    assert_eq!(
        projected.end_byte_offset,
        tokio::fs::metadata(active_path)
            .await
            .expect("read active rollout metadata")
            .len()
    );
    assert!(
        !store
            .has_history_projection(thread_id)
            .await
            .expect("reject active-only rebuilt projection"),
        "fork preparation must not promote an active-only projection"
    );
    append_turn(
        &store,
        thread_id,
        "projection-race-turn-3",
        "projection-race-item-3",
        "projection race message 3",
    )
    .await;
    store
        .flush_thread(thread_id)
        .await
        .expect("flush append while projection is incomplete");
    assert!(
        !store
            .has_history_projection(thread_id)
            .await
            .expect("retain incomplete marker after append"),
        "appending an active turn must not certify omitted predecessors"
    );
    assert!(
        store
            .rebuild_history_projection(thread_id)
            .await
            .expect("rebuild complete projection"),
        "one caller must own the complete rebuild"
    );
    assert!(
        store
            .has_history_projection(thread_id)
            .await
            .expect("accept complete rebuilt projection")
    );
    let rebuilt_turns = store
        .list_turns(crate::ListTurnsParams {
            thread_id,
            include_archived: true,
            cursor: None,
            page_size: 10,
            sort_direction: crate::SortDirection::Asc,
            items_view: crate::StoredTurnItemsView::Summary,
        })
        .await
        .expect("list rebuilt turns");
    assert_eq!(
        rebuilt_turns
            .turns
            .iter()
            .map(|turn| turn.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec![
            "projection-race-turn-0",
            "projection-race-turn-1",
            "projection-race-turn-2",
            "projection-race-turn-3",
        ]
    );
}

#[tokio::test]
async fn missing_projection_rebuilds_compressed_segmented_paginated_lineage() {
    let home = TempDir::new().expect("create compressed-rebuild Codex home");
    let store = state_backed_store(home.path()).await;
    let thread_id = ThreadId::new();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create compressed-rebuild thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist compressed-rebuild thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("compressed-rebuild rollout path");
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        stable_path,
        chrono::Utc::now(),
        SessionSource::Exec,
    );
    metadata.history_mode = ThreadHistoryMode::Paginated;
    metadata.cwd = home.path().to_path_buf();
    store
        .state_db()
        .await
        .expect("state runtime")
        .upsert_thread(&metadata.build(store.config.default_model_provider_id.as_str()))
        .await
        .expect("select compressed-rebuild rollout");

    for index in 0..3 {
        append_turn(
            &store,
            thread_id,
            format!("compressed-turn-{index}").as_str(),
            format!("compressed-item-{index}").as_str(),
            format!("compressed message {index}").as_str(),
        )
        .await;
        if index != 2 {
            store
                .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
                .await
                .expect("rotate compressed-rebuild segment");
        }
    }
    store
        .flush_thread(thread_id)
        .await
        .expect("flush compressed-rebuild thread");
    let lineage = store
        .resolve_rollout_lineage(thread_id)
        .await
        .expect("resolve compressed-rebuild lineage");
    assert_eq!(lineage.segments.len(), 3);
    for segment in &lineage.segments[..2] {
        let source = codex_rollout::existing_rollout_path(segment.rollout_path())
            .await
            .expect("immutable predecessor path");
        compress_rollout_for_test(source.as_path());
    }

    let rollout_id = lineage.root_rollout_id;
    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open compressed-rebuild history database");
    for statement in [
        "DELETE FROM thread_items WHERE thread_id = ?",
        "DELETE FROM thread_turns WHERE thread_id = ?",
        "DELETE FROM thread_history_projection_state WHERE thread_id = ?",
    ] {
        sqlx::query(statement)
            .bind(rollout_id.to_string())
            .execute(&pool)
            .await
            .expect("clear projection before compressed rebuild");
    }

    assert!(
        store
            .rebuild_history_projection(thread_id)
            .await
            .expect("rebuild compressed lineage projection")
    );
    assert!(
        store
            .has_history_projection(thread_id)
            .await
            .expect("accept compressed rebuilt projection")
    );
    let turns = store
        .list_turns(crate::ListTurnsParams {
            thread_id,
            include_archived: true,
            cursor: None,
            page_size: 10,
            sort_direction: crate::SortDirection::Asc,
            items_view: crate::StoredTurnItemsView::Summary,
        })
        .await
        .expect("list turns rebuilt from compressed lineage");
    assert_eq!(
        turns
            .turns
            .iter()
            .map(|turn| turn.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec![
            "compressed-turn-0",
            "compressed-turn-1",
            "compressed-turn-2"
        ]
    );
    for segment in &lineage.segments[..2] {
        assert!(
            segment.rollout_path().with_extension("jsonl.zst").exists(),
            "projection rebuild must leave immutable predecessors compressed"
        );
        assert!(!segment.rollout_path().exists());
    }
}

#[tokio::test]
async fn projection_rebuild_retries_after_concurrent_append_and_rotation() {
    let home = TempDir::new().expect("create rebuild-rotation Codex home");
    let store = state_backed_store(home.path()).await;
    let thread_id = ThreadId::new();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create rebuild-rotation thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist rebuild-rotation thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("rebuild-rotation rollout path");
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        stable_path.clone(),
        chrono::Utc::now(),
        SessionSource::Exec,
    );
    metadata.history_mode = ThreadHistoryMode::Paginated;
    metadata.cwd = home.path().to_path_buf();
    store
        .state_db()
        .await
        .expect("state runtime")
        .upsert_thread(&metadata.build(store.config.default_model_provider_id.as_str()))
        .await
        .expect("select rebuild-rotation rollout");

    for index in 0..2 {
        append_turn(
            &store,
            thread_id,
            format!("rebuild-turn-{index}").as_str(),
            format!("rebuild-item-{index}").as_str(),
            format!("rebuild message {index}").as_str(),
        )
        .await;
        if index == 0 {
            store
                .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
                .await
                .expect("rotate initial rebuild segment");
        }
    }
    store
        .flush_thread(thread_id)
        .await
        .expect("flush initial rebuild history");
    let rollout_id = store
        .resolve_rollout_lineage(thread_id)
        .await
        .expect("resolve initial rebuild lineage")
        .root_rollout_id;
    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open rebuild-rotation history database");
    for statement in [
        "DELETE FROM thread_items WHERE thread_id = ?",
        "DELETE FROM thread_turns WHERE thread_id = ?",
        "DELETE FROM thread_history_projection_state WHERE thread_id = ?",
    ] {
        sqlx::query(statement)
            .bind(rollout_id.to_string())
            .execute(&pool)
            .await
            .expect("clear projection before rebuild rotation");
    }

    let pause = inject_projection_rebuild_pause(thread_id);
    let rebuild_store = store.clone();
    let rebuild =
        tokio::spawn(async move { rebuild_store.rebuild_history_projection(thread_id).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), pause.entered.notified())
        .await
        .expect("projection rebuild reaches staged boundary");
    assert!(
        !store
            .has_history_projection(thread_id)
            .await
            .expect("staged projection remains invisible"),
        "an unpublished staging projection must never satisfy readers"
    );

    append_turn(
        &store,
        thread_id,
        "rebuild-turn-2",
        "rebuild-item-2",
        "rebuild message 2",
    )
    .await;
    store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("rotate while projection rebuild is staged");
    let fallback_history = store
        .read_thread_by_rollout_path(
            stable_path,
            /*include_archived*/ true,
            /*include_history*/ true,
        )
        .await
        .expect("read canonical history while rebuild is staged")
        .history
        .expect("fallback history while rebuild is staged")
        .items;
    for index in 0..3 {
        assert_eq!(
            turn_message_count(
                fallback_history.as_slice(),
                format!("rebuild message {index}").as_str()
            ),
            1
        );
    }

    pause.release.notify_one();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(15), rebuild)
            .await
            .expect("projection rebuild must not deadlock")
            .expect("projection rebuild task must join")
            .expect("projection rebuild retries changed lineage")
    );
    assert!(
        store
            .has_history_projection(thread_id)
            .await
            .expect("accept retried projection")
    );
    let turns = store
        .list_turns(crate::ListTurnsParams {
            thread_id,
            include_archived: true,
            cursor: None,
            page_size: 10,
            sort_direction: crate::SortDirection::Asc,
            items_view: crate::StoredTurnItemsView::Summary,
        })
        .await
        .expect("list turns after rebuild retry");
    assert_eq!(
        turns
            .turns
            .iter()
            .map(|turn| turn.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec!["rebuild-turn-0", "rebuild-turn-1", "rebuild-turn-2"]
    );
    let projection_id_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM thread_history_projection_state")
            .fetch_one(&pool)
            .await
            .expect("count projection identities after retry");
    assert_eq!(projection_id_count, 1, "stale staging rows must be removed");
}

#[tokio::test]
async fn unprojected_read_reschedules_and_cancels_an_active_background_rebuild() {
    let home = TempDir::new().expect("create rescheduled-rebuild Codex home");
    let store = state_backed_store(home.path()).await;
    let thread_id = ThreadId::new();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create rescheduled-rebuild thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist rescheduled-rebuild thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("rescheduled-rebuild rollout path");
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        stable_path,
        chrono::Utc::now(),
        SessionSource::Exec,
    );
    metadata.history_mode = ThreadHistoryMode::Paginated;
    metadata.cwd = home.path().to_path_buf();
    store
        .state_db()
        .await
        .expect("state runtime")
        .upsert_thread(&metadata.build(store.config.default_model_provider_id.as_str()))
        .await
        .expect("select rescheduled-rebuild rollout");

    for index in 0..2 {
        append_turn(
            &store,
            thread_id,
            format!("rescheduled-turn-{index}").as_str(),
            format!("rescheduled-item-{index}").as_str(),
            format!("rescheduled message {index}").as_str(),
        )
        .await;
        if index == 0 {
            store
                .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
                .await
                .expect("rotate rescheduled-rebuild segment");
        }
    }
    store
        .flush_thread(thread_id)
        .await
        .expect("flush rescheduled-rebuild history");
    let rollout_id = store
        .resolve_rollout_lineage(thread_id)
        .await
        .expect("resolve rescheduled-rebuild lineage")
        .root_rollout_id;
    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open rescheduled-rebuild history database");
    for statement in [
        "DELETE FROM thread_items WHERE thread_id = ?",
        "DELETE FROM thread_turns WHERE thread_id = ?",
        "DELETE FROM thread_history_projection_state WHERE thread_id = ?",
    ] {
        sqlx::query(statement)
            .bind(rollout_id.to_string())
            .execute(&pool)
            .await
            .expect("clear projection before rescheduled rebuild");
    }

    let first_pause = inject_projection_rebuild_pause(thread_id);
    store.schedule_history_projection_rebuild(thread_id).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        first_pause.entered.notified(),
    )
    .await
    .expect("first background rebuild reaches staged boundary");

    store.schedule_history_projection_rebuild(thread_id).await;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if store
                .has_history_projection(thread_id)
                .await
                .expect("query rescheduled projection")
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("replacement rebuild publishes after cancelling staged work");

    let projection_id_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM thread_history_projection_state")
            .fetch_one(&pool)
            .await
            .expect("count rescheduled projection identities");
    assert_eq!(
        projection_id_count, 1,
        "cancelled staging rows must be removed"
    );
}

#[tokio::test]
#[ignore = "subprocess helper for projection_rebuild_process_death_recovers_every_publication_boundary"]
async fn projection_rebuild_process_crash_child() {
    let Some(home) = std::env::var_os(PROJECTION_REBUILD_CRASH_HOME_ENV).map(PathBuf::from) else {
        return;
    };
    let thread_id = ThreadId::from_string(
        std::env::var(PROJECTION_REBUILD_CRASH_THREAD_ENV)
            .expect("projection crash thread id")
            .as_str(),
    )
    .expect("parse projection crash thread id");
    let store = state_backed_store(home.as_path()).await;
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create projection crash thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist projection crash thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("projection crash rollout path");
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        stable_path,
        chrono::Utc::now(),
        SessionSource::Exec,
    );
    metadata.history_mode = ThreadHistoryMode::Paginated;
    metadata.cwd = home;
    store
        .state_db()
        .await
        .expect("state runtime")
        .upsert_thread(&metadata.build(store.config.default_model_provider_id.as_str()))
        .await
        .expect("select projection crash rollout");
    for index in 0..2 {
        append_turn(
            &store,
            thread_id,
            format!("projection-crash-turn-{index}").as_str(),
            format!("projection-crash-item-{index}").as_str(),
            format!("projection crash message {index}").as_str(),
        )
        .await;
        if index == 0 {
            store
                .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
                .await
                .expect("rotate projection crash segment");
        }
    }
    store
        .flush_thread(thread_id)
        .await
        .expect("flush projection crash history");
    let rollout_id = store
        .resolve_rollout_lineage(thread_id)
        .await
        .expect("resolve projection crash lineage")
        .root_rollout_id;
    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open projection crash history database");
    for statement in [
        "DELETE FROM thread_items WHERE thread_id = ?",
        "DELETE FROM thread_turns WHERE thread_id = ?",
        "DELETE FROM thread_history_projection_state WHERE thread_id = ?",
    ] {
        sqlx::query(statement)
            .bind(rollout_id.to_string())
            .execute(&pool)
            .await
            .expect("clear projection before process crash");
    }

    store
        .rebuild_history_projection(thread_id)
        .await
        .expect("configured projection crash must terminate the process");
    panic!("projection rebuild completed without configured process death");
}

#[tokio::test]
async fn projection_rebuild_process_death_recovers_every_publication_boundary() {
    for (boundary, projection_published, expected_state_count) in [
        ("after_staging", false, 1),
        ("before_projection_commit", false, 2),
        ("after_projection_commit", true, 1),
    ] {
        let home = TempDir::new().expect("create projection process-crash Codex home");
        let thread_id = ThreadId::new();
        let output = std::process::Command::new(
            std::env::current_exe().expect("current thread-store test executable"),
        )
        .arg("--exact")
        .arg("local::segment::tests::projection_rebuild_process_crash_child")
        .arg("--ignored")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PROJECTION_REBUILD_CRASH_HOME_ENV, home.path())
        .env(PROJECTION_REBUILD_CRASH_THREAD_ENV, thread_id.to_string())
        .env(PROJECTION_REBUILD_CRASH_BOUNDARY_ENV, boundary)
        .output()
        .expect("run projection crash subprocess");
        assert_eq!(
            output.status.code(),
            Some(PROJECTION_REBUILD_CRASH_EXIT_CODE),
            "boundary {boundary}; stdout: {}; stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let restarted = state_backed_store(home.path()).await;
        let marker_directory = home.path().join(".tmp/projection-rebuilds");
        assert_eq!(
            std::fs::read_dir(&marker_directory)
                .expect("read process-crash staging markers")
                .count(),
            1,
            "boundary {boundary}"
        );
        let pool = codex_state::open_thread_history_db(&restarted.config.sqlite)
            .await
            .expect("open restarted projection history database");
        let state_count =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM thread_history_projection_state")
                .fetch_one(&pool)
                .await
                .expect("count process-crash projection state");
        assert_eq!(state_count, expected_state_count, "boundary {boundary}");
        assert_eq!(
            restarted
                .has_history_projection(thread_id)
                .await
                .expect("inspect projection after process crash"),
            projection_published,
            "boundary {boundary}"
        );
        assert!(
            restarted
                .rebuild_history_projection(thread_id)
                .await
                .expect("recover process-crash staging"),
            "boundary {boundary}"
        );
        let turns = restarted
            .list_turns(crate::ListTurnsParams {
                thread_id,
                include_archived: true,
                cursor: None,
                page_size: 10,
                sort_direction: crate::SortDirection::Asc,
                items_view: crate::StoredTurnItemsView::Summary,
            })
            .await
            .expect("list turns after process-crash recovery");
        assert_eq!(
            turns
                .turns
                .iter()
                .map(|turn| turn.turn_id.as_str())
                .collect::<Vec<_>>(),
            vec!["projection-crash-turn-0", "projection-crash-turn-1"],
            "boundary {boundary}"
        );
        assert_eq!(
            std::fs::read_dir(&marker_directory)
                .expect("read cleaned process-crash marker directory")
                .count(),
            0,
            "boundary {boundary}"
        );
        let final_state_count =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM thread_history_projection_state")
                .fetch_one(&pool)
                .await
                .expect("count final projection after process-crash cleanup");
        assert_eq!(final_state_count, 1, "boundary {boundary}");
    }
}

#[tokio::test]
async fn projection_rebuild_recovers_abandoned_staging_after_task_termination() {
    let home = TempDir::new().expect("create abandoned-rebuild Codex home");
    let store = state_backed_store(home.path()).await;
    let thread_id = ThreadId::new();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create abandoned-rebuild thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist abandoned-rebuild thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("abandoned-rebuild rollout path");
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        stable_path,
        chrono::Utc::now(),
        SessionSource::Exec,
    );
    metadata.history_mode = ThreadHistoryMode::Paginated;
    metadata.cwd = home.path().to_path_buf();
    store
        .state_db()
        .await
        .expect("state runtime")
        .upsert_thread(&metadata.build(store.config.default_model_provider_id.as_str()))
        .await
        .expect("select abandoned-rebuild rollout");
    append_turn(
        &store,
        thread_id,
        "abandoned-turn",
        "abandoned-item",
        "abandoned rebuild message",
    )
    .await;
    store
        .flush_thread(thread_id)
        .await
        .expect("flush abandoned-rebuild thread");
    let rollout_id = store
        .resolve_rollout_lineage(thread_id)
        .await
        .expect("resolve abandoned-rebuild lineage")
        .root_rollout_id;
    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open abandoned-rebuild history database");
    for statement in [
        "DELETE FROM thread_items WHERE thread_id = ?",
        "DELETE FROM thread_turns WHERE thread_id = ?",
        "DELETE FROM thread_history_projection_state WHERE thread_id = ?",
    ] {
        sqlx::query(statement)
            .bind(rollout_id.to_string())
            .execute(&pool)
            .await
            .expect("clear projection before abandoned rebuild");
    }

    let pause = inject_projection_rebuild_pause(thread_id);
    let rebuild_store = store.clone();
    let rebuild =
        tokio::spawn(async move { rebuild_store.rebuild_history_projection(thread_id).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), pause.entered.notified())
        .await
        .expect("projection rebuild reaches abandoned staging boundary");
    rebuild.abort();
    assert!(
        rebuild
            .await
            .expect_err("aborted rebuild must not complete")
            .is_cancelled(),
        "test termination must cancel the staged rebuild"
    );
    let marker_directory = home.path().join(".tmp/projection-rebuilds");
    assert_eq!(
        std::fs::read_dir(&marker_directory)
            .expect("read abandoned staging markers")
            .count(),
        1
    );
    let staged_state_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM thread_history_projection_state")
            .fetch_one(&pool)
            .await
            .expect("count abandoned staging projection");
    assert_eq!(staged_state_count, 1);
    assert!(
        !store
            .has_history_projection(thread_id)
            .await
            .expect("abandoned staging remains invisible")
    );

    assert!(
        store
            .rebuild_history_projection(thread_id)
            .await
            .expect("retry abandoned projection rebuild")
    );
    assert!(
        store
            .has_history_projection(thread_id)
            .await
            .expect("accept projection after abandoned cleanup")
    );
    assert_eq!(
        std::fs::read_dir(&marker_directory)
            .expect("read cleaned staging marker directory")
            .count(),
        0
    );
    let final_state_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM thread_history_projection_state")
            .fetch_one(&pool)
            .await
            .expect("count final projection after abandoned cleanup");
    assert_eq!(final_state_count, 1);
}

#[tokio::test]
async fn projection_rebuild_rejects_stale_and_ahead_active_offsets() {
    let home = TempDir::new().expect("create projection-offset Codex home");
    let store = state_backed_store(home.path()).await;
    let thread_id = ThreadId::new();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create projection-offset thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist projection-offset thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("projection-offset rollout path");
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        stable_path.clone(),
        chrono::Utc::now(),
        SessionSource::Exec,
    );
    metadata.history_mode = ThreadHistoryMode::Paginated;
    metadata.cwd = home.path().to_path_buf();
    store
        .state_db()
        .await
        .expect("state runtime")
        .upsert_thread(&metadata.build(store.config.default_model_provider_id.as_str()))
        .await
        .expect("select projection-offset rollout");
    for index in 0..2 {
        append_turn(
            &store,
            thread_id,
            format!("projection-offset-turn-{index}").as_str(),
            format!("projection-offset-item-{index}").as_str(),
            format!("projection offset message {index}").as_str(),
        )
        .await;
        if index == 0 {
            store
                .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
                .await
                .expect("rotate projection-offset segment");
        }
    }
    store
        .flush_thread(thread_id)
        .await
        .expect("flush projection-offset thread");
    let rollout_id = store
        .resolve_rollout_lineage(thread_id)
        .await
        .expect("resolve projection-offset lineage")
        .root_rollout_id;
    let active_len = tokio::fs::metadata(stable_path.as_path())
        .await
        .expect("read projection-offset active metadata")
        .len();
    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open projection-offset history database");

    for corrupt_offset in [active_len - 1, active_len + 1] {
        sqlx::query(
            "UPDATE thread_history_projection_state \
             SET next_rollout_byte_offset = ? WHERE thread_id = ?",
        )
        .bind(i64::try_from(corrupt_offset).expect("projection offset fits SQLite"))
        .bind(rollout_id.to_string())
        .execute(&pool)
        .await
        .expect("corrupt projection offset");
        assert!(
            !store
                .has_history_projection(thread_id)
                .await
                .expect("reject corrupt projection offset"),
            "offset {corrupt_offset} must not satisfy projected reads"
        );
        let fallback = store
            .read_thread_by_rollout_path(
                stable_path.clone(),
                /*include_archived*/ true,
                /*include_history*/ true,
            )
            .await
            .expect("read canonical history with corrupt projection")
            .history
            .expect("fallback history with corrupt projection")
            .items;
        for index in 0..2 {
            assert_eq!(
                turn_message_count(
                    fallback.as_slice(),
                    format!("projection offset message {index}").as_str()
                ),
                1
            );
        }
        assert!(
            store
                .rebuild_history_projection(thread_id)
                .await
                .expect("rebuild corrupt projection")
        );
        assert!(
            store
                .has_history_projection(thread_id)
                .await
                .expect("accept repaired projection")
        );
    }
}

#[tokio::test]
async fn projection_integrity_triggers_reject_missing_middle_rows() {
    let home = TempDir::new().expect("create projection-integrity Codex home");
    let store = state_backed_store(home.path()).await;
    let thread_id = ThreadId::new();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create projection-integrity thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist projection-integrity thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("projection-integrity rollout path");
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        stable_path.clone(),
        chrono::Utc::now(),
        SessionSource::Exec,
    );
    metadata.history_mode = ThreadHistoryMode::Paginated;
    metadata.cwd = home.path().to_path_buf();
    store
        .state_db()
        .await
        .expect("state runtime")
        .upsert_thread(&metadata.build(store.config.default_model_provider_id.as_str()))
        .await
        .expect("select projection-integrity rollout");
    for index in 0..3 {
        append_turn(
            &store,
            thread_id,
            format!("projection-integrity-turn-{index}").as_str(),
            format!("projection-integrity-item-{index}").as_str(),
            format!("projection integrity message {index}").as_str(),
        )
        .await;
        if index < 2 {
            store
                .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
                .await
                .expect("rotate projection-integrity segment");
        }
    }
    store
        .flush_thread(thread_id)
        .await
        .expect("flush projection-integrity thread");
    assert!(
        store
            .has_history_projection(thread_id)
            .await
            .expect("accept complete projection")
    );

    let rollout_id = store
        .resolve_rollout_lineage(thread_id)
        .await
        .expect("resolve projection-integrity lineage")
        .root_rollout_id;
    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open projection-integrity history database");
    let trigger_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_master \
         WHERE type = 'trigger' AND name LIKE 'frodex_thread_%_projection_%'",
    )
    .fetch_one(&pool)
    .await
    .expect("count projection-integrity triggers");
    assert_eq!(trigger_count, 6);

    for (table, statement) in [
        (
            "thread_items",
            "DELETE FROM thread_items WHERE thread_id = ? \
             AND turn_id = 'projection-integrity-turn-1'",
        ),
        (
            "thread_turns",
            "DELETE FROM thread_turns WHERE thread_id = ? \
             AND turn_id = 'projection-integrity-turn-1'",
        ),
    ] {
        let deleted = sqlx::query(statement)
            .bind(rollout_id.to_string())
            .execute(&pool)
            .await
            .expect("delete one middle projection row");
        assert!(
            deleted.rows_affected() > 0,
            "{table} mutation must change rows"
        );
        let encoded_offset = sqlx::query_scalar::<_, i64>(
            "SELECT next_rollout_byte_offset FROM thread_history_projection_state \
             WHERE thread_id = ?",
        )
        .bind(rollout_id.to_string())
        .fetch_one(&pool)
        .await
        .expect("read invalidated projection marker");
        assert!(
            encoded_offset < 0,
            "{table} deletion must invalidate projection"
        );
        assert!(
            !store
                .has_history_projection(thread_id)
                .await
                .expect("reject projection with a middle-row hole")
        );
        let fallback = store
            .read_thread_by_rollout_path(
                stable_path.clone(),
                /*include_archived*/ true,
                /*include_history*/ true,
            )
            .await
            .expect("read canonical history after projection corruption")
            .history
            .expect("fallback history after projection corruption")
            .items;
        for index in 0..3 {
            assert_eq!(
                turn_message_count(
                    fallback.as_slice(),
                    format!("projection integrity message {index}").as_str()
                ),
                1
            );
        }
        assert!(
            store
                .rebuild_history_projection(thread_id)
                .await
                .expect("rebuild projection after middle-row hole")
        );
        assert!(
            store
                .has_history_projection(thread_id)
                .await
                .expect("accept rebuilt projection")
        );
    }
}

#[tokio::test]
async fn external_writer_lock_rejects_segment_publication_and_fork_preparation() {
    let home = TempDir::new().expect("temp dir");
    let store = state_backed_store(home.path()).await;
    let thread_id = ThreadId::new();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create paginated thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist paginated metadata");
    append_message(&store, thread_id, "locked source").await;
    store.flush_thread(thread_id).await.expect("flush source");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    let stable_bytes = tokio::fs::read(stable_path.as_path())
        .await
        .expect("read stable source");
    store
        .shutdown_thread(thread_id)
        .await
        .expect("release store writer");

    let competing = Arc::new(WriterLockCoordinator::new(home.path()));
    let _competing_writer = competing
        .acquire(thread_id)
        .expect("acquire external writer lock");

    for params in [
        FreezeRolloutSegmentParams::snapshot(),
        FreezeRolloutSegmentParams::rotate(Vec::new()),
    ] {
        let error = store
            .freeze_thread_segment(thread_id, params)
            .await
            .expect_err("external writer must reject segment publication");
        assert!(matches!(error, ThreadStoreError::Conflict { .. }));
    }
    for boundary in [
        ForkBoundary::Latest,
        ForkBoundary::BeforeTurn("unread-turn".to_string()),
    ] {
        let error = store
            .prepare_fork(PrepareForkParams {
                thread_id,
                boundary,
            })
            .await
            .expect_err("external writer must reject fork preparation");
        assert!(matches!(error, ThreadStoreError::Conflict { .. }));
    }

    assert_eq!(
        tokio::fs::read(stable_path.as_path())
            .await
            .expect("read unchanged source"),
        stable_bytes
    );
    assert!(
        !tokio::fs::try_exists(
            home.path()
                .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        )
        .await
        .expect("check immutable root")
    );
}

#[tokio::test]
async fn direct_snapshot_reserves_a_mutable_referenced_owner_in_uuid_order() {
    let low = ThreadId::from_string("00000000-0000-4000-8000-000000000001").expect("low thread id");
    let high =
        ThreadId::from_string("ffffffff-ffff-4fff-bfff-ffffffffffff").expect("high thread id");
    for (child_id, parent_id) in [(low, high), (high, low)] {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        store
            .create_thread(create_params(parent_id, ThreadHistoryMode::Legacy))
            .await
            .expect("create parent");
        store
            .persist_thread(parent_id, PersistContext::Standard)
            .await
            .expect("persist parent");
        append_message(&store, parent_id, "mutable parent").await;
        store.flush_thread(parent_id).await.expect("flush parent");
        let parent_path = store
            .live_rollout_path(parent_id)
            .await
            .expect("parent path");
        let parent_segment_id = codex_rollout::read_session_meta_line(parent_path.as_path())
            .await
            .expect("read parent metadata")
            .meta
            .segment_id;

        store
            .create_thread(create_params(child_id, ThreadHistoryMode::Legacy))
            .await
            .expect("create child");
        store
            .persist_thread(child_id, PersistContext::Standard)
            .await
            .expect("persist child");
        store
            .append_items(AppendThreadItemsParams {
                thread_id: child_id,
                items: vec![RolloutItem::RolloutReference(RolloutReferenceItem {
                    rollout_id: Some(parent_id),
                    rollout_path: parent_path,
                    thread_id: Some(parent_id),
                    rollout_timestamp: None,
                    segment_id: parent_segment_id,
                    max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
                    nth_user_message: None,
                    compacted_replacement_history_filter_texts: None,
                })],
            })
            .await
            .expect("append mutable parent reference");
        store.flush_thread(child_id).await.expect("flush child");
        store
            .shutdown_thread(parent_id)
            .await
            .expect("release parent writer");
        store
            .shutdown_thread(child_id)
            .await
            .expect("release child writer");

        let competing = Arc::new(WriterLockCoordinator::new(home.path()));
        let _parent_writer = competing
            .acquire(parent_id)
            .expect("acquire external parent writer");
        let error = store
            .freeze_thread_segment(child_id, FreezeRolloutSegmentParams::snapshot())
            .await
            .expect_err("mutable parent writer must reject direct snapshot");
        assert!(matches!(error, ThreadStoreError::Conflict { .. }));
        assert!(
            !tokio::fs::try_exists(
                home.path()
                    .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
            )
            .await
            .expect("check immutable root")
        );
    }
}

#[tokio::test]
async fn direct_snapshot_does_not_reserve_a_valid_immutable_referenced_owner() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let parent_id = ThreadId::new();
    store
        .create_thread(create_params(parent_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create parent");
    store
        .persist_thread(parent_id, PersistContext::Standard)
        .await
        .expect("persist parent");
    append_message(&store, parent_id, "immutable parent").await;
    let parent = store
        .freeze_thread_segment(parent_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("snapshot parent");
    store
        .shutdown_thread(parent_id)
        .await
        .expect("release parent writer");

    let child_id = ThreadId::new();
    store
        .create_thread(create_params(child_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create child");
    store
        .persist_thread(child_id, PersistContext::Standard)
        .await
        .expect("persist child");
    store
        .append_items(AppendThreadItemsParams {
            thread_id: child_id,
            items: vec![RolloutItem::RolloutReference(parent.reference.clone())],
        })
        .await
        .expect("append immutable parent reference");
    store.flush_thread(child_id).await.expect("flush child");
    store
        .shutdown_thread(child_id)
        .await
        .expect("release child writer");
    assert!(
        super::reference_has_valid_recorded_immutable_candidate(
            &store,
            &parent.reference,
            parent_id,
        )
        .await,
        "the recorded parent reference must authenticate its immutable snapshot"
    );

    let competing = Arc::new(WriterLockCoordinator::new(home.path()));
    let _parent_writer = competing
        .acquire(parent_id)
        .expect("acquire external parent writer");
    let child_snapshot = store
        .freeze_thread_segment(child_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .unwrap_or_else(|error| {
            panic!(
                "immutable parent {parent_id} must not block child {child_id} snapshot: {error:?}"
            )
        });
    assert_eq!(
        child_snapshot.reference.rollout_path,
        parent.reference.rollout_path
    );
    assert_eq!(
        child_snapshot.reference.segment_id,
        parent.reference.segment_id
    );
}

#[tokio::test]
async fn resumed_segmented_legacy_history_preserves_projected_item_identity() {
    let home = TempDir::new().expect("temp dir");
    let thread_id = ThreadId::new();
    let params = create_params(thread_id, ThreadHistoryMode::Legacy);
    let metadata = params.metadata.clone();
    let store = state_backed_store(home.path()).await;
    store
        .create_thread(params)
        .await
        .expect("create legacy thread");
    append_message(&store, thread_id, "first indexed message").await;
    store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("index first legacy segment");
    append_message(&store, thread_id, "second indexed message").await;
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("read active rollout path");
    let history = store
        .read_thread_by_rollout_path(
            rollout_path.clone(),
            /*include_archived*/ true,
            /*include_history*/ true,
        )
        .await
        .expect("load canonical resumed history")
        .history
        .expect("canonical resumed history")
        .items;
    store
        .shutdown_thread(thread_id)
        .await
        .expect("shutdown initial recorder");

    let rollout_id = ThreadId::new();
    let file_name = rollout_path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("canonical rollout file name");
    let selected_path = rollout_path.with_file_name(format!(
        "{}_{rollout_id}.jsonl",
        file_name
            .strip_suffix(".jsonl")
            .expect("plain rollout suffix")
    ));
    tokio::fs::rename(rollout_path.as_path(), selected_path.as_path())
        .await
        .expect("rename selected physical rollout");
    let state_db = store.state_db().await.expect("state runtime");
    let mut selected_metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        selected_path.clone(),
        chrono::Utc::now(),
        SessionSource::Exec,
    );
    selected_metadata.history_mode = ThreadHistoryMode::Legacy;
    selected_metadata.cwd = home.path().to_path_buf();
    state_db
        .upsert_thread(&selected_metadata.build(store.config.default_model_provider_id.as_str()))
        .await
        .expect("select replacement physical rollout");
    let pool = codex_state::open_thread_history_db(&store.config.sqlite)
        .await
        .expect("open existing history projection");
    let original_thread_projection = sqlx::query_as::<_, (i64, i64, i64)>(
        r#"
SELECT
    (SELECT COUNT(*) FROM thread_turns WHERE thread_id = ?),
    (SELECT COUNT(*) FROM thread_items WHERE thread_id = ?),
    next_rollout_byte_offset
FROM thread_history_projection_state
WHERE thread_id = ?
"#,
    )
    .bind(thread_id.to_string())
    .bind(thread_id.to_string())
    .bind(thread_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("snapshot original thread projection");

    let resumed = state_backed_store(home.path()).await;
    super::super::live_writer::backfill_segmented_legacy_projection(
        &resumed,
        thread_id,
        selected_path.as_path(),
    )
    .await
    .expect("backfill selected physical rollout projection");
    assert_eq!(
        resumed
            .projected_history_position(thread_id)
            .await
            .expect("read selected projection position")
            .expect("selected projection position")
            .thread_id,
        rollout_id
    );
    resumed
        .resume_thread(ResumeThreadParams {
            thread_id,
            rollout_path: Some(selected_path.clone()),
            history: Some(Arc::new(history)),
            include_archived: true,
            metadata,
        })
        .await
        .expect("restore canonical legacy history reducer");
    append_message(&resumed, thread_id, "third indexed message").await;
    let frozen = resumed
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("preserve restored projection across rotation");
    assert_eq!(frozen.reference.thread_id, Some(thread_id));
    assert_eq!(frozen.reference.rollout_id, Some(rollout_id));
    append_message(&resumed, thread_id, "fourth indexed message").await;

    let canonical = resumed
        .read_thread_by_rollout_path(
            selected_path.clone(),
            /*include_archived*/ true,
            /*include_history*/ true,
        )
        .await
        .expect("load canonical history after resume and rotation")
        .history
        .expect("canonical history after resume")
        .items;
    let mut builder = ThreadHistoryBuilder::new();
    for item in &canonical {
        if codex_rollout::is_persisted_rollout_item(item, ThreadHistoryMode::Legacy) {
            builder.handle_rollout_item_with_changes(item);
        }
    }
    let expected_items: Vec<(String, String, serde_json::Value)> = builder
        .finish()
        .into_iter()
        .flat_map(|turn| {
            turn.items.into_iter().map(move |item| {
                (
                    turn.id.clone(),
                    item.id().to_string(),
                    serde_json::to_value(item).expect("serialize canonical history item"),
                )
            })
        })
        .collect();
    let pool = codex_state::open_thread_history_db(&resumed.config.sqlite)
        .await
        .expect("open existing history projection");
    let indexed_items = sqlx::query_as::<_, (String, String, String)>(
        "SELECT turn_id, item_id, item_json FROM thread_items WHERE thread_id = ? ORDER BY rollout_ordinal",
    )
    .bind(rollout_id.to_string())
    .fetch_all(&pool)
    .await
    .expect("read projected history items")
    .into_iter()
    .map(|(turn_id, item_id, item_json)| {
        (
            turn_id,
            item_id,
            serde_json::from_str(&item_json).expect("decode projected history item"),
        )
    })
    .collect::<Vec<_>>();
    assert_eq!(indexed_items, expected_items);
    let projected_offset = super::super::thread_history::projection_state(&resumed, rollout_id)
        .await
        .expect("read resumed projection state")
        .expect("resumed history projection exists")
        .next_byte_offset;
    assert_eq!(
        projected_offset,
        tokio::fs::metadata(selected_path)
            .await
            .expect("read resumed rollout length")
            .len()
    );
    let preserved_thread_projection = sqlx::query_as::<_, (i64, i64, i64)>(
        r#"
SELECT
    (SELECT COUNT(*) FROM thread_turns WHERE thread_id = ?),
    (SELECT COUNT(*) FROM thread_items WHERE thread_id = ?),
    next_rollout_byte_offset
FROM thread_history_projection_state
WHERE thread_id = ?
"#,
    )
    .bind(thread_id.to_string())
    .bind(thread_id.to_string())
    .bind(thread_id.to_string())
    .fetch_one(&pool)
    .await
    .expect("read preserved original thread projection");
    assert_eq!(preserved_thread_projection, original_thread_projection);
}

#[tokio::test]
async fn segmented_legacy_resume_without_canonical_history_never_claims_partial_projection() {
    let home = TempDir::new().expect("temp dir");
    let thread_id = ThreadId::new();
    let params = create_params(thread_id, ThreadHistoryMode::Legacy);
    let metadata = params.metadata.clone();
    let store = state_backed_store(home.path()).await;
    store
        .create_thread(params)
        .await
        .expect("create legacy thread");
    append_message(&store, thread_id, "immutable predecessor message").await;
    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("index initial legacy history");
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("read active rollout path");
    store
        .shutdown_thread(thread_id)
        .await
        .expect("shutdown initial recorder");
    let predecessor_path = frozen.reference.rollout_path;
    tokio::fs::remove_file(predecessor_path)
        .await
        .expect("remove unavailable immutable predecessor");

    let resumed = state_backed_store(home.path()).await;
    resumed
        .resume_thread(ResumeThreadParams {
            thread_id,
            rollout_path: Some(rollout_path),
            history: None,
            include_archived: true,
            metadata,
        })
        .await
        .expect("resume without claiming complete legacy history");
    resumed
        .flush_thread(thread_id)
        .await
        .expect("resume flush must not replay missing predecessor history");
    append_message(&resumed, thread_id, "unindexed resumed message").await;

    let pool = codex_state::open_thread_history_db(&resumed.config.sqlite)
        .await
        .expect("open existing history database");
    let projected_offset = sqlx::query_scalar::<_, i64>(
        "SELECT next_rollout_byte_offset FROM thread_history_projection_state WHERE thread_id = ?",
    )
    .bind(thread_id.to_string())
    .fetch_optional(&pool)
    .await
    .expect("check incomplete projection is not presented as current");
    let final_path = resumed
        .live_rollout_path(thread_id)
        .await
        .expect("read resumed rollout path");
    let final_len = tokio::fs::metadata(final_path)
        .await
        .expect("read resumed rollout length")
        .len();
    assert_ne!(
        projected_offset.and_then(|offset| u64::try_from(offset).ok()),
        Some(final_len)
    );
    assert!(
        !resumed
            .live_recorders
            .lock()
            .await
            .get(&thread_id)
            .expect("resumed recorder")
            .legacy_history_projection_enabled
    );
}

#[tokio::test]
async fn repeated_freeze_without_local_items_reuses_existing_reference() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist thread");
    append_message(&store, thread_id, "shared prefix").await;

    let first = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("freeze first segment");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    let stable_bytes = tokio::fs::read(stable_path.as_path())
        .await
        .expect("read stable rollout");

    let second = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("reuse frozen segment");

    assert_eq!(second.reference.rollout_path, first.reference.rollout_path);
    assert_eq!(second.reference.segment_id, first.reference.segment_id);
    assert_eq!(
        tokio::fs::read(stable_path)
            .await
            .expect("reread stable rollout"),
        stable_bytes
    );
}

#[tokio::test]
async fn snapshot_stabilizes_nested_references_to_legacy_mutable_rollouts() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let parent_id = ThreadId::default();
    store
        .create_thread(create_params(parent_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create parent");
    store
        .persist_thread(parent_id, PersistContext::Standard)
        .await
        .expect("persist parent");
    append_message(&store, parent_id, "stable inherited prefix").await;
    store.flush_thread(parent_id).await.expect("flush parent");
    let parent_path = store
        .live_rollout_path(parent_id)
        .await
        .expect("parent rollout path");
    let parent_meta = codex_rollout::read_session_meta_line(parent_path.as_path())
        .await
        .expect("read parent metadata");
    let parent_reference = RolloutReferenceItem {
        rollout_id: None,
        rollout_path: parent_path,
        thread_id: Some(parent_id),
        rollout_timestamp: None,
        segment_id: parent_meta.meta.segment_id,
        max_depth: codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };

    let child_id = ThreadId::default();
    store
        .create_thread(create_params(child_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create child");
    store
        .persist_thread(child_id, PersistContext::Standard)
        .await
        .expect("persist child");
    store
        .append_items(AppendThreadItemsParams {
            thread_id: child_id,
            items: vec![RolloutItem::RolloutReference(parent_reference)],
        })
        .await
        .expect("append legacy child reference");
    store.flush_thread(child_id).await.expect("flush child");
    let child_path = store
        .live_rollout_path(child_id)
        .await
        .expect("child rollout path");
    let child_meta = codex_rollout::read_session_meta_line(child_path.as_path())
        .await
        .expect("read child metadata");

    let grandchild_id = ThreadId::default();
    store
        .create_thread(create_params(grandchild_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create grandchild");
    store
        .persist_thread(grandchild_id, PersistContext::Standard)
        .await
        .expect("persist grandchild");
    store
        .append_items(AppendThreadItemsParams {
            thread_id: grandchild_id,
            items: vec![RolloutItem::RolloutReference(RolloutReferenceItem {
                rollout_id: None,
                rollout_path: child_path,
                thread_id: Some(child_id),
                rollout_timestamp: None,
                segment_id: child_meta.meta.segment_id,
                max_depth: codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
                nth_user_message: None,
                compacted_replacement_history_filter_texts: None,
            })],
        })
        .await
        .expect("append legacy grandchild reference");
    store
        .flush_thread(grandchild_id)
        .await
        .expect("flush grandchild");

    let frozen = store
        .freeze_thread_segment(grandchild_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("freeze legacy reference graph");
    append_message(&store, parent_id, "later mutable parent append").await;
    store.flush_thread(parent_id).await.expect("flush append");

    let frozen_lines = RolloutRecorder::load_rollout_lines(frozen.reference.rollout_path.as_path())
        .await
        .expect("load frozen child segment")
        .0;
    let RolloutItem::RolloutReference(stabilized_parent) = &frozen_lines[1].item else {
        panic!("frozen child segment must retain its parent reference");
    };
    assert!(
        stabilized_parent.rollout_path.starts_with(
            home.path()
                .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        )
    );
    let logical_items = codex_rollout::materialize_rollout_items(
        home.path(),
        frozen.reference.rollout_path.as_path(),
    )
    .await
    .expect("materialize stabilized graph");
    assert!(has_message(&logical_items, "stable inherited prefix"));
    assert!(!has_message(&logical_items, "later mutable parent append"));
}

#[tokio::test]
async fn snapshot_stabilizes_512_same_thread_segments() {
    const SEGMENT_COUNT: usize = 512;

    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::new();
    let segment_ids = (0..SEGMENT_COUNT)
        .map(|_| SegmentId::new())
        .collect::<Vec<_>>();
    let paths = segment_ids
        .iter()
        .map(|segment_id| {
            home.path()
                .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
                .join(thread_id.to_string())
                .join(segment_id.to_string())
                .join(format!("rollout-2026-08-03T00-00-00-{thread_id}.jsonl"))
        })
        .collect::<Vec<_>>();

    for index in 0..SEGMENT_COUNT {
        let mut lines = vec![RolloutLine {
            timestamp: "2026-08-03T00:00:00Z".to_string(),
            ordinal: Some(0),
            item: RolloutItem::SessionMeta(SessionMetaLine {
                meta: SessionMeta {
                    session_id: thread_id.into(),
                    id: thread_id,
                    segment_id: Some(segment_ids[index]),
                    history_mode: ThreadHistoryMode::Legacy,
                    ..SessionMeta::default()
                },
                git: None,
            }),
        }];
        if let Some(previous_index) = index.checked_sub(1) {
            lines.push(RolloutLine {
                timestamp: "2026-08-03T00:00:01Z".to_string(),
                ordinal: Some(1),
                item: RolloutItem::RolloutReference(RolloutReferenceItem {
                    rollout_id: None,
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
        std::fs::create_dir_all(paths[index].parent().expect("segment directory"))
            .expect("create immutable segment directory");
        let records = lines
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .expect("serialize immutable segment");
        std::fs::write(paths[index].as_path(), format!("{}\n", records.join("\n")))
            .expect("write immutable segment");
    }

    let root_reference = RolloutReferenceItem {
        rollout_id: None,
        rollout_path: paths[SEGMENT_COUNT - 1].clone(),
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(segment_ids[SEGMENT_COUNT - 1]),
        max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };
    let mut active_references = std::collections::HashSet::new();
    let reservation = store
        .reserve_rollout_writers(&[])
        .await
        .expect("reserve immutable snapshot traversal");
    let stabilized = stabilize_rollout_reference(
        &store,
        root_reference.clone(),
        &mut active_references,
        /*depth*/ 0,
        &reservation,
    )
    .await
    .expect("ordinary immutable segments must not exhaust fork depth");

    assert_eq!(stabilized.rollout_path, root_reference.rollout_path);
    assert!(active_references.is_empty());

    let overflow_segment = SegmentId::new();
    let overflow_path = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(overflow_segment.to_string())
        .join(format!("rollout-2026-08-03T00-00-00-{thread_id}.jsonl"));
    std::fs::create_dir_all(overflow_path.parent().expect("segment directory"))
        .expect("create overflowing segment directory");
    let overflow_lines = [
        RolloutLine {
            timestamp: "2026-08-03T00:00:00Z".to_string(),
            ordinal: Some(0),
            item: RolloutItem::SessionMeta(SessionMetaLine {
                meta: SessionMeta {
                    session_id: thread_id.into(),
                    id: thread_id,
                    segment_id: Some(overflow_segment),
                    history_mode: ThreadHistoryMode::Legacy,
                    ..SessionMeta::default()
                },
                git: None,
            }),
        },
        RolloutLine {
            timestamp: "2026-08-03T00:00:01Z".to_string(),
            ordinal: Some(1),
            item: RolloutItem::RolloutReference(root_reference),
        },
    ];
    let records = overflow_lines
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .expect("serialize overflowing segment");
    std::fs::write(overflow_path.as_path(), format!("{}\n", records.join("\n")))
        .expect("write overflowing segment");
    let overflow_reference = RolloutReferenceItem {
        rollout_id: None,
        rollout_path: overflow_path.clone(),
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(overflow_segment),
        max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };
    let stabilized = stabilize_rollout_reference(
        &store,
        overflow_reference,
        &mut active_references,
        /*depth*/ 0,
        &reservation,
    )
    .await
    .expect("same-thread snapshots must remain readable beyond 512 segments");
    assert_eq!(stabilized.rollout_path, overflow_path);
    assert!(active_references.is_empty());
}

#[tokio::test]
async fn snapshot_rejects_cross_thread_references_past_fork_depth_limit() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let reference = RolloutReferenceItem {
        rollout_id: None,
        rollout_path: home.path().join("unresolved-cross-thread.jsonl"),
        thread_id: Some(ThreadId::new()),
        rollout_timestamp: None,
        segment_id: Some(SegmentId::new()),
        max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };
    let reservation = store
        .reserve_rollout_writers(&[])
        .await
        .expect("reserve bounded traversal");

    let error = stabilize_rollout_reference(
        &store,
        reference,
        &mut std::collections::HashSet::new(),
        codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
        &reservation,
    )
    .await
    .expect_err("fork depth must remain bounded");

    assert!(error.to_string().contains("maximum depth"));
}

#[tokio::test]
async fn snapshots_after_append_get_new_identity_and_unchanged_snapshots_reuse_it() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist thread");
    append_message(&store, thread_id, "first snapshot").await;

    let first = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("freeze first snapshot");
    append_message(&store, thread_id, "second snapshot").await;
    store.flush_thread(thread_id).await.expect("flush append");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    let stable_bytes = tokio::fs::read(stable_path.as_path())
        .await
        .expect("read stable rollout");

    let second = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("freeze changed snapshot");
    let third = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("reuse unchanged snapshot");

    assert_ne!(first.reference.segment_id, second.reference.segment_id);
    assert_ne!(first.reference.rollout_path, second.reference.rollout_path);
    assert_eq!(third.reference.segment_id, second.reference.segment_id);
    assert_eq!(third.reference.rollout_path, second.reference.rollout_path);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = tokio::fs::metadata(second.reference.rollout_path.as_path())
            .await
            .expect("read snapshot permissions")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
    let installed_lines =
        RolloutRecorder::load_rollout_lines(second.reference.rollout_path.as_path())
            .await
            .expect("load installed snapshot")
            .0;
    assert_eq!(
        snapshot_segment_id(installed_lines.as_slice()).expect("rehash installed snapshot"),
        second.reference.segment_id.expect("snapshot segment ID")
    );
    assert_eq!(
        tokio::fs::read(stable_path)
            .await
            .expect("reread stable rollout"),
        stable_bytes
    );
}

#[tokio::test]
async fn snapshot_identity_ignores_source_segment_id_and_object_insertion_order() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    let mut first = RolloutRecorder::load_rollout_lines(stable_path.as_path())
        .await
        .expect("load first source")
        .0;
    let mut second = first.clone();
    let RolloutItem::SessionMeta(first_meta) = &mut first[0].item else {
        panic!("first source must start with session metadata");
    };
    first_meta.meta.segment_id = Some(SegmentId::new());
    let RolloutItem::SessionMeta(second_meta) = &mut second[0].item else {
        panic!("second source must start with session metadata");
    };
    second_meta.meta.segment_id = Some(SegmentId::new());

    let mut first_state = serde_json::Map::new();
    first_state.insert("z".to_string(), serde_json::json!(1));
    first_state.insert("a".to_string(), serde_json::json!(2));
    let mut second_state = serde_json::Map::new();
    second_state.insert("a".to_string(), serde_json::json!(2));
    second_state.insert("z".to_string(), serde_json::json!(1));
    first.push(RolloutLine {
        timestamp: "2026-07-14T00:00:00Z".to_string(),
        ordinal: None,
        item: RolloutItem::WorldState(WorldStateItem::full(first_state)),
    });
    second.push(RolloutLine {
        timestamp: "2026-07-14T00:00:00Z".to_string(),
        ordinal: None,
        item: RolloutItem::WorldState(WorldStateItem::full(second_state)),
    });

    assert_eq!(
        snapshot_segment_id(first.as_slice()).expect("hash first source"),
        snapshot_segment_id(second.as_slice()).expect("hash second source")
    );
}

#[tokio::test]
async fn full_history_child_storage_does_not_scale_with_parent_history() {
    for history_mode in [ThreadHistoryMode::Legacy, ThreadHistoryMode::Paginated] {
        let (small_parent_bytes, small_children_bytes) =
            stored_parent_and_children_bytes(history_mode, /*parent_message_count*/ 1).await;
        let (large_parent_bytes, large_children_bytes) =
            stored_parent_and_children_bytes(history_mode, /*parent_message_count*/ 128).await;

        assert!(
            large_parent_bytes > small_parent_bytes.saturating_add(32 * 1_024),
            "large parent fixture must be substantially larger for {history_mode:?}: \
             small={small_parent_bytes}, large={large_parent_bytes}"
        );
        assert!(
            large_children_bytes.abs_diff(small_children_bytes) < 1_024,
            "child rollout storage must not scale with parent history for {history_mode:?}: \
             small={small_children_bytes}, large={large_children_bytes}"
        );
    }
}

#[tokio::test]
async fn segmentless_legacy_history_freezes_under_initial() {
    assert_segmentless_source_freezes(ThreadHistoryMode::Legacy).await;
}

#[tokio::test]
async fn segmentless_paginated_history_freezes_under_initial() {
    assert_segmentless_source_freezes(ThreadHistoryMode::Paginated).await;
}

#[tokio::test]
async fn legacy_rotation_canonicalizes_malformed_historical_records() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create legacy thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist legacy thread");
    append_message(&store, thread_id, "before malformed records").await;
    store.flush_thread(thread_id).await.expect("flush thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    let original_meta = codex_rollout::read_session_meta_line(stable_path.as_path())
        .await
        .expect("read source metadata");
    append_malformed_historical_records(stable_path.as_path()).await;

    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("legacy rotation should discard unreadable ordinary records");
    let (segment_items, segment_thread_id, parse_errors) =
        RolloutRecorder::load_rollout_items(frozen.reference.rollout_path.as_path())
            .await
            .expect("read sanitized immutable segment");
    assert_eq!(segment_thread_id, Some(thread_id));
    assert_eq!(parse_errors, 0);
    assert!(has_message(&segment_items, "before malformed records"));
    let segment_meta =
        codex_rollout::read_session_meta_line(frozen.reference.rollout_path.as_path())
            .await
            .expect("read sanitized segment metadata");
    assert_eq!(segment_meta.meta.segment_id, original_meta.meta.segment_id);
    assert_eq!(frozen.reference.segment_id, original_meta.meta.segment_id);

    append_message(&store, thread_id, "after malformed records").await;
    store
        .flush_thread(thread_id)
        .await
        .expect("flush continuation");
    let logical_items =
        codex_rollout::materialize_rollout_items(home.path(), stable_path.as_path())
            .await
            .expect("strict reference reader should accept sanitized predecessor");
    assert!(has_message(&logical_items, "before malformed records"));
    assert!(has_message(&logical_items, "after malformed records"));
}

#[tokio::test]
async fn legacy_snapshot_canonicalizes_malformed_historical_records() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create legacy thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist legacy thread");
    append_message(&store, thread_id, "before malformed records").await;
    store.flush_thread(thread_id).await.expect("flush thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    append_malformed_historical_records(stable_path.as_path()).await;

    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("legacy snapshot should discard unreadable ordinary records");
    let (segment_items, segment_thread_id, parse_errors) =
        RolloutRecorder::load_rollout_items(frozen.reference.rollout_path.as_path())
            .await
            .expect("read sanitized snapshot");
    assert_eq!(segment_thread_id, Some(thread_id));
    assert_eq!(parse_errors, 0);
    assert!(has_message(&segment_items, "before malformed records"));
}

#[tokio::test]
async fn paginated_rotation_rejects_malformed_historical_records() {
    let home = TempDir::new().expect("temp dir");
    let store = state_backed_store(home.path()).await;
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create paginated thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist paginated thread");
    append_message(&store, thread_id, "before malformed records").await;
    store.flush_thread(thread_id).await.expect("flush thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    append_malformed_historical_records(stable_path.as_path()).await;

    let error = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect_err("paginated rollouts must reject skipped historical records");
    assert!(error.to_string().contains("invalid record"));
}

#[tokio::test]
async fn legacy_rotation_rejects_malformed_rollout_reference_records() {
    for reference_type in ["rollout_reference", "fork_reference"] {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let thread_id = ThreadId::default();
        store
            .create_thread(create_params(thread_id, ThreadHistoryMode::Legacy))
            .await
            .expect("create legacy thread");
        store
            .persist_thread(thread_id, PersistContext::Standard)
            .await
            .expect("persist legacy thread");
        store.flush_thread(thread_id).await.expect("flush thread");
        let stable_path = store
            .live_rollout_path(thread_id)
            .await
            .expect("stable path");
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(stable_path.as_path())
            .await
            .expect("open legacy rollout");
        let reference = serde_json::json!({
            "timestamp": "2025-01-03T12:00:01Z",
            "type": reference_type,
            "payload": {},
        });
        file.write_all(
            serde_json::to_string(&reference)
                .expect("serialize malformed reference")
                .as_bytes(),
        )
        .await
        .expect("append malformed reference");
        file.write_all(b"\n")
            .await
            .expect("finish malformed reference");
        file.flush().await.expect("flush malformed reference");

        let error = store
            .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
            .await
            .expect_err("malformed rollout references must remain fatal");
        assert!(error.to_string().contains("invalid rollout reference"));
    }
}

#[tokio::test]
async fn immutable_install_copies_source_and_rejects_different_existing_contents() {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    let home = TempDir::new().expect("temp dir");
    let source = home.path().join("source.jsonl");
    let destination = home.path().join("segments").join("segment.jsonl");
    tokio::fs::write(source.as_path(), b"frozen prefix")
        .await
        .expect("write source");
    #[cfg(unix)]
    tokio::fs::set_permissions(source.as_path(), std::fs::Permissions::from_mode(0o664))
        .await
        .expect("make source permissive");

    install_immutable_segment(source.as_path(), destination.as_path())
        .await
        .expect("install immutable copy");
    #[cfg(unix)]
    {
        assert_eq!(
            tokio::fs::metadata(source.as_path())
                .await
                .expect("read source permissions")
                .permissions()
                .mode()
                & 0o777,
            0o664
        );
        assert_eq!(
            tokio::fs::metadata(destination.as_path())
                .await
                .expect("read destination permissions")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    tokio::fs::write(source.as_path(), b"mutated source")
        .await
        .expect("mutate source");
    assert_eq!(
        tokio::fs::read(destination.as_path())
            .await
            .expect("read immutable copy"),
        b"frozen prefix"
    );

    tokio::fs::write(source.as_path(), b"frozen prefix")
        .await
        .expect("restore identical source");
    #[cfg(unix)]
    tokio::fs::set_permissions(
        destination.as_path(),
        std::fs::Permissions::from_mode(0o664),
    )
    .await
    .expect("make identical destination permissive");
    install_immutable_segment(source.as_path(), destination.as_path())
        .await
        .expect("accept identical immutable copy");
    #[cfg(unix)]
    assert_eq!(
        tokio::fs::metadata(destination.as_path())
            .await
            .expect("read repaired destination permissions")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    tokio::fs::write(source.as_path(), b"different prefix")
        .await
        .expect("write conflicting source");
    #[cfg(unix)]
    tokio::fs::set_permissions(
        destination.as_path(),
        std::fs::Permissions::from_mode(0o664),
    )
    .await
    .expect("make conflicting destination permissive");
    let destination_before_conflict = tokio::fs::read(destination.as_path())
        .await
        .expect("read destination before conflict");
    let err = install_immutable_segment(source.as_path(), destination.as_path())
        .await
        .expect_err("reject conflicting immutable copy");
    assert!(
        err.to_string().contains("different contents"),
        "unexpected error: {err}"
    );
    assert_eq!(
        tokio::fs::read(destination.as_path())
            .await
            .expect("read destination after conflict"),
        destination_before_conflict
    );
    #[cfg(unix)]
    assert_eq!(
        tokio::fs::metadata(destination.as_path())
            .await
            .expect("read conflicting destination permissions")
            .permissions()
            .mode()
            & 0o777,
        0o664
    );
}

#[cfg(unix)]
#[tokio::test]
async fn immutable_install_rejects_existing_symlink_without_mutating_its_target() {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::fs::symlink;

    let home = TempDir::new().expect("temp dir");
    let source = home.path().join("source.jsonl");
    let target = home.path().join("external-target.jsonl");
    let destination = home.path().join("segments").join("segment.jsonl");
    tokio::fs::write(source.as_path(), b"same bytes")
        .await
        .expect("write source");
    tokio::fs::write(target.as_path(), b"same bytes")
        .await
        .expect("write external target");
    tokio::fs::set_permissions(target.as_path(), std::fs::Permissions::from_mode(0o664))
        .await
        .expect("make target permissive");
    tokio::fs::create_dir_all(destination.parent().expect("destination parent"))
        .await
        .expect("create destination parent");
    symlink(target.as_path(), destination.as_path()).expect("create destination symlink");

    let error = install_immutable_segment(source.as_path(), destination.as_path())
        .await
        .expect_err("immutable publication must reject a destination symlink");
    assert!(matches!(error, ThreadStoreError::Conflict { .. }));
    assert!(
        tokio::fs::symlink_metadata(destination.as_path())
            .await
            .expect("read destination symlink")
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        tokio::fs::read(target.as_path())
            .await
            .expect("read unchanged target"),
        b"same bytes"
    );
    assert_eq!(
        tokio::fs::metadata(target.as_path())
            .await
            .expect("read unchanged target mode")
            .permissions()
            .mode()
            & 0o777,
        0o664
    );
}

#[tokio::test]
async fn unloaded_freeze_replaces_stable_rollout_without_installing_a_writer() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist thread");
    append_message(&store, thread_id, "closed prefix").await;
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    store
        .shutdown_thread(thread_id)
        .await
        .expect("shutdown thread");

    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("freeze unloaded segment");
    assert!(
        store.live_rollout_path(thread_id).await.is_err(),
        "unloaded freeze must not install a live writer"
    );
    assert!(
        tokio::fs::try_exists(stable_path.as_path())
            .await
            .expect("check stable path")
    );
    let logical_items =
        codex_rollout::materialize_rollout_items(home.path(), stable_path.as_path())
            .await
            .expect("materialize unloaded history");
    assert!(has_message(&logical_items, "closed prefix"));
    assert_eq!(frozen.reference.thread_id, Some(thread_id));
    assert_eq!(
        frozen.reference.segment_id,
        frozen.source_session_meta.meta.segment_id
    );
}

#[tokio::test]
async fn paginated_freeze_continues_ordinals_and_resets_only_projection_offset() {
    let home = TempDir::new().expect("temp dir");
    let store = state_backed_store(home.path()).await;
    let config = test_config(home.path());
    let sqlite = config.sqlite.clone();
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist thread");

    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("freeze paginated segment");
    assert_eq!(frozen.next_rollout_ordinal, Some(1));
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    let (lines, _, parse_errors) = RolloutRecorder::load_rollout_lines(stable_path.as_path())
        .await
        .expect("read replacement rollout");
    assert_eq!(parse_errors, 0);
    assert_eq!(
        lines.iter().map(|line| line.ordinal).collect::<Vec<_>>(),
        vec![Some(1)]
    );

    let pool = codex_state::open_thread_history_db(&sqlite)
        .await
        .expect("open history db");
    let projection_state = sqlx::query_as::<_, (i64, i64)>(
        "SELECT next_rollout_byte_offset, next_rollout_ordinal FROM thread_history_projection_state WHERE thread_id = ?",
    )
    .bind(
        codex_rollout::rollout_id_from_path(stable_path.as_path())
            .expect("active rollout id")
            .to_string(),
    )
    .fetch_one(&pool)
    .await
    .expect("read projection state");
    let replacement_len = i64::try_from(
        tokio::fs::metadata(stable_path)
            .await
            .expect("replacement metadata")
            .len(),
    )
    .expect("replacement length");
    assert_eq!(projection_state, (replacement_len, 2));
}

#[tokio::test]
async fn paginated_child_projection_contains_only_child_local_rows() {
    let home = TempDir::new().expect("temp dir");
    let store = state_backed_store(home.path()).await;
    let config = test_config(home.path());
    let sqlite = config.sqlite.clone();
    let parent_id = ThreadId::default();
    store
        .create_thread(create_params(parent_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create parent");
    store
        .persist_thread(parent_id, PersistContext::Standard)
        .await
        .expect("persist parent");
    append_turn(
        &store,
        parent_id,
        "parent-turn",
        "parent-item",
        "parent content",
    )
    .await;
    let frozen = store
        .freeze_thread_segment(parent_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("freeze parent");

    let child_id = ThreadId::default();
    let mut child_params = create_params(child_id, ThreadHistoryMode::Paginated);
    child_params.forked_from_id = Some(parent_id);
    child_params.initial_rollout_ordinal = frozen
        .next_rollout_ordinal
        .expect("paginated continuation ordinal");
    store
        .create_thread(child_params)
        .await
        .expect("create child");
    store
        .persist_thread(child_id, PersistContext::Standard)
        .await
        .expect("persist child");
    store
        .append_items(AppendThreadItemsParams {
            thread_id: child_id,
            items: vec![RolloutItem::RolloutReference(frozen.reference)],
        })
        .await
        .expect("append inherited reference");
    append_turn(
        &store,
        child_id,
        "child-turn",
        "child-item",
        "child content",
    )
    .await;

    let pool = codex_state::open_thread_history_db(&sqlite)
        .await
        .expect("open history db");
    let projected_turn_ids = sqlx::query_scalar::<_, String>(
        "SELECT turn_id FROM thread_turns WHERE thread_id = ? ORDER BY rollout_ordinal",
    )
    .bind(child_id.to_string())
    .fetch_all(&pool)
    .await
    .expect("read child turns");
    let projected_item_ids = sqlx::query_scalar::<_, String>(
        "SELECT item_id FROM thread_items WHERE thread_id = ? ORDER BY rollout_ordinal",
    )
    .bind(child_id.to_string())
    .fetch_all(&pool)
    .await
    .expect("read child items");
    assert_eq!(projected_turn_ids, vec!["child-turn"]);
    assert_eq!(projected_item_ids, vec!["child-item"]);
}

#[tokio::test]
async fn missing_immutable_segment_fails_strict_history_read() {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Legacy))
        .await
        .expect("create thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist thread");
    append_message(&store, thread_id, "before freeze").await;
    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("freeze segment");
    tokio::fs::remove_file(frozen.reference.rollout_path)
        .await
        .expect("remove immutable segment");

    let err = store
        .load_history(crate::LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect_err("missing reference must fail");
    assert!(
        err.to_string().contains("could not be resolved"),
        "unexpected error: {err}"
    );
}

async fn assert_segmentless_source_freezes(history_mode: ThreadHistoryMode) {
    let home = TempDir::new().expect("temp dir");
    let store = if history_mode == ThreadHistoryMode::Paginated {
        state_backed_store(home.path()).await
    } else {
        LocalThreadStore::new(test_config(home.path()), /*state_db*/ None)
    };
    let thread_id = ThreadId::default();
    store
        .create_thread(create_params(thread_id, history_mode))
        .await
        .expect("create thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist thread");
    append_message(&store, thread_id, "legacy segment prefix").await;
    store.flush_thread(thread_id).await.expect("flush thread");
    let stable_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("stable path");
    store
        .shutdown_thread(thread_id)
        .await
        .expect("shutdown thread");
    remove_segment_id(stable_path.as_path()).await;
    let source_bytes = tokio::fs::read(stable_path.as_path())
        .await
        .expect("read segmentless source");

    let frozen = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await
        .expect("freeze segmentless source");
    assert_eq!(frozen.source_session_meta.meta.segment_id, None);
    assert_eq!(frozen.reference.segment_id, None);
    if history_mode == ThreadHistoryMode::Paginated {
        assert!(
            frozen.reference.rollout_path.starts_with(
                home.path()
                    .join(codex_rollout::SESSIONS_SUBDIR)
                    .join(codex_rollout::ROLLOUT_SEGMENTS_SUBDIR)
            )
        );
        assert!(frozen.history_base.is_some());
    } else {
        assert_eq!(
            frozen
                .reference
                .rollout_path
                .parent()
                .and_then(|path| path.file_name())
                .and_then(|name| name.to_str()),
            Some("initial")
        );
    }
    assert_eq!(
        tokio::fs::read(frozen.reference.rollout_path.as_path())
            .await
            .expect("read immutable segment"),
        source_bytes
    );
    let replacement_path = if history_mode == ThreadHistoryMode::Paginated {
        store
            .read_thread(crate::ReadThreadParams {
                thread_id,
                include_archived: false,
                include_history: false,
            })
            .await
            .expect("read paginated continuation")
            .rollout_path
            .expect("paginated continuation path")
    } else {
        stable_path
    };
    let replacement_meta = codex_rollout::read_session_meta_line(replacement_path.as_path())
        .await
        .expect("read replacement metadata");
    assert!(replacement_meta.meta.segment_id.is_some());
    assert_eq!(replacement_meta.meta.history_mode, history_mode);
}

async fn state_backed_store(codex_home: &Path) -> LocalThreadStore {
    let config = test_config(codex_home);
    let state_db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("initialize state db");
    LocalThreadStore::new(config, Some(state_db))
}

#[tokio::test]
#[ignore = "writes an interoperability fixture to FRODEX_HISTORY_BASE_COMPAT_HOME"]
async fn exports_native_history_base_compatibility_fixture() {
    let home = PathBuf::from(
        std::env::var_os("FRODEX_HISTORY_BASE_COMPAT_HOME")
            .expect("FRODEX_HISTORY_BASE_COMPAT_HOME must name an empty fixture directory"),
    );
    tokio::fs::create_dir_all(home.as_path())
        .await
        .expect("create compatibility fixture home");
    let store = state_backed_store(home.as_path()).await;
    let thread_id = ThreadId::new();
    store
        .create_thread(create_params(thread_id, ThreadHistoryMode::Paginated))
        .await
        .expect("create compatibility thread");
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await
        .expect("persist compatibility thread");

    let turns = [
        ("frodex-segment-turn-0", "frodex-segment-item-0"),
        ("frodex-segment-turn-1", "frodex-segment-item-1"),
        ("frodex-segment-turn-2", "frodex-segment-item-2"),
        ("frodex-active-turn", "frodex-active-item"),
    ];
    for (index, (turn_id, item_id)) in turns.iter().enumerate() {
        append_turn(&store, thread_id, turn_id, item_id, turn_id).await;
        store
            .flush_thread(thread_id)
            .await
            .expect("flush compatibility segment");
        if index + 1 < turns.len() {
            store
                .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
                .await
                .expect("rotate compatibility segment");
        }
    }
    let rollout_path = store
        .live_rollout_path(thread_id)
        .await
        .expect("read compatibility rollout path");
    store
        .shutdown_thread(thread_id)
        .await
        .expect("close compatibility writer");

    let session_meta = codex_rollout::read_session_meta_line(rollout_path.as_path())
        .await
        .expect("read compatibility metadata");
    assert_eq!(session_meta.meta.id, thread_id);
    assert!(session_meta.meta.history_base.is_some());
    let manifest = serde_json::json!({
        "thread_id": thread_id,
        "rollout_path": rollout_path,
        "turn_ids": turns.map(|(turn_id, _)| turn_id),
    });
    tokio::fs::write(
        home.join("frodex-history-base-compatibility.json"),
        serde_json::to_vec_pretty(&manifest).expect("serialize compatibility manifest"),
    )
    .await
    .expect("write compatibility manifest");
}

#[tokio::test]
#[ignore = "reads a fixture after the official alpha.20 compatibility consumer appends to it"]
async fn imports_official_alpha20_append_to_native_history_base_fixture() {
    let home = PathBuf::from(
        std::env::var_os("FRODEX_HISTORY_BASE_COMPAT_HOME")
            .expect("FRODEX_HISTORY_BASE_COMPAT_HOME must name the consumed fixture directory"),
    );
    let manifest: serde_json::Value = serde_json::from_slice(
        &tokio::fs::read(home.join("frodex-history-base-compatibility.json"))
            .await
            .expect("read compatibility manifest"),
    )
    .expect("parse compatibility manifest");
    let thread_id = ThreadId::from_string(
        manifest["thread_id"]
            .as_str()
            .expect("compatibility thread id"),
    )
    .expect("parse compatibility thread id");
    let rollout_path = PathBuf::from(
        manifest["rollout_path"]
            .as_str()
            .expect("compatibility rollout path"),
    );
    let mut expected_turn_ids = manifest["turn_ids"]
        .as_array()
        .expect("compatibility turn ids")
        .iter()
        .map(|turn_id| turn_id.as_str().expect("compatibility turn id"))
        .collect::<Vec<_>>();
    let inherited_turn_ids = expected_turn_ids.clone();
    expected_turn_ids.push("official-alpha20-turn");
    let store = state_backed_store(home.as_path()).await;

    let history = store
        .read_thread_by_rollout_path(
            rollout_path,
            /*include_archived*/ true,
            /*include_history*/ true,
        )
        .await
        .expect("Frodex reloads the upstream-mutated lineage")
        .history
        .expect("complete compatibility history");
    for turn_id in &expected_turn_ids {
        assert_eq!(
            history
                .items
                .iter()
                .filter(|item| {
                    matches!(
                        item,
                        RolloutItem::EventMsg(EventMsg::ItemCompleted(event))
                            if event.turn_id == *turn_id
                    )
                })
                .count(),
            1,
            "turn {turn_id} must occur exactly once"
        );
    }
    assert!(
        store
            .rebuild_history_projection(thread_id)
            .await
            .expect("rebuild upstream-mutated projection")
    );
    let turns = store
        .list_turns(crate::ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: None,
            page_size: 20,
            sort_direction: crate::SortDirection::Asc,
            items_view: crate::StoredTurnItemsView::NotLoaded,
        })
        .await
        .expect("list upstream-mutated turns");
    assert_eq!(
        turns
            .turns
            .iter()
            .map(|turn| turn.turn_id.as_str())
            .collect::<Vec<_>>(),
        expected_turn_ids
    );
    assert!(
        store
            .prepare_fork(PrepareForkParams {
                thread_id,
                boundary: ForkBoundary::Latest,
            })
            .await
            .expect("prepare fork after upstream append")
            .history_base
            .is_some()
    );

    let child_thread_id = ThreadId::from_string(
        manifest["upstream_child_thread_id"]
            .as_str()
            .expect("official alpha.20 child thread id"),
    )
    .expect("parse official alpha.20 child thread id");
    let child_history = store
        .read_thread(crate::ReadThreadParams {
            thread_id: child_thread_id,
            include_archived: false,
            include_history: true,
        })
        .await
        .expect("Frodex reads the official alpha.20 child")
        .history
        .expect("complete official alpha.20 child history");
    for turn_id in &inherited_turn_ids {
        assert_eq!(
            child_history
                .items
                .iter()
                .filter(|item| {
                    matches!(
                        item,
                        RolloutItem::EventMsg(EventMsg::ItemCompleted(event))
                            if event.turn_id == *turn_id
                    )
                })
                .count(),
            1,
            "parent turn {turn_id} must occur exactly once in the upstream child"
        );
    }
    assert_eq!(
        child_history
            .items
            .iter()
            .filter(|item| {
                matches!(
                    item,
                    RolloutItem::EventMsg(EventMsg::ItemCompleted(event))
                        if event.turn_id == "official-alpha20-child-turn"
                )
            })
            .count(),
        1
    );
    assert!(
        store
            .prepare_fork(PrepareForkParams {
                thread_id: child_thread_id,
                boundary: ForkBoundary::Latest,
            })
            .await
            .expect("Frodex prepares a fork from the official alpha.20 child")
            .history_base
            .is_some()
    );
}

async fn staged_rollout_file_count(stable_path: &Path) -> usize {
    let stable_name = stable_path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("stable rollout file name");
    let prefix = format!("{stable_name}.staged-");
    let mut entries = tokio::fs::read_dir(stable_path.parent().expect("rollout parent"))
        .await
        .expect("read rollout directory");
    let mut count = 0;
    while let Some(entry) = entries.next_entry().await.expect("read rollout entry") {
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(prefix.as_str()) && name.ends_with(".tmp"))
        {
            count += 1;
        }
    }
    count
}

async fn stored_parent_and_children_bytes(
    history_mode: ThreadHistoryMode,
    parent_message_count: usize,
) -> (u64, u64) {
    let home = TempDir::new().expect("temp dir");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let parent_id = ThreadId::default();
    store
        .create_thread(create_params(parent_id, history_mode))
        .await
        .expect("create parent");
    store
        .persist_thread(parent_id, PersistContext::Standard)
        .await
        .expect("persist parent");
    for index in 0..parent_message_count {
        let turn_id = format!("parent-turn-{index}");
        let item_id = format!("parent-item-{index}");
        let content = format!("parent-{index}-{}", "x".repeat(1_024));
        append_turn(
            &store,
            parent_id,
            turn_id.as_str(),
            item_id.as_str(),
            content.as_str(),
        )
        .await;
    }
    let frozen = store
        .freeze_thread_segment(parent_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("freeze parent");
    let parent_bytes = tokio::fs::metadata(frozen.reference.rollout_path.as_path())
        .await
        .expect("read frozen parent metadata")
        .len();

    let mut children_bytes = 0;
    for _ in 0..3 {
        let child_id = ThreadId::default();
        let mut child_params = create_params(child_id, history_mode);
        child_params.forked_from_id = Some(parent_id);
        child_params.initial_rollout_ordinal = frozen.next_rollout_ordinal.unwrap_or_default();
        store
            .create_thread(child_params)
            .await
            .expect("create child");
        store
            .persist_thread(child_id, PersistContext::Standard)
            .await
            .expect("persist child");
        store
            .append_items(AppendThreadItemsParams {
                thread_id: child_id,
                items: vec![RolloutItem::RolloutReference(frozen.reference.clone())],
            })
            .await
            .expect("append child reference");
        store.flush_thread(child_id).await.expect("flush child");
        let child_path = store
            .live_rollout_path(child_id)
            .await
            .expect("child rollout path");
        let child_items = RolloutRecorder::load_rollout_items(child_path.as_path())
            .await
            .expect("read child rollout")
            .0;
        assert!(matches!(
            child_items.as_slice(),
            [
                RolloutItem::SessionMeta(_),
                RolloutItem::RolloutReference(_)
            ]
        ));
        children_bytes += tokio::fs::metadata(child_path)
            .await
            .expect("read child metadata")
            .len();
    }

    (parent_bytes, children_bytes)
}

async fn remove_segment_id(path: &std::path::Path) {
    let contents = tokio::fs::read_to_string(path).await.expect("read rollout");
    let mut lines = contents.lines();
    let first_line = lines.next().expect("session metadata line");
    let mut first_value =
        serde_json::from_str::<serde_json::Value>(first_line).expect("parse session metadata line");
    first_value
        .get_mut("payload")
        .and_then(serde_json::Value::as_object_mut)
        .expect("session metadata payload")
        .remove("segment_id")
        .expect("segment id");
    let mut rewritten = serde_json::to_string(&first_value).expect("serialize session metadata");
    rewritten.push('\n');
    for line in lines {
        rewritten.push_str(line);
        rewritten.push('\n');
    }
    tokio::fs::write(path, rewritten)
        .await
        .expect("write segmentless rollout");
}

async fn append_message(store: &LocalThreadStore, thread_id: ThreadId, message: &str) {
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![RolloutItem::EventMsg(EventMsg::UserMessage(
                UserMessageEvent {
                    message: message.to_string(),
                    images: None,
                    local_images: Vec::new(),
                    text_elements: Vec::new(),
                    ..Default::default()
                },
            ))],
        })
        .await
        .expect("append message");
}

async fn append_canonical_message(store: &LocalThreadStore, thread_id: ThreadId, message: &str) {
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![RolloutItem::ResponseItem(
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
            )],
        })
        .await
        .expect("append canonical message");
}

fn user_message_item(message: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
        message: message.to_string(),
        images: None,
        local_images: Vec::new(),
        text_elements: Vec::new(),
        ..Default::default()
    }))
}

async fn append_turn(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    turn_id: &str,
    item_id: &str,
    content: &str,
) {
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: turn_items(thread_id, turn_id, item_id, content),
        })
        .await
        .expect("append turn");
}

fn turn_items(
    thread_id: ThreadId,
    turn_id: &str,
    item_id: &str,
    content: &str,
) -> Vec<RolloutItem> {
    vec![
        RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
            turn_id: turn_id.to_string(),
            trace_id: None,
            started_at: Some(10),
            model_context_window: None,
            collaboration_mode_kind: Default::default(),
        })),
        RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
            thread_id,
            turn_id: turn_id.to_string(),
            item: TurnItem::UserMessage(UserMessageItem {
                id: item_id.to_string(),
                client_id: None,
                content: vec![UserInput::Text {
                    text: content.to_string(),
                    text_elements: Vec::new(),
                }],
            }),
            started_at_ms: None,
            completed_at_ms: 1,
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
    ]
}

fn has_message(items: &[RolloutItem], message: &str) -> bool {
    items.iter().any(|item| {
        matches!(
            item,
            RolloutItem::EventMsg(EventMsg::UserMessage(event)) if event.message == message
        )
    })
}

fn has_canonical_message(items: &[RolloutItem], message: &str) -> bool {
    items.iter().any(|item| {
        let RolloutItem::ResponseItem(response_item) = item else {
            return false;
        };
        let ResponseItem::Message { content, .. } = &response_item.item else {
            return false;
        };
        content
            .iter()
            .any(|content| matches!(content, ContentItem::InputText { text } if text == message))
    })
}

fn message_count(items: &[RolloutItem], message: &str) -> usize {
    items
        .iter()
        .filter(|item| {
            matches!(
                item,
                RolloutItem::EventMsg(EventMsg::UserMessage(event)) if event.message == message
            )
        })
        .count()
}

fn turn_message_count(items: &[RolloutItem], message: &str) -> usize {
    items
        .iter()
        .filter(|item| {
            let RolloutItem::EventMsg(EventMsg::ItemCompleted(event)) = item else {
                return false;
            };
            let TurnItem::UserMessage(user_message) = &event.item else {
                return false;
            };
            user_message
                .content
                .iter()
                .any(|input| matches!(input, UserInput::Text { text, .. } if text == message))
        })
        .count()
}

async fn append_malformed_historical_records(path: &Path) {
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .await
        .expect("open legacy rollout");
    file.write_all(
        b"{\"timestamp\":\"2025-01-03T12:00:01Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"historical_unknown_event\"}}\n",
    )
    .await
    .expect("append unsupported historical event");
    file.write_all(
        b"{\"timestamp\":\"2025-01-03T12:00:02Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"invalid\x01record\"}}\n",
    )
    .await
    .expect("append malformed historical event");
    file.flush().await.expect("flush historical records");
}

fn compress_rollout_for_test(path: &Path) {
    let compressed_path = path.with_extension("jsonl.zst");
    let compressed = zstd::stream::encode_all(
        std::fs::File::open(path).expect("open rollout for test compression"),
        /*level*/ 0,
    )
    .expect("compress rollout for projection rebuild");
    std::fs::write(&compressed_path, compressed)
        .expect("write compressed rollout for projection rebuild");
    std::fs::remove_file(path).expect("remove plain rollout after test compression");
}

fn create_params(thread_id: ThreadId, history_mode: ThreadHistoryMode) -> CreateThreadParams {
    CreateThreadParams {
        session_id: thread_id.into(),
        thread_id,
        extra_config: None,
        forked_from_id: None,
        forked_from_ordinal_exclusive: None,
        parent_thread_id: None,
        source: SessionSource::Exec,
        thread_source: None,
        originator: "test_originator".to_string(),
        base_instructions: BaseInstructions::default(),
        dynamic_tools: Vec::new(),
        selected_capability_roots: Vec::new(),
        multi_agent_version: None,
        history_mode,
        history_base: None,
        subagent_history_start_ordinal: None,
        persistence_mode: crate::ThreadPersistenceMode::Durable,
        initial_rollout_ordinal: 0,
        initial_window_id: uuid::Uuid::now_v7().to_string(),
        metadata: ThreadPersistenceMetadata {
            cwd: Some(std::env::current_dir().expect("cwd")),
            model_provider: "test-provider".to_string(),
            memory_mode: ThreadMemoryMode::Enabled,
        },
    }
}
