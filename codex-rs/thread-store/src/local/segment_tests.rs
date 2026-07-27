use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::models::BaseInstructions;
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
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;

use super::super::LocalThreadStore;
use super::super::test_support::test_config;
use super::super::writer_lock::WriterLockCoordinator;
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
use crate::PersistContext;
use crate::PrepareForkParams;
use crate::ResumeThreadParams;
use crate::SegmentCheckpointPersistenceOutcome;
use crate::ThreadPersistenceMetadata;
use crate::ThreadPersistenceMode;
use crate::ThreadStore;
use crate::ThreadStoreError;

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
    append_message(&store, thread_id, "paginated prefix").await;
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
    assert_eq!(
        tokio::fs::read(frozen.reference.rollout_path)
            .await
            .expect("read immutable paginated segment"),
        source_bytes
    );
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

    let competing = Arc::new(WriterLockCoordinator::new(home.path()));
    let _parent_writer = competing
        .acquire(parent_id)
        .expect("acquire external parent writer");
    let child_snapshot = store
        .freeze_thread_segment(child_id, FreezeRolloutSegmentParams::snapshot())
        .await
        .expect("immutable parent must not require its current writer lock");
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
        vec![Some(1), Some(2)]
    );

    let pool = codex_state::open_thread_history_db(&sqlite)
        .await
        .expect("open history db");
    let projection_state = sqlx::query_as::<_, (i64, i64)>(
        "SELECT next_rollout_byte_offset, next_rollout_ordinal FROM thread_history_projection_state WHERE thread_id = ?",
    )
    .bind(thread_id.to_string())
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
    assert_eq!(projection_state, (replacement_len, 3));
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
    assert_eq!(
        frozen
            .reference
            .rollout_path
            .parent()
            .and_then(|path| path.file_name())
            .and_then(|name| name.to_str()),
        Some("initial")
    );
    assert_eq!(
        tokio::fs::read(frozen.reference.rollout_path.as_path())
            .await
            .expect("read immutable segment"),
        source_bytes
    );
    let replacement_meta = codex_rollout::read_session_meta_line(stable_path.as_path())
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
            items: vec![
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
            ],
        })
        .await
        .expect("append turn");
}

fn has_message(items: &[RolloutItem], message: &str) -> bool {
    items.iter().any(|item| {
        matches!(
            item,
            RolloutItem::EventMsg(EventMsg::UserMessage(event)) if event.message == message
        )
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
