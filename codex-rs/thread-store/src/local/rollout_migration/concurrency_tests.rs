use super::super::super::dependencies::MigrationAdmission;
use super::super::super::dependencies::discover_dependencies;
use super::*;
use crate::local::rollout_migration::startup as coordinator;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn dependency_discovery_reads_bounded_headers_not_growing_payloads() {
    let home = TempDir::new().expect("home");
    let id = ThreadId::new();
    let path = write_rollout(home.path(), id, ThreadHistoryMode::Legacy);
    let filler = serde_json::to_vec(&serde_json::json!({
        "timestamp": TIMESTAMP,
        "type": "event_msg",
        "payload": {"type": "user_message", "message": "x".repeat(16 * 1024)}
    }))
    .expect("filler");
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("append");
    file.write_all(&filler).expect("small suffix");
    file.write_all(b"\n").expect("newline");
    let small = discover_dependencies(home.path(), &path)
        .await
        .expect("inspect")
        .expect("known");
    for _ in 0..128 {
        file.write_all(&filler).expect("large suffix");
        file.write_all(b"\n").expect("newline");
    }
    let large = discover_dependencies(home.path(), &path)
        .await
        .expect("inspect")
        .expect("known");
    assert_eq!(small.bytes_read, large.bytes_read);
    assert!(large.bytes_read <= 4096);
    assert_eq!(large.thread_ids, vec![id]);
    assert!(
        small
            .unchanged()
            .await
            .expect("tail append leaves header unchanged")
    );
    set_history_base(
        &path,
        HistoryPosition {
            thread_id: ThreadId::new(),
            end_ordinal_exclusive: 1,
            end_byte_offset: 100,
        },
    );
    assert!(!small.unchanged().await.expect("detect changed ancestry"));
    assert!(
        discover_dependencies(home.path(), &path)
            .await
            .expect("native boundary")
            .is_none()
    );
}

#[tokio::test]
async fn dependency_discovery_reserves_shared_legacy_ancestors_and_rejects_cycles() {
    let home = TempDir::new().expect("home");
    let parent = ThreadId::new();
    let child = ThreadId::new();
    let parent_path = write_rollout(home.path(), parent, ThreadHistoryMode::Legacy);
    let child_path = write_rollout(home.path(), child, ThreadHistoryMode::Legacy);
    let reference = |id, path| RolloutReferenceItem {
        rollout_path: path,
        thread_id: Some(id),
        rollout_id: Some(id),
        rollout_timestamp: None,
        segment_id: None,
        max_depth: 2,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };
    prepend_rollout_reference(&child_path, reference(parent, parent_path.clone()));
    let dependencies = discover_dependencies(home.path(), &child_path)
        .await
        .expect("inspect")
        .expect("complete chain");
    assert_eq!(dependencies.thread_ids.len(), 2);
    assert!(dependencies.thread_ids.contains(&parent));
    assert!(dependencies.thread_ids.contains(&child));
    let plan = super::super::super::lineage::plan_legacy_lineage(home.path(), &child_path)
        .await
        .expect("authenticated plan");
    assert!(
        dependencies.covers_plan(&plan).await,
        "ordinary Legacy references must remain eligible for shared admission"
    );
    prepend_rollout_reference(&parent_path, reference(child, child_path.clone()));
    assert!(
        discover_dependencies(home.path(), &child_path)
            .await
            .expect("cycle")
            .is_none()
    );
}

#[tokio::test]
async fn dependency_discovery_does_not_scan_past_oversized_or_malformed_headers() {
    let home = TempDir::new().expect("home");
    let id = ThreadId::new();
    let path = write_rollout(home.path(), id, ThreadHistoryMode::Legacy);
    let source = fs::read(&path).expect("source");
    for prefix in [b"{bad json}\n".to_vec(), vec![b' '; 128 * 1024]] {
        let mut bytes = prefix;
        bytes.extend_from_slice(&source);
        fs::write(&path, bytes).expect("malformed header");
        assert!(
            discover_dependencies(home.path(), &path)
                .await
                .expect("bounded inspect")
                .is_none()
        );
    }
}

#[tokio::test]
async fn dependency_discovery_reserves_rotated_physical_ids_and_their_logical_thread() {
    let home = TempDir::new().expect("home");
    let logical = ThreadId::new();
    let physical = [ThreadId::new(), ThreadId::new()];
    let mut paths = Vec::new();
    for id in physical {
        let path = write_rollout(home.path(), id, ThreadHistoryMode::Legacy);
        let contents = fs::read_to_string(&path).expect("source");
        let (head, suffix) = contents.split_once('\n').expect("metadata");
        let mut head: serde_json::Value = serde_json::from_str(head).expect("metadata JSON");
        head["payload"]["id"] = serde_json::to_value(logical).expect("logical ID");
        head["payload"]["session_id"] = serde_json::to_value(logical).expect("session ID");
        fs::write(&path, format!("{head}\n{suffix}")).expect("logical metadata");
        paths.push(path);
    }
    prepend_rollout_reference(
        &paths[1],
        RolloutReferenceItem {
            rollout_path: paths[0].clone(),
            thread_id: Some(logical),
            rollout_id: Some(physical[0]),
            rollout_timestamp: None,
            segment_id: None,
            max_depth: 2,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        },
    );
    let dependencies = discover_dependencies(home.path(), &paths[1])
        .await
        .expect("inspect")
        .expect("complete rotated chain");
    let mut expected = vec![logical, physical[0], physical[1]];
    expected.sort_unstable_by_key(ThreadId::to_string);
    assert_eq!(dependencies.thread_ids, expected);
    let plan = super::super::super::lineage::plan_legacy_lineage(home.path(), &paths[1])
        .await
        .expect("plan");
    assert!(dependencies.covers_plan(&plan).await);
}

#[tokio::test]
async fn blocked_migration_writers_do_not_starve_an_independent_migration() {
    let home = TempDir::new().expect("home");
    let ids = [ThreadId::new(), ThreadId::new(), ThreadId::new()];
    for id in ids {
        write_rollout(home.path(), id, ThreadHistoryMode::Legacy);
    }
    let store = indexed_store(home.path()).await;
    store.start_automatic_rollout_migration();
    let first = store.live_writer_locks.lock(ids[0]).await;
    let second = store.live_writer_locks.lock(ids[1]).await;
    let mut requests = Vec::new();
    for id in &ids[..2] {
        let store = store.clone();
        let id = *id;
        requests.push(tokio::spawn(async move {
            coordinator::await_thread_migration(&store, id).await
        }));
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let processed = coordinator::processed_thread_ids(&store).await;
            if processed.contains(&ids[0]) && processed.contains(&ids[1]) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both busy requests attempted");
    tokio::time::timeout(
        Duration::from_secs(5),
        coordinator::await_thread_migration(&store, ids[2]),
    )
    .await
    .expect("independent migration must not wait for busy writers")
    .expect("migration");
    assert!(requests.iter().all(|request| !request.is_finished()));
    drop(first);
    drop(second);
    for request in requests {
        tokio::time::timeout(Duration::from_secs(5), request)
            .await
            .expect("unblocked migration")
            .expect("join")
            .expect("migration");
    }
}

#[tokio::test]
async fn independent_migrations_both_create_journals_before_either_continues() {
    let home = TempDir::new().expect("home");
    let ids = [ThreadId::new(), ThreadId::new()];
    for id in ids {
        write_rollout(home.path(), id, ThreadHistoryMode::Legacy);
    }
    let store = indexed_store(home.path()).await;
    store.start_automatic_rollout_migration();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
    let mut requests = Vec::new();
    for id in ids {
        store
            .rollout_migration_coordinator
            .journal_barriers
            .lock()
            .await
            .insert(id, barrier.clone());
        let store = store.clone();
        requests.push(tokio::spawn(async move {
            coordinator::await_thread_migration(&store, id).await
        }));
    }
    tokio::time::timeout(Duration::from_secs(5), barrier.wait())
        .await
        .expect("both migrations reached their durable journal before release");
    for request in requests {
        tokio::time::timeout(Duration::from_secs(5), request)
            .await
            .expect("migration finishes")
            .expect("join")
            .expect("migration");
    }
    for id in ids {
        assert!(
            store
                .has_history_projection(id)
                .await
                .expect("complete projection")
        );
    }
}

#[tokio::test]
async fn canceled_waiter_does_not_cancel_or_duplicate_the_shared_migration() {
    let home = TempDir::new().expect("home");
    let id = ThreadId::new();
    write_rollout(home.path(), id, ThreadHistoryMode::Legacy);
    let store = indexed_store(home.path()).await;
    store.start_automatic_rollout_migration();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    store
        .rollout_migration_coordinator
        .journal_barriers
        .lock()
        .await
        .insert(id, barrier.clone());
    let mut requests = Vec::new();
    for _ in 0..2 {
        let store = store.clone();
        requests.push(tokio::spawn(async move {
            coordinator::await_thread_migration(&store, id).await
        }));
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if store
                .rollout_migration_coordinator
                .state
                .lock()
                .await
                .entries
                .get(&id)
                .is_some_and(|entry| entry.completion.receiver_count() == 2)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both callers joined");
    let canceled = requests.pop().expect("cancel one waiter");
    canceled.abort();
    assert!(canceled.await.expect_err("canceled").is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), barrier.wait())
        .await
        .expect("worker continues despite canceled caller");
    for request in requests {
        tokio::time::timeout(Duration::from_secs(5), request)
            .await
            .expect("remaining caller")
            .expect("join")
            .expect("migration");
    }
    assert_eq!(coordinator::processed_thread_ids(&store).await, vec![id]);
    assert!(
        store
            .has_history_projection(id)
            .await
            .expect("complete projection")
    );
}

#[tokio::test]
async fn changed_ancestry_or_pending_journal_retries_exclusively_without_publication() {
    for pending_journal in [false, true] {
        let home = TempDir::new().expect("home");
        let parent = ThreadId::new();
        let child = ThreadId::new();
        let parent_path = write_rollout(home.path(), parent, ThreadHistoryMode::Legacy);
        let child_path = write_rollout(home.path(), child, ThreadHistoryMode::Legacy);
        let segment_id = SegmentId::new();
        let contents = fs::read_to_string(&parent_path).expect("parent");
        let (head, suffix) = contents.split_once('\n').expect("metadata");
        let mut head: serde_json::Value = serde_json::from_str(head).expect("metadata JSON");
        head["payload"]["segment_id"] = serde_json::to_value(segment_id).expect("segment ID");
        fs::write(&parent_path, format!("{head}\n{suffix}")).expect("immutable parent");
        let store = indexed_store(home.path()).await;
        let dependencies = discover_dependencies(home.path(), &child_path)
            .await
            .expect("inspect")
            .expect("initially independent");
        let mut admission = MigrationAdmission::Shared(std::sync::Arc::new(dependencies));
        let journal = migration_journal_path(home.path(), child);
        if pending_journal {
            write_migration_journal(&journal)
                .await
                .expect("journal appeared after discovery");
        } else {
            prepend_rollout_reference(
                &child_path,
                RolloutReferenceItem {
                    rollout_path: parent_path,
                    thread_id: Some(parent),
                    rollout_id: Some(parent),
                    rollout_timestamp: None,
                    segment_id: Some(segment_id),
                    max_depth: 2,
                    nth_user_message: None,
                    compacted_replacement_history_filter_texts: None,
                },
            );
        }
        let original = fs::read(&child_path).expect("current source");
        let unrelated = codex_rollout::try_acquire_rollout_migration_dependency_lock(
            home.path(),
            &[ThreadId::new()],
        )
        .expect("reservation")
        .expect("unrelated job");
        let result = store
            .migrate_rollout_path_on_demand(child, child_path.clone(), &mut admission)
            .await;
        assert!(matches!(
            result,
            Err(crate::ThreadStoreError::Conflict { .. })
        ));
        assert!(
            matches!(admission, MigrationAdmission::Exclusive),
            "exclusive retry must survive contention"
        );
        assert_eq!(fs::read(&child_path).expect("source retained"), original);
        assert_eq!(journal.exists(), pending_journal);
        drop(unrelated);
        let outcome = store
            .migrate_rollout_path_on_demand(child, child_path, &mut admission)
            .await
            .expect("exclusive retry")
            .expect("outcome");
        assert_eq!(
            outcome.status,
            crate::RolloutMigrationStatus::Migrated,
            "{outcome:?}"
        );
        assert!(
            store
                .has_history_projection(child)
                .await
                .expect("complete history")
        );
        assert!(!journal.exists());
    }
}

#[tokio::test]
async fn failed_migration_reuses_supported_reader_then_retries_changed_or_expired_source() {
    let home = TempDir::new().expect("home");
    let parent = ThreadId::new();
    let child = ThreadId::new();
    let parent_path = write_rollout(home.path(), parent, ThreadHistoryMode::Legacy);
    let initial_directory = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(parent.to_string())
        .join("initial");
    fs::create_dir_all(&initial_directory).expect("canonical legacy initial directory");
    let initial_path = initial_directory.join(parent_path.file_name().expect("rollout filename"));
    fs::rename(parent_path, &initial_path).expect("retained legacy initial");
    let child_path = write_rollout(home.path(), child, ThreadHistoryMode::Legacy);
    prepend_rollout_reference(
        &child_path,
        RolloutReferenceItem {
            rollout_path: initial_path,
            thread_id: Some(parent),
            rollout_id: Some(parent),
            rollout_timestamp: None,
            segment_id: None,
            max_depth: 2,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        },
    );
    let original = fs::read(&child_path).expect("source");
    let store = indexed_store(home.path()).await;
    store.start_automatic_rollout_migration();
    for include_history in [false, true, false] {
        let thread = store
            .read_thread(ReadThreadParams {
                thread_id: child,
                include_archived: false,
                include_history,
            })
            .await
            .expect("supported reader after unsupported segmentless cross-thread conversion");
        assert_eq!(thread.thread_id, child);
    }
    assert_eq!(coordinator::processed_thread_ids(&store).await, vec![child]);
    assert_eq!(fs::read(&child_path).expect("unchanged source"), original);

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::time::resume();
    coordinator::await_thread_migration(&store, child)
        .await
        .expect("expired failure retries without depending on a selected-source change");
    assert_eq!(
        coordinator::processed_thread_ids(&store).await,
        vec![child, child]
    );

    write_rollout(home.path(), child, ThreadHistoryMode::Legacy);
    coordinator::await_thread_migration(&store, child)
        .await
        .expect("changed source retries immediately");
    assert_eq!(
        coordinator::processed_thread_ids(&store).await,
        vec![child, child, child]
    );
    assert!(
        store
            .has_history_projection(child)
            .await
            .expect("projection")
    );
}

#[tokio::test]
async fn failed_migration_does_not_suppress_pending_journal_recovery() {
    let home = TempDir::new().expect("home");
    let child = ThreadId::new();
    let path = write_rollout(home.path(), child, ThreadHistoryMode::Legacy);
    prepend_rollout_reference(
        &path,
        RolloutReferenceItem {
            rollout_path: home.path().join("missing-parent.jsonl"),
            thread_id: Some(ThreadId::new()),
            rollout_id: None,
            rollout_timestamp: None,
            segment_id: Some(SegmentId::new()),
            max_depth: 2,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        },
    );
    let store = indexed_store(home.path()).await;
    store.start_automatic_rollout_migration();
    coordinator::await_thread_migration(&store, child)
        .await
        .expect("initial fallback");
    assert_eq!(coordinator::processed_thread_ids(&store).await, vec![child]);
    let journal = migration_journal_path(home.path(), child);
    write_migration_journal(&journal).await.expect("journal");
    fs::write(&journal, b"{invalid}").expect("incomplete recovery state");
    coordinator::await_thread_migration(&store, child)
        .await
        .expect_err("pending recovery cannot use a cached fallback");
    assert!(journal.exists());
    assert_eq!(
        coordinator::processed_thread_ids(&store).await,
        vec![child, child]
    );
}
