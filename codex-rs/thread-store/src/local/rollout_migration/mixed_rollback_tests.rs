//! Rollbacks in a Legacy fork must also remove inherited native history from that fork.

use std::fs;

use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ThreadRolledBackEvent;
use codex_rollout::RolloutItem;
use pretty_assertions::assert_eq;

use super::RolloutMigrationOptions;
use super::RolloutMigrationRateLimiter;
use super::RolloutMigrationStatus;
use super::lineage::plan_legacy_lineage;
use super::lineage_journal::LineageMigrationPhase;
use super::publish::migration_journal_path;
use super::tests::*;
use crate::LoadThreadHistoryParams;
use crate::ThreadStore;

fn response(text: &str) -> RolloutItem {
    serde_json::from_value(serde_json::json!({"type":"response_item","payload":{
        "type":"message","role":"user","content":[{"type":"input_text","text":text}]
    }}))
    .expect("user response")
}

fn turn(id: &str) -> Vec<RolloutItem> {
    vec![
        turn_started(id),
        response(id),
        user_message(id),
        turn_complete(id),
    ]
}

fn native_turn(thread_id: ThreadId, id: &str) -> Vec<RolloutItem> {
    let item = completed_user_message(thread_id, id, &format!("explicit-{id}"), id);
    vec![turn_started(id), item.clone(), item, turn_complete(id)]
}

fn native_model_turn(thread_id: ThreadId, id: &str) -> Vec<RolloutItem> {
    vec![
        turn_started(id),
        response(id),
        completed_user_message(thread_id, id, &format!("explicit-{id}"), id),
        turn_complete(id),
    ]
}

#[tokio::test]
async fn mixed_rollback_consumes_filtered_native_reference_once() {
    let home = tempfile::tempdir().expect("home");
    let oldest_id = ThreadId::new();
    let parent_id = ThreadId::new();
    let child_id = ThreadId::new();
    let oldest = home
        .path()
        .join("sessions/2025/01/03")
        .join(format!("rollout-2025-01-03T12-00-00-{oldest_id}.jsonl"));
    let developer = serde_json::json!({"type":"message","role":"developer","content":[{"type":"input_text","text":"filtered-developer-text"}]});
    let mut items = vec![
        serde_json::from_value(
            serde_json::json!({"type":"response_item","payload":developer.clone()}),
        )
        .expect("developer response"),
    ];
    items.extend(native_model_turn(oldest_id, "keep-native"));
    items.push(serde_json::from_value(serde_json::json!({"type":"compacted","payload":{"message":"checkpoint","replacement_history":[developer,{"type":"message","role":"user","content":[{"type":"input_text","text":"keep-native"}]}]}})).expect("checkpoint"));
    items.extend(native_model_turn(oldest_id, "remove-native"));
    items.extend(native_model_turn(oldest_id, "excluded-native-suffix"));
    let end = write_paginated_segment(
        &oldest,
        home.path(),
        oldest_id,
        SegmentId::new(),
        /*start_ordinal*/ 0,
        items,
    );
    let parent = home
        .path()
        .join("sessions/2025/01/03")
        .join(format!("rollout-2025-01-03T12-00-01-{parent_id}.jsonl"));
    let parent_segment = SegmentId::new();
    write_paginated_segment(
        &parent,
        home.path(),
        parent_id,
        parent_segment,
        end,
        native_model_turn(parent_id, "excluded-parent"),
    );
    set_history_base(
        &parent,
        HistoryPosition {
            thread_id: oldest_id,
            end_ordinal_exclusive: end,
            end_byte_offset: fs::metadata(&oldest).expect("oldest size").len(),
        },
    );
    let mut reference = segment_reference(parent.clone(), parent_id, parent_segment);
    let RolloutItem::RolloutReference(reference_item) = &mut reference else {
        unreachable!()
    };
    reference_item.nth_user_message = Some(2);
    reference_item.compacted_replacement_history_filter_texts =
        Some(vec!["filtered-developer-text".to_string()]);
    let child = home
        .path()
        .join("sessions/2025/01/03")
        .join(format!("rollout-2025-01-03T12-00-02-{child_id}.jsonl"));
    let mut items = vec![
        reference,
        RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
            num_turns: 1,
        })),
    ];
    items.extend(turn("keep-child"));
    write_legacy_segment(&child, home.path(), child_id, SegmentId::new(), items);
    let preserved = [oldest, parent, child.clone()].map(|path| {
        let bytes = fs::read(&path).expect("source bytes");
        (path, bytes)
    });
    let store = indexed_store(home.path()).await;
    let parent_writer = store
        .writer_lock_coordinator
        .acquire(parent_id)
        .expect("hold omitted ancestor writer");
    let busy = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![child_id],
            ..apply_options()
        })
        .await
        .expect("busy migration");
    assert_eq!(busy.outcomes[0].status, RolloutMigrationStatus::SkippedBusy);
    for (path, bytes) in &preserved {
        assert_eq!(&fs::read(path).expect("unchanged busy source"), bytes);
    }
    drop(parent_writer);
    let plan = plan_legacy_lineage(home.path(), &child)
        .await
        .expect("filtered plan");
    let journal_path = migration_journal_path(home.path(), child_id);
    let mut limiter =
        RolloutMigrationRateLimiter::new(/*max_mib_per_second*/ None).expect("limiter");
    store
        .migrate_legacy_lineage_until_phase_for_test(
            &child,
            &journal_path,
            plan,
            &mut limiter,
            LineageMigrationPhase::TargetsDurable,
        )
        .await
        .expect_err("stop before filtered publication");
    let omitted_parent = &preserved[1];
    let mut changed = omitted_parent.1.clone();
    changed.push(b'\n');
    fs::write(&omitted_parent.0, changed).expect("simulate changed omitted ancestry header file");
    let changed = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![child_id],
            ..apply_options()
        })
        .await
        .expect("changed-source migration");
    assert_eq!(changed.outcomes[0].status, RolloutMigrationStatus::Failed);
    assert_eq!(
        store
            .state_db()
            .await
            .expect("database")
            .get_thread(child_id)
            .await
            .expect("metadata")
            .expect("child")
            .rollout_path,
        child
    );
    fs::write(&omitted_parent.0, &omitted_parent.1).expect("restore test source");
    let report = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![child_id],
            ..apply_options()
        })
        .await
        .expect("migrate");
    assert_eq!(
        report.outcomes[0].status,
        RolloutMigrationStatus::Migrated,
        "{:?}",
        report.outcomes[0].message
    );
    let turns = list_active_summary_turns(&store, child_id).await;
    assert_eq!(
        turns
            .turns
            .iter()
            .map(|turn| turn.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec!["keep-native", "keep-child"]
    );
    let context = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id: child_id,
            include_archived: false,
        })
        .await
        .expect("model context");
    let json = serde_json::to_string(&context.items).expect("model JSON");
    assert!(json.contains("keep-native"));
    assert!(json.contains("keep-child"));
    for excluded in [
        "remove-native",
        "excluded-native-suffix",
        "excluded-parent",
        "filtered-developer-text",
    ] {
        assert!(!json.contains(excluded), "unexpected {excluded}");
    }
    for (path, bytes) in preserved {
        assert_eq!(fs::read(path).expect("preserved source"), bytes);
    }
}

#[tokio::test]
async fn mixed_rollback_respects_compressed_native_prefix_and_event_only_turns() {
    let home = tempfile::tempdir().expect("home");
    let oldest_id = ThreadId::new();
    let parent_id = ThreadId::new();
    let child_id = ThreadId::new();
    let oldest = home
        .path()
        .join("sessions/2025/01/03")
        .join(format!("rollout-2025-01-03T12-00-00-{oldest_id}.jsonl"));
    let mut items = native_turn(oldest_id, "keep-oldest");
    items.extend(native_turn(oldest_id, "remove-oldest"));
    let end = write_paginated_segment(
        &oldest,
        home.path(),
        oldest_id,
        SegmentId::new(),
        /*start_ordinal*/ 0,
        items.clone(),
    );
    let end_byte_offset = fs::metadata(&oldest).expect("prefix metadata").len();
    items.extend(native_turn(oldest_id, "outside-selected-prefix"));
    write_paginated_segment(
        &oldest,
        home.path(),
        oldest_id,
        SegmentId::new(),
        /*start_ordinal*/ 0,
        items,
    );
    let oldest_bytes = fs::read(&oldest).expect("oldest bytes");
    let compressed = oldest.with_extension("jsonl.zst");
    fs::write(
        &compressed,
        zstd::stream::encode_all(oldest_bytes.as_slice(), 1).expect("compress"),
    )
    .expect("write compressed");
    fs::remove_file(&oldest).expect("remove uncompressed test fixture");
    let parent = home
        .path()
        .join("sessions/2025/01/03")
        .join(format!("rollout-2025-01-03T12-00-01-{parent_id}.jsonl"));
    let parent_segment = SegmentId::new();
    write_paginated_segment(
        &parent,
        home.path(),
        parent_id,
        parent_segment,
        end,
        native_turn(parent_id, "remove-parent"),
    );
    set_history_base(
        &parent,
        HistoryPosition {
            thread_id: oldest_id,
            end_ordinal_exclusive: end,
            end_byte_offset,
        },
    );
    let child = home
        .path()
        .join("sessions/2025/01/03")
        .join(format!("rollout-2025-01-03T12-00-02-{child_id}.jsonl"));
    let mut child_items = vec![
        segment_reference(parent.clone(), parent_id, parent_segment),
        RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
            num_turns: 2,
        })),
    ];
    child_items.extend(turn("keep-child"));
    write_legacy_segment(&child, home.path(), child_id, SegmentId::new(), child_items);
    let preserved = [compressed, parent, child.clone()].map(|path| {
        let bytes = fs::read(&path).expect("source bytes");
        (path, bytes)
    });
    let store = indexed_store(home.path()).await;
    let report = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![child_id],
            ..apply_options()
        })
        .await
        .expect("migrate child");
    assert_eq!(
        report.outcomes[0].status,
        RolloutMigrationStatus::Migrated,
        "{:?}",
        report.outcomes[0].message
    );
    let turns = list_active_summary_turns(&store, child_id).await;
    assert_eq!(
        turns
            .turns
            .iter()
            .map(|turn| turn.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec!["keep-oldest", "keep-child"]
    );
    for (path, bytes) in preserved {
        assert_eq!(fs::read(path).expect("unchanged source"), bytes);
    }
}

#[tokio::test]
async fn legacy_child_rollback_removes_inherited_native_turn_without_changing_parent() {
    mixed_rollback_case(
        /*stop_after*/ None, /*previous_policy*/ false, /*released_binary*/ None,
    )
    .await;
}

#[tokio::test]
async fn mixed_rollback_recovers_every_durable_phase() {
    for phase in [
        LineageMigrationPhase::Planned,
        LineageMigrationPhase::TargetsDurable,
        LineageMigrationPhase::ProjectionDurable,
        LineageMigrationPhase::Selected,
        LineageMigrationPhase::Verified,
        LineageMigrationPhase::Complete,
    ] {
        mixed_rollback_case(
            Some(phase),
            /*previous_policy*/ false,
            /*released_binary*/ None,
        )
        .await;
    }
}

#[tokio::test]
async fn mixed_rollback_upgrades_old_preselection_journals_without_removing_published_targets() {
    for phase in [
        LineageMigrationPhase::Planned,
        LineageMigrationPhase::TargetsDurable,
        LineageMigrationPhase::ProjectionDurable,
    ] {
        mixed_rollback_case(
            Some(phase),
            /*previous_policy*/ true,
            /*released_binary*/ None,
        )
        .await;
    }
}

#[tokio::test]
#[ignore = "requires CODEX_MIGRATION_RELEASE_BINARY"]
async fn mixed_rollback_released_binary_reproduction() {
    let binary = std::env::var_os("CODEX_MIGRATION_RELEASE_BINARY").expect("released binary");
    mixed_rollback_case(
        /*stop_after*/ None,
        /*previous_policy*/ false,
        Some(binary.into()),
    )
    .await;
}

async fn mixed_rollback_case(
    stop_after: Option<LineageMigrationPhase>,
    previous_policy: bool,
    released_binary: Option<std::path::PathBuf>,
) {
    let home = tempfile::tempdir().expect("home");
    let parent_id = ThreadId::new();
    let child_id = ThreadId::new();
    let oldest_segment = SegmentId::new();
    let parent_segment = SegmentId::new();
    let filename = format!("rollout-2025-01-03T12-00-00-{parent_id}.jsonl");
    let oldest = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(parent_id.to_string())
        .join(oldest_segment.to_string())
        .join(&filename);
    write_legacy_segment(
        &oldest,
        home.path(),
        parent_id,
        oldest_segment,
        turn("keep-oldest"),
    );
    let parent_source = home.path().join("sessions/2025/01/03").join(filename);
    let mut parent_items = vec![segment_reference(oldest.clone(), parent_id, oldest_segment)];
    parent_items.extend(turn("remove-parent"));
    write_legacy_segment(
        &parent_source,
        home.path(),
        parent_id,
        parent_segment,
        parent_items,
    );
    let store = indexed_store(home.path()).await;
    let report = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![parent_id],
            ..apply_options()
        })
        .await
        .expect("migrate parent");
    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Migrated);
    let parent = report.outcomes[0].rollout_path.clone();
    let parent_meta = codex_rollout::read_session_meta_line(&parent)
        .await
        .expect("parent metadata");
    assert!(parent_meta.meta.history_base.is_some());
    let mut reference = segment_reference(
        parent.clone(),
        parent_id,
        parent_meta.meta.segment_id.expect("segment"),
    );
    let RolloutItem::RolloutReference(reference_item) = &mut reference else {
        unreachable!()
    };
    reference_item.rollout_id = codex_rollout::rollout_id_from_path(&parent);
    let child = home
        .path()
        .join("sessions/2025/01/03")
        .join(format!("rollout-2025-01-03T12-00-01-{child_id}.jsonl"));
    let mut child_items = vec![
        reference,
        RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
            num_turns: 1,
        })),
    ];
    child_items.extend(turn("keep-child"));
    write_legacy_segment(&child, home.path(), child_id, SegmentId::new(), child_items);
    let metadata = codex_state::ThreadMetadataBuilder::new(
        child_id,
        child.clone(),
        chrono::Utc::now(),
        codex_protocol::protocol::SessionSource::Cli,
    );
    store
        .state_db()
        .await
        .expect("database")
        .upsert_thread(&metadata.build("test-provider"))
        .await
        .expect("index child created after backfill");
    let mut preserved = [oldest, parent_source, parent.clone(), child.clone()]
        .map(|path| {
            let bytes = fs::read(&path).expect("source bytes");
            (path, bytes)
        })
        .to_vec();
    let parent_before = list_active_summary_turns(&store, parent_id).await;
    if let Some(binary) = released_binary {
        let output = std::process::Command::new(binary)
            .env("CODEX_HOME", home.path())
            .args([
                "migrate-rollouts",
                "--apply",
                "--json",
                "--thread",
                &child_id.to_string(),
            ])
            .output()
            .expect("run released migration");
        let report: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("released JSON report");
        eprintln!("released status={} report={report}", output.status);
        let selected = store
            .state_db()
            .await
            .expect("database")
            .get_thread(child_id)
            .await
            .expect("metadata")
            .expect("child")
            .rollout_path;
        eprintln!("selected={}", selected.display());
        let turns = list_active_summary_turns(&store, child_id).await;
        eprintln!(
            "turns={:?}",
            turns
                .turns
                .iter()
                .map(|turn| &turn.turn_id)
                .collect::<Vec<_>>()
        );
        eprintln!("retained_repro_home={}", home.keep().display());
        return;
    }
    if previous_policy {
        let phase = stop_after.expect("old journal phase");
        let mut plan = super::lineage::plan_without_native_rollback_replay(home.path(), &child)
            .await
            .expect("old plan");
        assert!(!plan.replay_native_rollbacks);
        let journal_path = migration_journal_path(home.path(), child_id);
        let mut journal = super::lineage_journal::LineageMigrationJournal::from_plan(&plan);
        if phase != LineageMigrationPhase::Planned {
            let staged = super::lineage_compatibility::stage_compatible_lineage(
                home.path(),
                &mut plan,
                &journal_path.with_extension("staging"),
            )
            .await
            .expect("stage old bytes");
            journal
                .record_staged_targets(&staged)
                .expect("record old targets");
        }
        if phase == LineageMigrationPhase::ProjectionDurable {
            journal
                .advance(LineageMigrationPhase::ProjectionDurable)
                .expect("old projection phase");
            let selected_path = journal
                .targets
                .iter()
                .find(|target| target.selected)
                .expect("selected target")
                .path
                .clone();
            super::lineage_publish::publish_lineage_targets(
                &journal_path,
                &mut journal,
                &selected_path,
            )
            .await
            .expect("publish old targets");
            preserved.extend(journal.targets.iter().map(|target| {
                (
                    target.path.clone(),
                    fs::read(&target.path).expect("old published target"),
                )
            }));
        }
        super::lineage_journal::write_lineage_migration_journal(&journal_path, &journal)
            .await
            .expect("write old journal");
    } else if let Some(phase) = stop_after {
        let plan = plan_legacy_lineage(home.path(), &child)
            .await
            .expect("plan child");
        let journal_path = migration_journal_path(home.path(), child_id);
        let mut limiter =
            RolloutMigrationRateLimiter::new(/*max_mib_per_second*/ None).expect("limiter");
        let error = store
            .migrate_legacy_lineage_until_phase_for_test(
                &child,
                &journal_path,
                plan,
                &mut limiter,
                phase,
            )
            .await
            .expect_err("injected phase stop");
        assert!(
            error
                .to_string()
                .contains("injected lineage migration stop"),
            "{error}"
        );
    }
    drop(store);
    let store = indexed_store(home.path()).await;
    let report = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![child_id],
            ..apply_options()
        })
        .await
        .expect("migrate child");
    assert_eq!(
        report.outcomes[0].status,
        RolloutMigrationStatus::Migrated,
        "{:?}",
        report.outcomes[0].message
    );
    let child_turns = list_active_summary_turns(&store, child_id).await;
    assert_eq!(
        child_turns
            .turns
            .iter()
            .map(|turn| turn.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec!["keep-oldest", "keep-child"]
    );
    let model_context = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id: child_id,
            include_archived: false,
        })
        .await
        .expect("child model context");
    let model_json = serde_json::to_string(&model_context.items).expect("model JSON");
    assert!(model_json.contains("keep-oldest"));
    assert!(model_json.contains("keep-child"));
    assert!(!model_json.contains("remove-parent"));
    assert_eq!(
        serde_json::to_value(list_active_summary_turns(&store, parent_id).await)
            .expect("parent after"),
        serde_json::to_value(parent_before).expect("parent before")
    );
    assert_eq!(
        store
            .state_db()
            .await
            .expect("database")
            .get_thread(parent_id)
            .await
            .expect("metadata")
            .expect("parent")
            .rollout_path,
        parent
    );
    for (path, bytes) in preserved {
        assert_eq!(fs::read(path).expect("preserved source"), bytes);
    }
}
