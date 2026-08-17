use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use codex_protocol::realtime::BemItemPresentation;
use codex_protocol::realtime::RealtimeItem;
use codex_protocol::realtime::RealtimeItemContent;
use codex_protocol::realtime::RealtimeSessionOutcome;
use codex_rollout::RolloutItem;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::RolloutMigrationRateLimiter;
use super::lineage::hash_file;
use super::lineage::plan_legacy_lineage;
use super::lineage_journal::LineageMigrationJournal;
use super::lineage_projection::BULK_PROJECTION_BUDGET;
use super::lineage_projection::complete_staged_root;
use super::lineage_projection::try_project_staged_targets;
use super::lineage_stage::stage_legacy_lineage;
use super::tests::complete_projection_rows;
use super::tests::indexed_store;
use super::tests::set_paginated_subagent_history_start;
use super::tests::user_message;
use super::tests::write_rollout;
use super::thread_history;

#[tokio::test]
async fn bulk_lineage_projection_checks_phase_budget_and_authenticated_coordinates() {
    let home = tempfile::tempdir().expect("Codex home");
    let source = write_rollout(
        home.path(),
        ThreadId::new(),
        SessionSource::Cli,
        vec![user_message("retained user message")],
    );
    let plan = plan_legacy_lineage(home.path(), &source)
        .await
        .expect("plan source");
    let stage = tempfile::tempdir().expect("private stage");
    let staged = stage_legacy_lineage(&plan, stage.path())
        .await
        .expect("canonical targets");
    let store = indexed_store(home.path()).await;
    let mut journal = LineageMigrationJournal::from_plan(&plan);
    let mut limiter =
        RolloutMigrationRateLimiter::new(/*max_mib_per_second*/ None).expect("unlimited migration");
    assert!(
        try_project_staged_targets(
            &store,
            &journal,
            /*complete_root*/ None,
            &mut limiter,
            BULK_PROJECTION_BUDGET
        )
        .await
        .is_err()
    );
    journal
        .record_staged_targets(&staged)
        .expect("durable targets");
    journal.verify_sources().await.expect("unchanged source");
    journal
        .verify_staged_targets()
        .await
        .expect("authenticated target");
    let target = journal.targets[0].rollout_id;
    assert!(
        !try_project_staged_targets(
            &store,
            &journal,
            Some(target),
            &mut limiter,
            /*memory_budget*/ 0
        )
        .await
        .expect("bounded fallback")
    );
    assert!(
        thread_history::projection_state(&store, target)
            .await
            .expect("projection state")
            .is_none()
    );

    let mut wrong_boundary = journal.clone();
    *wrong_boundary.targets[0]
        .byte_count
        .as_mut()
        .expect("byte boundary") += 1;
    assert!(
        try_project_staged_targets(
            &store,
            &wrong_boundary,
            Some(target),
            &mut limiter,
            BULK_PROJECTION_BUDGET
        )
        .await
        .is_err()
    );
    assert!(
        thread_history::projection_state(&store, target)
            .await
            .expect("projection state")
            .is_none()
    );

    assert!(
        try_project_staged_targets(
            &store,
            &journal,
            Some(target),
            &mut limiter,
            BULK_PROJECTION_BUDGET
        )
        .await
        .expect("bulk projection")
    );
    let reference = ThreadId::new();
    thread_history::reset_projection_for_replacement(
        &store, reference, /*next_rollout_ordinal*/ 0,
    )
    .await
    .expect("reference checkpoint");
    store
        .project_rollout_in_batches(
            reference,
            &staged[0].staged_path,
            /*complete_root*/ None,
            &mut limiter,
        )
        .await
        .expect("ordered SQL writer");
    assert_eq!(
        complete_projection_rows(&store, target).await,
        complete_projection_rows(&store, reference).await
    );
}

#[tokio::test]
async fn realtime_only_lineage_projection_preserves_payloads_budget_and_subagent_boundary() {
    let items = [
        RealtimeItem {
            id: "parent:started".to_string(),
            realtime_session_id: "voice".to_string(),
            content: RealtimeItemContent::RealtimeSessionStarted,
        },
        RealtimeItem {
            id: "child:promoted".to_string(),
            realtime_session_id: "voice".to_string(),
            content: RealtimeItemContent::BemItemPromoted {
                turn_id: "child-turn".to_string(),
                item_id: "artifact".to_string(),
                presentation: BemItemPresentation::InlineVisualization { index: 3 },
            },
        },
        RealtimeItem {
            id: "child:closed".to_string(),
            realtime_session_id: "voice".to_string(),
            content: RealtimeItemContent::RealtimeSessionClosed {
                outcome: RealtimeSessionOutcome::Ended,
            },
        },
    ];
    for (boundary, expected_ordinals) in [
        (None, vec![1, 2, 3]),
        (Some(2), vec![2, 3]),
        (Some(4), Vec::new()),
    ] {
        let home = tempfile::tempdir().expect("Codex home");
        let source = write_rollout(
            home.path(),
            ThreadId::new(),
            SessionSource::Cli,
            items
                .iter()
                .cloned()
                .map(RolloutItem::RealtimeItem)
                .collect(),
        );
        let plan = plan_legacy_lineage(home.path(), &source)
            .await
            .expect("plan source");
        let stage = tempfile::tempdir().expect("private stage");
        let mut staged = stage_legacy_lineage(&plan, stage.path())
            .await
            .expect("canonical target");
        if let Some(boundary) = boundary {
            // Exercise the projection readers with an authenticated canonical subagent boundary.
            set_paginated_subagent_history_start(&staged[0].staged_path, boundary);
            (staged[0].byte_count, staged[0].sha256) = hash_file(&staged[0].staged_path)
                .await
                .expect("authenticate adjusted staged fixture");
        }
        let mut journal = LineageMigrationJournal::from_plan(&plan);
        journal
            .record_staged_targets(&staged)
            .expect("durable targets");
        journal.verify_sources().await.expect("unchanged source");
        journal
            .verify_staged_targets()
            .await
            .expect("authenticated target");
        let target = journal.targets[0].rollout_id;
        let complete_root = complete_staged_root(&plan, &journal)
            .await
            .expect("root eligibility");
        assert_eq!(complete_root, boundary.is_none().then_some(target));
        let store = indexed_store(home.path()).await;
        let mut limiter = RolloutMigrationRateLimiter::new(/*max_mib_per_second*/ None)
            .expect("unlimited migration");
        let fits_zero_budget = try_project_staged_targets(
            &store,
            &journal,
            complete_root,
            &mut limiter,
            /*memory_budget*/ 0,
        )
        .await
        .expect("realtime-only budget decision");
        assert_eq!(fits_zero_budget, expected_ordinals.is_empty());
        if !fits_zero_budget {
            assert!(
                thread_history::projection_state(&store, target)
                    .await
                    .expect("unpublished checkpoint")
                    .is_none()
            );
        }

        // This is the same bounded SQL producer used when the bulk memory budget is exceeded.
        let reference = ThreadId::new();
        thread_history::reset_projection_for_replacement(
            &store, reference, /*next_rollout_ordinal*/ 0,
        )
        .await
        .expect("fallback checkpoint");
        store
            .project_rollout_in_batches(
                reference,
                &staged[0].staged_path,
                /*complete_root*/ None,
                &mut limiter,
            )
            .await
            .expect("bounded SQL fallback");
        let expected_realtime = expected_ordinals
            .into_iter()
            .map(|ordinal| {
                let item = &items[ordinal - 1];
                let value = serde_json::to_value(item).expect("realtime payload");
                json!([
                    item.id,
                    ordinal,
                    1_735_905_600_000_i64,
                    value["type"],
                    serde_json::to_string(item).expect("serialize realtime payload"),
                ])
                .to_string()
            })
            .collect::<Vec<_>>();
        let expected = complete_projection_rows(&store, reference).await;
        assert_eq!(expected[2], expected_realtime);
        assert_eq!(
            expected[3],
            vec![json!([staged[0].byte_count, 4]).to_string()]
        );
        assert!(
            try_project_staged_targets(
                &store,
                &journal,
                complete_root,
                &mut limiter,
                BULK_PROJECTION_BUDGET,
            )
            .await
            .expect("bulk realtime projection")
        );
        assert_eq!(complete_projection_rows(&store, target).await, expected);
    }
}
