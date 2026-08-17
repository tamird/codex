use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;

use super::DecodeMode;
use super::TurnContextCache;
use crate::local::rollout_migration::line_parser;
use crate::local::rollout_migration::lineage::plan_legacy_lineage;
use crate::local::rollout_migration::lineage_stage::stage_legacy_lineage;
use crate::local::rollout_migration::lineage_stage::stage_legacy_lineage_without_context_cache;

fn context(turn_id: &str) -> Value {
    json!({"timestamp":"2025-01-03T12:00:00Z","type":"turn_context","payload":{
        "turn_id":turn_id,"cwd":std::env::temp_dir(),"approval_policy":"never",
        "sandbox_policy":{"type":"danger-full-access"},"model":"test-model",
        "personality":null,"effort":null,"summary":"auto"
    }})
}

#[test]
fn repeated_configuration_retains_ids_and_exact_canonical_bytes() {
    let mut cache = TurnContextCache::default();
    let mut previous = None;
    for turn_id in ["first", "second\"\\\n", "third"] {
        let bytes = serde_json::to_vec(&context(turn_id)).expect("source");
        let prepared = cache
            .parse(&bytes, DecodeMode::Legacy)
            .expect("cached context");
        if let Some(previous) = previous {
            assert!(Arc::ptr_eq(&previous, &prepared.configuration));
        }
        previous = Some(Arc::clone(&prepared.configuration));
        let mut expected = line_parser::parse_legacy_rollout_line(&bytes)
            .expect("decode")
            .expect("record");
        assert_eq!(
            serde_json::to_value(prepared.rollout_line()).expect("cached JSON"),
            serde_json::to_value(&expected).expect("reference JSON")
        );
        expected.ordinal = Some(123);
        assert_eq!(
            prepared
                .canonical_record(/*ordinal*/ 123)
                .expect("cached encoding"),
            serde_json::to_vec(&expected).expect("reference encoding")
        );
    }
    assert_eq!(cache.entries.len(), 1);
}

#[test]
fn warmed_configuration_cache_rejects_object_valued_record_type() {
    for mode in [DecodeMode::Legacy, DecodeMode::Paginated] {
        let mut cache = TurnContextCache::default();
        let mut value = context("turn");
        let valid = serde_json::to_vec(&value).expect("valid context");
        assert!(cache.parse(&valid, mode).is_some());
        value["type"] = json!({"turn_context": null});
        let malformed = serde_json::to_vec(&value).expect("malformed context");
        assert!(line_parser::parse_legacy_rollout_line(&malformed).is_err());
        assert!(line_parser::parse_paginated_rollout_line(&malformed).is_err());
        assert!(cache.parse(&malformed, mode).is_none());
        assert!(cache.parse(&valid, mode).is_some());
    }
}

#[test]
fn configuration_cache_preserves_strict_and_legacy_acceptance() {
    let mut cache = TurnContextCache::default();
    let mut value = context("turn");
    value["payload"]["sandbox_policy"] = json!({"mode":"danger-full-access"});
    let bytes = serde_json::to_vec(&value).expect("legacy record");
    assert!(cache.parse(&bytes, DecodeMode::Legacy).is_some());
    assert!(cache.parse(&bytes, DecodeMode::Paginated).is_none());
    for mutation in ["turn_id", "outer", "ordinal", "timestamp"] {
        let mut value = context("turn");
        match mutation {
            "turn_id" => value["payload"]["turn_id"] = json!(false),
            "outer" => {
                value["future"] =
                    serde_json::from_str("18446744073709551616").expect("wide integer")
            }
            "ordinal" => value["ordinal"] = json!(-1),
            "timestamp" => value["timestamp"] = Value::Null,
            _ => unreachable!(),
        }
        assert!(
            cache
                .parse(
                    &serde_json::to_vec(&value).expect("invalid fixture"),
                    DecodeMode::Legacy
                )
                .is_none()
        );
    }
    let mut changed = context("other");
    changed["payload"]["model"] = json!("changed-model");
    let prepared = cache
        .parse(
            &serde_json::to_vec(&changed).expect("changed source"),
            DecodeMode::Legacy,
        )
        .expect("changed context");
    assert_eq!(prepared.configuration.context.model, "changed-model");
}

#[tokio::test]
async fn cached_staging_matches_uncached_rollback_and_generated_item_coordinates() {
    use crate::local::rollout_migration::tests;
    use codex_protocol::ThreadId;
    use codex_protocol::protocol::EventMsg;
    use codex_protocol::protocol::SessionSource;
    use codex_protocol::protocol::ThreadRolledBackEvent;
    use codex_rollout::RolloutItem;

    let home = tempfile::tempdir().expect("isolated home");
    let context_item = |turn_id| {
        line_parser::parse_legacy_rollout_line(
            &serde_json::to_vec(&context(turn_id)).expect("context JSON"),
        )
        .expect("decode context")
        .expect("context record")
        .item
    };
    let source = tests::write_rollout(
        home.path(),
        ThreadId::new(),
        SessionSource::Cli,
        vec![
            tests::turn_started("keep"),
            context_item("keep"),
            tests::user_message("keep question"),
            tests::turn_complete("keep"),
            tests::turn_started("remove"),
            context_item("remove"),
            tests::user_message("remove question"),
            tests::turn_complete("remove"),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
            tests::turn_started("replacement"),
            context_item("replacement"),
            tests::user_message("replacement question"),
            tests::turn_complete("replacement"),
        ],
    );
    let original = std::fs::read(&source).expect("original source");
    let plan = plan_legacy_lineage(home.path(), &source)
        .await
        .expect("source plan");
    let cached_root = tempfile::tempdir().expect("cached staging");
    let reference_root = tempfile::tempdir().expect("uncached staging");
    let cached = stage_legacy_lineage(&plan, cached_root.path())
        .await
        .expect("cached staging");
    let reference = stage_legacy_lineage_without_context_cache(&plan, reference_root.path())
        .await
        .expect("uncached staging");
    assert_eq!(cached.len(), reference.len());
    for (mut actual, expected) in cached.into_iter().zip(reference) {
        assert_eq!(
            std::fs::read(&actual.staged_path).expect("cached target"),
            std::fs::read(&expected.staged_path).expect("uncached target")
        );
        actual.staged_path = expected.staged_path.clone();
        assert_eq!(actual, expected);
    }
    assert_eq!(std::fs::read(source).expect("preserved source"), original);
}

#[tokio::test]
#[ignore = "set CODEX_MIGRATION_BENCH_HOME and CODEX_MIGRATION_BENCH_SOURCE to copied original rollouts"]
async fn benchmark_supplied_lineage_turn_context_cache() {
    let home = PathBuf::from(std::env::var_os("CODEX_MIGRATION_BENCH_HOME").expect("home"));
    let source = PathBuf::from(std::env::var_os("CODEX_MIGRATION_BENCH_SOURCE").expect("source"));
    assert!(source.starts_with(&home));
    let plan = plan_legacy_lineage(&home, &source)
        .await
        .expect("source graph");
    let mut cache = TurnContextCache::default();
    let mut records = 0_u64;
    let mut eligible = 0_u64;
    let mut full = Duration::ZERO;
    let mut cached = Duration::ZERO;
    let mut cloning = Duration::ZERO;
    let mut ineligible = Duration::ZERO;
    for source in &plan.sources {
        let mut reader = codex_rollout::open_rollout_line_reader(&source.path)
            .await
            .expect("source");
        while let Some(raw) = reader.next_line().await.expect("record") {
            records += 1;
            let started = Instant::now();
            let Some(prepared) = cache.parse(raw.as_bytes(), DecodeMode::Legacy) else {
                ineligible += started.elapsed();
                continue;
            };
            let actual = prepared
                .canonical_record(/*ordinal*/ 123)
                .expect("cached bytes");
            cached += started.elapsed();
            let started = Instant::now();
            std::hint::black_box(prepared.rollout_line());
            cloning += started.elapsed();
            let started = Instant::now();
            let mut reference = line_parser::parse_legacy_rollout_line(raw.as_bytes())
                .expect("reference decoder")
                .expect("reference record");
            reference.ordinal = Some(123);
            let expected = serde_json::to_vec(&reference).expect("reference bytes");
            full += started.elapsed();
            assert_eq!(actual, expected, "{}", source.path.display());
            eligible += 1;
        }
    }
    assert!(eligible > 0);
    eprintln!(
        "records={records} eligible={eligible} entries={} charged_bytes={} full_ms={} cached_ms={} clone_ms={} ineligible_ms={}",
        cache.entries.len(),
        cache.charged_bytes,
        full.as_millis(),
        cached.as_millis(),
        cloning.as_millis(),
        ineligible.as_millis()
    );
}
