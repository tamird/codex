//! Opt-in measurements over an isolated copy of a rollout dependency closure.

use std::path::PathBuf;
use std::time::Instant;

use super::lineage::plan_legacy_lineage;
use super::lineage_compatibility::stage_compatible_lineage;
use super::lineage_stage::stage_legacy_lineage;
use super::lineage_stage::stage_legacy_lineage_without_context_cache;

#[tokio::test]
#[ignore = "set CODEX_MIGRATION_BENCH_HOME and CODEX_MIGRATION_BENCH_SOURCE to a completed isolated migration"]
async fn benchmark_supplied_lineage_regex_projection_spans() {
    use super::canonical_projection::candidate_spans;
    use super::canonical_projection::project_canonical_record;
    use super::jsonl_spans::JsonlSpanKind;
    use std::time::Duration;

    let home = PathBuf::from(std::env::var_os("CODEX_MIGRATION_BENCH_HOME").expect("home"));
    let source = PathBuf::from(std::env::var_os("CODEX_MIGRATION_BENCH_SOURCE").expect("source"));
    assert!(source.starts_with(&home));
    let plan = plan_legacy_lineage(&home, &source)
        .await
        .expect("source graph");
    let mut reference_time = Duration::ZERO;
    let mut selective_time = Duration::ZERO;
    let mut records = 0;
    let mut candidate_records = 0;
    let mut copied_bytes = 0;
    let mut total_bytes = 0;
    for target in &plan.targets {
        let bytes = std::fs::read(&target.path).expect("canonical target");
        total_bytes += bytes.len();
        let project = |record: &[u8]| {
            let line = project_canonical_record(record).expect("canonical projection");
            (!line.changes.is_empty()).then_some((line.ordinal, line.changes))
        };
        let started = Instant::now();
        let expected = bytes
            .split_inclusive(|byte| *byte == b'\n')
            .filter_map(project)
            .collect::<Vec<_>>();
        reference_time += started.elapsed();
        let started = Instant::now();
        let mut actual = Vec::new();
        for span in candidate_spans(&bytes).expect("candidate regex") {
            records += span.newline_count;
            match span.kind {
                JsonlSpanKind::Copy => copied_bytes += span.range.len(),
                JsonlSpanKind::Candidate => {
                    candidate_records += span.newline_count;
                    actual.extend(
                        bytes[span.range]
                            .split_inclusive(|byte| *byte == b'\n')
                            .filter_map(project),
                    );
                }
            }
        }
        selective_time += started.elapsed();
        assert_eq!(actual, expected, "{}", target.path.display());
    }
    eprintln!(
        "records={records} candidate_records={candidate_records} copied_bytes={copied_bytes} total_bytes={total_bytes} reference_ms={} selective_ms={}",
        reference_time.as_millis(),
        selective_time.as_millis()
    );
}

#[tokio::test]
#[ignore = "set CODEX_MIGRATION_BENCH_HOME and CODEX_MIGRATION_BENCH_SOURCE to plain copied original rollouts"]
async fn benchmark_supplied_lineage_read_capacity() {
    use sha2::Digest;
    use sha2::Sha256;
    use tokio::io::AsyncBufReadExt;

    let home = PathBuf::from(std::env::var_os("CODEX_MIGRATION_BENCH_HOME").expect("home"));
    let source = PathBuf::from(std::env::var_os("CODEX_MIGRATION_BENCH_SOURCE").expect("source"));
    assert!(source.starts_with(&home));
    let plan = plan_legacy_lineage(&home, &source)
        .await
        .expect("source graph");
    let mut expected = None;
    for capacity in [8 * 1024, 256 * 1024, 8 * 1024, 256 * 1024] {
        let started = Instant::now();
        let mut hasher = Sha256::new();
        let mut records = 0_u64;
        for source in &plan.sources {
            assert_ne!(
                source.path.extension().and_then(|value| value.to_str()),
                Some("zst")
            );
            let file = tokio::fs::File::open(&source.path).await.expect("source");
            let mut lines = tokio::io::BufReader::with_capacity(capacity, file).lines();
            while let Some(line) = lines.next_line().await.expect("record") {
                records += 1;
                hasher.update(line.as_bytes());
                hasher.update(b"\n");
            }
        }
        let result = (records, format!("{:x}", hasher.finalize()));
        if let Some(expected) = &expected {
            assert_eq!(&result, expected);
        } else {
            expected = Some(result.clone());
        }
        eprintln!(
            "capacity={capacity} records={records} elapsed_ms={}",
            started.elapsed().as_millis()
        );
    }
}

#[tokio::test]
#[ignore = "set CODEX_MIGRATION_BENCH_HOME and CODEX_MIGRATION_BENCH_SOURCE to copied original rollouts"]
async fn benchmark_supplied_lineage_decode_cost_by_record_type() {
    use serde::Deserialize;
    use serde_json::value::RawValue;
    use std::collections::HashMap;
    use std::time::Duration;
    #[derive(Deserialize)]
    struct Envelope<'a> {
        #[serde(rename = "type", borrow)]
        kind: &'a str,
        #[serde(borrow)]
        payload: &'a RawValue,
    }
    #[derive(Deserialize)]
    struct Kind<'a> {
        #[serde(rename = "type", borrow)]
        kind: &'a str,
    }
    #[derive(Default)]
    struct Cost {
        records: u64,
        bytes: u64,
        decode: Duration,
        encode: Duration,
    }
    let home = PathBuf::from(std::env::var_os("CODEX_MIGRATION_BENCH_HOME").expect("home"));
    let source = PathBuf::from(std::env::var_os("CODEX_MIGRATION_BENCH_SOURCE").expect("source"));
    assert!(source.starts_with(&home));
    let plan = plan_legacy_lineage(&home, &source)
        .await
        .expect("original source graph");
    let mut costs = HashMap::<String, Cost>::new();
    for source in &plan.sources {
        let mut reader = codex_rollout::open_rollout_line_reader(&source.path)
            .await
            .expect("source");
        while let Some(raw) = reader.next_line().await.expect("record") {
            let Ok(envelope) = serde_json::from_str::<Envelope<'_>>(&raw) else {
                continue;
            };
            let kind = if matches!(envelope.kind, "response_item" | "event_msg") {
                serde_json::from_str::<Kind<'_>>(envelope.payload.get())
                    .map(|kind| format!("{}/{}", envelope.kind, kind.kind))
                    .unwrap_or_else(|_| envelope.kind.to_string())
            } else {
                envelope.kind.to_string()
            };
            let cost = costs.entry(kind).or_default();
            cost.records += 1;
            cost.bytes += raw.len() as u64;
            let started = Instant::now();
            let decoded = super::line_parser::parse_legacy_rollout_line(raw.as_bytes());
            cost.decode += started.elapsed();
            if let Ok(Some(decoded)) = decoded {
                let started = Instant::now();
                std::hint::black_box(serde_json::to_vec(&decoded).expect("canonical record"));
                cost.encode += started.elapsed();
            }
        }
    }
    let mut costs = costs.into_iter().collect::<Vec<_>>();
    costs.sort_by_key(|(_, cost)| std::cmp::Reverse(cost.decode));
    for (kind, cost) in costs {
        eprintln!(
            "{kind} records={} bytes={} decode_ms={} encode_ms={}",
            cost.records,
            cost.bytes,
            cost.decode.as_millis(),
            cost.encode.as_millis()
        );
    }
}

#[tokio::test]
#[ignore = "set CODEX_MIGRATION_BENCH_HOME and CODEX_MIGRATION_BENCH_SOURCE to a completed isolated migration"]
async fn benchmark_supplied_lineage_bulk_projection() {
    use crate::local::thread_history;
    use crate::local::thread_history::ProjectedRolloutLine;
    use crate::local::thread_history::RolloutProjectionStep;
    use codex_protocol::ThreadId;
    use std::time::Duration;
    let home =
        PathBuf::from(std::env::var_os("CODEX_MIGRATION_BENCH_HOME").expect("benchmark home"));
    let source =
        PathBuf::from(std::env::var_os("CODEX_MIGRATION_BENCH_SOURCE").expect("benchmark source"));
    assert!(source.starts_with(&home));
    let plan = plan_legacy_lineage(&home, &source)
        .await
        .expect("authenticated original plan");
    let output = tempfile::tempdir().expect("private projection database");
    let store = super::tests::indexed_store(output.path()).await;
    let reference_root = ThreadId::new();
    let actual_root = ThreadId::new();
    let mut root = thread_history::BulkProjection::new(/*initial_ordinal*/ 0);
    let mut parse_reduce = Duration::ZERO;
    let mut reference_sql = Duration::ZERO;
    let mut bulk_sql = Duration::ZERO;
    let mut records = 0_u64;
    for target in &plan.targets {
        let started = Instant::now();
        let metadata = codex_rollout::read_session_meta_line(&target.path)
            .await
            .expect("canonical target head");
        let bytes = std::fs::read(&target.path).expect("completed canonical target");
        let mut offset = 0_u64;
        let mut lines = Vec::new();
        for bytes in bytes.split_inclusive(|byte| *byte == b'\n') {
            let record = super::canonical_projection::project_canonical_record(bytes)
                .expect("canonical record");
            let next_offset = offset + bytes.len() as u64;
            lines.push(ProjectedRolloutLine {
                ordinal: record.ordinal,
                start_byte_offset: offset,
                end_byte_offset: next_offset,
                fallback_created_at_ms: Some(
                    super::parse_rollout_timestamp(&record.timestamp)
                        .expect("timestamp")
                        .timestamp_millis(),
                ),
                changes: record.changes,
                realtime_item: record.realtime_item,
            });
            offset = next_offset;
        }
        let start = lines.first().expect("target records").ordinal;
        let mut physical = thread_history::BulkProjection::new(start);
        root.begin_segment(start).expect("contiguous root segment");
        for line in &lines {
            physical.apply(line).expect("physical reducer");
            root.apply(line).expect("root reducer");
        }
        records += lines.len() as u64;
        parse_reduce += started.elapsed();
        let reference = ThreadId::new();
        let actual = ThreadId::new();
        let complete = metadata.meta.history_base.is_none();
        let started = Instant::now();
        if !complete {
            thread_history::begin_incomplete_paginated_projection(&store, reference, start)
                .await
                .expect("incomplete physical");
        } else {
            thread_history::reset_projection_for_replacement(&store, reference, start)
                .await
                .expect("physical start");
        }
        let steps = lines
            .into_iter()
            .map(Box::new)
            .map(RolloutProjectionStep::Line)
            .collect::<Vec<_>>();
        thread_history::apply_projection(
            &store,
            reference,
            /*start_offset*/ 0,
            offset,
            start,
            steps.clone(),
        )
        .await
        .expect("reference physical SQL");
        thread_history::reset_projection_for_replacement(&store, reference_root, start)
            .await
            .expect("root segment start");
        thread_history::apply_projection(
            &store,
            reference_root,
            /*start_offset*/ 0,
            offset,
            start,
            steps,
        )
        .await
        .expect("reference root SQL");
        reference_sql += started.elapsed();
        let started = Instant::now();
        physical
            .replace_unpublished(&store, actual, complete)
            .await
            .expect("bulk physical SQL");
        bulk_sql += started.elapsed();
        assert_eq!(
            super::tests::complete_projection_rows(&store, actual).await,
            super::tests::complete_projection_rows(&store, reference).await,
            "physical target {}",
            target.path.display()
        );
    }
    let started = Instant::now();
    root.replace_unpublished(&store, actual_root, /*lineage_complete*/ true)
        .await
        .expect("bulk root SQL");
    bulk_sql += started.elapsed();
    assert_eq!(
        super::tests::complete_projection_rows(&store, actual_root).await,
        super::tests::complete_projection_rows(&store, reference_root).await
    );
    eprintln!(
        "records={records} parse_reduce_ms={} reference_sql_ms={} bulk_sql_ms={}",
        parse_reduce.as_millis(),
        reference_sql.as_millis(),
        bulk_sql.as_millis()
    );
}

#[tokio::test]
#[ignore = "set CODEX_MIGRATION_BENCH_HOME and CODEX_MIGRATION_BENCH_SOURCE to isolated copies"]
async fn benchmark_supplied_lineage_staging() {
    let home = PathBuf::from(
        std::env::var_os("CODEX_MIGRATION_BENCH_HOME").expect("isolated benchmark home"),
    );
    let source = PathBuf::from(
        std::env::var_os("CODEX_MIGRATION_BENCH_SOURCE").expect("isolated selected rollout"),
    );
    assert!(
        source.starts_with(&home),
        "source must be inside copied home"
    );
    let started = Instant::now();
    let mut plan = plan_legacy_lineage(&home, &source)
        .await
        .expect("plan supplied lineage");
    eprintln!(
        "plan_ms={} sources={}",
        started.elapsed().as_millis(),
        plan.sources.len()
    );
    let stage = tempfile::tempdir().expect("private staging directory");
    let started = Instant::now();
    let first = stage_legacy_lineage(&plan, stage.path())
        .await
        .expect("stage supplied lineage");
    eprintln!(
        "stage_once_ms={} bytes={}",
        started.elapsed().as_millis(),
        first.iter().map(|target| target.byte_count).sum::<u64>()
    );
    let started = Instant::now();
    let compatible = stage_compatible_lineage(&home, &mut plan, stage.path())
        .await
        .expect("stage compatible supplied lineage");
    eprintln!(
        "stage_compatible_ms={} remapped_ids={} bytes={}",
        started.elapsed().as_millis(),
        plan.synthetic_item_id_remap.len(),
        compatible
            .iter()
            .map(|target| target.byte_count)
            .sum::<u64>()
    );
    assert_eq!(first.len(), compatible.len());
    assert_eq!(
        first.last().map(|target| target.end_ordinal_exclusive),
        compatible.last().map(|target| target.end_ordinal_exclusive)
    );
    let reference_root = tempfile::tempdir().expect("reference staging directory");
    let started = Instant::now();
    let reference = stage_legacy_lineage_without_context_cache(&plan, reference_root.path())
        .await
        .expect("uncached full replay with remap");
    eprintln!("reference_replay_ms={}", started.elapsed().as_millis());
    for (actual, expected) in compatible.iter().zip(&reference) {
        assert_eq!(
            (
                actual.byte_count,
                actual.record_count,
                actual.start_ordinal,
                actual.end_ordinal_exclusive
            ),
            (
                expected.byte_count,
                expected.record_count,
                expected.start_ordinal,
                expected.end_ordinal_exclusive
            )
        );
        let mut actual = codex_rollout::open_rollout_line_reader(&actual.staged_path)
            .await
            .expect("open rewritten target");
        let mut expected = codex_rollout::open_rollout_line_reader(&expected.staged_path)
            .await
            .expect("open replayed target");
        loop {
            let actual = actual.next_line().await.expect("read rewritten record");
            let expected = expected.next_line().await.expect("read replayed record");
            if actual == expected {
                if actual.is_none() {
                    break;
                }
                continue;
            }
            // Protocol HashMaps may choose a different key order on independent full replays.
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&actual.expect("rewritten record"))
                    .expect("rewritten JSON"),
                serde_json::from_str::<serde_json::Value>(&expected.expect("replayed record"))
                    .expect("replayed JSON")
            );
        }
    }
}
