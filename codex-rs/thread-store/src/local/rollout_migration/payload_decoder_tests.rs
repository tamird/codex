use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use codex_rollout::RolloutLine;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;

use super::decode;
use crate::local::rollout_migration::line_parser;

fn assert_same_result(
    expected: Result<Option<RolloutLine>, String>,
    actual: Result<Option<RolloutLine>, String>,
) {
    match (expected, actual) {
        (Ok(expected), Ok(actual)) => assert_eq!(
            expected.map(|line| serde_json::to_value(line).expect("reference JSON")),
            actual.map(|line| serde_json::to_value(line).expect("candidate JSON")),
        ),
        (Err(_), Err(_)) => {}
        (expected, actual) => panic!(
            "decoder acceptance differs: reference error {:?}, candidate error {:?}",
            expected.err(),
            actual.err()
        ),
    }
}

fn reference(bytes: &[u8], normalize: bool) -> Result<Option<RolloutLine>, String> {
    let value = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    let value = if normalize {
        line_parser::normalize_legacy_rollout_value(value)?
    } else {
        Some(value)
    };
    value
        .map(|value| codex_rollout::decode_rollout_line(value).map_err(|error| error.to_string()))
        .transpose()
}

fn candidate(bytes: &[u8], normalize: bool) -> Result<Option<RolloutLine>, String> {
    if normalize {
        line_parser::parse_legacy_rollout_line(bytes)
    } else {
        line_parser::parse_paginated_rollout_line(bytes).map(Some)
    }
}

#[test]
fn guarded_payload_decoder_preserves_numeric_and_malformed_acceptance() {
    for number in [
        "0",
        "-1",
        "1.2300",
        "-0.0",
        "1e2",
        "18446744073709551615",
        "18446744073709551616",
        "0.123456789012345678901",
        "1e400",
    ] {
        let number: Value = serde_json::from_str(number).expect("numeric JSON");
        for value in [
            json!({"timestamp":"now","type":"event_msg","payload":{
                "type":"token_count","info":null,"rate_limits":{"primary":{
                    "used_percent":number,"window_minutes":300,"resets_at":1800000000}}}}),
            json!({"timestamp":"now","type":"compacted","payload":{"message":"checkpoint"},"future":number}),
            json!({"timestamp":"now","type":"compacted","payload":{"message":"checkpoint","future":[number]}}),
        ] {
            let bytes = serde_json::to_vec(&value).expect("fixture JSON");
            for normalize in [false, true] {
                assert_same_result(reference(&bytes, normalize), candidate(&bytes, normalize));
            }
        }
    }
    for encoded in [
        r#"{"timestamp":"old","timestamp":"new","type":"event_msg","payload":{"type":"warning","message":"old","message":"new"}}"#,
        r#"{"timestamp":"now","type":"response_item","payload":{"type":"message","role":"developer","content":[]},"metadata":{"client_authored":true}}"#,
        r#"{"timestamp":"now","type":"response_item","payload":{"type":"message","role":"user","content":[]},"metadata":null}"#,
        r#"{"timestamp":"now","type":"response_item","payload":{"type":"message","role":"user","content":[]},"metadata":false}"#,
        r#"{"timestamp":"now","type":"compacted","payload":{"message":"checkpoint","replacement_history":[],"replacement_history_metadata":[{}]}}"#,
        r#"{"timestamp":"now","type":"compacted","payload":{"message":"checkpoint","replacement_history":[{"type":"message","role":"user","content":[]}],"replacement_history_metadata":[{"client_authored":true}]}}"#,
        r#"{"timestamp":"now","type":"compacted","payload":null}"#,
        r#"{"timestamp":"now","type":"compacted"}"#,
        r#"{"timestamp":"now","ordinal":-1,"type":"event_msg","payload":{"type":"warning","message":"x"}}"#,
        r#"{"timestamp":"now","type":"event_msg","payload":{"type":"unknown"}}"#,
        r#"{"timestamp":"now","type":"fork_reference","payload":{}}"#,
        r#"{"timestamp":"now","type":"unknown","payload":{}}"#,
        r#"{"timestamp":"now","type":"compacted","payload":{"message":"checkpoint"},"future":18446744073709551616}"#,
        r#"{"timestamp":"now","type":"event_msg","payload":{"type":"guardian_assessment"}}"#,
    ] {
        for normalize in [false, true] {
            assert_same_result(
                reference(encoded.as_bytes(), normalize),
                candidate(encoded.as_bytes(), normalize),
            );
        }
    }
    let oversized: Value = serde_json::from_str(r#"{"timestamp":"now","type":"compacted","payload":{"message":"checkpoint"},"future":18446744073709551616}"#).expect("oversized integer fixture");
    assert!(codex_rollout::decode_rollout_line(oversized.clone()).is_err());
    assert!(decode(oversized).is_err());
}

/// Private incident rollouts stay outside the repository; compare both unmodified and normalized
/// inputs because source authentication and Legacy canonicalization use different acceptance.
#[tokio::test]
#[ignore = "set CODEX_ROLLOUT_DECODER_CORPUS to copied rollout files"]
async fn benchmark_guarded_payload_decoder_against_existing_decoder() {
    let mut directories = vec![PathBuf::from(
        std::env::var_os("CODEX_ROLLOUT_DECODER_CORPUS").expect("corpus directory"),
    )];
    let mut records = 0_u64;
    let mut existing = Duration::ZERO;
    let mut guarded = Duration::ZERO;
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(directory).expect("corpus directory") {
            let entry = entry.expect("corpus entry");
            let path = entry.path();
            if entry.file_type().expect("file type").is_dir() {
                directories.push(path);
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with("rollout-")
                || !(name.ends_with(".jsonl") || name.ends_with(".jsonl.zst"))
            {
                continue;
            }
            let mut reader = codex_rollout::open_rollout_line_reader(&path)
                .await
                .expect("source reader");
            while let Some(raw) = reader.next_line().await.expect("source record") {
                for normalize in [false, true] {
                    let started = Instant::now();
                    let expected = reference(raw.as_bytes(), normalize);
                    existing += started.elapsed();
                    let started = Instant::now();
                    let actual = candidate(raw.as_bytes(), normalize);
                    guarded += started.elapsed();
                    assert_same_result(expected, actual);
                }
                records += 1;
            }
        }
    }
    assert!(records > 0);
    eprintln!(
        "records={records} raw_and_normalized_existing_ms={} guarded_ms={}",
        existing.as_millis(),
        guarded.as_millis()
    );
}
