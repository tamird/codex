use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::path::Path;

use pretty_assertions::assert_eq;
use serde::Deserialize;
use serde::Serialize;

use super::ReverseJsonlScanner;
use super::ScanOutcome;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct TestRecord {
    value: String,
}

fn record(value: &str) -> TestRecord {
    TestRecord {
        value: value.to_string(),
    }
}

fn parsed<T>(outcome: Option<ScanOutcome<T>>) -> T {
    let Some(ScanOutcome::Parsed(record)) = outcome else {
        panic!("expected parsed record");
    };
    record
}

fn assert_records<R>(scanner: &mut ReverseJsonlScanner<R>, expected: &[&str]) -> std::io::Result<()>
where
    R: Read + Seek,
{
    for value in expected {
        assert_eq!(parsed(scanner.scan_next::<TestRecord>()?), record(value));
    }
    assert!(scanner.scan_next::<TestRecord>()?.is_none());
    Ok(())
}

#[test]
fn scans_jsonl_records_from_end() -> std::io::Result<()> {
    let input = br#"{"value":"first"}
{"value":"second"}
{"value":"third"}
"#;

    assert_records(
        &mut ReverseJsonlScanner::new(Cursor::new(input))?,
        &["third", "second", "first"],
    )
}

#[test]
fn reports_source_bytes_read_from_the_logical_end() -> std::io::Result<()> {
    let input = format!(
        "{}\n{}\n",
        serde_json::to_string(&record("first"))?,
        serde_json::to_string(&record(&"x".repeat(super::READ_CHUNK_SIZE * 2)))?
    );
    let input_len = input.len() as u64;
    let mut scanner = ReverseJsonlScanner::new(Cursor::new(input.into_bytes()))?;

    assert_eq!(scanner.bytes_scanned(), 0);
    let _ = scanner.scan_next::<TestRecord>()?;
    assert_eq!(scanner.bytes_scanned(), input_len);

    Ok(())
}

#[test]
fn rejects_invalid_json_and_continues_scanning() -> std::io::Result<()> {
    let input = br#"{"value":"first"}
not-json
{"value":"third"}
"#;
    let mut scanner = ReverseJsonlScanner::new(Cursor::new(input))?;

    assert_eq!(parsed(scanner.scan_next::<TestRecord>()?), record("third"));
    let Some(ScanOutcome::Rejected(error)) = scanner.scan_next::<TestRecord>()? else {
        panic!("expected rejected record");
    };
    assert!(error.is_syntax());
    assert_eq!(parsed(scanner.scan_next::<TestRecord>()?), record("first"));
    Ok(())
}

#[test]
fn skips_records_over_the_configured_limit() -> std::io::Result<()> {
    let oversized = record(&"x".repeat(128));
    let input = format!(
        "{}\n{}\n{}\n",
        serde_json::to_string(&record("first"))?,
        serde_json::to_string(&oversized)?,
        serde_json::to_string(&record("third"))?
    );
    let mut scanner = ReverseJsonlScanner::new(Cursor::new(input.into_bytes()))?
        .with_max_record_bytes(/*max_record_bytes*/ 32);

    assert_records(&mut scanner, &["third", "first"])?;
    assert_eq!(scanner.oversized_records_skipped(), 1);
    Ok(())
}

#[test]
fn counts_an_oversized_record_at_the_start_of_the_file() -> std::io::Result<()> {
    let input = serde_json::to_string(&record(&"x".repeat(128)))?;
    let mut scanner = ReverseJsonlScanner::new(Cursor::new(input.into_bytes()))?
        .with_max_record_bytes(/*max_record_bytes*/ 32);

    assert!(scanner.scan_next::<TestRecord>()?.is_none());
    assert_eq!(scanner.oversized_records_skipped(), 1);
    Ok(())
}

#[test]
fn accepts_valid_json_at_eof() -> std::io::Result<()> {
    let input = b"{\"value\":\"first\"}\n{\"value\":\"second\"}";

    assert_records(
        &mut ReverseJsonlScanner::new(Cursor::new(input))?,
        &["second", "first"],
    )
}

#[test]
fn scans_from_a_frozen_prefix_end() -> std::io::Result<()> {
    let prefix = b"{\"value\":\"first\"}\n{\"value\":\"second\"}\n";
    let mut input = prefix.to_vec();
    input.extend_from_slice(b"{\"value\":\"later\"}\n");

    assert_records(
        &mut ReverseJsonlScanner::new_at(Cursor::new(input), prefix.len() as u64)?,
        &["second", "first"],
    )
}

#[test]
fn rejects_invalid_json_at_eof_and_continues_scanning() -> std::io::Result<()> {
    let input = b"{\"value\":\"first\"}\n{\"value\":";
    let mut scanner = ReverseJsonlScanner::new(Cursor::new(input))?;

    let Some(ScanOutcome::Rejected(error)) = scanner.scan_next::<TestRecord>()? else {
        panic!("expected rejected record");
    };
    assert!(error.is_eof());
    assert_eq!(parsed(scanner.scan_next::<TestRecord>()?), record("first"));
    Ok(())
}

#[test]
fn skips_blank_lines_with_or_without_termination() -> std::io::Result<()> {
    let input = b"{\"value\":\"first\"}\r\n\n \t\r";

    assert_records(
        &mut ReverseJsonlScanner::new(Cursor::new(input))?,
        &["first"],
    )
}

#[test]
fn scans_across_read_chunk_boundaries() -> std::io::Result<()> {
    let empty_record_len = serde_json::to_string(&record(""))?.len();
    for distance_from_eof in [
        super::READ_CHUNK_SIZE - 1,
        super::READ_CHUNK_SIZE,
        super::READ_CHUNK_SIZE + 1,
    ] {
        let large_value = "x".repeat(distance_from_eof - empty_record_len - 2);
        let input = format!(
            "{}\n{}\n",
            serde_json::to_string(&record("first"))?,
            serde_json::to_string(&record(&large_value))?
        );
        let mut scanner = ReverseJsonlScanner::new(Cursor::new(input.into_bytes()))?;

        assert_eq!(
            parsed(scanner.scan_next::<TestRecord>()?),
            record(&large_value)
        );
        assert_eq!(parsed(scanner.scan_next::<TestRecord>()?), record("first"));
    }
    Ok(())
}

#[test]
fn scans_record_spanning_three_read_chunks() -> std::io::Result<()> {
    let large_value = "x".repeat(super::READ_CHUNK_SIZE * 2);
    let input = format!(
        "{}\n{}\n{}\n",
        serde_json::to_string(&record("first"))?,
        serde_json::to_string(&record(&large_value))?,
        serde_json::to_string(&record("third"))?
    );
    let mut scanner = ReverseJsonlScanner::new(Cursor::new(input.into_bytes()))?;

    assert_records(&mut scanner, &["third", &large_value, "first"])
}

#[test]
fn scans_rollout_line_with_arbitrary_precision_decimal_payload() -> std::io::Result<()> {
    let input = br#"{"timestamp":"2026-08-14T00:00:00Z","ordinal":7,"type":"event_msg","payload":{"type":"token_count","info":null,"rate_limits":{"primary":{"used_percent":5.0,"window_minutes":300,"resets_at":1786689000},"secondary":{"used_percent":12.5,"window_minutes":10080,"resets_at":1787292000}}}}
"#;
    let directly_decoded = serde_json::from_slice::<crate::RolloutLine>(input)?;
    assert_eq!(directly_decoded.ordinal, Some(7));
    let mut scanner = ReverseJsonlScanner::new(Cursor::new(input))?;

    let line = parsed(scanner.scan_next_rollout_line()?);

    assert_eq!(line.ordinal, Some(7));
    assert!(matches!(
        line.item,
        crate::RolloutItem::EventMsg(codex_protocol::protocol::EventMsg::TokenCount(_))
    ));
    Ok(())
}

#[test]
fn runtime_history_readers_use_rollout_compatibility_decoders() {
    fn inspect(path: &Path, violations: &mut Vec<String>) {
        if path.is_dir() {
            for entry in std::fs::read_dir(path).expect("read source directory") {
                inspect(&entry.expect("read source entry").path(), violations);
            }
            return;
        }
        if path.extension().and_then(|extension| extension.to_str()) != Some("rs")
            || path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "tests.rs" || name.ends_with("_tests.rs"))
            || path
                .components()
                .any(|component| component.as_os_str() == "tests")
        {
            return;
        }
        let source = std::fs::read_to_string(path).expect("read Rust source");
        for (index, line) in source.lines().enumerate() {
            let direct_turbofish = line.contains("::<RolloutLine>")
                || line.contains("::<codex_rollout::RolloutLine>")
                || line.contains("::<crate::RolloutLine>");
            let inferred_result =
                line.contains("Result<RolloutLine") && line.contains("serde_json::from_");
            if direct_turbofish || inferred_result {
                violations.push(format!("{}:{}: {line}", path.display(), index + 1));
            }
        }
    }

    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rollout crate has workspace parent");
    let mut violations = Vec::new();
    for relative in ["rollout/src", "app-server/src", "tui/src"] {
        let root = workspace.join(relative);
        if root.exists() {
            inspect(root.as_path(), &mut violations);
        }
    }
    for relative in [
        "thread-store/src/local/live_writer.rs",
        "thread-store/src/local/rollout_lineage.rs",
        "thread-store/src/local/segment.rs",
        "thread-store/src/local/thread_history/read.rs",
        "thread-store/src/local/thread_history_materialization.rs",
    ] {
        let file = workspace.join(relative);
        if file.exists() {
            inspect(file.as_path(), &mut violations);
        }
    }

    assert!(
        violations.is_empty(),
        "persisted RolloutLine records must use decode_rollout_line, \
         RolloutRecorder::parse_rollout_line_*, or scan_next_rollout_line:\n{}",
        violations.join("\n")
    );
}
