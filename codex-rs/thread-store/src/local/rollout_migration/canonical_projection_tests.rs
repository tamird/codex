use codex_app_server_protocol::project_rollout_line;
use codex_protocol::ThreadId;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::project_canonical_record;
use crate::local::rollout_migration::line_parser::parse_paginated_rollout_line;

fn assert_matches_full_parser(value: serde_json::Value) {
    let record = serde_json::to_string(&value).expect("serialize record");
    let escaped = record.replace(r#""type""#, r#""\u0074ype""#);
    for record in [&record, &escaped] {
        let full = parse_paginated_rollout_line(record.as_bytes()).expect("parse complete record");
        let projected = project_canonical_record(record.as_bytes()).expect("project record");
        let realtime_item = if let RolloutItem::RealtimeItem(item) = &full.item {
            Some(item)
        } else {
            None
        };
        assert_eq!(
            (
                projected.timestamp.as_ref(),
                Some(projected.ordinal),
                projected.changes,
                projected.realtime_item.as_ref(),
            ),
            (
                full.timestamp.as_str(),
                full.ordinal,
                project_rollout_line(&full),
                realtime_item,
            )
        );
    }
}

#[test]
fn selective_projection_matches_lifecycle_aliases_and_completed_items() {
    for kind in [
        "task_started",
        "turn_started",
        "task_complete",
        "turn_complete",
        "turn_aborted",
    ] {
        assert_matches_full_parser(json!({
            "timestamp": "2026-08-20T00:00:00Z", "ordinal": 42,
            "type": "event_msg", "payload": {
                "type": kind, "turn_id": "turn-1", "reason": "interrupted",
                "started_at": 10, "completed_at": 20, "duration_ms": 10000,
            }
        }));
    }
    assert_matches_full_parser(
        serde_json::to_value(RolloutLine {
            timestamp: "2026-08-20T00:00:00Z".into(),
            ordinal: Some(43),
            item: RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
                thread_id: ThreadId::default(),
                turn_id: "turn-1".into(),
                item: TurnItem::UserMessage(UserMessageItem {
                    id: "user-1".into(),
                    client_id: None,
                    content: Vec::new(),
                }),
                started_at_ms: Some(100),
                completed_at_ms: 200,
            })),
        })
        .expect("serialize completed item"),
    );
}

#[test]
fn selective_projection_preserves_complete_realtime_payloads() {
    for content in [
        json!({"type": "realtime_session_started"}),
        json!({"type": "transcript_segment", "role": "assistant", "text": "A spoken result."}),
        json!({"type": "bem_item_promoted", "turn_id": "turn-1", "item_id": "artifact-1",
            "presentation": {"type": "inline_visualization", "index": 7}}),
        json!({"type": "realtime_session_closed", "outcome": "failed"}),
    ] {
        let mut payload = content;
        payload["id"] = json!("voice:item");
        payload["realtime_session_id"] = json!("voice");
        // The numeric presentation index traverses tagged enums with arbitrary_precision active.
        assert_matches_full_parser(json!({
            "timestamp": "2026-08-20T01:02:03.456+02:00", "ordinal": 45,
            "type": "realtime_item", "payload": payload,
        }));
    }
}

#[test]
fn selective_projection_matches_large_ignored_payloads_and_numeric_events() {
    let text = "tool output\n".repeat(100_000);
    for (kind, payload) in [
        (
            "response_item",
            json!({"type": "function_call_output", "call_id": "call-1", "output": text}),
        ),
        (
            "compacted",
            json!({"message": "summary", "replacement_history": [
                {"type": "function_call_output", "call_id": "call-1", "output": text}
            ]}),
        ),
        (
            "event_msg",
            json!({"type": "token_count", "info": null, "rate_limits": {
                "primary": {"used_percent": 2.5, "window_minutes": 300, "resets_at": 1770414841}
            }}),
        ),
    ] {
        assert_matches_full_parser(json!({
            "timestamp": "2026-08-20T00:00:00Z", "ordinal": 44,
            "type": kind, "payload": payload,
        }));
    }
}

#[test]
fn selective_projection_rejects_invalid_json_coordinates_and_visible_events() {
    for record in [
        r#"{"timestamp":"now","ordinal":1,"type":"response_item","payload":{"a":]}"#,
        r#"{"timestamp":"now","type":"response_item","payload":{}}"#,
        r#"{"timestamp":"now","ordinal":-1,"type":"response_item","payload":{}}"#,
        r#"{"timestamp":"now","ordinal":1,"type":"event_msg","payload":{"type":"item_completed"}}"#,
        r#"{"timestamp":"now","ordinal":1,"type":"unknown","payload":{}}"#,
    ] {
        assert!(
            project_canonical_record(record.as_bytes()).is_err(),
            "{record}"
        );
    }
}

#[tokio::test]
async fn regex_spans_preserve_all_projection_changes_across_read_chunks() {
    use super::candidate_spans;
    use super::read_canonical_chunk;
    use crate::local::rollout_migration::jsonl_spans::JsonlSpanKind;

    let ignored = json!({"timestamp":"2026-08-20T00:00:00Z","ordinal":0,
        "type":"response_item","payload":{"type":"function_call_output",
        "call_id":"call","output":"unchanged bytes".repeat(50_000)}});
    let mut bytes = serde_json::to_vec(&ignored).expect("large record");
    bytes.push(b'\n');
    for (ordinal, kind) in [
        (1, "task_started"),
        (2, "turn_complete"),
        (3, "turn_aborted"),
    ] {
        serde_json::to_writer(
            &mut bytes,
            &json!({"timestamp":"2026-08-20T00:00:00Z",
            "ordinal":ordinal,"type":"event_msg","payload":{"type":kind,"turn_id":"turn",
            "reason":"interrupted"}}),
        )
        .expect("event");
        bytes.push(b'\n');
    }
    // An escaped JSON key is conservatively parsed rather than silently skipped.
    bytes.extend_from_slice(br#"{"timestamp":"2026-08-20T00:00:00Z","ordinal":4,"type":"event_msg","payload":{"\u0074ype":"turn_complete","turn_id":"turn"}}"#);
    bytes.push(b'\n');
    bytes.extend_from_slice(br#"{"timestamp":"2026-08-20T00:00:01.234Z","ordinal":5,"type":"realtime_item","payload":{"id":"voice:started","realtime_session_id":"voice","type":"realtime_session_started"}}"#);
    bytes.push(b'\n');
    bytes.extend_from_slice(br#"{"timestamp":"2026-08-20T00:00:02.345Z","ordinal":6,"\u0074ype":"realtime_\u0069tem","payload":{"id":"voice:artifact","realtime_session_id":"voice","type":"bem_item_promoted","turn_id":"turn","item_id":"artifact","presentation":{"type":"inline_visualization","index":3}}}"#);
    bytes.push(b'\n');
    let nonempty = |record: &[u8]| {
        let line = project_canonical_record(record).expect("canonical projection");
        (!line.changes.is_empty() || line.realtime_item.is_some()).then_some((
            line.timestamp.into_owned(),
            line.ordinal,
            line.changes,
            line.realtime_item,
        ))
    };
    let expected = bytes
        .split_inclusive(|byte| *byte == b'\n')
        .filter_map(nonempty)
        .collect::<Vec<_>>();
    let mut reader = bytes.as_slice();
    let mut chunk = Vec::new();
    let mut actual = Vec::new();
    let mut copied_bytes = 0;
    let mut newlines = 0;
    while read_canonical_chunk(&mut reader, &mut chunk)
        .await
        .expect("read chunk")
    {
        for span in candidate_spans(&chunk).expect("candidate regex") {
            newlines += span.newline_count;
            match span.kind {
                JsonlSpanKind::Copy => copied_bytes += span.range.len(),
                JsonlSpanKind::Candidate => actual.extend(
                    chunk[span.range]
                        .split_inclusive(|byte| *byte == b'\n')
                        .filter_map(nonempty),
                ),
            }
        }
    }
    assert_eq!(actual, expected);
    assert_eq!(newlines, 7);
    assert!(copied_bytes > 500_000);
}
