use codex_protocol::protocol::EventMsg;
use codex_rollout::RolloutItem;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::parse_legacy_rollout_line;
use super::parse_paginated_rollout_line;

fn assert_decoder_equivalence(value: serde_json::Value) -> bool {
    let old = serde_json::from_value::<codex_rollout::RolloutLine>(value.clone());
    let new = codex_rollout::decode_rollout_line(value);
    match (old, new) {
        (Ok(old), Ok(new)) => {
            let old = serde_json::to_vec(&old).expect("serialize old decoded record");
            let new = serde_json::to_vec(&new).expect("serialize new decoded record");
            if old == new {
                return true;
            }
            // Some protocol payloads contain HashMaps. Independent decodes can serialize those
            // keys in different orders even when both use the same decoder.
            assert!(
                serde_json::from_slice::<serde_json::Value>(&old).expect("old JSON")
                    == serde_json::from_slice::<serde_json::Value>(&new).expect("new JSON"),
                "decoded rollout contents differ"
            );
            false
        }
        (Err(_), Err(_)) => true,
        (old, new) => panic!(
            "decoder acceptance differs: old error {:?}, new error {:?}",
            old.err(),
            new.err()
        ),
    }
}

#[test]
fn value_decoders_preserve_acceptance_and_canonical_bytes() {
    for encoded in [
        r#"{"timestamp":"old","timestamp":"new","ordinal":null,"type":"event_msg","payload":{"type":"token_count","info":null,"rate_limits":{"primary":{"used_percent":0.1234567890123456789,"window_minutes":300,"resets_at":1800000000}}}}"#,
        r#"{"payload":{"type":"message","role":"developer","content":[{"type":"input_text","text":"hello"}]},"metadata":{"client_authored":true},"type":"response_item","ordinal":18446744073709551615,"timestamp":"now"}"#,
        r#"{"timestamp":"now","ordinal":3,"type":"event_msg","metadata":"ignored","payload":{"type":"warning","message":"old","message":"new"}}"#,
        r#"{"timestamp":"now","ordinal":-1,"type":"event_msg","payload":{"type":"warning","message":"bad ordinal"}}"#,
        r#"{"timestamp":"now","type":"event_msg","payload":{"type":"unknown"}}"#,
    ] {
        let value = serde_json::from_str(encoded).expect("parse fixture JSON");
        assert_decoder_equivalence(value);
    }
}

/// Optional differential admission over private incident files without checking them into Git.
#[tokio::test]
#[ignore = "set CODEX_ROLLOUT_DECODER_CORPUS to a directory of copied rollouts"]
async fn value_decoders_match_supplied_rollout_corpus() {
    let mut directories = vec![std::path::PathBuf::from(
        std::env::var_os("CODEX_ROLLOUT_DECODER_CORPUS").expect("rollout corpus directory"),
    )];
    let mut records = 0_u64;
    let mut reordered = 0_u64;
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(directory).expect("read corpus directory") {
            let entry = entry.expect("read corpus entry");
            let path = entry.path();
            if entry.file_type().expect("read corpus file type").is_dir() {
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
                .expect("open corpus rollout");
            while let Some(raw) = reader.next_line().await.expect("read corpus record") {
                let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
                    continue;
                };
                reordered += u64::from(!assert_decoder_equivalence(value.clone()));
                if let Ok(Some(normalized)) = super::normalize_legacy_rollout_value(value) {
                    reordered += u64::from(!assert_decoder_equivalence(normalized));
                }
                records += 1;
            }
        }
    }
    assert!(records > 0, "corpus contains no rollout records");
    eprintln!(
        "compared {records} raw and normalized rollout records; {reordered} object-key reorderings"
    );
}

fn line(payload_type: &str, payload: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "timestamp": "2025-01-03T12:00:00Z",
        "type": payload_type,
        "payload": payload,
    }))
    .expect("serialize fixture")
}

#[test]
fn parses_legacy_numeric_event_payloads_through_value() {
    let bytes = line(
        "event_msg",
        json!({
            "type": "token_count",
            "info": null,
            "rate_limits": {
                "primary": {
                    "used_percent": 2,
                    "window_minutes": 300,
                    "resets_at": 1_770_414_841,
                },
                "secondary": {
                    "used_percent": 12,
                    "window_minutes": 10_080,
                    "resets_at": 1_770_698_702,
                },
                "credits": {
                    "has_credits": false,
                    "unlimited": false,
                    "balance": null,
                },
                "plan_type": null,
            },
        }),
    );

    let parsed = parse_legacy_rollout_line(&bytes)
        .expect("parse legacy token count")
        .expect("keep legacy token count");
    assert!(matches!(
        parsed.item,
        RolloutItem::EventMsg(EventMsg::TokenCount(_))
    ));
}

#[test]
fn parses_paginated_numeric_event_payloads_through_value() {
    let bytes = serde_json::to_vec(&json!({
        "timestamp": "2026-08-18T21:03:49.690Z",
        "ordinal": 11938,
        "type": "event_msg",
        "payload": {
            "type": "token_count",
            "info": {
                "total_token_usage": {
                    "input_tokens": 319590193,
                    "cached_input_tokens": 312717039,
                    "cache_write_input_tokens": 6711808,
                    "output_tokens": 364803,
                    "reasoning_output_tokens": 57778,
                    "total_tokens": 319954996
                },
                "last_token_usage": {
                    "input_tokens": 203881,
                    "cached_input_tokens": 0,
                    "cache_write_input_tokens": 203740,
                    "output_tokens": 280,
                    "reasoning_output_tokens": 184,
                    "total_tokens": 204161
                },
                "model_context_window": 258400
            },
            "rate_limits": {
                "limit_id": "codex",
                "limit_name": null,
                "primary": {
                    "used_percent": 0.0,
                    "window_minutes": 1,
                    "resets_at": 1787087041
                },
                "secondary": {
                    "used_percent": 0.0,
                    "window_minutes": 300,
                    "resets_at": 1787102386
                },
                "credits": {
                    "has_credits": true,
                    "unlimited": true,
                    "balance": null
                },
                "individual_limit": null,
                "spend_control_reached": null,
                "plan_type": "business",
                "rate_limit_reached_type": null
            }
        }
    }))
    .expect("serialize Paginated token count");

    let parsed = parse_paginated_rollout_line(&bytes).expect("parse Paginated token count");
    assert_eq!(
        serde_json::to_value(
            serde_json::from_slice::<codex_rollout::RolloutLine>(&bytes)
                .expect("manual rollout decoder accepts numeric token count"),
        )
        .expect("serialize direct decode"),
        serde_json::to_value(&parsed).expect("serialize canonical decode")
    );
    assert_eq!(parsed.ordinal, Some(11938));
    assert!(matches!(
        parsed.item,
        RolloutItem::EventMsg(EventMsg::TokenCount(_))
    ));
}

#[test]
fn normalizes_legacy_rate_limit_reset_timestamps() {
    let bytes = line(
        "event_msg",
        json!({
            "type": "token_count",
            "info": null,
            "rate_limits": {
                "primary": {
                    "used_percent": 2,
                    "window_minutes": 300,
                    "resets_at": "2025-10-19T08:51:37.876641+00:00",
                },
                "secondary": null,
                "credits": null,
                "plan_type": null,
            },
        }),
    );

    let parsed = parse_legacy_rollout_line(&bytes)
        .expect("parse legacy reset timestamp")
        .expect("keep legacy token count");
    assert!(matches!(
        parsed.item,
        RolloutItem::EventMsg(EventMsg::TokenCount(_))
    ));
}

#[test]
fn normalizes_legacy_turn_context_collaboration_mode() {
    let cwd = std::env::temp_dir().to_string_lossy().into_owned();
    let bytes = line(
        "turn_context",
        json!({
            "cwd": cwd,
            "approval_policy": "never",
            "sandbox_policy": {"type": "danger-full-access"},
            "model": "gpt-test",
            "personality": null,
            "collaboration_mode": {
                "mode": "plan",
                "model": "gpt-test",
                "reasoning_effort": null,
                "developer_instructions": null,
            },
            "effort": null,
            "summary": "auto",
        }),
    );

    let parsed = parse_legacy_rollout_line(&bytes)
        .expect("parse legacy turn context")
        .expect("keep legacy turn context");
    let RolloutItem::TurnContext(context) = parsed.item else {
        panic!("expected turn context");
    };
    assert_eq!(
        context
            .collaboration_mode
            .expect("collaboration mode")
            .model(),
        "gpt-test"
    );
}

#[test]
fn normalizes_legacy_turn_context_sandbox_policy() {
    let cwd = std::env::temp_dir().to_string_lossy().into_owned();
    let bytes = line(
        "turn_context",
        json!({
            "cwd": cwd,
            "approval_policy": "never",
            "sandbox_policy": {"mode": "danger-full-access"},
            "model": "gpt-test",
            "personality": null,
            "effort": null,
            "summary": "auto",
        }),
    );

    let parsed = parse_legacy_rollout_line(&bytes)
        .expect("parse legacy sandbox policy")
        .expect("keep legacy turn context");
    assert!(matches!(parsed.item, RolloutItem::TurnContext(_)));
}

#[test]
fn normalizes_legacy_review_entry_prompt() {
    let bytes = line(
        "event_msg",
        json!({
            "type": "entered_review_mode",
            "prompt": "review these changes",
            "user_facing_hint": "Review requested.",
        }),
    );

    let parsed = parse_legacy_rollout_line(&bytes)
        .expect("parse legacy review entry")
        .expect("keep legacy review entry");
    assert!(matches!(
        parsed.item,
        RolloutItem::EventMsg(EventMsg::EnteredReviewMode(_))
    ));
}

#[test]
fn normalizes_legacy_plain_command_cwd() {
    let cwd = std::env::temp_dir().to_string_lossy().into_owned();
    let bytes = line(
        "event_msg",
        json!({
            "type": "exec_command_end",
            "call_id": "call-1",
            "turn_id": "turn-1",
            "command": ["echo", "ok"],
            "cwd": cwd,
            "parsed_cmd": [],
            "source": "agent",
            "stdout": "",
            "stderr": "",
            "aggregated_output": "",
            "exit_code": 0,
            "duration": {"secs": 0, "nanos": 0},
            "formatted_output": "",
            "status": "completed",
        }),
    );

    let parsed = parse_legacy_rollout_line(&bytes)
        .expect("parse legacy command")
        .expect("keep legacy command");
    assert!(matches!(
        parsed.item,
        RolloutItem::EventMsg(EventMsg::ExecCommandEnd(_))
    ));
}

#[test]
fn skips_only_known_retired_events() {
    for event_type in [
        "guardian_assessment",
        "thread_name_updated",
        "undo_completed",
    ] {
        let bytes = line("event_msg", json!({"type": event_type}));
        assert!(
            parse_legacy_rollout_line(&bytes)
                .expect("inspect retired event")
                .is_none()
        );
    }

    let unknown = line("event_msg", json!({"type": "unknown_legacy_event"}));
    assert!(parse_legacy_rollout_line(&unknown).is_err());
}

#[test]
fn skips_legacy_ghost_snapshots() {
    let ghost_snapshot = line(
        "response_item",
        json!({
            "type": "ghost_snapshot",
            "ghost_commit": {"id": "legacy"},
        }),
    );

    assert!(
        parse_legacy_rollout_line(&ghost_snapshot)
            .expect("inspect ghost snapshot")
            .is_none()
    );
}
