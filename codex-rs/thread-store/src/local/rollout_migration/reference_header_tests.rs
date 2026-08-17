//! Tests the minimum physical rewrite needed to replace a native-compatible reference.

use std::fs;

use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use pretty_assertions::assert_eq;
use serde::Deserialize;
use serde_json::value::RawValue;

use super::ordinal_rewrite::OrdinalRecord;
use super::tests::completed_user_message;
use super::tests::indexed_store;
use super::tests::list_active_summary_turns;
use super::tests::segment_reference;
use super::tests::turn_complete;
use super::tests::turn_started;
use super::tests::write_paginated_segment;

#[tokio::test]
async fn paginated_reference_conversion_preserves_payload_bytes() {
    let home = tempfile::tempdir().expect("Codex home");
    let parent_id = ThreadId::new();
    let child_id = ThreadId::new();
    let parent_segment = SegmentId::new();
    let parent = home
        .path()
        .join("sessions/2025/01/03")
        .join(format!("rollout-2025-01-03T12-00-00-{parent_id}.jsonl"));
    let parent_end = write_paginated_segment(
        &parent,
        home.path(),
        parent_id,
        parent_segment,
        /*start_ordinal*/ 0,
        vec![
            turn_started("parent"),
            completed_user_message(parent_id, "parent", "parent-item", "parent message"),
            turn_complete("parent"),
        ],
    );
    let child = home
        .path()
        .join("sessions/2025/01/03")
        .join(format!("rollout-2025-01-03T12-00-01-{child_id}.jsonl"));
    let model_output = "retained model output".repeat(4096);
    write_paginated_segment(
        &child,
        home.path(),
        child_id,
        SegmentId::new(),
        parent_end,
        vec![
            segment_reference(parent.clone(), parent_id, parent_segment),
            turn_started("child"),
            completed_user_message(child_id, "child", "child-item", "child message"),
            serde_json::from_value(serde_json::json!({"type":"response_item", "payload":{
                "type":"function_call_output", "call_id":"call", "output":model_output
            }}))
            .expect("model-history record"),
            turn_complete("child"),
        ],
    );
    let parent_bytes = fs::read(&parent).expect("parent bytes");
    let original = fs::read(&child).expect("original child");
    let mut records = original.splitn(3, |byte| *byte == b'\n');
    let mut head: RolloutLine =
        serde_json::from_slice(records.next().expect("head")).expect("session metadata");
    let reference: RolloutLine =
        serde_json::from_slice(records.next().expect("reference")).expect("reference record");
    assert!(matches!(reference.item, RolloutItem::RolloutReference(_)));
    let suffix = records.next().expect("unchanged suffix");
    let RolloutItem::SessionMeta(metadata) = &mut head.item else {
        panic!("metadata")
    };
    metadata.meta.history_base = Some(HistoryPosition {
        thread_id: parent_id,
        end_ordinal_exclusive: parent_end,
        end_byte_offset: fs::metadata(&parent).expect("parent metadata").len(),
    });
    let mut replacement = serde_json::to_vec(&head).expect("new header");
    replacement.push(b'\n');
    // Removing a reference removes one ordinal. The payload bytes need no conversion.
    #[derive(Deserialize)]
    struct Ordinal<'a> {
        #[serde(borrow)]
        payload: &'a RawValue,
    }
    for line in suffix.split_inclusive(|byte| *byte == b'\n') {
        let line = line.strip_suffix(b"\n").unwrap_or(line);
        let envelope: OrdinalRecord<'_> = serde_json::from_slice(line).expect("ordinal envelope");
        envelope
            .write_with_ordinal(
                line,
                envelope.ordinal().expect("ordinal") - 1,
                &mut replacement,
            )
            .await
            .expect("rewrite ordinal only");
    }
    fs::write(&child, &replacement).expect("replace test header");
    let rewritten_suffix = replacement
        .splitn(2, |byte| *byte == b'\n')
        .nth(1)
        .expect("rewritten suffix");
    let payloads = |bytes: &[u8]| {
        bytes
            .split_inclusive(|byte| *byte == b'\n')
            .map(|line| {
                serde_json::from_slice::<Ordinal<'_>>(line)
                    .expect("record envelope")
                    .payload
                    .get()
                    .to_string()
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(payloads(suffix), payloads(rewritten_suffix));
    let store = indexed_store(home.path()).await;
    assert!(
        store
            .rebuild_history_projection(child_id)
            .await
            .expect("header and ordinal projection")
    );
    let turns = list_active_summary_turns(&store, child_id).await;
    assert_eq!(
        turns
            .turns
            .iter()
            .map(|turn| turn.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec!["parent", "child"]
    );
    assert_eq!(fs::read(&parent).expect("unchanged parent"), parent_bytes);
    let materialized = codex_rollout::materialize_rollout_lines(home.path(), &child)
        .await
        .expect("model history");
    assert!(
        serde_json::to_string(&materialized)
            .expect("materialized JSON")
            .contains(&model_output)
    );
}
