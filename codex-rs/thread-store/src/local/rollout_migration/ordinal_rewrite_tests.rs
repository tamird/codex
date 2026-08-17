use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use pretty_assertions::assert_eq;

use super::OrdinalRecord;

#[tokio::test]
async fn regex_ordinal_edit_matches_the_full_envelope_reader() {
    let item: RolloutItem = serde_json::from_value(serde_json::json!({
        "type":"response_item","payload":{"type":"function_call_output",
        "call_id":"call", "output":"large output\n".repeat(100_000)}
    }))
    .expect("response item");
    for timestamp in ["2026-08-20T00:00:00Z", "quoted\"\\timestamp"] {
        let bytes = serde_json::to_vec(&RolloutLine {
            timestamp: timestamp.to_string(),
            ordinal: Some(999),
            item: item.clone(),
        })
        .expect("canonical record");
        let fast = OrdinalRecord::from_canonical(&bytes).expect("canonical header");
        let reference: OrdinalRecord<'_> = serde_json::from_slice(&bytes).expect("full envelope");
        assert_eq!(fast.ordinal().expect("ordinal"), 999);
        for ordinal in [0, 999, 1000, u64::MAX] {
            let mut actual = Vec::new();
            let mut expected = Vec::new();
            fast.write_with_ordinal(&bytes, ordinal, &mut actual)
                .await
                .expect("regex edit");
            reference
                .write_with_ordinal(&bytes, ordinal, &mut expected)
                .await
                .expect("reference edit");
            assert_eq!(actual, expected);
            let mut value: serde_json::Value =
                serde_json::from_slice(&bytes).expect("original JSON");
            value["ordinal"] = ordinal.into();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&actual).expect("rewritten JSON"),
                value
            );
        }
    }
    assert!(
        OrdinalRecord::from_canonical(
            br#"{"ordinal":1,"timestamp":"now","type":"event_msg","payload":{}}"#
        )
        .is_none()
    );
}
