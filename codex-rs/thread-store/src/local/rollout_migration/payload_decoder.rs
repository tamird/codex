//! Decodes common rollout payloads without buffering the entire outer tagged enum.
//!
//! The existing decoder remains authoritative for numbers that Serde's generic enum buffer can
//! represent differently. In particular, bypassing that buffer can newly accept an oversized
//! integer in an otherwise ignored field and change the records included in a migration.

use codex_rollout::ResponseItemEnvelope;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use serde::de::Error as _;
use serde_json::Value;

pub(super) fn decode(value: Value) -> serde_json::Result<RolloutLine> {
    if !matches!(
        value.get("type").and_then(Value::as_str),
        Some("response_item" | "turn_context" | "compacted" | "event_msg")
    ) || !has_only_content_safe_numbers(&value)
    {
        return codex_rollout::decode_rollout_line(value);
    }
    let Value::Object(mut fields) = value else {
        return codex_rollout::decode_rollout_line(value);
    };
    let kind = fields
        .remove("type")
        .ok_or_else(|| serde_json::Error::missing_field("type"))?;
    let timestamp = serde_json::from_value(
        fields
            .remove("timestamp")
            .ok_or_else(|| serde_json::Error::missing_field("timestamp"))?,
    )?;
    let ordinal = fields
        .remove("ordinal")
        .map(serde_json::from_value::<Option<u64>>)
        .transpose()?
        .flatten();
    let payload = fields
        .remove("payload")
        .ok_or_else(|| serde_json::Error::missing_field("payload"))?;
    let item = match kind.as_str() {
        Some("response_item") => RolloutItem::ResponseItem(ResponseItemEnvelope {
            item: serde_json::from_value(payload)?,
            metadata: fields
                .remove("metadata")
                .map(serde_json::from_value)
                .transpose()?
                .flatten(),
        }),
        Some("turn_context") => RolloutItem::TurnContext(serde_json::from_value(payload)?),
        Some("compacted") => RolloutItem::Compacted(serde_json::from_value(payload)?),
        Some("event_msg") => RolloutItem::EventMsg(serde_json::from_value(payload)?),
        _ => return Err(serde_json::Error::custom("unexpected rollout payload type")),
    };
    Ok(RolloutLine {
        timestamp,
        ordinal,
        item,
    })
}

/// Signed and unsigned 64-bit integers survive Serde's Content buffer without conversion.
/// Floating-point lexemes and wider integers keep the old decoder, including ignored fields.
fn has_only_content_safe_numbers(value: &Value) -> bool {
    match value {
        Value::Number(number) => number.is_i64() || number.is_u64(),
        Value::Array(values) => values.iter().all(has_only_content_safe_numbers),
        Value::Object(fields) => fields.values().all(has_only_content_safe_numbers),
        Value::Null | Value::Bool(_) | Value::String(_) => true,
    }
}

#[cfg(test)]
#[path = "payload_decoder_tests.rs"]
mod tests;
