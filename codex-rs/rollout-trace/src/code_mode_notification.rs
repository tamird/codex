//! Trace-only provenance for model-visible code-mode notifications.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

/// Trusted harness origin of an `exec` notification, before any prompt projection.
///
/// This must come from persisted harness metadata, never from message text or
/// provider-controlled Responses metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeModeNotificationOrigin {
    pub source_item_id: String,
    pub call_id: String,
    pub cell_id: String,
}

/// Notification origins keyed by the ID of the item actually sent to the model.
pub type CodeModeNotificationOrigins = BTreeMap<String, CodeModeNotificationOrigin>;

pub(crate) const INFERENCE_NOTIFICATIONS_FIELD: &str = "_codex_code_mode_notifications";

/// Adds provenance only to the trace copy, and only for items in this request.
pub(crate) fn trace_request_with_notifications(
    request: &impl Serialize,
    origins: &CodeModeNotificationOrigins,
) -> serde_json::Result<Value> {
    let mut request = serde_json::to_value(request)?;
    let present = request
        .get("input")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| {
            let id = item.get("id")?.as_str()?;
            origins.get(id).map(|origin| (id, origin))
        })
        .collect::<BTreeMap<_, _>>();
    let sidecar = (!present.is_empty())
        .then(|| serde_json::to_value(present))
        .transpose()?;
    if let Some(object) = request.as_object_mut() {
        object.remove(INFERENCE_NOTIFICATIONS_FIELD);
        if let Some(sidecar) = sidecar {
            object.insert(INFERENCE_NOTIFICATIONS_FIELD.to_string(), sidecar);
        }
    }
    Ok(request)
}
