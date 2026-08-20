use codex_history::ResponseItemEnvelope;
use codex_rollout_trace::CodeModeNotificationOrigin;
use codex_rollout_trace::CodeModeNotificationOrigins;

/// Keeps trusted notification attribution outside the Responses API payload.
pub(crate) fn code_mode_notification_origins(
    items: &[ResponseItemEnvelope],
) -> CodeModeNotificationOrigins {
    items
        .iter()
        .filter_map(|envelope| {
            let origin = envelope
                .metadata
                .as_ref()?
                .code_mode_notification
                .as_ref()?;
            let item_id = envelope.item.id()?;
            Some((
                item_id.to_string(),
                CodeModeNotificationOrigin {
                    source_item_id: origin.source_item_id.to_string(),
                    call_id: origin.call_id.clone(),
                    cell_id: origin.cell_id.clone(),
                },
            ))
        })
        .collect()
}
