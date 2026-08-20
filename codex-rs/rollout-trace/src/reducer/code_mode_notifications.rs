//! Trusted code-mode output identity, separate from Responses call identity.

use anyhow::Result;
use anyhow::bail;

use super::TraceReducer;
use crate::code_mode_notification::CodeModeNotificationOrigin;

impl TraceReducer {
    pub(super) fn notification_identity_matches(
        &self,
        item_id: &str,
        origin: Option<&CodeModeNotificationOrigin>,
    ) -> bool {
        self.code_mode_notifications.get(item_id) == origin
    }

    /// Notifications can share a call ID with its ordinary result, and compaction
    /// can truncate a previously observed notification without changing its origin.
    pub(super) fn distinct_notification_output(
        &self,
        item_id: &str,
        origin: Option<&CodeModeNotificationOrigin>,
    ) -> bool {
        self.code_mode_notifications.contains_key(item_id) || origin.is_some()
    }

    pub(super) fn attach_code_mode_notification_item(
        &mut self,
        item_id: &str,
        origin: &CodeModeNotificationOrigin,
    ) -> Result<()> {
        let code_cell_id = self.reduced_code_cell_id_for_model_visible_call(&origin.call_id);
        let Some(cell) = self.rollout.code_cells.get(&code_cell_id) else {
            // A queued cell start will attach these items when its source appears.
            return Ok(());
        };
        let Some(item) = self.rollout.conversation_items.get(item_id) else {
            bail!("notification referenced missing conversation item {item_id}");
        };
        if cell.thread_id != item.thread_id
            || cell.runtime_cell_id.as_deref() != Some(origin.cell_id.as_str())
        {
            bail!("notification {item_id} does not belong to code cell {code_cell_id}");
        }
        self.add_code_cell_output_item(&code_cell_id, item_id)
    }

    pub(super) fn attach_existing_code_mode_notifications(
        &mut self,
        thread_id: &str,
        call_id: &str,
    ) -> Result<()> {
        let notifications = self
            .code_mode_notifications
            .iter()
            .filter(|(item_id, origin)| {
                origin.call_id == call_id
                    && self
                        .rollout
                        .conversation_items
                        .get(*item_id)
                        .is_some_and(|item| item.thread_id == thread_id)
            })
            .map(|(item_id, origin)| (item_id.clone(), origin.clone()))
            .collect::<Vec<_>>();
        for (item_id, origin) in notifications {
            self.attach_code_mode_notification_item(&item_id, &origin)?;
        }
        Ok(())
    }
}
