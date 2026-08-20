//! The maintenance footer belongs to the app, not a model turn or transcript item.

use super::ChatWidget;

impl ChatWidget {
    pub(crate) fn rollout_maintenance(&self) -> Option<&str> {
        self.bottom_pane.rollout_maintenance()
    }

    pub(crate) fn set_rollout_maintenance(&mut self, status: Option<String>) {
        self.bottom_pane.set_rollout_maintenance(status);
    }
}
