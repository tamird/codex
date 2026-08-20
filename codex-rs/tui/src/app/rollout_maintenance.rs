//! Repaints maintenance progress during the existing synchronous thread-switch waits.

use super::App;
use crate::app_server_session::RolloutMaintenanceState;
use crate::tui::Tui;
use color_eyre::eyre::Result;
use std::future::Future;
use tokio::sync::watch;

impl App {
    pub(super) async fn wait_with_rollout_maintenance<F>(
        &mut self,
        tui: &mut Tui,
        mut status: watch::Receiver<RolloutMaintenanceState>,
        future: F,
    ) -> Result<F::Output>
    where
        F: Future,
    {
        tokio::pin!(future);
        let result = loop {
            let text = status.borrow_and_update().display_text();
            self.render_rollout_maintenance(tui, text)?;
            tokio::select! {
                result = &mut future => break result,
                changed = status.changed() => {
                    if changed.is_err() {
                        break future.await;
                    }
                }
            }
        };
        self.render_rollout_maintenance(tui, status.borrow().background_text())?;
        Ok(result)
    }

    fn render_rollout_maintenance(&mut self, tui: &mut Tui, text: Option<String>) -> Result<()> {
        if self.chat_widget.rollout_maintenance() != text.as_deref() {
            self.chat_widget.set_rollout_maintenance(text);
            let size = tui.terminal.last_known_screen_size;
            self.handle_draw_pre_render(tui, size)?;
            self.chat_widget.pre_draw_tick();
            self.render_chat_widget_frame(tui, size)?;
        }
        Ok(())
    }
}
