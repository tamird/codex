//! Pane placement for blank standalone side conversations.

use super::side::SIDE_NO_STARTED_CONVERSATION_MESSAGE;
use super::*;
use crate::app_event::ForkPanePlacement;
use crate::ghostty_fork::ghostty_placement;
use crate::ghostty_fork::spawn_standalone_side_in_ghostty_split;
use crate::terminal_multiplexer::ForkPaneSpawnResult;
use crate::terminal_multiplexer::spawn_standalone_side_in_new_pane;

const REMOTE_SIDE_PANE_UNAVAILABLE_MESSAGE: &str =
    "Side pane placement is unavailable for remote app-server sessions.";
const SIDE_PLACEMENT_REQUIRES_PANE_HOST_MESSAGE: &str =
    "Side pane placement requires tmux, Zellij, or macOS Ghostty.";

impl App {
    fn placed_side_launch_config(&self) -> Config {
        let mut launch_config = self.chat_widget.config_ref().clone();
        let model = self.chat_widget.current_model();
        if !model.trim().is_empty() {
            launch_config.model = Some(model.to_string());
        }
        launch_config.model_reasoning_effort = self.chat_widget.current_reasoning_effort();
        launch_config.service_tier = self.chat_widget.configured_service_tier();
        launch_config
    }

    pub(super) async fn handle_start_placed_side(
        &mut self,
        tui: &mut tui::Tui,
        parent_thread_id: ThreadId,
        placement: ForkPanePlacement,
    ) -> Result<AppRunControl> {
        if self
            .chat_widget
            .rollout_path()
            .as_deref()
            .is_none_or(|path| !rollout_path_is_resumable(path))
        {
            self.chat_widget
                .add_error_message(SIDE_NO_STARTED_CONVERSATION_MESSAGE.to_string());
            tui.frame_requester().schedule_frame();
            return Ok(AppRunControl::Continue);
        }
        if self.app_server_target.uses_remote_workspace() {
            self.chat_widget
                .add_error_message(REMOTE_SIDE_PANE_UNAVAILABLE_MESSAGE.to_string());
            tui.frame_requester().schedule_frame();
            return Ok(AppRunControl::Continue);
        }

        self.session_telemetry.counter(
            "codex.thread.side",
            /*inc*/ 1,
            &[("source", "slash_command_pane")],
        );
        self.refresh_in_memory_config_from_disk_best_effort(
            "starting a standalone side conversation",
        )
        .await;

        let launch_config = self.placed_side_launch_config();
        let terminal_info = codex_terminal_detection::terminal_info();
        let result = if let Some(multiplexer) = terminal_info.multiplexer.as_ref() {
            spawn_standalone_side_in_new_pane(
                multiplexer,
                &parent_thread_id,
                &launch_config,
                &self.harness_overrides.additional_writable_roots,
                placement,
            )
            .await
        } else if let Some(placement) = ghostty_placement(&terminal_info, Some(placement)) {
            spawn_standalone_side_in_ghostty_split(
                &parent_thread_id,
                &launch_config,
                &self.harness_overrides.additional_writable_roots,
                placement,
            )
            .await
        } else {
            self.chat_widget
                .add_error_message(SIDE_PLACEMENT_REQUIRES_PANE_HOST_MESSAGE.to_string());
            tui.frame_requester().schedule_frame();
            return Ok(AppRunControl::Continue);
        };

        match result {
            ForkPaneSpawnResult::Spawned => {}
            ForkPaneSpawnResult::InvalidPlacement(message) => {
                self.chat_widget.add_error_message(message);
            }
            ForkPaneSpawnResult::Failed(err) => {
                self.chat_widget
                    .add_error_message(placed_side_spawn_failure_message(&err));
            }
        }
        tui.frame_requester().schedule_frame();
        Ok(AppRunControl::Continue)
    }
}

fn placed_side_spawn_failure_message(err: &str) -> String {
    format!("Failed to open a new pane for /side: {err}")
}

#[cfg(test)]
#[path = "placed_side_tests.rs"]
mod tests;
