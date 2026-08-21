//! Pane placement for blank standalone side conversations.

use super::side::SIDE_NO_STARTED_CONVERSATION_MESSAGE;
use super::*;
use crate::app_event::ForkPanePlacement;
use crate::ghostty_fork::ghostty_placement;
use crate::ghostty_fork::spawn_standalone_side_in_ghostty_split;
use crate::terminal_multiplexer::ForkPaneSpawnResult;
use crate::terminal_multiplexer::spawn_standalone_side_in_new_pane;
use codex_terminal_detection::TerminalInfo;

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
        app_server: &mut AppServerSession,
        parent_thread_id: ThreadId,
        placement: ForkPanePlacement,
        terminal_info: &TerminalInfo,
    ) -> Result<AppRunControl> {
        if self.app_server_target.uses_remote_workspace()
            || (terminal_info.multiplexer.is_none()
                && ghostty_placement(terminal_info, Some(placement)).is_none())
        {
            let message = if self.app_server_target.uses_remote_workspace() {
                REMOTE_SIDE_PANE_UNAVAILABLE_MESSAGE
            } else {
                SIDE_PLACEMENT_REQUIRES_PANE_HOST_MESSAGE
            };
            return self
                .handle_side_pane_result(
                    tui,
                    app_server,
                    parent_thread_id,
                    ForkPaneSpawnResult::InvalidPlacement(message.to_string()),
                )
                .await;
        }

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
        let handoff = match app_server
            .prepare_fork_handoff(
                Self::standalone_side_config(&launch_config),
                parent_thread_id,
            )
            .await
        {
            Ok(path) => path,
            Err(error) => {
                return self
                    .handle_side_pane_result(
                        tui,
                        app_server,
                        parent_thread_id,
                        ForkPaneSpawnResult::Failed(format!("{error:#}")),
                    )
                    .await;
            }
        };
        let result = if let Some(multiplexer) = terminal_info.multiplexer.as_ref() {
            spawn_standalone_side_in_new_pane(
                multiplexer,
                &parent_thread_id,
                &launch_config,
                &self.harness_overrides.additional_writable_roots,
                placement,
                &handoff,
            )
            .await
        } else if let Some(placement) = ghostty_placement(terminal_info, Some(placement)) {
            spawn_standalone_side_in_ghostty_split(
                &parent_thread_id,
                &launch_config,
                &self.harness_overrides.additional_writable_roots,
                placement,
                &handoff,
            )
            .await
        } else {
            ForkPaneSpawnResult::InvalidPlacement(
                SIDE_PLACEMENT_REQUIRES_PANE_HOST_MESSAGE.to_string(),
            )
        };

        self.handle_side_pane_result(tui, app_server, parent_thread_id, result)
            .await
    }

    async fn handle_side_pane_result(
        &mut self,
        tui: &mut tui::Tui,
        app_server: &mut AppServerSession,
        parent_thread_id: ThreadId,
        result: ForkPaneSpawnResult,
    ) -> Result<AppRunControl> {
        match result {
            ForkPaneSpawnResult::Spawned => {}
            ForkPaneSpawnResult::InvalidPlacement(err) | ForkPaneSpawnResult::Failed(err) => {
                tracing::debug!(%err, "side pane unavailable; using the current TUI");
                self.chat_widget
                    .add_info_message(placed_side_spawn_failure_message(&err), /*hint*/ None);
                // Pane placement is optional; keep the existing ephemeral side lifecycle and
                // parent-return behavior instead of making terminal support a prerequisite.
                return self
                    .handle_start_side(
                        tui,
                        app_server,
                        parent_thread_id,
                        /*user_message*/ None,
                    )
                    .await;
            }
        }
        tui.frame_requester().schedule_frame();
        Ok(AppRunControl::Continue)
    }
}

fn placed_side_spawn_failure_message(err: &str) -> String {
    format!("Could not open a side pane: {err} Opening /side in this terminal instead.")
}

#[cfg(test)]
#[path = "placed_side_tests.rs"]
mod tests;
