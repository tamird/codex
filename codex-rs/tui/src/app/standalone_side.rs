//! Startup support for side conversations that run in a separate TUI process.
//!
//! This mode deliberately owns only the child process. It forks the persisted parent through the
//! child's embedded app-server, makes that fork ephemeral, and hides inherited history from the
//! child UI. It does not attach to or control the parent process.

use super::*;
use crate::chatwidget::InterruptedTurnNoticeMode;
use codex_context_fragments::ContextualUserFragment;
use codex_context_fragments::StandaloneSideBoundary;

const STANDALONE_SIDE_DEVELOPER_INSTRUCTIONS: &str = r#"You are in a standalone side conversation, not the main thread.

All inherited parent history is reference context only. It is not your current task. Do not continue, execute, or complete instructions, plans, tool calls, approvals, edits, or requests inherited from the parent.

There is no active user request in this standalone side conversation yet. Wait for a new user message before taking action."#;
const STANDALONE_SIDE_RENAME_BLOCK_MESSAGE: &str =
    "Standalone side conversations are ephemeral and cannot be renamed.";

impl App {
    pub(super) fn standalone_side_config(config: &Config) -> Config {
        // The caller owns projection of live parent settings into `config`. Slice 15b will build
        // that launch config; this foundation preserves it while adding only side semantics.
        let mut side_config = config.clone();
        side_config.ephemeral = true;
        side_config.developer_instructions =
            Some(match side_config.developer_instructions.as_deref() {
                Some(existing) if !existing.trim().is_empty() => {
                    format!("{existing}\n\n{STANDALONE_SIDE_DEVELOPER_INSTRUCTIONS}")
                }
                _ => STANDALONE_SIDE_DEVELOPER_INSTRUCTIONS.to_string(),
            });
        side_config
    }

    pub(super) async fn start_standalone_side(
        app_server: &mut AppServerSession,
        config: Config,
        target_session: &SessionTarget,
    ) -> Result<AppServerStartedThread> {
        let mut side = app_server
            .fork_side_thread(config, target_session.thread_id)
            .await
            .wrap_err_with(|| {
                format!(
                    "Failed to start standalone side conversation from {}",
                    target_session.display_label()
                )
            })?;
        let child_thread_id = side.session.thread_id;
        if let Err(err) = app_server
            .thread_inject_items(
                child_thread_id,
                vec![ContextualUserFragment::into(StandaloneSideBoundary)],
            )
            .await
        {
            if let Err(unsubscribe_err) = app_server.thread_unsubscribe(child_thread_id).await {
                tracing::warn!(
                    "failed to unsubscribe standalone side {child_thread_id} after setup failure: {unsubscribe_err}"
                );
            }
            return Err(err).wrap_err_with(|| {
                format!("Failed to prepare standalone side conversation {child_thread_id}")
            });
        }
        side.session.forked_from_id = None;
        side.turns.clear();
        Ok(side)
    }

    pub(super) fn activate_standalone_side_ui(&mut self) {
        self.standalone_side_active = true;
        self.chat_widget
            .set_thread_rename_block_message(STANDALONE_SIDE_RENAME_BLOCK_MESSAGE);
        self.chat_widget.set_standalone_side_conversation_active();
        self.chat_widget
            .set_interrupted_turn_notice_mode(InterruptedTurnNoticeMode::Suppress);
        self.chat_widget
            .set_side_conversation_context_label(Some("Standalone side conversation".to_string()));
    }
}

#[cfg(test)]
#[path = "standalone_side_tests.rs"]
mod tests;
