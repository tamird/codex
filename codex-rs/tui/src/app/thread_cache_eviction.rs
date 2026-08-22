//! Reclaim cold transcript payloads without discarding thread ownership or pending input.
//!
//! Replay is already lossy under the per-thread event and delta limits. This adds a shared byte
//! budget for inactive, idle, persisted transcripts. Live turns, ephemeral side conversations,
//! pending submissions, interactive requests, and busy stores stay pinned. Hook/MCP status is not
//! rehydratable and stays outside the transcript budget, alongside composer and request state.
//! A cache miss reloads history through the existing paginated reader; completion does not guarantee
//! a successful flush, and replacing replay payloads is lossy like ordinary history refresh.

use super::thread_cache::json_bytes;
use super::*;
use crate::app_server_session::HistoryHydrationScope;
use codex_app_server_protocol::ThreadClosedNotification;
use codex_app_server_protocol::TurnCompletedNotification;
use std::cmp::Reverse;

pub(super) const INACTIVE_HISTORY_BUDGET: usize = 64 * 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum TerminalDelivery {
    PendingLive,
    Applied,
}

#[derive(Debug)]
pub(super) enum TerminalNotification {
    Completed {
        notification: Box<TurnCompletedNotification>,
        delivery: TerminalDelivery,
    },
    Closed(ThreadClosedNotification),
}

impl ThreadBufferedEvent {
    pub(super) fn is_transcript(&self) -> bool {
        match self {
            Self::Notification(notification) => !matches!(
                notification.as_ref(),
                ServerNotification::HookStarted(_)
                    | ServerNotification::HookCompleted(_)
                    | ServerNotification::McpServerStatusUpdated(_)
            ),
            // Composer recall responses and request bodies are not transcript history.
            Self::Request(_) | Self::HistoryEntryResponse(_) | Self::FeedbackSubmission(_) => false,
        }
    }
}

impl ThreadEventStore {
    pub(super) fn can_evict_history(&self) -> bool {
        !self.active
            && self.active_turn_id.is_none()
            && self
                .session
                .as_ref()
                .is_some_and(|session| session.rollout_path.is_some())
            && self.side_parent_pending_status().is_none()
            && !self
                .input_state
                .as_ref()
                .is_some_and(ThreadInputState::has_in_flight_input)
            && !matches!(
                self.terminal_notification.as_ref(),
                Some(TerminalNotification::Completed {
                    notification: _,
                    delivery: TerminalDelivery::PendingLive,
                })
            )
    }

    pub(super) fn history_payload_bytes(&self) -> usize {
        self.turn_payload_bytes
            .saturating_add(self.buffered_history_bytes)
    }

    pub(super) fn evict_history(&mut self) {
        // Do not use set_turns: an absent cache says nothing about live lifecycle or saved input.
        self.turns = Vec::new();
        self.turn_payload_bytes = 0;
        self.buffer.retain(|event| match event {
            ThreadBufferedEvent::Request(request) => self
                .pending_interactive_replay
                .should_replay_snapshot_request(request),
            ThreadBufferedEvent::Notification(_)
            | ThreadBufferedEvent::HistoryEntryResponse(_)
            | ThreadBufferedEvent::FeedbackSubmission(_) => !event.is_transcript(),
        });
        self.buffer.shrink_to_fit();
        self.buffered_payload_bytes = self
            .buffer
            .iter()
            .map(ThreadBufferedEvent::payload_bytes)
            .fold(/*init*/ 0, usize::saturating_add);
        self.buffered_history_bytes = 0;
        self.buffered_agent_message_delta_bytes = 0;
        self.history_reload_required = true;
    }

    pub(super) fn set_history_payload(&mut self, turns: Vec<Turn>) {
        self.merge_recap_progress(recap::RecapProgress::from_turns(&turns));
        self.turn_payload_bytes = turns
            .iter()
            .map(json_bytes)
            .fold(/*init*/ 0, usize::saturating_add);
        self.turns = turns;
        self.history_reload_required = false;
    }

    pub(super) fn terminal_replay_event(&self) -> Option<ThreadBufferedEvent> {
        let terminal = match self.terminal_notification.as_ref()? {
            TerminalNotification::Completed {
                notification: _,
                delivery: TerminalDelivery::PendingLive,
            } => {
                // The receiver still owns this completion; replay is not live delivery.
                return None;
            }
            TerminalNotification::Completed {
                notification,
                delivery: TerminalDelivery::Applied,
            } => {
                let mut history = self
                    .turns
                    .iter()
                    .skip_while(|turn| turn.id != notification.turn.id);
                if let Some(turn) = history.next()
                    && ((turn.status == notification.turn.status
                        // Matching status alone does not restore a policy stop.
                        && notification.turn.error.as_ref().is_none_or(|terminal_error| {
                            turn.error.as_ref().and_then(|error| error.codex_error_info.as_ref())
                                == terminal_error.codex_error_info.as_ref()
                        }))
                        || history.any(|turn| turn.status == TurnStatus::InProgress))
                {
                    // Matching terminal history, or a later running turn in the ordered history,
                    // already accounts for this applied completion. A different id alone does not.
                    return None;
                }
                ServerNotification::TurnCompleted(notification.as_ref().clone())
            }
            TerminalNotification::Closed(notification) => {
                ServerNotification::ThreadClosed(notification.clone())
            }
        };
        let retained = self.buffer.iter().any(|event| {
            let ThreadBufferedEvent::Notification(notification) = event else {
                return false;
            };
            match (notification.as_ref(), &terminal) {
                (
                    ServerNotification::TurnCompleted(candidate),
                    ServerNotification::TurnCompleted(latest),
                ) => candidate.turn.id == latest.turn.id,
                (ServerNotification::ThreadClosed(_), ServerNotification::ThreadClosed(_)) => true,
                _ => false,
            }
        });
        (!retained).then(|| ThreadBufferedEvent::Notification(Box::new(terminal)))
    }
}

impl App {
    pub(super) async fn acknowledge_live_terminal(&self, event: &ThreadBufferedEvent) {
        let ThreadBufferedEvent::Notification(notification) = event else {
            return;
        };
        let ServerNotification::TurnCompleted(completed) = notification.as_ref() else {
            return;
        };
        let Ok(thread_id) = ThreadId::from_string(&completed.thread_id) else {
            return;
        };
        let Some(channel) = self.thread_event_channels.get(&thread_id) else {
            return;
        };
        let mut store = channel.store.lock().await;
        if let Some(TerminalNotification::Completed {
            notification,
            delivery,
        }) = store.terminal_notification.as_mut()
            && notification.turn.id == completed.turn.id
        {
            *delivery = TerminalDelivery::Applied;
        }
    }

    pub(super) fn trim_thread_cache_after_event(&self, thread_id: ThreadId) {
        let Some(channel) = self.thread_event_channels.get(&thread_id) else {
            return;
        };
        let can_evict = channel
            .store
            .try_lock()
            .is_ok_and(|store| store.can_evict_history() && store.history_payload_bytes() != 0);
        if can_evict {
            self.trim_thread_cache(INACTIVE_HISTORY_BUDGET);
        }
    }

    /// Sweep only at idle-event and thread-switch checkpoints, not on each running stream delta.
    pub(super) fn trim_thread_cache(&self, budget: usize) {
        let mut retained_bytes = 0usize;
        let mut candidates = Vec::new();
        for (thread_id, channel) in &self.thread_event_channels {
            if !self.side_threads.contains_key(thread_id)
                && let Ok(store) = channel.store.try_lock()
                && store.can_evict_history()
            {
                let bytes = store.history_payload_bytes();
                retained_bytes = retained_bytes.saturating_add(bytes);
                candidates.push((bytes, store));
            }
        }
        candidates.sort_unstable_by_key(|(bytes, _)| Reverse(*bytes));
        for (bytes, mut store) in candidates {
            if retained_bytes <= budget {
                break;
            }
            store.evict_history();
            retained_bytes = retained_bytes.saturating_sub(bytes);
        }
    }

    pub(super) async fn reload_evicted_thread_history(
        &mut self,
        tui: &mut tui::Tui,
        app_server: &mut AppServerSession,
        thread_id: ThreadId,
    ) -> Result<()> {
        let Some(store) = self
            .thread_event_channels
            .get(&thread_id)
            .map(|channel| Arc::clone(&channel.store))
        else {
            return Ok(());
        };
        if !store.lock().await.history_reload_required {
            return Ok(());
        }
        let config = self.config.clone();
        let turns = self
            .wait_with_rollout_maintenance(tui, app_server.rollout_maintenance(), async {
                let mut thread = app_server
                    .thread_read(thread_id, /*include_turns*/ false)
                    .await?;
                app_server
                    .hydrate_initial_thread_history(
                        &mut thread,
                        /*turn_cursor*/ None,
                        /*item_cursor*/ None,
                        Some(&config),
                        HistoryHydrationScope::Initial,
                    )
                    .await?;
                if thread.turns.is_empty() {
                    color_eyre::eyre::bail!(
                        "Saved history for thread {thread_id} is not yet available"
                    );
                }
                Ok(thread.turns)
            })
            .await??;
        // Rebase events received since eviction so hydrated turns do not replay twice. Keep the
        // receiver, composer checkpoint, non-rehydratable controls, and live terminal state.
        let mut store = store.lock().await;
        store.evict_history();
        store.set_history_payload(turns);
        Ok(())
    }
}
