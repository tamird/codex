//! Preserve terminal delivery across lossy replay and session refresh.

use super::*;
use codex_app_server_protocol::ThreadClosedNotification;
use codex_app_server_protocol::TurnCompletedNotification;

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

impl ThreadEventStore {
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
}
