//! One UI observation of rollout maintenance while a foreground RPC is pending.
//!
//! The initialized snapshot and later notifications are consumed in event-stream order. There is
//! deliberately no status/read seed: responses and notifications use separate client queues, so a
//! read response cannot serve as an ordering barrier. Older servers simply emit no maintenance UI.

use super::AppServerSession;
use codex_app_server_client::AppServerEvent;
use codex_app_server_client::TypedRequestError;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::RolloutMaintenanceRequestStatus;
use codex_app_server_protocol::RolloutMaintenanceStatusChangedNotification;
use codex_app_server_protocol::ServerNotification;
use serde::de::DeserializeOwned;
use tokio::sync::watch;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RolloutMaintenanceState {
    background: Option<RolloutMaintenanceRequestStatus>,
    request: Option<(RequestId, RolloutMaintenanceRequestStatus)>,
}

impl RolloutMaintenanceState {
    pub(crate) fn background_text(&self) -> Option<String> {
        self.background
            .as_ref()
            .and_then(crate::rollout_maintenance::background_status_text)
    }

    pub(crate) fn display_text(&self) -> Option<String> {
        self.request
            .as_ref()
            .and_then(|(_, status)| crate::rollout_maintenance::request_status_text(status))
            .or_else(|| self.background_text())
    }
}

struct PendingMaintenanceRequest {
    sender: watch::Sender<RolloutMaintenanceState>,
    request_id: RequestId,
}

impl Drop for PendingMaintenanceRequest {
    fn drop(&mut self) {
        self.sender.send_modify(|state| {
            if state
                .request
                .as_ref()
                .is_some_and(|(id, _)| id == &self.request_id)
            {
                state.request = None;
            }
        });
    }
}

impl AppServerSession {
    pub(crate) fn rollout_maintenance(&self) -> watch::Receiver<RolloutMaintenanceState> {
        self.rollout_maintenance.subscribe()
    }

    pub(super) fn observe_rollout_maintenance(&self, event: &AppServerEvent) -> bool {
        let AppServerEvent::ServerNotification(notification) = event else {
            return false;
        };
        let ServerNotification::RolloutMaintenanceStatusChanged(notification) =
            notification.as_ref()
        else {
            return false;
        };
        self.rollout_maintenance.send_if_modified(|state| {
            match notification {
                RolloutMaintenanceStatusChangedNotification::Snapshot { status } => {
                    if state.background == status.background_migration {
                        return false;
                    }
                    state.background = status.background_migration.clone();
                }
                RolloutMaintenanceStatusChangedNotification::Request { request_id, status } => {
                    let Some((pending_id, current)) = state.request.as_mut() else {
                        return false;
                    };
                    if pending_id != request_id || current == status {
                        return false;
                    }
                    *current = status.clone();
                }
            }
            true
        });
        true
    }

    pub(super) async fn request_with_maintenance<T>(
        &mut self,
        request: ClientRequest,
    ) -> Result<T, TypedRequestError>
    where
        T: DeserializeOwned,
    {
        let request_id = request.id().clone();
        let handle = self.client.request_handle();
        let response = handle.request_typed(request);
        if self.rollout_maintenance.receiver_count() == 0 {
            return response.await;
        }
        self.rollout_maintenance.send_modify(|state| {
            state.request = Some((request_id.clone(), RolloutMaintenanceRequestStatus::Idle));
        });
        // The response/error (or cancellation) is authoritative. Never let queued progress
        // from a finished request become the next request's display.
        let _pending = PendingMaintenanceRequest {
            sender: self.rollout_maintenance.clone(),
            request_id,
        };
        tokio::pin!(response);
        loop {
            tokio::select! {
                result = &mut response => return result,
                event = self.client.next_event() => {
                    let Some(event) = event else {
                        return response.await;
                    };
                    if !self.observe_rollout_maintenance(&event) {
                        // Both app-server-client transports deliberately use an unbounded
                        // ordered consumer queue so unread events cannot deadlock an RPC.
                        // Move each event once, retaining that contract.
                        self.pending_events.push_back(event);
                    }
                }
            }
        }
    }
}
