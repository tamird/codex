//! Bounded app-server delivery of storage-owned maintenance status.

use std::future::Future;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::RolloutMaintenanceSnapshot;
use codex_app_server_protocol::RolloutMaintenanceStatusChangedNotification;
use codex_app_server_protocol::RolloutMaintenanceStatusReadResponse;
use codex_app_server_protocol::ServerNotification;
use codex_rollout::RolloutMaintenanceRequestStatus;
use codex_rollout::read_rollout_maintenance_status;
use codex_rollout::with_rollout_maintenance_observer;
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::error_code::internal_error;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::OutgoingMessageSender;
use crate::outgoing_message::RequestContext;

pub(crate) struct RolloutMaintenanceProcessor {
    codex_home: PathBuf,
    background: Option<watch::Receiver<RolloutMaintenanceRequestStatus>>,
    outgoing: Arc<OutgoingMessageSender>,
    snapshot_publication: Arc<Mutex<()>>,
    shutdown: CancellationToken,
}

impl RolloutMaintenanceProcessor {
    pub(crate) fn new(
        codex_home: PathBuf,
        background: Option<watch::Receiver<RolloutMaintenanceRequestStatus>>,
        outgoing: Arc<OutgoingMessageSender>,
    ) -> Self {
        let shutdown = CancellationToken::new();
        // Sampling and enqueueing are one ordered publication. Otherwise a slow initialized
        // snapshot can overwrite a newer terminal background update at the client.
        let snapshot_publication = Arc::new(Mutex::new(()));
        if let Some(mut updates) = background.clone() {
            let codex_home = codex_home.clone();
            let outgoing = Arc::clone(&outgoing);
            let shutdown = shutdown.clone();
            let snapshot_publication = Arc::clone(&snapshot_publication);
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        changed = updates.changed() => {
                            if changed.is_err() {
                                break;
                            }
                            let _publication = tokio::select! {
                                _ = shutdown.cancelled() => break,
                                publication = Arc::clone(&snapshot_publication).lock_owned() => publication,
                            };
                            match read_status(&codex_home, Some(&updates)) {
                                Ok(status) => {
                                    tokio::select! {
                                        _ = shutdown.cancelled() => break,
                                        _ = outgoing.send_server_notification(
                                            ServerNotification::RolloutMaintenanceStatusChanged(
                                                RolloutMaintenanceStatusChangedNotification::Snapshot { status },
                                            ),
                                        ) => {}
                                    }
                                }
                                Err(error) => tracing::warn!("failed to read rollout maintenance status: {error:?}"),
                            }
                        }
                    }
                }
            });
        }
        Self {
            codex_home,
            background,
            outgoing,
            snapshot_publication,
            shutdown,
        }
    }

    pub(crate) fn shutdown(&self) {
        self.shutdown.cancel();
    }

    pub(crate) fn status_read(
        &self,
    ) -> Result<RolloutMaintenanceStatusReadResponse, JSONRPCErrorError> {
        read_status(&self.codex_home, self.background.as_ref())
            .map(|status| RolloutMaintenanceStatusReadResponse { status })
    }

    pub(crate) async fn send_snapshot(&self, connections: &[ConnectionId]) {
        let _publication = Arc::clone(&self.snapshot_publication).lock_owned().await;
        match read_status(&self.codex_home, self.background.as_ref()) {
            Ok(status) => {
                self.outgoing
                    .send_server_notification_to_connections(
                        connections,
                        ServerNotification::RolloutMaintenanceStatusChanged(
                            RolloutMaintenanceStatusChangedNotification::Snapshot { status },
                        ),
                    )
                    .await
            }
            Err(error) => tracing::warn!("failed to read rollout maintenance status: {error:?}"),
        }
    }

    /// Observe the actual queued handler, not the task that merely enqueues it.
    pub(crate) async fn observe_request<F: Future>(
        &self,
        request_context: RequestContext,
        future: F,
    ) -> F::Output {
        let (updates, mut receiver) = watch::channel(RolloutMaintenanceRequestStatus::Idle);
        let (finished, mut completion) = oneshot::channel();
        let request = async {
            let output = with_rollout_maintenance_observer(
                Arc::new(move |status| {
                    updates.send_replace(status);
                }),
                future,
            )
            .await;
            let _ = finished.send(());
            output
        };
        let forwarding = async {
            let mut previous = RolloutMaintenanceRequestStatus::Idle;
            loop {
                tokio::select! {
                    _ = &mut completion => {
                        break;
                    }
                    changed = receiver.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        let status = *receiver.borrow_and_update();
                        if status != previous {
                            previous = status;
                            self.outgoing.try_send_rollout_maintenance_status(&request_context, status.into()).await;
                        }
                    }
                }
            }
        };
        // Outgoing backpressure must not stop the handler while it owns a storage lock.
        let (output, ()) = tokio::join!(request, forwarding);
        output
    }
}

impl Drop for RolloutMaintenanceProcessor {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn read_status(
    codex_home: &Path,
    background: Option<&watch::Receiver<RolloutMaintenanceRequestStatus>>,
) -> Result<RolloutMaintenanceSnapshot, JSONRPCErrorError> {
    let lock = read_rollout_maintenance_status(codex_home).map_err(|error| {
        internal_error(format!(
            "failed to read rollout maintenance status: {error}"
        ))
    })?;
    Ok(RolloutMaintenanceSnapshot {
        lock: lock.into(),
        background_migration: background.map(|receiver| (*receiver.borrow()).into()),
    })
}
