//! `thread/delete` request handling.

use super::thread_processor::unsupported_thread_store_operation;
use super::*;

impl ThreadRequestProcessor {
    pub(crate) async fn thread_delete(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadDeleteParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        let mut deleted_thread_ids = Vec::new();
        let result = {
            let _thread_list_state_permit = self.acquire_thread_list_state_permit().await?;
            self.thread_delete_response(params, &mut deleted_thread_ids)
                .await
        };
        match result {
            Ok(response) => {
                self.outgoing
                    .send_response(request_id.clone(), response)
                    .await;
                self.send_thread_deleted_notifications(deleted_thread_ids)
                    .await;
                Ok(None)
            }
            Err(error) => {
                self.send_thread_deleted_notifications(deleted_thread_ids)
                    .await;
                Err(error)
            }
        }
    }

    async fn thread_delete_response(
        &self,
        params: ThreadDeleteParams,
        deleted_thread_ids: &mut Vec<String>,
    ) -> Result<ThreadDeleteResponse, JSONRPCErrorError> {
        let thread_id = ThreadId::from_string(&params.thread_id)
            .map_err(|err| invalid_request(format!("invalid thread id: {err}")))?;

        let current_agent_membership = self
            .thread_manager
            .prepare_current_agent_membership_eviction(thread_id)
            .await
            .map_err(|err| {
                internal_error(format!(
                    "failed to prepare thread subtree {thread_id} for delete: {err}"
                ))
            })?;
        let thread_ids = current_agent_membership.candidate_thread_ids().to_vec();

        self.validate_root_thread_delete(thread_id, thread_ids.len() > 1)
            .await?;
        for thread_id_to_delete in thread_ids.iter().copied() {
            let identity_preserved = current_agent_membership
                .unload_candidate_runtime_preserving_identity(thread_id_to_delete)
                .await
                .map_err(|err| {
                    internal_error(format!(
                        "failed to prepare thread {thread_id_to_delete} for delete: {err}"
                    ))
                })?;
            if identity_preserved {
                self.finalize_thread_teardown(thread_id_to_delete).await;
                if let Some(log_db) = self.log_db.as_ref() {
                    log_db.flush().await;
                }
            } else {
                self.prepare_thread_for_delete(thread_id_to_delete).await;
            }
        }

        let mut persisted_thread_ids = Vec::new();
        for candidate_thread_id in thread_ids.iter().copied() {
            match self
                .thread_store
                .read_thread(StoreReadThreadParams {
                    thread_id: candidate_thread_id,
                    include_archived: true,
                    include_history: false,
                })
                .await
            {
                Ok(_) => persisted_thread_ids.push(candidate_thread_id),
                Err(ThreadStoreError::ThreadNotFound { .. }) => {}
                // Deletion remains available when persisted metadata cannot be decoded.
                Err(_) => persisted_thread_ids.push(candidate_thread_id),
            }
        }

        let mut delete_order: Vec<_> = persisted_thread_ids
            .iter()
            .filter(|candidate_thread_id| **candidate_thread_id != thread_id)
            .rev()
            .copied()
            .collect();
        if persisted_thread_ids.contains(&thread_id) {
            delete_order.push(thread_id);
        }

        let delete_outcome = self
            .thread_store
            .delete_threads_with_outcome(StoreDeleteThreadsParams {
                thread_ids: delete_order.clone(),
            })
            .await
            .map_err(thread_store_delete_error)?;

        let reconciliation_seeds = if delete_outcome.failure.is_none() {
            thread_ids.as_slice()
        } else {
            delete_outcome.deleted_thread_ids.as_slice()
        };
        let reconciled_thread_ids = current_agent_membership
            .current_ids_with_current_only_descendants(reconciliation_seeds);

        let state_cleanup_error = if let Some(state_db) = self.state_db.as_ref() {
            state_db
                .delete_threads_strict(reconciled_thread_ids.as_slice())
                .await
                .err()
                .map(|err| {
                    internal_error(format!(
                        "failed to delete app-server state for {thread_id}: {err}"
                    ))
                })
        } else {
            None
        };

        if let Err(err) = current_agent_membership
            .evict_exact(&reconciled_thread_ids)
            .await
        {
            warn!(
                "deleted thread {thread_id} and retired its current identities, but runtime shutdown reported an error: {err}"
            );
        }

        let persisted_thread_id_set = thread_ids.iter().copied().collect::<HashSet<_>>();
        deleted_thread_ids.extend(
            reconciled_thread_ids
                .iter()
                .filter(|thread_id| !persisted_thread_id_set.contains(thread_id))
                .map(ToString::to_string),
        );
        let reconciled_thread_ids = reconciled_thread_ids.into_iter().collect::<HashSet<_>>();
        deleted_thread_ids.extend(
            thread_ids[1..]
                .iter()
                .rev()
                .chain(thread_ids.first())
                .filter(|thread_id| reconciled_thread_ids.contains(thread_id))
                .map(ToString::to_string),
        );

        if let Some(error) = state_cleanup_error {
            return Err(error);
        }

        if let Some(failure) = delete_outcome.failure {
            return Err(thread_store_delete_error(failure.error));
        }

        Ok(ThreadDeleteResponse {})
    }

    async fn send_thread_deleted_notifications(&self, deleted_thread_ids: Vec<String>) {
        for thread_id in deleted_thread_ids {
            self.outgoing
                .send_server_notification(ServerNotification::ThreadDeleted(
                    ThreadDeletedNotification { thread_id },
                ))
                .await;
        }
    }

    async fn validate_root_thread_delete(
        &self,
        thread_id: ThreadId,
        has_descendants: bool,
    ) -> Result<(), JSONRPCErrorError> {
        if let Ok(thread) = self.thread_manager.get_thread(thread_id).await {
            if !thread.config_snapshot().await.ephemeral {
                return Ok(());
            }
            return Err(invalid_request(format!(
                "thread is not persisted and cannot be deleted: {thread_id}"
            )));
        }
        match self
            .thread_store
            .read_thread(StoreReadThreadParams {
                thread_id,
                include_archived: true,
                include_history: false,
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(ThreadStoreError::ThreadNotFound { .. }) => {
                if has_descendants {
                    return Ok(());
                }
                let Some(state_db) = self.state_db.as_ref() else {
                    return Err(thread_store_delete_error(
                        ThreadStoreError::ThreadNotFound { thread_id },
                    ));
                };
                if state_db
                    .get_thread(thread_id)
                    .await
                    .map_err(|err| {
                        internal_error(format!(
                            "failed to read app-server state for {thread_id}: {err}"
                        ))
                    })?
                    .is_some()
                {
                    Ok(())
                } else {
                    Err(thread_store_delete_error(
                        ThreadStoreError::ThreadNotFound { thread_id },
                    ))
                }
            }
            Err(err) => Err(thread_store_delete_error(err)),
        }
    }

    async fn prepare_thread_for_delete(&self, thread_id: ThreadId) {
        self.prepare_thread_for_removal(thread_id, "delete").await;
        if let Some(log_db) = self.log_db.as_ref() {
            log_db.flush().await;
        }
    }
}

fn thread_store_delete_error(err: ThreadStoreError) -> JSONRPCErrorError {
    match err {
        ThreadStoreError::ThreadNotFound { thread_id } => {
            invalid_request(format!("thread not found: {thread_id}"))
        }
        ThreadStoreError::InvalidRequest { message } | ThreadStoreError::Conflict { message } => {
            invalid_request(message)
        }
        ThreadStoreError::Unsupported { operation } => {
            unsupported_thread_store_operation(operation)
        }
        err => internal_error(format!("failed to delete thread: {err}")),
    }
}
