//! Completes selected-rollout migration before serializing resume lifecycle changes.

use super::*;
use codex_thread_store::ReadThreadsParams;

impl ThreadRequestProcessor {
    pub(super) async fn acquire_thread_resume_permit(
        &self,
        params: &ThreadResumeParams,
    ) -> Result<SemaphorePermit<'_>, JSONRPCErrorError> {
        let mut prepared = params.history.is_some();
        let requested_id = ThreadId::from_string(&params.thread_id).ok();
        loop {
            let running = if let Some(thread_id) = requested_id {
                self.thread_manager.get_thread(thread_id).await.is_ok()
            } else {
                false
            };
            if !prepared && !running {
                let source = self
                    .read_stored_thread_for_resume(
                        &params.thread_id,
                        params.path.as_ref(),
                        /*include_history*/ false,
                    )
                    .await?;
                if params.path.is_some() {
                    // Explicit paths can identify external or stale rollouts. Only migrate the
                    // selected source; the protected resume still validates the requested path.
                    let indexed = self
                        .thread_store
                        .read_threads(ReadThreadsParams {
                            thread_ids: vec![source.thread_id],
                        })
                        .await
                        .map_err(thread_store_resume_read_error)?;
                    let selected_path = indexed.iter().any(|current| {
                        current
                            .rollout_path
                            .as_ref()
                            .zip(source.rollout_path.as_ref())
                            .is_some_and(|(current, requested)| {
                                path_utils::paths_match_after_normalization(
                                    codex_rollout::plain_rollout_path(current).as_path(),
                                    codex_rollout::plain_rollout_path(requested).as_path(),
                                )
                            })
                    });
                    let unindexed_local_path = indexed.is_empty()
                        && source.rollout_path.as_ref().is_some_and(|path| {
                            path.starts_with(
                                self.config.codex_home.join(codex_rollout::SESSIONS_SUBDIR),
                            )
                        });
                    if selected_path || unindexed_local_path {
                        self.thread_store
                            .read_thread(StoreReadThreadParams {
                                thread_id: source.thread_id,
                                include_archived: true,
                                include_history: false,
                            })
                            .await
                            .map_err(thread_store_resume_read_error)?;
                    }
                }
                prepared = true;
            }
            let permit = self.acquire_thread_list_state_permit().await?;
            if !prepared
                && let Some(thread_id) = requested_id
                && self.thread_manager.get_thread(thread_id).await.is_err()
            {
                // An idle runtime can unload while this request waits for the lifecycle permit.
                drop(permit);
                continue;
            }
            // Do not reuse preflight metadata: archive, deletion and source selection may have
            // changed while migration ran. Existing resume checks run again under this permit.
            return Ok(permit);
        }
    }
}
