//! Maps shared rollout writer ownership to thread-store errors.

use std::path::Path;
use std::sync::Arc;

use codex_protocol::ThreadId;
use codex_rollout::RolloutWriterLockCoordinator;

use crate::ThreadStoreError;
use crate::ThreadStoreResult;

#[cfg(test)]
const WRITER_LOCK_DIR: &str = "thread-writer-locks";
#[cfg(test)]
const COORDINATION_LOCK_FILE: &str = ".coordination.lock";

/// Thread-store adapter for the writer locks also held by rollout compression.
pub(super) struct WriterLockCoordinator {
    inner: Arc<RolloutWriterLockCoordinator>,
}

pub(super) type WriterLockGuard = codex_rollout::RolloutWriterLockGuard;

impl WriterLockCoordinator {
    pub(super) fn new(codex_home: &Path) -> Self {
        Self {
            inner: Arc::new(RolloutWriterLockCoordinator::new(codex_home)),
        }
    }

    pub(super) fn acquire(&self, thread_id: ThreadId) -> ThreadStoreResult<WriterLockGuard> {
        self.inner
            .try_acquire(thread_id)
            .map_err(|error| ThreadStoreError::Internal {
                message: error.to_string(),
            })?
            .ok_or_else(|| ThreadStoreError::Conflict {
                message: format!("thread {thread_id} already has an active writer"),
            })
    }
}

#[cfg(test)]
#[path = "writer_lock_tests.rs"]
mod tests;
