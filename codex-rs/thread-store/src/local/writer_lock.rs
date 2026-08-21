//! Maps shared rollout writer ownership to thread-store errors.

use std::io;
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

/// Translates ownership and fork-reader reservation errors into thread-store errors.
#[derive(Debug)]
pub(super) struct WriterLockGuard {
    inner: codex_rollout::RolloutWriterLockGuard,
}

impl WriterLockCoordinator {
    pub(super) fn new(codex_home: &Path) -> Self {
        Self {
            inner: Arc::new(RolloutWriterLockCoordinator::new(codex_home)),
        }
    }

    pub(super) fn acquire(&self, thread_id: ThreadId) -> ThreadStoreResult<WriterLockGuard> {
        self.inner
            .try_acquire(thread_id)
            .map_err(lifecycle_lock_error)?
            .map(|inner| WriterLockGuard { inner })
            .ok_or_else(|| ThreadStoreError::Conflict {
                message: format!("thread {thread_id} already has an active writer"),
            })
    }

    pub(super) fn acquire_fork_reader(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<WriterLockGuard> {
        self.inner
            .try_acquire_fork_reader(thread_id)
            .map_err(lifecycle_lock_error)?
            .map(|inner| WriterLockGuard { inner })
            .ok_or_else(|| ThreadStoreError::Conflict {
                message: format!("thread {thread_id} has not exported a fork snapshot"),
            })
    }
}

impl WriterLockGuard {
    pub(super) fn share_for_fork(&mut self) -> ThreadStoreResult<()> {
        self.inner.share_for_fork().map_err(lifecycle_lock_error)
    }

    pub(super) fn require_exclusive(&mut self) -> ThreadStoreResult<()> {
        self.inner.require_exclusive().map_err(|error| {
            if error.kind() == io::ErrorKind::WouldBlock {
                ThreadStoreError::Conflict {
                    message: "thread history is reserved by an initializing fork".to_string(),
                }
            } else {
                lifecycle_lock_error(error)
            }
        })
    }

    pub(super) fn is_reserved(&self) -> bool {
        self.inner.is_reserved()
    }
}

fn lifecycle_lock_error(error: io::Error) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("failed to reserve fork source: {error}"),
    }
}

#[cfg(test)]
#[path = "writer_lock_tests.rs"]
mod tests;
