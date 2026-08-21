//! Defer repeated failed conversion without caching the history returned by its supported reader.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::Duration;

use codex_protocol::ThreadId;
use tokio::time::Instant;

use super::LocalThreadStore;
use super::MigrationWork;
use super::RolloutFingerprint;
use super::retained_selected_source_is_unchanged;

const MAX_RECENT_FAILURES: usize = 128;
const RETRY_DELAY: Duration = Duration::from_secs(60);

/// Stores only the selected source identity, not decoded history or dependency headers.
#[derive(Clone)]
struct RecentFailure {
    thread_id: ThreadId,
    path: PathBuf,
    rollout_id: codex_protocol::RolloutId,
    source_fingerprint: RolloutFingerprint,
    retry_after: Instant,
}

/// Bounded retry suppression for unchanged sources that explicitly retained supported-reader use.
#[derive(Default)]
pub(super) struct RecentFailures(VecDeque<RecentFailure>);

impl RecentFailures {
    pub(super) fn record(&mut self, work: &MigrationWork) {
        self.0.retain(|entry| {
            entry.thread_id != work.thread_id && entry.retry_after > Instant::now()
        });
        if self.0.len() == MAX_RECENT_FAILURES {
            self.0.pop_front();
        }
        self.0.push_back(RecentFailure {
            thread_id: work.thread_id,
            path: work.path.clone(),
            rollout_id: work.rollout_id,
            source_fingerprint: work.source_fingerprint,
            retry_after: Instant::now() + RETRY_DELAY,
        });
    }
}

pub(super) async fn should_defer_retry(store: &LocalThreadStore, thread_id: ThreadId) -> bool {
    let recent = {
        let mut state = store.rollout_migration_coordinator.state.lock().await;
        state
            .recent_failures
            .0
            .retain(|entry| entry.retry_after > Instant::now());
        state
            .recent_failures
            .0
            .iter()
            .find(|entry| entry.thread_id == thread_id)
            .cloned()
    };
    let Some(recent) = recent else {
        return false;
    };
    // A new selection, an append, or a recovery journal requires an immediate retry. Ancestor-only
    // repairs cannot be detected from this bounded identity check, so suppression expires rather
    // than indefinitely trusting the selected file. History is still read fresh on every request.
    retained_selected_source_is_unchanged(
        store,
        thread_id,
        &recent.path,
        recent.rollout_id,
        recent.source_fingerprint,
    )
    .await
    .unwrap_or(false)
}
