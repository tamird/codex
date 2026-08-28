//! Payload accounting for the TUI's retained transcript caches.
//!
//! JSON byte counts estimate payload size, not allocated or resident memory. They exclude saved
//! input, compact lifecycle/session metadata, channel copies, transient snapshots, and rendered cells.
//! Count payloads when the cache changes; the activity-window report only reads cached counters.

use super::App;
use super::ThreadBufferedEvent;
use super::thread_cache_eviction::HistoryPinReason;
use super::thread_cache_eviction::INACTIVE_HISTORY_BUDGET;
use crate::app_event::HistoryLookupResponse;
use codex_protocol::ThreadId;
use serde::Serialize;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::io;
use std::time::Duration;
use std::time::Instant;

pub(super) fn json_bytes(value: &impl Serialize) -> usize {
    #[derive(Default)]
    struct Counter(usize);

    impl io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len());
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let mut counter = Counter::default();
    if let Err(error) = serde_json::to_writer(&mut counter, value) {
        tracing::warn!(%error, "failed to count retained TUI payload");
    }
    counter.0
}

impl ThreadBufferedEvent {
    pub(super) fn payload_bytes(&self) -> usize {
        match self {
            Self::Notification(notification) => json_bytes(notification),
            Self::Request(request) => json_bytes(request),
            Self::HistoryEntryResponse(event) => match event {
                HistoryLookupResponse::Entry {
                    offset: _,
                    log_id: _,
                    entry,
                } => json_bytes(entry),
                HistoryLookupResponse::Batch {
                    cursor: _,
                    log_id: _,
                    entries,
                    next_older_cursor: _,
                } => entries
                    .iter()
                    .map(|entry| json_bytes(&entry.entry))
                    .fold(/*init*/ 0, usize::saturating_add),
                HistoryLookupResponse::BatchError {
                    cursor: _,
                    log_id: _,
                } => 0,
            },
            Self::FeedbackSubmission(event) => json_bytes(&event.result),
        }
    }
}

struct ThreadCacheSample {
    thread_id: ThreadId,
    event_bytes: usize,
    turn_bytes: usize,
    event_count: usize,
    turn_count: usize,
    active: bool,
    running: bool,
    first_pin_reason: Option<HistoryPinReason>,
    reclaimable_bytes: usize,
    history_evicted: bool,
}

impl App {
    /// Report totals and the eight largest caches without traversing their retained payloads.
    pub(super) async fn report_thread_cache(&self, thread_id: ThreadId) {
        let started_at = Instant::now();
        let mut samples = Vec::with_capacity(self.thread_event_channels.len());
        // This map has at most one entry per fixed reason, independent of retained payload size.
        let mut first_pin_reason_counts = BTreeMap::<HistoryPinReason, usize>::new();
        let mut total_lock_acquire_duration = Duration::ZERO;
        let mut max_lock_acquire_duration = Duration::ZERO;
        let mut event_bytes = 0usize;
        let mut turn_bytes = 0usize;
        let mut inactive_bytes = 0usize;
        let mut running_bytes = 0usize;
        let mut reclaimable_bytes = 0usize;
        let mut pinned_bytes = 0usize;
        let mut event_count = 0usize;
        let mut turn_count = 0usize;
        let mut queued_event_count = self
            .active_thread_rx
            .as_ref()
            .map_or(/*default*/ 0, tokio::sync::mpsc::Receiver::len);
        for (thread_id, channel) in &self.thread_event_channels {
            let lock_started_at = Instant::now();
            let store = channel.store.lock().await;
            // Includes time descheduled while acquiring the lock, not lock hold time or CPU time.
            let lock_acquire_duration = lock_started_at.elapsed();
            total_lock_acquire_duration += lock_acquire_duration;
            max_lock_acquire_duration = max_lock_acquire_duration.max(lock_acquire_duration);
            let bytes = store
                .buffered_payload_bytes
                .saturating_add(store.turn_payload_bytes);
            event_bytes = event_bytes.saturating_add(store.buffered_payload_bytes);
            turn_bytes = turn_bytes.saturating_add(store.turn_payload_bytes);
            event_count = event_count.saturating_add(store.buffer.len());
            turn_count = turn_count.saturating_add(store.turns.len());
            let running = store.active_turn_id.is_some();
            if !store.active {
                inactive_bytes = inactive_bytes.saturating_add(bytes);
            }
            if running {
                running_bytes = running_bytes.saturating_add(bytes);
            }
            let first_pin_reason = if self.side_threads.contains_key(thread_id) {
                Some(HistoryPinReason::SideThread)
            } else {
                store.history_pin_reason()
            };
            let reclaimable = if let Some(reason) = first_pin_reason {
                *first_pin_reason_counts.entry(reason).or_default() += 1;
                0
            } else {
                store.history_payload_bytes()
            };
            reclaimable_bytes = reclaimable_bytes.saturating_add(reclaimable);
            pinned_bytes = pinned_bytes.saturating_add(bytes.saturating_sub(reclaimable));
            queued_event_count = queued_event_count.saturating_add(
                channel
                    .receiver
                    .as_ref()
                    .map_or(/*default*/ 0, tokio::sync::mpsc::Receiver::len),
            );
            samples.push(ThreadCacheSample {
                thread_id: *thread_id,
                event_bytes: store.buffered_payload_bytes,
                turn_bytes: store.turn_payload_bytes,
                event_count: store.buffer.len(),
                turn_count: store.turns.len(),
                active: store.active,
                running,
                first_pin_reason,
                reclaimable_bytes: reclaimable,
                history_evicted: store.history_reload_required,
            });
        }
        samples.sort_unstable_by_key(|sample| {
            Reverse(sample.event_bytes.saturating_add(sample.turn_bytes))
        });
        // Include collection, lock acquisition, and sorting; exclude writing the diagnostics.
        let duration = started_at.elapsed();
        tracing::debug!(
            target: "codex.performance",
            operation = "tui.thread_cache",
            %thread_id,
            thread_count = samples.len(),
            event_count,
            turn_count,
            event_payload_json_bytes = event_bytes,
            turn_payload_json_bytes = turn_bytes,
            inactive_payload_json_bytes = inactive_bytes,
            running_payload_json_bytes = running_bytes,
            reclaimable_payload_json_bytes = reclaimable_bytes,
            pinned_payload_json_bytes = pinned_bytes,
            inactive_history_budget_bytes = INACTIVE_HISTORY_BUDGET,
            first_pin_reason_counts = ?first_pin_reason_counts,
            duration_us = duration.as_micros(),
            total_lock_acquire_duration_us = total_lock_acquire_duration.as_micros(),
            max_lock_acquire_duration_us = max_lock_acquire_duration.as_micros(),
            queued_event_count,
            transcript_cell_count = self.transcript_cells.len(),
            "TUI retained payload estimates (not heap bytes)"
        );
        for sample in samples.into_iter().take(/*n*/ 8) {
            tracing::debug!(
                target: "codex.performance",
                operation = "tui.thread_cache.largest",
                thread_id = %sample.thread_id,
                event_payload_json_bytes = sample.event_bytes,
                turn_payload_json_bytes = sample.turn_bytes,
                event_count = sample.event_count,
                turn_count = sample.turn_count,
                active = sample.active,
                running = sample.running,
                first_pin_reason = ?sample.first_pin_reason,
                reclaimable_payload_json_bytes = sample.reclaimable_bytes,
                history_evicted = sample.history_evicted,
                "TUI retained thread payload estimate (not heap bytes)"
            );
        }
    }
}
