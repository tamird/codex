//! Bounded, deployment-aware performance diagnostics for interactive sessions.

use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

use crate::version::CODEX_CLI_VERSION;

static EMBEDDED_BUILD_REVISION: OnceLock<&'static str> = OnceLock::new();

/// Supplies executable-owned provenance without stamping the TUI library.
///
/// Call before constructing the CLI or starting the TUI, since version lookups
/// are cached. Only the first supplied candidate is retained.
pub fn initialize_build_revision(revision: Option<&'static str>) {
    if let Some(revision) = revision {
        let _ = EMBEDDED_BUILD_REVISION.set(revision);
    }
}

const BUILD_REVISION_ENV: &str = "FRANKENDEX_BUILD_REVISION";
const BUILD_COHORT_ENV: &str = "FRANKENDEX_BUILD_COHORT";
const PERFORMANCE_WINDOW_INTERVAL: Duration = Duration::from_secs(30);

/// Report operations that take at least two redraw intervals.
pub(crate) const SLOW_TUI_OPERATION_THRESHOLD: Duration =
    crate::tui::TARGET_FRAME_INTERVAL.saturating_mul(2);

/// Records slow turn requests without retaining their inputs or response bodies.
pub(crate) fn record_turn_request<T, E>(
    thread_id: impl std::fmt::Display,
    method: &'static str,
    started_at: Instant,
    result: &Result<T, E>,
) {
    let duration = started_at.elapsed();
    if duration >= SLOW_TUI_OPERATION_THRESHOLD {
        let outcome = if result.is_ok() { "ok" } else { "error" };
        tracing::debug!(
            target: "codex.performance",
            thread_id = %thread_id,
            operation = "tui.turn_request",
            method,
            outcome,
            duration_us = duration.as_micros(),
            "slow TUI turn request"
        );
    }
}

/// Identifies locally deployed builds without stamping the complete Bazel graph.
pub(crate) fn record_deployment_start() {
    let Some(deployment) = Deployment::from_environment() else {
        return;
    };

    tracing::debug!(
        target: "codex.performance",
        operation = "process.start",
        build_revision = deployment.revision,
        build_cohort = deployment.cohort,
        app_version = env!("CARGO_PKG_VERSION"),
        "Frankendex deployment started"
    );
}

/// Bounded local-build attribution retained alongside each activity window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Deployment {
    revision: String,
    cohort: String,
}

impl Deployment {
    pub(crate) fn from_environment() -> Option<Self> {
        let revision = EMBEDDED_BUILD_REVISION
            .get()
            .copied()
            .filter(|revision| valid_build_revision(revision))
            .map(str::to_string)
            .or_else(|| std::env::var(BUILD_REVISION_ENV).ok())?;
        Self::new(revision, std::env::var(BUILD_COHORT_ENV).ok())
    }

    fn new(revision: String, cohort: Option<String>) -> Option<Self> {
        if !valid_build_revision(&revision) {
            return None;
        }

        let cohort = cohort
            .filter(|value| valid_build_cohort(value))
            .unwrap_or_else(|| "default".to_string());
        Some(Self { revision, cohort })
    }

    pub(crate) fn display_version(&self) -> String {
        format!("{CODEX_CLI_VERSION}+frankendex.{}", self.revision)
    }
}

fn valid_build_revision(revision: &str) -> bool {
    let revision = revision.strip_suffix("-dirty").unwrap_or(revision);
    (7..=40).contains(&revision.len())
        && revision
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn valid_build_cohort(cohort: &str) -> bool {
    (1..=32).contains(&cohort.len())
        && cohort
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

/// Summarizes one bounded interval of interactive event-loop activity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PerformanceSummary {
    pub(crate) window_duration: Duration,
    pub(crate) event_count: u64,
    pub(crate) draw_count: u64,
    pub(crate) slow_event_count: u64,
    pub(crate) slow_draw_count: u64,
    pub(crate) total_duration: Duration,
    pub(crate) max_duration: Duration,
    pub(crate) duration_buckets: [u64; 5],
}

/// Accumulates bounded activity summaries without timers or background tasks.
pub(crate) struct PerformanceWindow {
    started_at: Instant,
    deployment: Option<Deployment>,
    event_count: u64,
    draw_count: u64,
    slow_event_count: u64,
    slow_draw_count: u64,
    total_duration: Duration,
    max_duration: Duration,
    duration_buckets: [u64; 5],
}

impl PerformanceWindow {
    pub(crate) fn new(started_at: Instant) -> Self {
        Self::with_deployment(started_at, Deployment::from_environment())
    }

    fn with_deployment(started_at: Instant, deployment: Option<Deployment>) -> Self {
        Self {
            started_at,
            deployment,
            event_count: 0,
            draw_count: 0,
            slow_event_count: 0,
            slow_draw_count: 0,
            total_duration: Duration::ZERO,
            max_duration: Duration::ZERO,
            duration_buckets: [0; 5],
        }
    }

    pub(crate) fn record(
        &mut self,
        event_kind: &str,
        duration: Duration,
        slow_threshold: Duration,
        completed_at: Instant,
    ) -> Option<PerformanceSummary> {
        self.event_count = self.event_count.saturating_add(1);
        if event_kind == "draw" {
            self.draw_count = self.draw_count.saturating_add(1);
        }
        if duration >= slow_threshold {
            self.slow_event_count = self.slow_event_count.saturating_add(1);
            if event_kind == "draw" {
                self.slow_draw_count = self.slow_draw_count.saturating_add(1);
            }
        }
        self.total_duration = self.total_duration.saturating_add(duration);
        self.max_duration = self.max_duration.max(duration);
        let bucket = match duration.as_millis() {
            0 => 0,
            1..=3 => 1,
            4..=7 => 2,
            8..=15 => 3,
            _ => 4,
        };
        self.duration_buckets[bucket] = self.duration_buckets[bucket].saturating_add(1);

        let window_duration = completed_at.saturating_duration_since(self.started_at);
        if window_duration < PERFORMANCE_WINDOW_INTERVAL {
            return None;
        }

        let summary = PerformanceSummary {
            window_duration,
            event_count: self.event_count,
            draw_count: self.draw_count,
            slow_event_count: self.slow_event_count,
            slow_draw_count: self.slow_draw_count,
            total_duration: self.total_duration,
            max_duration: self.max_duration,
            duration_buckets: self.duration_buckets,
        };
        let deployment = self.deployment.take();
        *self = Self::with_deployment(completed_at, deployment);
        Some(summary)
    }

    pub(crate) fn report(&self, summary: PerformanceSummary, thread_id: impl std::fmt::Display) {
        let (build_revision, build_cohort) = self
            .deployment
            .as_ref()
            .map_or(("unknown", "default"), |deployment| {
                (deployment.revision.as_str(), deployment.cohort.as_str())
            });

        tracing::debug!(
            target: "codex.performance",
            thread_id = %thread_id,
            operation = "tui.window",
            build_revision,
            build_cohort,
            window_duration_us = summary.window_duration.as_micros(),
            event_count = summary.event_count,
            draw_count = summary.draw_count,
            slow_event_count = summary.slow_event_count,
            slow_draw_count = summary.slow_draw_count,
            total_duration_us = summary.total_duration.as_micros(),
            max_duration_us = summary.max_duration.as_micros(),
            under_1ms = summary.duration_buckets[0],
            under_4ms = summary.duration_buckets[1],
            under_8ms = summary.duration_buckets[2],
            under_16ms = summary.duration_buckets[3],
            at_least_16ms = summary.duration_buckets[4],
            "TUI activity window"
        );
    }
}

#[cfg(test)]
#[path = "performance_tests.rs"]
mod tests;
