//! Routes measured migration work through the shared maintenance reporter.

use codex_protocol::ThreadId;
use codex_rollout::RolloutMaintenanceActivity;
use codex_rollout::RolloutMaintenanceOperation;
use codex_rollout::RolloutMaintenancePhase;
use codex_rollout::RolloutMaintenanceProgress;
use codex_rollout::RolloutMaintenanceProgressUnit;
use codex_rollout::RolloutMaintenanceReporter;

use super::RolloutMigrationRateLimiter;

impl RolloutMigrationRateLimiter {
    pub(super) fn activity(
        &self,
        operation: RolloutMaintenanceOperation,
        thread_id: Option<ThreadId>,
    ) -> RolloutMaintenanceActivity {
        RolloutMaintenanceActivity {
            io_bytes: self.reporter.activity().io_bytes,
            ..RolloutMaintenanceActivity::new(operation, thread_id)
        }
    }

    pub(super) fn attach_reporter(&mut self, reporter: RolloutMaintenanceReporter) {
        self.reporter = reporter;
    }

    pub(super) fn phase(&self, phase: RolloutMaintenancePhase) {
        let mut activity = self.reporter.activity();
        activity.phase = phase;
        activity.progress = None;
        self.reporter.update(activity);
    }

    pub(super) fn selected_thread(&self, thread_id: ThreadId) {
        let mut activity = self.reporter.activity();
        activity.thread_id = Some(thread_id);
        self.reporter.update(activity);
    }

    pub(super) fn phase_progress(
        &self,
        completed: u64,
        total: u64,
        unit: RolloutMaintenanceProgressUnit,
    ) {
        let mut activity = self.reporter.activity();
        activity.progress = Some(RolloutMaintenanceProgress {
            completed,
            total: Some(total),
            unit,
        });
        self.reporter.update(activity);
    }
}
