//! Bounded, request-local activity reporting for rollout maintenance.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::maintenance_observer::RolloutMaintenanceRequestScope;

const UPDATE_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RolloutMaintenanceOperation {
    ManualMigration,
    BackgroundMigration,
    Compression,
    HistoryRepair,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RolloutMaintenancePhase {
    Starting,
    Inventory,
    Planning,
    Validating,
    WaitingForWriter,
    Staging,
    Projecting,
    Publishing,
    Verifying,
    Recovering,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RolloutMaintenanceProgressUnit {
    Paths,
    Segments,
    Bytes,
}

/// A measured amount of work in the current phase, not an overall estimate.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RolloutMaintenanceProgress {
    pub completed: u64,
    pub total: Option<u64>,
    pub unit: RolloutMaintenanceProgressUnit,
}

/// Content-free identity and activity of one maintenance acquisition.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RolloutMaintenanceActivity {
    pub operation_id: Uuid,
    pub process_id: u32,
    pub operation: RolloutMaintenanceOperation,
    pub thread_id: Option<ThreadId>,
    pub phase: RolloutMaintenancePhase,
    pub progress: Option<RolloutMaintenanceProgress>,
    /// Observed bytes handled by migration, including repeated/decoded passes. This is not a
    /// physical-I/O total or a completion percentage; uninstrumented reads are not estimated.
    pub io_bytes: u64,
}

impl RolloutMaintenanceActivity {
    pub fn new(operation: RolloutMaintenanceOperation, thread_id: Option<ThreadId>) -> Self {
        Self {
            operation_id: Uuid::new_v4(),
            process_id: std::process::id(),
            operation,
            thread_id,
            phase: RolloutMaintenancePhase::Starting,
            progress: None,
            io_bytes: 0,
        }
    }
}

/// Activity observed by one request or one local migration run.
///
/// `Running` describes work, not lock ownership: a migration may release maintenance before
/// rebuilding its projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RolloutMaintenanceRequestStatus {
    Idle,
    QueuedForMigration {
        thread_id: ThreadId,
    },
    WaitingForMaintenance {
        thread_id: Option<ThreadId>,
        owner: Option<RolloutMaintenanceActivity>,
    },
    Running {
        activity: RolloutMaintenanceActivity,
    },
}

struct ReporterState {
    activity: RolloutMaintenanceActivity,
    last_published: Instant,
    transition: Option<RolloutMaintenanceRequestScope>,
}

/// A reporting handle cannot prolong the lifetime of the maintenance lock.
#[derive(Clone)]
pub struct RolloutMaintenanceReporter(Arc<Mutex<ReporterState>>);

impl RolloutMaintenanceReporter {
    /// Report work that does not currently own the global maintenance lock.
    pub fn new(activity: RolloutMaintenanceActivity) -> Self {
        Self(Arc::new(Mutex::new(ReporterState {
            activity,
            last_published: Instant::now(),
            transition: None,
        })))
    }

    pub(super) fn start(owner: RolloutMaintenanceActivity) -> Self {
        let owner = RolloutMaintenanceActivity {
            process_id: std::process::id(),
            ..owner
        };
        Self(Arc::new(Mutex::new(ReporterState {
            activity: owner,
            last_published: Instant::now(),
            transition: Some(RolloutMaintenanceRequestScope::new(
                RolloutMaintenanceRequestStatus::Running { activity: owner },
            )),
        })))
    }

    pub fn activity(&self) -> RolloutMaintenanceActivity {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .activity
    }

    /// Publish changed phases immediately and coalesce numeric progress updates.
    pub fn update(&self, owner: RolloutMaintenanceActivity) {
        self.update_inner(|activity| {
            *activity = RolloutMaintenanceActivity {
                operation_id: activity.operation_id,
                process_id: activity.process_id,
                operation: activity.operation,
                io_bytes: activity.io_bytes.max(owner.io_bytes),
                ..owner
            };
        });
    }

    /// Count bytes at an existing streaming boundary without changing migration throttling.
    pub fn observe_io(&self, bytes: u64) {
        self.update_inner(|activity| {
            activity.io_bytes = activity.io_bytes.saturating_add(bytes);
        });
    }

    fn update_inner(&self, update: impl FnOnce(&mut RolloutMaintenanceActivity)) {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let previous = state.activity;
        update(&mut state.activity);
        let owner = state.activity;
        if previous == owner {
            return;
        }
        let changed_phase = previous.phase != owner.phase || previous.thread_id != owner.thread_id;
        let complete = previous.progress != owner.progress
            && owner
                .progress
                .is_some_and(|progress| progress.total == Some(progress.completed));
        if !changed_phase && !complete && state.last_published.elapsed() < UPDATE_INTERVAL {
            return;
        }
        state.last_published = Instant::now();
        let status = RolloutMaintenanceRequestStatus::Running { activity: owner };
        match &state.transition {
            Some(transition) => transition.update(status),
            None => state.transition = Some(RolloutMaintenanceRequestScope::new(status)),
        }
    }
}
