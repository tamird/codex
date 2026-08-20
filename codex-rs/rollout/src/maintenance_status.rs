//! Bounded, advisory status for the process that owns rollout maintenance.

use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::maintenance::MaintenanceFileLock;
use crate::maintenance_observer::RolloutMaintenanceRequestScope;

const STATUS_VERSION: u32 = 1;
const MAX_STATUS_BYTES: u64 = 4096;
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

/// The OS lock is authoritative. Older processes may own it without reporting details.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RolloutMaintenanceStatus {
    Idle,
    Busy {
        owner: Option<RolloutMaintenanceActivity>,
    },
}

/// Activity observed by one request or one local migration run.
///
/// `Running` describes work, not lock ownership: a migration may release maintenance before
/// rebuilding its projection. Use [`super::read_rollout_maintenance_status`] for lock ownership.
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

#[derive(Serialize, Deserialize)]
struct Snapshot {
    version: u32,
    generation: Uuid,
    owner: RolloutMaintenanceActivity,
}

struct Publication {
    directory: PathBuf,
    prefix: String,
    generation: Uuid,
    lease: Option<MaintenanceFileLock>,
}

struct ReporterState {
    activity: RolloutMaintenanceActivity,
    publication: Option<Publication>,
    last_published: Instant,
    transition: Option<RolloutMaintenanceRequestScope>,
}

/// A reporting handle cannot prolong the lifetime of either maintenance lock.
#[derive(Clone)]
pub struct RolloutMaintenanceReporter(Arc<Mutex<ReporterState>>);

impl RolloutMaintenanceReporter {
    /// Report work that does not currently own the global maintenance lock.
    pub fn new(activity: RolloutMaintenanceActivity) -> Self {
        Self(Arc::new(Mutex::new(ReporterState {
            activity,
            publication: None,
            last_published: Instant::now(),
            transition: None,
        })))
    }

    pub(super) fn start(
        directory: PathBuf,
        prefix: String,
        owner: RolloutMaintenanceActivity,
    ) -> Self {
        let owner = RolloutMaintenanceActivity {
            process_id: std::process::id(),
            ..owner
        };
        let mut state = ReporterState {
            activity: owner,
            publication: Some(Publication {
                directory,
                prefix,
                generation: Uuid::new_v4(),
                lease: None,
            }),
            last_published: Instant::now(),
            transition: Some(RolloutMaintenanceRequestScope::new(
                RolloutMaintenanceRequestStatus::Running { activity: owner },
            )),
        };
        state.publish();
        Self(Arc::new(Mutex::new(state)))
    }

    pub fn activity(&self) -> RolloutMaintenanceActivity {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = state.activity;
        update(&mut state.activity);
        let owner = state.activity;
        if previous == owner
            && state
                .publication
                .as_ref()
                .is_none_or(|publication| publication.lease.is_some())
        {
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
        state.publish();
        state.last_published = Instant::now();
        let status = RolloutMaintenanceRequestStatus::Running { activity: owner };
        match &state.transition {
            Some(transition) => transition.update(status),
            None => state.transition = Some(RolloutMaintenanceRequestScope::new(status)),
        }
    }

    pub(super) fn finish(&self) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Release before the global lock; stale JSON alone never proves a current owner.
        state.publication.take();
    }
}

impl ReporterState {
    fn publish(&mut self) {
        let Some(publication) = &mut self.publication else {
            return;
        };
        // Publish before taking the reporter lease. A reader's brief probe may contend with
        // acquisition; retry on the next permitted update without blocking maintenance.
        if write_snapshot(
            &publication.directory,
            &publication.prefix,
            publication.generation,
            self.activity,
        )
        .is_ok()
            && publication.lease.is_none()
        {
            publication.lease = open_lock(
                &publication
                    .directory
                    .join(format!("{}-reporter.lock", publication.prefix)),
            )
            .ok()
            .and_then(|file| file.try_lock().ok().map(|()| MaintenanceFileLock { file }));
        }
    }
}

pub(super) fn open_lock(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
}

fn write_snapshot(
    directory: &Path,
    prefix: &str,
    generation: Uuid,
    owner: RolloutMaintenanceActivity,
) -> io::Result<()> {
    let bytes = serde_json::to_vec(&Snapshot {
        version: STATUS_VERSION,
        generation,
        owner,
    })?;
    if bytes.len() as u64 > MAX_STATUS_BYTES {
        return Err(io::Error::other("rollout maintenance status is too large"));
    }
    let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
    temporary.write_all(&bytes)?;
    temporary
        .persist(directory.join(format!("{prefix}-status.json")))
        .map_err(|error| error.error)?;
    Ok(())
}

fn read_snapshot(directory: &Path, prefix: &str) -> Option<Snapshot> {
    let mut bytes = Vec::new();
    File::open(directory.join(format!("{prefix}-status.json")))
        .ok()?
        .take(MAX_STATUS_BYTES + 1)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() as u64 > MAX_STATUS_BYTES {
        return None;
    }
    let snapshot: Snapshot = serde_json::from_slice(&bytes).ok()?;
    (snapshot.version == STATUS_VERSION).then_some(snapshot)
}

pub(super) fn read_owner(directory: &Path, prefix: &str) -> Option<RolloutMaintenanceActivity> {
    let before = read_snapshot(directory, prefix)?;
    let lease = OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.join(format!("{prefix}-reporter.lock")))
        .ok()?;
    // Shared reader probes cannot impersonate the exclusive reporting owner.
    match lease.try_lock_shared() {
        Err(std::fs::TryLockError::WouldBlock) => {}
        Ok(()) => {
            let _probe = MaintenanceFileLock { file: lease };
            return None;
        }
        Err(std::fs::TryLockError::Error(_)) => return None,
    }
    let after = read_snapshot(directory, prefix)?;
    (before.generation == after.generation).then_some(after.owner)
}

#[cfg(test)]
#[path = "maintenance_status_tests.rs"]
mod tests;
