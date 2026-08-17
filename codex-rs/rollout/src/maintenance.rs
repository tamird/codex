//! Coordinates maintenance jobs that replace local rollout files.
//!
//! New migration jobs serialize with each other while clean history readers retain shared
//! ownership of the original maintenance lock. Old binaries and history repair still acquire
//! that original lock exclusively. Compression yields when a reader or migration is waiting.
//!
//! This is separate from per-thread writer locks, which protect live rollout appenders. It is also
//! separate from compression's durable run marker, which throttles how often compression scans.

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;

const ROLLOUT_MAINTENANCE_LOCK: &str = "rollout-maintenance.lock";
const ROLLOUT_MAINTENANCE_JOB_LOCK: &str = "rollout-maintenance-job.lock";
const ROLLOUT_MAINTENANCE_FOREGROUND_LOCK: &str = "rollout-maintenance-foreground.lock";

/// Holds exclusive ownership of operations that replace local rollout files.
pub struct RolloutMaintenanceGuard {
    _file: File,
}

/// Excludes old maintenance implementations and exclusive history repair, but permits clean
/// readers and new maintenance jobs to coexist. Per-thread writer locks protect mutable files.
pub struct RolloutMaintenanceReadGuard {
    _file: File,
}

/// Serializes new migration and compression jobs without excluding unrelated clean readers.
pub struct RolloutMaintenanceJobGuard {
    _job: File,
    _compatibility: RolloutMaintenanceReadGuard,
    _foreground: File,
}

/// Retains compression's original exclusive protocol while foreground waiters ask it to stop.
pub(crate) struct RolloutCompressionMaintenanceGuard {
    _job: File,
    _compatibility: RolloutMaintenanceGuard,
    home: std::path::PathBuf,
    /// Once interrupted, this run must not publish or persist its six-hour marker.
    yielded: std::sync::atomic::AtomicBool,
    /// Prevents this worker's concurrent compression jobs from mistaking each other for waiters.
    foreground_check: std::sync::Mutex<()>,
}

impl RolloutCompressionMaintenanceGuard {
    pub(crate) fn should_yield(&self) -> io::Result<bool> {
        use std::sync::atomic::Ordering;
        if self.yielded.load(Ordering::Acquire) {
            return Ok(true);
        }
        let _check = self
            .foreground_check
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        let waiting = foreground_maintenance_waiting(&self.home)?;
        if waiting {
            self.yielded.store(true, Ordering::Release);
        }
        Ok(waiting)
    }
}

pub(crate) fn foreground_maintenance_waiting(home: &Path) -> io::Result<bool> {
    let file = open_lock(home, ROLLOUT_MAINTENANCE_FOREGROUND_LOCK)?;
    match file.try_lock() {
        Ok(()) => Ok(false),
        Err(std::fs::TryLockError::WouldBlock) => Ok(true),
        Err(std::fs::TryLockError::Error(error)) => Err(error),
    }
}

pub(crate) fn try_acquire_compression_maintenance(
    codex_home: &Path,
) -> io::Result<Option<RolloutCompressionMaintenanceGuard>> {
    let priority = open_lock(codex_home, ROLLOUT_MAINTENANCE_FOREGROUND_LOCK)?;
    match priority.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => return Ok(None),
        Err(std::fs::TryLockError::Error(error)) => return Err(error),
    }
    let job = open_lock(codex_home, ROLLOUT_MAINTENANCE_JOB_LOCK)?;
    match job.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => return Ok(None),
        Err(std::fs::TryLockError::Error(error)) => return Err(error),
    }
    let Some(compatibility) = try_acquire_rollout_maintenance_lock(codex_home)? else {
        return Ok(None);
    };
    Ok(Some(RolloutCompressionMaintenanceGuard {
        _job: job,
        _compatibility: compatibility,
        home: codex_home.to_path_buf(),
        yielded: std::sync::atomic::AtomicBool::new(false),
        foreground_check: std::sync::Mutex::new(()),
    }))
}

fn open_lock(codex_home: &Path, name: &str) -> io::Result<File> {
    let directory = codex_home.join(".tmp");
    fs::create_dir_all(&directory)?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(directory.join(name))
}

/// Try to reserve clean history access against old maintenance and exclusive repair.
pub fn try_acquire_rollout_maintenance_read_lock(
    codex_home: &Path,
) -> io::Result<Option<RolloutMaintenanceReadGuard>> {
    let file = open_lock(codex_home, ROLLOUT_MAINTENANCE_LOCK)?;
    match file.try_lock_shared() {
        Ok(()) => Ok(Some(RolloutMaintenanceReadGuard { _file: file })),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(error)) => Err(error),
    }
}

/// Wait for clean history access, asking interruptible compression to yield.
pub async fn acquire_rollout_maintenance_read_lock(
    codex_home: &Path,
) -> io::Result<RolloutMaintenanceReadGuard> {
    let _foreground = acquire_foreground_intent(codex_home).await?;
    loop {
        if let Some(guard) = try_acquire_rollout_maintenance_read_lock(codex_home)? {
            return Ok(guard);
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// Try to serialize a migration without excluding unrelated clean readers.
pub fn try_acquire_rollout_maintenance_job_lock(
    codex_home: &Path,
) -> io::Result<Option<RolloutMaintenanceJobGuard>> {
    let foreground = open_lock(codex_home, ROLLOUT_MAINTENANCE_FOREGROUND_LOCK)?;
    match foreground.try_lock_shared() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => return Ok(None),
        Err(std::fs::TryLockError::Error(error)) => return Err(error),
    }
    try_acquire_foreground_job(codex_home, foreground)
}

fn try_acquire_foreground_job(
    codex_home: &Path,
    foreground: File,
) -> io::Result<Option<RolloutMaintenanceJobGuard>> {
    let job = open_lock(codex_home, ROLLOUT_MAINTENANCE_JOB_LOCK)?;
    match job.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => return Ok(None),
        Err(std::fs::TryLockError::Error(error)) => return Err(error),
    }
    let Some(compatibility) = try_acquire_rollout_maintenance_read_lock(codex_home)? else {
        return Ok(None);
    };
    Ok(Some(RolloutMaintenanceJobGuard {
        _job: job,
        _compatibility: compatibility,
        _foreground: foreground,
    }))
}

/// Wait for a migration job while retaining foreground priority over compression.
pub async fn acquire_rollout_maintenance_job_lock(
    codex_home: &Path,
) -> io::Result<RolloutMaintenanceJobGuard> {
    let foreground = acquire_foreground_intent(codex_home).await?;
    loop {
        if let Some(guard) = try_acquire_foreground_job(codex_home, foreground.try_clone()?)? {
            return Ok(guard);
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

async fn acquire_foreground_intent(codex_home: &Path) -> io::Result<File> {
    loop {
        let file = open_lock(codex_home, ROLLOUT_MAINTENANCE_FOREGROUND_LOCK)?;
        match file.try_lock_shared() {
            Ok(()) => return Ok(file),
            Err(std::fs::TryLockError::WouldBlock) => {}
            Err(std::fs::TryLockError::Error(error)) => return Err(error),
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// Try to exclude rollout compression and migration for one Codex home.
pub fn try_acquire_rollout_maintenance_lock(
    codex_home: &Path,
) -> io::Result<Option<RolloutMaintenanceGuard>> {
    let file = open_lock(codex_home, ROLLOUT_MAINTENANCE_LOCK)?;

    match file.try_lock() {
        Ok(()) => Ok(Some(RolloutMaintenanceGuard { _file: file })),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(error)) => Err(error),
    }
}

#[cfg(test)]
#[path = "maintenance_tests.rs"]
mod tests;

/// Wait for exclusive ownership of operations that replace local rollout files.
///
/// The operating system releases the file lock if its owning process exits. Callers that require
/// maintenance to finish can cancel this future instead of translating ordinary contention into
/// an unrecoverable user-visible error.
pub async fn acquire_rollout_maintenance_lock(
    codex_home: &Path,
) -> io::Result<RolloutMaintenanceGuard> {
    let _foreground = acquire_foreground_intent(codex_home).await?;
    let mut delay = std::time::Duration::from_millis(25);
    loop {
        if let Some(guard) = try_acquire_rollout_maintenance_lock(codex_home)? {
            return Ok(guard);
        }
        tokio::time::sleep(delay).await;
        delay = delay
            .saturating_mul(2)
            .min(std::time::Duration::from_millis(500));
    }
}
