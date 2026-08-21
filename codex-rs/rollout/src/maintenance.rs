//! Coordinates maintenance jobs that replace local rollout files.
//!
//! Migrations with reserved dependencies may overlap. Unknown ancestry and older migration
//! implementations retain exclusive job ownership. Clean history readers share the original
//! maintenance lock; old binaries and history repair acquire it exclusively. Compression yields
//! when a reader or migration is waiting.
//!
//! This is separate from per-thread writer locks, which protect live rollout appenders. It is also
//! separate from compression's durable run marker, which throttles how often compression scans.

use codex_protocol::ThreadId;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::sync::Arc;

const ROLLOUT_MAINTENANCE_LOCK: &str = "rollout-maintenance.lock";
const ROLLOUT_MAINTENANCE_JOB_LOCK: &str = "rollout-maintenance-job.lock";
const ROLLOUT_MAINTENANCE_FOREGROUND_LOCK: &str = "rollout-maintenance-foreground.lock";

/// Bounds migration memory and SQLite contention across processes sharing one Codex home.
pub const MAX_CONCURRENT_ROLLOUT_MIGRATIONS: usize = 2;

enum LockMode {
    Shared,
    Exclusive,
}

/// Owns a successful lock, including temporary and partially acquired reservations.
struct MaintenanceFileLock {
    file: File,
}

impl Drop for MaintenanceFileLock {
    fn drop(&mut self) {
        // Closing this descriptor alone leaves flock ownership with a dup or pre-exec child.
        if let Err(error) = self.file.unlock() {
            tracing::warn!("failed to release rollout maintenance lock: {error}");
        }
    }
}

/// Keeps compression aware of queued migrations even between nonblocking lock attempts.
pub struct RolloutMaintenanceIntentGuard {
    _file: Arc<MaintenanceFileLock>,
}

/// Holds exclusive ownership of operations that replace local rollout files.
pub struct RolloutMaintenanceGuard {
    _file: MaintenanceFileLock,
}

/// Excludes old maintenance implementations and exclusive history repair, but permits clean
/// readers and new maintenance jobs to coexist. Per-thread writer locks protect mutable files.
pub struct RolloutMaintenanceReadGuard {
    _file: MaintenanceFileLock,
}

/// Reserves a migration against compression and incompatible maintenance implementations.
pub struct RolloutMaintenanceJobGuard {
    _job: MaintenanceFileLock,
    _compatibility: RolloutMaintenanceReadGuard,
    _foreground: Arc<MaintenanceFileLock>,
    /// Stable lock files are never unlinked: otherwise another process could lock a new inode.
    _dependencies: Vec<MaintenanceFileLock>,
    _slot: Option<MaintenanceFileLock>,
}

pub async fn acquire_rollout_maintenance_intent(
    codex_home: &Path,
) -> io::Result<RolloutMaintenanceIntentGuard> {
    Ok(RolloutMaintenanceIntentGuard {
        _file: acquire_foreground_intent(codex_home).await?,
    })
}

/// Try to admit a migration whose complete dependency identities the caller will revalidate.
/// Busy dependencies or capacity return immediately and release every partial reservation.
pub fn try_acquire_rollout_migration_dependency_lock(
    codex_home: &Path,
    thread_ids: &[ThreadId],
) -> io::Result<Option<RolloutMaintenanceJobGuard>> {
    if thread_ids.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty migration dependency set",
        ));
    }
    let Some(foreground) = try_open_lock(
        codex_home,
        ROLLOUT_MAINTENANCE_FOREGROUND_LOCK,
        LockMode::Shared,
    )?
    else {
        return Ok(None);
    };
    let Some(job) = try_open_lock(codex_home, ROLLOUT_MAINTENANCE_JOB_LOCK, LockMode::Shared)?
    else {
        return Ok(None);
    };
    let Some(compatibility) = try_acquire_rollout_maintenance_read_lock(codex_home)? else {
        return Ok(None);
    };
    let mut ids = thread_ids.to_vec();
    ids.sort_unstable_by_key(ThreadId::to_string);
    ids.dedup();
    let mut dependencies = Vec::with_capacity(ids.len());
    for thread_id in ids {
        let Some(file) = try_open_lock(
            codex_home,
            &format!("rollout-migration-{thread_id}.lock"),
            LockMode::Exclusive,
        )?
        else {
            return Ok(None);
        };
        dependencies.push(file);
    }
    for slot in 0..MAX_CONCURRENT_ROLLOUT_MIGRATIONS {
        if let Some(file) = try_open_lock(
            codex_home,
            &format!("rollout-migration-slot-{slot}.lock"),
            LockMode::Exclusive,
        )? {
            return Ok(Some(RolloutMaintenanceJobGuard {
                _job: job,
                _compatibility: compatibility,
                _foreground: Arc::new(foreground),
                _dependencies: dependencies,
                _slot: Some(file),
            }));
        }
    }
    Ok(None)
}

/// Retains compression's original exclusive protocol while foreground waiters ask it to stop.
pub(crate) struct RolloutCompressionMaintenanceGuard {
    _job: MaintenanceFileLock,
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
    let lock = try_open_lock(
        home,
        ROLLOUT_MAINTENANCE_FOREGROUND_LOCK,
        LockMode::Exclusive,
    )?;
    Ok(lock.is_none())
}

pub(crate) fn try_acquire_compression_maintenance(
    codex_home: &Path,
) -> io::Result<Option<RolloutCompressionMaintenanceGuard>> {
    let Some(_priority) = try_open_lock(
        codex_home,
        ROLLOUT_MAINTENANCE_FOREGROUND_LOCK,
        LockMode::Exclusive,
    )?
    else {
        return Ok(None);
    };
    let Some(job) = try_open_lock(
        codex_home,
        ROLLOUT_MAINTENANCE_JOB_LOCK,
        LockMode::Exclusive,
    )?
    else {
        return Ok(None);
    };
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

fn try_open_lock(
    codex_home: &Path,
    name: &str,
    mode: LockMode,
) -> io::Result<Option<MaintenanceFileLock>> {
    let directory = codex_home.join(".tmp");
    fs::create_dir_all(&directory)?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(directory.join(name))?;
    let acquired = match mode {
        LockMode::Shared => file.try_lock_shared(),
        LockMode::Exclusive => file.try_lock(),
    };
    match acquired {
        Ok(()) => Ok(Some(MaintenanceFileLock { file })),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(error)) => Err(error),
    }
}

/// Try to reserve clean history access against old maintenance and exclusive repair.
pub fn try_acquire_rollout_maintenance_read_lock(
    codex_home: &Path,
) -> io::Result<Option<RolloutMaintenanceReadGuard>> {
    let lock = try_open_lock(codex_home, ROLLOUT_MAINTENANCE_LOCK, LockMode::Shared)?;
    Ok(lock.map(|file| RolloutMaintenanceReadGuard { _file: file }))
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
    let Some(foreground) = try_open_lock(
        codex_home,
        ROLLOUT_MAINTENANCE_FOREGROUND_LOCK,
        LockMode::Shared,
    )?
    else {
        return Ok(None);
    };
    try_acquire_foreground_job(codex_home, Arc::new(foreground))
}

fn try_acquire_foreground_job(
    codex_home: &Path,
    foreground: Arc<MaintenanceFileLock>,
) -> io::Result<Option<RolloutMaintenanceJobGuard>> {
    let Some(job) = try_open_lock(
        codex_home,
        ROLLOUT_MAINTENANCE_JOB_LOCK,
        LockMode::Exclusive,
    )?
    else {
        return Ok(None);
    };
    let Some(compatibility) = try_acquire_rollout_maintenance_read_lock(codex_home)? else {
        return Ok(None);
    };
    Ok(Some(RolloutMaintenanceJobGuard {
        _job: job,
        _compatibility: compatibility,
        _foreground: foreground,
        _dependencies: Vec::new(),
        _slot: None,
    }))
}

/// Wait for a migration job while retaining foreground priority over compression.
pub async fn acquire_rollout_maintenance_job_lock(
    codex_home: &Path,
) -> io::Result<RolloutMaintenanceJobGuard> {
    let foreground = acquire_foreground_intent(codex_home).await?;
    loop {
        if let Some(guard) = try_acquire_foreground_job(codex_home, Arc::clone(&foreground))? {
            return Ok(guard);
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

async fn acquire_foreground_intent(codex_home: &Path) -> io::Result<Arc<MaintenanceFileLock>> {
    loop {
        if let Some(file) = try_open_lock(
            codex_home,
            ROLLOUT_MAINTENANCE_FOREGROUND_LOCK,
            LockMode::Shared,
        )? {
            return Ok(Arc::new(file));
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// Try to exclude rollout compression and migration for one Codex home.
pub fn try_acquire_rollout_maintenance_lock(
    codex_home: &Path,
) -> io::Result<Option<RolloutMaintenanceGuard>> {
    let lock = try_open_lock(codex_home, ROLLOUT_MAINTENANCE_LOCK, LockMode::Exclusive)?;
    Ok(lock.map(|file| RolloutMaintenanceGuard { _file: file }))
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
