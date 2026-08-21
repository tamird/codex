//! Cross-process ownership of mutable rollout files.
//!
//! Compression and thread-store use the same coordination lock when opening or removing a
//! per-thread lock file. Without that coordination, unlinking a released file could let two
//! processes lock different inodes for the same thread.

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use codex_protocol::ThreadId;
use tracing::warn;

const WRITER_LOCK_DIR: &str = "thread-writer-locks";
const COORDINATION_LOCK_FILE: &str = ".coordination.lock";

/// Acquires per-thread writer locks and removes stale lock files on first use.
#[derive(Debug)]
pub struct RolloutWriterLockCoordinator {
    directory: PathBuf,
    cleanup_attempted: AtomicBool,
}

/// Cross-process ownership of one thread's mutable rollout files or frozen fork history.
#[derive(Debug)]
pub struct RolloutWriterLockGuard {
    coordinator: Arc<RolloutWriterLockCoordinator>,
    path: PathBuf,
    state: WriterLockState,
}

/// A shared lock excludes new writers while the existing owner admits immutable fork readers.
#[derive(Debug)]
enum WriterLockState {
    Exclusive(File),
    Shared(File),
    /// A failed lock conversion must not leave the recorder authorized to write.
    Unreserved,
}

impl RolloutWriterLockCoordinator {
    pub fn new(codex_home: &Path) -> Self {
        Self {
            directory: codex_home.join(WRITER_LOCK_DIR),
            cleanup_attempted: AtomicBool::new(false),
        }
    }

    /// Returns `None` when another process already owns this thread.
    pub fn try_acquire(
        self: &Arc<Self>,
        thread_id: ThreadId,
    ) -> io::Result<Option<RolloutWriterLockGuard>> {
        let coordination_lock = self.lock_coordination()?;
        if !self.cleanup_attempted.swap(true, Ordering::Relaxed)
            && let Err(err) = self.remove_stale_thread_locks()
        {
            warn!("failed to clean up stale thread writer locks: {err}");
        }

        let path = self.directory.join(format!("{thread_id}.lock"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!(
                        "failed to open thread writer lock {}: {err}",
                        path.display()
                    ),
                )
            })?;

        match file.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Ok(None);
            }
            Err(std::fs::TryLockError::Error(err)) => {
                return Err(io::Error::new(
                    err.kind(),
                    format!(
                        "failed to acquire thread writer lock {}: {err}",
                        path.display()
                    ),
                ));
            }
        }

        drop(coordination_lock);
        Ok(Some(RolloutWriterLockGuard {
            coordinator: Arc::clone(self),
            path,
            state: WriterLockState::Exclusive(file),
        }))
    }

    /// Reserves immutable fork history without receiving writer authority. Sharing the existing
    /// lock inode also excludes compression and older binaries' deletion operations.
    pub fn try_acquire_fork_reader(
        self: &Arc<Self>,
        thread_id: ThreadId,
    ) -> io::Result<Option<RolloutWriterLockGuard>> {
        let _coordination = self.lock_coordination()?;
        let path = self.directory.join(format!("{thread_id}.lock"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        match file.try_lock_shared() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Ok(None),
            Err(std::fs::TryLockError::Error(error)) => return Err(error),
        }
        Ok(Some(RolloutWriterLockGuard {
            coordinator: Arc::clone(self),
            path,
            state: WriterLockState::Shared(file),
        }))
    }

    fn lock_coordination(&self) -> io::Result<File> {
        fs::create_dir_all(&self.directory).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "failed to create thread writer lock directory {}: {err}",
                    self.directory.display()
                ),
            )
        })?;
        let path = self.directory.join(COORDINATION_LOCK_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|err| {
                io::Error::new(
                    err.kind(),
                    format!(
                        "failed to open thread writer coordination lock {}: {err}",
                        path.display()
                    ),
                )
            })?;
        file.lock().map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "failed to acquire thread writer coordination lock {}: {err}",
                    path.display()
                ),
            )
        })?;
        Ok(file)
    }

    fn remove_stale_thread_locks(&self) -> io::Result<()> {
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some(thread_id) = file_name.strip_suffix(".lock") else {
                continue;
            };
            if ThreadId::from_string(thread_id).is_err() {
                continue;
            }

            let path = entry.path();
            let file = match OpenOptions::new().read(true).write(true).open(&path) {
                Ok(file) => file,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => {
                    warn!(
                        "failed to inspect thread writer lock {}: {err}",
                        path.display()
                    );
                    continue;
                }
            };
            match file.try_lock() {
                Ok(()) => {
                    drop(file);
                    if let Err(err) = fs::remove_file(&path)
                        && err.kind() != io::ErrorKind::NotFound
                    {
                        warn!(
                            "failed to remove stale thread writer lock {}: {err}",
                            path.display()
                        );
                    }
                }
                Err(std::fs::TryLockError::WouldBlock) => {}
                Err(std::fs::TryLockError::Error(err)) => {
                    warn!(
                        "failed to inspect thread writer lock {}: {err}",
                        path.display()
                    );
                }
            }
        }
        Ok(())
    }
}

impl RolloutWriterLockGuard {
    /// Retains the sole logical writer, but admits read-only fork lifetime reservations.
    /// The coordination lock excludes acquisition and cleanup across the explicit unlock/relock;
    /// converting an already-locked handle is not portable.
    pub fn share_for_fork(&mut self) -> io::Result<()> {
        let _coordination = self.coordinator.lock_coordination()?;
        match std::mem::replace(&mut self.state, WriterLockState::Unreserved) {
            WriterLockState::Exclusive(file) => {
                file.unlock()?;
                file.try_lock_shared().map_err(io::Error::from)?;
                self.state = WriterLockState::Shared(file);
                Ok(())
            }
            WriterLockState::Shared(file) => {
                self.state = WriterLockState::Shared(file);
                Ok(())
            }
            WriterLockState::Unreserved => Err(io::Error::other("writer lock is closed")),
        }
    }

    /// Destruction requires exclusive ownership, unlike appending to the mutable source tail.
    /// Returns WouldBlock if an initializing fork still holds a reader reservation.
    pub fn require_exclusive(&mut self) -> io::Result<()> {
        let _coordination = self.coordinator.lock_coordination()?;
        match std::mem::replace(&mut self.state, WriterLockState::Unreserved) {
            WriterLockState::Exclusive(file) => {
                self.state = WriterLockState::Exclusive(file);
                Ok(())
            }
            WriterLockState::Shared(file) => {
                file.unlock()?;
                match file.try_lock() {
                    Ok(()) => {
                        self.state = WriterLockState::Exclusive(file);
                        Ok(())
                    }
                    Err(error) => {
                        // Restore exclusion before returning. Failed restoration leaves an
                        // unreserved guard so the caller can fence its drained recorder.
                        file.try_lock_shared().map_err(io::Error::from)?;
                        self.state = WriterLockState::Shared(file);
                        Err(error.into())
                    }
                }
            }
            WriterLockState::Unreserved => Err(io::Error::other("writer lock is closed")),
        }
    }

    /// Callers must stop using their recorder when a failed conversion loses its reservation.
    pub fn is_reserved(&self) -> bool {
        !matches!(self.state, WriterLockState::Unreserved)
    }
}

impl Drop for RolloutWriterLockGuard {
    fn drop(&mut self) {
        let coordination_lock = match self.coordinator.lock_coordination() {
            Ok(lock) => lock,
            Err(err) => {
                warn!("failed to coordinate thread writer lock cleanup: {err}");
                return;
            }
        };

        // Close the writer lock before deleting it so cleanup works on Windows too.
        drop(std::mem::replace(
            &mut self.state,
            WriterLockState::Unreserved,
        ));
        // An imported fork may outlive the source process. Keep its inode so compression and
        // subsequent writers cannot acquire a different lock file for the same thread.
        match OpenOptions::new().read(true).write(true).open(&self.path) {
            Ok(file) => match file.try_lock() {
                Ok(()) => drop(file),
                Err(std::fs::TryLockError::WouldBlock) => return,
                Err(std::fs::TryLockError::Error(error)) => {
                    warn!("failed to inspect writer lock during cleanup: {error}");
                    return;
                }
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => return,
            Err(error) => {
                warn!("failed to inspect writer lock during cleanup: {error}");
                return;
            }
        }
        if let Err(err) = fs::remove_file(&self.path)
            && err.kind() != io::ErrorKind::NotFound
        {
            warn!(
                "failed to remove thread writer lock {}: {err}",
                self.path.display()
            );
        }
        drop(coordination_lock);
    }
}
