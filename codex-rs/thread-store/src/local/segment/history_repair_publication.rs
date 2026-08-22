//! Publishes rollout history repairs with the existing rollout segment representation.
//!
//! Repair installs immutable descendants before replacing a mutable root. Each install and the
//! final root replacement is independently durable, so a crash leaves either the old graph or a
//! graph that a deterministic retry can finish. No repair journal or backup format is required.

use std::io;
use std::io::Read as _;
use std::path::Path;
use std::path::PathBuf;
#[cfg(test)]
use std::sync::LazyLock;
#[cfg(test)]
use std::sync::Mutex;

use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use serde::Deserialize;
use serde_json::value::RawValue;
use sha2::Digest as _;
use sha2::Sha256;
use tokio::fs;
use tokio::sync::OwnedRwLockReadGuard;

use super::super::LocalThreadStore;
use super::super::RolloutWriterReservation;
use super::confined_publication::ConfinedInstallOutcome;
use super::confined_publication::ConfinedMutationOutcome;
use super::confined_publication::ConfinedRootIdentity;
use super::confined_publication::confined_entry_exists_under_root;
use super::confined_publication::confined_root_identity;
use super::confined_publication::confined_staged_entries_exist_under_root;
use super::confined_publication::ensure_confined_directory_under_root;
use super::confined_publication::install_confined_file_under_root;
use super::confined_publication::read_confined_file_under_root;
use super::confined_publication::replace_confined_file_under_root;
use super::immutable_segment_parent;
use super::immutable_segment_path;
use super::thread_store_io_error;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

#[cfg(test)]
static PRECOMMIT_FAILURES: LazyLock<Mutex<std::collections::HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(std::collections::HashSet::new()));
#[cfg(test)]
static POSTCOMMIT_SYNC_FAILURES: LazyLock<Mutex<std::collections::HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(std::collections::HashSet::new()));

#[cfg(test)]
pub(crate) fn inject_history_repair_postcommit_sync_failure(path: PathBuf) {
    POSTCOMMIT_SYNC_FAILURES
        .lock()
        .expect("postcommit sync failure mutex")
        .insert(std::fs::canonicalize(path).expect("resolve postcommit injection path"));
}
#[cfg(test)]
static SOURCE_REPLACEMENTS: LazyLock<Mutex<std::collections::HashMap<PathBuf, Vec<u8>>>> =
    LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));
#[cfg(test)]
static PUBLISHED_REPLACEMENTS: LazyLock<Mutex<std::collections::HashMap<PathBuf, Vec<u8>>>> =
    LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));
#[cfg(all(test, unix))]
static CODEX_HOME_RETARGETS: LazyLock<Mutex<std::collections::HashMap<PathBuf, PathBuf>>> =
    LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));

/// Durability result after an active rollout replacement becomes visible.
#[derive(Debug)]
pub(crate) enum HistoryRepairPublication {
    /// The replacement file and its parent directory acknowledged synchronization.
    Durable,
    /// The replacement is visible, but storage did not acknowledge complete durability.
    DurabilityUnknown { error: ThreadStoreError },
}

/// Proof that the caller excludes every writer that can mutate one repair root.
///
/// Only thread-store runtime code holding the rollout-maintenance lock, the thread lifecycle
/// lease, and the in-process/cross-process writer reservation can mint this token. Keeping those
/// borrows in the token prevents publication after any exclusion has been released.
pub(crate) struct HistoryRepairWriterToken<'a> {
    thread_id: ThreadId,
    canonical_home: PathBuf,
    root_identity: ConfinedRootIdentity,
    _maintenance: &'a HistoryRepairMaintenanceLease,
    _lifecycle: &'a HistoryRepairLifecycleLease,
    _writers: &'a RolloutWriterReservation,
}

/// Thread-bound lifecycle lease used by history repair publication.
pub(crate) struct HistoryRepairLifecycleLease {
    store_identity: usize,
    thread_id: ThreadId,
    _guard: OwnedRwLockReadGuard<()>,
}

impl HistoryRepairLifecycleLease {
    pub(crate) fn thread_id(&self) -> ThreadId {
        self.thread_id
    }

    pub(crate) fn into_guard(self) -> OwnedRwLockReadGuard<()> {
        self._guard
    }
}

/// Store- and root-bound ownership of the rollout-maintenance lock.
pub(crate) struct HistoryRepairMaintenanceLease {
    store_identity: usize,
    canonical_home: PathBuf,
    root_identity: ConfinedRootIdentity,
    _guard: codex_rollout::RolloutMaintenanceGuard,
}

#[cfg(test)]
pub(crate) async fn reserve_history_repair_maintenance(
    store: &LocalThreadStore,
) -> ThreadStoreResult<Option<HistoryRepairMaintenanceLease>> {
    let canonical_home = fs::canonicalize(store.config.codex_home.as_path())
        .await
        .map_err(thread_store_io_error)?;
    let root_identity = confined_root_identity(canonical_home.as_path())
        .await
        .map_err(thread_store_io_error)?;
    let guard = codex_rollout::try_acquire_rollout_maintenance_lock(canonical_home.as_path())
        .map_err(thread_store_io_error)?;
    Ok(guard.map(|guard| HistoryRepairMaintenanceLease {
        store_identity: store_identity(store),
        root_identity,
        canonical_home,
        _guard: guard,
    }))
}

pub(crate) async fn acquire_history_repair_maintenance(
    store: &LocalThreadStore,
) -> ThreadStoreResult<HistoryRepairMaintenanceLease> {
    let canonical_home = fs::canonicalize(store.config.codex_home.as_path())
        .await
        .map_err(thread_store_io_error)?;
    let root_identity = confined_root_identity(canonical_home.as_path())
        .await
        .map_err(thread_store_io_error)?;
    let guard = codex_rollout::acquire_rollout_maintenance_lock(canonical_home.as_path())
        .await
        .map_err(thread_store_io_error)?;
    Ok(HistoryRepairMaintenanceLease {
        store_identity: store_identity(store),
        root_identity,
        canonical_home,
        _guard: guard,
    })
}

pub(crate) async fn reserve_history_repair_lifecycle(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> HistoryRepairLifecycleLease {
    HistoryRepairLifecycleLease {
        store_identity: store_identity(store),
        thread_id,
        _guard: store.live_writer_locks.reserve_lifecycle(thread_id).await,
    }
}

pub(crate) async fn authorize_history_repair_writer<'a>(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    maintenance: &'a HistoryRepairMaintenanceLease,
    lifecycle: &'a HistoryRepairLifecycleLease,
    writers: &'a RolloutWriterReservation,
) -> ThreadStoreResult<HistoryRepairWriterToken<'a>> {
    let store_identity = store_identity(store);
    let canonical_home = fs::canonicalize(store.config.codex_home.as_path())
        .await
        .map_err(thread_store_io_error)?;
    if lifecycle.thread_id != thread_id
        || !writers.contains(thread_id)
        || maintenance.store_identity != store_identity
        || lifecycle.store_identity != store_identity
        || writers.store_identity != store_identity
        || maintenance.canonical_home != canonical_home
    {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "history repair does not own every lock for thread {thread_id} in {}",
                canonical_home.display()
            ),
        });
    }
    Ok(HistoryRepairWriterToken {
        thread_id,
        canonical_home,
        root_identity: maintenance.root_identity.clone(),
        _maintenance: maintenance,
        _lifecycle: lifecycle,
        _writers: writers,
    })
}

fn store_identity(store: &LocalThreadStore) -> usize {
    std::sync::Arc::as_ptr(&store.live_writer_locks) as usize
}

impl HistoryRepairWriterToken<'_> {
    fn require_thread(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        if self.thread_id == thread_id {
            Ok(())
        } else {
            Err(ThreadStoreError::Conflict {
                message: format!(
                    "history repair writer for thread {} cannot publish thread {thread_id}",
                    self.thread_id
                ),
            })
        }
    }

    fn require_home(&self, canonical_home: &Path) -> ThreadStoreResult<()> {
        if self.canonical_home == canonical_home {
            Ok(())
        } else {
            Err(ThreadStoreError::Conflict {
                message: "history repair writer is bound to a different CODEX_HOME".to_string(),
            })
        }
    }

    fn require_root(&self, root: &ConfinedRootIdentity) -> ThreadStoreResult<()> {
        if &self.root_identity == root {
            Ok(())
        } else {
            Err(ThreadStoreError::Conflict {
                message: "CODEX_HOME changed after history repair locks were acquired".to_string(),
            })
        }
    }
}

#[cfg(test)]
pub(super) struct TestHistoryRepairWriterToken {
    thread_id: ThreadId,
}

#[cfg(test)]
impl TestHistoryRepairWriterToken {
    fn require_thread(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        if self.thread_id == thread_id {
            Ok(())
        } else {
            Err(ThreadStoreError::Conflict {
                message: "test history repair writer owns a different thread".to_string(),
            })
        }
    }

    fn require_home(&self, _canonical_home: &Path) -> ThreadStoreResult<()> {
        Ok(())
    }

    fn require_root(&self, _root: &ConfinedRootIdentity) -> ThreadStoreResult<()> {
        Ok(())
    }
}

pub(crate) trait HistoryRepairWriterAuthorization {
    fn require_thread(&self, thread_id: ThreadId) -> ThreadStoreResult<()>;
    fn require_home(&self, canonical_home: &Path) -> ThreadStoreResult<()>;
    fn require_root(&self, root: &ConfinedRootIdentity) -> ThreadStoreResult<()>;
}

impl HistoryRepairWriterAuthorization for HistoryRepairWriterToken<'_> {
    fn require_thread(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        self.require_thread(thread_id)
    }

    fn require_home(&self, canonical_home: &Path) -> ThreadStoreResult<()> {
        self.require_home(canonical_home)
    }

    fn require_root(&self, root: &ConfinedRootIdentity) -> ThreadStoreResult<()> {
        self.require_root(root)
    }
}

#[cfg(test)]
impl HistoryRepairWriterAuthorization for TestHistoryRepairWriterToken {
    fn require_thread(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        self.require_thread(thread_id)
    }

    fn require_home(&self, canonical_home: &Path) -> ThreadStoreResult<()> {
        self.require_home(canonical_home)
    }

    fn require_root(&self, root: &ConfinedRootIdentity) -> ThreadStoreResult<()> {
        self.require_root(root)
    }
}

/// Canonical root used for every pathname in one repair publication.
///
/// `CODEX_HOME` may itself be a supported symlink. Resolving it once prevents a concurrent
/// retarget from changing which home later descriptor-relative operations authorize.
struct ConfinedRepairAuthority {
    lexical_home: PathBuf,
    canonical_home: PathBuf,
    root_identity: ConfinedRootIdentity,
}

impl ConfinedRepairAuthority {
    async fn bind(codex_home: &Path) -> ThreadStoreResult<Self> {
        let canonical_home = fs::canonicalize(codex_home)
            .await
            .map_err(thread_store_io_error)?;
        let authority = Self {
            lexical_home: codex_home.to_path_buf(),
            root_identity: confined_root_identity(canonical_home.as_path())
                .await
                .map_err(thread_store_io_error)?,
            canonical_home,
        };
        #[cfg(all(test, unix))]
        if let Some(target) = CODEX_HOME_RETARGETS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(codex_home)
        {
            use std::os::unix::fs::symlink;

            std::fs::remove_file(codex_home).map_err(thread_store_io_error)?;
            symlink(target, codex_home).map_err(thread_store_io_error)?;
        }
        Ok(authority)
    }

    fn bind_path(&self, path: &Path) -> ThreadStoreResult<PathBuf> {
        let relative = path
            .strip_prefix(self.lexical_home.as_path())
            .or_else(|_| path.strip_prefix(self.canonical_home.as_path()))
            .map_err(|_| ThreadStoreError::Conflict {
                message: format!(
                    "rollout {} is outside CODEX_HOME {}",
                    path.display(),
                    self.lexical_home.display()
                ),
            })?;
        Ok(self.canonical_home.join(relative))
    }

    fn lexical_path(&self, path: &Path) -> ThreadStoreResult<PathBuf> {
        let relative = path
            .strip_prefix(self.canonical_home.as_path())
            .map_err(|_| ThreadStoreError::Internal {
                message: format!(
                    "confined rollout {} is outside bound CODEX_HOME {}",
                    path.display(),
                    self.canonical_home.display()
                ),
            })?;
        Ok(self.lexical_home.join(relative))
    }
}

/// Derives an immutable repair identity from bytes with `SessionMeta.segment_id` cleared.
///
/// Rejected records, blank records, whitespace, and record boundaries remain in the hash input.
pub(crate) fn history_repair_segment_id(identity_cleared_bytes: &[u8]) -> SegmentId {
    let digest = Sha256::digest(identity_cleared_bytes);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    SegmentId::from_bytes(bytes)
}

/// Clears the persisted segment identity without reserializing any rollout record.
pub(crate) fn clear_history_repair_segment_id(
    bytes: &[u8],
    segment_id: SegmentId,
) -> ThreadStoreResult<Vec<u8>> {
    rewrite_physical_segment_id(bytes, segment_id, /*replacement_segment_id*/ None)
}

/// Replaces the fixed-width persisted segment identity without changing any other byte.
pub(crate) fn replace_history_repair_segment_id(
    bytes: &[u8],
    old_segment_id: SegmentId,
    new_segment_id: SegmentId,
) -> ThreadStoreResult<Vec<u8>> {
    rewrite_physical_segment_id(bytes, old_segment_id, Some(new_segment_id))
}

/// Installs raw physical JSONL bytes as an ordinary immutable rollout segment.
///
/// Existing equal bytes are reused; existing different bytes fail closed.
pub(crate) async fn install_history_repair_segment(
    writer: &impl HistoryRepairWriterAuthorization,
    codex_home: &Path,
    thread_id: ThreadId,
    segment_id: SegmentId,
    source_path: &Path,
    identity_cleared_bytes: &[u8],
    bytes: &[u8],
) -> ThreadStoreResult<PathBuf> {
    writer.require_thread(thread_id)?;
    let derived_preimage = identity_cleared_preimage(bytes, thread_id, segment_id)?;
    if derived_preimage != identity_cleared_bytes
        || history_repair_segment_id(derived_preimage.as_slice()) != segment_id
    {
        return Err(ThreadStoreError::Conflict {
            message: "immutable history repair segment does not match its content-derived identity"
                .to_string(),
        });
    }
    install_history_repair_segment_bytes(
        writer,
        codex_home,
        thread_id,
        segment_id,
        source_path,
        bytes,
    )
    .await
}

/// Installs exact source bytes under their already-persisted ordinary segment identity.
///
/// Active rollouts may carry random historical segment IDs. This creates the byte-exact immutable
/// backup required before the active root is replaced, while still requiring SessionMeta and the
/// destination directory to use that same identity.
pub(crate) async fn install_existing_identity_history_repair_backup(
    writer: &impl HistoryRepairWriterAuthorization,
    codex_home: &Path,
    thread_id: ThreadId,
    segment_id: SegmentId,
    source_path: &Path,
    bytes: &[u8],
) -> ThreadStoreResult<PathBuf> {
    writer.require_thread(thread_id)?;
    install_history_repair_segment_bytes(
        writer,
        codex_home,
        thread_id,
        segment_id,
        source_path,
        bytes,
    )
    .await
}

async fn install_history_repair_segment_bytes(
    writer: &impl HistoryRepairWriterAuthorization,
    codex_home: &Path,
    thread_id: ThreadId,
    segment_id: SegmentId,
    source_path: &Path,
    bytes: &[u8],
) -> ThreadStoreResult<PathBuf> {
    validate_segment_identity(bytes, thread_id, segment_id)?;
    let authority = ConfinedRepairAuthority::bind(codex_home).await?;
    writer.require_home(authority.canonical_home.as_path())?;
    writer.require_root(&authority.root_identity)?;
    let lexical_destination = immutable_segment_path(
        codex_home,
        thread_id,
        Some(segment_id),
        codex_rollout::plain_rollout_path(source_path).as_path(),
    )?;
    let destination = authority.bind_path(lexical_destination.as_path())?;
    let parent = immutable_segment_parent(destination.as_path())?;
    let directory_permissions = {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::Permissions::from_mode(0o700)
        }
        #[cfg(not(unix))]
        {
            std::fs::metadata(authority.canonical_home.as_path())
                .map_err(thread_store_io_error)?
                .permissions()
        }
    };
    ensure_confined_directory_under_root(
        authority.canonical_home.as_path(),
        parent.as_path(),
        directory_permissions,
        &authority.root_identity,
    )
    .await
    .map_err(thread_store_io_error)?;
    let existing = validate_rollout_representations(
        authority.canonical_home.as_path(),
        destination.as_path(),
        bytes,
        &authority.root_identity,
    )
    .await?;
    if let Some(existing) = existing.filter(|path| path != &destination) {
        return authority.lexical_path(existing.as_path());
    }
    let permissions = {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::Permissions::from_mode(0o600)
        }
        #[cfg(not(unix))]
        {
            std::fs::metadata(parent.as_path())
                .map_err(thread_store_io_error)?
                .permissions()
        }
    };
    let result = install_confined_file_under_root(
        authority.canonical_home.as_path(),
        destination.as_path(),
        bytes,
        permissions,
        /*modified*/ None,
        &authority.root_identity,
    )
    .await;
    match result {
        Ok(ConfinedInstallOutcome::Installed | ConfinedInstallOutcome::Reused) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return Err(ThreadStoreError::Conflict {
                message: format!(
                    "immutable history repair segment {} already exists with different contents",
                    destination.display()
                ),
            });
        }
        Err(error) => return Err(thread_store_io_error(error)),
    }
    let installed = validate_rollout_representations(
        authority.canonical_home.as_path(),
        destination.as_path(),
        bytes,
        &authority.root_identity,
    )
    .await?
    .ok_or_else(|| ThreadStoreError::Internal {
        message: format!(
            "immutable history repair segment {} disappeared after installation",
            destination.display()
        ),
    })?;
    authority.lexical_path(installed.as_path())
}

fn validate_segment_identity(
    bytes: &[u8],
    thread_id: ThreadId,
    segment_id: SegmentId,
) -> ThreadStoreResult<()> {
    let mut saw_meta = false;
    for physical_line in bytes.split_inclusive(|byte| *byte == b'\n') {
        let Ok(line) = serde_json::from_slice::<RolloutLine>(physical_line) else {
            continue;
        };
        let RolloutItem::SessionMeta(meta) = line.item else {
            if saw_meta {
                continue;
            }
            return Err(ThreadStoreError::Conflict {
                message:
                    "immutable history repair segment has a valid record before session metadata"
                        .to_string(),
            });
        };
        if meta.meta.id != thread_id || meta.meta.segment_id != Some(segment_id) {
            return Err(ThreadStoreError::Conflict {
                message: "immutable history repair segment metadata does not match its identity"
                    .to_string(),
            });
        }
        saw_meta = true;
        break;
    }
    if !saw_meta {
        return Err(ThreadStoreError::Conflict {
            message: "immutable history repair segment contains no session metadata".to_string(),
        });
    }
    Ok(())
}

/// Atomically publishes same-span replacement bytes while preserving source mode and mtime.
///
/// The temporary file is synchronized before rename. Once rename succeeds, any remaining error is
/// reported as `DurabilityUnknown` because callers must restart and rescan instead of writing.
pub(crate) async fn publish_history_repair_replacement(
    writer: &impl HistoryRepairWriterAuthorization,
    codex_home: &Path,
    stable_path: &Path,
    replacement: &[u8],
) -> ThreadStoreResult<HistoryRepairPublication> {
    let authority = ConfinedRepairAuthority::bind(codex_home).await?;
    writer.require_home(authority.canonical_home.as_path())?;
    writer.require_root(&authority.root_identity)?;
    let stable_path = authority.bind_path(stable_path)?;
    let codex_home = authority.canonical_home.as_path();
    let stable_path = validate_mutable_repair_source(codex_home, stable_path.as_path()).await?;
    let (replacement_thread_id, _) =
        validate_mutable_segment_identity(replacement, stable_path.as_path())?;
    writer.require_thread(replacement_thread_id)?;
    let compressed_path = compressed_sibling(stable_path.as_path());
    if confined_entry_exists_under_root(
        codex_home,
        compressed_path.as_path(),
        &authority.root_identity,
    )
    .await
    .map_err(thread_store_io_error)?
    {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "compressed rollout {} exists beside plain repair source",
                compressed_path.display()
            ),
        });
    }
    let (source, source_snapshot) =
        read_confined_file_under_root(codex_home, stable_path.as_path(), &authority.root_identity)
            .await
            .map_err(thread_store_io_error)?;
    let replacement_len =
        u64::try_from(replacement.len()).map_err(|error| ThreadStoreError::Internal {
            message: format!("rollout replacement length does not fit u64: {error}"),
        })?;
    if u64::try_from(source.len()).ok() != Some(replacement_len) {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "history repair for {} changed the rollout length",
                stable_path.display()
            ),
        });
    }
    let (thread_id, old_segment_id) =
        validate_mutable_segment_identity(source.as_slice(), stable_path.as_path())?;
    require_exact_existing_identity_backup(
        codex_home,
        thread_id,
        old_segment_id,
        stable_path.as_path(),
        source.as_slice(),
        &authority.root_identity,
    )
    .await?;
    if physical_record_lengths(source.as_slice()) != physical_record_lengths(replacement) {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "history repair for {} changed a physical record boundary",
                stable_path.display()
            ),
        });
    }
    validate_replacement_semantics(
        source.as_slice(),
        replacement,
        stable_path.as_path(),
        thread_id,
        old_segment_id,
    )?;

    #[cfg(test)]
    if PRECOMMIT_FAILURES
        .lock()
        .expect("history repair precommit failure mutex")
        .remove(stable_path.as_path())
    {
        return Err(ThreadStoreError::Internal {
            message: "injected history repair precommit failure".to_string(),
        });
    }
    #[cfg(test)]
    let source_replacement = SOURCE_REPLACEMENTS
        .lock()
        .expect("history repair source replacement mutex")
        .remove(stable_path.as_path());
    #[cfg(test)]
    if let Some(bytes) = source_replacement {
        fs::write(stable_path.as_path(), bytes)
            .await
            .map_err(thread_store_io_error)?;
    }
    let outcome = replace_confined_file_under_root(
        codex_home,
        stable_path.as_path(),
        &source_snapshot,
        replacement,
        &authority.root_identity,
    )
    .await
    .map_err(thread_store_io_error)?;
    #[cfg(test)]
    let published_replacement = PUBLISHED_REPLACEMENTS
        .lock()
        .expect("published replacement mutex")
        .remove(stable_path.as_path());
    #[cfg(test)]
    if let Some(bytes) = published_replacement {
        fs::write(stable_path.as_path(), bytes)
            .await
            .map_err(thread_store_io_error)?;
        return Ok(HistoryRepairPublication::DurabilityUnknown {
            error: ThreadStoreError::Internal {
                message: "published rollout changed before synchronization".to_string(),
            },
        });
    }
    #[cfg(test)]
    if POSTCOMMIT_SYNC_FAILURES
        .lock()
        .expect("history repair postcommit failure mutex")
        .remove(stable_path.as_path())
    {
        return Ok(HistoryRepairPublication::DurabilityUnknown {
            error: ThreadStoreError::Internal {
                message: "injected history repair postcommit sync failure".to_string(),
            },
        });
    }
    Ok(map_confined_mutation(outcome))
}

/// Atomically replaces a compressed active rollout with compressed repaired bytes.
///
/// Keeping the existing representation avoids a two-name publication protocol: a crash leaves
/// either the old compressed rollout or the repaired compressed rollout. A plain sibling makes
/// the selected source ambiguous and therefore fails closed.
pub(crate) async fn publish_compressed_history_repair_replacement(
    writer: &impl HistoryRepairWriterAuthorization,
    codex_home: &Path,
    compressed_path: &Path,
    replacement: &[u8],
) -> ThreadStoreResult<HistoryRepairPublication> {
    let authority = ConfinedRepairAuthority::bind(codex_home).await?;
    writer.require_home(authority.canonical_home.as_path())?;
    writer.require_root(&authority.root_identity)?;
    let compressed_path = authority.bind_path(compressed_path)?;
    let codex_home = authority.canonical_home.as_path();
    let compressed_path =
        validate_mutable_repair_source(codex_home, compressed_path.as_path()).await?;
    let plain_path = codex_rollout::plain_rollout_path(compressed_path.as_path());
    if plain_path == compressed_path {
        return Err(ThreadStoreError::Internal {
            message: format!("rollout {} is not compressed", compressed_path.display()),
        });
    }
    if confined_entry_exists_under_root(codex_home, plain_path.as_path(), &authority.root_identity)
        .await
        .map_err(thread_store_io_error)?
    {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "plain rollout {} already exists beside compressed repair source",
                plain_path.display()
            ),
        });
    }
    let (replacement_thread_id, _) =
        validate_mutable_segment_identity(replacement, compressed_path.as_path())?;
    writer.require_thread(replacement_thread_id)?;
    let (compressed_bytes, source_snapshot) = read_confined_file_under_root(
        codex_home,
        compressed_path.as_path(),
        &authority.root_identity,
    )
    .await
    .map_err(thread_store_io_error)?;
    let source = tokio::task::spawn_blocking(move || -> io::Result<Vec<u8>> {
        let mut decoder = zstd::stream::read::Decoder::new(compressed_bytes.as_slice())?;
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded)?;
        Ok(decoded)
    })
    .await
    .map_err(|error| ThreadStoreError::Internal {
        message: format!("failed to join compressed repair reader: {error}"),
    })?
    .map_err(thread_store_io_error)?;
    if source == replacement {
        return Ok(HistoryRepairPublication::Durable);
    }
    let (thread_id, old_segment_id) =
        validate_mutable_segment_identity(source.as_slice(), compressed_path.as_path())?;
    require_exact_existing_identity_backup(
        codex_home,
        thread_id,
        old_segment_id,
        compressed_path.as_path(),
        source.as_slice(),
        &authority.root_identity,
    )
    .await?;
    if source.len() != replacement.len()
        || physical_record_lengths(source.as_slice()) != physical_record_lengths(replacement)
    {
        return Err(ThreadStoreError::Internal {
            message: format!(
                "history repair for {} changed a physical record boundary",
                compressed_path.display()
            ),
        });
    }
    validate_replacement_semantics(
        source.as_slice(),
        replacement,
        compressed_path.as_path(),
        thread_id,
        old_segment_id,
    )?;

    let compressed_replacement = tokio::task::spawn_blocking({
        let replacement = replacement.to_vec();
        move || zstd::stream::encode_all(replacement.as_slice(), 0)
    })
    .await
    .map_err(|error| ThreadStoreError::Internal {
        message: format!("failed to join compressed repair writer: {error}"),
    })?
    .map_err(thread_store_io_error)?;
    #[cfg(test)]
    let source_replacement = SOURCE_REPLACEMENTS
        .lock()
        .expect("history repair source replacement mutex")
        .remove(compressed_path.as_path());
    #[cfg(test)]
    if let Some(bytes) = source_replacement {
        fs::write(compressed_path.as_path(), bytes)
            .await
            .map_err(thread_store_io_error)?;
    }
    let outcome = replace_confined_file_under_root(
        codex_home,
        compressed_path.as_path(),
        &source_snapshot,
        compressed_replacement.as_slice(),
        &authority.root_identity,
    )
    .await
    .map_err(thread_store_io_error)?;
    #[cfg(test)]
    if POSTCOMMIT_SYNC_FAILURES
        .lock()
        .expect("history repair postcommit failure mutex")
        .remove(compressed_path.as_path())
    {
        return Ok(HistoryRepairPublication::DurabilityUnknown {
            error: ThreadStoreError::Internal {
                message: "injected history repair postcommit sync failure".to_string(),
            },
        });
    }
    Ok(map_confined_mutation(outcome))
}

/// Cleans a crash-left publication name and revalidates a mutable rollout before a clean return.
///
/// Runtime calls this after acquiring the full writer token and before its locked repair rescan.
/// A repaired destination can otherwise look clean while its displaced source remains at the
/// transaction's destination-scoped temporary name.
pub(crate) async fn recover_history_repair_publication(
    writer: &HistoryRepairWriterToken<'_>,
    codex_home: &Path,
    thread_id: ThreadId,
    selected_path: &Path,
) -> ThreadStoreResult<()> {
    recover_history_repair_publication_authorized(writer, codex_home, thread_id, selected_path)
        .await
}

/// A clean reader may inspect names, but only an exclusive repair owner may clean up a
/// displaced source or resolve ambiguous plain/compressed representations.
pub(crate) async fn history_repair_publication_needs_exclusive(
    codex_home: &Path,
    selected_path: &Path,
) -> ThreadStoreResult<bool> {
    let authority = ConfinedRepairAuthority::bind(codex_home).await?;
    let selected_path = authority.bind_path(selected_path)?;
    let selected_path =
        validate_mutable_repair_source(authority.canonical_home.as_path(), selected_path.as_path())
            .await?;
    let plain_path = codex_rollout::plain_rollout_path(&selected_path);
    let sibling = if plain_path == selected_path {
        compressed_sibling(&plain_path)
    } else {
        plain_path
    };
    if confined_entry_exists_under_root(
        &authority.canonical_home,
        &sibling,
        &authority.root_identity,
    )
    .await
    .map_err(thread_store_io_error)?
    {
        return Ok(true);
    }
    confined_staged_entries_exist_under_root(
        &authority.canonical_home,
        &selected_path,
        &authority.root_identity,
    )
    .await
    .map_err(thread_store_io_error)
}

async fn recover_history_repair_publication_authorized(
    writer: &impl HistoryRepairWriterAuthorization,
    codex_home: &Path,
    thread_id: ThreadId,
    selected_path: &Path,
) -> ThreadStoreResult<()> {
    writer.require_thread(thread_id)?;
    let authority = ConfinedRepairAuthority::bind(codex_home).await?;
    writer.require_home(authority.canonical_home.as_path())?;
    writer.require_root(&authority.root_identity)?;
    let selected_path = authority.bind_path(selected_path)?;
    let selected_path =
        validate_mutable_repair_source(authority.canonical_home.as_path(), selected_path.as_path())
            .await?;
    let plain_path = codex_rollout::plain_rollout_path(selected_path.as_path());
    let sibling = if plain_path == selected_path {
        compressed_sibling(plain_path.as_path())
    } else {
        plain_path
    };
    if confined_entry_exists_under_root(
        authority.canonical_home.as_path(),
        sibling.as_path(),
        &authority.root_identity,
    )
    .await
    .map_err(thread_store_io_error)?
    {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "rollout {} has an ambiguous sibling representation {}",
                selected_path.display(),
                sibling.display()
            ),
        });
    }
    let (stored, _) = read_confined_file_under_root(
        authority.canonical_home.as_path(),
        selected_path.as_path(),
        &authority.root_identity,
    )
    .await
    .map_err(thread_store_io_error)?;
    let bytes = if codex_rollout::plain_rollout_path(selected_path.as_path()) != selected_path {
        decode_compressed(stored).await?
    } else {
        stored
    };
    let actual_thread_id =
        validate_mutable_thread_identity(bytes.as_slice(), selected_path.as_path())?;
    writer.require_thread(actual_thread_id)
}

#[cfg(test)]
async fn recover_history_repair_publication_for_test(
    writer: &TestHistoryRepairWriterToken,
    codex_home: &Path,
    thread_id: ThreadId,
    selected_path: &Path,
) -> ThreadStoreResult<()> {
    recover_history_repair_publication_authorized(writer, codex_home, thread_id, selected_path)
        .await
}

fn map_confined_mutation(outcome: ConfinedMutationOutcome) -> HistoryRepairPublication {
    match outcome {
        ConfinedMutationOutcome::Durable => HistoryRepairPublication::Durable,
        ConfinedMutationOutcome::DurabilityUnknown { error } => {
            HistoryRepairPublication::DurabilityUnknown {
                error: thread_store_io_error(error),
            }
        }
    }
}

async fn require_exact_existing_identity_backup(
    codex_home: &Path,
    thread_id: ThreadId,
    segment_id: SegmentId,
    source_path: &Path,
    source: &[u8],
    root_identity: &ConfinedRootIdentity,
) -> ThreadStoreResult<()> {
    let backup = immutable_segment_path(
        codex_home,
        thread_id,
        Some(segment_id),
        codex_rollout::plain_rollout_path(source_path).as_path(),
    )?;
    validate_rollout_representations(codex_home, backup.as_path(), source, root_identity)
        .await?
        .ok_or_else(|| ThreadStoreError::Conflict {
            message: format!("immutable backup {} is missing", backup.display()),
        })?;
    Ok(())
}

async fn validate_rollout_representations(
    codex_home: &Path,
    plain_path: &Path,
    expected: &[u8],
    root_identity: &ConfinedRootIdentity,
) -> ThreadStoreResult<Option<PathBuf>> {
    let plain_path = codex_rollout::plain_rollout_path(plain_path);
    let compressed_path = compressed_sibling(plain_path.as_path());
    let mut selected = None;
    for (path, compressed) in [
        (plain_path.as_path(), false),
        (compressed_path.as_path(), true),
    ] {
        let stored = match read_confined_file_under_root(codex_home, path, root_identity).await {
            Ok((bytes, snapshot)) => {
                require_private_rollout_permissions(path, &snapshot.permissions)?;
                if compressed {
                    decode_compressed(bytes).await?
                } else {
                    bytes
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(thread_store_io_error(error)),
        };
        if stored != expected {
            return Err(ThreadStoreError::Conflict {
                message: format!(
                    "immutable history repair segment {} already exists with different contents",
                    path.display()
                ),
            });
        }
        if selected.is_none() {
            selected = Some(path.to_path_buf());
        }
    }
    Ok(selected)
}

fn require_private_rollout_permissions(
    path: &Path,
    permissions: &std::fs::Permissions,
) -> ThreadStoreResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        if permissions.mode() & 0o777 != 0o600 {
            return Err(ThreadStoreError::Conflict {
                message: format!(
                    "immutable history repair segment {} does not have private 0600 permissions",
                    path.display()
                ),
            });
        }
    }
    #[cfg(not(unix))]
    let _ = (path, permissions);
    Ok(())
}

fn compressed_sibling(plain_path: &Path) -> PathBuf {
    let mut name = plain_path.file_name().unwrap_or_default().to_os_string();
    name.push(".zst");
    plain_path.with_file_name(name)
}

async fn decode_compressed(compressed: Vec<u8>) -> ThreadStoreResult<Vec<u8>> {
    tokio::task::spawn_blocking(move || -> io::Result<Vec<u8>> {
        let mut decoder = zstd::stream::read::Decoder::new(compressed.as_slice())?;
        let mut bytes = Vec::new();
        decoder.read_to_end(&mut bytes)?;
        Ok(bytes)
    })
    .await
    .map_err(|error| ThreadStoreError::Internal {
        message: format!("failed to join compressed rollout read: {error}"),
    })?
    .map_err(thread_store_io_error)
}

/// Restricts a path-addressed legacy identity to its canonical `initial` directory.
///
/// Publication deliberately has no legacy-initial counterpart. A separately reference-valid,
/// byte-identical backup of `segment_id=None` cannot be represented by the ordinary segment
/// identity, so affected legacy initial history must fail closed before mutation.
pub(crate) async fn validate_legacy_initial_repair_path(
    codex_home: &Path,
    thread_id: ThreadId,
    path: &Path,
) -> ThreadStoreResult<PathBuf> {
    let authority = ConfinedRepairAuthority::bind(codex_home).await?;
    let logical_path = authority.bind_path(codex_rollout::plain_rollout_path(path).as_path())?;
    let codex_home = authority.canonical_home.as_path();
    let expected_parent = codex_home
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join("initial");
    if logical_path.parent() != Some(expected_parent.as_path()) {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "legacy initial repair path {} is outside the canonical directory for thread {thread_id}",
                logical_path.display()
            ),
        });
    }
    validate_real_directory_tree(codex_home, expected_parent.as_path()).await?;
    if let Some(existing) = codex_rollout::existing_rollout_path(logical_path.as_path()).await {
        validate_regular_file_in_parent(existing.as_path(), expected_parent.as_path()).await?;
    }
    Ok(logical_path)
}

async fn validate_mutable_repair_source(
    codex_home: &Path,
    stable_path: &Path,
) -> ThreadStoreResult<PathBuf> {
    reject_immutable_publication_target(codex_home, stable_path).await?;
    let parent = stable_path
        .parent()
        .ok_or_else(|| ThreadStoreError::Conflict {
            message: format!("rollout {} has no parent directory", stable_path.display()),
        })?;
    validate_real_directory_tree(codex_home, parent).await?;
    validate_regular_file_in_parent(stable_path, parent).await?;
    Ok(stable_path.to_path_buf())
}

async fn reject_immutable_publication_target(
    codex_home: &Path,
    stable_path: &Path,
) -> ThreadStoreResult<()> {
    let canonical_home = fs::canonicalize(codex_home)
        .await
        .map_err(thread_store_io_error)?;
    let canonical_stable = fs::canonicalize(stable_path)
        .await
        .map_err(thread_store_io_error)?;
    let rotated = canonical_home.join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR);
    if stable_path.starts_with(codex_home.join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR))
        || canonical_stable.starts_with(rotated.as_path())
    {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "immutable rollout {} cannot be repaired in place",
                stable_path.display()
            ),
        });
    }
    Ok(())
}

async fn validate_real_directory_tree(codex_home: &Path, path: &Path) -> ThreadStoreResult<()> {
    let canonical_home = fs::canonicalize(codex_home)
        .await
        .map_err(thread_store_io_error)?;
    for directory in ancestors_below(path, codex_home)? {
        let metadata = fs::symlink_metadata(directory.as_path())
            .await
            .map_err(thread_store_io_error)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(ThreadStoreError::Conflict {
                message: format!(
                    "history repair directory {} is not a real directory",
                    directory.display()
                ),
            });
        }
    }
    let canonical_path = fs::canonicalize(path)
        .await
        .map_err(thread_store_io_error)?;
    if !canonical_path.starts_with(canonical_home) {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "history repair directory {} resolves outside CODEX_HOME",
                path.display()
            ),
        });
    }
    Ok(())
}

fn ancestors_below(path: &Path, root: &Path) -> ThreadStoreResult<Vec<PathBuf>> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| ThreadStoreError::Conflict {
            message: format!("repair path {} is outside CODEX_HOME", path.display()),
        })?;
    let mut result = Vec::new();
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component);
        result.push(current.clone());
    }
    Ok(result)
}

async fn validate_regular_file_in_parent(path: &Path, parent: &Path) -> ThreadStoreResult<()> {
    let metadata = fs::symlink_metadata(path)
        .await
        .map_err(thread_store_io_error)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "history repair source {} is not a real file",
                path.display()
            ),
        });
    }
    let canonical_path = fs::canonicalize(path)
        .await
        .map_err(thread_store_io_error)?;
    let canonical_parent = fs::canonicalize(parent)
        .await
        .map_err(thread_store_io_error)?;
    if canonical_path.parent() != Some(canonical_parent.as_path()) {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "history repair source {} resolves outside its expected directory",
                path.display()
            ),
        });
    }
    Ok(())
}

fn validate_mutable_segment_identity(
    source: &[u8],
    path: &Path,
) -> ThreadStoreResult<(ThreadId, SegmentId)> {
    let (thread_id, segment_id) = validate_mutable_rollout_identity(source, path)?;
    let Some(segment_id) = segment_id else {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "rollout {} has legacy segment_id=None and cannot be repaired without a byte-identical backup",
                path.display()
            ),
        });
    };
    Ok((thread_id, segment_id))
}

fn validate_mutable_thread_identity(source: &[u8], path: &Path) -> ThreadStoreResult<ThreadId> {
    validate_mutable_rollout_identity(source, path).map(|(thread_id, _)| thread_id)
}

fn validate_mutable_rollout_identity(
    source: &[u8],
    path: &Path,
) -> ThreadStoreResult<(ThreadId, Option<SegmentId>)> {
    for physical_line in source.split_inclusive(|byte| *byte == b'\n') {
        let Ok(line) = serde_json::from_slice::<RolloutLine>(physical_line) else {
            continue;
        };
        let RolloutItem::SessionMeta(meta) = line.item else {
            return Err(ThreadStoreError::Conflict {
                message: format!(
                    "rollout {} has a valid record before session metadata",
                    path.display()
                ),
            });
        };
        return Ok((meta.meta.id, meta.meta.segment_id));
    }
    Err(ThreadStoreError::Conflict {
        message: format!("rollout {} contains no session metadata", path.display()),
    })
}

fn validate_replacement_semantics(
    source: &[u8],
    replacement: &[u8],
    path: &Path,
    expected_thread_id: ThreadId,
    old_segment_id: SegmentId,
) -> ThreadStoreResult<()> {
    let source_records = physical_records(source);
    let replacement_records = physical_records(replacement);
    let mut replacement_segment_id = None;
    let session_index = source_records.iter().position(|record| {
        serde_json::from_slice::<RolloutLine>(record)
            .is_ok_and(|line| matches!(line.item, RolloutItem::SessionMeta(_)))
    });
    let equivalent = source_records.len() == replacement_records.len()
        && source_records
            .iter()
            .zip(replacement_records.iter())
            .enumerate()
            .all(|(index, (source_record, replacement_record))| {
                match (
                    serde_json::from_slice::<RolloutLine>(source_record),
                    serde_json::from_slice::<RolloutLine>(replacement_record),
                ) {
                    (Ok(source_line), Ok(replacement_line)) => {
                        let identity_matches = match (&source_line.item, &replacement_line.item) {
                            (
                                RolloutItem::SessionMeta(source_meta),
                                RolloutItem::SessionMeta(replacement_meta),
                            ) if Some(index) == session_index => {
                                let candidate = replacement_meta.meta.segment_id;
                                replacement_segment_id = candidate;
                                source_meta.meta.id == expected_thread_id
                                    && replacement_meta.meta.id == expected_thread_id
                                    && source_meta.meta.segment_id == Some(old_segment_id)
                                    && candidate
                                        .is_some_and(|candidate| candidate != old_segment_id)
                                    && candidate.is_some_and(|candidate| {
                                        identity_cleared_session_record(
                                            source_record,
                                            old_segment_id,
                                        ) == identity_cleared_session_record(
                                            replacement_record,
                                            candidate,
                                        )
                                    })
                            }
                            (RolloutItem::SessionMeta(_), _) | (_, RolloutItem::SessionMeta(_)) => {
                                false
                            }
                            _ => true,
                        };
                        identity_matches
                            && source_line.ordinal == replacement_line.ordinal
                            && source_line.timestamp == replacement_line.timestamp
                            && codex_app_server_protocol::project_rollout_line(&source_line)
                                == codex_app_server_protocol::project_rollout_line(
                                    &replacement_line,
                                )
                    }
                    (Err(_), Err(_)) => source_record == replacement_record,
                    _ => false,
                }
            });
    let identity_matches = replacement_segment_id.is_some_and(|segment_id| {
        identity_cleared_preimage(replacement, expected_thread_id, segment_id)
            .is_ok_and(|preimage| history_repair_segment_id(preimage.as_slice()) == segment_id)
    });
    if !equivalent || !identity_matches {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "history repair replacement for {} changed persisted rollout semantics",
                path.display()
            ),
        });
    }
    Ok(())
}

fn identity_cleared_session_record(record: &[u8], segment_id: SegmentId) -> Option<Vec<u8>> {
    clear_physical_segment_id(record, segment_id).ok()
}

fn physical_records(source: &[u8]) -> Vec<&[u8]> {
    source.split_inclusive(|byte| *byte == b'\n').collect()
}

fn identity_cleared_preimage(
    bytes: &[u8],
    thread_id: ThreadId,
    segment_id: SegmentId,
) -> ThreadStoreResult<Vec<u8>> {
    let records = physical_records(bytes);
    let Some(session_index) = records.iter().position(|record| {
        serde_json::from_slice::<RolloutLine>(record)
            .is_ok_and(|line| matches!(line.item, RolloutItem::SessionMeta(_)))
    }) else {
        return Err(ThreadStoreError::Conflict {
            message: "immutable history repair segment contains no session metadata".to_string(),
        });
    };
    let session_record = records[session_index];
    let first_line = serde_json::from_slice::<RolloutLine>(session_record).map_err(|_| {
        ThreadStoreError::Conflict {
            message: "immutable history repair segment does not start with session metadata"
                .to_string(),
        }
    })?;
    let RolloutItem::SessionMeta(meta) = &first_line.item else {
        return Err(ThreadStoreError::Conflict {
            message: "immutable history repair segment does not start with session metadata"
                .to_string(),
        });
    };
    if meta.meta.id != thread_id || meta.meta.segment_id != Some(segment_id) {
        return Err(ThreadStoreError::Conflict {
            message: "immutable history repair segment metadata does not match its identity"
                .to_string(),
        });
    }
    let rewritten_first = clear_physical_segment_id(session_record, segment_id)?;
    let mut preimage = Vec::with_capacity(bytes.len());
    for (index, record) in records.iter().enumerate() {
        if index == session_index {
            preimage.extend_from_slice(rewritten_first.as_slice());
        } else {
            preimage.extend_from_slice(record);
        }
    }
    Ok(preimage)
}

fn rewrite_physical_segment_id(
    bytes: &[u8],
    expected_segment_id: SegmentId,
    replacement_segment_id: Option<SegmentId>,
) -> ThreadStoreResult<Vec<u8>> {
    let records = physical_records(bytes);
    let Some(session_index) = records.iter().position(|record| {
        serde_json::from_slice::<RolloutLine>(record)
            .is_ok_and(|line| matches!(line.item, RolloutItem::SessionMeta(_)))
    }) else {
        return Err(ThreadStoreError::Conflict {
            message: "immutable history repair segment contains no session metadata".to_string(),
        });
    };
    let rewritten = rewrite_physical_segment_id_record(
        records[session_index],
        expected_segment_id,
        replacement_segment_id,
    )?;
    let mut output = Vec::with_capacity(bytes.len());
    for (index, record) in records.iter().enumerate() {
        if index == session_index {
            output.extend_from_slice(rewritten.as_slice());
        } else {
            output.extend_from_slice(record);
        }
    }
    Ok(output)
}

#[derive(Deserialize)]
struct RawSessionEnvelope<'a> {
    #[serde(borrow)]
    payload: &'a RawValue,
}

#[derive(Deserialize)]
struct RawSessionPayload<'a> {
    #[serde(borrow)]
    segment_id: &'a RawValue,
}

fn clear_physical_segment_id(
    session_record: &[u8],
    segment_id: SegmentId,
) -> ThreadStoreResult<Vec<u8>> {
    rewrite_physical_segment_id_record(
        session_record,
        segment_id,
        /*replacement_segment_id*/ None,
    )
}

fn rewrite_physical_segment_id_record(
    session_record: &[u8],
    expected_segment_id: SegmentId,
    replacement_segment_id: Option<SegmentId>,
) -> ThreadStoreResult<Vec<u8>> {
    let envelope =
        serde_json::from_slice::<RawSessionEnvelope<'_>>(session_record).map_err(|_| {
            ThreadStoreError::Conflict {
                message: "immutable history repair session metadata is not valid JSON".to_string(),
            }
        })?;
    let payload =
        serde_json::from_str::<RawSessionPayload<'_>>(envelope.payload.get()).map_err(|_| {
            ThreadStoreError::Conflict {
                message: "immutable history repair session metadata has no segment identity"
                    .to_string(),
            }
        })?;
    let raw_identity = payload.segment_id.get().as_bytes();
    let parsed_identity = serde_json::from_slice::<SegmentId>(raw_identity).map_err(|_| {
        ThreadStoreError::Conflict {
            message: "immutable history repair session identity is invalid".to_string(),
        }
    })?;
    if parsed_identity != expected_segment_id {
        return Err(ThreadStoreError::Conflict {
            message: "immutable history repair session identity changed during hashing".to_string(),
        });
    }
    let input_start = session_record.as_ptr() as usize;
    let identity_start = raw_identity.as_ptr() as usize;
    let offset = identity_start
        .checked_sub(input_start)
        .filter(|offset| *offset + raw_identity.len() <= session_record.len())
        .ok_or_else(|| ThreadStoreError::Internal {
            message: "raw segment identity is outside its session metadata record".to_string(),
        })?;
    let replacement = match replacement_segment_id {
        Some(segment_id) => {
            serde_json::to_vec(&segment_id).map_err(|error| ThreadStoreError::Internal {
                message: format!("failed to serialize replacement segment identity: {error}"),
            })?
        }
        None => b"null".to_vec(),
    };
    if replacement.len() > raw_identity.len() {
        return Err(ThreadStoreError::Conflict {
            message: "immutable history repair segment identity cannot be replaced in place"
                .to_string(),
        });
    }
    let mut rewritten = session_record.to_vec();
    rewritten[offset..offset + replacement.len()].copy_from_slice(replacement.as_slice());
    rewritten[offset + replacement.len()..offset + raw_identity.len()].fill(b' ');
    Ok(rewritten)
}

#[cfg(test)]
fn serialize_rollout_line_same_length(
    line: &RolloutLine,
    original_record: &[u8],
) -> ThreadStoreResult<Vec<u8>> {
    let has_newline = original_record.ends_with(b"\n");
    let mut serialized = serde_json::to_vec(line).map_err(|error| ThreadStoreError::Internal {
        message: format!("failed to serialize rollout test record: {error}"),
    })?;
    let required = serialized.len() + usize::from(has_newline);
    if required > original_record.len() {
        return Err(ThreadStoreError::Conflict {
            message: "rollout test record does not fit its physical span".to_string(),
        });
    }
    serialized.extend(std::iter::repeat_n(b' ', original_record.len() - required));
    if has_newline {
        serialized.push(b'\n');
    }
    Ok(serialized)
}

fn physical_record_lengths(source: &[u8]) -> Vec<usize> {
    source
        .split_inclusive(|byte| *byte == b'\n')
        .map(<[u8]>::len)
        .collect()
}

#[cfg(test)]
#[path = "history_repair_publication_tests.rs"]
mod tests;
