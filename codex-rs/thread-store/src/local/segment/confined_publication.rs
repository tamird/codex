//! Publishes rollout files relative to a validated directory descriptor.
//!
//! Rollout repair first validates a path below `CODEX_HOME`, but validation by pathname is not
//! enough: a directory or symlink can change before the final rename. These helpers hold the
//! destination parent open, perform the final operation relative to that descriptor, and keep the
//! staged file open through synchronization. The caller must hold `RolloutMaintenanceGuard` plus
//! the lifecycle, in-process writer, and cross-process writer exclusions for every participating
//! thread. These helpers do not make the final name exchange conditional without that exclusion.

use std::fs::File;
use std::fs::FileTimes;
use std::fs::Metadata;
use std::fs::Permissions;
use std::io;
use std::io::Read as _;
use std::io::Write as _;
use std::path::Component;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use sha2::Digest as _;
use sha2::Sha256;

static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
pub(crate) static CRASH_TEST_LOCK: std::sync::LazyLock<std::sync::Arc<tokio::sync::Mutex<()>>> =
    std::sync::LazyLock::new(|| std::sync::Arc::new(tokio::sync::Mutex::new(())));

#[cfg(test)]
static CRASH_BOUNDARIES: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, ConfinedCrashBoundary>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// Descriptor-backed observation used to reject a changed source at publication time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConfinedFileSnapshot {
    identity: FileIdentity,
    digest: [u8; 32],
    /// Permissions to apply to a replacement before it becomes visible.
    pub(crate) permissions: Permissions,
    /// Modification time to apply to a replacement before it becomes visible.
    pub(crate) modified: Option<SystemTime>,
}

/// Result of installing an immutable file without replacing an existing entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConfinedInstallOutcome {
    Installed,
    Reused,
}

/// Durability state after a mutation may have become visible.
#[derive(Debug)]
pub(crate) enum ConfinedMutationOutcome {
    Durable,
    DurabilityUnknown { error: io::Error },
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConfinedCrashBoundary {
    AfterExchangeBeforeParentSync = 1,
    AfterParentSyncBeforeUnlink = 2,
}

#[cfg(test)]
pub(crate) fn inject_crash_boundary(path: &Path, boundary: ConfinedCrashBoundary) {
    CRASH_BOUNDARIES
        .lock()
        .expect("confined crash boundary mutex")
        .insert(path.to_path_buf(), boundary);
}

#[cfg(test)]
fn take_crash_boundary(path: &Path, boundary: ConfinedCrashBoundary) -> bool {
    let mut boundaries = CRASH_BOUNDARIES
        .lock()
        .expect("confined crash boundary mutex");
    if boundaries.get(path) == Some(&boundary) {
        boundaries.remove(path);
        true
    } else {
        false
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileIdentity {
    len: u64,
    modified: Option<SystemTime>,
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
    #[cfg(unix)]
    mode: u32,
}

/// Identity of the opened CODEX_HOME directory that authorizes one repair transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConfinedRootIdentity {
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
}

impl ConfinedRootIdentity {
    fn from_metadata(metadata: &Metadata) -> io::Result<Self> {
        if !metadata.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "CODEX_HOME is not a directory",
            ));
        }
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt as _;
        Ok(Self {
            #[cfg(unix)]
            dev: metadata.dev(),
            #[cfg(unix)]
            ino: metadata.ino(),
        })
    }
}

pub(crate) async fn confined_root_identity(path: &Path) -> io::Result<ConfinedRootIdentity> {
    ConfinedRootIdentity::from_metadata(&tokio::fs::metadata(path).await?)
}

impl FileIdentity {
    fn from_metadata(metadata: &Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt as _;

        Self {
            len: metadata.len(),
            modified: metadata.modified().ok(),
            #[cfg(unix)]
            dev: metadata.dev(),
            #[cfg(unix)]
            ino: metadata.ino(),
            #[cfg(unix)]
            mode: metadata.mode(),
        }
    }
}

/// Reads one confined regular file and fingerprints the opened file descriptor.
#[cfg(test)]
pub(crate) async fn read_confined_file(
    codex_home: &Path,
    path: &Path,
) -> io::Result<(Vec<u8>, ConfinedFileSnapshot)> {
    let codex_home = codex_home.to_path_buf();
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || read_confined_file_sync(&codex_home, &path))
        .await
        .map_err(|error| {
            io::Error::other(format!("failed to join confined rollout read: {error}"))
        })?
}

pub(crate) async fn read_confined_file_under_root(
    codex_home: &Path,
    path: &Path,
    root: &ConfinedRootIdentity,
) -> io::Result<(Vec<u8>, ConfinedFileSnapshot)> {
    let codex_home = codex_home.to_path_buf();
    let path = path.to_path_buf();
    let root = root.clone();
    tokio::task::spawn_blocking(move || {
        read_confined_file_sync_under_root(&codex_home, &path, Some(&root))
    })
    .await
    .map_err(|error| io::Error::other(format!("failed to join confined rollout read: {error}")))?
}

/// Reports whether a directory entry exists without following the final component.
///
/// A dangling symlink is an existing entry. The caller can therefore reject ambiguous sibling
/// representations without relying on pathname existence checks that follow symlinks.
pub(crate) async fn confined_entry_exists_under_root(
    codex_home: &Path,
    path: &Path,
    root: &ConfinedRootIdentity,
) -> io::Result<bool> {
    let codex_home = codex_home.to_path_buf();
    let path = path.to_path_buf();
    let root = root.clone();
    tokio::task::spawn_blocking(move || {
        confined_entry_exists_sync_under_root(&codex_home, &path, Some(&root))
    })
    .await
    .map_err(|error| io::Error::other(format!("failed to join confined entry check: {error}")))?
}

/// Creates and opens every directory component below `CODEX_HOME` without following symlinks.
pub(crate) async fn ensure_confined_directory_under_root(
    codex_home: &Path,
    directory: &Path,
    permissions: Permissions,
    root: &ConfinedRootIdentity,
) -> io::Result<()> {
    let codex_home = codex_home.to_path_buf();
    let directory = directory.to_path_buf();
    let root = root.clone();
    tokio::task::spawn_blocking(move || {
        ensure_confined_directory_sync_under_root(&codex_home, &directory, permissions, Some(&root))
    })
    .await
    .map_err(|error| {
        io::Error::other(format!(
            "failed to join confined directory creation: {error}"
        ))
    })?
}

/// Atomically replaces an existing file if it still matches `expected`.
///
/// `replacement` is synchronized before rename. The installed descriptor and destination parent
/// are synchronized before success is returned.
#[cfg(test)]
pub(crate) async fn replace_confined_file(
    codex_home: &Path,
    destination: &Path,
    expected: &ConfinedFileSnapshot,
    replacement: &[u8],
) -> io::Result<ConfinedMutationOutcome> {
    let codex_home = codex_home.to_path_buf();
    let destination = destination.to_path_buf();
    let expected = expected.clone();
    let replacement = replacement.to_vec();
    tokio::task::spawn_blocking(move || {
        replace_confined_file_sync(&codex_home, &destination, &expected, &replacement)
    })
    .await
    .map_err(|error| {
        io::Error::other(format!(
            "failed to join confined rollout replacement: {error}"
        ))
    })?
}

pub(crate) async fn replace_confined_file_under_root(
    codex_home: &Path,
    destination: &Path,
    expected: &ConfinedFileSnapshot,
    replacement: &[u8],
    root: &ConfinedRootIdentity,
) -> io::Result<ConfinedMutationOutcome> {
    let codex_home = codex_home.to_path_buf();
    let destination = destination.to_path_buf();
    let expected = expected.clone();
    let replacement = replacement.to_vec();
    let root = root.clone();
    tokio::task::spawn_blocking(move || {
        replace_confined_file_sync_under_root(
            &codex_home,
            &destination,
            &expected,
            &replacement,
            Some(&root),
        )
    })
    .await
    .map_err(|error| {
        io::Error::other(format!(
            "failed to join confined rollout replacement: {error}"
        ))
    })?
}

/// Installs immutable bytes without replacing an existing destination.
///
/// An existing file is reused only when its bytes and metadata already match. Different existing
/// bytes fail with `AlreadyExists`.
#[cfg(test)]
pub(crate) async fn install_confined_file(
    codex_home: &Path,
    destination: &Path,
    bytes: &[u8],
    permissions: Permissions,
    modified: Option<SystemTime>,
) -> io::Result<ConfinedInstallOutcome> {
    let codex_home = codex_home.to_path_buf();
    let destination = destination.to_path_buf();
    let bytes = bytes.to_vec();
    tokio::task::spawn_blocking(move || {
        install_confined_file_sync(&codex_home, &destination, &bytes, permissions, modified)
    })
    .await
    .map_err(|error| {
        io::Error::other(format!("failed to join confined rollout install: {error}"))
    })?
}

pub(crate) async fn install_confined_file_under_root(
    codex_home: &Path,
    destination: &Path,
    bytes: &[u8],
    permissions: Permissions,
    modified: Option<SystemTime>,
    root: &ConfinedRootIdentity,
) -> io::Result<ConfinedInstallOutcome> {
    let codex_home = codex_home.to_path_buf();
    let destination = destination.to_path_buf();
    let bytes = bytes.to_vec();
    let root = root.clone();
    tokio::task::spawn_blocking(move || {
        install_confined_file_sync_under_root(
            &codex_home,
            &destination,
            &bytes,
            permissions,
            modified,
            Some(&root),
        )
    })
    .await
    .map_err(|error| {
        io::Error::other(format!("failed to join confined rollout install: {error}"))
    })?
}

#[cfg(unix)]
mod platform {
    use std::ffi::CString;
    use std::os::fd::AsRawFd as _;
    use std::os::fd::FromRawFd as _;
    use std::os::fd::IntoRawFd as _;
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::fs::MetadataExt as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;

    pub(super) struct ConfinedParent {
        root: File,
        relative_parent: Vec<CString>,
        parent: File,
        destination_name: CString,
    }

    impl ConfinedParent {
        pub(super) fn open(
            codex_home: &Path,
            destination: &Path,
            expected_root: Option<&ConfinedRootIdentity>,
        ) -> io::Result<Self> {
            let parent_path = destination.parent().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("destination {} has no parent", destination.display()),
                )
            })?;
            let destination_name = normal_name(destination.file_name(), destination)?;
            let relative = parent_path.strip_prefix(codex_home).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "destination {} is outside CODEX_HOME {}",
                        destination.display(),
                        codex_home.display()
                    ),
                )
            })?;
            let relative_parent = normal_components(relative)?;
            let root = open_directory_path(codex_home)?;
            verify_root_identity(&root, expected_root)?;
            let parent = walk_directory(&root, &relative_parent)?;
            Ok(Self {
                root,
                relative_parent,
                parent,
                destination_name,
            })
        }

        pub(super) fn open_destination(&self) -> io::Result<File> {
            openat_file(&self.parent, &self.destination_name, libc::O_RDONLY)
        }

        pub(super) fn destination_exists(&self) -> io::Result<bool> {
            let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
            let result = unsafe {
                libc::fstatat(
                    self.parent.as_raw_fd(),
                    self.destination_name.as_ptr(),
                    metadata.as_mut_ptr(),
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            };
            if result == 0 {
                return Ok(true);
            }
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::NotFound {
                Ok(false)
            } else {
                Err(error)
            }
        }

        pub(super) fn create_staged(&self) -> io::Result<(CString, File)> {
            for _ in 0..128 {
                let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
                let name = CString::new(format!(
                    "{}{}-{}-{sequence}",
                    self.staged_prefix(),
                    std::process::id(),
                    thread_token()
                ))
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "temporary rollout name contains NUL",
                    )
                })?;
                match openat_file(
                    &self.parent,
                    &name,
                    libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
                ) {
                    Ok(file) => return Ok((name, file)),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                    Err(error) => return Err(error),
                }
            }
            Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "could not allocate a confined rollout temporary file",
            ))
        }

        pub(super) fn cleanup_staged_entries(&self) -> io::Result<()> {
            #[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos")))]
            return Ok(());

            #[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
            {
                let directory = self.parent.try_clone()?;
                let raw_directory = directory.into_raw_fd();
                let directory_pointer = unsafe { libc::fdopendir(raw_directory) };
                if directory_pointer.is_null() {
                    unsafe { File::from_raw_fd(raw_directory) };
                    return Err(io::Error::last_os_error());
                }
                let mut result = Ok(());
                loop {
                    set_errno_zero();
                    let entry = unsafe { libc::readdir(directory_pointer) };
                    if entry.is_null() {
                        let error = io::Error::last_os_error();
                        if error.raw_os_error() != Some(0) {
                            result = Err(error);
                        }
                        break;
                    }
                    let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) };
                    if name.to_bytes().starts_with(self.staged_prefix().as_bytes())
                        && let Err(error) = self.unlink_cstr(name)
                        && error.kind() != io::ErrorKind::NotFound
                    {
                        result = Err(error);
                        break;
                    }
                }
                if unsafe { libc::closedir(directory_pointer) } == -1 && result.is_ok() {
                    result = Err(io::Error::last_os_error());
                }
                if result.is_ok() {
                    self.sync()?;
                }
                result
            }
        }

        fn staged_prefix(&self) -> String {
            let digest = Sha256::digest(self.destination_name.as_bytes());
            let encoded: String = digest[..12]
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            format!(".codex-history-repair-{encoded}-")
        }

        #[cfg(test)]
        pub(super) fn create_stale_staged_for_test(&self, bytes: &[u8]) -> io::Result<CString> {
            let (name, mut file) = self.create_staged()?;
            file.write_all(bytes)?;
            file.sync_all()?;
            self.sync()?;
            Ok(name)
        }

        pub(super) fn revalidate(&self) -> io::Result<()> {
            let current = walk_directory(&self.root, &self.relative_parent)?;
            if !same_inode(&self.parent.metadata()?, &current.metadata()?) {
                return Err(io::Error::other(
                    "destination parent changed during confined publication",
                ));
            }
            Ok(())
        }

        #[cfg(any(target_os = "linux", target_os = "android"))]
        pub(super) fn exchange_with_destination(&self, staged_name: &CString) -> io::Result<()> {
            let result = unsafe {
                // `RENAME_EXCHANGE` atomically leaves the displaced source at `staged_name`, so
                // the caller can verify the exact inode that was replaced.
                libc::renameat2(
                    self.parent.as_raw_fd(),
                    staged_name.as_ptr(),
                    self.parent.as_raw_fd(),
                    self.destination_name.as_ptr(),
                    libc::RENAME_EXCHANGE,
                )
            };
            cvt(result).map(|_| ())
        }

        #[cfg(target_os = "macos")]
        pub(super) fn exchange_with_destination(&self, staged_name: &CString) -> io::Result<()> {
            let result = unsafe {
                // `RENAME_SWAP` provides the same old-or-new publication boundary as
                // `RENAME_EXCHANGE` while keeping both names relative to the held descriptor.
                libc::renameatx_np(
                    self.parent.as_raw_fd(),
                    staged_name.as_ptr(),
                    self.parent.as_raw_fd(),
                    self.destination_name.as_ptr(),
                    libc::RENAME_SWAP,
                )
            };
            cvt(result).map(|_| ())
        }

        pub(super) fn link_noclobber(&self, staged_name: &CString) -> io::Result<()> {
            let result = unsafe {
                // Both names are relative to the same held directory descriptor.
                libc::linkat(
                    self.parent.as_raw_fd(),
                    staged_name.as_ptr(),
                    self.parent.as_raw_fd(),
                    self.destination_name.as_ptr(),
                    0,
                )
            };
            cvt(result).map(|_| ())
        }

        pub(super) fn open_named(&self, name: &CString) -> io::Result<File> {
            openat_file(&self.parent, name, libc::O_RDONLY)
        }

        pub(super) fn unlink(&self, name: &CString) -> io::Result<()> {
            self.unlink_cstr(name.as_c_str())
        }

        fn unlink_cstr(&self, name: &std::ffi::CStr) -> io::Result<()> {
            let result = unsafe { libc::unlinkat(self.parent.as_raw_fd(), name.as_ptr(), 0) };
            cvt(result).map(|_| ())
        }

        pub(super) fn verify_destination(&self, expected: &Metadata) -> io::Result<()> {
            let destination = self.open_destination()?;
            if !same_inode(expected, &destination.metadata()?) {
                return Err(io::Error::other(
                    "destination changed during confined publication",
                ));
            }
            Ok(())
        }

        pub(super) fn sync(&self) -> io::Result<()> {
            self.parent.sync_all()
        }
    }

    fn thread_token() -> u64 {
        use std::hash::Hash as _;
        use std::hash::Hasher as _;

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::thread::current().id().hash(&mut hasher);
        hasher.finish()
    }

    #[cfg(target_os = "linux")]
    fn set_errno_zero() {
        unsafe { *libc::__errno_location() = 0 };
    }

    #[cfg(target_os = "android")]
    fn set_errno_zero() {
        unsafe { *libc::__errno() = 0 };
    }

    #[cfg(target_os = "macos")]
    fn set_errno_zero() {
        unsafe { *libc::__error() = 0 };
    }

    fn open_directory_path(path: &Path) -> io::Result<File> {
        let mut options = std::fs::OpenOptions::new();
        options
            .read(true)
            // `CODEX_HOME` itself may be a user-configured symlink. Holding this opened root
            // descriptor fixes its authority for the rest of the operation; every descendant is
            // still opened with `O_NOFOLLOW` by `openat_file`.
            .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY);
        options.open(path)
    }

    fn verify_root_identity(
        root: &File,
        expected: Option<&ConfinedRootIdentity>,
    ) -> io::Result<()> {
        if let Some(expected) = expected
            && ConfinedRootIdentity::from_metadata(&root.metadata()?)? != *expected
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "CODEX_HOME changed after history repair locks were acquired",
            ));
        }
        Ok(())
    }

    fn walk_directory(root: &File, components: &[CString]) -> io::Result<File> {
        let duplicated = unsafe { libc::dup(root.as_raw_fd()) };
        if duplicated == -1 {
            return Err(io::Error::last_os_error());
        }
        let mut current = unsafe {
            // `dup` returned a new owned descriptor.
            File::from_raw_fd(duplicated)
        };
        for component in components {
            current = openat_file(&current, component, libc::O_RDONLY | libc::O_DIRECTORY)?;
        }
        Ok(current)
    }

    pub(super) fn ensure_directory(
        codex_home: &Path,
        directory: &Path,
        permissions: Permissions,
        expected_root: Option<&ConfinedRootIdentity>,
    ) -> io::Result<()> {
        let relative = directory.strip_prefix(codex_home).map_err(|_| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "directory {} is outside CODEX_HOME {}",
                    directory.display(),
                    codex_home.display()
                ),
            )
        })?;
        let components = normal_components(relative)?;
        let root = open_directory_path(codex_home)?;
        verify_root_identity(&root, expected_root)?;
        let duplicated = unsafe { libc::dup(root.as_raw_fd()) };
        if duplicated == -1 {
            return Err(io::Error::last_os_error());
        }
        let mut current = unsafe { File::from_raw_fd(duplicated) };
        for component in components {
            match openat_file(&current, &component, libc::O_RDONLY | libc::O_DIRECTORY) {
                Ok(next) => current = next,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    let mode = permission_mode(&permissions);
                    let result =
                        unsafe { libc::mkdirat(current.as_raw_fd(), component.as_ptr(), mode) };
                    if result == -1 {
                        let mkdir_error = io::Error::last_os_error();
                        if mkdir_error.kind() != io::ErrorKind::AlreadyExists {
                            return Err(mkdir_error);
                        }
                    }
                    let next =
                        openat_file(&current, &component, libc::O_RDONLY | libc::O_DIRECTORY)?;
                    next.set_permissions(permissions.clone())?;
                    next.sync_all()?;
                    current.sync_all()?;
                    current = next;
                }
                Err(error) => return Err(error),
            }
        }
        current.sync_all()?;
        let revalidated = walk_directory(&root, &normal_components(relative)?)?;
        if !same_inode(&current.metadata()?, &revalidated.metadata()?) {
            return Err(io::Error::other(
                "confined directory changed during creation",
            ));
        }
        Ok(())
    }

    fn permission_mode(permissions: &Permissions) -> libc::mode_t {
        permissions.mode() as libc::mode_t
    }

    fn openat_file(parent: &File, name: &CString, flags: libc::c_int) -> io::Result<File> {
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                flags | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
                0o600,
            )
        };
        if fd == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(unsafe {
            // `openat` returned a new owned descriptor.
            File::from_raw_fd(fd)
        })
    }

    fn normal_components(path: &Path) -> io::Result<Vec<CString>> {
        path.components()
            .map(|component| match component {
                Component::Normal(value) => CString::new(value.as_bytes()).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "path component contains NUL")
                }),
                _ => Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("non-normal path component in {}", path.display()),
                )),
            })
            .collect()
    }

    fn normal_name(value: Option<&std::ffi::OsStr>, path: &Path) -> io::Result<CString> {
        let value = value.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("destination {} has no file name", path.display()),
            )
        })?;
        CString::new(value.as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file name contains NUL"))
    }

    fn same_inode(left: &Metadata, right: &Metadata) -> bool {
        left.dev() == right.dev() && left.ino() == right.ino()
    }

    fn cvt(value: libc::c_int) -> io::Result<libc::c_int> {
        if value == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(value)
        }
    }
}

#[cfg(unix)]
use platform::ConfinedParent;

#[cfg(unix)]
fn ensure_confined_directory_sync_under_root(
    codex_home: &Path,
    directory: &Path,
    permissions: Permissions,
    expected_root: Option<&ConfinedRootIdentity>,
) -> io::Result<()> {
    platform::ensure_directory(codex_home, directory, permissions, expected_root)
}

#[cfg(unix)]
#[cfg(test)]
fn read_confined_file_sync(
    codex_home: &Path,
    path: &Path,
) -> io::Result<(Vec<u8>, ConfinedFileSnapshot)> {
    read_confined_file_sync_under_root(codex_home, path, /*expected_root*/ None)
}

#[cfg(unix)]
fn read_confined_file_sync_under_root(
    codex_home: &Path,
    path: &Path,
    expected_root: Option<&ConfinedRootIdentity>,
) -> io::Result<(Vec<u8>, ConfinedFileSnapshot)> {
    let confined = ConfinedParent::open(codex_home, path, expected_root)?;
    confined.revalidate()?;
    confined.cleanup_staged_entries()?;
    let mut file = confined.open_destination()?;
    let (bytes, snapshot) = snapshot_file(&mut file)?;
    confined.revalidate()?;
    Ok((bytes, snapshot))
}

#[cfg(unix)]
fn confined_entry_exists_sync_under_root(
    codex_home: &Path,
    path: &Path,
    expected_root: Option<&ConfinedRootIdentity>,
) -> io::Result<bool> {
    let confined = ConfinedParent::open(codex_home, path, expected_root)?;
    confined.revalidate()?;
    confined.destination_exists()
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
#[cfg(test)]
fn replace_confined_file_sync(
    codex_home: &Path,
    destination: &Path,
    expected: &ConfinedFileSnapshot,
    replacement: &[u8],
) -> io::Result<ConfinedMutationOutcome> {
    replace_confined_file_sync_under_root(
        codex_home,
        destination,
        expected,
        replacement,
        /*expected_root*/ None,
    )
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
fn replace_confined_file_sync_under_root(
    codex_home: &Path,
    destination: &Path,
    expected: &ConfinedFileSnapshot,
    replacement: &[u8],
    expected_root: Option<&ConfinedRootIdentity>,
) -> io::Result<ConfinedMutationOutcome> {
    let confined = ConfinedParent::open(codex_home, destination, expected_root)?;
    confined.revalidate()?;
    confined.cleanup_staged_entries()?;
    verify_opened_file(confined.open_destination()?, expected)?;
    let (staged_name, mut staged) = confined.create_staged()?;
    let precommit = (|| {
        staged.write_all(replacement)?;
        staged.set_permissions(expected.permissions.clone())?;
        if let Some(modified) = expected.modified {
            staged.set_times(FileTimes::new().set_modified(modified))?;
        }
        staged.sync_all()?;
        let staged_metadata = staged.metadata()?;
        confined.revalidate()?;
        verify_opened_file(confined.open_destination()?, expected)?;
        Ok::<Metadata, io::Error>(staged_metadata)
    })();
    let staged_metadata = match precommit {
        Ok(metadata) => metadata,
        Err(error) => {
            let _ = confined.unlink(&staged_name);
            return Err(error);
        }
    };

    if let Err(error) = confined.exchange_with_destination(&staged_name) {
        let _ = confined.unlink(&staged_name);
        let _ = confined.sync();
        return Err(error);
    }
    #[cfg(test)]
    if take_crash_boundary(
        destination,
        ConfinedCrashBoundary::AfterExchangeBeforeParentSync,
    ) {
        return Ok(ConfinedMutationOutcome::DurabilityUnknown {
            error: io::Error::other("injected crash after exchange before parent sync"),
        });
    }
    let displaced_verification = confined
        .open_named(&staged_name)
        .and_then(|file| verify_opened_file(file, expected));
    if let Err(error) = displaced_verification {
        // The destination now holds the staged replacement, but another writer changed the
        // displaced inode despite the caller's writer exclusion. A second exchange cannot be a
        // conditional rollback: it could remove a newer destination. Preserve both names and
        // force restart instead.
        return Ok(ConfinedMutationOutcome::DurabilityUnknown { error });
    }
    if let Err(error) = confined.sync() {
        return Ok(ConfinedMutationOutcome::DurabilityUnknown { error });
    }
    #[cfg(test)]
    if take_crash_boundary(
        destination,
        ConfinedCrashBoundary::AfterParentSyncBeforeUnlink,
    ) {
        return Ok(ConfinedMutationOutcome::DurabilityUnknown {
            error: io::Error::other("injected crash after parent sync before displaced unlink"),
        });
    }
    if let Err(error) = confined.unlink(&staged_name) {
        return Ok(ConfinedMutationOutcome::DurabilityUnknown { error });
    }
    if let Err(error) = confined.sync() {
        return Ok(ConfinedMutationOutcome::DurabilityUnknown { error });
    }

    let postcommit = (|| {
        staged.sync_all()?;
        confined.verify_destination(&staged_metadata)?;
        confined.revalidate()
    })();
    match postcommit {
        Ok(()) => Ok(ConfinedMutationOutcome::Durable),
        Err(error) => Ok(ConfinedMutationOutcome::DurabilityUnknown { error }),
    }
}

#[cfg(all(
    unix,
    test,
    not(any(target_os = "linux", target_os = "android", target_os = "macos"))
))]
fn replace_confined_file_sync(
    codex_home: &Path,
    destination: &Path,
    expected: &ConfinedFileSnapshot,
    replacement: &[u8],
) -> io::Result<ConfinedMutationOutcome> {
    let _ = (codex_home, destination, expected, replacement);
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic verified rollout replacement is unsupported on this platform",
    ))
}

#[cfg(not(unix))]
fn ensure_confined_directory_sync(
    codex_home: &Path,
    directory: &Path,
    permissions: Permissions,
) -> io::Result<()> {
    let _ = (codex_home, directory, permissions);
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "confined rollout directory creation is unsupported on this platform",
    ))
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
#[cfg(test)]
fn install_confined_file_sync(
    codex_home: &Path,
    destination: &Path,
    bytes: &[u8],
    permissions: Permissions,
    modified: Option<SystemTime>,
) -> io::Result<ConfinedInstallOutcome> {
    install_confined_file_sync_under_root(
        codex_home,
        destination,
        bytes,
        permissions,
        modified,
        /*expected_root*/ None,
    )
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
fn install_confined_file_sync_under_root(
    codex_home: &Path,
    destination: &Path,
    bytes: &[u8],
    permissions: Permissions,
    modified: Option<SystemTime>,
    expected_root: Option<&ConfinedRootIdentity>,
) -> io::Result<ConfinedInstallOutcome> {
    let confined = ConfinedParent::open(codex_home, destination, expected_root)?;
    confined.revalidate()?;
    confined.cleanup_staged_entries()?;
    let (staged_name, mut staged) = confined.create_staged()?;
    let result = (|| {
        staged.write_all(bytes)?;
        staged.set_permissions(permissions.clone())?;
        if let Some(modified) = modified {
            staged.set_times(FileTimes::new().set_modified(modified))?;
        }
        staged.sync_all()?;
        let staged_metadata = staged.metadata()?;
        confined.revalidate()?;
        match confined.link_noclobber(&staged_name) {
            Ok(()) => {
                confined.verify_destination(&staged_metadata)?;
                staged.sync_all()?;
                confined.sync()?;
                confined.revalidate()?;
                Ok(ConfinedInstallOutcome::Installed)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let mut existing = confined.open_destination()?;
                let existing_metadata = existing.metadata()?;
                if read_file_from_start(&mut existing)? != bytes {
                    return Err(error);
                }
                if !permissions_match(&existing_metadata, &permissions)
                    || modified
                        .is_some_and(|modified| existing_metadata.modified().ok() != Some(modified))
                {
                    return Err(io::Error::other(
                        "confined immutable rollout metadata does not match",
                    ));
                }
                if read_file_from_start(&mut existing)? != bytes {
                    return Err(io::Error::other(
                        "confined rollout changed while reusing immutable bytes",
                    ));
                }
                confined.verify_destination(&existing_metadata)?;
                confined.sync()?;
                confined.revalidate()?;
                Ok(ConfinedInstallOutcome::Reused)
            }
            Err(error) => Err(error),
        }
    })();
    if let Err(error) = confined.unlink(&staged_name)
        && result.is_ok()
    {
        return Err(error);
    }
    confined.sync()?;
    result
}

#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "android", target_os = "macos"))
))]
fn install_confined_file_sync(
    codex_home: &Path,
    destination: &Path,
    bytes: &[u8],
    permissions: Permissions,
    modified: Option<SystemTime>,
) -> io::Result<ConfinedInstallOutcome> {
    let _ = (codex_home, destination, bytes, permissions, modified);
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "confined rollout installation is unsupported on this platform",
    ))
}

fn permissions_match(metadata: &Metadata, expected: &Permissions) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        metadata.permissions().mode() & 0o777 == expected.mode() & 0o777
    }
    #[cfg(not(unix))]
    {
        metadata.permissions().readonly() == expected.readonly()
    }
}

#[cfg(not(unix))]
fn read_confined_file_sync(
    codex_home: &Path,
    path: &Path,
) -> io::Result<(Vec<u8>, ConfinedFileSnapshot)> {
    let _ = (codex_home, path);
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "confined rollout reads are unsupported on this platform",
    ))
}

#[cfg(not(unix))]
fn confined_entry_exists_sync(codex_home: &Path, path: &Path) -> io::Result<bool> {
    let _ = (codex_home, path);
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "confined rollout entry checks are unsupported on this platform",
    ))
}

#[cfg(not(unix))]
fn replace_confined_file_sync(
    codex_home: &Path,
    destination: &Path,
    expected: &ConfinedFileSnapshot,
    replacement: &[u8],
) -> io::Result<ConfinedMutationOutcome> {
    let _ = (codex_home, destination, expected, replacement);
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic verified rollout replacement is unsupported on this platform",
    ))
}

#[cfg(not(unix))]
fn install_confined_file_sync(
    codex_home: &Path,
    destination: &Path,
    bytes: &[u8],
    permissions: Permissions,
    modified: Option<SystemTime>,
) -> io::Result<ConfinedInstallOutcome> {
    let _ = (codex_home, destination, bytes, permissions, modified);
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "confined rollout installation is unsupported on this platform",
    ))
}

fn snapshot_file(file: &mut File) -> io::Result<(Vec<u8>, ConfinedFileSnapshot)> {
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "confined rollout entry is not a regular file",
        ));
    }
    let bytes = read_file_from_start(file)?;
    let digest = Sha256::digest(bytes.as_slice()).into();
    Ok((
        bytes,
        ConfinedFileSnapshot {
            identity: FileIdentity::from_metadata(&metadata),
            digest,
            permissions: metadata.permissions(),
            modified: metadata.modified().ok(),
        },
    ))
}

fn verify_opened_file(mut file: File, expected: &ConfinedFileSnapshot) -> io::Result<()> {
    let metadata = file.metadata()?;
    let bytes = read_file_from_start(&mut file)?;
    let actual = ConfinedFileSnapshot {
        identity: FileIdentity::from_metadata(&metadata),
        digest: Sha256::digest(bytes).into(),
        permissions: metadata.permissions(),
        modified: metadata.modified().ok(),
    };
    if actual.identity != expected.identity || actual.digest != expected.digest {
        return Err(io::Error::other(
            "confined rollout source changed before publication",
        ));
    }
    Ok(())
}

fn read_file_from_start(file: &mut File) -> io::Result<Vec<u8>> {
    use std::io::Seek as _;
    file.rewind()?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

#[cfg(test)]
#[path = "confined_publication_tests.rs"]
mod tests;
