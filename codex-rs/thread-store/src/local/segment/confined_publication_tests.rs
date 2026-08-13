use std::time::Duration;
use std::time::SystemTime;

use tempfile::TempDir;

use super::*;

async fn reset_crash_injection() -> tokio::sync::OwnedMutexGuard<()> {
    let guard = std::sync::Arc::clone(&CRASH_TEST_LOCK).lock_owned().await;
    CRASH_BOUNDARIES
        .lock()
        .expect("confined crash boundary mutex")
        .clear();
    guard
}

#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
async fn replacement_requires_the_same_opened_source_and_preserves_metadata() {
    let _guard = reset_crash_injection().await;
    let home = TempDir::new().expect("home");
    let parent = home.path().join("sessions");
    tokio::fs::create_dir(parent.as_path())
        .await
        .expect("create parent");
    let path = parent.join("rollout.jsonl");
    tokio::fs::write(path.as_path(), b"source")
        .await
        .expect("write source");
    let old = SystemTime::now() - Duration::from_secs(300);
    std::fs::File::open(path.as_path())
        .expect("open source")
        .set_times(FileTimes::new().set_modified(old))
        .expect("set mtime");
    let (_, snapshot) = read_confined_file(home.path(), path.as_path())
        .await
        .expect("snapshot");

    tokio::fs::write(path.as_path(), b"changed")
        .await
        .expect("change source");
    let error = replace_confined_file(home.path(), path.as_path(), &snapshot, b"repair")
        .await
        .expect_err("changed source");
    assert!(error.to_string().contains("changed before publication"));
    assert_eq!(tokio::fs::read(path).await.expect("current"), b"changed");
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
#[tokio::test]
async fn replacement_preserves_mode_and_mtime() {
    let _guard = reset_crash_injection().await;
    use std::os::unix::fs::PermissionsExt as _;

    let home = TempDir::new().expect("home");
    let path = home.path().join("rollout.jsonl");
    tokio::fs::write(path.as_path(), b"source")
        .await
        .expect("write source");
    tokio::fs::set_permissions(path.as_path(), std::fs::Permissions::from_mode(0o640))
        .await
        .expect("chmod source");
    let old = SystemTime::now() - Duration::from_secs(300);
    std::fs::File::open(path.as_path())
        .expect("open source")
        .set_times(FileTimes::new().set_modified(old))
        .expect("set mtime");
    let (_, snapshot) = read_confined_file(home.path(), path.as_path())
        .await
        .expect("snapshot");

    let outcome = replace_confined_file(home.path(), path.as_path(), &snapshot, b"repair")
        .await
        .expect("replace");
    match outcome {
        ConfinedMutationOutcome::Durable => {}
        ConfinedMutationOutcome::DurabilityUnknown { error } => {
            panic!("replacement durability unknown: {error}")
        }
    }
    let metadata = tokio::fs::metadata(path.as_path()).await.expect("metadata");
    assert_eq!(metadata.permissions().mode() & 0o777, 0o640);
    assert_eq!(metadata.modified().expect("mtime"), old);
    assert_eq!(tokio::fs::read(path).await.expect("bytes"), b"repair");
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_parent_cannot_escape_codex_home() {
    use std::os::unix::fs::symlink;

    let home = TempDir::new().expect("home");
    let outside = TempDir::new().expect("outside");
    symlink(outside.path(), home.path().join("linked")).expect("symlink parent");
    let path = home.path().join("linked").join("rollout.jsonl");
    tokio::fs::write(outside.path().join("rollout.jsonl"), b"outside")
        .await
        .expect("outside file");
    let error = read_confined_file(home.path(), path.as_path())
        .await
        .expect_err("symlink parent");
    assert!(matches!(
        error.raw_os_error(),
        Some(libc::ELOOP | libc::ENOTDIR)
    ));
    assert_eq!(
        tokio::fs::read(outside.path().join("rollout.jsonl"))
            .await
            .expect("outside bytes"),
        b"outside"
    );
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
#[tokio::test]
async fn symlinked_codex_home_can_publish_without_following_descendant_links() {
    let _guard = reset_crash_injection().await;
    use std::os::unix::fs::symlink;

    let physical = TempDir::new().expect("physical home");
    let link_parent = TempDir::new().expect("link parent");
    let linked_home = link_parent.path().join(".codex");
    symlink(physical.path(), linked_home.as_path()).expect("symlink CODEX_HOME");
    let path = linked_home.join("rollout.jsonl");
    tokio::fs::write(path.as_path(), b"source").await.unwrap();
    let (_, snapshot) = read_confined_file(linked_home.as_path(), path.as_path())
        .await
        .expect("read through CODEX_HOME symlink");
    let outcome =
        replace_confined_file(linked_home.as_path(), path.as_path(), &snapshot, b"repair")
            .await
            .expect("publish through CODEX_HOME symlink");
    assert!(matches!(outcome, ConfinedMutationOutcome::Durable));
    assert_eq!(
        tokio::fs::read(physical.path().join("rollout.jsonl"))
            .await
            .unwrap(),
        b"repair"
    );
}

#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
async fn immutable_install_never_replaces_different_bytes_and_reuses_equal_bytes() {
    let home = TempDir::new().expect("home");
    let parent = home.path().join("rotated");
    tokio::fs::create_dir(parent.as_path())
        .await
        .expect("create parent");
    let path = parent.join("rollout.jsonl");
    let permissions = private_permissions();

    assert_eq!(
        install_confined_file(
            home.path(),
            path.as_path(),
            b"bytes",
            permissions.clone(),
            /*modified*/ None,
        )
        .await
        .expect("install"),
        ConfinedInstallOutcome::Installed
    );
    assert_eq!(
        install_confined_file(
            home.path(),
            path.as_path(),
            b"bytes",
            permissions.clone(),
            /*modified*/ None,
        )
        .await
        .expect("reuse"),
        ConfinedInstallOutcome::Reused
    );
    let error = install_confined_file(
        home.path(),
        path.as_path(),
        b"other",
        permissions,
        /*modified*/ None,
    )
    .await
    .expect_err("different existing bytes");
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(tokio::fs::read(path).await.expect("bytes"), b"bytes");
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
#[tokio::test]
async fn immutable_reuse_does_not_mutate_a_hard_link_target() {
    use std::os::unix::fs::PermissionsExt as _;

    let home = TempDir::new().expect("home");
    let outside = TempDir::new().expect("outside");
    let target = outside.path().join("shared.jsonl");
    let destination = home.path().join("rollout.jsonl");
    tokio::fs::write(target.as_path(), b"bytes").await.unwrap();
    tokio::fs::set_permissions(target.as_path(), Permissions::from_mode(0o640))
        .await
        .unwrap();
    tokio::fs::hard_link(target.as_path(), destination.as_path())
        .await
        .unwrap();

    let outcome = install_confined_file(
        home.path(),
        destination.as_path(),
        b"bytes",
        Permissions::from_mode(0o640),
        /*modified*/ None,
    )
    .await
    .expect("reuse equal hard-linked bytes without mutation");
    assert_eq!(outcome, ConfinedInstallOutcome::Reused);
    assert_eq!(
        tokio::fs::metadata(target.as_path())
            .await
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o640
    );
}

#[cfg(unix)]
#[tokio::test]
async fn confined_read_rejects_a_fifo_without_blocking() {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt as _;

    let home = TempDir::new().expect("home");
    let path = home.path().join("rollout.jsonl");
    let c_path = CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        read_confined_file(home.path(), path.as_path()),
    )
    .await
    .expect("confined read must not block");
    let error = result.expect_err("fifo must be rejected");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
#[tokio::test]
async fn read_removes_a_crash_left_staged_entry_before_returning() {
    use std::os::unix::ffi::OsStrExt as _;

    let home = TempDir::new().expect("home");
    let path = home.path().join("rollout.jsonl");
    tokio::fs::write(path.as_path(), b"source")
        .await
        .expect("write source");
    let confined = ConfinedParent::open(home.path(), path.as_path(), /*expected_root*/ None)
        .expect("open parent");
    let staged = confined
        .create_stale_staged_for_test(b"stale repair")
        .expect("stage repair");
    let staged_path = home
        .path()
        .join(std::ffi::OsStr::from_bytes(staged.as_bytes()));
    assert!(tokio::fs::try_exists(staged_path.as_path()).await.unwrap());

    let (bytes, _) = read_confined_file(home.path(), path.as_path())
        .await
        .expect("read canonical source");
    assert_eq!(bytes, b"source");
    assert!(!tokio::fs::try_exists(staged_path).await.unwrap());
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
#[tokio::test]
async fn staged_cleanup_is_scoped_to_one_destination() {
    use std::os::unix::ffi::OsStrExt as _;

    let home = TempDir::new().expect("home");
    let first_path = home.path().join("first.jsonl");
    let second_path = home.path().join("second.jsonl");
    tokio::fs::write(first_path.as_path(), b"first")
        .await
        .unwrap();
    tokio::fs::write(second_path.as_path(), b"second")
        .await
        .unwrap();
    let first = ConfinedParent::open(
        home.path(),
        first_path.as_path(),
        /*expected_root*/ None,
    )
    .unwrap();
    let second = ConfinedParent::open(
        home.path(),
        second_path.as_path(),
        /*expected_root*/ None,
    )
    .unwrap();
    let first_staged = first.create_stale_staged_for_test(b"first repair").unwrap();
    let second_staged = second
        .create_stale_staged_for_test(b"second repair")
        .unwrap();
    let first_staged_path = home
        .path()
        .join(std::ffi::OsStr::from_bytes(first_staged.as_bytes()));
    let second_staged_path = home
        .path()
        .join(std::ffi::OsStr::from_bytes(second_staged.as_bytes()));

    read_confined_file(home.path(), first_path.as_path())
        .await
        .expect("read first");
    assert!(!tokio::fs::try_exists(first_staged_path).await.unwrap());
    assert!(tokio::fs::try_exists(second_staged_path).await.unwrap());
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
#[tokio::test]
async fn retry_cleans_displaced_source_after_exchange_before_parent_sync() {
    let _guard = reset_crash_injection().await;
    let home = TempDir::new().expect("home");
    let path = home.path().join("rollout.jsonl");
    tokio::fs::write(path.as_path(), b"source").await.unwrap();
    let (_, source_snapshot) = read_confined_file(home.path(), path.as_path())
        .await
        .unwrap();
    inject_crash_boundary(
        path.as_path(),
        ConfinedCrashBoundary::AfterExchangeBeforeParentSync,
    );
    let outcome = replace_confined_file(home.path(), path.as_path(), &source_snapshot, b"repair")
        .await
        .expect("injected outcome");
    assert!(matches!(
        outcome,
        ConfinedMutationOutcome::DurabilityUnknown { .. }
    ));
    assert_eq!(tokio::fs::read(path.as_path()).await.unwrap(), b"repair");

    let (repair, repair_snapshot) = read_confined_file(home.path(), path.as_path())
        .await
        .unwrap();
    assert_eq!(repair, b"repair");
    let retry = replace_confined_file(home.path(), path.as_path(), &repair_snapshot, b"repair")
        .await
        .expect("retry publication");
    assert!(matches!(retry, ConfinedMutationOutcome::Durable));
    assert_eq!(tokio::fs::read(path.as_path()).await.unwrap(), b"repair");
    assert_no_repair_staging(home.path()).await;
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
#[tokio::test]
async fn retry_cleans_displaced_source_after_parent_sync_before_unlink() {
    let _guard = reset_crash_injection().await;
    let home = TempDir::new().expect("home");
    let path = home.path().join("rollout.jsonl");
    tokio::fs::write(path.as_path(), b"source").await.unwrap();
    let (_, source_snapshot) = read_confined_file(home.path(), path.as_path())
        .await
        .unwrap();
    inject_crash_boundary(
        path.as_path(),
        ConfinedCrashBoundary::AfterParentSyncBeforeUnlink,
    );
    let outcome = replace_confined_file(home.path(), path.as_path(), &source_snapshot, b"repair")
        .await
        .expect("injected outcome");
    assert!(matches!(
        outcome,
        ConfinedMutationOutcome::DurabilityUnknown { .. }
    ));
    assert_eq!(tokio::fs::read(path.as_path()).await.unwrap(), b"repair");

    let (repair, repair_snapshot) = read_confined_file(home.path(), path.as_path())
        .await
        .unwrap();
    assert_eq!(repair, b"repair");
    let retry = replace_confined_file(home.path(), path.as_path(), &repair_snapshot, b"repair")
        .await
        .expect("retry publication");
    assert!(matches!(retry, ConfinedMutationOutcome::Durable));
    assert_eq!(tokio::fs::read(path.as_path()).await.unwrap(), b"repair");
    assert_no_repair_staging(home.path()).await;
}

#[cfg(unix)]
async fn assert_no_repair_staging(directory: &std::path::Path) {
    let mut entries = tokio::fs::read_dir(directory)
        .await
        .expect("read directory");
    while let Some(entry) = entries.next_entry().await.expect("next entry") {
        assert!(
            !entry
                .file_name()
                .to_string_lossy()
                .starts_with(".codex-history-repair-"),
            "staged entry remained at {}",
            entry.path().display()
        );
    }
}

fn private_permissions() -> Permissions {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        Permissions::from_mode(0o600)
    }
    #[cfg(not(unix))]
    {
        std::fs::metadata(".")
            .expect("current directory")
            .permissions()
    }
}
