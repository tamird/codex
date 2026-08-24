use std::io::Read;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::process::Command;
use std::time::Duration;

use codex_protocol::ThreadId;
use tempfile::TempDir;
use uuid::Uuid;

use crate::ArchiveThreadParams;
use crate::ThreadStore;
use crate::local::LocalThreadStore;
use crate::local::test_support::test_config;
use crate::local::test_support::write_session_file;

#[tokio::test]
async fn archive_after_writer_shutdown_does_not_wait_for_subprocess_exec() {
    let home = TempDir::new().expect("home");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let id = Uuid::from_u128(304);
    let thread_id = ThreadId::from_string(&id.to_string()).expect("thread id");
    let path = write_session_file(home.path(), "2025-01-03T12-00-00", id).expect("rollout");
    let writer = store
        .writer_lock_coordinator
        .acquire(thread_id)
        .expect("writer");
    let (mut parent, mut child) = UnixStream::pair().expect("handshake");
    parent
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("parent timeout");
    child
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("child timeout");
    let subprocess = std::thread::spawn(move || {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", ":"]);
        // SAFETY: the callback only reads and writes already-open descriptors. No locks or
        // allocations are used between fork and exec. The timeout also bounds failed tests.
        unsafe {
            command.pre_exec(move || {
                child.write_all(&[1])?;
                child.read_exact(&mut [0])?;
                Ok(())
            });
        }
        command.spawn().and_then(|mut process| process.wait())
    });
    let ready = parent.read_exact(&mut [0]);
    // Session shutdown drops this same guard. The child has inherited its CLOEXEC descriptor,
    // but has not executed a program yet, so close alone cannot release the writer's flock.
    drop(writer);
    let archived = if ready.is_ok() {
        Some(
            store
                .archive_thread(ArchiveThreadParams { thread_id })
                .await,
        )
    } else {
        None
    };
    let released = parent.write_all(&[1]);
    let child_result = subprocess.join().expect("spawn worker");
    // Reap the child before asserting the archive result, including on the expected red run.
    ready.expect("child reached pre_exec");
    released.expect("release child");
    assert!(child_result.expect("subprocess").success());
    archived
        .expect("archive attempted")
        .expect("first archive must succeed");
    assert!(!path.exists());
}
