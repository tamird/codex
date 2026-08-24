use super::*;

#[test]
fn writer_release_does_not_wait_for_inherited_file_descriptors() -> io::Result<()> {
    let home = tempfile::TempDir::new()?;
    let coordinator = Arc::new(RolloutWriterLockCoordinator::new(home.path()));
    let thread_id = ThreadId::new();
    let guard = coordinator.try_acquire(thread_id)?.expect("writer");
    // dup and fork share the open file description. CLOEXEC does not help while a child is
    // still preparing to exec, so closing the parent's descriptor alone retains flock ownership.
    let inherited = match &guard.state {
        WriterLockState::Exclusive(file) => file.try_clone()?,
        _ => panic!("expected exclusive writer"),
    };
    drop(guard);
    let next_writer = coordinator.try_acquire(thread_id)?;
    assert!(
        next_writer.is_some(),
        "shutdown must release the writer before inherited descriptors close"
    );
    drop(inherited);
    Ok(())
}

#[test]
fn writer_release_preserves_independently_acquired_fork_reader() -> io::Result<()> {
    let home = tempfile::TempDir::new()?;
    let coordinator = Arc::new(RolloutWriterLockCoordinator::new(home.path()));
    let other = Arc::new(RolloutWriterLockCoordinator::new(home.path()));
    let thread_id = ThreadId::new();
    let mut writer = coordinator.try_acquire(thread_id)?.expect("writer");
    writer.share_for_fork()?;
    let reader = other
        .try_acquire_fork_reader(thread_id)?
        .expect("fork reader");
    let inherited = match &writer.state {
        WriterLockState::Shared(file) => file.try_clone()?,
        _ => panic!("expected shared writer"),
    };
    drop(writer);
    assert!(
        coordinator.try_acquire(thread_id)?.is_none(),
        "the independent reader must still exclude archive"
    );
    drop(reader);
    assert!(
        coordinator.try_acquire(thread_id)?.is_some(),
        "only independent reservations may retain ownership"
    );
    drop(inherited);
    Ok(())
}
