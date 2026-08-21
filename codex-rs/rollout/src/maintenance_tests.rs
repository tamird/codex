use super::*;

#[test]
fn migration_dependency_child() -> std::io::Result<()> {
    use std::io::Write;
    let Ok(home) = std::env::var("FRODEX_MIGRATION_LOCK_TEST_HOME") else {
        return Ok(());
    };
    let id = ThreadId::from_string(
        &std::env::var("FRODEX_MIGRATION_LOCK_TEST_ID").expect("child identity"),
    )
    .expect("UUID");
    let _guard = try_acquire_rollout_migration_dependency_lock(Path::new(&home), &[id])?
        .expect("child reservation");
    println!("MIGRATION_ADMITTED");
    std::io::stdout().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(())
}

#[test]
fn migration_dependencies_and_capacity_are_reserved_across_processes() -> std::io::Result<()> {
    use std::io::BufRead;
    use std::io::Write;
    use std::process::Stdio;
    let home = tempfile::tempdir()?;
    let first = ThreadId::new();
    let second = ThreadId::new();
    let mut child = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "maintenance::tests::migration_dependency_child",
            "--nocapture",
        ])
        .env("FRODEX_MIGRATION_LOCK_TEST_HOME", home.path())
        .env("FRODEX_MIGRATION_LOCK_TEST_ID", first.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let mut output = std::io::BufReader::new(child.stdout.take().expect("stdout"));
    loop {
        let mut line = String::new();
        assert_ne!(
            output.read_line(&mut line)?,
            0,
            "child exited before reservation"
        );
        if line.contains("MIGRATION_ADMITTED") {
            break;
        }
    }
    assert!(try_acquire_rollout_migration_dependency_lock(home.path(), &[first])?.is_none());
    let independent = try_acquire_rollout_migration_dependency_lock(home.path(), &[second])?
        .expect("second process overlaps");
    assert!(
        try_acquire_rollout_migration_dependency_lock(home.path(), &[ThreadId::new()])?.is_none()
    );
    assert!(try_acquire_rollout_maintenance_job_lock(home.path())?.is_none());
    child.stdin.take().expect("stdin").write_all(b"finish\n")?;
    assert!(child.wait()?.success());
    assert!(try_acquire_rollout_migration_dependency_lock(home.path(), &[first])?.is_some());
    drop(independent);
    Ok(())
}

#[test]
fn migration_release_does_not_wait_for_inherited_descriptors() -> std::io::Result<()> {
    let home = tempfile::tempdir()?;
    let first = ThreadId::new();
    let second = ThreadId::new();
    let job = try_acquire_rollout_migration_dependency_lock(home.path(), &[first])?
        .expect("migration reservation");
    let independent = try_acquire_rollout_migration_dependency_lock(home.path(), &[second])?
        .expect("independent migration");
    // dup and fork share the open file description; CLOEXEC cannot release it before exec.
    let inherited = [
        job._job.file.try_clone()?,
        job._compatibility._file.file.try_clone()?,
        job._foreground.file.try_clone()?,
        job._dependencies[0].file.try_clone()?,
        job._slot
            .as_ref()
            .expect("migration slot")
            .file
            .try_clone()?,
    ];
    drop(job);
    let next = try_acquire_rollout_migration_dependency_lock(home.path(), &[first])?;
    assert!(next.is_some(), "dependencies and capacity must be released");
    drop(next);
    assert!(try_acquire_rollout_maintenance_job_lock(home.path())?.is_none());
    assert!(try_acquire_rollout_maintenance_lock(home.path())?.is_none());
    drop(independent);
    let next = try_acquire_rollout_maintenance_job_lock(home.path())?;
    assert!(
        next.is_some(),
        "releasing migration ownership must not wait for inherited descriptors"
    );
    drop(next);
    let exclusive = try_acquire_rollout_maintenance_lock(home.path())?;
    assert!(exclusive.is_some());
    drop(exclusive);
    let compression = try_acquire_compression_maintenance(home.path())?;
    assert!(
        compression.is_some(),
        "foreground ownership must also be released"
    );
    drop(compression);
    drop(inherited);
    Ok(())
}

#[tokio::test]
async fn foreground_intent_survives_failed_attempts_and_returned_jobs() -> std::io::Result<()> {
    let home = tempfile::tempdir()?;
    let compression = try_acquire_compression_maintenance(home.path())?.expect("compression");
    let foreground = acquire_foreground_intent(home.path()).await?;
    let inherited = foreground.file.try_clone()?;
    assert!(try_acquire_foreground_job(home.path(), Arc::clone(&foreground))?.is_none());
    assert!(foreground_maintenance_waiting(home.path())?);
    drop(compression);
    let job = try_acquire_foreground_job(home.path(), Arc::clone(&foreground))?
        .expect("retry after compression releases ownership");
    drop(foreground);
    assert!(foreground_maintenance_waiting(home.path())?);
    let independent = acquire_rollout_maintenance_intent(home.path()).await?;
    drop(job);
    assert!(
        foreground_maintenance_waiting(home.path())?,
        "independent intent remains owned"
    );
    drop(independent);
    assert!(
        !foreground_maintenance_waiting(home.path())?,
        "inherited descriptors do not retain intent"
    );
    drop(inherited);
    Ok(())
}

#[test]
fn independent_migrations_overlap_but_shared_ancestors_and_capacity_do_not() -> std::io::Result<()>
{
    let home = tempfile::tempdir()?;
    let mut dependencies = [ThreadId::new(), ThreadId::new()];
    dependencies.sort_unstable_by_key(ThreadId::to_string);
    let [second, parent] = dependencies;
    let first = ThreadId::new();
    let unrelated = ThreadId::new();
    let first_job = try_acquire_rollout_migration_dependency_lock(home.path(), &[first, parent])?
        .expect("first lineage");
    assert!(
        try_acquire_rollout_migration_dependency_lock(home.path(), &[second, parent])?.is_none()
    );
    // The unsuccessful multi-lock attempt released second's partial reservation.
    let second_job = try_acquire_rollout_migration_dependency_lock(home.path(), &[second])?
        .expect("independent lineage");
    assert!(try_acquire_rollout_migration_dependency_lock(home.path(), &[unrelated])?.is_none());
    assert!(try_acquire_rollout_maintenance_job_lock(home.path())?.is_none());
    assert!(try_acquire_rollout_maintenance_lock(home.path())?.is_none());
    assert!(try_acquire_compression_maintenance(home.path())?.is_none());
    assert!(try_acquire_rollout_maintenance_read_lock(home.path())?.is_some());
    drop(first_job);
    assert!(try_acquire_rollout_migration_dependency_lock(home.path(), &[unrelated])?.is_some());
    drop(second_job);
    assert!(try_acquire_rollout_maintenance_job_lock(home.path())?.is_some());
    Ok(())
}

#[tokio::test]
async fn queued_migration_retains_compression_priority_between_attempts() -> std::io::Result<()> {
    let home = tempfile::tempdir()?;
    let compression = try_acquire_compression_maintenance(home.path())?.expect("compression");
    let intent = acquire_rollout_maintenance_intent(home.path()).await?;
    assert!(
        try_acquire_rollout_migration_dependency_lock(home.path(), &[ThreadId::new()])?.is_none()
    );
    assert!(compression.should_yield()?);
    drop(compression);
    assert!(try_acquire_compression_maintenance(home.path())?.is_none());
    drop(intent);
    assert!(try_acquire_compression_maintenance(home.path())?.is_some());
    Ok(())
}

#[test]
fn jobs_allow_readers_but_exclude_other_jobs_and_legacy_maintenance() -> std::io::Result<()> {
    let home = tempfile::tempdir()?;
    let job = try_acquire_rollout_maintenance_job_lock(home.path())?.expect("first job");
    let reader = try_acquire_rollout_maintenance_read_lock(home.path())?.expect("clean reader");
    assert!(try_acquire_rollout_maintenance_job_lock(home.path())?.is_none());
    assert!(try_acquire_rollout_maintenance_lock(home.path())?.is_none());
    drop(job);
    assert!(try_acquire_rollout_maintenance_lock(home.path())?.is_none());
    drop(reader);
    let exclusive = try_acquire_rollout_maintenance_lock(home.path())?.expect("legacy maintenance");
    assert!(try_acquire_rollout_maintenance_read_lock(home.path())?.is_none());
    assert!(try_acquire_rollout_maintenance_job_lock(home.path())?.is_none());
    drop(exclusive);
    assert!(try_acquire_rollout_maintenance_job_lock(home.path())?.is_some());
    Ok(())
}

#[tokio::test]
async fn waiting_readers_and_migrations_request_compression_yield() -> std::io::Result<()> {
    let home = tempfile::tempdir()?;
    let compression = try_acquire_compression_maintenance(home.path())?.expect("compression");
    let read_home = home.path().to_path_buf();
    let reader =
        tokio::spawn(async move { acquire_rollout_maintenance_read_lock(&read_home).await });
    let job_home = home.path().to_path_buf();
    let job = tokio::spawn(async move { acquire_rollout_maintenance_job_lock(&job_home).await });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !compression.should_yield().expect("check foreground intent") {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("foreground intent is retained while waiting");
    assert!(try_acquire_rollout_maintenance_lock(home.path())?.is_none());
    drop(compression);
    let reader = tokio::time::timeout(std::time::Duration::from_secs(2), reader)
        .await
        .expect("reader proceeds")
        .expect("join reader")?;
    let job = tokio::time::timeout(std::time::Duration::from_secs(2), job)
        .await
        .expect("migration proceeds")
        .expect("join migration")?;
    assert!(try_acquire_compression_maintenance(home.path())?.is_none());
    drop(reader);
    drop(job);
    assert!(try_acquire_compression_maintenance(home.path())?.is_some());
    Ok(())
}
