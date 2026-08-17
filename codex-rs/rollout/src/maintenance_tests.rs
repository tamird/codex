use super::*;

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
