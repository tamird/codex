use std::io::BufRead;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;

use pretty_assertions::assert_eq;

use super::*;
use crate::RolloutMaintenanceOperation;
use crate::RolloutMaintenancePhase;
use crate::RolloutMaintenanceRequestScope;
use crate::with_rollout_maintenance_observer;

const CHILD_HOME: &str = "CODEX_ROLLOUT_MAINTENANCE_TEST_HOME";
const CHILD_MODE: &str = "CODEX_ROLLOUT_MAINTENANCE_TEST_MODE";

fn start_holder(home: &Path, mode: &str) -> Child {
    let mut child = Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--ignored",
            "--exact",
            "maintenance::tests::holder_child",
            "--nocapture",
        ])
        .env(CHILD_HOME, home)
        .env(CHILD_MODE, mode)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start lock holder");
    let mut output = std::io::BufReader::new(child.stdout.take().expect("child stdout"));
    let mut line = String::new();
    loop {
        line.clear();
        assert_ne!(
            output.read_line(&mut line).expect("read readiness"),
            0,
            "holder exited before readiness"
        );
        if line.trim() == "maintenance-ready" {
            break;
        }
    }
    child.stdout = Some(output.into_inner());
    child
}

#[test]
#[ignore = "subprocess fixture for owner handoff"]
fn holder_child() {
    let home = std::env::var_os(CHILD_HOME).expect("child home");
    let home = Path::new(&home);
    let reported = std::env::var(CHILD_MODE).expect("child mode") == "reported";
    let guard = if reported {
        try_acquire_rollout_maintenance(
            home,
            RolloutMaintenanceActivity {
                phase: RolloutMaintenancePhase::Staging,
                ..RolloutMaintenanceActivity::new(
                    RolloutMaintenanceOperation::BackgroundMigration,
                    /*thread_id*/ None,
                )
            },
        )
    } else {
        try_acquire_rollout_maintenance_lock(home)
    }
    .expect("acquire lock")
    .expect("available lock");
    println!("maintenance-ready");
    std::io::stdout().flush().expect("flush readiness");
    let _ = std::io::stdin().read_exact(&mut [0_u8]);
    if reported {
        std::process::exit(0);
    }
    drop(guard);
}

#[test]
fn current_owner_requires_a_live_reporter_lease() {
    let home = tempfile::tempdir().expect("temporary home");
    let mut reported = start_holder(home.path(), "reported");
    let RolloutMaintenanceStatus::Busy { owner: Some(owner) } =
        read_rollout_maintenance_status(home.path()).expect("reported status")
    else {
        panic!("reported process must own maintenance")
    };
    assert_eq!(
        (owner.process_id, owner.operation, owner.phase),
        (
            reported.id(),
            RolloutMaintenanceOperation::BackgroundMigration,
            RolloutMaintenancePhase::Staging
        )
    );
    // Process death leaves a valid snapshot behind, but releases both kernel locks.
    drop(reported.stdin.take());
    assert!(reported.wait().expect("reap owner").success());
    assert_eq!(
        read_rollout_maintenance_status(home.path()).expect("idle status"),
        RolloutMaintenanceStatus::Idle
    );

    let mut legacy = start_holder(home.path(), "legacy");
    let reader = crate::maintenance_status::open_lock(
        &home.path().join(".tmp/rollout-maintenance-reporter.lock"),
    )
    .expect("open concurrent reporter reader");
    reader
        .try_lock_shared()
        .expect("hold shared reporter probe");
    assert_eq!(
        read_rollout_maintenance_status(home.path()).expect("legacy status"),
        RolloutMaintenanceStatus::Busy { owner: None }
    );
    drop(reader);
    drop(legacy.stdin.take());
    assert!(legacy.wait().expect("reap legacy owner").success());
    assert_eq!(
        read_rollout_maintenance_status(home.path()).expect("finished status"),
        RolloutMaintenanceStatus::Idle
    );
    let idle_reader =
        crate::maintenance_status::open_lock(&home.path().join(".tmp/rollout-maintenance.lock"))
            .expect("open concurrent maintenance reader");
    idle_reader
        .try_lock_shared()
        .expect("hold shared maintenance probe");
    assert_eq!(
        read_rollout_maintenance_status(home.path()).expect("concurrent idle status"),
        RolloutMaintenanceStatus::Idle
    );
    drop(idle_reader);

    let probe = crate::maintenance_status::open_lock(
        &home.path().join(".tmp/rollout-maintenance-reporter.lock"),
    )
    .expect("open reporter probe");
    probe.try_lock_shared().expect("hold reporter probe");
    let previous_snapshot: serde_json::Value = serde_json::from_slice(
        &std::fs::read(home.path().join(".tmp/rollout-maintenance-status.json"))
            .expect("previous snapshot"),
    )
    .expect("decode previous snapshot");
    let guard = try_acquire_rollout_maintenance(home.path(), owner)
        .expect("acquire while probe is active")
        .expect("maintenance is available");
    let next_snapshot: serde_json::Value = serde_json::from_slice(
        &std::fs::read(home.path().join(".tmp/rollout-maintenance-status.json"))
            .expect("next snapshot"),
    )
    .expect("decode next snapshot");
    assert_ne!(
        previous_snapshot["generation"], next_snapshot["generation"],
        "reusing an activity must not reuse its lock-acquisition generation"
    );
    let reporter = guard.reporter().expect("reporter");
    assert_eq!(reporter.activity().process_id, std::process::id());
    assert_eq!(
        read_rollout_maintenance_status(home.path()).expect("contended reporter"),
        RolloutMaintenanceStatus::Busy { owner: None }
    );
    drop(probe);
    let mut next = reporter.activity();
    next.phase = RolloutMaintenancePhase::Projecting;
    reporter.update(next);
    assert_eq!(
        read_rollout_maintenance_status(home.path()).expect("retried reporter"),
        RolloutMaintenanceStatus::Busy { owner: Some(next) }
    );
    drop(guard);
    let _legacy = try_acquire_rollout_maintenance_lock(home.path())
        .expect("raw lock")
        .expect("available");
    next.phase = RolloutMaintenancePhase::Verifying;
    reporter.update(next);
    assert_eq!(
        read_rollout_maintenance_status(home.path()).expect("released reporter"),
        RolloutMaintenanceStatus::Busy { owner: None }
    );
}

#[tokio::test]
async fn canceled_wait_and_last_reporter_clear_only_their_own_status() {
    use std::future::Future;
    use std::task::Poll;

    let home = tempfile::tempdir().expect("temporary home");
    let held = try_acquire_rollout_maintenance_lock(home.path())
        .expect("raw lock")
        .expect("available");
    let statuses = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = std::sync::Arc::clone(&statuses);
    let escaped = with_rollout_maintenance_observer(
        std::sync::Arc::new(move |status| {
            observed.lock().expect("statuses").push(status);
        }),
        async {
            let activity = RolloutMaintenanceActivity::new(
                RolloutMaintenanceOperation::HistoryRepair,
                /*thread_id*/ None,
            );
            let mut pending = Box::pin(acquire_rollout_maintenance(home.path(), activity));
            assert!(
                std::future::poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            drop(pending);
            assert_eq!(
                statuses.lock().expect("statuses").last(),
                Some(&RolloutMaintenanceRequestStatus::Idle)
            );
            drop(held);
            let guard = acquire_rollout_maintenance(home.path(), activity)
                .await
                .expect("reported guard");
            let reporter = guard.reporter().expect("retained migration reporter");
            drop(guard);
            assert_eq!(
                statuses.lock().expect("statuses").last(),
                Some(&RolloutMaintenanceRequestStatus::Running { activity })
            );
            let newer = RolloutMaintenanceRequestScope::new(
                RolloutMaintenanceRequestStatus::WaitingForMaintenance {
                    thread_id: None,
                    owner: None,
                },
            );
            drop(reporter);
            assert_eq!(
                statuses.lock().expect("statuses").last(),
                Some(&RolloutMaintenanceRequestStatus::WaitingForMaintenance {
                    thread_id: None,
                    owner: None
                })
            );
            drop(newer);
            assert_eq!(
                statuses.lock().expect("statuses").last(),
                Some(&RolloutMaintenanceRequestStatus::Idle)
            );
            RolloutMaintenanceRequestScope::new(RolloutMaintenanceRequestStatus::Running {
                activity,
            })
        },
    )
    .await;
    let count = statuses.lock().expect("statuses").len();
    escaped.update(RolloutMaintenanceRequestStatus::WaitingForMaintenance {
        thread_id: None,
        owner: None,
    });
    drop(escaped);
    assert_eq!(
        statuses.lock().expect("statuses").len(),
        count,
        "a completed request cannot be revived"
    );
}

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
            .1
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
    let first_activity = RolloutMaintenanceActivity::new(
        RolloutMaintenanceOperation::BackgroundMigration,
        Some(first),
    );
    let second_activity = RolloutMaintenanceActivity::new(
        RolloutMaintenanceOperation::BackgroundMigration,
        Some(second),
    );
    let first_job = try_acquire_rollout_migration_dependency_lock(home.path(), &[first, parent])?
        .expect("first lineage")
        .with_activity(home.path(), first_activity);
    assert!(
        try_acquire_rollout_migration_dependency_lock(home.path(), &[second, parent])?.is_none()
    );
    // The unsuccessful multi-lock attempt released second's partial reservation.
    let second_job = try_acquire_rollout_migration_dependency_lock(home.path(), &[second])?
        .expect("independent lineage")
        .with_activity(home.path(), second_activity);
    assert_eq!(
        read_rollout_maintenance_status(home.path())?,
        RolloutMaintenanceStatus::Busy {
            owner: Some(first_activity),
        }
    );
    assert_eq!(
        read_rollout_maintenance_exclusive_status(home.path())?,
        RolloutMaintenanceStatus::Idle,
        "an independent slot owner is not necessarily the dependency blocker"
    );
    assert!(try_acquire_rollout_migration_dependency_lock(home.path(), &[unrelated])?.is_none());
    assert!(try_acquire_rollout_maintenance_job_lock(home.path())?.is_none());
    assert!(try_acquire_rollout_maintenance_lock(home.path())?.is_none());
    assert!(try_acquire_compression_maintenance(home.path())?.is_none());
    assert!(try_acquire_rollout_maintenance_read_lock(home.path())?.is_some());
    drop(first_job);
    assert_eq!(
        read_rollout_maintenance_status(home.path())?,
        RolloutMaintenanceStatus::Busy {
            owner: Some(second_activity),
        }
    );
    assert!(try_acquire_rollout_migration_dependency_lock(home.path(), &[unrelated])?.is_some());
    drop(second_job);
    assert_eq!(
        read_rollout_maintenance_status(home.path())?,
        RolloutMaintenanceStatus::Idle
    );
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
