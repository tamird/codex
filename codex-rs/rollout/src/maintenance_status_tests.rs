use super::*;
use crate::read_rollout_maintenance_status;
use crate::try_acquire_rollout_maintenance;
use crate::try_acquire_rollout_maintenance_lock;
use pretty_assertions::assert_eq;

#[test]
fn reporter_release_does_not_wait_for_inherited_descriptors() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let activity = RolloutMaintenanceActivity::new(
        RolloutMaintenanceOperation::HistoryRepair,
        /*thread_id*/ None,
    );
    let guard = try_acquire_rollout_maintenance(home.path(), activity)?
        .expect("reported maintenance reservation");
    let reporter = guard.reporter().expect("maintenance reporter");
    assert_eq!(
        read_rollout_maintenance_status(home.path())?,
        RolloutMaintenanceStatus::Busy {
            owner: Some(activity)
        }
    );
    let inherited = {
        let state = reporter.0.lock().expect("reporter state");
        state
            .publication
            .as_ref()
            .expect("active publication")
            .lease
            .as_ref()
            .expect("reporter lease")
            .file
            .try_clone()?
    };
    drop(guard);
    let legacy = try_acquire_rollout_maintenance_lock(home.path())?
        .expect("legacy maintenance reservation after reported owner releases");
    reporter.update(RolloutMaintenanceActivity {
        phase: RolloutMaintenancePhase::Verifying,
        ..activity
    });
    assert_eq!(
        read_rollout_maintenance_status(home.path())?,
        RolloutMaintenanceStatus::Busy { owner: None }
    );
    drop(legacy);
    assert_eq!(
        read_rollout_maintenance_status(home.path())?,
        RolloutMaintenanceStatus::Idle
    );
    drop(inherited);
    Ok(())
}
