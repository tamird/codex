use super::maintenance_line;
use codex_protocol::ThreadId;
use codex_rollout::RolloutMaintenanceActivity;
use codex_rollout::RolloutMaintenanceOperation;
use codex_rollout::RolloutMaintenancePhase;
use codex_rollout::RolloutMaintenanceProgress;
use codex_rollout::RolloutMaintenanceProgressUnit;
use codex_rollout::RolloutMaintenanceRequestStatus;
use pretty_assertions::assert_eq;

#[test]
fn reports_measured_phase_progress_and_external_lock_waits() {
    let thread_id =
        ThreadId::from_string("00000000-0000-0000-0000-000000000001").expect("thread ID");
    let activity = RolloutMaintenanceActivity {
        process_id: 42,
        phase: RolloutMaintenancePhase::Projecting,
        progress: Some(RolloutMaintenanceProgress {
            completed: 2,
            total: Some(5),
            unit: RolloutMaintenanceProgressUnit::Segments,
        }),
        io_bytes: 1024,
        ..RolloutMaintenanceActivity::new(
            RolloutMaintenanceOperation::BackgroundMigration,
            Some(thread_id),
        )
    };
    assert_eq!(
        [
            RolloutMaintenanceRequestStatus::Running { activity },
            RolloutMaintenanceRequestStatus::WaitingForMaintenance {
                thread_id: None,
                owner: Some(activity),
            },
            RolloutMaintenanceRequestStatus::WaitingForMaintenance {
                thread_id: None,
                owner: None,
            },
            RolloutMaintenanceRequestStatus::Idle,
        ]
        .map(maintenance_line),
        [
            Some(format!(
                "Projecting history {thread_id}  2/5 segments  •  1.0 KB handled"
            )),
            Some(format!(
                "Waiting for rollout maintenance  •  process 42 (background migration)  •  Projecting history {thread_id}  2/5 segments  •  1.0 KB handled"
            )),
            Some("Waiting for rollout maintenance".to_string()),
            None,
        ],
    );
}
