//! Human-readable, measured maintenance activity for the foreground migration command.

use codex_rollout::RolloutMaintenanceActivity;
use codex_rollout::RolloutMaintenanceOperation;
use codex_rollout::RolloutMaintenancePhase;
use codex_rollout::RolloutMaintenanceProgressUnit;
use codex_rollout::RolloutMaintenanceRequestStatus;

use super::format_bytes;

pub(super) fn maintenance_line(status: RolloutMaintenanceRequestStatus) -> Option<String> {
    match status {
        RolloutMaintenanceRequestStatus::Idle => None,
        RolloutMaintenanceRequestStatus::QueuedForMigration { thread_id } => {
            Some(format!("Waiting for queued migration of {thread_id}"))
        }
        RolloutMaintenanceRequestStatus::WaitingForMaintenance { owner, .. } => Some(match owner {
            Some(owner) => {
                let operation = match owner.operation {
                    RolloutMaintenanceOperation::ManualMigration => "manual migration",
                    RolloutMaintenanceOperation::BackgroundMigration => "background migration",
                    RolloutMaintenanceOperation::Compression => "rollout compression",
                    RolloutMaintenanceOperation::HistoryRepair => "history repair",
                };
                format!(
                    "Waiting for rollout maintenance  •  process {} ({operation})  •  {}",
                    owner.process_id,
                    activity_line(owner)
                )
            }
            None => "Waiting for rollout maintenance".to_string(),
        }),
        RolloutMaintenanceRequestStatus::Running { activity } => Some(activity_line(activity)),
    }
}

fn activity_line(activity: RolloutMaintenanceActivity) -> String {
    let phase = match activity.phase {
        RolloutMaintenancePhase::Starting => "Starting",
        RolloutMaintenancePhase::Inventory => "Scanning rollouts",
        RolloutMaintenancePhase::Planning => "Planning",
        RolloutMaintenancePhase::Validating => "Validating",
        RolloutMaintenancePhase::WaitingForWriter => "Waiting for rollout writer",
        RolloutMaintenancePhase::Staging => "Staging",
        RolloutMaintenancePhase::Projecting => "Projecting history",
        RolloutMaintenancePhase::Publishing => "Publishing",
        RolloutMaintenancePhase::Verifying => "Verifying",
        RolloutMaintenancePhase::Recovering => "Recovering",
    };
    let mut line = phase.to_string();
    if let Some(thread_id) = activity.thread_id {
        line.push_str(&format!(" {thread_id}"));
    }
    if let Some(progress) = activity.progress {
        let amount = match progress.unit {
            RolloutMaintenanceProgressUnit::Bytes => match progress.total {
                Some(total) => format!(
                    "{}/{}",
                    format_bytes(progress.completed),
                    format_bytes(total)
                ),
                None => format_bytes(progress.completed),
            },
            unit @ (RolloutMaintenanceProgressUnit::Paths
            | RolloutMaintenanceProgressUnit::Segments) => {
                let unit = if unit == RolloutMaintenanceProgressUnit::Paths {
                    "paths"
                } else {
                    "segments"
                };
                match progress.total {
                    Some(total) => format!("{}/{total} {unit}", progress.completed),
                    None => format!("{} {unit}", progress.completed),
                }
            }
        };
        line.push_str(&format!("  {amount}"));
    }
    if activity.io_bytes > 0 {
        line.push_str(&format!("  •  {} handled", format_bytes(activity.io_bytes)));
    }
    line
}

#[cfg(test)]
#[path = "progress_tests.rs"]
mod tests;
