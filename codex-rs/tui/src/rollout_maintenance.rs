//! Ephemeral, content-free rollout maintenance text shared by startup and the chat footer.

use codex_app_server_protocol::RolloutMaintenanceActivity;
use codex_app_server_protocol::RolloutMaintenanceOperation;
use codex_app_server_protocol::RolloutMaintenancePhase;
use codex_app_server_protocol::RolloutMaintenanceProgressUnit;
use codex_app_server_protocol::RolloutMaintenanceRequestStatus;

pub(crate) fn request_status_text(status: &RolloutMaintenanceRequestStatus) -> Option<String> {
    match status {
        RolloutMaintenanceRequestStatus::Idle => None,
        RolloutMaintenanceRequestStatus::QueuedForMigration { thread_id } => Some(format!(
            "Waiting for rollout migration …{:.12}",
            thread_suffix(thread_id)
        )),
        RolloutMaintenanceRequestStatus::WaitingForMaintenance { owner, .. } => Some(match owner {
            Some(owner) => format!(
                "Waiting for PID {} · {}",
                owner.process_id,
                activity_text(owner)
            ),
            None => "Waiting for rollout maintenance…".to_string(),
        }),
        RolloutMaintenanceRequestStatus::Running { activity } => Some(activity_text(activity)),
    }
}

pub(crate) fn background_status_text(status: &RolloutMaintenanceRequestStatus) -> Option<String> {
    match status {
        RolloutMaintenanceRequestStatus::Idle => None,
        RolloutMaintenanceRequestStatus::QueuedForMigration { thread_id } => Some(format!(
            "Background migration …{:.12}: queued",
            thread_suffix(thread_id)
        )),
        RolloutMaintenanceRequestStatus::WaitingForMaintenance { .. } => {
            Some("Background migration: waiting for maintenance".to_string())
        }
        RolloutMaintenanceRequestStatus::Running { activity } => Some(activity_text(activity)),
    }
}

fn activity_text(activity: &RolloutMaintenanceActivity) -> String {
    let operation = match activity.operation {
        RolloutMaintenanceOperation::ManualMigration => "Rollout migration",
        RolloutMaintenanceOperation::BackgroundMigration => "Background migration",
        RolloutMaintenanceOperation::Compression => "Rollout compression",
        RolloutMaintenanceOperation::HistoryRepair => "Rollout history repair",
    };
    let phase = match activity.phase {
        RolloutMaintenancePhase::Starting => "starting",
        RolloutMaintenancePhase::Inventory => "inventorying",
        RolloutMaintenancePhase::Planning => "planning",
        RolloutMaintenancePhase::Validating => "validating",
        RolloutMaintenancePhase::WaitingForWriter => "waiting for writer",
        RolloutMaintenancePhase::Staging => "staging",
        RolloutMaintenancePhase::Projecting => "projecting",
        RolloutMaintenancePhase::Publishing => "publishing",
        RolloutMaintenancePhase::Verifying => "verifying",
        RolloutMaintenancePhase::Recovering => "recovering",
    };
    let mut text = operation.to_string();
    if let Some(thread_id) = &activity.thread_id {
        text.push_str(&format!(" …{:.12}", thread_suffix(thread_id)));
    }
    text.push_str(&format!(": {phase}"));
    if let Some(progress) = activity.progress {
        match progress.unit {
            RolloutMaintenanceProgressUnit::Bytes => {
                text.push_str(&format!(" {}", format_bytes(progress.completed)));
                if let Some(total) = progress.total {
                    text.push_str(&format!("/{}", format_bytes(total)));
                }
            }
            unit @ (RolloutMaintenanceProgressUnit::Paths
            | RolloutMaintenanceProgressUnit::Segments) => {
                let unit = if unit == RolloutMaintenanceProgressUnit::Paths {
                    "paths"
                } else {
                    "segments"
                };
                let completed = progress.completed;
                match progress.total {
                    Some(total) => text.push_str(&format!(" {completed}/{total} {unit}")),
                    None => text.push_str(&format!(" {completed} {unit}")),
                }
            }
        }
    }
    if activity.io_bytes != 0 {
        text.push_str(&format!(" · {} handled", format_bytes(activity.io_bytes)));
    }
    text
}

fn thread_suffix(thread_id: &str) -> &str {
    // UUIDv7 prefixes are timestamps; the final group distinguishes nearby threads.
    thread_id
        .rsplit_once('-')
        .map_or(thread_id, |(_, suffix)| suffix)
}

fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let value = bytes as f64;
    if value >= GIB {
        format!("{:.1} GiB", value / GIB)
    } else if value >= MIB {
        format!("{:.1} MiB", value / MIB)
    } else if value >= KIB {
        format!("{:.1} KiB", value / KIB)
    } else {
        format!("{bytes} B")
    }
}
