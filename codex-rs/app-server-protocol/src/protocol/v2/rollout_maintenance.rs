use crate::JsonSchema;
use crate::RequestId;
use crate::TS;
use serde::Deserialize;
use serde::Serialize;

/// Result of sampling rollout maintenance without waiting for it to finish.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct RolloutMaintenanceStatusReadResponse {
    pub status: RolloutMaintenanceSnapshot,
}

/// Current cross-process lock ownership and this server's automatic migration activity.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct RolloutMaintenanceSnapshot {
    pub lock: RolloutMaintenanceLockStatus,
    /// Null when this thread store does not expose an automatic migration worker.
    pub background_migration: Option<RolloutMaintenanceRequestStatus>,
}

/// Global snapshots and progress for one request on the receiving connection.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
#[ts(tag = "type", rename_all = "camelCase", export_to = "v2/")]
pub enum RolloutMaintenanceStatusChangedNotification {
    Snapshot {
        status: RolloutMaintenanceSnapshot,
    },
    Request {
        #[ts(rename = "requestId")]
        request_id: RequestId,
        status: RolloutMaintenanceRequestStatus,
    },
}

/// The operating-system lock is authoritative; older owners may provide no details.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(tag = "type", rename_all = "camelCase")]
#[ts(tag = "type", rename_all = "camelCase", export_to = "v2/")]
pub enum RolloutMaintenanceLockStatus {
    Idle,
    Busy {
        owner: Option<RolloutMaintenanceActivity>,
    },
}

/// Request-local activity. Running does not imply ownership of the global lock.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
#[ts(tag = "type", rename_all = "camelCase", export_to = "v2/")]
pub enum RolloutMaintenanceRequestStatus {
    Idle,
    QueuedForMigration {
        #[ts(rename = "threadId")]
        thread_id: String,
    },
    WaitingForMaintenance {
        #[ts(rename = "threadId")]
        thread_id: Option<String>,
        owner: Option<RolloutMaintenanceActivity>,
    },
    Running {
        activity: RolloutMaintenanceActivity,
    },
}

/// Content-free activity details; counts describe only their named phase.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct RolloutMaintenanceActivity {
    pub operation_id: String,
    pub process_id: u32,
    pub operation: RolloutMaintenanceOperation,
    pub thread_id: Option<String>,
    pub phase: RolloutMaintenancePhase,
    pub progress: Option<RolloutMaintenanceProgress>,
    /// Observed bytes handled, including repeated passes; not physical I/O or overall progress.
    pub io_bytes: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", export_to = "v2/")]
pub enum RolloutMaintenanceOperation {
    ManualMigration,
    BackgroundMigration,
    Compression,
    HistoryRepair,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", export_to = "v2/")]
pub enum RolloutMaintenancePhase {
    Starting,
    Inventory,
    Planning,
    Validating,
    WaitingForWriter,
    Staging,
    Projecting,
    Publishing,
    Verifying,
    Recovering,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct RolloutMaintenanceProgress {
    pub completed: u64,
    pub total: Option<u64>,
    pub unit: RolloutMaintenanceProgressUnit,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", export_to = "v2/")]
pub enum RolloutMaintenanceProgressUnit {
    Paths,
    Segments,
    Bytes,
}

impl From<codex_rollout::RolloutMaintenanceStatus> for RolloutMaintenanceLockStatus {
    fn from(value: codex_rollout::RolloutMaintenanceStatus) -> Self {
        match value {
            codex_rollout::RolloutMaintenanceStatus::Idle => Self::Idle,
            codex_rollout::RolloutMaintenanceStatus::Busy { owner } => Self::Busy {
                owner: owner.map(Into::into),
            },
        }
    }
}

impl From<codex_rollout::RolloutMaintenanceRequestStatus> for RolloutMaintenanceRequestStatus {
    fn from(value: codex_rollout::RolloutMaintenanceRequestStatus) -> Self {
        use codex_rollout::RolloutMaintenanceRequestStatus as Status;
        match value {
            Status::Idle => Self::Idle,
            Status::QueuedForMigration { thread_id } => Self::QueuedForMigration {
                thread_id: thread_id.to_string(),
            },
            Status::WaitingForMaintenance { thread_id, owner } => Self::WaitingForMaintenance {
                thread_id: thread_id.map(|id| id.to_string()),
                owner: owner.map(Into::into),
            },
            Status::Running { activity } => Self::Running {
                activity: activity.into(),
            },
        }
    }
}

impl From<codex_rollout::RolloutMaintenanceActivity> for RolloutMaintenanceActivity {
    fn from(value: codex_rollout::RolloutMaintenanceActivity) -> Self {
        Self {
            operation_id: value.operation_id.to_string(),
            process_id: value.process_id,
            operation: value.operation.into(),
            thread_id: value.thread_id.map(|id| id.to_string()),
            phase: value.phase.into(),
            progress: value.progress.map(Into::into),
            io_bytes: value.io_bytes,
        }
    }
}

impl From<codex_rollout::RolloutMaintenanceProgress> for RolloutMaintenanceProgress {
    fn from(value: codex_rollout::RolloutMaintenanceProgress) -> Self {
        Self {
            completed: value.completed,
            total: value.total,
            unit: value.unit.into(),
        }
    }
}

impl From<codex_rollout::RolloutMaintenanceOperation> for RolloutMaintenanceOperation {
    fn from(value: codex_rollout::RolloutMaintenanceOperation) -> Self {
        use codex_rollout::RolloutMaintenanceOperation as Operation;
        match value {
            Operation::ManualMigration => Self::ManualMigration,
            Operation::BackgroundMigration => Self::BackgroundMigration,
            Operation::Compression => Self::Compression,
            Operation::HistoryRepair => Self::HistoryRepair,
        }
    }
}

impl From<codex_rollout::RolloutMaintenancePhase> for RolloutMaintenancePhase {
    fn from(value: codex_rollout::RolloutMaintenancePhase) -> Self {
        use codex_rollout::RolloutMaintenancePhase as Phase;
        match value {
            Phase::Starting => Self::Starting,
            Phase::Inventory => Self::Inventory,
            Phase::Planning => Self::Planning,
            Phase::Validating => Self::Validating,
            Phase::WaitingForWriter => Self::WaitingForWriter,
            Phase::Staging => Self::Staging,
            Phase::Projecting => Self::Projecting,
            Phase::Publishing => Self::Publishing,
            Phase::Verifying => Self::Verifying,
            Phase::Recovering => Self::Recovering,
        }
    }
}

impl From<codex_rollout::RolloutMaintenanceProgressUnit> for RolloutMaintenanceProgressUnit {
    fn from(value: codex_rollout::RolloutMaintenanceProgressUnit) -> Self {
        use codex_rollout::RolloutMaintenanceProgressUnit as Unit;
        match value {
            Unit::Paths => Self::Paths,
            Unit::Segments => Self::Segments,
            Unit::Bytes => Self::Bytes,
        }
    }
}
