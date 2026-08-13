use strum::AsRefStr;
use strum::Display;
use strum::EnumString;

use codex_protocol::ThreadId;

/// Status attached to a directional thread-spawn edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum DirectionalThreadSpawnEdgeStatus {
    Open,
    Closed,
}

/// One persisted incoming thread-spawn edge selected by child thread ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectionalThreadSpawnEdge {
    pub parent_thread_id: ThreadId,
    pub child_thread_id: ThreadId,
    pub status: DirectionalThreadSpawnEdgeStatus,
}
