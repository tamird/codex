use serde::Deserialize;
use serde::Serialize;

/// Lifecycle status attached to a directional thread-spawn edge.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadSpawnEdgeStatus {
    /// The child thread is still live or resumable as an open spawned agent.
    Open,
    /// The child thread has been closed from the parent/child graph's perspective.
    Closed,
}

/// One persisted incoming thread-spawn edge selected by child thread ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ThreadSpawnEdge {
    pub parent_thread_id: codex_protocol::ThreadId,
    pub child_thread_id: codex_protocol::ThreadId,
    pub status: ThreadSpawnEdgeStatus,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn thread_spawn_edge_status_serializes_as_snake_case() {
        assert_eq!(
            serde_json::to_string(&ThreadSpawnEdgeStatus::Open)
                .expect("open status should serialize"),
            "\"open\""
        );
        assert_eq!(
            serde_json::to_string(&ThreadSpawnEdgeStatus::Closed)
                .expect("closed status should serialize"),
            "\"closed\""
        );
        assert_eq!(
            serde_json::from_str::<ThreadSpawnEdgeStatus>("\"open\"")
                .expect("open status should deserialize"),
            ThreadSpawnEdgeStatus::Open
        );
        assert_eq!(
            serde_json::from_str::<ThreadSpawnEdgeStatus>("\"closed\"")
                .expect("closed status should deserialize"),
            ThreadSpawnEdgeStatus::Closed
        );
    }
}
