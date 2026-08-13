use super::*;
use codex_agent_graph_store::AgentGraphStore;
use codex_agent_graph_store::ThreadSpawnEdgeStatus;
use std::collections::HashSet;

impl AgentControl {
    /// Return registered descendants that are current without a persisted ownership edge.
    ///
    /// Persisted Open edges define the durable subtree. A missing edge defines a current-only
    /// child, including an adopted child whose metadata is no longer marked ephemeral. Closed,
    /// PermanentlyClosed, and parent-mismatched edges are ownership boundaries.
    pub(crate) async fn current_only_descendant_parents_within(
        &self,
        root_thread_id: ThreadId,
        persisted_owned_thread_ids: &HashSet<ThreadId>,
        agent_graph_store: Option<&dyn AgentGraphStore>,
    ) -> CodexResult<HashMap<ThreadId, ThreadId>> {
        self.current_only_descendant_parents_with_seeded_ownership(
            root_thread_id,
            persisted_owned_thread_ids,
            HashSet::from([root_thread_id]),
            agent_graph_store,
        )
        .await
    }

    async fn current_only_descendant_parents_with_seeded_ownership(
        &self,
        root_thread_id: ThreadId,
        persisted_owned_thread_ids: &HashSet<ThreadId>,
        mut included_thread_ids: HashSet<ThreadId>,
        agent_graph_store: Option<&dyn AgentGraphStore>,
    ) -> CodexResult<HashMap<ThreadId, ThreadId>> {
        let mut remaining_parent_by_thread_id =
            self.current_membership_descendant_parents(root_thread_id);
        let incoming_edges = if let Some(agent_graph_store) = agent_graph_store {
            let child_thread_ids = remaining_parent_by_thread_id
                .keys()
                .copied()
                .collect::<Vec<_>>();
            agent_graph_store
                .list_thread_spawn_edges_by_child_ids(&child_thread_ids)
                .await
                .map_err(|err| {
                    CodexErr::Fatal(format!("failed to load current thread-spawn edges: {err}"))
                })?
                .into_iter()
                .map(|edge| (edge.child_thread_id, edge))
                .collect::<HashMap<_, _>>()
        } else {
            HashMap::new()
        };
        included_thread_ids.insert(root_thread_id);
        let mut current_only_parent_by_thread_id = HashMap::new();
        loop {
            let mut progressed = false;
            let candidates = remaining_parent_by_thread_id
                .iter()
                .map(|(child, parent)| (*child, *parent))
                .collect::<Vec<_>>();
            for (child_thread_id, parent_thread_id) in candidates {
                if !included_thread_ids.contains(&parent_thread_id) {
                    continue;
                }
                remaining_parent_by_thread_id.remove(&child_thread_id);
                progressed = true;
                let child_is_persisted = persisted_owned_thread_ids.contains(&child_thread_id);
                let parent_is_current_only =
                    !persisted_owned_thread_ids.contains(&parent_thread_id);
                let edge_allows_membership = match incoming_edges.get(&child_thread_id) {
                    Some(edge) => {
                        edge.parent_thread_id == parent_thread_id
                            && edge.status == ThreadSpawnEdgeStatus::Open
                            && (child_is_persisted || parent_is_current_only)
                    }
                    None => !child_is_persisted,
                };
                if !edge_allows_membership {
                    continue;
                }
                included_thread_ids.insert(child_thread_id);
                if !child_is_persisted {
                    current_only_parent_by_thread_id.insert(child_thread_id, parent_thread_id);
                }
            }
            if !progressed {
                break;
            }
        }
        Ok(current_only_parent_by_thread_id)
    }
}
