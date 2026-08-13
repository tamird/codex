use super::OwnedDescendantTree;
use super::rebase_owned_agent_path;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;

#[test]
fn ownership_tree_contains_its_root_and_rejects_unrelated_threads() {
    let root_thread_id = ThreadId::new();
    let tree = OwnedDescendantTree {
        root_thread_id,
        original_root_path: AgentPath::root(),
        original_root_depth: 0,
        descendants: Vec::new(),
        _transition_guards: Vec::new(),
    };

    assert!(tree.contains_thread(root_thread_id));
    assert!(!tree.contains_thread(ThreadId::new()));
}

#[test]
fn adoption_rebases_each_descendant_under_the_destination_agent() {
    let original = AgentPath::try_from("/root/worker/reviewer").unwrap();
    let original_root = AgentPath::root();
    let destination_root = AgentPath::try_from("/root/adopted_worker").unwrap();

    assert_eq!(
        rebase_owned_agent_path(&original, &original_root, &destination_root).unwrap(),
        AgentPath::try_from("/root/adopted_worker/worker/reviewer").unwrap()
    );
}

#[test]
fn promotion_rebases_each_descendant_under_the_promoted_root() {
    let original = AgentPath::try_from("/root/adopted_worker/worker/reviewer").unwrap();
    let original_root = AgentPath::try_from("/root/adopted_worker").unwrap();

    assert_eq!(
        rebase_owned_agent_path(&original, &original_root, &AgentPath::root()).unwrap(),
        AgentPath::try_from("/root/worker/reviewer").unwrap()
    );
}

#[test]
fn descendant_rebasing_rejects_an_unrelated_agent_path() {
    let original = AgentPath::try_from("/root/adopted_worker_extra/reviewer").unwrap();
    let original_root = AgentPath::try_from("/root/adopted_worker").unwrap();

    assert!(rebase_owned_agent_path(&original, &original_root, &AgentPath::root()).is_err());
}
