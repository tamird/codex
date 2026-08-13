use super::*;
use codex_core::config::ConfigBuilder;
use codex_protocol::AgentPath;
use codex_protocol::protocol::AgentStatus;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

fn thread_id(value: &str) -> ThreadId {
    ThreadId::from_string(value).expect("test thread id must be valid")
}

fn member(thread_id: ThreadId, parent_thread_id: ThreadId, agent_path: &str) -> CurrentAgentMember {
    CurrentAgentMember {
        thread_id,
        parent_thread_id,
        agent_path: Some(AgentPath::try_from(agent_path).expect("test agent path must be valid")),
        agent_nickname: Some("Robie".to_string()),
        agent_role: Some("explorer".to_string()),
        status: AgentStatus::Completed(None),
        last_task_message: None,
    }
}

#[test]
fn current_agent_relation_filters_direct_children_from_scoped_membership() {
    let scope_id = thread_id("00000000-0000-7000-8000-000000000001");
    let direct_id = thread_id("00000000-0000-7000-8000-000000000002");
    let hidden_intermediate_id = thread_id("00000000-0000-7000-8000-000000000003");
    let descendant_id = thread_id("00000000-0000-7000-8000-000000000004");
    let members = [
        member(direct_id, scope_id, "/root/scope/direct"),
        member(
            descendant_id,
            hidden_intermediate_id,
            "/root/scope/hidden/descendant",
        ),
    ];

    let descendants = members
        .iter()
        .filter(|member| {
            current_agent_member_matches_relation(
                member, scope_id, /*direct_children_only*/ false,
            )
        })
        .map(|member| member.thread_id)
        .collect::<Vec<_>>();
    assert_eq!(descendants, vec![direct_id, descendant_id]);

    let direct_children = members
        .iter()
        .filter(|member| {
            current_agent_member_matches_relation(
                member, scope_id, /*direct_children_only*/ true,
            )
        })
        .map(|member| member.thread_id)
        .collect::<Vec<_>>();
    assert_eq!(direct_children, vec![direct_id]);
}

#[tokio::test]
async fn current_agent_registry_identity_overrides_every_hydration_source() {
    let codex_home = TempDir::new().expect("temp codex home");
    let config = ConfigBuilder::default()
        .codex_home(codex_home.path().to_path_buf())
        .build()
        .await
        .expect("test config");
    let root_id = thread_id("00000000-0000-7000-8000-000000000001");
    let child_id = thread_id("00000000-0000-7000-8000-000000000002");
    let member = member(child_id, root_id, "/root/explorer");
    let (mut thread, mut core_source, ..) = minimal_current_agent_thread(&config, &member);

    thread.agent_nickname = Some("stale nickname".to_string());
    thread.agent_role = Some("stale role".to_string());
    if let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        agent_nickname,
        agent_role,
        ..
    }) = &mut thread.source
    {
        *agent_nickname = Some("stale nickname".to_string());
        *agent_role = Some("stale role".to_string());
    }
    if let codex_protocol::protocol::SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        agent_nickname,
        agent_role,
        ..
    }) = &mut core_source
    {
        *agent_nickname = Some("stale nickname".to_string());
        *agent_role = Some("stale role".to_string());
    }

    apply_current_agent_member(&mut thread, &member);
    apply_current_agent_member_core_source(&mut core_source, &member);

    assert_eq!(thread.agent_nickname.as_deref(), Some("Robie"));
    assert_eq!(thread.agent_role.as_deref(), Some("explorer"));
    let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        agent_nickname,
        agent_role,
        ..
    }) = &thread.source
    else {
        panic!("minimal app-server source must be a thread spawn");
    };
    assert_eq!(agent_nickname.as_deref(), Some("Robie"));
    assert_eq!(agent_role.as_deref(), Some("explorer"));
    let codex_protocol::protocol::SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        agent_nickname,
        agent_role,
        ..
    }) = &core_source
    else {
        panic!("minimal core source must be a thread spawn");
    };
    assert_eq!(agent_nickname.as_deref(), Some("Robie"));
    assert_eq!(agent_role.as_deref(), Some("explorer"));
}
