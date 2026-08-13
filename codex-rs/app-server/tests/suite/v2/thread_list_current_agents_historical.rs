use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn thread_list_relation_filters_ignore_historical_edges_for_unloaded_root() -> Result<()> {
    let codex_home = TempDir::new()?;
    create_minimal_config(codex_home.path())?;
    let mut mcp = init_mcp(codex_home.path()).await?;
    let parent_id = ThreadId::new();
    let older_child_id = ThreadId::new();
    let newer_child_id = ThreadId::new();
    let closed_child_id = ThreadId::new();
    let grandchild_id = ThreadId::new();
    let state_db = codex_state::StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(codex_home.path().abs()),
        "mock_provider".to_string(),
    )
    .await?;
    for (thread_id, created_at, source, model_provider) in [
        (
            older_child_id,
            "2025-02-01T10:00:00Z",
            CoreSessionSource::SubAgent(SubAgentSource::Other("custom:worker-1".to_string())),
            "other_provider",
        ),
        (
            newer_child_id,
            "2025-02-01T11:00:00Z",
            CoreSessionSource::Cli,
            "mock_provider",
        ),
        (
            closed_child_id,
            "2025-02-01T11:30:00Z",
            CoreSessionSource::SubAgent(SubAgentSource::Other("agent_job:closed".to_string())),
            "mock_provider",
        ),
        (
            grandchild_id,
            "2025-02-01T12:00:00Z",
            CoreSessionSource::SubAgent(SubAgentSource::Other("custom:worker-2".to_string())),
            "mock_provider",
        ),
    ] {
        let created_at = DateTime::parse_from_rfc3339(created_at)?.with_timezone(&Utc);
        let mut builder = codex_state::ThreadMetadataBuilder::new(
            thread_id,
            codex_home.path().join(format!("{thread_id}.jsonl")),
            created_at,
            source,
        );
        builder.model_provider = Some(model_provider.to_string());
        builder.cwd = codex_home.path().to_path_buf();
        builder.cli_version = Some("0.0.0".to_string());
        let mut metadata = builder.build(model_provider);
        metadata.preview = Some("child thread".to_string());
        metadata.first_user_message = metadata.preview.clone();
        state_db.upsert_thread(&metadata).await?;
    }
    for (parent_thread_id, child_thread_id, status) in [
        (
            parent_id,
            older_child_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        ),
        (
            parent_id,
            newer_child_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        ),
        (
            parent_id,
            closed_child_id,
            DirectionalThreadSpawnEdgeStatus::Closed,
        ),
        (
            newer_child_id,
            grandchild_id,
            DirectionalThreadSpawnEdgeStatus::Open,
        ),
    ] {
        state_db
            .upsert_thread_spawn_edge(parent_thread_id, child_thread_id, status)
            .await?;
    }
    state_db
        .mark_backfill_complete(/*last_watermark*/ None)
        .await?;

    for relation in [
        ThreadListRelation::DirectChildrenOf(parent_id),
        ThreadListRelation::DescendantsOf(parent_id),
    ] {
        let response = list_threads_for_relation(
            &mut mcp, relation, /*cursor*/ None, /*limit*/ 10,
            /*model_providers*/ None, /*source_kinds*/ None,
        )
        .await?;
        assert_eq!(response.data, Vec::new());
        assert_eq!(response.next_cursor, None);
        assert_eq!(response.backwards_cursor, None);
    }
    Ok(())
}

#[tokio::test]
async fn thread_list_relation_filters_do_not_page_historical_edges_for_unloaded_root() -> Result<()>
{
    let codex_home = TempDir::new()?;
    create_minimal_config(codex_home.path())?;
    let mut mcp = init_mcp(codex_home.path()).await?;
    let parent_id = ThreadId::new();
    let state_db = codex_state::StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(codex_home.path().abs()),
        "mock_provider".to_string(),
    )
    .await?;
    let created_at = DateTime::parse_from_rfc3339("2025-02-01T10:00:00Z")?.with_timezone(&Utc);
    for index in 0..205 {
        let child_id = ThreadId::new();
        let source = CoreSessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id: parent_id,
            depth: 1,
            agent_path: None,
            agent_nickname: Some(format!("worker-{index}")),
            agent_role: None,
        });
        let mut builder = codex_state::ThreadMetadataBuilder::new(
            child_id,
            codex_home.path().join(format!("{child_id}.jsonl")),
            created_at + chrono::Duration::seconds(index),
            source,
        );
        builder.model_provider = Some("mock_provider".to_string());
        builder.cwd = codex_home.path().to_path_buf();
        builder.cli_version = Some("0.0.0".to_string());
        state_db
            .upsert_thread(&builder.build("mock_provider"))
            .await?;
        state_db
            .upsert_thread_spawn_edge(parent_id, child_id, DirectionalThreadSpawnEdgeStatus::Open)
            .await?;
    }
    state_db
        .mark_backfill_complete(/*last_watermark*/ None)
        .await?;
    for relation in [
        ThreadListRelation::DirectChildrenOf(parent_id),
        ThreadListRelation::DescendantsOf(parent_id),
    ] {
        let response = list_threads_for_relation(
            &mut mcp, relation, /*cursor*/ None, /*limit*/ 200,
            /*model_providers*/ None, /*source_kinds*/ None,
        )
        .await?;
        assert_eq!(response.data, Vec::new());
        assert_eq!(response.next_cursor, None);
        assert_eq!(response.backwards_cursor, None);
    }
    Ok(())
}
