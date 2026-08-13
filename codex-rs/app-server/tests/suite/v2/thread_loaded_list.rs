use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_mock_responses_server_repeating_assistant;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadLoadedListParams;
use codex_app_server_protocol::ThreadLoadedListResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_protocol::ThreadId;
use codex_state::DirectionalThreadSpawnEdgeStatus;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::time::timeout;

const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[tokio::test]
async fn thread_loaded_list_returns_loaded_thread_ids() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized_with_timeout(DEFAULT_READ_TIMEOUT)
        .await?;

    let thread_id = start_thread(&mut mcp).await?;

    let list_id = mcp
        .send_thread_loaded_list_request(ThreadLoadedListParams::default())
        .await?;
    let ThreadLoadedListResponse {
        mut data,
        next_cursor,
    } = timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(list_id)).await??;
    data.sort();
    assert_eq!(data, vec![thread_id]);
    assert_eq!(next_cursor, None);

    Ok(())
}

#[tokio::test]
async fn thread_loaded_list_paginates() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized_with_timeout(DEFAULT_READ_TIMEOUT)
        .await?;

    let first = start_thread(&mut mcp).await?;
    let second = start_thread(&mut mcp).await?;

    let mut expected = [first, second];
    expected.sort();

    let list_id = mcp
        .send_thread_loaded_list_request(ThreadLoadedListParams {
            cursor: None,
            limit: Some(1),
            ancestor_thread_id: None,
        })
        .await?;
    let ThreadLoadedListResponse {
        data: first_page,
        next_cursor,
    } = timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(list_id)).await??;
    assert_eq!(first_page, vec![expected[0].clone()]);
    assert_eq!(next_cursor, Some(expected[0].clone()));

    let list_id = mcp
        .send_thread_loaded_list_request(ThreadLoadedListParams {
            cursor: next_cursor,
            limit: Some(1),
            ancestor_thread_id: None,
        })
        .await?;
    let ThreadLoadedListResponse {
        data: second_page,
        next_cursor,
    } = timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(list_id)).await??;
    assert_eq!(second_page, vec![expected[1].clone()]);
    assert_eq!(next_cursor, None);

    Ok(())
}

#[tokio::test]
async fn thread_loaded_list_filters_and_paginates_loaded_descendants() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let state_db = codex_state::StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(codex_home.path().abs()),
        "mock_provider".to_string(),
    )
    .await?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized_with_timeout(DEFAULT_READ_TIMEOUT)
        .await?;

    let root = ThreadId::from_string(&start_thread(&mut mcp).await?)?;
    let child = ThreadId::from_string(&start_thread(&mut mcp).await?)?;
    let grandchild = ThreadId::from_string(&start_thread(&mut mcp).await?)?;
    let unrelated_root = ThreadId::from_string(&start_thread(&mut mcp).await?)?;
    let unloaded_intermediate = ThreadId::new();

    for (parent, descendant) in [
        (root, child),
        (root, unloaded_intermediate),
        (unloaded_intermediate, grandchild),
    ] {
        state_db
            .upsert_thread_spawn_edge(parent, descendant, DirectionalThreadSpawnEdgeStatus::Open)
            .await?;
    }
    assert!(state_db.get_thread(child).await?.is_none());
    assert!(state_db.get_thread(grandchild).await?.is_none());

    let mut expected = [child.to_string(), grandchild.to_string()];
    expected.sort();
    let mut cursor = None;
    for (index, expected_id) in expected.iter().enumerate() {
        let request_id = mcp
            .send_thread_loaded_list_request(ThreadLoadedListParams {
                cursor,
                limit: Some(1),
                ancestor_thread_id: Some(root.to_string()),
            })
            .await?;
        let response: ThreadLoadedListResponse =
            timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(request_id)).await??;
        assert_eq!(response.data, vec![expected_id.clone()]);
        assert_eq!(response.next_cursor.is_some(), index + 1 < expected.len());
        cursor = response.next_cursor;
    }
    assert!(cursor.is_none());

    for empty_root in [unrelated_root, ThreadId::new()] {
        let request_id = mcp
            .send_thread_loaded_list_request(ThreadLoadedListParams {
                ancestor_thread_id: Some(empty_root.to_string()),
                ..Default::default()
            })
            .await?;
        let response: ThreadLoadedListResponse =
            timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(request_id)).await??;
        assert!(response.data.is_empty());
    }

    let request_id = mcp
        .send_thread_loaded_list_request(ThreadLoadedListParams {
            ancestor_thread_id: Some("not-a-thread-id".to_string()),
            ..Default::default()
        })
        .await?;
    let error = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_error_message(RequestId::Integer(request_id)),
    )
    .await??;
    assert_eq!(error.error.code, -32600);
    assert!(error.error.message.contains("invalid ancestor thread id"));

    Ok(())
}

#[tokio::test]
async fn thread_loaded_list_stops_at_closed_promotion_boundary() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let state_db = codex_state::StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(codex_home.path().abs()),
        "mock_provider".to_string(),
    )
    .await?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized_with_timeout(DEFAULT_READ_TIMEOUT)
        .await?;

    let former_owner = ThreadId::from_string(&start_thread(&mut mcp).await?)?;
    let promoted_root = ThreadId::from_string(&start_thread(&mut mcp).await?)?;
    let promoted_child = ThreadId::from_string(&start_thread(&mut mcp).await?)?;
    state_db
        .upsert_thread_spawn_edge(
            former_owner,
            promoted_root,
            DirectionalThreadSpawnEdgeStatus::Closed,
        )
        .await?;
    state_db
        .upsert_thread_spawn_edge(
            promoted_root,
            promoted_child,
            DirectionalThreadSpawnEdgeStatus::Open,
        )
        .await?;

    let former_owner_descendants: ThreadLoadedListResponse = mcp
        .request(
            |request_id| codex_app_server_protocol::ClientRequest::ThreadLoadedList {
                request_id,
                params: ThreadLoadedListParams {
                    ancestor_thread_id: Some(former_owner.to_string()),
                    ..Default::default()
                },
            },
        )
        .await?;
    assert_eq!(former_owner_descendants.data, Vec::<String>::new());

    let promoted_descendants: ThreadLoadedListResponse = mcp
        .request(
            |request_id| codex_app_server_protocol::ClientRequest::ThreadLoadedList {
                request_id,
                params: ThreadLoadedListParams {
                    ancestor_thread_id: Some(promoted_root.to_string()),
                    ..Default::default()
                },
            },
        )
        .await?;
    assert_eq!(promoted_descendants.data, vec![promoted_child.to_string()]);

    Ok(())
}

async fn start_thread(mcp: &mut TestAppServer) -> Result<String> {
    let req_id = mcp
        .send_thread_start_request_with_auto_env(ThreadStartParams {
            model: Some("gpt-5.2".to_string()),
            ..Default::default()
        })
        .await?;
    let ThreadStartResponse { thread, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(req_id)).await??;
    Ok(thread.id)
}
