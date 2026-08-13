use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn thread_list_relation_filters_page_current_members_and_exclude_closed_agents() -> Result<()>
{
    const PARENT_PROMPT: &str = "spawn two current-membership workers";
    const CHILD_A_PROMPT: &str = "current-membership worker a";
    const CHILD_B_PROMPT: &str = "current-membership worker b";
    const SPAWN_A_CALL_ID: &str = "spawn-current-worker-a";
    const SPAWN_B_CALL_ID: &str = "spawn-current-worker-b";
    const CLOSE_PROMPT: &str = "close current-membership worker a";
    const CLOSE_CALL_ID: &str = "close-current-worker-a";

    let server = responses::start_mock_server().await;
    let spawn_a_args = serde_json::to_string(&json!({
        "message": CHILD_A_PROMPT,
        "task_name": "current_worker_a",
        "fork_turns": "none",
    }))?;
    let spawn_b_args = serde_json::to_string(&json!({
        "message": CHILD_B_PROMPT,
        "task_name": "current_worker_b",
        "fork_turns": "none",
    }))?;
    responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            String::from_utf8_lossy(&request.body).contains(PARENT_PROMPT)
        },
        responses::sse(vec![
            responses::ev_response_created("parent-spawn-current-workers"),
            responses::ev_function_call_with_namespace(
                SPAWN_A_CALL_ID,
                "collaboration",
                "spawn_agent",
                &spawn_a_args,
            ),
            responses::ev_function_call_with_namespace(
                SPAWN_B_CALL_ID,
                "collaboration",
                "spawn_agent",
                &spawn_b_args,
            ),
            responses::ev_completed("parent-spawn-current-workers"),
        ]),
    )
    .await;
    for (prompt, response_id, message_id) in [
        (
            CHILD_A_PROMPT,
            "current-worker-a",
            "current-worker-a-message",
        ),
        (
            CHILD_B_PROMPT,
            "current-worker-b",
            "current-worker-b-message",
        ),
    ] {
        responses::mount_sse_once_match(
            &server,
            move |request: &wiremock::Request| {
                let body = String::from_utf8_lossy(&request.body);
                body.contains(prompt)
                    && !body.contains(SPAWN_A_CALL_ID)
                    && !body.contains(SPAWN_B_CALL_ID)
            },
            responses::sse(vec![
                responses::ev_response_created(response_id),
                responses::ev_assistant_message(message_id, "worker complete"),
                responses::ev_completed(response_id),
            ]),
        )
        .await;
    }
    responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            let body = String::from_utf8_lossy(&request.body);
            body.contains(SPAWN_A_CALL_ID) && body.contains(SPAWN_B_CALL_ID)
        },
        responses::sse(vec![
            responses::ev_response_created("parent-spawn-return"),
            responses::ev_assistant_message("parent-spawn-message", "workers spawned"),
            responses::ev_completed("parent-spawn-return"),
        ]),
    )
    .await;
    let close_args = serde_json::to_string(&json!({"target": "/root/current_worker_a"}))?;
    responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| String::from_utf8_lossy(&request.body).contains(CLOSE_PROMPT),
        responses::sse(vec![
            responses::ev_response_created("parent-close-current-worker"),
            responses::ev_function_call_with_namespace(
                CLOSE_CALL_ID,
                "frodex",
                "close_agent",
                &close_args,
            ),
            responses::ev_completed("parent-close-current-worker"),
        ]),
    )
    .await;
    responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            String::from_utf8_lossy(&request.body).contains(CLOSE_CALL_ID)
        },
        responses::sse(vec![
            responses::ev_response_created("parent-close-return"),
            responses::ev_assistant_message("parent-close-message", "worker closed"),
            responses::ev_completed("parent-close-return"),
        ]),
    )
    .await;

    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_model("gpt-5.4")
        .with_extra_config("[features.multi_agent_v2]\nenabled = true")
        .write(codex_home.path())?;
    write_models_cache(codex_home.path())?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let ThreadStartResponse { thread, .. } = mcp
        .start_thread(ThreadStartParams {
            model: Some("gpt-5.4".to_string()),
            ..Default::default()
        })
        .await?;
    let parent_thread_id = ThreadId::from_string(&thread.id)?;
    let spawn_turn: TurnStartResponse = mcp
        .request(|request_id| ClientRequest::TurnStart {
            request_id,
            params: TurnStartParams {
                thread_id: thread.id.clone(),
                input: vec![UserInput::Text {
                    text: PARENT_PROMPT.to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        })
        .await?;

    let mut spawned = Vec::new();
    timeout(DEFAULT_READ_TIMEOUT, async {
        while spawned.len() < 2 {
            let completed: ItemCompletedNotification =
                mcp.read_notification("item/completed").await?;
            if let ThreadItem::SubAgentActivity {
                id,
                kind: SubAgentActivityKind::Started,
                agent_thread_id,
                ..
            } = completed.item
                && matches!(id.as_str(), SPAWN_A_CALL_ID | SPAWN_B_CALL_ID)
            {
                let agent_path = match id.as_str() {
                    SPAWN_A_CALL_ID => "/root/current_worker_a",
                    SPAWN_B_CALL_ID => "/root/current_worker_b",
                    _ => unreachable!("matches! limits the accepted call ids"),
                };
                spawned.push((
                    agent_thread_id,
                    codex_protocol::AgentPath::from_string(agent_path.to_string())
                        .expect("test agent path must be valid"),
                ));
            }
        }
        Ok::<(), anyhow::Error>(())
    })
    .await??;
    timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            let completed: TurnCompletedNotification =
                mcp.read_notification("turn/completed").await?;
            if completed.thread_id == thread.id && completed.turn.id == spawn_turn.turn.id {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;

    let descendants = {
        let mut attempts = 0;
        loop {
            let response = list_threads_for_relation(
                &mut mcp,
                ThreadListRelation::DescendantsOf(parent_thread_id),
                /*cursor*/ None,
                /*limit*/ 10,
                /*model_providers*/ None,
                /*source_kinds*/ None,
            )
            .await?;
            if response.data.len() == 2
                && response
                    .data
                    .iter()
                    .all(|listed| listed.agent_status == Some(CollabAgentStatus::Completed))
            {
                break response;
            }
            attempts += 1;
            assert!(
                attempts < 100,
                "current agent membership did not settle: {response:#?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    };

    let persisted_worker_id = spawned
        .iter()
        .find(|(_, path)| path.as_str() == "/root/current_worker_b")
        .map(|(thread_id, _)| ThreadId::from_string(thread_id))
        .transpose()?
        .expect("worker b must have spawned");
    let state_db = codex_state::StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(codex_home.path().abs()),
        "mock_provider".to_string(),
    )
    .await?;
    let section = state_db
        .create_thread_section("Current Workers", /*appearance*/ None)
        .await?;
    let mut persisted_metadata = state_db
        .get_thread(persisted_worker_id)
        .await?
        .expect("spawned worker metadata must be persisted");
    persisted_metadata.preview = Some("Persisted worker b preview".to_string());
    persisted_metadata.title = "Persisted Worker B".to_string();
    state_db.upsert_thread(&persisted_metadata).await?;
    assert!(
        state_db
            .update_thread_name(persisted_worker_id, Some("Persisted Worker B"))
            .await?
    );
    assert!(
        state_db
            .move_thread_to_section(
                persisted_worker_id,
                Some(&section.id),
                /*before_thread_id*/ None
            )
            .await?
    );

    let persisted_view: ThreadListResponse = mcp
        .request(|request_id| ClientRequest::ThreadList {
            request_id,
            params: codex_app_server_protocol::ThreadListParams {
                cursor: None,
                limit: Some(10),
                sort_key: Some(ThreadSortKey::SectionPosition),
                sort_direction: Some(SortDirection::Asc),
                model_providers: None,
                source_kinds: None,
                archived: None,
                section_id: Some(Some(section.id.clone())),
                project_id: None,
                cwd: None,
                use_state_db_only: true,
                search_term: Some("Persisted Worker B".to_string()),
                parent_thread_id: Some(parent_thread_id.to_string()),
                ancestor_thread_id: None,
            },
        })
        .await?;
    assert_eq!(persisted_view.data.len(), 1);
    assert_eq!(persisted_view.data[0].id, persisted_worker_id.to_string());
    assert_eq!(persisted_view.data[0].preview, "Persisted worker b preview");
    assert_eq!(
        persisted_view.data[0].name.as_deref(),
        Some("Persisted Worker B")
    );
    assert_eq!(
        persisted_view.data[0]
            .section
            .as_ref()
            .map(|section| &section.id),
        Some(&section.id)
    );
    state_db.close().await;

    let first_page = list_threads_for_relation(
        &mut mcp,
        ThreadListRelation::DirectChildrenOf(parent_thread_id),
        /*cursor*/ None,
        /*limit*/ 1,
        /*model_providers*/ None,
        /*source_kinds*/ None,
    )
    .await?;
    assert_eq!(first_page.data.len(), 1);
    assert!(first_page.next_cursor.is_some());
    let second_page = list_threads_for_relation(
        &mut mcp,
        ThreadListRelation::DirectChildrenOf(parent_thread_id),
        first_page.next_cursor.clone(),
        /*limit*/ 1,
        /*model_providers*/ None,
        /*source_kinds*/ None,
    )
    .await?;
    assert_eq!(second_page.data.len(), 1);
    assert_eq!(second_page.next_cursor, None);

    let reverse_from_watermark: ThreadListResponse = mcp
        .request(|request_id| ClientRequest::ThreadList {
            request_id,
            params: codex_app_server_protocol::ThreadListParams {
                cursor: first_page.backwards_cursor.clone(),
                limit: Some(10),
                sort_key: Some(ThreadSortKey::CreatedAt),
                sort_direction: Some(SortDirection::Asc),
                model_providers: None,
                source_kinds: None,
                archived: None,
                section_id: None,
                project_id: None,
                cwd: None,
                use_state_db_only: true,
                search_term: None,
                parent_thread_id: Some(parent_thread_id.to_string()),
                ancestor_thread_id: None,
            },
        })
        .await?;
    assert_eq!(reverse_from_watermark.data.len(), 1);
    assert_eq!(reverse_from_watermark.data[0].id, first_page.data[0].id);

    let mut expected = spawned.clone();
    expected.sort();
    let mut actual = first_page
        .data
        .iter()
        .chain(&second_page.data)
        .map(|listed| {
            let agent_path = match &listed.source {
                SessionSource::SubAgent(SubAgentSource::ThreadSpawn { agent_path, .. }) => {
                    agent_path.clone()
                }
                source => panic!("current agent had unexpected source: {source:?}"),
            };
            (
                listed.id.clone(),
                agent_path.expect("current agent must expose its canonical path"),
                listed.parent_thread_id.clone(),
                listed.agent_status.clone(),
            )
        })
        .collect::<Vec<_>>();
    actual.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        actual,
        expected
            .iter()
            .map(|(thread_id, agent_path)| {
                (
                    thread_id.clone(),
                    agent_path.clone(),
                    Some(thread.id.clone()),
                    Some(CollabAgentStatus::Completed),
                )
            })
            .collect::<Vec<_>>()
    );

    assert_eq!(
        descendants
            .data
            .iter()
            .map(|listed| listed.id.clone())
            .collect::<std::collections::HashSet<_>>(),
        spawned
            .iter()
            .map(|(thread_id, _)| thread_id.clone())
            .collect()
    );

    let fork_request_id = mcp
        .send_thread_fork_request(ThreadForkParams {
            thread_id: thread.id.clone(),
            ..Default::default()
        })
        .await?;
    let fork: ThreadForkResponse =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(fork_request_id)).await??;
    let fork_thread_id = ThreadId::from_string(&fork.thread.id)?;
    for relation in [
        ThreadListRelation::DirectChildrenOf(fork_thread_id),
        ThreadListRelation::DescendantsOf(fork_thread_id),
    ] {
        let fork_members = list_threads_for_relation(
            &mut mcp, relation, /*cursor*/ None, /*limit*/ 10,
            /*model_providers*/ None, /*source_kinds*/ None,
        )
        .await?;
        assert_eq!(fork_members.data, Vec::new());
        assert_eq!(fork_members.next_cursor, None);
    }

    let close_turn: TurnStartResponse = mcp
        .request(|request_id| ClientRequest::TurnStart {
            request_id,
            params: TurnStartParams {
                thread_id: thread.id.clone(),
                input: vec![UserInput::Text {
                    text: CLOSE_PROMPT.to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        })
        .await?;
    timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            let completed: TurnCompletedNotification =
                mcp.read_notification("turn/completed").await?;
            if completed.thread_id == thread.id && completed.turn.id == close_turn.turn.id {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;

    let after_close = list_threads_for_relation(
        &mut mcp,
        ThreadListRelation::DescendantsOf(parent_thread_id),
        /*cursor*/ None,
        /*limit*/ 10,
        /*model_providers*/ None,
        /*source_kinds*/ None,
    )
    .await?;
    assert_eq!(after_close.data.len(), 1);
    let closed_thread_id = spawned
        .iter()
        .find(|(_, agent_path)| agent_path.as_str() == "/root/current_worker_a")
        .map(|(thread_id, _)| thread_id)
        .expect("worker a must have spawned");
    assert_ne!(&after_close.data[0].id, closed_thread_id);
    assert_eq!(
        after_close.data[0].agent_status,
        Some(CollabAgentStatus::Completed)
    );

    Ok(())
}

#[tokio::test]
async fn thread_list_relation_filters_reject_invalid_requests() -> Result<()> {
    let codex_home = TempDir::new()?;
    create_minimal_config(codex_home.path())?;
    let mut mcp = init_mcp(codex_home.path()).await?;
    let request_id = mcp
        .send_thread_list_request(codex_app_server_protocol::ThreadListParams {
            cursor: None,
            limit: Some(10),
            sort_key: None,
            sort_direction: None,
            model_providers: None,
            source_kinds: None,
            archived: None,
            section_id: None,
            project_id: None,
            cwd: None,
            use_state_db_only: false,
            search_term: None,
            parent_thread_id: Some("not-a-thread-id".to_string()),
            ancestor_thread_id: None,
        })
        .await?;
    let error = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_error_message(RequestId::Integer(request_id)),
    )
    .await??;
    assert_eq!(error.error.code, -32600);

    let thread_id = ThreadId::new().to_string();
    let request_id = mcp
        .send_thread_list_request(codex_app_server_protocol::ThreadListParams {
            cursor: None,
            limit: Some(10),
            sort_key: None,
            sort_direction: None,
            model_providers: None,
            source_kinds: None,
            archived: None,
            section_id: None,
            project_id: None,
            cwd: None,
            use_state_db_only: false,
            search_term: None,
            parent_thread_id: Some(thread_id.clone()),
            ancestor_thread_id: Some(thread_id),
        })
        .await?;
    let error = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_error_message(RequestId::Integer(request_id)),
    )
    .await??;
    assert_eq!(error.error.code, -32600);
    assert_eq!(
        error.error.message,
        "parentThreadId and ancestorThreadId are mutually exclusive"
    );

    Ok(())
}
