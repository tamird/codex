use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn thread_list_relation_matches_list_agents_immediately_after_spawn_started() -> Result<()> {
    const PARENT_PROMPT: &str = "spawn one worker and list current agents immediately";
    const CHILD_PROMPT: &str = "stay active while membership is compared";
    const SPAWN_CALL_ID: &str = "spawn-immediate-membership-worker";
    const LIST_CALL_ID: &str = "list-immediate-membership";

    let server = responses::start_mock_server().await;
    let spawn_args = serde_json::to_string(&json!({
        "message": CHILD_PROMPT,
        "task_name": "immediate_worker",
        "fork_turns": "none",
    }))?;
    responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            String::from_utf8_lossy(&request.body).contains(PARENT_PROMPT)
        },
        responses::sse(vec![
            responses::ev_response_created("parent-immediate-spawn"),
            responses::ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                "collaboration",
                "spawn_agent",
                &spawn_args,
            ),
            responses::ev_completed("parent-immediate-spawn"),
        ]),
    )
    .await;
    responses::mount_response_once_match(
        &server,
        |request: &wiremock::Request| {
            let body = String::from_utf8_lossy(&request.body);
            body.contains(CHILD_PROMPT) && !body.contains(SPAWN_CALL_ID)
        },
        responses::sse_response(responses::sse(vec![
            responses::ev_response_created("immediate-worker-response"),
            responses::ev_assistant_message("immediate-worker-message", "worker complete"),
            responses::ev_completed("immediate-worker-response"),
        ]))
        .set_delay(std::time::Duration::from_secs(3)),
    )
    .await;
    responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            String::from_utf8_lossy(&request.body).contains(SPAWN_CALL_ID)
        },
        responses::sse(vec![
            responses::ev_response_created("parent-immediate-list"),
            responses::ev_function_call_with_namespace(
                LIST_CALL_ID,
                "collaboration",
                "list_agents",
                "{}",
            ),
            responses::ev_completed("parent-immediate-list"),
        ]),
    )
    .await;
    let list_return = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| String::from_utf8_lossy(&request.body).contains(LIST_CALL_ID),
        responses::sse(vec![
            responses::ev_response_created("parent-immediate-finished"),
            responses::ev_assistant_message("parent-immediate-message", "membership compared"),
            responses::ev_completed("parent-immediate-finished"),
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
    let turn: TurnStartResponse = mcp
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

    let child_thread_id = timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            let completed: ItemCompletedNotification =
                mcp.read_notification("item/completed").await?;
            if let ThreadItem::SubAgentActivity {
                id,
                kind: SubAgentActivityKind::Started,
                agent_thread_id,
                ..
            } = completed.item
                && id == SPAWN_CALL_ID
            {
                return Ok::<String, anyhow::Error>(agent_thread_id);
            }
        }
    })
    .await??;

    let immediate_app_members = list_threads_for_relation(
        &mut mcp,
        ThreadListRelation::DescendantsOf(parent_thread_id),
        /*cursor*/ None,
        /*limit*/ 25,
        /*model_providers*/ None,
        /*source_kinds*/ None,
    )
    .await?;
    assert_eq!(immediate_app_members.data.len(), 1);
    assert_eq!(immediate_app_members.data[0].id, child_thread_id);

    let list_agents_output = timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            if let Some(output) = list_return.function_call_output_text(LIST_CALL_ID) {
                return Ok::<String, anyhow::Error>(output);
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await??;
    let list_agents_output: serde_json::Value = serde_json::from_str(&list_agents_output)?;
    assert_eq!(list_agents_output["total_count"], 1);
    assert_eq!(list_agents_output["next_cursor"], serde_json::Value::Null);
    let model_member = list_agents_output["agents"]
        .as_array()
        .expect("list_agents must return agents")
        .first()
        .expect("list_agents must return the spawned worker");

    let app_members = list_threads_for_relation(
        &mut mcp,
        ThreadListRelation::DescendantsOf(parent_thread_id),
        /*cursor*/ None,
        /*limit*/ 25,
        /*model_providers*/ None,
        /*source_kinds*/ None,
    )
    .await?;
    assert_eq!(app_members.data.len(), 1);
    let app_member = &app_members.data[0];
    assert_eq!(app_member.id, child_thread_id);
    assert_eq!(
        app_member.parent_thread_id.as_deref(),
        Some(thread.id.as_str())
    );
    let app_path = match &app_member.source {
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn { agent_path, .. }) => agent_path
            .as_ref()
            .map(ToString::to_string)
            .expect("current app member must expose its canonical path"),
        source => panic!("current app member had unexpected source: {source:?}"),
    };
    assert_eq!(model_member["agent_id"], child_thread_id);
    assert_eq!(model_member["parent_agent_id"], thread.id);
    assert_eq!(model_member["agent_name"], app_path);
    let expected_status = match app_member
        .agent_status
        .as_ref()
        .expect("relation listing must expose agentStatus")
    {
        CollabAgentStatus::PendingInit => "pending_init",
        CollabAgentStatus::Running => "running",
        CollabAgentStatus::Interrupted => "interrupted",
        CollabAgentStatus::Completed => "completed",
        CollabAgentStatus::Errored => "errored",
        CollabAgentStatus::Shutdown => "shutdown",
        CollabAgentStatus::NotFound => "not_found",
    };
    assert_eq!(model_member["agent_status"], expected_status);

    timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            let completed: TurnCompletedNotification =
                mcp.read_notification("turn/completed").await?;
            if completed.thread_id == thread.id && completed.turn.id == turn.turn.id {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn thread_list_relation_scopes_nested_ancestors_without_siblings() -> Result<()> {
    const ROOT_PROMPT: &str = "spawn nested parent and sibling";
    const NESTED_PARENT_PROMPT: &str = "spawn a grandchild from nested parent";
    const SIBLING_PROMPT: &str = "complete sibling";
    const GRANDCHILD_PROMPT: &str = "complete nested grandchild";
    const QUIESCE_NESTED_PARENT_PROMPT: &str = "quiesce nested parent after its grandchild";
    const QUIESCE_NESTED_PARENT_MESSAGE: &str = "finish the nested parent follow-up";
    const ROOT_A_CALL: &str = "spawn-nested-parent";
    const ROOT_B_CALL: &str = "spawn-nested-sibling";
    const GRANDCHILD_CALL: &str = "spawn-nested-grandchild";

    let server = responses::start_mock_server().await;
    let nested_parent_args = serde_json::to_string(&json!({
        "message": NESTED_PARENT_PROMPT,
        "task_name": "nested_parent",
        "fork_turns": "none",
    }))?;
    let sibling_args = serde_json::to_string(&json!({
        "message": SIBLING_PROMPT,
        "task_name": "nested_sibling",
        "fork_turns": "none",
    }))?;
    responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| String::from_utf8_lossy(&request.body).contains(ROOT_PROMPT),
        responses::sse(vec![
            responses::ev_response_created("nested-root-spawn"),
            responses::ev_function_call_with_namespace(
                ROOT_A_CALL,
                "collaboration",
                "spawn_agent",
                &nested_parent_args,
            ),
            responses::ev_function_call_with_namespace(
                ROOT_B_CALL,
                "collaboration",
                "spawn_agent",
                &sibling_args,
            ),
            responses::ev_completed("nested-root-spawn"),
        ]),
    )
    .await;
    let grandchild_args = serde_json::to_string(&json!({
        "message": GRANDCHILD_PROMPT,
        "task_name": "nested_grandchild",
        "fork_turns": "none",
    }))?;
    responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            let body = String::from_utf8_lossy(&request.body);
            body.contains(NESTED_PARENT_PROMPT) && !body.contains(ROOT_A_CALL)
        },
        responses::sse(vec![
            responses::ev_response_created("nested-parent-spawn"),
            responses::ev_function_call_with_namespace(
                GRANDCHILD_CALL,
                "collaboration",
                "spawn_agent",
                &grandchild_args,
            ),
            responses::ev_completed("nested-parent-spawn"),
        ]),
    )
    .await;
    for (prompt, spawn_call_id, response_id) in [
        (SIBLING_PROMPT, ROOT_B_CALL, "nested-sibling-complete"),
        (
            GRANDCHILD_PROMPT,
            GRANDCHILD_CALL,
            "nested-grandchild-complete",
        ),
    ] {
        responses::mount_sse_once_match(
            &server,
            move |request: &wiremock::Request| {
                let body = String::from_utf8_lossy(&request.body);
                body.contains(prompt) && !body.contains(spawn_call_id)
            },
            responses::sse(vec![
                responses::ev_response_created(response_id),
                responses::ev_assistant_message(&format!("{response_id}-message"), "complete"),
                responses::ev_completed(response_id),
            ]),
        )
        .await;
    }
    for (call_id, response_id) in [
        (GRANDCHILD_CALL, "nested-parent-complete"),
        (ROOT_B_CALL, "nested-root-complete"),
    ] {
        responses::mount_sse_once_match(
            &server,
            move |request: &wiremock::Request| {
                String::from_utf8_lossy(&request.body).contains(call_id)
            },
            responses::sse(vec![
                responses::ev_response_created(response_id),
                responses::ev_assistant_message(&format!("{response_id}-message"), "complete"),
                responses::ev_completed(response_id),
            ]),
        )
        .await;
    }
    const RESIDENCY_PRESSURE_AGENT_COUNT: usize = 16;
    for index in 0..RESIDENCY_PRESSURE_AGENT_COUNT {
        let root_prompt = format!("dispatch nested residency pressure worker {index:02}");
        let call_id = format!("nested-pressure-spawn-{index:02}");
        let worker_prompt = format!("complete nested residency pressure worker {index:02}");
        let task_name = format!("nested_pressure_worker_{index:02}");
        let arguments = serde_json::to_string(&json!({
            "message": worker_prompt,
            "task_name": task_name,
            "fork_turns": "none",
        }))?;
        let root_prompt_match = root_prompt.clone();
        let spawn_call_id = call_id.clone();
        let spawn_response_id = format!("nested-pressure-root-spawn-{index:02}");
        responses::mount_sse_once_match(
            &server,
            move |request: &wiremock::Request| {
                String::from_utf8_lossy(&request.body).contains(&root_prompt_match)
            },
            responses::sse(vec![
                responses::ev_response_created(&spawn_response_id),
                responses::ev_function_call_with_namespace(
                    &spawn_call_id,
                    "collaboration",
                    "spawn_agent",
                    &arguments,
                ),
                responses::ev_completed(&spawn_response_id),
            ]),
        )
        .await;
        let worker_prompt_match = worker_prompt.clone();
        let worker_call_id = call_id.clone();
        let worker_response_id = format!("nested-pressure-worker-complete-{index:02}");
        responses::mount_sse_once_match(
            &server,
            move |request: &wiremock::Request| {
                let body = String::from_utf8_lossy(&request.body);
                body.contains(&worker_prompt_match) && !body.contains(&worker_call_id)
            },
            responses::sse(vec![
                responses::ev_response_created(&worker_response_id),
                responses::ev_assistant_message(
                    &format!("{worker_response_id}-message"),
                    "nested residency pressure worker complete",
                ),
                responses::ev_completed(&worker_response_id),
            ]),
        )
        .await;
        let completion_call_id = call_id.clone();
        let root_complete_id = format!("nested-pressure-root-complete-{index:02}");
        responses::mount_sse_once_match(
            &server,
            move |request: &wiremock::Request| {
                String::from_utf8_lossy(&request.body).contains(&completion_call_id)
            },
            responses::sse(vec![
                responses::ev_response_created(&root_complete_id),
                responses::ev_assistant_message(
                    &format!("{root_complete_id}-message"),
                    "nested residency pressure worker dispatched",
                ),
                responses::ev_completed(&root_complete_id),
            ]),
        )
        .await;
    }

    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_model("gpt-5.6-sol")
        .with_extra_config("[features.multi_agent_v2]\nenabled = true")
        .write(codex_home.path())?;
    write_models_cache(codex_home.path())?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let root = mcp
        .start_thread(ThreadStartParams {
            model: Some("gpt-5.6-sol".to_string()),
            ..Default::default()
        })
        .await?
        .thread;
    mcp.start_turn_and_wait_for_completion(TurnStartParams {
        thread_id: root.id.clone(),
        input: vec![UserInput::Text {
            text: ROOT_PROMPT.to_string(),
            text_elements: Vec::new(),
        }],
        ..Default::default()
    })
    .await?;
    let root_id = ThreadId::from_string(&root.id)?;

    let root_descendants = timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            let response = list_threads_for_relation(
                &mut mcp,
                ThreadListRelation::DescendantsOf(root_id),
                None,
                10,
                None,
                None,
            )
            .await?;
            if response.data.len() == 3
                && response.data.iter().all(|thread| {
                    thread.agent_status.as_ref() == Some(&CollabAgentStatus::Completed)
                })
            {
                return Ok::<ThreadListResponse, anyhow::Error>(response);
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await??;
    let nested_parent = root_descendants
        .data
        .iter()
        .find(|thread| match &thread.source {
            SessionSource::SubAgent(SubAgentSource::ThreadSpawn { agent_path, .. }) => agent_path
                .as_ref()
                .is_some_and(|path| path.as_str() == "/root/nested_parent"),
            _ => false,
        })
        .expect("nested parent must be listed");
    let nested_parent_id = ThreadId::from_string(&nested_parent.id)?;
    let nested_direct = list_threads_for_relation(
        &mut mcp,
        ThreadListRelation::DirectChildrenOf(nested_parent_id),
        None,
        10,
        None,
        None,
    )
    .await?;
    let nested_descendants = list_threads_for_relation(
        &mut mcp,
        ThreadListRelation::DescendantsOf(nested_parent_id),
        None,
        10,
        None,
        None,
    )
    .await?;
    assert_eq!(nested_direct.data, nested_descendants.data);
    assert_eq!(nested_descendants.data.len(), 1);
    assert!(matches!(
        &nested_descendants.data[0].source,
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn { agent_path: Some(path), .. })
            if path.as_str() == "/root/nested_parent/nested_grandchild"
    ));
    let root_direct = list_threads_for_relation(
        &mut mcp,
        ThreadListRelation::DirectChildrenOf(root_id),
        None,
        10,
        None,
        None,
    )
    .await?;
    assert_eq!(root_direct.data.len(), 2);

    let nested_parent_request_id = nested_parent.id.clone();
    let nested_parent_followup = responses::mount_sse_once_match(
        &server,
        move |request: &wiremock::Request| {
            let body = serde_json::from_slice::<serde_json::Value>(&request.body)
                .expect("request body should be valid JSON");
            body["client_metadata"]["thread_id"] == nested_parent_request_id.as_str()
                && body.to_string().contains(QUIESCE_NESTED_PARENT_MESSAGE)
        },
        responses::sse(vec![
            responses::ev_response_created("nested-parent-followup"),
            responses::ev_assistant_message(
                "nested-parent-followup-message",
                "nested parent follow-up complete",
            ),
            responses::ev_completed("nested-parent-followup"),
        ]),
    )
    .await;
    let _followup_output = invoke_model_tool(
        &mut mcp,
        &server,
        &root.id,
        QUIESCE_NESTED_PARENT_PROMPT,
        "quiesce-nested-parent",
        "collaboration",
        "followup_task",
        json!({
            "target": "nested_parent",
            "message": QUIESCE_NESTED_PARENT_MESSAGE,
        }),
    )
    .await?;
    timeout(DEFAULT_READ_TIMEOUT, async {
        while nested_parent_followup.requests().is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            let response = list_threads_for_relation(
                &mut mcp,
                ThreadListRelation::DescendantsOf(root_id),
                None,
                25,
                None,
                None,
            )
            .await?;
            if response.data.iter().any(|thread| {
                thread.id == nested_parent.id
                    && thread.agent_status.as_ref() == Some(&CollabAgentStatus::Completed)
            }) {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await??;

    let mut nested_parent_unloaded = false;
    for index in 0..RESIDENCY_PRESSURE_AGENT_COUNT {
        timeout(
            std::time::Duration::from_secs(30),
            mcp.start_turn_and_wait_for_completion(TurnStartParams {
                thread_id: root.id.clone(),
                input: vec![UserInput::Text {
                    text: format!("dispatch nested residency pressure worker {index:02}"),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            }),
        )
        .await??;
        let loaded: ThreadLoadedListResponse = mcp
            .request(|request_id| ClientRequest::ThreadLoadedList {
                request_id,
                params: ThreadLoadedListParams::default(),
            })
            .await?;
        if !loaded.data.contains(&nested_parent.id) {
            nested_parent_unloaded = true;
            break;
        }
    }

    assert!(
        nested_parent_unloaded,
        "residency pressure must unload the nested relation root"
    );

    let cold_nested_direct = list_threads_for_relation(
        &mut mcp,
        ThreadListRelation::DirectChildrenOf(nested_parent_id),
        None,
        25,
        None,
        None,
    )
    .await?;
    let cold_nested_descendants = list_threads_for_relation(
        &mut mcp,
        ThreadListRelation::DescendantsOf(nested_parent_id),
        None,
        25,
        None,
        None,
    )
    .await?;
    assert_eq!(cold_nested_direct.data, cold_nested_descendants.data);
    assert_eq!(cold_nested_descendants.data.len(), 1);
    assert!(matches!(
        &cold_nested_descendants.data[0].source,
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn { agent_path: Some(path), .. })
            if path.as_str() == "/root/nested_parent/nested_grandchild"
    ));

    let model_members =
        list_all_model_current_agents(&mut mcp, &server, &root.id, "nested-cold-root").await?;
    let app_members = list_threads_for_relation(
        &mut mcp,
        ThreadListRelation::DescendantsOf(root_id),
        None,
        200,
        None,
        None,
    )
    .await?;
    let mut app_members = app_members
        .data
        .iter()
        .map(normalize_app_current_agent)
        .collect::<Result<Vec<_>>>()?;
    app_members.sort();
    assert_eq!(model_members, app_members);
    assert!(
        app_members.iter().any(|member| {
            member.id == nested_parent.id && member.path == "/root/nested_parent"
        })
    );
    assert!(
        app_members
            .iter()
            .any(|member| { member.path == "/root/nested_parent/nested_grandchild" })
    );

    let lazy_grandchild_id = cold_nested_descendants.data[0].id.clone();
    timeout(DEFAULT_READ_TIMEOUT, mcp.shutdown_gracefully()).await??;
    drop(mcp);
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let resume_id = mcp
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: root.id.clone(),
            ..Default::default()
        })
        .await?;
    let _: ThreadResumeResponse =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(resume_id)).await??;
    const LAZY_GRANDCHILD_MESSAGE: &str = "remain current without restoring your parent";
    let _send_output = invoke_model_tool(
        &mut mcp,
        &server,
        &root.id,
        "select only the nested grandchild by UUID",
        "lazy-grandchild-send",
        "collaboration",
        "send_message",
        json!({
            "target": lazy_grandchild_id,
            "message": LAZY_GRANDCHILD_MESSAGE,
        }),
    )
    .await?;
    let lazy_model_members =
        list_all_model_current_agents(&mut mcp, &server, &root.id, "lazy-grandchild-root").await?;
    let lazy_descendants = list_threads_for_relation(
        &mut mcp,
        ThreadListRelation::DescendantsOf(nested_parent_id),
        None,
        25,
        None,
        None,
    )
    .await?;
    assert_eq!(lazy_model_members.len(), 1);
    assert_eq!(lazy_model_members[0].id, lazy_grandchild_id);
    assert_ne!(lazy_model_members[0].id, nested_parent.id);

    let lazy_direct = list_threads_for_relation(
        &mut mcp,
        ThreadListRelation::DirectChildrenOf(nested_parent_id),
        None,
        25,
        None,
        None,
    )
    .await?;
    assert_eq!(lazy_direct.data, lazy_descendants.data);
    let lazy_app_members = lazy_descendants
        .data
        .iter()
        .map(normalize_app_current_agent)
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(lazy_model_members, lazy_app_members);

    let _: ThreadArchiveResponse = mcp
        .request(|request_id| ClientRequest::ThreadArchive {
            request_id,
            params: ThreadArchiveParams {
                thread_id: nested_parent.id.clone(),
            },
        })
        .await?;
    let model_after_archive =
        list_all_model_current_agents(&mut mcp, &server, &root.id, "nested-after-archive").await?;
    let app_after_archive = list_threads_for_relation(
        &mut mcp,
        ThreadListRelation::DescendantsOf(root_id),
        None,
        200,
        None,
        None,
    )
    .await?;
    let mut app_after_archive = app_after_archive
        .data
        .iter()
        .map(normalize_app_current_agent)
        .collect::<Result<Vec<_>>>()?;
    app_after_archive.sort();
    assert_eq!(model_after_archive, app_after_archive);
    assert!(
        app_after_archive
            .iter()
            .all(|member| { !member.path.starts_with("/root/nested_parent") })
    );
    assert!(
        list_threads_for_relation(
            &mut mcp,
            ThreadListRelation::DescendantsOf(nested_parent_id),
            None,
            25,
            None,
            None,
        )
        .await?
        .data
        .is_empty()
    );

    let state_db = codex_state::StateRuntime::init(
        codex_state::SqliteConfig::new_for_testing(codex_home.path().abs()),
        "mock_provider".to_string(),
    )
    .await?;
    let open_descendants = state_db
        .list_thread_spawn_descendants_with_status(root_id, DirectionalThreadSpawnEdgeStatus::Open)
        .await?;
    assert!(open_descendants.contains(&nested_parent_id));
    assert!(
        open_descendants.contains(&ThreadId::from_string(&cold_nested_descendants.data[0].id,)?)
    );
    assert!(
        state_db
            .get_thread(nested_parent_id)
            .await?
            .is_some_and(|thread| thread.archived_at.is_some())
    );
    state_db.close().await;

    Ok(())
}
