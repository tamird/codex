use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn thread_list_relation_includes_explicitly_resumed_archived_v1_agent() -> Result<()> {
    const CHILD_PROMPT: &str = "complete the legacy archived resume worker";
    const SPAWN_PROMPT: &str = "spawn the legacy archived resume worker";
    const SPAWN_CALL_ID: &str = "spawn-legacy-archived-resume-worker";

    let server = responses::start_mock_server().await;
    responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            let body = String::from_utf8_lossy(&request.body);
            body.contains(CHILD_PROMPT) && !body.contains(SPAWN_CALL_ID)
        },
        responses::sse(vec![
            responses::ev_response_created("legacy-archived-child-complete"),
            responses::ev_assistant_message(
                "legacy-archived-child-message",
                "legacy archived resume worker complete",
            ),
            responses::ev_completed("legacy-archived-child-complete"),
        ]),
    )
    .await;

    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .enable_feature(Feature::Collab)
        .disable_feature(Feature::MultiAgentV2)
        .write(codex_home.path())?;
    write_models_cache(codex_home.path())?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;
    let root = mcp
        .start_thread(ThreadStartParams {
            model: Some("gpt-5.4".to_string()),
            ..Default::default()
        })
        .await?
        .thread;
    let root_id = ThreadId::from_string(&root.id)?;

    let spawn_output = invoke_model_tool(
        &mut mcp,
        &server,
        &root.id,
        SPAWN_PROMPT,
        SPAWN_CALL_ID,
        "multi_agent_v1",
        "spawn_agent",
        json!({"message": CHILD_PROMPT}),
    )
    .await?;
    let child_id = spawn_output["agent_id"]
        .as_str()
        .context("spawn_agent output must include agent_id")?
        .to_string();

    timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            let response = list_threads_for_relation(
                &mut mcp,
                ThreadListRelation::DescendantsOf(root_id),
                /*cursor*/ None,
                /*limit*/ 25,
                /*model_providers*/ None,
                /*source_kinds*/ None,
            )
            .await?;
            if response.data.len() == 1
                && response.data[0].id == child_id
                && response.data[0].agent_status.as_ref() == Some(&CollabAgentStatus::Completed)
            {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await??;

    let _: ThreadArchiveResponse = mcp
        .request(|request_id| ClientRequest::ThreadArchive {
            request_id,
            params: ThreadArchiveParams {
                thread_id: child_id.clone(),
            },
        })
        .await?;
    assert!(
        list_threads_for_relation(
            &mut mcp,
            ThreadListRelation::DescendantsOf(root_id),
            /*cursor*/ None,
            /*limit*/ 25,
            /*model_providers*/ None,
            /*source_kinds*/ None,
        )
        .await?
        .data
        .is_empty()
    );

    let resume_output = invoke_model_tool(
        &mut mcp,
        &server,
        &root.id,
        "resume the legacy archived worker explicitly",
        "resume-legacy-archived-worker",
        "multi_agent_v1",
        "resume_agent",
        json!({"id": child_id}),
    )
    .await?;
    assert_ne!(resume_output["status"], "not_found");

    let app_members = list_threads_for_relation(
        &mut mcp,
        ThreadListRelation::DescendantsOf(root_id),
        /*cursor*/ None,
        /*limit*/ 25,
        /*model_providers*/ None,
        /*source_kinds*/ None,
    )
    .await?;
    assert_eq!(app_members.data.len(), 1);
    assert_eq!(app_members.data[0].id, child_id);

    let archived_current = list_threads_for_relation_with_archived(
        &mut mcp,
        ThreadListRelation::DescendantsOf(root_id),
        /*cursor*/ None,
        /*limit*/ 25,
        /*model_providers*/ None,
        /*source_kinds*/ None,
        Some(true),
    )
    .await?;
    assert_eq!(archived_current.data, app_members.data);
    assert!(
        list_threads_for_relation_with_archived(
            &mut mcp,
            ThreadListRelation::DescendantsOf(root_id),
            /*cursor*/ None,
            /*limit*/ 25,
            /*model_providers*/ None,
            /*source_kinds*/ None,
            Some(false),
        )
        .await?
        .data
        .is_empty()
    );

    let _: ThreadArchiveResponse = mcp
        .request(|request_id| ClientRequest::ThreadArchive {
            request_id,
            params: ThreadArchiveParams {
                thread_id: child_id,
            },
        })
        .await?;
    assert!(
        list_threads_for_relation(
            &mut mcp,
            ThreadListRelation::DescendantsOf(root_id),
            /*cursor*/ None,
            /*limit*/ 25,
            /*model_providers*/ None,
            /*source_kinds*/ None,
        )
        .await?
        .data
        .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn thread_list_relation_paginates_app_members_and_matches_list_agents() -> Result<()> {
    const AGENT_COUNT: usize = 26;

    let server = responses::start_mock_server().await;
    for index in 0..AGENT_COUNT {
        let root_prompt = format!("dispatch bulk current-membership worker {index:02}");
        let call_id = format!("bulk-spawn-{index:02}");
        let worker_prompt = format!("bulk worker {index:02}");
        let task_name = format!("bulk_worker_{index:02}");
        let arguments = serde_json::to_string(&json!({
            "message": worker_prompt,
            "task_name": task_name,
            "fork_turns": "none",
            "agent_type": "explorer",
        }))?;
        let root_prompt_match = root_prompt.clone();
        let spawn_response_id = format!("bulk-root-spawn-{index:02}");
        responses::mount_sse_once_match(
            &server,
            move |request: &wiremock::Request| {
                String::from_utf8_lossy(&request.body).contains(&root_prompt_match)
            },
            responses::sse(vec![
                responses::ev_response_created(&spawn_response_id),
                responses::ev_function_call_with_namespace(
                    &call_id,
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
        let response_id = format!("bulk-worker-{index:02}-complete");
        responses::mount_sse_once_match(
            &server,
            move |request: &wiremock::Request| {
                let body = String::from_utf8_lossy(&request.body);
                body.contains(&worker_prompt_match) && !body.contains(&worker_call_id)
            },
            responses::sse(vec![
                responses::ev_response_created(&response_id),
                responses::ev_assistant_message(
                    &format!("{response_id}-message"),
                    "bulk worker complete",
                ),
                responses::ev_completed(&response_id),
            ]),
        )
        .await;
        let completion_call_id = call_id.clone();
        let root_complete_id = format!("bulk-root-complete-{index:02}");
        responses::mount_sse_once_match(
            &server,
            move |request: &wiremock::Request| {
                String::from_utf8_lossy(&request.body).contains(&completion_call_id)
            },
            responses::sse(vec![
                responses::ev_response_created(&root_complete_id),
                responses::ev_assistant_message(
                    &format!("{root_complete_id}-message"),
                    "bulk worker dispatched",
                ),
                responses::ev_completed(&root_complete_id),
            ]),
        )
        .await;
    }

    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_model("gpt-5.4")
        .with_extra_config("[features.multi_agent_v2]\nenabled = true")
        .write(codex_home.path())?;
    write_models_cache(codex_home.path())?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .with_json_logging("warn,codex_current_agent_list=debug")
        .build_initialized()
        .await?;
    let root = mcp
        .start_thread(ThreadStartParams {
            model: Some("gpt-5.4".to_string()),
            ephemeral: Some(true),
            ..Default::default()
        })
        .await?
        .thread;
    for index in 0..AGENT_COUNT {
        timeout(
            std::time::Duration::from_secs(30),
            mcp.start_turn_and_wait_for_completion(TurnStartParams {
                thread_id: root.id.clone(),
                input: vec![UserInput::Text {
                    text: format!("dispatch bulk current-membership worker {index:02}"),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            }),
        )
        .await??;
    }
    let root_id = ThreadId::from_string(&root.id)?;

    let model_members =
        list_all_model_current_agents(&mut mcp, &server, &root.id, "bulk-membership").await?;
    assert_eq!(model_members.len(), AGENT_COUNT);

    let mut app_members = Vec::new();
    let mut app_cursor = None;
    let mut app_cursors = std::collections::HashSet::new();
    let mut cold_identity_checked = false;
    loop {
        let response = list_threads_for_relation(
            &mut mcp,
            ThreadListRelation::DescendantsOf(root_id),
            app_cursor,
            /*limit*/ 7,
            /*model_providers*/ None,
            /*source_kinds*/ None,
        )
        .await?;
        for thread in &response.data {
            let normalized = normalize_app_current_agent(thread)?;
            if normalized.path == "/root/bulk_worker_00" {
                let SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    agent_nickname,
                    agent_role,
                    ..
                }) = &thread.source
                else {
                    anyhow::bail!("cold ephemeral relation member must be a thread spawn");
                };
                assert!(
                    agent_nickname
                        .as_deref()
                        .is_some_and(|name| !name.is_empty()),
                    "cold ephemeral relation member must retain its generated nickname"
                );
                assert_eq!(agent_role.as_deref(), Some("explorer"));
                assert_eq!(thread.agent_nickname.as_deref(), agent_nickname.as_deref());
                assert_eq!(thread.agent_role.as_deref(), agent_role.as_deref());
                cold_identity_checked = true;
            }
            app_members.push(normalized);
        }
        app_cursor = response.next_cursor;
        let Some(cursor) = app_cursor.as_ref() else {
            break;
        };
        assert!(app_cursors.insert(cursor.clone()), "app cursor repeated");
    }
    app_members.sort();
    let mut canonical_app_members = app_members
        .iter()
        .map(NormalizedCurrentAgent::canonical)
        .collect::<Vec<_>>();
    canonical_app_members.sort();
    assert_eq!(canonical_app_members, model_members);
    assert!(
        cold_identity_checked,
        "the oldest evicted ephemeral worker must retain registry identity"
    );

    mcp.wait_for_json_log_event("codex.current_agents.list")
        .await?;
    let events = mcp.json_log_events()?;
    let relation_events = events
        .iter()
        .filter(|event| event["fields"]["event.name"] == "codex.current_agents.list")
        .collect::<Vec<_>>();
    assert_eq!(relation_events.len(), 4);
    for event in relation_events {
        assert_eq!(event["fields"]["member_count"], AGENT_COUNT);
        assert_eq!(event["fields"]["metadata_batch_queries"], 1);
        assert_eq!(event["fields"]["scalar_thread_reads"], 0);
        assert_eq!(event["fields"]["persisted_count"], 0);
        assert!(event["fields"]["minimal_fallback_count"] != 0);
        assert!(event["fields"]["elapsed_ms"].as_u64().is_some());
    }

    const CLOSED_AGENT_PATH: &str = "/root/bulk_worker_00";
    let closed_agent_id = app_members
        .iter()
        .find(|member| member.path == CLOSED_AGENT_PATH)
        .map(|member| member.id.clone())
        .expect("oldest ephemeral worker must be current before close");
    const CLOSE_PROMPT: &str = "close the evicted ephemeral bulk worker";
    const CLOSE_CALL_ID: &str = "bulk-close-cold-ephemeral";
    let close_arguments = serde_json::to_string(&json!({"target": CLOSED_AGENT_PATH}))?;
    responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| String::from_utf8_lossy(&request.body).contains(CLOSE_PROMPT),
        responses::sse(vec![
            responses::ev_response_created("bulk-close-cold-ephemeral-request"),
            responses::ev_function_call_with_namespace(
                CLOSE_CALL_ID,
                "frodex",
                "close_agent",
                &close_arguments,
            ),
            responses::ev_completed("bulk-close-cold-ephemeral-request"),
        ]),
    )
    .await;
    let close_result = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            String::from_utf8_lossy(&request.body).contains(CLOSE_CALL_ID)
        },
        responses::sse(vec![
            responses::ev_response_created("bulk-close-cold-ephemeral-complete"),
            responses::ev_assistant_message(
                "bulk-close-cold-ephemeral-message",
                "closed cold ephemeral worker",
            ),
            responses::ev_completed("bulk-close-cold-ephemeral-complete"),
        ]),
    )
    .await;
    mcp.start_turn_and_wait_for_completion(TurnStartParams {
        thread_id: root.id.clone(),
        input: vec![UserInput::Text {
            text: CLOSE_PROMPT.to_string(),
            text_elements: Vec::new(),
        }],
        ..Default::default()
    })
    .await?;
    close_result
        .function_call_output_text(CLOSE_CALL_ID)
        .context("missing cold ephemeral close output")?;

    let app_after_close = list_threads_for_relation(
        &mut mcp,
        ThreadListRelation::DescendantsOf(root_id),
        /*cursor*/ None,
        /*limit*/ 200,
        /*model_providers*/ None,
        /*source_kinds*/ None,
    )
    .await?;
    assert_eq!(app_after_close.data.len(), AGENT_COUNT - 1);
    assert!(
        app_after_close
            .data
            .iter()
            .all(|thread| thread.id != closed_agent_id)
    );

    const AFTER_CLOSE_PROMPT: &str = "list bulk membership after cold ephemeral close";
    const AFTER_CLOSE_CALL_ID: &str = "bulk-list-after-cold-ephemeral-close";
    let after_close_arguments = serde_json::to_string(&json!({}))?;
    responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            String::from_utf8_lossy(&request.body).contains(AFTER_CLOSE_PROMPT)
        },
        responses::sse(vec![
            responses::ev_response_created("bulk-list-after-close-request"),
            responses::ev_function_call_with_namespace(
                AFTER_CLOSE_CALL_ID,
                "collaboration",
                "list_agents",
                &after_close_arguments,
            ),
            responses::ev_completed("bulk-list-after-close-request"),
        ]),
    )
    .await;
    let after_close_result = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            String::from_utf8_lossy(&request.body).contains(AFTER_CLOSE_CALL_ID)
        },
        responses::sse(vec![
            responses::ev_response_created("bulk-list-after-close-complete"),
            responses::ev_assistant_message(
                "bulk-list-after-close-message",
                "listed membership after close",
            ),
            responses::ev_completed("bulk-list-after-close-complete"),
        ]),
    )
    .await;
    mcp.start_turn_and_wait_for_completion(TurnStartParams {
        thread_id: root.id,
        input: vec![UserInput::Text {
            text: AFTER_CLOSE_PROMPT.to_string(),
            text_elements: Vec::new(),
        }],
        ..Default::default()
    })
    .await?;
    let after_close_output: serde_json::Value = serde_json::from_str(
        &after_close_result
            .function_call_output_text(AFTER_CLOSE_CALL_ID)
            .context("missing list_agents output after close")?,
    )?;
    let after_close_agents = after_close_output["agents"]
        .as_array()
        .context("list_agents agents must be an array")?;
    assert_eq!(after_close_agents.len(), AGENT_COUNT);
    assert!(after_close_agents.iter().all(|agent| {
        agent["agent_name"] != CLOSED_AGENT_PATH
            && agent.get("agent_id").is_none()
            && agent.get("parent_agent_id").is_none()
    }));
    Ok(())
}
