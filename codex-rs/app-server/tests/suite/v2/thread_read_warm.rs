use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn complete_native_projection_reads_and_resumes_without_opening_ancestors() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
    let thread_id = codex_protocol::ThreadId::new();
    let sqlite = codex_state::SqliteConfig::new_for_testing(codex_home.path().abs());
    let state_db =
        codex_state::StateRuntime::init(sqlite.clone(), "mock_provider".to_string()).await?;
    let store = LocalThreadStore::new(
        LocalThreadStoreConfig {
            codex_home: codex_home.path().to_path_buf(),
            sqlite,
            default_model_provider_id: "mock_provider".to_string(),
        },
        Some(state_db),
    );
    store
        .create_thread(CreateThreadParams {
            session_id: thread_id.into(),
            thread_id,
            extra_config: None,
            forked_from_id: None,
            forked_from_ordinal_exclusive: None,
            parent_thread_id: None,
            source: ProtocolSessionSource::Cli,
            thread_source: None,
            originator: "test_originator".to_string(),
            base_instructions: BaseInstructions::default(),
            dynamic_tools: Vec::new(),
            selected_capability_roots: Vec::new(),
            multi_agent_version: None,
            history_mode: codex_protocol::protocol::ThreadHistoryMode::Paginated,
            history_base: None,
            subagent_history_start_ordinal: None,
            persistence_mode: Default::default(),
            initial_rollout_ordinal: 0,
            initial_window_id: Uuid::now_v7().to_string(),
            metadata: ThreadPersistenceMetadata {
                cwd: Some(codex_home.path().to_path_buf()),
                model_provider: "mock_provider".to_string(),
                memory_mode: ThreadMemoryMode::Enabled,
            },
        })
        .await?;
    store
        .persist_thread(thread_id, PersistContext::Standard)
        .await?;
    let mut ancestors = Vec::new();
    for index in 0..8 {
        let turn_id = format!("turn-{index}");
        let mut items = if index == 7 {
            certified_recent_history_checkpoint(codex_home.path()).into_items()
        } else {
            Vec::new()
        };
        items.extend([
            paginated_turn_started(&turn_id),
            paginated_completed_item(
                thread_id,
                &turn_id,
                CoreTurnItem::UserMessage(UserMessageItem {
                    id: format!("user-{index}"),
                    client_id: None,
                    content: vec![codex_protocol::user_input::UserInput::Text {
                        text: format!("user {index}"),
                        text_elements: Vec::new(),
                    }],
                }),
            ),
            paginated_turn_completed(&turn_id),
        ]);
        store
            .append_items(AppendThreadItemsParams { thread_id, items })
            .await?;
        if index < 7 {
            ancestors.push(
                store
                    .freeze_thread_segment(
                        thread_id,
                        FreezeRolloutSegmentParams::rotate(Vec::new()),
                    )
                    .await?
                    .reference
                    .rollout_path,
            );
        }
    }
    store.shutdown_thread(thread_id).await?;
    assert!(store.rebuild_history_projection(thread_id).await?);
    assert!(store.has_history_projection(thread_id).await?);
    drop(store);

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .with_env_overrides(&[("FRODEX_HISTORY_IO_TRACE", Some("1"))])
        .with_json_logging("codex_history_io=trace")
        .build_initialized()
        .await?;
    // A warm daemon does not imply a resident task. Exercise repeated reads before resume,
    // then cold task resume and resident rejoin in the same daemon.
    for _ in 0..2 {
        let response: ThreadReadResponse = mcp
            .request(|request_id| ClientRequest::ThreadRead {
                request_id,
                params: ThreadReadParams {
                    thread_id: thread_id.to_string(),
                    include_turns: false,
                },
            })
            .await?;
        assert!(response.thread.turns.is_empty());
        let page = read_turns_page(
            &mut mcp,
            thread_id,
            /*cursor*/ None,
            Some(2),
            SortDirection::Desc,
            Some(TurnItemsView::Summary),
        )
        .await?;
        assert_eq!(turn_user_texts(&page.data), vec!["user 7", "user 6"]);
        let items = read_items_page(
            &mut mcp,
            thread_id,
            Some("turn-6"),
            /*cursor*/ None,
            Some(2),
            SortDirection::Desc,
        )
        .await?;
        assert_eq!(items.data.len(), 1);
        assert_eq!(items.data[0].item.id(), "user-6");
    }
    for _ in 0..2 {
        let response: ThreadResumeResponse = mcp
            .request(|request_id| ClientRequest::ThreadResume {
                request_id,
                params: ThreadResumeParams {
                    thread_id: thread_id.to_string(),
                    exclude_turns: true,
                    initial_turns_page: Some(ThreadResumeInitialTurnsPageParams {
                        limit: Some(2),
                        sort_direction: Some(SortDirection::Desc),
                        items_view: Some(TurnItemsView::Summary),
                    }),
                    ..Default::default()
                },
            })
            .await?;
        assert!(response.thread.turns.is_empty());
        assert_eq!(
            turn_user_texts(&response.initial_turns_page.expect("initial page").data),
            vec!["user 7", "user 6"]
        );
    }
    mcp.shutdown_gracefully().await?;
    mcp.wait_for_json_log_event("codex.history.rollout.open")
        .await?;
    let opened_paths = mcp
        .json_log_events()?
        .into_iter()
        .filter(|event| event["fields"]["event.name"] == "codex.history.rollout.open")
        .filter_map(|event| {
            event["fields"]["rollout_path"]
                .as_str()
                .map(std::path::PathBuf::from)
        })
        .collect::<Vec<_>>();
    assert!(
        !opened_paths.is_empty(),
        "rollout I/O observation must be active"
    );
    let opened_ancestors = opened_paths
        .into_iter()
        .filter(|path| ancestors.contains(path))
        .collect::<Vec<_>>();
    assert_eq!(opened_ancestors, Vec::<std::path::PathBuf>::new());
    Ok(())
}
