use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_fake_paginated_rollout;
use app_test_support::create_fake_rollout_with_text_elements;
use app_test_support::create_mock_responses_server_repeating_assistant;
use app_test_support::rollout_path;
use app_test_support::test_absolute_path;
use app_test_support::to_response;
use codex_app_server::in_process;
use codex_app_server::in_process::InProcessStartArgs;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::DeprecationNoticeNotification;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::InitializeParams;
use codex_app_server_protocol::JSONRPCError;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SessionSource;
use codex_app_server_protocol::SortDirection;
use codex_app_server_protocol::ThreadForkParams;
use codex_app_server_protocol::ThreadForkResponse;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadItemEntry;
use codex_app_server_protocol::ThreadItemsListParams;
use codex_app_server_protocol::ThreadItemsListResponse;
use codex_app_server_protocol::ThreadListParams;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadNameUpdatedNotification;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadResumeInitialTurnsPageParams;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadSearchOccurrencesParams;
use codex_app_server_protocol::ThreadSearchOccurrencesResponse;
use codex_app_server_protocol::ThreadSetNameParams;
use codex_app_server_protocol::ThreadSetNameResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::ThreadStatus;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::ThreadTurnsListResponse;
use codex_app_server_protocol::Turn;
use codex_app_server_protocol::TurnItemsView;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;
use codex_arg0::Arg0DispatchPaths;
use codex_config::CloudConfigBundleLoader;
use codex_config::LoaderOverrides;
use codex_core::ARCHIVED_SESSIONS_SUBDIR;
use codex_core::config::ConfigBuilder;
use codex_exec_server::EnvironmentManager;
use codex_feedback::CodexFeedback;
use codex_protocol::AgentPath;
use codex_protocol::SegmentId;
use codex_protocol::config_types::ApprovalsReviewer as ProtocolApprovalsReviewer;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::ReasoningSummary;
use codex_protocol::config_types::Settings;
use codex_protocol::config_types::WindowsSandboxLevel;
use codex_protocol::items::AgentMessageContent;
use codex_protocol::items::AgentMessageItem;
use codex_protocol::items::PlanItem;
use codex_protocol::items::TurnItem as CoreTurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::ContentItem;
use codex_protocol::models::MessagePhase;
use codex_protocol::models::PermissionProfile;
use codex_protocol::models::ResponseItem;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::AgentMessageEvent;
use codex_protocol::protocol::AgentStatus as CoreAgentStatus;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::CollabAgentSpawnEndEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SandboxPolicy;
use codex_protocol::protocol::SegmentPreviousTurnSettings;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource as ProtocolSessionSource;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::ThreadSettingsSnapshot;
use codex_protocol::protocol::TokenCountEvent;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnContextItem;
use codex_protocol::protocol::TurnEnvironmentSelections;
use codex_protocol::protocol::TurnStartedEvent;
use codex_protocol::protocol::UserMessageEvent;
use codex_protocol::user_input::ByteRange;
use codex_protocol::user_input::TextElement;
use codex_rollout::CertifiedSegmentStateCheckpoint;
use codex_rollout::CompactedItem;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_rollout::RolloutRecorder;
use codex_thread_store::AppendThreadItemsParams;
use codex_thread_store::CreateThreadParams;
use codex_thread_store::FreezeRolloutSegmentParams;
use codex_thread_store::InMemoryThreadStore;
use codex_thread_store::ListTurnsParams as StoreListTurnsParams;
use codex_thread_store::LocalThreadStore;
use codex_thread_store::LocalThreadStoreConfig;
use codex_thread_store::PersistContext;
use codex_thread_store::ReadThreadParams as StoreReadThreadParams;
use codex_thread_store::SortDirection as StoreSortDirection;
use codex_thread_store::StoredTurnItemsView;
use codex_thread_store::ThreadMetadataPatch;
use codex_thread_store::ThreadPersistenceMetadata;
use codex_thread_store::ThreadStore;
use codex_thread_store::UpdateThreadMetadataParams;
use codex_utils_absolute_path::test_support::PathExt;
use core_test_support::responses;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::time::timeout;
use uuid::Uuid;

#[cfg(windows)]
const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(25);
#[cfg(not(windows))]
const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

fn certified_recent_history_checkpoint(codex_home: &Path) -> CertifiedSegmentStateCheckpoint {
    let window_id = Uuid::now_v7();
    let cwd = codex_home.abs();
    CertifiedSegmentStateCheckpoint::new(
        CompactedItem {
            message: "latest certified history checkpoint".to_string(),
            replacement_history: Some(vec![
                ResponseItem::Message {
                    id: None,
                    role: "user".to_string(),
                    content: vec![ContentItem::InputText {
                        text: "checkpoint replacement history".to_string(),
                    }],
                    phase: None,
                    internal_chat_message_metadata_passthrough: None,
                }
                .into(),
            ]),
            mcp_resource_origins: None,
            window_number: Some(1),
            first_window_id: Some(window_id.to_string()),
            previous_window_id: None,
            window_id: Some(window_id.to_string()),
            segment_state_checkpoint: None,
        },
        Some(SegmentPreviousTurnSettings {
            model: "mock-model".to_string(),
            comp_hash: None,
            realtime_active: None,
        }),
        /*world_state*/ None,
        /*reference_context*/ None,
        ThreadSettingsAppliedEvent {
            thread_settings: ThreadSettingsSnapshot {
                model: "mock-model".to_string(),
                model_provider_id: "mock_provider".to_string(),
                service_tier: None,
                approval_policy: AskForApproval::Never,
                approvals_reviewer: ProtocolApprovalsReviewer::User,
                permission_profile: PermissionProfile::workspace_write(),
                active_permission_profile: None,
                cwd: cwd.clone(),
                environments: Some(TurnEnvironmentSelections::new(cwd, Vec::new())),
                workspace_roots: Some(Vec::new()),
                profile_workspace_roots: Some(Vec::new()),
                windows_sandbox_level: Some(WindowsSandboxLevel::Disabled),
                reasoning_effort: None,
                reasoning_summary: None,
                personality: None,
                collaboration_mode: CollaborationMode {
                    mode: ModeKind::Default,
                    settings: Settings {
                        model: "mock-model".to_string(),
                        reasoning_effort: None,
                        developer_instructions: None,
                    },
                },
            },
        },
        TokenCountEvent {
            info: None,
            rate_limits: None,
        },
    )
    .expect("valid recent history checkpoint")
}

#[tokio::test]
async fn thread_read_returns_summary_without_turns() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let preview = "Saved user message";
    let text_elements = [TextElement::new(
        ByteRange { start: 0, end: 5 },
        Some("<note>".into()),
    )];
    let conversation_id = create_fake_rollout_with_text_elements(
        codex_home.path(),
        "2025-01-05T12-00-00",
        "2025-01-05T12:00:00Z",
        preview,
        text_elements
            .iter()
            .map(|elem| serde_json::to_value(elem).expect("serialize text element"))
            .collect(),
        Some("mock_provider"),
        /*git_info*/ None,
    )?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;

    let read_id = mcp
        .send_thread_read_request(ThreadReadParams {
            thread_id: conversation_id.clone(),
            include_turns: false,
        })
        .await?;
    let ThreadReadResponse { thread, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(read_id)).await??;

    assert_eq!(thread.id, conversation_id);
    assert_eq!(thread.preview, preview);
    assert_eq!(thread.model_provider, "mock_provider");
    assert!(!thread.ephemeral, "stored rollouts should not be ephemeral");
    assert!(thread.path.as_ref().expect("thread path").is_absolute());
    assert_eq!(thread.cwd, test_absolute_path("/"));
    assert_eq!(thread.cli_version, "0.0.0");
    assert_eq!(thread.source, SessionSource::Cli);
    assert_eq!(thread.git_info, None);
    assert_eq!(thread.turns.len(), 0);
    assert_eq!(thread.status, ThreadStatus::NotLoaded);

    Ok(())
}

#[tokio::test]
async fn thread_read_can_include_turns() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let preview = "Saved user message";
    let text_elements = vec![TextElement::new(
        ByteRange { start: 0, end: 5 },
        Some("<note>".into()),
    )];
    let conversation_id = create_fake_rollout_with_text_elements(
        codex_home.path(),
        "2025-01-05T12-00-00",
        "2025-01-05T12:00:00Z",
        preview,
        text_elements
            .iter()
            .map(|elem| serde_json::to_value(elem).expect("serialize text element"))
            .collect(),
        Some("mock_provider"),
        /*git_info*/ None,
    )?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;

    let read_id = mcp
        .send_thread_read_request(ThreadReadParams {
            thread_id: conversation_id.clone(),
            include_turns: true,
        })
        .await?;
    let ThreadReadResponse { thread, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(read_id)).await??;

    assert_eq!(thread.turns.len(), 1);
    let turn = &thread.turns[0];
    assert_eq!(turn.status, TurnStatus::Completed);
    assert_eq!(turn.items_view, TurnItemsView::Full);
    assert_eq!(turn.items.len(), 1, "expected user message item");
    match &turn.items[0] {
        ThreadItem::UserMessage { content, .. } => {
            assert_eq!(
                content,
                &vec![UserInput::Text {
                    text: preview.to_string(),
                    text_elements: text_elements.clone().into_iter().map(Into::into).collect(),
                }]
            );
        }
        other => panic!("expected user message item, got {other:?}"),
    }
    assert_eq!(thread.status, ThreadStatus::NotLoaded);
    assert!(
        !mcp.pending_notification_methods()
            .contains(&"deprecationNotice".to_string())
    );

    Ok(())
}

#[tokio::test]
async fn paginated_stored_thread_reads_unprojected_turns_through_read_apis() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let conversation_id = create_fake_paginated_rollout(
        codex_home.path(),
        "2025-01-05T12-00-00",
        "2025-01-05T12:00:00Z",
        "Saved user message",
        Some("mock_provider"),
        /*git_info*/ None,
    )?;

    app_test_support::append_fake_paginated_user_message(
        &rollout_path(codex_home.path(), "2025-01-05T12-00-00", &conversation_id),
        &conversation_id,
        "Saved user message",
    )
    .await?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;

    let read_id = mcp
        .send_thread_read_request(ThreadReadParams {
            thread_id: conversation_id.clone(),
            include_turns: false,
        })
        .await?;
    let ThreadReadResponse { thread } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(read_id)).await??;
    assert_eq!(thread.history_mode, ThreadHistoryMode::Paginated);
    assert!(thread.turns.is_empty());
    assert!(
        !mcp.pending_notification_methods()
            .contains(&"deprecationNotice".to_string())
    );

    let full_read_id = mcp
        .send_thread_read_request(ThreadReadParams {
            thread_id: conversation_id.clone(),
            include_turns: true,
        })
        .await?;
    let notice: DeprecationNoticeNotification = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_notification("deprecationNotice"),
    )
    .await??;
    assert_eq!(
        notice,
        DeprecationNoticeNotification {
            summary: "Full-history hydration is deprecated for paginated threads; omit `includeTurns` or set it to `false`, then page with `thread/turns/list` and `thread/items/list`.".to_string(),
            details: None,
        }
    );
    let _: ThreadReadResponse =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(full_read_id)).await??;

    let list_id = mcp
        .send_thread_list_request(ThreadListParams {
            cursor: None,
            limit: Some(50),
            sort_key: None,
            sort_direction: None,
            model_providers: Some(vec!["mock_provider".to_string()]),
            source_kinds: None,
            archived: None,
            section_id: None,
            project_id: None,
            cwd: None,
            use_state_db_only: false,
            search_term: None,
            parent_thread_id: None,
            ancestor_thread_id: None,
        })
        .await?;
    let ThreadListResponse { data, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(list_id)).await??;
    let listed = data
        .iter()
        .find(|thread| thread.id == conversation_id)
        .expect("thread/list should include paginated thread");
    assert_eq!(listed.history_mode, ThreadHistoryMode::Paginated);

    let read_id = mcp
        .send_thread_read_request(ThreadReadParams {
            thread_id: conversation_id.clone(),
            include_turns: true,
        })
        .await?;
    let ThreadReadResponse { thread } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(read_id)).await??;
    assert_eq!(thread.history_mode, ThreadHistoryMode::Paginated);
    assert_eq!(turn_user_texts(&thread.turns), vec!["Saved user message"]);

    let turns_list_id = mcp
        .send_thread_turns_list_request(ThreadTurnsListParams {
            thread_id: conversation_id.clone(),
            cursor: None,
            limit: None,
            sort_direction: None,
            items_view: None,
        })
        .await?;
    let turns_list_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(turns_list_id)),
    )
    .await??;
    let turns = to_response::<ThreadTurnsListResponse>(turns_list_resp)?;
    assert_eq!(turn_user_texts(&turns.data), vec!["Saved user message"]);
    assert_eq!(turns.next_cursor, None);

    Ok(())
}

#[tokio::test]
async fn paginated_segmented_history_without_index_returns_latest_five_turns() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
    let thread_id = codex_protocol::ThreadId::new();
    let sqlite = codex_state::SqliteConfig::new_for_testing(codex_home.path().abs());
    let state_db =
        codex_state::StateRuntime::init(sqlite.clone(), "mock_provider".to_string()).await?;
    let history_db_path = sqlite.thread_history_db_path();
    let store = LocalThreadStore::new(
        LocalThreadStoreConfig {
            codex_home: codex_home.path().to_path_buf(),
            sqlite: sqlite.clone(),
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

    for index in 0..8 {
        let turn_id = format!("turn-{index}");
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![
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
                    paginated_completed_item(
                        thread_id,
                        &turn_id,
                        CoreTurnItem::AgentMessage(AgentMessageItem {
                            id: format!("agent-{index}"),
                            content: vec![AgentMessageContent::Text {
                                text: format!("answer {index}"),
                            }],
                            phase: None,
                            delivery: None,
                            memory_citation: None,
                        }),
                    ),
                    paginated_turn_completed(&turn_id),
                ],
            })
            .await?;
        if index < 7 {
            store
                .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
                .await?;
        }
    }
    store.shutdown_thread(thread_id).await?;
    drop(store);

    let history_db_name = history_db_path
        .file_name()
        .expect("thread history database filename")
        .to_string_lossy();
    for path in [
        history_db_path.clone(),
        history_db_path.with_file_name(format!("{history_db_name}-wal")),
        history_db_path.with_file_name(format!("{history_db_name}-shm")),
    ] {
        if path.exists() {
            std::fs::remove_file(path)?;
        }
    }
    assert!(!history_db_path.exists());

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let first_page = read_turns_page(
        &mut mcp,
        thread_id,
        /*cursor*/ None,
        Some(5),
        SortDirection::Desc,
        Some(TurnItemsView::Full),
    )
    .await?;
    assert_eq!(
        first_page
            .data
            .iter()
            .map(|turn| turn.id.as_str())
            .collect::<Vec<_>>(),
        vec!["turn-7", "turn-6", "turn-5", "turn-4", "turn-3"]
    );
    assert_eq!(
        turn_user_texts(&first_page.data),
        vec!["user 7", "user 6", "user 5", "user 4", "user 3"]
    );
    assert_eq!(
        turn_agent_texts(&first_page.data),
        vec!["answer 7", "answer 6", "answer 5", "answer 4", "answer 3"]
    );

    let first_items_page = read_items_page(
        &mut mcp,
        thread_id,
        Some("turn-7"),
        /*cursor*/ None,
        Some(1),
        SortDirection::Asc,
    )
    .await?;
    assert_eq!(first_items_page.data.len(), 1);
    assert_eq!(first_items_page.data[0].turn_id, "turn-7");
    assert_eq!(first_items_page.data[0].item.id(), "user-7");
    let next_item_cursor = first_items_page
        .next_cursor
        .expect("user item should have an assistant item after it");

    let second_items_page = read_items_page(
        &mut mcp,
        thread_id,
        Some("turn-7"),
        Some(next_item_cursor),
        Some(1),
        SortDirection::Asc,
    )
    .await?;
    assert_eq!(second_items_page.data.len(), 1);
    assert_eq!(second_items_page.data[0].turn_id, "turn-7");
    assert_eq!(second_items_page.data[0].item.id(), "agent-7");
    assert!(second_items_page.next_cursor.is_none());

    let latest_items_page = read_items_page(
        &mut mcp,
        thread_id,
        /*turn_id*/ None,
        /*cursor*/ None,
        Some(8),
        SortDirection::Desc,
    )
    .await?;
    assert_eq!(
        latest_items_page
            .data
            .iter()
            .map(|entry| (entry.turn_id.as_str(), entry.item.id()))
            .collect::<Vec<_>>(),
        vec![
            ("turn-7", "agent-7"),
            ("turn-7", "user-7"),
            ("turn-6", "agent-6"),
            ("turn-6", "user-6"),
            ("turn-5", "agent-5"),
            ("turn-5", "user-5"),
            ("turn-4", "agent-4"),
            ("turn-4", "user-4"),
        ]
    );
    assert!(latest_items_page.next_cursor.is_some());

    let second_page = read_turns_page(
        &mut mcp,
        thread_id,
        first_page.next_cursor.clone(),
        Some(5),
        SortDirection::Desc,
        Some(TurnItemsView::Full),
    )
    .await?;
    assert_eq!(
        second_page
            .data
            .iter()
            .map(|turn| turn.id.as_str())
            .collect::<Vec<_>>(),
        vec!["turn-2", "turn-1", "turn-0"]
    );
    assert!(second_page.next_cursor.is_none());

    let resume_id = mcp
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            exclude_turns: true,
            initial_turns_page: Some(ThreadResumeInitialTurnsPageParams {
                limit: Some(5),
                sort_direction: Some(SortDirection::Desc),
                items_view: Some(TurnItemsView::Full),
            }),
            ..Default::default()
        })
        .await?;
    let ThreadResumeResponse {
        thread,
        initial_turns_page,
        turns_backwards_cursor,
        items_backwards_cursor,
        ..
    } = timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(resume_id)).await??;
    assert!(thread.turns.is_empty());
    let resumed_page = initial_turns_page.expect("resume should return the latest five turns");
    assert_eq!(resumed_page.data, first_page.data);
    let turns_backwards_cursor = turns_backwards_cursor.expect("unprojected turn head cursor");
    let items_backwards_cursor = items_backwards_cursor.expect("unprojected item head cursor");
    let resumed_head_turns = read_turns_page(
        &mut mcp,
        thread_id,
        Some(turns_backwards_cursor),
        Some(1),
        SortDirection::Desc,
        Some(TurnItemsView::NotLoaded),
    )
    .await?;
    assert_eq!(resumed_head_turns.data[0].id, "turn-7");
    let resumed_head_items = read_items_page(
        &mut mcp,
        thread_id,
        /*turn_id*/ None,
        Some(items_backwards_cursor),
        Some(1),
        SortDirection::Desc,
    )
    .await?;
    assert_eq!(resumed_head_items.data[0].turn_id, "turn-7");
    assert_eq!(resumed_head_items.data[0].item.id(), "agent-7");

    let post_resume_page = read_turns_page(
        &mut mcp,
        thread_id,
        /*cursor*/ None,
        Some(5),
        SortDirection::Desc,
        Some(TurnItemsView::Full),
    )
    .await?;
    assert_eq!(post_resume_page.data, first_page.data);
    assert_eq!(post_resume_page.next_cursor, first_page.next_cursor);

    let projection_state =
        codex_state::StateRuntime::init(sqlite.clone(), "mock_provider".to_string()).await?;
    let projection_inspector = LocalThreadStore::new(
        LocalThreadStoreConfig {
            codex_home: codex_home.path().to_path_buf(),
            sqlite,
            default_model_provider_id: "mock_provider".to_string(),
        },
        Some(projection_state),
    );
    timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            if projection_inspector
                .has_history_projection(thread_id)
                .await
                .expect("inspect rebuilt projection")
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await?;
    drop(projection_inspector);

    let saved_fallback_cursor = first_page
        .next_cursor
        .clone()
        .expect("fallback first page should have an older cursor");
    drop(mcp);
    let mut restarted = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let continued_after_restart = read_turns_page(
        &mut restarted,
        thread_id,
        Some(saved_fallback_cursor),
        Some(5),
        SortDirection::Desc,
        Some(TurnItemsView::Full),
    )
    .await?;
    assert_eq!(
        continued_after_restart
            .data
            .iter()
            .map(|turn| turn.id.as_str())
            .collect::<Vec<_>>(),
        vec!["turn-2", "turn-1", "turn-0"]
    );
    drop(restarted);
    let mut indexed_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let indexed_after_restart = read_turns_page(
        &mut indexed_server,
        thread_id,
        /*cursor*/ None,
        Some(5),
        SortDirection::Desc,
        Some(TurnItemsView::Full),
    )
    .await?;
    assert_eq!(indexed_after_restart.data, first_page.data);
    assert!(
        indexed_after_restart
            .next_cursor
            .as_deref()
            .is_some_and(|cursor| cursor.contains("rolloutOrdinal")),
        "a fresh process should use the atomically published SQLite projection"
    );

    Ok(())
}

#[tokio::test]
async fn paginated_resume_without_index_does_not_open_obsolete_predecessor() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
    let thread_id = codex_protocol::ThreadId::new();
    let sqlite = codex_state::SqliteConfig::new_for_testing(codex_home.path().abs());
    let state_db =
        codex_state::StateRuntime::init(sqlite.clone(), "mock_provider".to_string()).await?;
    let history_db_path = sqlite.thread_history_db_path();
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

    let mut oldest_segment = None;
    for index in 0..8 {
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![RolloutItem::EventMsg(EventMsg::AgentMessage(
                    AgentMessageEvent {
                        message: format!("obsolete {index}"),
                        phase: None,
                        delivery: None,
                        memory_citation: None,
                    },
                ))],
            })
            .await?;
        let frozen = store
            .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
            .await?;
        oldest_segment.get_or_insert(frozen.reference.rollout_path);
    }
    let mut latest = certified_recent_history_checkpoint(codex_home.path()).into_items();
    latest.extend([
        paginated_turn_started("latest-turn"),
        paginated_completed_item(
            thread_id,
            "latest-turn",
            CoreTurnItem::UserMessage(UserMessageItem {
                id: "latest-user".to_string(),
                client_id: None,
                content: vec![codex_protocol::user_input::UserInput::Text {
                    text: "latest user".to_string(),
                    text_elements: Vec::new(),
                }],
            }),
        ),
        paginated_completed_item(
            thread_id,
            "latest-turn",
            CoreTurnItem::AgentMessage(AgentMessageItem {
                id: "latest-agent".to_string(),
                content: vec![AgentMessageContent::Text {
                    text: "latest answer".to_string(),
                }],
                phase: None,
                delivery: None,
                memory_citation: None,
            }),
        ),
        paginated_turn_completed("latest-turn"),
    ]);
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: latest,
        })
        .await?;
    store.shutdown_thread(thread_id).await?;
    drop(store);

    let history_db_name = history_db_path
        .file_name()
        .expect("thread history database filename")
        .to_string_lossy();
    for path in [
        history_db_path.clone(),
        history_db_path.with_file_name(format!("{history_db_name}-wal")),
        history_db_path.with_file_name(format!("{history_db_name}-shm")),
    ] {
        if path.exists() {
            std::fs::remove_file(path)?;
        }
    }
    let oldest_segment = oldest_segment.expect("oldest predecessor");
    let unavailable = oldest_segment.with_extension("jsonl.unavailable");
    std::fs::rename(oldest_segment.as_path(), unavailable.as_path())?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let resume_id = mcp
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            exclude_turns: true,
            initial_turns_page: Some(ThreadResumeInitialTurnsPageParams {
                limit: Some(1),
                sort_direction: Some(SortDirection::Desc),
                items_view: Some(TurnItemsView::Full),
            }),
            ..Default::default()
        })
        .await?;
    let ThreadResumeResponse {
        thread,
        initial_turns_page,
        turns_backwards_cursor,
        items_backwards_cursor,
        ..
    } = timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(resume_id)).await??;
    assert!(thread.turns.is_empty());
    let page = initial_turns_page.expect("bounded initial page");
    assert_eq!(page.data.len(), 1);
    assert_eq!(page.data[0].id, "latest-turn");
    assert!(turns_backwards_cursor.is_some());
    assert!(items_backwards_cursor.is_some());

    std::fs::rename(unavailable, oldest_segment)?;
    Ok(())
}

#[tokio::test]
async fn thread_turns_list_can_page_backward_and_forward() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let filename_ts = "2025-01-05T12-00-00";
    let conversation_id = create_fake_rollout_with_text_elements(
        codex_home.path(),
        filename_ts,
        "2025-01-05T12:00:00Z",
        "first",
        vec![],
        Some("mock_provider"),
        /*git_info*/ None,
    )?;
    let rollout_path = rollout_path(codex_home.path(), filename_ts, &conversation_id);
    append_user_message(rollout_path.as_path(), "2025-01-05T12:01:00Z", "second")?;
    append_user_message(rollout_path.as_path(), "2025-01-05T12:02:00Z", "third")?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;

    let read_id = mcp
        .send_thread_turns_list_request(ThreadTurnsListParams {
            thread_id: conversation_id.clone(),
            cursor: None,
            limit: Some(2),
            sort_direction: Some(SortDirection::Desc),
            items_view: None,
        })
        .await?;
    let ThreadTurnsListResponse {
        data,
        next_cursor,
        backwards_cursor,
    } = timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(read_id)).await??;
    assert_eq!(turn_user_texts(&data), vec!["third", "second"]);
    assert!(
        data.iter()
            .all(|turn| turn.items_view == TurnItemsView::Summary)
    );
    let next_cursor = next_cursor.expect("expected nextCursor for older turns");
    let backwards_cursor = backwards_cursor.expect("expected backwardsCursor for newest turn");

    let read_id = mcp
        .send_thread_turns_list_request(ThreadTurnsListParams {
            thread_id: conversation_id.clone(),
            cursor: Some(next_cursor),
            limit: Some(10),
            sort_direction: Some(SortDirection::Desc),
            items_view: None,
        })
        .await?;
    let ThreadTurnsListResponse { data, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(read_id)).await??;
    assert_eq!(turn_user_texts(&data), vec!["first"]);

    append_user_message(rollout_path.as_path(), "2025-01-05T12:03:00Z", "fourth")?;

    let read_id = mcp
        .send_thread_turns_list_request(ThreadTurnsListParams {
            thread_id: conversation_id.clone(),
            cursor: Some(backwards_cursor),
            limit: Some(10),
            sort_direction: Some(SortDirection::Asc),
            items_view: None,
        })
        .await?;
    let ThreadTurnsListResponse { data, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(read_id)).await??;
    assert_eq!(turn_user_texts(&data), vec!["third", "fourth"]);

    Ok(())
}

#[tokio::test]
async fn thread_turns_list_pages_complete_turns_across_rollout_segments() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
    let thread_id = codex_protocol::ThreadId::new();
    let sqlite = codex_state::SqliteConfig::new_for_testing(codex_home.path().abs());
    let store = LocalThreadStore::new(
        LocalThreadStoreConfig {
            codex_home: codex_home.path().to_path_buf(),
            sqlite,
            default_model_provider_id: "mock_provider".to_string(),
        },
        /*state_db*/ None,
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
            history_mode: codex_protocol::protocol::ThreadHistoryMode::Legacy,
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
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![RolloutItem::EventMsg(EventMsg::AgentMessage(
                AgentMessageEvent {
                    message: "must not be read".to_string(),
                    phase: None,
                    delivery: None,
                    memory_citation: None,
                },
            ))],
        })
        .await?;
    let malformed_segment = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await?
        .reference
        .rollout_path;
    for _ in 0..2 {
        store
            .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
            .await?;
    }
    let mut recent_items = certified_recent_history_checkpoint(codex_home.path()).into_items();
    recent_items.extend([
        paginated_turn_started("previous-turn"),
        RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
            message: "previous user".to_string(),
            ..Default::default()
        })),
        RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
            message: "previous answer".to_string(),
            phase: None,
            delivery: None,
            memory_citation: None,
        })),
        paginated_turn_completed("previous-turn"),
        paginated_turn_started("latest-turn"),
        RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
            message: "latest user".to_string(),
            ..Default::default()
        })),
    ]);
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: recent_items,
        })
        .await?;
    // Keep the split turn inside the shipped five-segment fallback window. A projection may span
    // arbitrary history, but an unprojected normal read must not open a sixth predecessor.
    for _ in 0..3 {
        store
            .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
            .await?;
    }
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
                    message: "latest answer".to_string(),
                    phase: None,
                    delivery: None,
                    memory_citation: None,
                })),
                paginated_turn_completed("latest-turn"),
            ],
        })
        .await?;
    store.shutdown_thread(thread_id).await?;
    writeln!(
        OpenOptions::new().append(true).open(malformed_segment)?,
        "{{malformed rollout line"
    )?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build()
        .await?;
    timeout(DEFAULT_READ_TIMEOUT, mcp.initialize()).await??;

    let ThreadTurnsListResponse {
        data,
        next_cursor,
        backwards_cursor,
    } = read_turns_page(
        &mut mcp,
        thread_id,
        /*cursor*/ None,
        Some(1),
        SortDirection::Desc,
        Some(TurnItemsView::Summary),
    )
    .await?;
    assert_eq!(
        data.iter().map(|turn| turn.id.as_str()).collect::<Vec<_>>(),
        vec!["latest-turn"]
    );
    assert_eq!(turn_user_texts(&data), vec!["latest user"]);
    assert_eq!(turn_agent_texts(&data), vec!["latest answer"]);
    let next_cursor = next_cursor.expect("older referenced history should have a cursor");
    let backwards_cursor =
        backwards_cursor.expect("the latest referenced turn should have a backwards cursor");

    let ThreadTurnsListResponse { data, .. } = read_turns_page(
        &mut mcp,
        thread_id,
        Some(backwards_cursor),
        Some(1),
        SortDirection::Asc,
        Some(TurnItemsView::Summary),
    )
    .await?;
    assert_eq!(
        data.iter().map(|turn| turn.id.as_str()).collect::<Vec<_>>(),
        vec!["latest-turn"]
    );

    let ThreadTurnsListResponse { data, .. } = read_turns_page(
        &mut mcp,
        thread_id,
        Some(next_cursor),
        Some(1),
        SortDirection::Desc,
        Some(TurnItemsView::Summary),
    )
    .await?;
    assert_eq!(
        data.iter().map(|turn| turn.id.as_str()).collect::<Vec<_>>(),
        vec!["previous-turn"]
    );
    assert_eq!(turn_user_texts(&data), vec!["previous user"]);
    assert_eq!(turn_agent_texts(&data), vec!["previous answer"]);

    let fork_id = mcp
        .send_thread_fork_request(ThreadForkParams {
            thread_id: thread_id.to_string(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let fork_response: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(fork_id)),
    )
    .await??;
    let ThreadForkResponse { thread: fork, .. } = to_response::<ThreadForkResponse>(fork_response)?;
    assert!(fork.turns.is_empty());
    let fork_path = fork.path.as_ref().expect("fork should have a rollout path");
    let fork_physical_items = RolloutRecorder::load_rollout_items(fork_path.as_path())
        .await?
        .0;
    assert!(matches!(
        fork_physical_items.as_slice(),
        [
            RolloutItem::SessionMeta(_),
            RolloutItem::RolloutReference(_),
            RolloutItem::Compacted(CompactedItem {
                segment_state_checkpoint: Some(_),
                ..
            }),
            RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(_)),
            RolloutItem::EventMsg(EventMsg::TokenCount(_))
        ]
    ));
    assert!(
        !std::fs::read_to_string(fork_path.as_path())?.contains("latest user"),
        "forked rollout must not copy inherited source messages"
    );
    let fork_thread_id = codex_protocol::ThreadId::from_string(&fork.id)?;
    let ThreadTurnsListResponse {
        data, next_cursor, ..
    } = read_turns_page(
        &mut mcp,
        fork_thread_id,
        /*cursor*/ None,
        Some(1),
        SortDirection::Desc,
        Some(TurnItemsView::Summary),
    )
    .await?;
    assert_eq!(
        data.iter().map(|turn| turn.id.as_str()).collect::<Vec<_>>(),
        vec!["latest-turn"]
    );
    assert_eq!(turn_user_texts(&data), vec!["latest user"]);
    assert_eq!(turn_agent_texts(&data), vec!["latest answer"]);
    assert!(next_cursor.is_some());

    Ok(())
}

#[tokio::test]
async fn rotated_legacy_fork_turns_list_preserves_inherited_parent_turns() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
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
    let create_params = |thread_id: codex_protocol::ThreadId,
                         forked_from_id: Option<codex_protocol::ThreadId>| {
        CreateThreadParams {
            session_id: thread_id.into(),
            thread_id,
            extra_config: None,
            forked_from_id,
            forked_from_ordinal_exclusive: None,
            parent_thread_id: None,
            source: ProtocolSessionSource::Cli,
            thread_source: None,
            originator: "test_originator".to_string(),
            base_instructions: BaseInstructions::default(),
            dynamic_tools: Vec::new(),
            selected_capability_roots: Vec::new(),
            multi_agent_version: None,
            history_mode: codex_protocol::protocol::ThreadHistoryMode::Legacy,
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
        }
    };

    let parent_id = codex_protocol::ThreadId::new();
    store.create_thread(create_params(parent_id, None)).await?;
    store
        .persist_thread(parent_id, PersistContext::Standard)
        .await?;
    store
        .append_items(AppendThreadItemsParams {
            thread_id: parent_id,
            items: vec![
                paginated_turn_started("inherited-parent-turn"),
                RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
                    message: "inherited parent user".to_string(),
                    ..Default::default()
                })),
                RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
                    message: "inherited parent answer".to_string(),
                    phase: None,
                    delivery: None,
                    memory_citation: None,
                })),
                paginated_turn_completed("inherited-parent-turn"),
            ],
        })
        .await?;
    let inherited_parent = store
        .freeze_thread_segment(parent_id, FreezeRolloutSegmentParams::snapshot())
        .await?
        .reference;

    let mut child_ids = Vec::new();
    for forked_from_id in [Some(parent_id), None] {
        let child_id = codex_protocol::ThreadId::new();
        store
            .create_thread(create_params(child_id, forked_from_id))
            .await?;
        store
            .persist_thread(child_id, PersistContext::Standard)
            .await?;
        store
            .append_items(AppendThreadItemsParams {
                thread_id: child_id,
                items: vec![RolloutItem::RolloutReference(inherited_parent.clone())],
            })
            .await?;
        store
            .freeze_thread_segment(child_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
            .await?;
        store.shutdown_thread(child_id).await?;
        child_ids.push(child_id);
    }
    store.shutdown_thread(parent_id).await?;

    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    for child_id in child_ids {
        let ThreadTurnsListResponse { data, .. } = read_turns_page(
            &mut app_server,
            child_id,
            /*cursor*/ None,
            Some(1),
            SortDirection::Desc,
            Some(TurnItemsView::Full),
        )
        .await?;

        assert_eq!(
            data.iter().map(|turn| turn.id.as_str()).collect::<Vec<_>>(),
            vec!["inherited-parent-turn"],
            "an empty child-local projection must not hide an inherited parent turn after rotation"
        );
        assert_eq!(turn_user_texts(&data), vec!["inherited parent user"]);
        assert_eq!(turn_agent_texts(&data), vec!["inherited parent answer"]);
    }

    Ok(())
}

#[tokio::test]
async fn frodex_running_legacy_resume_returns_newest_five_segments_only() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
    let thread_id = codex_protocol::ThreadId::new();
    let sqlite = codex_state::SqliteConfig::new_for_testing(codex_home.path().abs());
    let store = LocalThreadStore::new(
        LocalThreadStoreConfig {
            codex_home: codex_home.path().to_path_buf(),
            sqlite,
            default_model_provider_id: "mock_provider".to_string(),
        },
        /*state_db*/ None,
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
            history_mode: codex_protocol::protocol::ThreadHistoryMode::Legacy,
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
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![RolloutItem::EventMsg(EventMsg::AgentMessage(
                AgentMessageEvent {
                    message: "must not be read".to_string(),
                    phase: None,
                    delivery: None,
                    memory_citation: None,
                },
            ))],
        })
        .await?;
    let malformed_segment = store
        .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
        .await?
        .reference
        .rollout_path;

    for index in 0..8 {
        let turn_id = format!("turn-{index}");
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![
                    paginated_turn_started(&turn_id),
                    RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
                        message: format!("user {index}"),
                        ..Default::default()
                    })),
                    RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
                        message: format!("answer {index}"),
                        phase: None,
                        delivery: None,
                        memory_citation: None,
                    })),
                    paginated_turn_completed(&turn_id),
                ],
            })
            .await?;
        if index < 7 {
            store
                .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
                .await?;
        }
    }
    store.shutdown_thread(thread_id).await?;
    writeln!(
        OpenOptions::new().append(true).open(malformed_segment)?,
        "{{malformed rollout line"
    )?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let initial_resume = mcp
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let _: ThreadResumeResponse =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(initial_resume)).await??;

    let expected_page = read_turns_page(
        &mut mcp,
        thread_id,
        /*cursor*/ None,
        Some(5),
        SortDirection::Desc,
        Some(TurnItemsView::Full),
    )
    .await?;
    assert_eq!(
        expected_page
            .data
            .iter()
            .map(|turn| turn.id.as_str())
            .collect::<Vec<_>>(),
        vec!["turn-7", "turn-6", "turn-5", "turn-4", "turn-3"]
    );
    let newest_turn = &expected_page.data[0];
    let newest_items = read_items_page(
        &mut mcp,
        thread_id,
        Some(newest_turn.id.as_str()),
        /*cursor*/ None,
        Some(20),
        SortDirection::Asc,
    )
    .await?;
    assert_eq!(
        newest_items.data,
        newest_turn
            .items
            .iter()
            .cloned()
            .map(|item| ThreadItemEntry {
                turn_id: newest_turn.id.clone(),
                item,
            })
            .collect::<Vec<_>>(),
        "thread/items/list must preserve the IDs already returned for a bounded Legacy turn"
    );
    // The initial Desktop request remains bounded to five segments. An explicit cursor may page
    // farther back, and corruption outside the initial window is reported only when the client
    // reaches it.
    let older_cursor = expected_page
        .next_cursor
        .clone()
        .expect("bounded latest history advertises explicit older paging");
    let older_page = read_turns_page(
        &mut mcp,
        thread_id,
        Some(older_cursor),
        Some(5),
        SortDirection::Desc,
        Some(TurnItemsView::Full),
    )
    .await?;
    assert_eq!(
        older_page
            .data
            .iter()
            .map(|turn| turn.id.as_str())
            .collect::<Vec<_>>(),
        vec!["turn-2", "turn-1", "turn-0"]
    );
    let corrupt_cursor = older_page
        .next_cursor
        .expect("the malformed predecessor remains beyond the valid older page");
    let corrupt_request_id = mcp
        .send_thread_turns_list_request(ThreadTurnsListParams {
            thread_id: thread_id.to_string(),
            cursor: Some(corrupt_cursor),
            limit: Some(5),
            sort_direction: Some(SortDirection::Desc),
            items_view: Some(TurnItemsView::Full),
        })
        .await?;
    let corrupt_error: JSONRPCError = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_error_message(RequestId::Integer(corrupt_request_id)),
    )
    .await??;
    assert!(
        corrupt_error.error.message.contains("invalid record")
            || corrupt_error
                .error
                .message
                .contains("anchor turn is no longer present"),
        "the cursor beyond the last valid page must fail explicitly: {corrupt_error:?}"
    );

    let resume_id = mcp
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            exclude_turns: true,
            initial_turns_page: Some(ThreadResumeInitialTurnsPageParams {
                limit: Some(5),
                sort_direction: Some(SortDirection::Desc),
                items_view: Some(TurnItemsView::Full),
            }),
            ..Default::default()
        })
        .await?;
    let ThreadResumeResponse {
        thread,
        initial_turns_page,
        ..
    } = timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(resume_id)).await??;

    assert!(thread.turns.is_empty());
    assert_eq!(
        initial_turns_page,
        Some(codex_app_server_protocol::TurnsPage::from(expected_page))
    );
    Ok(())
}

#[tokio::test]
async fn frodex_thread_turns_list_stays_bounded_with_258_segments() -> Result<()> {
    assert_thread_turns_list_same_thread_segment_limit(
        codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH + 2,
        /*expected_error*/ None,
    )
    .await
}

#[tokio::test]
async fn frodex_thread_turns_list_stays_bounded_with_512_segments() -> Result<()> {
    assert_thread_turns_list_same_thread_segment_limit(
        /*segment_count*/ 512, /*expected_error*/ None,
    )
    .await
}

#[tokio::test]
async fn frodex_thread_turns_list_stays_bounded_with_513_segments() -> Result<()> {
    assert_thread_turns_list_same_thread_segment_limit(
        /*segment_count*/ 513, /*expected_error*/ None,
    )
    .await
}

#[tokio::test]
async fn frodex_thread_turns_list_stays_bounded_with_1025_segments() -> Result<()> {
    assert_thread_turns_list_same_thread_segment_limit(
        /*segment_count*/ 1025, /*expected_error*/ None,
    )
    .await
}

#[tokio::test]
async fn frodex_thread_turns_list_stays_bounded_with_4097_segments() -> Result<()> {
    assert_thread_turns_list_same_thread_segment_limit(
        /*segment_count*/ 4097, /*expected_error*/ None,
    )
    .await
}

#[tokio::test]
#[ignore = "manual five-figure same-thread segment scaling validation"]
async fn frodex_thread_turns_list_stays_bounded_with_10001_segments() -> Result<()> {
    assert_thread_turns_list_same_thread_segment_limit(
        /*segment_count*/ 10001, /*expected_error*/ None,
    )
    .await
}

async fn assert_thread_turns_list_same_thread_segment_limit(
    segment_count: usize,
    expected_error: Option<&str>,
) -> Result<()> {
    let read_timeout = if segment_count >= 4_096 {
        DEFAULT_READ_TIMEOUT.saturating_mul(12)
    } else {
        DEFAULT_READ_TIMEOUT
    };
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
    let thread_id = codex_protocol::ThreadId::new();
    let segment_ids = (0..segment_count)
        .map(|_| SegmentId::new())
        .collect::<Vec<_>>();
    let active_path = rollout_path(
        codex_home.path(),
        "2026-08-03T00-00-00",
        thread_id.to_string().as_str(),
    );
    let paths = segment_ids
        .iter()
        .enumerate()
        .map(|(index, segment_id)| {
            if index + 1 == segment_count {
                active_path.clone()
            } else {
                codex_home
                    .path()
                    .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
                    .join(thread_id.to_string())
                    .join(segment_id.to_string())
                    .join(active_path.file_name().expect("active rollout filename"))
            }
        })
        .collect::<Vec<_>>();

    for index in 0..segment_count {
        let mut items = vec![RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                session_id: thread_id.into(),
                id: thread_id,
                segment_id: Some(segment_ids[index]),
                timestamp: "2026-08-03T00:00:00Z".to_string(),
                cwd: codex_home.path().to_path_buf(),
                originator: "same-thread-reference-test".to_string(),
                cli_version: "0.147.0-alpha.4".to_string(),
                source: ProtocolSessionSource::Cli,
                model_provider: Some("mock_provider".to_string()),
                history_mode: codex_protocol::protocol::ThreadHistoryMode::Legacy,
                ..SessionMeta::default()
            },
            git: None,
        })];
        if let Some(previous_index) = index.checked_sub(1) {
            items.push(RolloutItem::RolloutReference(RolloutReferenceItem {
                rollout_path: paths[previous_index].clone(),
                thread_id: Some(thread_id),
                rollout_id: Some(thread_id),
                rollout_timestamp: None,
                segment_id: Some(segment_ids[previous_index]),
                max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
                nth_user_message: None,
                compacted_replacement_history_filter_texts: None,
            }));
        }
        if index + 1 == paths.len() {
            items.extend(certified_recent_history_checkpoint(codex_home.path()).into_items());
        }
        let turn_id = format!("turn-{index}");
        items.extend([
            paginated_turn_started(turn_id.as_str()),
            RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
                message: format!("user {index}"),
                ..Default::default()
            })),
            RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
                message: format!("answer {index}"),
                phase: None,
                delivery: None,
                memory_citation: None,
            })),
            paginated_turn_completed(turn_id.as_str()),
        ]);

        let base_ordinal = u64::try_from(index)? * 8;
        let records = items
            .into_iter()
            .enumerate()
            .map(|(item_index, item)| {
                serde_json::to_string(&RolloutLine {
                    timestamp: "2026-08-03T00:00:00Z".to_string(),
                    ordinal: Some(base_ordinal + u64::try_from(item_index).expect("item ordinal")),
                    item,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(parent) = paths[index].parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(paths[index].as_path(), format!("{}\n", records.join("\n")))?;
    }

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let request_id = mcp
        .send_thread_turns_list_request(ThreadTurnsListParams {
            thread_id: thread_id.to_string(),
            cursor: None,
            limit: Some(1),
            sort_direction: Some(SortDirection::Desc),
            items_view: Some(TurnItemsView::Summary),
        })
        .await?;
    if let Some(expected_error) = expected_error {
        let error: JSONRPCError = timeout(
            read_timeout,
            mcp.read_stream_until_error_message(RequestId::Integer(request_id)),
        )
        .await??;
        assert!(
            error.error.message.contains(expected_error),
            "unexpected error: {}",
            error.error.message
        );
        return Ok(());
    }

    let ThreadTurnsListResponse { data, .. } =
        timeout(read_timeout, mcp.read_response(request_id)).await??;
    let newest_index = segment_count - 1;
    assert_eq!(
        data.iter().map(|turn| turn.id.as_str()).collect::<Vec<_>>(),
        vec![format!("turn-{newest_index}")]
    );
    assert_eq!(turn_user_texts(&data), vec![format!("user {newest_index}")]);
    assert_eq!(
        turn_agent_texts(&data),
        vec![format!("answer {newest_index}")]
    );
    Ok(())
}

// LOAD BEARING: normal Frodex history APIs MUST NOT open the sixth-newest rollout segment.
// Removing this bound made Desktop "Continue in new chat" take tens of seconds on long threads.
#[tokio::test]
async fn frodex_history_apis_ignore_deleted_sixth_segment() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
    let thread_id = codex_protocol::ThreadId::new();
    let segment_ids = (0..codex_rollout::FRODEX_RECENT_ROLLOUT_SEGMENTS + 1)
        .map(|_| SegmentId::new())
        .collect::<Vec<_>>();
    let active_path = rollout_path(
        codex_home.path(),
        "2026-08-11T00-00-00",
        thread_id.to_string().as_str(),
    );
    let paths = segment_ids
        .iter()
        .enumerate()
        .map(|(index, segment_id)| {
            if index + 1 == segment_ids.len() {
                active_path.clone()
            } else {
                codex_home
                    .path()
                    .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
                    .join(thread_id.to_string())
                    .join(segment_id.to_string())
                    .join(active_path.file_name().expect("active rollout filename"))
            }
        })
        .collect::<Vec<_>>();

    for index in 0..paths.len() {
        let mut items = vec![RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                session_id: thread_id.into(),
                id: thread_id,
                segment_id: Some(segment_ids[index]),
                timestamp: "2026-08-11T00:00:00Z".to_string(),
                cwd: codex_home.path().to_path_buf(),
                originator: "frodex-five-segment-test".to_string(),
                cli_version: "test".to_string(),
                source: ProtocolSessionSource::Cli,
                model_provider: Some("mock_provider".to_string()),
                history_mode: codex_protocol::protocol::ThreadHistoryMode::Legacy,
                ..SessionMeta::default()
            },
            git: None,
        })];
        if let Some(previous_index) = index.checked_sub(1) {
            items.push(RolloutItem::RolloutReference(RolloutReferenceItem {
                rollout_path: paths[previous_index].clone(),
                thread_id: Some(thread_id),
                rollout_id: Some(thread_id),
                rollout_timestamp: None,
                segment_id: Some(segment_ids[previous_index]),
                max_depth: codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH,
                nth_user_message: None,
                compacted_replacement_history_filter_texts: None,
            }));
        }
        if index + 1 == paths.len() {
            items.extend(certified_recent_history_checkpoint(codex_home.path()).into_items());
        }
        let turn_id = format!("turn-{index}");
        items.extend([
            paginated_turn_started(turn_id.as_str()),
            RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
                message: format!("user {index}"),
                ..Default::default()
            })),
            RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
                message: format!("answer {index}"),
                phase: None,
                delivery: None,
                memory_citation: None,
            })),
            paginated_turn_completed(turn_id.as_str()),
        ]);
        let records = items
            .into_iter()
            .map(|item| {
                serde_json::to_string(&RolloutLine {
                    timestamp: "2026-08-11T00:00:00Z".to_string(),
                    ordinal: None,
                    item,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(parent) = paths[index].parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(paths[index].as_path(), format!("{}\n", records.join("\n")))?;
    }
    std::fs::remove_file(paths[0].as_path())?;

    let expected_user_texts = (1..=5)
        .map(|index| format!("user {index}"))
        .collect::<Vec<_>>();
    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;

    let read_id = mcp
        .send_thread_read_request(ThreadReadParams {
            thread_id: thread_id.to_string(),
            include_turns: true,
        })
        .await?;
    let ThreadReadResponse { thread } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(read_id)).await??;
    assert_eq!(turn_user_texts(&thread.turns), expected_user_texts);

    let turns_id = mcp
        .send_thread_turns_list_request(ThreadTurnsListParams {
            thread_id: thread_id.to_string(),
            cursor: None,
            limit: Some(100),
            sort_direction: Some(SortDirection::Desc),
            items_view: Some(TurnItemsView::Full),
        })
        .await?;
    let ThreadTurnsListResponse {
        data, next_cursor, ..
    } = timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(turns_id)).await??;
    assert_eq!(
        turn_user_texts(&data),
        expected_user_texts
            .iter()
            .rev()
            .cloned()
            .collect::<Vec<_>>()
    );
    assert_eq!(next_cursor, None);

    let resume_id = mcp
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            ..Default::default()
        })
        .await?;
    let ThreadResumeResponse { thread, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(resume_id)).await??;
    assert_eq!(turn_user_texts(&thread.turns), expected_user_texts);

    let fork_id = mcp
        .send_thread_fork_request(ThreadForkParams {
            thread_id: thread_id.to_string(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let ThreadForkResponse { thread, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(fork_id)).await??;
    assert!(thread.turns.is_empty());

    let complete_fork_id = mcp
        .send_thread_fork_request(ThreadForkParams {
            thread_id: thread_id.to_string(),
            ..Default::default()
        })
        .await?;
    let ThreadForkResponse { thread, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(complete_fork_id)).await??;
    assert_eq!(turn_user_texts(&thread.turns), expected_user_texts);

    let side_id = mcp
        .send_thread_fork_request(ThreadForkParams {
            thread_id: thread_id.to_string(),
            ephemeral: true,
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let ThreadForkResponse { thread, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(side_id)).await??;
    assert!(thread.ephemeral);
    assert!(thread.turns.is_empty());
    Ok(())
}

#[tokio::test]
async fn segmented_legacy_index_preserves_full_items_cursors_and_restart() -> Result<()> {
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
            forked_from_id: Some(codex_protocol::ThreadId::new()),
            forked_from_ordinal_exclusive: None,
            parent_thread_id: None,
            source: ProtocolSessionSource::Cli,
            thread_source: None,
            originator: "test_originator".to_string(),
            base_instructions: BaseInstructions::default(),
            dynamic_tools: Vec::new(),
            selected_capability_roots: Vec::new(),
            multi_agent_version: None,
            history_mode: codex_protocol::protocol::ThreadHistoryMode::Legacy,
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

    let mut oldest_segment_path = None;
    let active_path = store.live_rollout_path(thread_id).await?;
    for (index, text) in ["first", "second", "third", "fourth"].iter().enumerate() {
        let turn_id = format!("{text}-turn");
        let mut items = Vec::new();
        if *text != "second" {
            items.push(paginated_turn_started(&turn_id));
        }
        if *text == "third" {
            items.push(RolloutItem::Compacted(CompactedItem {
                message: "indexed Legacy resume checkpoint".to_string(),
                replacement_history: Some(Vec::new()),
                mcp_resource_origins: None,
                window_number: Some(1),
                first_window_id: None,
                previous_window_id: None,
                window_id: None,
                segment_state_checkpoint: None,
            }));
            items.push(RolloutItem::TurnContext(TurnContextItem {
                turn_id: Some(turn_id.clone()),
                cwd: codex_home.path().abs(),
                workspace_roots: None,
                current_date: None,
                timezone: None,
                approval_policy: AskForApproval::Never,
                approvals_reviewer: None,
                sandbox_policy: SandboxPolicy::new_read_only_policy(),
                permission_profile: None,
                active_permission_profile: None,
                network: None,
                file_system_sandbox_policy: None,
                model: "mock-model".to_string(),
                comp_hash: None,
                cyber_access_program: None,
                personality: None,
                collaboration_mode: None,
                multi_agent_version: None,
                multi_agent_mode: None,
                realtime_active: None,
                effort: None,
                service_tier: None,
                model_profile: None,
                summary: ReasoningSummary::Auto,
            }));
        }
        items.extend([
            RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
                message: (*text).to_string(),
                ..Default::default()
            })),
            RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
                message: format!("{text} answer"),
                phase: None,
                delivery: None,
                memory_citation: None,
            })),
        ]);
        if *text == "fourth" {
            items.push(RolloutItem::InterAgentCommunication(
                InterAgentCommunication::new(
                    AgentPath::try_from("/root/worker").expect("valid worker agent path"),
                    AgentPath::root(),
                    Vec::new(),
                    "worker completed".to_string(),
                    /*trigger_turn*/ false,
                ),
            ));
        }
        if *text != "second" {
            items.push(paginated_turn_completed(&turn_id));
        }
        store
            .append_items(AppendThreadItemsParams { thread_id, items })
            .await?;
        if index < 2 {
            let frozen = store
                .freeze_thread_segment(thread_id, FreezeRolloutSegmentParams::rotate(Vec::new()))
                .await?;
            if index == 0 {
                oldest_segment_path = Some(frozen.reference.rollout_path);
            }
        }
    }
    store.shutdown_thread(thread_id).await?;
    let excluded_historical_event = RolloutLine {
        timestamp: "2026-08-03T00:00:00Z".to_string(),
        ordinal: None,
        item: RolloutItem::EventMsg(EventMsg::CollabAgentSpawnEnd(CollabAgentSpawnEndEvent {
            call_id: "historically-excluded-spawn".to_string(),
            completed_at_ms: 0,
            sender_thread_id: thread_id,
            new_thread_id: Some(codex_protocol::ThreadId::new()),
            new_agent_nickname: None,
            new_agent_role: None,
            prompt: "historical transient event".to_string(),
            model: "mock-model".to_string(),
            reasoning_effort: ReasoningEffort::Medium,
            status: CoreAgentStatus::Completed(None),
        })),
    };
    writeln!(
        OpenOptions::new().append(true).open(&active_path)?,
        "{}",
        serde_json::to_string(&excluded_historical_event)?
    )?;
    store.discard_segmented_legacy_projection(thread_id).await?;
    let indexed_page_params = || StoreListTurnsParams {
        thread_id,
        include_archived: true,
        cursor: None,
        page_size: 2,
        sort_direction: StoreSortDirection::Desc,
        items_view: StoredTurnItemsView::Summary,
    };
    assert!(
        store
            .list_existing_segmented_legacy_turns(indexed_page_params())
            .await?
            .is_none()
    );

    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let initial_resume_id = app_server
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            exclude_turns: true,
            initial_turns_page: Some(ThreadResumeInitialTurnsPageParams {
                limit: Some(1),
                sort_direction: Some(SortDirection::Desc),
                items_view: Some(TurnItemsView::Full),
            }),
            ..Default::default()
        })
        .await?;
    let ThreadResumeResponse {
        thread,
        initial_turns_page,
        ..
    } = timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.read_response(initial_resume_id),
    )
    .await??;
    assert!(thread.turns.is_empty());
    let bounded_page = initial_turns_page.expect("Legacy resume returns the requested latest page");
    assert_eq!(turn_user_texts(&bounded_page.data), vec!["fourth"]);
    assert!(
        store
            .list_existing_segmented_legacy_turns(indexed_page_params())
            .await?
            .is_none(),
        "the latest Legacy page must not synchronously backfill complete history"
    );

    let second_page = read_turns_page(
        &mut app_server,
        thread_id,
        bounded_page.next_cursor.clone(),
        Some(1),
        SortDirection::Desc,
        Some(TurnItemsView::Full),
    )
    .await?;
    assert_eq!(turn_user_texts(&second_page.data), vec!["third"]);
    assert!(
        store
            .list_existing_segmented_legacy_turns(indexed_page_params())
            .await?
            .is_some(),
        "an explicit older Legacy page backfills the complete history index"
    );
    let third_page = read_turns_page(
        &mut app_server,
        thread_id,
        second_page.next_cursor,
        Some(1),
        SortDirection::Desc,
        Some(TurnItemsView::Full),
    )
    .await?;
    assert_eq!(turn_user_texts(&third_page.data), vec!["second"]);
    assert!(third_page.data[0].id.starts_with("rollout-"));
    let bounded_page_again = read_turns_page(
        &mut app_server,
        thread_id,
        /*cursor*/ None,
        Some(1),
        SortDirection::Desc,
        Some(TurnItemsView::Full),
    )
    .await?;
    assert_eq!(
        serde_json::to_value(bounded_page_again.data)?,
        serde_json::to_value(&bounded_page.data)?,
        "indexing older Legacy turns must not rewrite an already displayed latest turn"
    );
    let canonical_read_id = app_server
        .send_thread_read_request(ThreadReadParams {
            thread_id: thread_id.to_string(),
            include_turns: true,
        })
        .await?;
    let ThreadReadResponse { thread } = timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.read_response(canonical_read_id),
    )
    .await??;
    let canonical_latest_turn = thread
        .turns
        .last()
        .expect("full Legacy replay includes the newest turn")
        .clone();
    let canonical_implicit_turn = thread
        .turns
        .iter()
        .find(|turn| turn_user_texts(std::slice::from_ref(*turn)) == vec!["second"])
        .expect("full Legacy replay includes the historical implicit turn")
        .clone();
    assert!(canonical_implicit_turn.id.starts_with("rollout-"));

    drop(app_server);
    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let indexed_resume_id = app_server
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            exclude_turns: true,
            initial_turns_page: Some(ThreadResumeInitialTurnsPageParams {
                limit: Some(2),
                sort_direction: Some(SortDirection::Desc),
                items_view: Some(TurnItemsView::Full),
            }),
            ..Default::default()
        })
        .await?;
    let ThreadResumeResponse {
        thread,
        initial_turns_page,
        ..
    } = timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.read_response(indexed_resume_id),
    )
    .await??;
    assert!(thread.turns.is_empty());
    let latest_page =
        initial_turns_page.expect("projected Legacy resume returns the complete indexed page");
    assert_eq!(turn_user_texts(&latest_page.data), vec!["fourth", "third"]);
    let hot_resume_id = app_server
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            exclude_turns: true,
            initial_turns_page: Some(ThreadResumeInitialTurnsPageParams {
                limit: Some(2),
                sort_direction: Some(SortDirection::Desc),
                items_view: Some(TurnItemsView::Full),
            }),
            ..Default::default()
        })
        .await?;
    let ThreadResumeResponse {
        initial_turns_page, ..
    } = timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.read_response(hot_resume_id),
    )
    .await??;
    let hot_resume_page =
        initial_turns_page.expect("running Legacy resume preserves its indexed history");
    assert_eq!(
        serde_json::to_value(&hot_resume_page.data)?,
        serde_json::to_value(&latest_page.data)?,
        "a hot indexed Legacy resume must preserve cold-resume item identifiers"
    );
    let hot_list_page = read_turns_page(
        &mut app_server,
        thread_id,
        /*cursor*/ None,
        Some(2),
        SortDirection::Desc,
        Some(TurnItemsView::Full),
    )
    .await?;
    assert_eq!(
        serde_json::to_value(&hot_list_page.data)?,
        serde_json::to_value(&latest_page.data)?,
        "a running indexed Legacy thread must not revert to bounded synthetic item identifiers"
    );
    assert_eq!(
        latest_page.data[0].id, canonical_latest_turn.id,
        "indexed Legacy history must preserve the canonical full-replay implicit turn ID"
    );
    assert_eq!(
        serde_json::to_value(&latest_page.data[0].items)?,
        serde_json::to_value(&canonical_latest_turn.items)?,
        "indexed Legacy history must preserve every canonical implicit-turn item"
    );
    assert_eq!(
        turn_agent_texts(&latest_page.data),
        vec!["fourth answer", "third answer"]
    );
    let item_ids = latest_page
        .data
        .iter()
        .flat_map(|turn| turn.items.iter().map(ThreadItem::id))
        .collect::<Vec<_>>();
    assert_eq!(
        item_ids,
        vec!["item-7", "item-8", "item-9", "item-5", "item-6"]
    );
    assert!(matches!(
        &latest_page.data[0].items[2],
        ThreadItem::InterAgentCommunication { communication, .. }
            if communication.content == "worker completed"
    ));

    let older_page = read_turns_page(
        &mut app_server,
        thread_id,
        latest_page.next_cursor.clone(),
        Some(2),
        SortDirection::Desc,
        Some(TurnItemsView::Full),
    )
    .await?;
    assert_eq!(turn_user_texts(&older_page.data), vec!["second", "first"]);
    assert_eq!(
        older_page.data[0].id, canonical_implicit_turn.id,
        "indexed Legacy history must preserve canonical implicit historical turn IDs"
    );
    let indexed_item_ids = latest_page
        .data
        .iter()
        .chain(older_page.data.iter())
        .flat_map(|turn| turn.items.iter().map(ThreadItem::id))
        .collect::<Vec<_>>();
    let unique_indexed_item_ids = indexed_item_ids
        .iter()
        .copied()
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        unique_indexed_item_ids.len(),
        indexed_item_ids.len(),
        "indexed Legacy pages must not reuse synthetic item identifiers"
    );

    std::fs::remove_file(oldest_segment_path.expect("the first immutable segment exists"))?;
    drop(app_server);
    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let restarted_page = read_turns_page(
        &mut app_server,
        thread_id,
        /*cursor*/ None,
        Some(2),
        SortDirection::Desc,
        Some(TurnItemsView::Full),
    )
    .await?;
    assert_eq!(
        serde_json::to_value(restarted_page.data)?,
        serde_json::to_value(latest_page.data)?
    );

    let read_id = app_server
        .send_thread_read_request(ThreadReadParams {
            thread_id: thread_id.to_string(),
            include_turns: false,
        })
        .await?;
    let ThreadReadResponse { thread } =
        timeout(DEFAULT_READ_TIMEOUT, app_server.read_response(read_id)).await??;
    assert_eq!(thread.history_mode, ThreadHistoryMode::Legacy);

    Ok(())
}

#[tokio::test]
async fn thread_turns_list_reuses_legacy_reference_depths_without_changing_history() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
    let thread_id = codex_protocol::ThreadId::new();
    let sqlite = codex_state::SqliteConfig::new_for_testing(codex_home.path().abs());
    let store = LocalThreadStore::new(
        LocalThreadStoreConfig {
            codex_home: codex_home.path().to_path_buf(),
            sqlite,
            default_model_provider_id: "mock_provider".to_string(),
        },
        /*state_db*/ None,
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
            history_mode: codex_protocol::protocol::ThreadHistoryMode::Legacy,
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

    const TURN_COUNT: usize = 12;
    let mut recent_immutable_segment = None;
    for index in 0..TURN_COUNT {
        let turn_id = format!("turn-{index:02}");
        store
            .append_items(AppendThreadItemsParams {
                thread_id,
                items: vec![
                    paginated_turn_started(turn_id.as_str()),
                    RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
                        message: format!("user-{index}"),
                        ..Default::default()
                    })),
                    RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
                        message: "x".repeat(1024),
                        phase: None,
                        delivery: None,
                        memory_citation: None,
                    })),
                    paginated_turn_completed(turn_id.as_str()),
                ],
            })
            .await?;
        if index + 1 != TURN_COUNT {
            recent_immutable_segment = Some(
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
    let active_rollout_path = store
        .read_thread(StoreReadThreadParams {
            thread_id,
            include_archived: false,
            include_history: false,
        })
        .await?
        .rollout_path
        .expect("Legacy thread has an active rollout");
    store.shutdown_thread(thread_id).await?;

    let mut app_server = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;

    let first_page = read_turns_page(
        &mut app_server,
        thread_id,
        /*cursor*/ None,
        Some(1),
        SortDirection::Desc,
        Some(TurnItemsView::Full),
    )
    .await?;
    let repeated_first_page = read_turns_page(
        &mut app_server,
        thread_id,
        /*cursor*/ None,
        Some(1),
        SortDirection::Desc,
        Some(TurnItemsView::Full),
    )
    .await?;
    assert_eq!(first_page.data, repeated_first_page.data);
    assert_eq!(first_page.next_cursor, repeated_first_page.next_cursor);
    assert_eq!(
        first_page.backwards_cursor,
        repeated_first_page.backwards_cursor
    );

    let mut cursor = None;
    let mut turn_ids = Vec::new();
    let mut item_ids = Vec::new();
    while turn_ids.len() < TURN_COUNT {
        let page = read_turns_page(
            &mut app_server,
            thread_id,
            cursor,
            Some(1),
            SortDirection::Desc,
            Some(TurnItemsView::Full),
        )
        .await?;
        assert_eq!(page.data.len(), 1);
        turn_ids.push(page.data[0].id.clone());
        item_ids.push(
            page.data[0]
                .items
                .iter()
                .map(|item| item.id().to_string())
                .collect::<Vec<_>>(),
        );
        cursor = page.next_cursor;
    }
    assert!(cursor.is_none());
    assert_eq!(
        turn_ids,
        (0..TURN_COUNT)
            .rev()
            .map(|index| format!("turn-{index:02}"))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        item_ids,
        vec![
            vec!["item-5", "item-6"],
            vec!["item-3", "item-4"],
            vec!["item-1", "item-2"],
            vec!["item-3", "item-4"],
            vec!["item-1", "item-2"],
            vec!["item-7", "item-8"],
            vec!["item-5", "item-6"],
            vec!["item-3", "item-4"],
            vec!["item-1", "item-2"],
            vec!["item-5", "item-6"],
            vec!["item-3", "item-4"],
            vec!["item-1", "item-2"],
        ]
    );

    let late_item = RolloutLine {
        timestamp: "2026-08-03T00:00:00.000Z".to_string(),
        ordinal: None,
        item: paginated_completed_item(
            thread_id,
            "turn-00",
            CoreTurnItem::Plan(PlanItem {
                id: "late-older-plan".to_string(),
                text: "older turn updated from a newer segment".to_string(),
            }),
        ),
    };
    writeln!(
        OpenOptions::new().append(true).open(active_rollout_path)?,
        "{}",
        serde_json::to_string(&late_item)?
    )?;

    let mut cursor = None;
    let mut cached_continuation = None;
    loop {
        let page = read_turns_page(
            &mut app_server,
            thread_id,
            cursor,
            Some(1),
            SortDirection::Desc,
            Some(TurnItemsView::Full),
        )
        .await?;
        if cached_continuation.is_none() {
            cached_continuation = page.next_cursor.clone();
        }
        if page.data[0].id == "turn-00" {
            assert!(page.data[0].items.iter().any(|item| matches!(
                item,
                ThreadItem::Plan { id, text }
                    if id == "late-older-plan"
                        && text == "older turn updated from a newer segment"
            )));
            break;
        }
        cursor = page.next_cursor;
        assert!(cursor.is_some(), "the oldest turn must remain reachable");
    }

    writeln!(
        OpenOptions::new()
            .append(true)
            .open(recent_immutable_segment.expect("thread has immutable segments"))?,
        "{{malformed immutable rollout line"
    )?;
    let read_id = app_server
        .send_thread_turns_list_request(ThreadTurnsListParams {
            thread_id: thread_id.to_string(),
            cursor: cached_continuation,
            limit: Some(1),
            sort_direction: Some(SortDirection::Desc),
            items_view: Some(TurnItemsView::Full),
        })
        .await?;
    let read_error: JSONRPCError = timeout(
        DEFAULT_READ_TIMEOUT,
        app_server.read_stream_until_error_message(RequestId::Integer(read_id)),
    )
    .await??;
    assert_eq!(read_error.error.code, -32603);

    Ok(())
}

#[tokio::test]
async fn thread_turns_list_supports_requested_items_view() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let filename_ts = "2025-01-05T12-00-00";
    let conversation_id = create_fake_rollout_with_text_elements(
        codex_home.path(),
        filename_ts,
        "2025-01-05T12:00:00Z",
        "first",
        vec![],
        Some("mock_provider"),
        /*git_info*/ None,
    )?;
    let rollout_path = rollout_path(codex_home.path(), filename_ts, &conversation_id);
    append_agent_message(rollout_path.as_path(), "2025-01-05T12:01:00Z", "draft")?;
    append_agent_message(rollout_path.as_path(), "2025-01-05T12:02:00Z", "final")?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;

    let full = read_single_turn_items_view(
        &mut mcp,
        conversation_id.as_str(),
        Some(TurnItemsView::Full),
    )
    .await?;
    assert_eq!(full.items_view, TurnItemsView::Full);
    assert_eq!(
        turn_agent_texts(std::slice::from_ref(&full)),
        vec!["draft", "final"]
    );

    let summary = read_single_turn_items_view(
        &mut mcp,
        conversation_id.as_str(),
        Some(TurnItemsView::Summary),
    )
    .await?;
    assert_eq!(summary.items_view, TurnItemsView::Summary);
    assert_eq!(
        turn_user_texts(std::slice::from_ref(&summary)),
        vec!["first"]
    );
    assert_eq!(
        turn_agent_texts(std::slice::from_ref(&summary)),
        vec!["final"]
    );

    let not_loaded = read_single_turn_items_view(
        &mut mcp,
        conversation_id.as_str(),
        Some(TurnItemsView::NotLoaded),
    )
    .await?;
    assert_eq!(not_loaded.items_view, TurnItemsView::NotLoaded);
    assert!(not_loaded.items.is_empty());
    assert_eq!(not_loaded.id, full.id);
    assert_eq!(not_loaded.status, full.status);
    assert_eq!(not_loaded.started_at, full.started_at);
    assert_eq!(not_loaded.completed_at, full.completed_at);
    assert_eq!(not_loaded.duration_ms, full.duration_ms);

    Ok(())
}

#[tokio::test]
async fn thread_search_occurrences_reads_paginated_projection() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
    let thread_id = codex_protocol::ThreadId::default();
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
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                paginated_turn_started("turn-1"),
                paginated_completed_item(
                    thread_id,
                    "turn-1",
                    CoreTurnItem::UserMessage(UserMessageItem {
                        id: "user-1".to_string(),
                        client_id: None,
                        content: vec![
                            codex_protocol::user_input::UserInput::Text {
                                text: "Nee".to_string(),
                                text_elements: Vec::new(),
                            },
                            codex_protocol::user_input::UserInput::Text {
                                text: "dle needle needle needle".to_string(),
                                text_elements: Vec::new(),
                            },
                        ],
                    }),
                ),
                paginated_completed_item(
                    thread_id,
                    "turn-1",
                    CoreTurnItem::UserMessage(UserMessageItem {
                        id: "steer-1".to_string(),
                        client_id: None,
                        content: vec![codex_protocol::user_input::UserInput::Text {
                            text: "steer toward needle".to_string(),
                            text_elements: Vec::new(),
                        }],
                    }),
                ),
                paginated_completed_item(
                    thread_id,
                    "turn-1",
                    CoreTurnItem::AgentMessage(AgentMessageItem {
                        id: "commentary-1".to_string(),
                        content: vec![AgentMessageContent::Text {
                            text: "commentary needle".to_string(),
                        }],
                        phase: Some(MessagePhase::Commentary),
                        memory_citation: None,
                        delivery: None,
                    }),
                ),
                paginated_completed_item(
                    thread_id,
                    "turn-1",
                    CoreTurnItem::AgentMessage(AgentMessageItem {
                        id: "final-1".to_string(),
                        content: vec![AgentMessageContent::Text {
                            text: "😀 **Final**  \nneedle".to_string(),
                        }],
                        phase: Some(MessagePhase::FinalAnswer),
                        memory_citation: None,
                        delivery: None,
                    }),
                ),
                paginated_turn_completed("turn-1"),
            ],
        })
        .await?;
    store.shutdown_thread(thread_id).await?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;
    let request_id = mcp
        .send_thread_search_occurrences_request(ThreadSearchOccurrencesParams {
            thread_id: thread_id.to_string(),
            search_term: "needle".to_string(),
            cursor: None,
            limit: Some(3),
        })
        .await?;
    let response: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    let ThreadSearchOccurrencesResponse { data, next_cursor } = to_response(response)?;

    assert_eq!(
        data.iter()
            .map(|occurrence| occurrence.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["user-1", "user-1", "user-1"]
    );
    assert_eq!(
        data.iter()
            .map(|occurrence| occurrence.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec!["turn-1", "turn-1", "turn-1"]
    );
    assert_eq!(
        data.iter()
            .map(|occurrence| occurrence.snippet_match_range.start)
            .collect::<Vec<_>>(),
        vec![0, 7, 14]
    );
    let next_cursor = next_cursor.expect("first page should have another occurrence");

    let request_id = mcp
        .send_thread_search_occurrences_request(ThreadSearchOccurrencesParams {
            thread_id: thread_id.to_string(),
            search_term: "needle".to_string(),
            cursor: Some(next_cursor),
            limit: Some(3),
        })
        .await?;
    let response: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    let ThreadSearchOccurrencesResponse { data, next_cursor } = to_response(response)?;

    assert_eq!(
        data.iter()
            .map(|occurrence| occurrence.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["user-1", "steer-1", "final-1"]
    );
    assert_eq!(
        data.iter()
            .map(|occurrence| occurrence.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec!["turn-1", "turn-1", "turn-1"]
    );
    assert_eq!(data[2].snippet, "😀 Final needle");
    assert_eq!(data[2].snippet_match_range.start, 9);
    assert_eq!(data[2].snippet_match_range.end, 15);
    assert_eq!(next_cursor, None);

    let fork_request_id = mcp
        .send_thread_fork_request(ThreadForkParams {
            thread_id: thread_id.to_string(),
            ..Default::default()
        })
        .await?;
    let ThreadForkResponse { thread, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(fork_request_id)).await??;
    let forked_thread_id = thread.id;
    let source_resume_id = mcp
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            ..Default::default()
        })
        .await?;
    let _: ThreadResumeResponse =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(source_resume_id)).await??;
    for (target_thread_id, text) in [
        (thread_id.to_string(), "excluded parent needle"),
        (forked_thread_id.clone(), "child needle"),
    ] {
        let turn_id = mcp
            .send_turn_start_request(TurnStartParams {
                thread_id: target_thread_id,
                input: vec![UserInput::Text {
                    text: text.to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            })
            .await?;
        let _: TurnStartResponse =
            timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(turn_id)).await??;
        timeout(
            DEFAULT_READ_TIMEOUT,
            mcp.read_stream_until_notification_message("turn/completed"),
        )
        .await??;
    }
    let request_id = mcp
        .send_thread_search_occurrences_request(ThreadSearchOccurrencesParams {
            thread_id: forked_thread_id.clone(),
            search_term: "needle".to_string(),
            cursor: None,
            limit: Some(6),
        })
        .await?;
    let ThreadSearchOccurrencesResponse { data, next_cursor } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(request_id)).await??;
    assert_eq!(data.len(), 6);
    assert!(
        data.iter()
            .all(|occurrence| !occurrence.snippet.contains("excluded parent needle"))
    );
    let next_cursor = next_cursor.expect("search should continue into child history");
    let request_id = mcp
        .send_thread_search_occurrences_request(ThreadSearchOccurrencesParams {
            thread_id: forked_thread_id,
            search_term: "needle".to_string(),
            cursor: Some(next_cursor),
            limit: Some(6),
        })
        .await?;
    let ThreadSearchOccurrencesResponse { data, next_cursor } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(request_id)).await??;
    assert_eq!(data.len(), 1);
    assert!(data[0].snippet.contains("child needle"));
    assert_eq!(next_cursor, None);

    Ok(())
}

#[tokio::test]
async fn thread_turns_list_reads_store_history_without_rollout_path() -> Result<()> {
    let codex_home = TempDir::new()?;
    let thread_id = codex_protocol::ThreadId::from_string("00000000-0000-4000-8000-000000000123")?;
    let store_id = Uuid::new_v4().to_string();
    MockResponsesConfig::new("http://127.0.0.1:1")
        .with_root_config(&format!(
            r#"experimental_thread_store = {{ type = "in_memory", id = "{store_id}" }}"#
        ))
        .write(codex_home.path())?;
    let store = InMemoryThreadStore::for_id(store_id.clone());
    let _in_memory_store = InMemoryThreadStoreId { store_id };
    seed_pathless_store_thread(&store, thread_id).await?;

    let loader_overrides = LoaderOverrides::without_managed_config_for_tests();
    let config = ConfigBuilder::default()
        .codex_home(codex_home.path().to_path_buf())
        .fallback_cwd(Some(codex_home.path().to_path_buf()))
        .loader_overrides(loader_overrides.clone())
        .build()
        .await?;
    let client = in_process::start(InProcessStartArgs {
        arg0_paths: Arg0DispatchPaths::default(),
        config: Arc::new(config),
        cli_overrides: Vec::new(),
        loader_overrides,
        strict_config: false,
        cloud_config_bundle: CloudConfigBundleLoader::default(),
        thread_config_loader: Arc::new(codex_config::NoopThreadConfigLoader),
        feedback: CodexFeedback::new(),
        log_db: None,
        state_db: None,
        environment_manager: Arc::new(EnvironmentManager::default_for_tests()),
        config_warnings: Vec::new(),
        session_source: SessionSource::Cli.into(),
        enable_codex_api_key_env: false,
        initialize: InitializeParams {
            client_info: ClientInfo {
                name: "codex-app-server-tests".to_string(),
                title: None,
                version: "0.1.0".to_string(),
            },
            capabilities: Some(InitializeCapabilities {
                experimental_api: true,
                ..Default::default()
            }),
        },
        channel_capacity: in_process::DEFAULT_IN_PROCESS_CHANNEL_CAPACITY,
    })
    .await?;

    let result = client
        .request(ClientRequest::ThreadTurnsList {
            request_id: RequestId::Integer(1),
            params: ThreadTurnsListParams {
                thread_id: thread_id.to_string(),
                cursor: None,
                limit: Some(10),
                sort_direction: Some(SortDirection::Asc),
                items_view: None,
            },
        })
        .await?
        .expect("thread/turns/list should succeed");
    let ThreadTurnsListResponse { data, .. } = serde_json::from_value(result)?;

    assert_eq!(turn_user_texts(&data), vec!["history from store"]);

    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn thread_read_loaded_include_turns_reads_store_history_without_rollout_path() -> Result<()> {
    let codex_home = TempDir::new()?;
    let store_id = Uuid::new_v4().to_string();
    MockResponsesConfig::new("http://127.0.0.1:1")
        .with_root_config(&format!(
            r#"experimental_thread_store = {{ type = "in_memory", id = "{store_id}" }}"#
        ))
        .write(codex_home.path())?;
    let store = InMemoryThreadStore::for_id(store_id.clone());
    let _in_memory_store = InMemoryThreadStoreId { store_id };

    let loader_overrides = LoaderOverrides::without_managed_config_for_tests();
    let config = ConfigBuilder::default()
        .codex_home(codex_home.path().to_path_buf())
        .fallback_cwd(Some(codex_home.path().to_path_buf()))
        .loader_overrides(loader_overrides.clone())
        .build()
        .await?;
    let client = in_process::start(InProcessStartArgs {
        arg0_paths: Arg0DispatchPaths::default(),
        config: Arc::new(config),
        cli_overrides: Vec::new(),
        loader_overrides,
        strict_config: false,
        cloud_config_bundle: CloudConfigBundleLoader::default(),
        thread_config_loader: Arc::new(codex_config::NoopThreadConfigLoader),
        feedback: CodexFeedback::new(),
        log_db: None,
        state_db: None,
        environment_manager: Arc::new(EnvironmentManager::default_for_tests()),
        config_warnings: Vec::new(),
        session_source: SessionSource::Cli.into(),
        enable_codex_api_key_env: false,
        initialize: InitializeParams {
            client_info: ClientInfo {
                name: "codex-app-server-tests".to_string(),
                title: None,
                version: "0.1.0".to_string(),
            },
            capabilities: Some(InitializeCapabilities {
                experimental_api: true,
                ..Default::default()
            }),
        },
        channel_capacity: in_process::DEFAULT_IN_PROCESS_CHANNEL_CAPACITY,
    })
    .await?;

    let result = client
        .request(ClientRequest::ThreadStart {
            request_id: RequestId::Integer(1),
            params: ThreadStartParams {
                model: Some("mock-model".to_string()),
                ..Default::default()
            },
        })
        .await?
        .expect("thread/start should succeed");
    let ThreadStartResponse { thread, .. } = serde_json::from_value(result)?;
    assert_eq!(thread.path, None);

    let thread_id = codex_protocol::ThreadId::from_string(&thread.id)?;
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: store_history_items(),
        })
        .await?;

    let result = client
        .request(ClientRequest::ThreadRead {
            request_id: RequestId::Integer(2),
            params: ThreadReadParams {
                thread_id: thread.id,
                include_turns: true,
            },
        })
        .await?
        .expect("thread/read should succeed");
    let ThreadReadResponse { thread, .. } = serde_json::from_value(result)?;

    assert_eq!(turn_user_texts(&thread.turns), vec!["history from store"]);
    let [ThreadItem::UserMessage { content, .. }] = thread.turns[0].items.as_slice() else {
        panic!("expected one user message item");
    };
    assert_eq!(
        content,
        &vec![
            UserInput::Text {
                text: "history from store".to_string(),
                text_elements: Vec::new(),
            },
            UserInput::Audio {
                url: "https://example.com/recording.mp3".to_string(),
            },
            UserInput::LocalAudio {
                path: "recording.wav".into(),
            },
        ]
    );

    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn thread_list_includes_store_thread_without_rollout_path() -> Result<()> {
    let codex_home = TempDir::new()?;
    let thread_id = codex_protocol::ThreadId::from_string("00000000-0000-4000-8000-000000000124")?;
    let store_id = Uuid::new_v4().to_string();
    MockResponsesConfig::new("http://127.0.0.1:1")
        .with_root_config(&format!(
            r#"experimental_thread_store = {{ type = "in_memory", id = "{store_id}" }}"#
        ))
        .write(codex_home.path())?;
    let store = InMemoryThreadStore::for_id(store_id.clone());
    let _in_memory_store = InMemoryThreadStoreId { store_id };
    seed_pathless_store_thread(&store, thread_id).await?;

    let loader_overrides = LoaderOverrides::without_managed_config_for_tests();
    let config = ConfigBuilder::default()
        .codex_home(codex_home.path().to_path_buf())
        .fallback_cwd(Some(codex_home.path().to_path_buf()))
        .loader_overrides(loader_overrides.clone())
        .build()
        .await?;
    let client = in_process::start(InProcessStartArgs {
        arg0_paths: Arg0DispatchPaths::default(),
        config: Arc::new(config),
        cli_overrides: Vec::new(),
        loader_overrides,
        strict_config: false,
        cloud_config_bundle: CloudConfigBundleLoader::default(),
        thread_config_loader: Arc::new(codex_config::NoopThreadConfigLoader),
        feedback: CodexFeedback::new(),
        log_db: None,
        state_db: None,
        environment_manager: Arc::new(EnvironmentManager::default_for_tests()),
        config_warnings: Vec::new(),
        session_source: SessionSource::Cli.into(),
        enable_codex_api_key_env: false,
        initialize: InitializeParams {
            client_info: ClientInfo {
                name: "codex-app-server-tests".to_string(),
                title: None,
                version: "0.1.0".to_string(),
            },
            capabilities: Some(InitializeCapabilities {
                experimental_api: true,
                ..Default::default()
            }),
        },
        channel_capacity: in_process::DEFAULT_IN_PROCESS_CHANNEL_CAPACITY,
    })
    .await?;

    let result = client
        .request(ClientRequest::ThreadList {
            request_id: RequestId::Integer(1),
            params: ThreadListParams {
                cursor: None,
                limit: Some(10),
                sort_key: None,
                sort_direction: None,
                model_providers: Some(Vec::new()),
                source_kinds: None,
                archived: None,
                section_id: None,
                project_id: None,
                cwd: None,
                use_state_db_only: false,
                search_term: None,
                parent_thread_id: None,
                ancestor_thread_id: None,
            },
        })
        .await?
        .expect("thread/list should succeed");
    let ThreadListResponse { data, .. } = serde_json::from_value(result)?;

    assert_eq!(data.len(), 1);
    let thread = &data[0];
    assert_eq!(thread.id, thread_id.to_string());
    assert_eq!(thread.path, None);
    assert_eq!(thread.preview, "");
    assert_eq!(thread.name.as_deref(), Some("named pathless thread"));

    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn thread_read_can_return_archived_threads_by_id() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let filename_ts = "2025-01-05T12-00-00";
    let preview = "Archived saved user message";
    let conversation_id = create_fake_rollout_with_text_elements(
        codex_home.path(),
        filename_ts,
        "2025-01-05T12:00:00Z",
        preview,
        vec![],
        Some("mock_provider"),
        /*git_info*/ None,
    )?;
    let active_rollout_path = rollout_path(codex_home.path(), filename_ts, &conversation_id);
    let archived_dir = codex_home.path().join(ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir_all(&archived_dir)?;
    let archived_rollout_path =
        archived_dir.join(active_rollout_path.file_name().expect("rollout file name"));
    std::fs::rename(&active_rollout_path, &archived_rollout_path)?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;

    let read_id = mcp
        .send_thread_read_request(ThreadReadParams {
            thread_id: conversation_id.clone(),
            include_turns: false,
        })
        .await?;
    let ThreadReadResponse { thread } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(read_id)).await??;

    assert_eq!(thread.id, conversation_id);
    assert_eq!(thread.preview, preview);
    let path = thread.path.expect("thread path");
    assert_eq!(path.canonicalize()?, archived_rollout_path.canonicalize()?);

    Ok(())
}

#[tokio::test]
async fn thread_resume_initial_turns_page_matches_requested_turns_list_page() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let filename_ts = "2025-01-05T12-00-00";
    let conversation_id = create_fake_rollout_with_text_elements(
        codex_home.path(),
        filename_ts,
        "2025-01-05T12:00:00Z",
        "first",
        vec![],
        Some("mock_provider"),
        /*git_info*/ None,
    )?;
    let rollout_path = rollout_path(codex_home.path(), filename_ts, &conversation_id);
    append_user_message(rollout_path.as_path(), "2025-01-05T12:01:00Z", "second")?;
    append_user_message(rollout_path.as_path(), "2025-01-05T12:02:00Z", "third")?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;

    let turns_list_id = mcp
        .send_thread_turns_list_request(ThreadTurnsListParams {
            thread_id: conversation_id.clone(),
            cursor: None,
            limit: Some(2),
            sort_direction: Some(SortDirection::Asc),
            items_view: Some(TurnItemsView::NotLoaded),
        })
        .await?;
    let turns_list_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(turns_list_id)),
    )
    .await??;
    let expected_page = to_response::<ThreadTurnsListResponse>(turns_list_resp)?;

    let resume_id = mcp
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: conversation_id.clone(),
            exclude_turns: true,
            initial_turns_page: Some(ThreadResumeInitialTurnsPageParams {
                limit: Some(2),
                sort_direction: Some(SortDirection::Asc),
                items_view: Some(TurnItemsView::NotLoaded),
            }),
            ..Default::default()
        })
        .await?;
    let ThreadResumeResponse {
        thread,
        initial_turns_page,
        ..
    } = timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(resume_id)).await??;

    assert!(thread.turns.is_empty());
    assert_eq!(
        initial_turns_page,
        Some(codex_app_server_protocol::TurnsPage::from(
            expected_page.clone()
        ))
    );

    let post_resume_id = mcp
        .send_thread_turns_list_request(ThreadTurnsListParams {
            thread_id: conversation_id.clone(),
            cursor: None,
            limit: Some(2),
            sort_direction: Some(SortDirection::Asc),
            items_view: Some(TurnItemsView::NotLoaded),
        })
        .await?;
    let post_resume_page: ThreadTurnsListResponse =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(post_resume_id)).await??;
    assert_eq!(post_resume_page, expected_page);

    let full_page = read_turns_page(
        &mut mcp,
        codex_protocol::ThreadId::from_string(&conversation_id)?,
        /*cursor*/ None,
        Some(1),
        SortDirection::Asc,
        Some(TurnItemsView::Full),
    )
    .await?;
    let turn = full_page.data.first().expect("one full turn");
    let items_page = read_items_page(
        &mut mcp,
        codex_protocol::ThreadId::from_string(&conversation_id)?,
        Some(turn.id.as_str()),
        /*cursor*/ None,
        Some(20),
        SortDirection::Asc,
    )
    .await?;
    assert_eq!(
        items_page.data,
        turn.items
            .iter()
            .cloned()
            .map(|item| ThreadItemEntry {
                turn_id: turn.id.clone(),
                item,
            })
            .collect::<Vec<_>>()
    );

    Ok(())
}

#[tokio::test]
async fn thread_turns_list_rejects_cursor_when_anchor_turn_is_rolled_back() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let filename_ts = "2025-01-05T12-00-00";
    let conversation_id = create_fake_rollout_with_text_elements(
        codex_home.path(),
        filename_ts,
        "2025-01-05T12:00:00Z",
        "first",
        vec![],
        Some("mock_provider"),
        /*git_info*/ None,
    )?;
    let rollout_path = rollout_path(codex_home.path(), filename_ts, &conversation_id);
    append_user_message(rollout_path.as_path(), "2025-01-05T12:01:00Z", "second")?;
    append_user_message(rollout_path.as_path(), "2025-01-05T12:02:00Z", "third")?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;

    let read_id = mcp
        .send_thread_turns_list_request(ThreadTurnsListParams {
            thread_id: conversation_id.clone(),
            cursor: None,
            limit: Some(2),
            sort_direction: Some(SortDirection::Desc),
            items_view: None,
        })
        .await?;
    let ThreadTurnsListResponse {
        backwards_cursor, ..
    } = timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(read_id)).await??;
    let backwards_cursor = backwards_cursor.expect("expected backwardsCursor for newest turn");

    append_thread_rollback(
        rollout_path.as_path(),
        "2025-01-05T12:03:00Z",
        /*num_turns*/ 1,
    )?;

    let read_id = mcp
        .send_thread_turns_list_request(ThreadTurnsListParams {
            thread_id: conversation_id,
            cursor: Some(backwards_cursor),
            limit: Some(10),
            sort_direction: Some(SortDirection::Asc),
            items_view: None,
        })
        .await?;
    let read_err: JSONRPCError = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_error_message(RequestId::Integer(read_id)),
    )
    .await??;

    assert_eq!(
        read_err.error.message,
        "invalid cursor: anchor turn is no longer present"
    );

    Ok(())
}

#[tokio::test]
async fn thread_read_returns_forked_from_id_for_forked_threads() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let conversation_id = create_fake_rollout_with_text_elements(
        codex_home.path(),
        "2025-01-05T12-00-00",
        "2025-01-05T12:00:00Z",
        "Saved user message",
        vec![],
        Some("mock_provider"),
        /*git_info*/ None,
    )?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;

    let fork_id = mcp
        .send_thread_fork_request(ThreadForkParams {
            thread_id: conversation_id.clone(),
            ..Default::default()
        })
        .await?;
    let ThreadForkResponse { thread: forked, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(fork_id)).await??;

    let read_id = mcp
        .send_thread_read_request(ThreadReadParams {
            thread_id: forked.id,
            include_turns: false,
        })
        .await?;
    let ThreadReadResponse { thread, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(read_id)).await??;

    assert_eq!(thread.forked_from_id, Some(conversation_id));

    Ok(())
}

#[tokio::test]
async fn thread_read_loaded_thread_returns_precomputed_path_before_materialization() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;

    let start_id = mcp
        .send_thread_start_request_with_auto_env(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })
        .await?;
    let ThreadStartResponse { thread, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(start_id)).await??;
    let thread_path = thread.path.clone().expect("thread path");
    assert!(
        !thread_path.exists(),
        "fresh thread rollout should not be materialized yet"
    );

    let read_id = mcp
        .send_thread_read_request(ThreadReadParams {
            thread_id: thread.id.clone(),
            include_turns: false,
        })
        .await?;
    let ThreadReadResponse { thread: read, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(read_id)).await??;

    assert_eq!(read.id, thread.id);
    assert_eq!(read.path, Some(thread_path));
    assert!(read.preview.is_empty());
    assert_eq!(read.turns.len(), 0);
    assert_eq!(read.status, ThreadStatus::Idle);

    Ok(())
}

#[tokio::test]
async fn paginated_thread_name_set_is_reflected_in_read_list_and_metadata_resume() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let conversation_id = create_fake_paginated_rollout(
        codex_home.path(),
        "2025-01-05T12-00-00",
        "2025-01-05T12:00:00Z",
        "Saved user message",
        Some("mock_provider"),
        /*git_info*/ None,
    )?;

    app_test_support::append_fake_paginated_user_message(
        &rollout_path(codex_home.path(), "2025-01-05T12-00-00", &conversation_id),
        &conversation_id,
        "Saved user message",
    )
    .await?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;

    // Set a user-facing thread title.
    let new_name = "Custom saved name";
    let set_id = mcp
        .send_thread_set_name_request(ThreadSetNameParams {
            thread_id: conversation_id.clone(),
            name: new_name.to_string(),
        })
        .await?;
    let _: ThreadSetNameResponse =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(set_id)).await??;
    let notification = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("thread/name/updated"),
    )
    .await??;
    let notification: ThreadNameUpdatedNotification =
        serde_json::from_value(notification.params.expect("thread/name/updated params"))?;
    assert_eq!(notification.thread_id, conversation_id);
    assert_eq!(notification.thread_name.as_deref(), Some(new_name));

    // Read should now surface `thread.name`, and the wire payload must include `name`.
    let read_id = mcp
        .send_thread_read_request(ThreadReadParams {
            thread_id: conversation_id.clone(),
            include_turns: false,
        })
        .await?;
    let read_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(read_id)),
    )
    .await??;
    let read_result = read_resp.result.clone();
    let ThreadReadResponse { thread, .. } = to_response::<ThreadReadResponse>(read_resp)?;
    assert_eq!(thread.id, conversation_id);
    assert_eq!(thread.name.as_deref(), Some(new_name));
    assert_eq!(thread.history_mode, ThreadHistoryMode::Paginated);
    let thread_json = read_result
        .get("thread")
        .and_then(Value::as_object)
        .expect("thread/read result.thread must be an object");
    assert_eq!(
        thread_json.get("name").and_then(Value::as_str),
        Some(new_name),
        "thread/read must serialize `thread.name` on the wire"
    );
    assert_eq!(
        thread_json.get("ephemeral").and_then(Value::as_bool),
        Some(false),
        "thread/read must serialize `thread.ephemeral` on the wire"
    );

    // List should also surface the name.
    let list_id = mcp
        .send_thread_list_request(ThreadListParams {
            cursor: None,
            limit: Some(50),
            sort_key: None,
            sort_direction: None,
            model_providers: Some(vec!["mock_provider".to_string()]),
            source_kinds: None,
            archived: None,
            section_id: None,
            project_id: None,
            cwd: None,
            use_state_db_only: true,
            search_term: None,
            parent_thread_id: None,
            ancestor_thread_id: None,
        })
        .await?;
    let list_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(list_id)),
    )
    .await??;
    let list_result = list_resp.result.clone();
    let ThreadListResponse { data, .. } = to_response::<ThreadListResponse>(list_resp)?;
    let listed = data
        .iter()
        .find(|t| t.id == conversation_id)
        .expect("thread/list should include the created thread");
    assert_eq!(listed.name.as_deref(), Some(new_name));
    let listed_json = list_result
        .get("data")
        .and_then(Value::as_array)
        .expect("thread/list result.data must be an array")
        .iter()
        .find(|t| t.get("id").and_then(Value::as_str) == Some(&conversation_id))
        .and_then(Value::as_object)
        .expect("thread/list should include the created thread as an object");
    assert_eq!(
        listed_json.get("name").and_then(Value::as_str),
        Some(new_name),
        "thread/list must serialize `thread.name` on the wire"
    );
    assert_eq!(
        listed_json.get("ephemeral").and_then(Value::as_bool),
        Some(false),
        "thread/list must serialize `thread.ephemeral` on the wire"
    );

    // Resume should also surface the name.
    let resume_id = mcp
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: conversation_id.clone(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let resume_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(resume_id)),
    )
    .await??;
    let resume_result = resume_resp.result.clone();
    let ThreadResumeResponse {
        thread: resumed, ..
    } = to_response::<ThreadResumeResponse>(resume_resp)?;
    assert_eq!(resumed.id, conversation_id);
    assert_eq!(resumed.name.as_deref(), Some(new_name));
    let resumed_json = resume_result
        .get("thread")
        .and_then(Value::as_object)
        .expect("thread/resume result.thread must be an object");
    assert_eq!(
        resumed_json.get("name").and_then(Value::as_str),
        Some(new_name),
        "thread/resume must serialize `thread.name` on the wire"
    );
    assert_eq!(
        resumed_json.get("ephemeral").and_then(Value::as_bool),
        Some(false),
        "thread/resume must serialize `thread.ephemeral` on the wire"
    );

    Ok(())
}

#[tokio::test]
async fn thread_read_include_turns_rejects_unmaterialized_loaded_thread() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;

    let start_id = mcp
        .send_thread_start_request_with_auto_env(ThreadStartParams {
            model: Some("mock-model".to_string()),
            history_mode: Some(ThreadHistoryMode::Legacy),
            ..Default::default()
        })
        .await?;
    let ThreadStartResponse { thread, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(start_id)).await??;
    let thread_path = thread.path.clone().expect("thread path");
    assert!(
        !thread_path.exists(),
        "fresh thread rollout should not be materialized yet"
    );

    let read_id = mcp
        .send_thread_read_request(ThreadReadParams {
            thread_id: thread.id.clone(),
            include_turns: true,
        })
        .await?;
    let read_err: JSONRPCError = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_error_message(RequestId::Integer(read_id)),
    )
    .await??;

    assert!(
        read_err
            .error
            .message
            .contains("includeTurns is unavailable before first user message"),
        "unexpected error: {}",
        read_err.error.message
    );

    Ok(())
}

#[tokio::test]
async fn thread_turns_list_rejects_unmaterialized_loaded_thread() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;

    let start_id = mcp
        .send_thread_start_request_with_auto_env(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })
        .await?;
    let ThreadStartResponse { thread, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(start_id)).await??;
    let thread_path = thread.path.clone().expect("thread path");
    assert!(
        !thread_path.exists(),
        "fresh thread rollout should not be materialized yet"
    );

    let read_id = mcp
        .send_thread_turns_list_request(ThreadTurnsListParams {
            thread_id: thread.id,
            cursor: None,
            limit: None,
            sort_direction: None,
            items_view: None,
        })
        .await?;
    let read_err: JSONRPCError = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_error_message(RequestId::Integer(read_id)),
    )
    .await??;

    assert!(
        read_err
            .error
            .message
            .contains("thread/turns/list is unavailable before first user message"),
        "unexpected error: {}",
        read_err.error.message
    );

    Ok(())
}

#[tokio::test]
async fn paginated_history_lists_and_legacy_reads_use_projected_turns_and_items() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;
    let thread_id = codex_protocol::ThreadId::default();
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
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: vec![
                paginated_turn_started("turn-1"),
                paginated_completed_item(
                    thread_id,
                    "turn-1",
                    CoreTurnItem::UserMessage(UserMessageItem {
                        id: "user-1".to_string(),
                        client_id: None,
                        content: Vec::new(),
                    }),
                ),
                paginated_completed_item(
                    thread_id,
                    "turn-1",
                    CoreTurnItem::UserMessage(UserMessageItem {
                        id: "steer-1".to_string(),
                        client_id: None,
                        content: Vec::new(),
                    }),
                ),
                paginated_completed_item(
                    thread_id,
                    "turn-1",
                    CoreTurnItem::AgentMessage(AgentMessageItem {
                        id: "agent-1".to_string(),
                        content: vec![AgentMessageContent::Text {
                            text: "first".to_string(),
                        }],
                        phase: None,
                        memory_citation: None,
                        delivery: None,
                    }),
                ),
                paginated_completed_item(
                    thread_id,
                    "turn-1",
                    CoreTurnItem::UserMessage(UserMessageItem {
                        id: "steer-1".to_string(),
                        client_id: Some("updated-steer".to_string()),
                        content: Vec::new(),
                    }),
                ),
                paginated_turn_completed("turn-1"),
                paginated_turn_started("turn-2"),
                paginated_completed_item(
                    thread_id,
                    "turn-2",
                    CoreTurnItem::UserMessage(UserMessageItem {
                        id: "user-2".to_string(),
                        client_id: None,
                        content: Vec::new(),
                    }),
                ),
            ],
        })
        .await?;
    store.shutdown_thread(thread_id).await?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;

    let expected_turn_1_full = Turn {
        id: "turn-1".to_string(),
        items: vec![
            ThreadItem::UserMessage {
                id: "user-1".to_string(),
                client_id: None,
                content: Vec::new(),
            },
            ThreadItem::UserMessage {
                id: "steer-1".to_string(),
                client_id: Some("updated-steer".to_string()),
                content: Vec::new(),
            },
            ThreadItem::AgentMessage {
                id: "agent-1".to_string(),
                text: "first".to_string(),
                phase: None,
                memory_citation: None,
                delivery: None,
            },
        ],
        items_view: TurnItemsView::Full,
        status: TurnStatus::Completed,
        error: None,
        started_at: Some(10),
        completed_at: Some(20),
        duration_ms: Some(10_000),
    };
    let expected_turn_2_full = Turn {
        id: "turn-2".to_string(),
        items: vec![ThreadItem::UserMessage {
            id: "user-2".to_string(),
            client_id: None,
            content: Vec::new(),
        }],
        items_view: TurnItemsView::Full,
        status: TurnStatus::Interrupted,
        error: None,
        started_at: Some(10),
        completed_at: None,
        duration_ms: None,
    };
    let expected_full_turns = vec![expected_turn_1_full.clone(), expected_turn_2_full.clone()];

    let read_id = mcp
        .send_thread_read_request(ThreadReadParams {
            thread_id: thread_id.to_string(),
            include_turns: true,
        })
        .await?;
    let ThreadReadResponse {
        thread: unloaded_thread,
    } = timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(read_id)).await??;
    assert_eq!(unloaded_thread.turns, expected_full_turns);

    let legacy_resume_id = mcp
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            ..Default::default()
        })
        .await?;
    let ThreadResumeResponse {
        thread: legacy_thread,
        ..
    } = timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(legacy_resume_id)).await??;
    assert_eq!(legacy_thread.turns, expected_full_turns);

    let loaded_read_id = mcp
        .send_thread_read_request(ThreadReadParams {
            thread_id: thread_id.to_string(),
            include_turns: true,
        })
        .await?;
    let loaded_read_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(loaded_read_id)),
    )
    .await??;
    let ThreadReadResponse {
        thread: loaded_read_thread,
    } = to_response::<ThreadReadResponse>(loaded_read_resp)?;
    assert_eq!(loaded_read_thread.turns, expected_full_turns);

    let initial_page_resume_id = mcp
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            exclude_turns: true,
            initial_turns_page: Some(ThreadResumeInitialTurnsPageParams {
                limit: Some(1),
                sort_direction: Some(SortDirection::Desc),
                items_view: Some(TurnItemsView::Full),
            }),
            ..Default::default()
        })
        .await?;
    let ThreadResumeResponse {
        thread: initial_page_thread,
        initial_turns_page,
        ..
    } = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_response(initial_page_resume_id),
    )
    .await??;
    assert!(initial_page_thread.turns.is_empty());
    assert_eq!(
        initial_turns_page.expect("initial turns page").data,
        vec![expected_turn_2_full]
    );

    let resume_id = mcp
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let ThreadResumeResponse {
        thread,
        turns_backwards_cursor,
        items_backwards_cursor,
        ..
    } = timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(resume_id)).await??;
    assert!(thread.turns.is_empty());
    let turns_backwards_cursor =
        turns_backwards_cursor.expect("resume should return a turn head cursor");
    let items_backwards_cursor =
        items_backwards_cursor.expect("resume should return an item head cursor");

    let rejoin_id = mcp
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let ThreadResumeResponse {
        turns_backwards_cursor: rejoin_turns_backwards_cursor,
        items_backwards_cursor: rejoin_items_backwards_cursor,
        ..
    } = timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(rejoin_id)).await??;
    assert_eq!(
        rejoin_turns_backwards_cursor.as_deref(),
        Some(turns_backwards_cursor.as_str())
    );
    assert_eq!(
        rejoin_items_backwards_cursor.as_deref(),
        Some(items_backwards_cursor.as_str())
    );

    let ThreadTurnsListResponse { data, .. } = read_turns_page(
        &mut mcp,
        thread_id,
        Some(turns_backwards_cursor),
        Some(2),
        SortDirection::Desc,
        Some(TurnItemsView::NotLoaded),
    )
    .await?;
    assert_eq!(
        data.into_iter().map(|turn| turn.id).collect::<Vec<_>>(),
        vec!["turn-2", "turn-1"]
    );

    let ThreadItemsListResponse { data, .. } = read_items_page(
        &mut mcp,
        thread_id,
        /*turn_id*/ None,
        Some(items_backwards_cursor.clone()),
        Some(3),
        SortDirection::Desc,
    )
    .await?;
    assert_eq!(
        data.into_iter()
            .map(|entry| entry.item.id().to_string())
            .collect::<Vec<_>>(),
        vec!["user-2", "agent-1", "steer-1"]
    );

    let ThreadItemsListResponse { data, .. } = read_items_page(
        &mut mcp,
        thread_id,
        Some("turn-1"),
        Some(items_backwards_cursor),
        Some(2),
        SortDirection::Desc,
    )
    .await?;
    assert_eq!(
        data.into_iter()
            .map(|entry| entry.item.id().to_string())
            .collect::<Vec<_>>(),
        vec!["agent-1", "steer-1"]
    );

    let first_page = read_turns_page(
        &mut mcp,
        thread_id,
        /*cursor*/ None,
        Some(1),
        SortDirection::Asc,
        Some(TurnItemsView::Summary),
    )
    .await?;
    assert_eq!(
        first_page.data,
        vec![Turn {
            id: "turn-1".to_string(),
            items: vec![
                ThreadItem::UserMessage {
                    id: "user-1".to_string(),
                    client_id: None,
                    content: Vec::new(),
                },
                ThreadItem::AgentMessage {
                    id: "agent-1".to_string(),
                    text: "first".to_string(),
                    phase: None,
                    memory_citation: None,
                    delivery: None,
                },
            ],
            items_view: TurnItemsView::Summary,
            status: TurnStatus::Completed,
            error: None,
            started_at: Some(10),
            completed_at: Some(20),
            duration_ms: Some(10_000),
        }]
    );
    let next_cursor = first_page.next_cursor.expect("next turn cursor");
    let second_page = read_turns_page(
        &mut mcp,
        thread_id,
        Some(next_cursor),
        Some(1),
        SortDirection::Asc,
        Some(TurnItemsView::NotLoaded),
    )
    .await?;
    assert_eq!(
        second_page.data,
        vec![Turn {
            id: "turn-2".to_string(),
            items: Vec::new(),
            items_view: TurnItemsView::NotLoaded,
            status: TurnStatus::Interrupted,
            error: None,
            started_at: Some(10),
            completed_at: None,
            duration_ms: None,
        }]
    );

    let full_page = read_turns_page(
        &mut mcp,
        thread_id,
        /*cursor*/ None,
        Some(1),
        SortDirection::Asc,
        Some(TurnItemsView::Full),
    )
    .await?;
    assert_eq!(full_page.data, vec![expected_turn_1_full]);

    let first_items_page = read_items_page(
        &mut mcp,
        thread_id,
        /*turn_id*/ None,
        /*cursor*/ None,
        Some(1),
        SortDirection::Asc,
    )
    .await?;
    assert_eq!(first_items_page.data.len(), 1);
    assert_eq!(first_items_page.data[0].turn_id, "turn-1");
    assert_eq!(first_items_page.data[0].item.id(), "user-1");
    let second_items_page = read_items_page(
        &mut mcp,
        thread_id,
        /*turn_id*/ None,
        Some(first_items_page.next_cursor.expect("next item cursor")),
        Some(1),
        SortDirection::Asc,
    )
    .await?;
    assert_eq!(second_items_page.data.len(), 1);
    assert_eq!(second_items_page.data[0].turn_id, "turn-1");
    assert_eq!(second_items_page.data[0].item.id(), "steer-1");
    let third_items_page = read_items_page(
        &mut mcp,
        thread_id,
        /*turn_id*/ None,
        Some(second_items_page.next_cursor.expect("next item cursor")),
        Some(2),
        SortDirection::Asc,
    )
    .await?;
    assert_eq!(third_items_page.data.len(), 2);
    assert_eq!(third_items_page.data[0].turn_id, "turn-1");
    assert_eq!(third_items_page.data[0].item.id(), "agent-1");
    assert_eq!(third_items_page.data[1].turn_id, "turn-2");
    assert_eq!(third_items_page.data[1].item.id(), "user-2");

    let turn_start_id = mcp
        .send_turn_start_request(TurnStartParams {
            thread_id: thread_id.to_string(),
            client_user_message_id: None,
            input: vec![UserInput::Text {
                text: "continue after legacy resume".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })
        .await?;
    let _: TurnStartResponse =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(turn_start_id)).await??;
    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/completed"),
    )
    .await??;

    let read_id = mcp
        .send_thread_read_request(ThreadReadParams {
            thread_id: thread_id.to_string(),
            include_turns: true,
        })
        .await?;
    let ThreadReadResponse {
        thread: loaded_thread,
    } = timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(read_id)).await??;
    assert_eq!(&loaded_thread.turns[..2], expected_full_turns);
    assert_eq!(
        turn_user_texts(&loaded_thread.turns),
        vec!["continue after legacy resume"]
    );

    Ok(())
}

#[tokio::test]
async fn thread_items_list_returns_unsupported() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build_initialized()
        .await?;

    let read_id = mcp
        .send_thread_items_list_request(ThreadItemsListParams {
            thread_id: "00000000-0000-4000-8000-000000000123".to_string(),
            turn_id: None,
            cursor: None,
            limit: None,
            sort_direction: None,
        })
        .await?;
    let read_err: JSONRPCError = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_error_message(RequestId::Integer(read_id)),
    )
    .await??;

    assert_eq!(read_err.error.code, -32601);
    assert_eq!(
        read_err.error.message,
        "thread/items/list is not supported yet"
    );

    Ok(())
}

#[tokio::test]
async fn thread_read_reports_system_error_idle_flag_after_failed_turn() -> Result<()> {
    let server = responses::start_mock_server().await;
    let _response_mock = responses::mount_sse_once(
        &server,
        responses::sse_failed("resp-1", "server_error", "simulated failure"),
    )
    .await;
    let codex_home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri()).write(codex_home.path())?;

    let mut mcp = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build_initialized()
        .await?;

    let start_id = mcp
        .send_thread_start_request_with_auto_env(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })
        .await?;
    let ThreadStartResponse { thread, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(start_id)).await??;

    let turn_start_id = mcp
        .send_turn_start_request(TurnStartParams {
            thread_id: thread.id.clone(),
            client_user_message_id: None,
            input: vec![UserInput::Text {
                text: "fail this turn".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })
        .await?;
    let _: TurnStartResponse =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(turn_start_id)).await??;
    timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("error"),
    )
    .await??;

    let read_id = mcp
        .send_thread_read_request(ThreadReadParams {
            thread_id: thread.id,
            include_turns: false,
        })
        .await?;
    let ThreadReadResponse { thread, .. } =
        timeout(DEFAULT_READ_TIMEOUT, mcp.read_response(read_id)).await??;

    assert_eq!(thread.status, ThreadStatus::SystemError,);

    Ok(())
}

fn append_user_message(path: &Path, timestamp: &str, text: &str) -> std::io::Result<()> {
    let mut file = std::fs::OpenOptions::new().append(true).open(path)?;
    writeln!(
        file,
        "{}",
        json!({
            "timestamp": timestamp,
            "type":"event_msg",
            "payload": {
                "type":"user_message",
                "message": text,
                "text_elements": [],
                "local_images": []
            }
        })
    )
}

fn append_agent_message(path: &Path, timestamp: &str, text: &str) -> anyhow::Result<()> {
    let mut file = std::fs::OpenOptions::new().append(true).open(path)?;
    writeln!(
        file,
        "{}",
        json!({
            "timestamp": timestamp,
            "type": "event_msg",
            "payload": serde_json::to_value(EventMsg::AgentMessage(AgentMessageEvent {
                message: text.to_string(),
                phase: None,
                memory_citation: None,
                delivery: None,
            }))?,
        })
    )?;
    Ok(())
}

fn append_thread_rollback(path: &Path, timestamp: &str, num_turns: u32) -> std::io::Result<()> {
    let mut file = std::fs::OpenOptions::new().append(true).open(path)?;
    writeln!(
        file,
        "{}",
        json!({
            "timestamp": timestamp,
            "type":"event_msg",
            "payload": {
                "type":"thread_rolled_back",
                "num_turns": num_turns
            }
        })
    )
}

async fn read_single_turn_items_view(
    mcp: &mut TestAppServer,
    thread_id: &str,
    items_view: Option<TurnItemsView>,
) -> anyhow::Result<codex_app_server_protocol::Turn> {
    let read_id = mcp
        .send_thread_turns_list_request(ThreadTurnsListParams {
            thread_id: thread_id.to_string(),
            cursor: None,
            limit: Some(10),
            sort_direction: Some(SortDirection::Asc),
            items_view,
        })
        .await?;
    let read_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(read_id)),
    )
    .await??;
    let ThreadTurnsListResponse { mut data, .. } =
        to_response::<ThreadTurnsListResponse>(read_resp)?;
    assert_eq!(data.len(), 1);
    Ok(data.remove(0))
}

async fn read_turns_page(
    mcp: &mut TestAppServer,
    thread_id: codex_protocol::ThreadId,
    cursor: Option<String>,
    limit: Option<u32>,
    sort_direction: SortDirection,
    items_view: Option<TurnItemsView>,
) -> Result<ThreadTurnsListResponse> {
    let request_id = mcp
        .send_thread_turns_list_request(ThreadTurnsListParams {
            thread_id: thread_id.to_string(),
            cursor,
            limit,
            sort_direction: Some(sort_direction),
            items_view,
        })
        .await?;
    let response: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    to_response(response)
}

async fn read_items_page(
    mcp: &mut TestAppServer,
    thread_id: codex_protocol::ThreadId,
    turn_id: Option<&str>,
    cursor: Option<String>,
    limit: Option<u32>,
    sort_direction: SortDirection,
) -> Result<ThreadItemsListResponse> {
    let request_id = mcp
        .send_thread_items_list_request(ThreadItemsListParams {
            thread_id: thread_id.to_string(),
            turn_id: turn_id.map(str::to_string),
            cursor,
            limit,
            sort_direction: Some(sort_direction),
        })
        .await?;
    let response: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(request_id)),
    )
    .await??;
    to_response(response)
}

fn paginated_turn_started(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_id: turn_id.to_string(),
        trace_id: None,
        started_at: Some(10),
        model_context_window: None,
        collaboration_mode_kind: Default::default(),
    }))
}

fn paginated_turn_completed(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
        turn_id: turn_id.to_string(),
        last_agent_message: None,
        error: None,
        started_at: Some(10),
        completed_at: Some(20),
        duration_ms: Some(10_000),
        time_to_first_token_ms: None,
    }))
}

fn paginated_completed_item(
    thread_id: codex_protocol::ThreadId,
    turn_id: &str,
    item: CoreTurnItem,
) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
        thread_id,
        turn_id: turn_id.to_string(),
        item,
        started_at_ms: Some(0),
        completed_at_ms: 1,
    }))
}

fn turn_user_texts(turns: &[codex_app_server_protocol::Turn]) -> Vec<&str> {
    turns
        .iter()
        .filter_map(|turn| match turn.items.first()? {
            ThreadItem::UserMessage { content, .. } => match content.first()? {
                UserInput::Text { text, .. } => Some(text.as_str()),
                UserInput::Image { .. }
                | UserInput::LocalImage { .. }
                | UserInput::Audio { .. }
                | UserInput::LocalAudio { .. }
                | UserInput::Skill { .. }
                | UserInput::Mention { .. } => None,
            },
            _ => None,
        })
        .collect()
}

fn turn_agent_texts(turns: &[codex_app_server_protocol::Turn]) -> Vec<&str> {
    turns
        .iter()
        .flat_map(|turn| &turn.items)
        .filter_map(|item| match item {
            ThreadItem::AgentMessage { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

struct InMemoryThreadStoreId {
    store_id: String,
}

impl Drop for InMemoryThreadStoreId {
    fn drop(&mut self) {
        InMemoryThreadStore::remove_id(&self.store_id);
    }
}

async fn seed_pathless_store_thread(
    store: &InMemoryThreadStore,
    thread_id: codex_protocol::ThreadId,
) -> Result<()> {
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
            history_mode: Default::default(),
            history_base: None,
            subagent_history_start_ordinal: None,
            persistence_mode: Default::default(),
            initial_rollout_ordinal: 0,
            initial_window_id: Uuid::now_v7().to_string(),
            metadata: ThreadPersistenceMetadata {
                cwd: None,
                model_provider: "test-provider".to_string(),
                memory_mode: ThreadMemoryMode::Disabled,
            },
        })
        .await?;
    store
        .append_items(AppendThreadItemsParams {
            thread_id,
            items: store_history_items(),
        })
        .await?;
    store
        .update_thread_metadata(UpdateThreadMetadataParams {
            thread_id,
            patch: ThreadMetadataPatch {
                name: Some(Some("named pathless thread".to_string())),
                ..Default::default()
            },
            include_archived: true,
        })
        .await?;
    Ok(())
}

fn store_history_items() -> Vec<RolloutItem> {
    vec![RolloutItem::EventMsg(EventMsg::UserMessage(
        UserMessageEvent {
            client_id: None,
            message: "history from store".to_string(),
            images: None,
            local_images: Vec::new(),
            audio: Some(vec!["https://example.com/recording.mp3".to_string()]),
            local_audio: vec!["recording.wav".into()],
            text_elements: Vec::new(),
            ..Default::default()
        },
    ))]
}
