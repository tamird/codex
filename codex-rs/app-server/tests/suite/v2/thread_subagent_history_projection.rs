use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::to_response;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SortDirection;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadResumeInitialTurnsPageParams;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::ThreadTurnsListResponse;
use codex_app_server_protocol::Turn;
use codex_app_server_protocol::TurnItemsView;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::items::CollabAgentTool;
use codex_protocol::items::CollabAgentToolCallItem;
use codex_protocol::items::CollabAgentToolCallStatus;
use codex_protocol::items::SubAgentActivityItem;
use codex_protocol::items::TurnItem as CoreTurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::models::BaseInstructions;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentActivityEvent;
use codex_protocol::protocol::SubAgentActivityKind;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use codex_rollout::RolloutItem;
use codex_thread_store::AppendThreadItemsParams;
use codex_thread_store::CreateThreadParams;
use codex_thread_store::FreezeRolloutSegmentParams;
use codex_thread_store::LocalThreadStore;
use codex_thread_store::LocalThreadStoreConfig;
use codex_thread_store::PersistContext;
use codex_thread_store::ReadThreadParams as StoreReadThreadParams;
use codex_thread_store::ThreadPersistenceMetadata;
use codex_thread_store::ThreadStore;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use std::collections::HashMap;
use std::path::PathBuf;
use tempfile::TempDir;
use tokio::time::timeout;
use uuid::Uuid;

#[cfg(windows)]
const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(25);
#[cfg(not(windows))]
const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

const STALE_ACTIVITY_ID: &str = "stale-activity";
const STALE_SPAWN_ID: &str = "stale-spawn";
const MIXED_SPAWN_ID: &str = "mixed-old-spawn";
const BOUNDARY_ACTIVITY_ID: &str = "boundary-activity";
const BOUNDARY_SPAWN_ID: &str = "boundary-spawn";
const RECENT_INTERACTION_ID: &str = "recent-interaction";

#[tokio::test]
async fn legacy_thread_read_filters_subagents_older_than_five_segments() -> Result<()> {
    let fixture = SubagentHistoryFixture::new(ThreadHistoryMode::Legacy).await?;
    let mut app_server = fixture.app_server().await?;

    let turns = read_thread_turns(&mut app_server, fixture.thread_id).await?;
    assert_eq!(turns.len(), 5, "legacy reads retain the newest five turns");
    assert_projection_wiring(&turns, &fixture);
    let paged_turns = read_all_turn_pages(
        &mut app_server,
        fixture.thread_id,
        /*cursor*/ None,
        SortDirection::Asc,
    )
    .await?;
    assert_eq!(paged_turns.len(), 7, "pagination retains turn envelopes");
    assert_projection_wiring(&paged_turns, &fixture);
    let expected_resume_turns = paged_turns.iter().rev().cloned().collect::<Vec<_>>();
    let cold_resume_turns = read_resume_turns(&mut app_server, fixture.thread_id).await?;
    assert_eq!(cold_resume_turns, expected_resume_turns);
    fixture.assert_stored_history_is_unfiltered().await?;
    Ok(())
}

#[tokio::test]
async fn paginated_thread_history_filters_subagents_without_changing_turn_pages() -> Result<()> {
    let fixture = SubagentHistoryFixture::new(ThreadHistoryMode::Paginated).await?;
    let mut app_server = fixture.app_server().await?;

    let read_turns = read_thread_turns(&mut app_server, fixture.thread_id).await?;
    let paged_turns = read_all_turn_pages(
        &mut app_server,
        fixture.thread_id,
        /*cursor*/ None,
        SortDirection::Asc,
    )
    .await?;

    assert_eq!(
        read_turns
            .iter()
            .map(|turn| turn.id.as_str())
            .collect::<Vec<_>>(),
        [
            "turn-0", "turn-1", "turn-2", "turn-3", "turn-4", "turn-5", "turn-6"
        ],
        "paginated reads retain every turn envelope"
    );
    assert_projection_wiring(&read_turns, &fixture);
    assert_eq!(paged_turns, read_turns);
    assert_eq!(paged_turns.len(), 7, "filtering must not remove turns");
    assert_projection_wiring(&paged_turns, &fixture);
    let resume_turns = read_resume_turns(&mut app_server, fixture.thread_id).await?;
    assert_eq!(
        resume_turns,
        paged_turns.iter().rev().cloned().collect::<Vec<_>>()
    );
    fixture.assert_stored_history_is_unfiltered().await?;
    Ok(())
}

struct SubagentHistoryFixture {
    codex_home: TempDir,
    thread_id: ThreadId,
    active_rollout_path: PathBuf,
    history_mode: ThreadHistoryMode,
    boundary_agent_id: ThreadId,
}

impl SubagentHistoryFixture {
    async fn new(history_mode: ThreadHistoryMode) -> Result<Self> {
        let codex_home = TempDir::new()?;
        MockResponsesConfig::new("http://127.0.0.1:1")
            .disable_feature(codex_features::Feature::BackgroundPaginatedRolloutMigration)
            .write(codex_home.path())?;
        let thread_id = ThreadId::new();
        let stale_agent_id = ThreadId::new();
        let recent_agent_id = ThreadId::new();
        let boundary_agent_id = ThreadId::new();
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
                source: SessionSource::Cli,
                thread_source: None,
                originator: "subagent-history-projection-test".to_string(),
                base_instructions: BaseInstructions::default(),
                dynamic_tools: Vec::new(),
                selected_capability_roots: Vec::new(),
                multi_agent_version: None,
                history_mode,
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

        for segment_index in 0..7 {
            let turn_id = format!("turn-{segment_index}");
            let mut turn_items = vec![CoreTurnItem::UserMessage(UserMessageItem {
                id: format!("user-{segment_index}"),
                client_id: None,
                content: vec![codex_protocol::user_input::UserInput::Text {
                    text: format!("segment {segment_index}"),
                    text_elements: Vec::new(),
                }],
            })];
            match segment_index {
                0 => turn_items.push(collab_item(
                    "retained-old-spawn",
                    CollabAgentTool::SpawnAgent,
                    thread_id,
                    vec![recent_agent_id],
                )),
                1 => turn_items.extend([
                    subagent_activity_item(STALE_ACTIVITY_ID, stale_agent_id, "stale"),
                    collab_item(
                        STALE_SPAWN_ID,
                        CollabAgentTool::SpawnAgent,
                        thread_id,
                        vec![stale_agent_id],
                    ),
                    collab_item(
                        MIXED_SPAWN_ID,
                        CollabAgentTool::SpawnAgent,
                        thread_id,
                        vec![stale_agent_id, recent_agent_id],
                    ),
                ]),
                2 => turn_items.extend([
                    subagent_activity_item(BOUNDARY_ACTIVITY_ID, boundary_agent_id, "boundary"),
                    collab_item(
                        BOUNDARY_SPAWN_ID,
                        CollabAgentTool::SpawnAgent,
                        thread_id,
                        vec![boundary_agent_id],
                    ),
                ]),
                6 => turn_items.push(collab_item(
                    RECENT_INTERACTION_ID,
                    CollabAgentTool::SendInput,
                    thread_id,
                    vec![recent_agent_id],
                )),
                _ => {}
            }
            let mut rollout_items = Vec::with_capacity(turn_items.len() + 2);
            rollout_items.push(turn_started(&turn_id));
            rollout_items.extend(turn_items.into_iter().map(|item| {
                RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
                    thread_id,
                    turn_id: turn_id.clone(),
                    item,
                    started_at_ms: Some(0),
                    completed_at_ms: 1,
                }))
            }));
            if history_mode == ThreadHistoryMode::Legacy {
                match segment_index {
                    1 => rollout_items.push(subagent_activity_event(
                        STALE_ACTIVITY_ID,
                        stale_agent_id,
                        "stale",
                    )),
                    2 => rollout_items.push(subagent_activity_event(
                        BOUNDARY_ACTIVITY_ID,
                        boundary_agent_id,
                        "boundary",
                    )),
                    _ => {}
                }
            }
            rollout_items.push(turn_completed(&turn_id));
            store
                .append_items(AppendThreadItemsParams {
                    thread_id,
                    items: rollout_items,
                })
                .await?;
            if segment_index < 6 {
                store
                    .freeze_thread_segment(
                        thread_id,
                        FreezeRolloutSegmentParams::rotate(Vec::new()),
                    )
                    .await?;
            }
        }
        let active_rollout_path = store
            .read_thread(StoreReadThreadParams {
                thread_id,
                include_archived: true,
                include_history: false,
            })
            .await?
            .rollout_path
            .expect("persisted thread rollout path");
        store.shutdown_thread(thread_id).await?;

        Ok(Self {
            codex_home,
            thread_id,
            active_rollout_path,
            history_mode,
            boundary_agent_id,
        })
    }

    async fn app_server(&self) -> Result<TestAppServer> {
        TestAppServer::builder()
            .with_codex_home(self.codex_home.path())
            .without_auto_env()
            .build_initialized()
            .await
    }

    async fn assert_stored_history_is_unfiltered(&self) -> Result<()> {
        let items = codex_rollout::materialize_rollout_items(
            self.codex_home.path(),
            self.active_rollout_path.as_path(),
        )
        .await?;
        let stored_item_ids = items
            .iter()
            .filter_map(|item| match item {
                RolloutItem::EventMsg(EventMsg::ItemCompleted(event)) => Some(event.item.id()),
                RolloutItem::EventMsg(EventMsg::SubAgentActivity(event)) => {
                    Some(event.event_id.clone())
                }
                _ => None,
            })
            .collect::<std::collections::HashSet<_>>();
        assert!(stored_item_ids.contains(STALE_ACTIVITY_ID));
        if self.history_mode == ThreadHistoryMode::Paginated {
            assert!(stored_item_ids.contains(STALE_SPAWN_ID));
            assert!(stored_item_ids.contains(MIXED_SPAWN_ID));
        }
        Ok(())
    }
}

async fn read_thread_turns(
    app_server: &mut TestAppServer,
    thread_id: ThreadId,
) -> Result<Vec<Turn>> {
    let request_id = app_server
        .send_thread_read_request(ThreadReadParams {
            thread_id: thread_id.to_string(),
            include_turns: true,
        })
        .await?;
    let ThreadReadResponse { thread, .. } =
        timeout(READ_TIMEOUT, app_server.read_response(request_id)).await??;
    Ok(thread.turns)
}

async fn read_all_turn_pages(
    app_server: &mut TestAppServer,
    thread_id: ThreadId,
    mut cursor: Option<String>,
    sort_direction: SortDirection,
) -> Result<Vec<Turn>> {
    let mut turns = Vec::new();
    for _ in 0..7 {
        let request_id = app_server
            .send_thread_turns_list_request(ThreadTurnsListParams {
                thread_id: thread_id.to_string(),
                cursor: cursor.clone(),
                limit: Some(1),
                sort_direction: Some(sort_direction),
                items_view: Some(TurnItemsView::Full),
            })
            .await?;
        let response: JSONRPCResponse = timeout(
            READ_TIMEOUT,
            app_server.read_stream_until_response_message(RequestId::Integer(request_id)),
        )
        .await??;
        let page = to_response::<ThreadTurnsListResponse>(response)?;
        turns.extend(page.data);
        let Some(next_cursor) = page.next_cursor else {
            return Ok(turns);
        };
        assert_ne!(cursor.as_ref(), Some(&next_cursor));
        cursor = Some(next_cursor);
    }
    anyhow::bail!("pagination exceeded the seven-turn fixture")
}

async fn read_resume_turns(
    app_server: &mut TestAppServer,
    thread_id: ThreadId,
) -> Result<Vec<Turn>> {
    let request_id = app_server
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: thread_id.to_string(),
            exclude_turns: true,
            initial_turns_page: Some(ThreadResumeInitialTurnsPageParams {
                limit: Some(7),
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
    } = timeout(READ_TIMEOUT, app_server.read_response(request_id)).await??;
    assert!(thread.turns.is_empty());
    let page = initial_turns_page.expect("resume returns the requested initial turns page");
    if page.data.len() < 7 {
        assert!(
            page.next_cursor.is_some(),
            "a bounded initial page must advertise the remaining turns"
        );
    }
    let mut turns = page.data;
    if let Some(cursor) = page.next_cursor {
        turns.extend(
            read_all_turn_pages(app_server, thread_id, Some(cursor), SortDirection::Desc).await?,
        );
    }
    Ok(turns)
}

fn assert_projection_wiring(turns: &[Turn], fixture: &SubagentHistoryFixture) {
    let items = turns
        .iter()
        .flat_map(|turn| turn.items.iter())
        .collect::<Vec<_>>();
    assert!(!items.iter().any(|item| item.id() == STALE_ACTIVITY_ID));
    assert!(items.iter().any(|item| item.id() == BOUNDARY_ACTIVITY_ID));

    let boundary_activity = items
        .iter()
        .find(|item| item.id() == BOUNDARY_ACTIVITY_ID)
        .expect("fifth-newest activity remains");
    assert!(matches!(
        boundary_activity,
        ThreadItem::SubAgentActivity { agent_thread_id, .. }
            if agent_thread_id == &fixture.boundary_agent_id.to_string()
    ));
}

fn collab_item(
    id: &str,
    tool: CollabAgentTool,
    sender_thread_id: ThreadId,
    receiver_thread_ids: Vec<ThreadId>,
) -> CoreTurnItem {
    let agents_states = receiver_thread_ids
        .iter()
        .copied()
        .map(|thread_id| (thread_id, AgentStatus::Running))
        .collect::<HashMap<_, _>>();
    CoreTurnItem::CollabAgentToolCall(CollabAgentToolCallItem {
        id: id.to_string(),
        tool,
        status: CollabAgentToolCallStatus::Completed,
        sender_thread_id,
        receiver_thread_ids,
        receiver_agents: Vec::new(),
        prompt: Some(id.to_string()),
        model: None,
        reasoning_effort: None,
        agents_states,
    })
}

fn subagent_activity_item(id: &str, agent_thread_id: ThreadId, path: &str) -> CoreTurnItem {
    CoreTurnItem::SubAgentActivity(SubAgentActivityItem {
        id: id.to_string(),
        kind: SubAgentActivityKind::Started,
        agent_thread_id,
        agent_path: AgentPath::try_from(format!("/root/{path}")).expect("valid test agent path"),
    })
}

fn subagent_activity_event(id: &str, agent_thread_id: ThreadId, path: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::SubAgentActivity(SubAgentActivityEvent {
        event_id: id.to_string(),
        occurred_at_ms: 1,
        agent_thread_id,
        agent_path: AgentPath::try_from(format!("/root/{path}")).expect("valid test agent path"),
        kind: SubAgentActivityKind::Started,
    }))
}

fn turn_started(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_id: turn_id.to_string(),
        trace_id: None,
        started_at: Some(10),
        model_context_window: None,
        collaboration_mode_kind: Default::default(),
    }))
}

fn turn_completed(turn_id: &str) -> RolloutItem {
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
