use anyhow::Context;
use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::write_models_cache;
use codex_app_server::in_process;
use codex_app_server::in_process::InProcessClientHandle;
use codex_app_server::in_process::InProcessServerEvent;
use codex_app_server::in_process::InProcessStartArgs;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::CollabAgentStatus;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::InitializeParams;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::SessionSource;
use codex_app_server_protocol::ThreadListParams;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::UserInput;
use codex_arg0::Arg0DispatchPaths;
use codex_config::CloudConfigBundleLoader;
use codex_config::LoaderOverrides;
use codex_config::NoopThreadConfigLoader;
use codex_core::config::ConfigBuilder;
use codex_exec_server::EnvironmentManager;
use codex_features::Feature;
use codex_feedback::CodexFeedback;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_state::SqliteConfig;
use codex_state::StateRuntime;
use codex_thread_store::DeleteThreadParams as StoreDeleteThreadParams;
use codex_thread_store::InMemoryThreadStore;
use codex_thread_store::ResumeThreadParams;
use codex_thread_store::ThreadMetadataPatch;
use codex_thread_store::ThreadPersistenceMetadata;
use codex_thread_store::ThreadStore;
use codex_thread_store::UpdateThreadMetadataParams;
use codex_utils_absolute_path::test_support::PathExt;
use core_test_support::responses;
use pretty_assertions::assert_eq;
use serde::de::DeserializeOwned;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;

const RPC_TIMEOUT: Duration = Duration::from_secs(30);
const ROOT_PROMPT: &str = "adopt both reconciliation branches";
const ROOT_A_CALL: &str = "reconciliation-root-a";
const ROOT_B_CALL: &str = "reconciliation-root-b";
const BRANCH_A_PROMPT: &str = "spawn the reconciliation leaf for branch a";
const BRANCH_B_PROMPT: &str = "spawn the reconciliation leaf for branch b";
const SETTLE_A_PROMPT: &str = "settle reconciliation branch a";
const SETTLE_B_PROMPT: &str = "settle reconciliation branch b";
const ADOPT_A_PROMPT: &str = "finish adopted reconciliation branch a";
const ADOPT_B_PROMPT: &str = "finish adopted reconciliation branch b";
const LEAF_A_CALL: &str = "reconciliation-leaf-a";
const LEAF_B_CALL: &str = "reconciliation-leaf-b";
const LEAF_A_PROMPT: &str = "finish reconciliation leaf a";
const LEAF_B_PROMPT: &str = "finish reconciliation leaf b";
static NEXT_REQUEST_ID: AtomicI64 = AtomicI64::new(1);

struct InMemoryThreadStoreId {
    store_id: String,
}

impl Drop for InMemoryThreadStoreId {
    fn drop(&mut self) {
        InMemoryThreadStore::remove_id(&self.store_id);
    }
}

struct CurrentTreeFixture {
    _codex_home: TempDir,
    _server: wiremock::MockServer,
    _in_memory_store: InMemoryThreadStoreId,
    app: InProcessClientHandle,
    store: Arc<InMemoryThreadStore>,
    state_db: Arc<StateRuntime>,
    root_thread_id: ThreadId,
    ids_by_path: BTreeMap<String, ThreadId>,
}

async fn mount_branch(
    server: &wiremock::MockServer,
    branch_prompt: &'static str,
    leaf_prompt: &'static str,
    leaf_call_id: &'static str,
    leaf_task_name: &'static str,
) -> Result<()> {
    let leaf_args = serde_json::to_string(&json!({
        "message": leaf_prompt,
        "task_name": leaf_task_name,
        "fork_turns": "none",
    }))?;
    responses::mount_sse_once_match(
        server,
        move |request: &wiremock::Request| {
            let body = String::from_utf8_lossy(&request.body);
            body.contains(branch_prompt) && !body.contains(leaf_call_id)
        },
        responses::sse(vec![
            responses::ev_response_created(&format!("{leaf_call_id}-spawn")),
            responses::ev_function_call_with_namespace(
                leaf_call_id,
                "collaboration",
                "spawn_agent",
                &leaf_args,
            ),
            responses::ev_completed(&format!("{leaf_call_id}-spawn")),
        ]),
    )
    .await;
    responses::mount_sse_once_match(
        server,
        move |request: &wiremock::Request| {
            let body = String::from_utf8_lossy(&request.body);
            body.contains(leaf_prompt) && !body.contains(leaf_call_id)
        },
        responses::sse(vec![
            responses::ev_response_created(&format!("{leaf_call_id}-worker")),
            responses::ev_assistant_message(
                &format!("{leaf_call_id}-worker-message"),
                "leaf complete",
            ),
            responses::ev_completed(&format!("{leaf_call_id}-worker")),
        ]),
    )
    .await;
    responses::mount_sse_once_match(
        server,
        move |request: &wiremock::Request| {
            String::from_utf8_lossy(&request.body).contains(leaf_call_id)
        },
        responses::sse(vec![
            responses::ev_response_created(&format!("{leaf_call_id}-branch-complete")),
            responses::ev_assistant_message(
                &format!("{leaf_call_id}-branch-message"),
                "branch complete",
            ),
            responses::ev_completed(&format!("{leaf_call_id}-branch-complete")),
        ]),
    )
    .await;
    Ok(())
}

async fn mount_adopted_branch_completion(
    server: &wiremock::MockServer,
    thread_id: ThreadId,
    prompt: &'static str,
    response_id: &'static str,
) {
    let thread_id = thread_id.to_string();
    responses::mount_sse_once_match(
        server,
        move |request: &wiremock::Request| {
            String::from_utf8_lossy(&request.body).contains(prompt)
                && request
                    .headers
                    .get("thread-id")
                    .or_else(|| request.headers.get("x-client-request-id"))
                    .and_then(|value| value.to_str().ok())
                    == Some(thread_id.as_str())
        },
        responses::sse(vec![
            responses::ev_response_created(response_id),
            responses::ev_assistant_message(&format!("{response_id}-message"), "branch adopted"),
            responses::ev_completed(response_id),
        ]),
    )
    .await;
}

fn next_request_id() -> RequestId {
    RequestId::Integer(NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed))
}

async fn request<T>(app: &InProcessClientHandle, request: ClientRequest) -> Result<T>
where
    T: DeserializeOwned,
{
    let value = app
        .request(request)
        .await?
        .map_err(|error| anyhow::anyhow!(error.message))?;
    Ok(serde_json::from_value(value)?)
}

async fn start_turn_and_wait(
    app: &mut InProcessClientHandle,
    thread_id: ThreadId,
    prompt: &str,
) -> Result<()> {
    let _: serde_json::Value = request(
        app,
        ClientRequest::TurnStart {
            request_id: next_request_id(),
            params: TurnStartParams {
                thread_id: thread_id.to_string(),
                input: vec![UserInput::Text {
                    text: prompt.to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        },
    )
    .await?;
    timeout(RPC_TIMEOUT, async {
        loop {
            let Some(event) = app.next_event().await else {
                anyhow::bail!("in-process app-server stopped before thread turn completed");
            };
            if let InProcessServerEvent::ServerNotification(notification) = event
                && let ServerNotification::TurnCompleted(completed) = notification.as_ref()
                && completed.thread_id == thread_id.to_string()
            {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await
    .with_context(|| format!("waiting for thread {thread_id} turn to complete"))??;
    Ok(())
}

async fn list_current_members(
    app: &InProcessClientHandle,
    root_thread_id: ThreadId,
) -> Result<BTreeMap<String, ThreadId>> {
    let response: ThreadListResponse = request(
        app,
        ClientRequest::ThreadList {
            request_id: next_request_id(),
            params: ThreadListParams {
                cursor: None,
                limit: Some(200),
                sort_key: None,
                sort_direction: None,
                model_providers: None,
                source_kinds: None,
                archived: None,
                section_id: None,
                project_id: None,
                cwd: None,
                use_state_db_only: true,
                search_term: None,
                parent_thread_id: None,
                ancestor_thread_id: Some(root_thread_id.to_string()),
            },
        },
    )
    .await?;
    assert_eq!(response.next_cursor, None);
    response
        .data
        .into_iter()
        .map(|thread| {
            let path = match thread.source {
                SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    agent_path: Some(path),
                    ..
                }) => path.to_string(),
                source => anyhow::bail!("current member had unexpected source: {source:?}"),
            };
            Ok((path, ThreadId::from_string(&thread.id)?))
        })
        .collect()
}

async fn wait_for_completed_current_tree(
    app: &InProcessClientHandle,
    root_thread_id: ThreadId,
    expected_count: usize,
) -> Result<BTreeMap<String, ThreadId>> {
    let deadline = tokio::time::Instant::now() + RPC_TIMEOUT;
    loop {
        let response: ThreadListResponse = request(
            app,
            ClientRequest::ThreadList {
                request_id: next_request_id(),
                params: ThreadListParams {
                    cursor: None,
                    limit: Some(200),
                    sort_key: None,
                    sort_direction: None,
                    model_providers: None,
                    source_kinds: None,
                    archived: None,
                    section_id: None,
                    project_id: None,
                    cwd: None,
                    use_state_db_only: true,
                    search_term: None,
                    parent_thread_id: None,
                    ancestor_thread_id: Some(root_thread_id.to_string()),
                },
            },
        )
        .await?;
        if response.data.len() == expected_count
            && response
                .data
                .iter()
                .all(|thread| thread.agent_status == Some(CollabAgentStatus::Completed))
        {
            return response
                .data
                .into_iter()
                .map(|thread| {
                    let path = match thread.source {
                        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                            agent_path: Some(path),
                            ..
                        }) => path.to_string(),
                        source => {
                            anyhow::bail!("current member had unexpected source: {source:?}")
                        }
                    };
                    Ok((path, ThreadId::from_string(&thread.id)?))
                })
                .collect();
        }
        if tokio::time::Instant::now() >= deadline {
            let observed = response
                .data
                .iter()
                .map(|thread| {
                    format!(
                        "{}:{:?}:{:?}",
                        thread.id, thread.agent_status, thread.source
                    )
                })
                .collect::<Vec<_>>();
            anyhow::bail!(
                "waiting for {expected_count} completed current agents; observed {observed:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn read_archived_notifications(
    app: &mut InProcessClientHandle,
    count: usize,
) -> Result<HashSet<ThreadId>> {
    timeout(RPC_TIMEOUT, async {
        let mut archived = HashSet::new();
        while archived.len() < count {
            let Some(event) = app.next_event().await else {
                anyhow::bail!("in-process app-server stopped before thread/archived");
            };
            if let InProcessServerEvent::ServerNotification(notification) = event
                && let ServerNotification::ThreadArchived(notification) = notification.as_ref()
            {
                archived.insert(ThreadId::from_string(&notification.thread_id)?);
            }
        }
        Ok::<_, anyhow::Error>(archived)
    })
    .await
    .context("waiting for thread/archived notifications")?
}

async fn read_deleted_notifications(
    app: &mut InProcessClientHandle,
    count: usize,
) -> Result<HashSet<ThreadId>> {
    let mut deleted = HashSet::new();
    let result = timeout(RPC_TIMEOUT, async {
        while deleted.len() < count {
            let Some(event) = app.next_event().await else {
                anyhow::bail!("in-process app-server stopped before thread/deleted");
            };
            if let InProcessServerEvent::ServerNotification(notification) = event
                && let ServerNotification::ThreadDeleted(notification) = notification.as_ref()
            {
                deleted.insert(ThreadId::from_string(&notification.thread_id)?);
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await;
    match result {
        Ok(result) => result?,
        Err(_) => {
            anyhow::bail!("waiting for {count} thread/deleted notifications; observed {deleted:?}")
        }
    }
    Ok(deleted)
}

async fn build_current_tree() -> Result<CurrentTreeFixture> {
    let codex_home = TempDir::new()?;
    let server = responses::start_mock_server().await;
    let store_id = uuid::Uuid::new_v4().to_string();
    MockResponsesConfig::new(&server.uri())
        .with_model("gpt-5.6-sol")
        .enable_feature(Feature::Collab)
        .with_root_config(&format!(
            "experimental_thread_store = {{ type = \"in_memory\", id = \"{store_id}\" }}"
        ))
        .with_extra_config(
            "[features.multi_agent_v2]\nenabled = true\nenable_thread_adoption = true",
        )
        .write(codex_home.path())?;
    write_models_cache(codex_home.path())?;
    let store = InMemoryThreadStore::for_id(store_id.clone());
    let in_memory_store = InMemoryThreadStoreId { store_id };

    mount_branch(
        &server,
        BRANCH_A_PROMPT,
        LEAF_A_PROMPT,
        LEAF_A_CALL,
        "reconciliation_a_leaf",
    )
    .await?;
    mount_branch(
        &server,
        BRANCH_B_PROMPT,
        LEAF_B_PROMPT,
        LEAF_B_CALL,
        "reconciliation_b_leaf",
    )
    .await?;

    let loader_overrides = LoaderOverrides::without_managed_config_for_tests();
    let config = Arc::new(
        ConfigBuilder::default()
            .codex_home(codex_home.path().to_path_buf())
            .fallback_cwd(Some(codex_home.path().to_path_buf()))
            .loader_overrides(loader_overrides.clone())
            .build()
            .await?,
    );
    let state_db = StateRuntime::init(
        SqliteConfig::new_for_testing(codex_home.path().abs()),
        "mock_provider".into(),
    )
    .await?;
    let mut app = in_process::start(InProcessStartArgs {
        arg0_paths: Arg0DispatchPaths::default(),
        config,
        cli_overrides: Vec::new(),
        loader_overrides,
        strict_config: false,
        cloud_config_bundle: CloudConfigBundleLoader::default(),
        thread_config_loader: Arc::new(NoopThreadConfigLoader),
        feedback: CodexFeedback::new(),
        log_db: None,
        state_db: Some(state_db.clone()),
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
    let ThreadStartResponse {
        thread: branch_a, ..
    } = request(
        &app,
        ClientRequest::ThreadStart {
            request_id: next_request_id(),
            params: ThreadStartParams {
                ephemeral: Some(true),
                model: Some("gpt-5.6-sol".to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let ThreadStartResponse {
        thread: branch_b, ..
    } = request(
        &app,
        ClientRequest::ThreadStart {
            request_id: next_request_id(),
            params: ThreadStartParams {
                ephemeral: Some(true),
                model: Some("gpt-5.6-sol".to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let branch_a_id = ThreadId::from_string(&branch_a.id)?;
    let branch_b_id = ThreadId::from_string(&branch_b.id)?;
    start_turn_and_wait(&mut app, branch_a_id, BRANCH_A_PROMPT).await?;
    start_turn_and_wait(&mut app, branch_b_id, BRANCH_B_PROMPT).await?;
    let branch_a_members =
        wait_for_completed_current_tree(&app, branch_a_id, /*expected_count*/ 1).await?;
    let branch_b_members =
        wait_for_completed_current_tree(&app, branch_b_id, /*expected_count*/ 1).await?;
    let branch_a_leaf_id = *branch_a_members
        .values()
        .next()
        .context("missing ephemeral branch a leaf")?;
    let branch_b_leaf_id = *branch_b_members
        .values()
        .next()
        .context("missing ephemeral branch b leaf")?;
    mount_adopted_branch_completion(
        &server,
        branch_a_id,
        SETTLE_A_PROMPT,
        "reconciliation-a-settled",
    )
    .await;
    mount_adopted_branch_completion(
        &server,
        branch_b_id,
        SETTLE_B_PROMPT,
        "reconciliation-b-settled",
    )
    .await;
    start_turn_and_wait(&mut app, branch_a_id, SETTLE_A_PROMPT).await?;
    start_turn_and_wait(&mut app, branch_b_id, SETTLE_B_PROMPT).await?;
    for thread_id in [branch_a_id, branch_a_leaf_id, branch_b_id, branch_b_leaf_id] {
        let rollout_path = codex_home
            .path()
            .join(format!("rollout-2026-08-09T00-00-00-{thread_id}.jsonl"));
        ThreadStore::resume_thread(
            store.as_ref(),
            ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path.clone()),
                history: None,
                include_archived: true,
                metadata: ThreadPersistenceMetadata {
                    cwd: Some(codex_home.path().to_path_buf()),
                    model_provider: "mock_provider".to_string(),
                    memory_mode: ThreadMemoryMode::Enabled,
                },
            },
        )
        .await?;
        ThreadStore::update_thread_metadata(
            store.as_ref(),
            UpdateThreadMetadataParams {
                thread_id,
                patch: ThreadMetadataPatch {
                    rollout_path: Some(rollout_path),
                    ..Default::default()
                },
                include_archived: true,
            },
        )
        .await?;
    }

    mount_adopted_branch_completion(
        &server,
        branch_a_id,
        ADOPT_A_PROMPT,
        "reconciliation-a-adopted",
    )
    .await;
    mount_adopted_branch_completion(
        &server,
        branch_b_id,
        ADOPT_B_PROMPT,
        "reconciliation-b-adopted",
    )
    .await;
    let branch_a_args = serde_json::to_string(&json!({
        "existing_thread_id": branch_a.id,
        "message": ADOPT_A_PROMPT,
        "task_name": "reconciliation_a",
    }))?;
    let branch_b_args = serde_json::to_string(&json!({
        "existing_thread_id": branch_b.id,
        "message": ADOPT_B_PROMPT,
        "task_name": "reconciliation_b",
    }))?;
    let _adoption_call = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| String::from_utf8_lossy(&request.body).contains(ROOT_PROMPT),
        responses::sse(vec![
            responses::ev_response_created("reconciliation-root-adopt"),
            responses::ev_function_call_with_namespace(
                ROOT_A_CALL,
                "frodex",
                "adopt_agent",
                &branch_a_args,
            ),
            responses::ev_function_call_with_namespace(
                ROOT_B_CALL,
                "frodex",
                "adopt_agent",
                &branch_b_args,
            ),
            responses::ev_completed("reconciliation-root-adopt"),
        ]),
    )
    .await;
    let adoption_return = responses::mount_sse_once_match(
        &server,
        |request: &wiremock::Request| {
            let body = String::from_utf8_lossy(&request.body);
            body.contains(ROOT_A_CALL) && body.contains(ROOT_B_CALL)
        },
        responses::sse(vec![
            responses::ev_response_created("reconciliation-root-complete"),
            responses::ev_assistant_message("reconciliation-root-message", "branches adopted"),
            responses::ev_completed("reconciliation-root-complete"),
        ]),
    )
    .await;
    let ThreadStartResponse { thread: root, .. } = request(
        &app,
        ClientRequest::ThreadStart {
            request_id: next_request_id(),
            params: ThreadStartParams {
                model: Some("gpt-5.6-sol".to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let root_thread_id = ThreadId::from_string(&root.id)?;
    start_turn_and_wait(&mut app, root_thread_id, ROOT_PROMPT).await?;
    for (call_id, expected_path) in [
        (ROOT_A_CALL, "/root/reconciliation_a"),
        (ROOT_B_CALL, "/root/reconciliation_b"),
    ] {
        let output = adoption_return
            .function_call_output_text(call_id)
            .with_context(|| format!("missing adoption output for {call_id}"))?;
        let output: serde_json::Value = serde_json::from_str(&output)
            .with_context(|| format!("adoption call {call_id} returned {output:?}"))?;
        anyhow::ensure!(
            output["task_name"] == expected_path,
            "adoption call {call_id} returned {output}"
        );
    }
    let ids_by_path =
        wait_for_completed_current_tree(&app, root_thread_id, /*expected_count*/ 4).await?;

    assert_eq!(
        list_current_members(&app, root_thread_id).await?,
        ids_by_path
    );
    let current_only_leaf_ids = [
        *ids_by_path
            .get("/root/reconciliation_a/reconciliation_a_leaf")
            .context("missing branch a leaf")?,
        *ids_by_path
            .get("/root/reconciliation_b/reconciliation_b_leaf")
            .context("missing branch b leaf")?,
    ];
    for thread_id in current_only_leaf_ids {
        ThreadStore::delete_thread(store.as_ref(), StoreDeleteThreadParams { thread_id }).await?;
    }
    state_db
        .delete_threads_strict(&current_only_leaf_ids)
        .await?;
    assert_eq!(
        list_current_members(&app, root_thread_id).await?,
        ids_by_path
    );

    Ok(CurrentTreeFixture {
        _codex_home: codex_home,
        _server: server,
        _in_memory_store: in_memory_store,
        app,
        store,
        state_db,
        root_thread_id,
        ids_by_path,
    })
}

fn branch_ids(fixture: &CurrentTreeFixture) -> Result<[(ThreadId, ThreadId); 2]> {
    Ok([
        (
            *fixture
                .ids_by_path
                .get("/root/reconciliation_a")
                .context("missing branch a")?,
            *fixture
                .ids_by_path
                .get("/root/reconciliation_a/reconciliation_a_leaf")
                .context("missing branch a leaf")?,
        ),
        (
            *fixture
                .ids_by_path
                .get("/root/reconciliation_b")
                .context("missing branch b")?,
            *fixture
                .ids_by_path
                .get("/root/reconciliation_b/reconciliation_b_leaf")
                .context("missing branch b leaf")?,
        ),
    ])
}

#[path = "thread_membership_reconciliation_partial.rs"]
mod partial;
