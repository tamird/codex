use anyhow::Context;
use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::write_models_cache;
use chrono::DateTime;
use chrono::Utc;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::CollabAgentStatus;
use codex_app_server_protocol::ItemCompletedNotification;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SessionSource;
use codex_app_server_protocol::SortDirection;
use codex_app_server_protocol::SubAgentActivityKind;
use codex_app_server_protocol::ThreadArchiveParams;
use codex_app_server_protocol::ThreadArchiveResponse;
use codex_app_server_protocol::ThreadForkParams;
use codex_app_server_protocol::ThreadForkResponse;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadLoadedListParams;
use codex_app_server_protocol::ThreadLoadedListResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadSortKey;
use codex_app_server_protocol::ThreadSourceKind;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::UserInput;
use codex_features::Feature;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource as CoreSessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_state::DirectionalThreadSpawnEdgeStatus;
use codex_utils_absolute_path::test_support::PathExt;
use core_test_support::responses;
use serde_json::json;
use std::path::Path;
use tempfile::TempDir;
use tokio::time::timeout;

const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct NormalizedCurrentAgent {
    id: String,
    parent_id: String,
    path: String,
    status: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct NormalizedCanonicalAgent {
    path: String,
    status: String,
}

fn model_agent_status_class(status: &serde_json::Value) -> Result<String> {
    if let Some(status) = status.as_str() {
        return Ok(status.to_string());
    }
    let status = status
        .as_object()
        .and_then(|status| {
            ["completed", "errored"]
                .into_iter()
                .find(|variant| status.contains_key(*variant))
        })
        .ok_or_else(|| anyhow::anyhow!("unexpected model agent status: {status}"))?;
    Ok(status.to_string())
}

fn normalize_model_current_agent(agent: &serde_json::Value) -> Result<NormalizedCanonicalAgent> {
    Ok(NormalizedCanonicalAgent {
        path: agent["agent_name"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("model agent is missing agent_name"))?
            .to_string(),
        status: model_agent_status_class(&agent["agent_status"])?,
    })
}

impl NormalizedCurrentAgent {
    fn canonical(&self) -> NormalizedCanonicalAgent {
        NormalizedCanonicalAgent {
            path: self.path.clone(),
            status: self.status.clone(),
        }
    }
}

fn normalize_app_current_agent(
    thread: &codex_app_server_protocol::Thread,
) -> Result<NormalizedCurrentAgent> {
    let path = match &thread.source {
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            agent_path: Some(agent_path),
            ..
        }) => agent_path.to_string(),
        source => anyhow::bail!("unexpected relation member source: {source:?}"),
    };
    let status = match thread
        .agent_status
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("relation member is missing agentStatus"))?
    {
        CollabAgentStatus::PendingInit => "pending_init",
        CollabAgentStatus::Running => "running",
        CollabAgentStatus::Interrupted => "interrupted",
        CollabAgentStatus::Completed => "completed",
        CollabAgentStatus::Errored => "errored",
        CollabAgentStatus::Shutdown => "shutdown",
        CollabAgentStatus::NotFound => "not_found",
    };
    Ok(NormalizedCurrentAgent {
        id: thread.id.clone(),
        parent_id: thread
            .parent_thread_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("relation member is missing parentThreadId"))?,
        path,
        status: status.to_string(),
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "the test helper mirrors one model tool invocation"
)]
async fn invoke_model_tool(
    app: &mut TestAppServer,
    server: &wiremock::MockServer,
    thread_id: &str,
    prompt: &str,
    call_id: &str,
    namespace: &str,
    tool_name: &str,
    arguments: serde_json::Value,
) -> Result<serde_json::Value> {
    let arguments = serde_json::to_string(&arguments)?;
    let prompt_match = prompt.to_string();
    responses::mount_sse_once_match(
        server,
        move |request: &wiremock::Request| {
            String::from_utf8_lossy(&request.body).contains(prompt_match.as_str())
        },
        responses::sse(vec![
            responses::ev_response_created(&format!("{call_id}-request")),
            responses::ev_function_call_with_namespace(call_id, namespace, tool_name, &arguments),
            responses::ev_completed(&format!("{call_id}-request")),
        ]),
    )
    .await;
    let call_id_match = call_id.to_string();
    let result_request = responses::mount_sse_once_match(
        server,
        move |request: &wiremock::Request| {
            String::from_utf8_lossy(&request.body).contains(call_id_match.as_str())
        },
        responses::sse(vec![
            responses::ev_response_created(&format!("{call_id}-complete")),
            responses::ev_assistant_message(
                &format!("{call_id}-message"),
                &format!("completed {tool_name}"),
            ),
            responses::ev_completed(&format!("{call_id}-complete")),
        ]),
    )
    .await;

    timeout(
        DEFAULT_READ_TIMEOUT,
        app.start_turn_and_wait_for_completion(TurnStartParams {
            thread_id: thread_id.to_string(),
            input: vec![UserInput::Text {
                text: prompt.to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        }),
    )
    .await??;
    let output = result_request
        .function_call_output_text(call_id)
        .with_context(|| format!("missing tool output for {call_id}"))?;
    if output.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    serde_json::from_str(&output).with_context(|| format!("parse tool output for {call_id}"))
}

async fn list_all_model_current_agents(
    app: &mut TestAppServer,
    server: &wiremock::MockServer,
    thread_id: &str,
    label: &str,
) -> Result<Vec<NormalizedCanonicalAgent>> {
    let output = invoke_model_tool(
        app,
        server,
        thread_id,
        &format!("list current agents {label}"),
        &format!("{label}-list-agents"),
        "collaboration",
        "list_agents",
        json!({}),
    )
    .await?;
    let mut agents = output["agents"]
        .as_array()
        .context("list_agents agents must be an array")?
        .iter()
        .map(normalize_model_current_agent)
        .collect::<Result<Vec<_>>>()?;
    agents.retain(|agent| agent.path != "/root");
    agents.sort();
    Ok(agents)
}

async fn init_mcp(codex_home: &Path) -> Result<TestAppServer> {
    TestAppServer::builder()
        .with_codex_home(codex_home)
        .build_initialized()
        .await
}

#[derive(Clone, Copy)]
enum ThreadListRelation {
    DirectChildrenOf(ThreadId),
    DescendantsOf(ThreadId),
}

async fn list_threads_for_relation(
    mcp: &mut TestAppServer,
    relation: ThreadListRelation,
    cursor: Option<String>,
    limit: u32,
    model_providers: Option<Vec<String>>,
    source_kinds: Option<Vec<ThreadSourceKind>>,
) -> Result<ThreadListResponse> {
    list_threads_for_relation_with_archived(
        mcp,
        relation,
        cursor,
        limit,
        model_providers,
        source_kinds,
        /*archived*/ None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn list_threads_for_relation_with_archived(
    mcp: &mut TestAppServer,
    relation: ThreadListRelation,
    cursor: Option<String>,
    limit: u32,
    model_providers: Option<Vec<String>>,
    source_kinds: Option<Vec<ThreadSourceKind>>,
    archived: Option<bool>,
) -> Result<ThreadListResponse> {
    let (parent_thread_id, ancestor_thread_id) = match relation {
        ThreadListRelation::DirectChildrenOf(thread_id) => (Some(thread_id.to_string()), None),
        ThreadListRelation::DescendantsOf(thread_id) => (None, Some(thread_id.to_string())),
    };
    mcp.request(|request_id| ClientRequest::ThreadList {
        request_id,
        params: codex_app_server_protocol::ThreadListParams {
            cursor,
            limit: Some(limit),
            sort_key: None,
            sort_direction: None,
            model_providers,
            source_kinds,
            archived,
            section_id: None,
            project_id: None,
            cwd: None,
            use_state_db_only: true,
            search_term: None,
            parent_thread_id,
            ancestor_thread_id,
        },
    })
    .await
}
fn create_minimal_config(codex_home: &std::path::Path) -> std::io::Result<()> {
    let config_toml = codex_home.join("config.toml");
    std::fs::write(
        config_toml,
        r#"
model = "mock-model"
approval_policy = "never"
"#,
    )
}

#[cfg(test)]
#[path = "thread_list_current_agents_historical.rs"]
mod historical_tests;

#[cfg(test)]
#[path = "thread_list_current_agents_live.rs"]
mod live_tests;

#[cfg(test)]
#[path = "thread_list_current_agents_pagination.rs"]
mod pagination_tests;

#[cfg(test)]
#[path = "thread_list_current_agents_filters.rs"]
mod filter_tests;
