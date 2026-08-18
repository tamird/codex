use rmcp::model::ReadResourceRequestParams;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use codex_config::McpServerConfig;
use codex_config::McpServerTransportConfig;
use codex_core::StartThreadOptions;
use codex_core::TurnInputRequest;
use codex_core::config::Config;
use codex_features::Feature;
use codex_history::InitialHistory;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::ElicitationAction;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::user_input::UserInput;
use codex_utils_path_uri::LegacyAppPathString;
use core_test_support::process::process_is_alive;
use core_test_support::process::wait_for_pid_file;
use core_test_support::process::wait_for_process_exit;
use core_test_support::responses::ResponseMock;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call_with_namespace;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::ev_tool_search_call;
use core_test_support::responses::mount_sse_once_match;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::stdio_server_bin;
use core_test_support::test_codex::test_codex;
use serde_json::Value;
use serde_json::json;
use wiremock::MockServer;
use wiremock::Request;

const MCP_SERVER_NAME: &str = "agent_tree";
const SPAWN_CALL_ID: &str = "spawn-shared-mcp-child";
const ROOT_PROMPT: &str = "spawn one child to inspect its MCP tools";
const CHILD_PROMPT: &str = "inspect the MCP tools available to this child";
const MULTI_AGENT_V1_NAMESPACE: &str = "multi_agent_v1";
const MULTI_AGENT_V2_NAMESPACE: &str = "collaboration";
const MEMO_URI: &str = "memo://codex/example-note";

fn configure_mcp(config: &mut Config, command: String) {
    configure_mcp_with_env(config, command, HashMap::new());
}

fn configure_mcp_with_env(
    config: &mut Config,
    command: String,
    extra_env: HashMap<String, String>,
) {
    configure_mcp_with_env_and_timeout(config, command, extra_env, /*tool_timeout_sec*/ None);
}

fn configure_mcp_with_env_and_timeout(
    config: &mut Config,
    command: String,
    extra_env: HashMap<String, String>,
    tool_timeout_sec: Option<Duration>,
) {
    config
        .features
        .disable(Feature::MultiAgentV2)
        .expect("the default fixture calls multi_agent_v1 tools");
    config
        .features
        .enable(Feature::Collab)
        .expect("test config should enable collaboration");
    config
        .features
        .enable(Feature::AuthElicitation)
        .expect("test config should enable MCP elicitation");
    let mut servers = config.mcp_servers.get().clone();
    let mut env = HashMap::from([
        (
            "MCP_TEST_DYNAMIC_SERVER_METADATA".to_string(),
            "1".to_string(),
        ),
        ("MCP_TEST_AGENT_TREE_TOOLS".to_string(), "1".to_string()),
    ]);
    env.extend(extra_env);
    servers.insert(
        MCP_SERVER_NAME.to_string(),
        McpServerConfig {
            transport: McpServerTransportConfig::Stdio {
                command,
                args: Vec::new(),
                env: Some(env),
                env_vars: Vec::new(),
                cwd: Some(LegacyAppPathString::from_path(&config.cwd)),
            },
            environment_id: codex_config::DEFAULT_MCP_SERVER_ENVIRONMENT_ID.to_string(),
            enabled: true,
            required: true,
            auth: Default::default(),
            supports_parallel_tool_calls: false,
            omit_tools_from: None,
            disabled_reason: None,
            startup_timeout_sec: Some(std::time::Duration::from_secs(10)),
            tool_timeout_sec,
            default_tools_approval_mode: None,
            enabled_tools: None,
            disabled_tools: None,
            scopes: None,
            oauth: None,
            oauth_resource: None,
            tools: HashMap::new(),
        },
    );
    config
        .mcp_servers
        .set(servers)
        .expect("test MCP config should be valid");
}

fn process_label(response: &ResponseMock, agent: &str) -> String {
    process_label_from_body(&response.single_request().body_json(), agent)
}

fn process_labels(response: &ResponseMock, agent: &str) -> Vec<String> {
    response
        .requests()
        .iter()
        .map(|request| process_label_from_body(&request.body_json(), agent))
        .collect()
}

fn process_label_from_body(body: &Value, agent: &str) -> String {
    let tools = body.get("tools").unwrap_or(&Value::Null).to_string();
    let marker = "rmcp-test-process-";
    let start = tools.find(marker).unwrap_or_else(|| {
        panic!("{agent} request should describe the test MCP process; tools={tools}")
    });
    let label = &tools[start..];
    let end = label
        .find(|character: char| !character.is_ascii_alphanumeric() && character != '-')
        .unwrap_or(label.len());
    label[..end].to_string()
}

fn body_contains(request: &Request, text: &str) -> bool {
    std::str::from_utf8(&request.body).is_ok_and(|body| body.contains(text))
}

fn has_function_call_output(request: &Request, call_id: &str) -> bool {
    serde_json::from_slice::<Value>(&request.body)
        .ok()
        .and_then(|body| body.get("input").and_then(Value::as_array).cloned())
        .is_some_and(|input| {
            input.iter().any(|item| {
                item.get("type").and_then(Value::as_str) == Some("function_call_output")
                    && item.get("call_id").and_then(Value::as_str) == Some(call_id)
            })
        })
}

async fn wait_for_replacement_pid_file(path: &std::path::Path, old_pid: &str) -> Result<String> {
    Ok(tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(pid) = fs::read_to_string(path)
                && pid != old_pid
            {
                break pid;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?)
}

async fn wait_for_process_exit_after_grace(pid: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        while process_is_alive(pid)? {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Result::<()>::Ok(())
    })
    .await??;
    Ok(())
}

async fn mount_idle_child_responses(
    server: &MockServer,
    root_prompt: &'static str,
    child_prompt: &'static str,
    spawn_call_id: &'static str,
    id_prefix: &str,
) -> Result<()> {
    let spawn_response_id = format!("{id_prefix}-root-spawn");
    let child_response_id = format!("{id_prefix}-child");
    let child_message_id = format!("{id_prefix}-child-message");
    let root_final_id = format!("{id_prefix}-root-final");
    let root_message_id = format!("{id_prefix}-root-message");
    mount_sse_once_match(
        server,
        move |request: &Request| body_contains(request, root_prompt),
        sse(vec![
            ev_response_created(&spawn_response_id),
            ev_function_call_with_namespace(
                spawn_call_id,
                MULTI_AGENT_V1_NAMESPACE,
                "spawn_agent",
                &serde_json::to_string(&json!({ "message": child_prompt }))?,
            ),
            ev_completed(&spawn_response_id),
        ]),
    )
    .await;
    mount_sse_once_match(
        server,
        move |request: &Request| {
            body_contains(request, child_prompt) && !body_contains(request, spawn_call_id)
        },
        sse(vec![
            ev_response_created(&child_response_id),
            ev_assistant_message(&child_message_id, "child ready"),
            ev_completed(&child_response_id),
        ]),
    )
    .await;
    mount_sse_once_match(
        server,
        move |request: &Request| body_contains(request, spawn_call_id),
        sse(vec![
            ev_response_created(&root_final_id),
            ev_assistant_message(&root_message_id, "root ready"),
            ev_completed(&root_final_id),
        ]),
    )
    .await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_and_child_share_one_mcp_server_process() -> Result<()> {
    let server = start_mock_server().await;
    let root_response = mount_sse_once_match(
        &server,
        |request: &Request| {
            body_contains(request, ROOT_PROMPT) && !body_contains(request, SPAWN_CALL_ID)
        },
        sse(vec![
            ev_response_created("root-response"),
            ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                MULTI_AGENT_V1_NAMESPACE,
                "spawn_agent",
                &serde_json::to_string(&json!({ "message": CHILD_PROMPT }))?,
            ),
            ev_completed("root-response"),
        ]),
    )
    .await;
    let child_response = mount_sse_once_match(
        &server,
        |request: &Request| {
            body_contains(request, CHILD_PROMPT) && !body_contains(request, SPAWN_CALL_ID)
        },
        sse(vec![
            ev_response_created("child-response"),
            ev_assistant_message("child-message", "child done"),
            ev_completed("child-response"),
        ]),
    )
    .await;
    let _root_followup = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, SPAWN_CALL_ID),
        sse(vec![
            ev_response_created("root-followup"),
            ev_assistant_message("root-message", "root done"),
            ev_completed("root-followup"),
        ]),
    )
    .await;

    let command = stdio_server_bin()?;
    let test = test_codex()
        .with_config(move |config| configure_mcp(config, command))
        .build(&server)
        .await?;

    test.submit_turn(ROOT_PROMPT).await?;

    assert_eq!(
        process_label(&child_response, "child"),
        process_label(&root_response, "root")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_spawned_descendant_shares_the_root_mcp_server_process() -> Result<()> {
    let server = start_mock_server().await;
    let command = stdio_server_bin()?;
    let test = test_codex()
        .with_config(move |config| configure_mcp(config, command))
        .build(&server)
        .await?;
    let root_result = test
        .codex
        .call_mcp_tool(
            MCP_SERVER_NAME,
            "shared_counter",
            /*arguments*/ None,
            /*meta*/ None,
        )
        .await?;
    let environments = test
        .thread_manager
        .default_environment_selections(&test.config.cwd, &test.config.workspace_roots);
    let child = test
        .thread_manager
        .spawn_subagent(
            test.session_configured.thread_id,
            StartThreadOptions {
                config: test.config.clone(),
                allow_provider_model_fallback: false,
                initial_history: InitialHistory::New,
                history_mode: None,
                session_source: None,
                thread_source: None,
                dynamic_tools: Vec::new(),
                metrics_service_name: None,
                parent_trace: None,
                environments: Some(environments),
                thread_extension_init: Default::default(),
                client_mcp_extensions: Default::default(),
                reserved_thread_id: None,
            },
        )
        .await?;
    let child_result = child
        .thread
        .call_mcp_tool(
            MCP_SERVER_NAME,
            "shared_counter",
            /*arguments*/ None,
            /*meta*/ None,
        )
        .await?;

    let root_pid = root_result
        .structured_content
        .as_ref()
        .and_then(|value| value.get("pid"))
        .and_then(Value::as_u64);
    let child_pid = child_result
        .structured_content
        .as_ref()
        .and_then(|value| value.get("pid"))
        .and_then(Value::as_u64);
    assert_eq!(root_pid, child_pid);

    child.thread.shutdown_and_wait().await?;
    test.codex.shutdown_and_wait().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_idle_initial_startup_does_not_restart_the_server() -> Result<()> {
    let server = start_mock_server().await;
    let temp_dir = tempfile::tempdir()?;
    let pid_file = temp_dir.path().join("startup-cancel.pid");
    let barrier_file = temp_dir.path().join("release-initialize");
    let command = stdio_server_bin()?;
    let pid_file_for_config = pid_file.clone();
    let barrier_file_for_config = barrier_file.clone();
    let test = test_codex()
        .with_config(move |config| {
            configure_mcp_with_env(
                config,
                command,
                HashMap::from([
                    (
                        "MCP_TEST_PID_FILE".to_string(),
                        pid_file_for_config.to_string_lossy().into_owned(),
                    ),
                    (
                        "MCP_TEST_INITIALIZE_BARRIER_FILE".to_string(),
                        barrier_file_for_config.to_string_lossy().into_owned(),
                    ),
                ]),
            );
            let mut servers = config.mcp_servers.get().clone();
            servers
                .get_mut(MCP_SERVER_NAME)
                .expect("test MCP server should be configured")
                .required = false;
            config
                .mcp_servers
                .set(servers)
                .expect("test MCP config should remain valid");
        })
        .build(&server)
        .await?;
    let pid = wait_for_pid_file(&pid_file).await?;

    test.codex.submit(Op::Interrupt).await?;
    wait_for_process_exit_after_grace(&pid).await?;
    if fs::read_to_string(&pid_file).ok().as_deref() == Some(pid.as_str()) {
        fs::remove_file(&pid_file)?;
    }
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        !pid_file.exists(),
        "cancelling idle startup must not launch a replacement process"
    );

    fs::write(&barrier_file, b"release")?;
    test.codex.shutdown_and_wait().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_agent_v2_child_shares_the_root_mcp_server_process() -> Result<()> {
    const ROOT: &str = "spawn one V2 child to inspect its MCP tools";
    const CHILD: &str = "inspect MCP tools as a V2 child";
    const SPAWN: &str = "spawn-v2-shared-mcp-child";

    let server = start_mock_server().await;
    let root_response = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, ROOT) && !body_contains(request, SPAWN),
        sse(vec![
            ev_response_created("v2-root-response"),
            ev_function_call_with_namespace(
                SPAWN,
                MULTI_AGENT_V2_NAMESPACE,
                "spawn_agent",
                &serde_json::to_string(&json!({
                    "message": CHILD,
                    "task_name": "mcp-child",
                }))?,
            ),
            ev_completed("v2-root-response"),
        ]),
    )
    .await;
    let child_response = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, CHILD) && !body_contains(request, SPAWN),
        sse(vec![
            ev_response_created("v2-child-response"),
            ev_assistant_message("v2-child-message", "child done"),
            ev_completed("v2-child-response"),
        ]),
    )
    .await;
    mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, SPAWN),
        sse(vec![
            ev_response_created("v2-root-followup"),
            ev_assistant_message("v2-root-message", "root done"),
            ev_completed("v2-root-followup"),
        ]),
    )
    .await;

    let command = stdio_server_bin()?;
    let test = test_codex()
        .with_model("koffing")
        .with_config(move |config| {
            configure_mcp(config, command);
            config
                .features
                .enable(Feature::MultiAgentV2)
                .expect("test config should enable multi-agent V2");
        })
        .build(&server)
        .await?;
    test.submit_turn(ROOT).await?;

    assert_eq!(
        process_label(&child_response, "V2 child"),
        process_label(&root_response, "root")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_direct_children_share_the_root_mcp_process() -> Result<()> {
    const ROOT: &str = "spawn two direct children to inspect their MCP tools";
    const FIRST_CHILD: &str = "inspect MCP tools as the first direct child";
    const SECOND_CHILD: &str = "inspect MCP tools as the second direct child";
    const FIRST_SPAWN: &str = "spawn-first-shared-mcp-child";
    const SECOND_SPAWN: &str = "spawn-second-shared-mcp-child";

    let server = start_mock_server().await;
    let root_response = mount_sse_once_match(
        &server,
        |request: &Request| {
            body_contains(request, ROOT)
                && !body_contains(request, FIRST_SPAWN)
                && !body_contains(request, SECOND_SPAWN)
        },
        sse(vec![
            ev_response_created("multi-child-root-response"),
            ev_function_call_with_namespace(
                FIRST_SPAWN,
                MULTI_AGENT_V1_NAMESPACE,
                "spawn_agent",
                &serde_json::to_string(&json!({ "message": FIRST_CHILD }))?,
            ),
            ev_function_call_with_namespace(
                SECOND_SPAWN,
                MULTI_AGENT_V1_NAMESPACE,
                "spawn_agent",
                &serde_json::to_string(&json!({ "message": SECOND_CHILD }))?,
            ),
            ev_completed("multi-child-root-response"),
        ]),
    )
    .await;
    let first_child_response = mount_sse_once_match(
        &server,
        |request: &Request| {
            body_contains(request, FIRST_CHILD) && !body_contains(request, FIRST_SPAWN)
        },
        sse(vec![
            ev_response_created("multi-first-child-response"),
            ev_assistant_message("multi-first-child-message", "first child done"),
            ev_completed("multi-first-child-response"),
        ]),
    )
    .await;
    let second_child_response = mount_sse_once_match(
        &server,
        |request: &Request| {
            body_contains(request, SECOND_CHILD) && !body_contains(request, SECOND_SPAWN)
        },
        sse(vec![
            ev_response_created("multi-second-child-response"),
            ev_assistant_message("multi-second-child-message", "second child done"),
            ev_completed("multi-second-child-response"),
        ]),
    )
    .await;
    let _root_followup = mount_sse_once_match(
        &server,
        |request: &Request| {
            body_contains(request, FIRST_SPAWN) && body_contains(request, SECOND_SPAWN)
        },
        sse(vec![
            ev_response_created("multi-child-root-followup"),
            ev_assistant_message("multi-child-root-message", "root done"),
            ev_completed("multi-child-root-followup"),
        ]),
    )
    .await;

    let command = stdio_server_bin()?;
    let test = test_codex()
        .with_config(move |config| configure_mcp(config, command))
        .build(&server)
        .await?;
    test.submit_turn(ROOT).await?;

    let processes = [
        process_labels(&root_response, "root"),
        process_labels(&first_child_response, "first child"),
        process_labels(&second_child_response, "second child"),
    ]
    .into_iter()
    .flatten()
    .collect::<HashSet<_>>();
    assert_eq!(
        processes.len(),
        1,
        "all three agents must share one process"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_descendant_reuses_the_root_mcp_process() -> Result<()> {
    const ROOT: &str = "spawn one direct child for a nested MCP process check";
    const CHILD: &str = "spawn one grandchild for the nested MCP process check";
    const GRANDCHILD: &str = "inspect the nested MCP process";
    const ROOT_SPAWN: &str = "spawn-direct-child";
    const CHILD_SPAWN: &str = "spawn-nested-child";

    let server = start_mock_server().await;
    let root_response = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, ROOT) && !body_contains(request, CHILD),
        sse(vec![
            ev_response_created("nested-root-response"),
            ev_function_call_with_namespace(
                ROOT_SPAWN,
                MULTI_AGENT_V1_NAMESPACE,
                "spawn_agent",
                &serde_json::to_string(&json!({ "message": CHILD }))?,
            ),
            ev_completed("nested-root-response"),
        ]),
    )
    .await;
    let child_response = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, CHILD) && !body_contains(request, ROOT_SPAWN),
        sse(vec![
            ev_response_created("nested-child-response"),
            ev_function_call_with_namespace(
                CHILD_SPAWN,
                MULTI_AGENT_V1_NAMESPACE,
                "spawn_agent",
                &serde_json::to_string(&json!({ "message": GRANDCHILD }))?,
            ),
            ev_completed("nested-child-response"),
        ]),
    )
    .await;
    let grandchild_response = mount_sse_once_match(
        &server,
        |request: &Request| {
            body_contains(request, GRANDCHILD) && !body_contains(request, CHILD_SPAWN)
        },
        sse(vec![
            ev_response_created("nested-grandchild-response"),
            ev_assistant_message("nested-grandchild-message", "grandchild done"),
            ev_completed("nested-grandchild-response"),
        ]),
    )
    .await;
    let _child_followup = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, CHILD_SPAWN),
        sse(vec![
            ev_response_created("nested-child-followup"),
            ev_assistant_message("nested-child-final", "child done"),
            ev_completed("nested-child-followup"),
        ]),
    )
    .await;
    let _root_followup = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, ROOT_SPAWN),
        sse(vec![
            ev_response_created("nested-root-followup"),
            ev_assistant_message("nested-root-final", "root done"),
            ev_completed("nested-root-followup"),
        ]),
    )
    .await;

    let command = stdio_server_bin()?;
    let test = test_codex()
        .with_config(move |config| configure_mcp(config, command))
        .build(&server)
        .await?;
    test.submit_turn(ROOT).await?;

    let processes = [
        process_labels(&root_response, "root"),
        process_labels(&child_response, "child"),
        process_labels(&grandchild_response, "grandchild"),
    ]
    .into_iter()
    .flatten()
    .collect::<HashSet<_>>();
    assert_eq!(
        processes.len(),
        1,
        "all three agents must share one process"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrelated_roots_do_not_share_an_mcp_process() -> Result<()> {
    const FIRST_ROOT: &str = "inspect MCP tools from unrelated root one";
    const SECOND_ROOT: &str = "inspect MCP tools from unrelated root two";

    let server = start_mock_server().await;
    let first_response = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, FIRST_ROOT),
        sse(vec![
            ev_response_created("first-unrelated-root"),
            ev_assistant_message("first-unrelated-message", "done"),
            ev_completed("first-unrelated-root"),
        ]),
    )
    .await;
    let second_response = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, SECOND_ROOT),
        sse(vec![
            ev_response_created("second-unrelated-root"),
            ev_assistant_message("second-unrelated-message", "done"),
            ev_completed("second-unrelated-root"),
        ]),
    )
    .await;
    let command = stdio_server_bin()?;
    let first = test_codex()
        .with_config({
            let command = command.clone();
            move |config| configure_mcp(config, command)
        })
        .build(&server)
        .await?;
    let second = test_codex()
        .with_config(move |config| configure_mcp(config, command))
        .build(&server)
        .await?;

    first.submit_turn(FIRST_ROOT).await?;
    second.submit_turn(SECOND_ROOT).await?;

    assert_ne!(
        process_label(&first_response, "first root"),
        process_label(&second_response, "second root")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_and_child_observe_one_stateful_mcp_server() -> Result<()> {
    const ROOT: &str = "increment shared MCP state, then spawn a child";
    const CHILD: &str = "increment the same shared MCP state";
    const ROOT_CALL: &str = "root-shared-counter";
    const CHILD_CALL: &str = "child-shared-counter";
    const SPAWN: &str = "spawn-state-child";
    const ROOT_SEARCH: &str = "root-search-shared-counter";
    const CHILD_SEARCH: &str = "child-search-shared-counter";

    let server = start_mock_server().await;
    let _root_search = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, ROOT),
        sse(vec![
            ev_response_created("state-root-search"),
            ev_tool_search_call(
                ROOT_SEARCH,
                &json!({
                    "query": "increment process local shared counter",
                    "limit": 20,
                }),
            ),
            ev_completed("state-root-search"),
        ]),
    )
    .await;
    let _root_call = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, ROOT_SEARCH),
        sse(vec![
            ev_response_created("state-root-call"),
            ev_function_call_with_namespace(ROOT_CALL, "mcp__agent_tree", "shared_counter", "{}"),
            ev_completed("state-root-call"),
        ]),
    )
    .await;
    let _root_spawn = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, ROOT_CALL),
        sse(vec![
            ev_response_created("state-root-spawn"),
            ev_function_call_with_namespace(
                SPAWN,
                MULTI_AGENT_V1_NAMESPACE,
                "spawn_agent",
                &serde_json::to_string(&json!({ "message": CHILD }))?,
            ),
            ev_completed("state-root-spawn"),
        ]),
    )
    .await;
    let _child_search = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, CHILD) && !body_contains(request, SPAWN),
        sse(vec![
            ev_response_created("state-child-search"),
            ev_tool_search_call(
                CHILD_SEARCH,
                &json!({
                    "query": "increment process local shared counter",
                    "limit": 20,
                }),
            ),
            ev_completed("state-child-search"),
        ]),
    )
    .await;
    let _child_call = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, CHILD_SEARCH),
        sse(vec![
            ev_response_created("state-child-call"),
            ev_function_call_with_namespace(CHILD_CALL, "mcp__agent_tree", "shared_counter", "{}"),
            ev_completed("state-child-call"),
        ]),
    )
    .await;
    let child_result = mount_sse_once_match(
        &server,
        |request: &Request| has_function_call_output(request, CHILD_CALL),
        sse(vec![
            ev_response_created("state-child-final"),
            ev_assistant_message("state-child-message", "child done"),
            ev_completed("state-child-final"),
        ]),
    )
    .await;
    let _root_final = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, SPAWN),
        sse(vec![
            ev_response_created("state-root-final"),
            ev_assistant_message("state-root-message", "root done"),
            ev_completed("state-root-final"),
        ]),
    )
    .await;

    let command = stdio_server_bin()?;
    let test = test_codex()
        .with_config(move |config| configure_mcp(config, command))
        .build(&server)
        .await?;
    test.submit_turn(ROOT).await?;

    let child_output = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if let Some(output) = child_result.function_call_output_text(CHILD_CALL) {
                break output;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("child should finish its shared MCP call");
    assert!(
        child_output.contains("\"count\":2") || child_output.contains("\"count\": 2"),
        "child should observe the root's process-local counter state: {child_output}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_receives_elicitation_from_the_shared_mcp_server() -> Result<()> {
    const ROOT: &str = "spawn a child that requests MCP elicitation";
    const CHILD: &str = "request confirmation from the shared MCP server";
    const SPAWN: &str = "spawn-elicitation-child";
    const SEARCH: &str = "search-child-elicitation";
    const CALL: &str = "call-child-elicitation";

    let server = start_mock_server().await;
    let _root_spawn = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, ROOT),
        sse(vec![
            ev_response_created("elicitation-root-spawn"),
            ev_function_call_with_namespace(
                SPAWN,
                MULTI_AGENT_V1_NAMESPACE,
                "spawn_agent",
                &serde_json::to_string(&json!({ "message": CHILD }))?,
            ),
            ev_completed("elicitation-root-spawn"),
        ]),
    )
    .await;
    let _child_search = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, CHILD) && !body_contains(request, SPAWN),
        sse(vec![
            ev_response_created("elicitation-child-search"),
            ev_tool_search_call(
                SEARCH,
                &json!({
                    "query": "request confirmation from the MCP user",
                    "limit": 20,
                }),
            ),
            ev_completed("elicitation-child-search"),
        ]),
    )
    .await;
    let _child_call = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, SEARCH),
        sse(vec![
            ev_response_created("elicitation-child-call"),
            ev_function_call_with_namespace(CALL, "mcp__agent_tree", "request_elicitation", "{}"),
            ev_completed("elicitation-child-call"),
        ]),
    )
    .await;
    let child_result = mount_sse_once_match(
        &server,
        |request: &Request| has_function_call_output(request, CALL),
        sse(vec![
            ev_response_created("elicitation-child-final"),
            ev_assistant_message("elicitation-child-message", "child done"),
            ev_completed("elicitation-child-final"),
        ]),
    )
    .await;
    let _root_final = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, SPAWN),
        sse(vec![
            ev_response_created("elicitation-root-final"),
            ev_assistant_message("elicitation-root-message", "root done"),
            ev_completed("elicitation-root-final"),
        ]),
    )
    .await;

    let command = stdio_server_bin()?;
    let test = test_codex()
        .with_config(move |config| configure_mcp(config, command))
        .build(&server)
        .await?;
    let mut created_threads = test.thread_manager.subscribe_thread_created();
    test.codex
        .start_or_steer_turn(
            TurnInputRequest::user_input(vec![UserInput::Text {
                text: ROOT.to_string(),
                text_elements: Vec::new(),
            }])
            .with_thread_settings(ThreadSettingsOverrides {
                approval_policy: Some(AskForApproval::OnRequest),
                permission_profile: Some(PermissionProfile::Disabled),
                ..Default::default()
            }),
        )
        .await?;
    let child_thread_id = tokio::time::timeout(Duration::from_secs(10), created_threads.recv())
        .await
        .expect("child thread should be created")?;
    let child_thread = test.thread_manager.get_thread(child_thread_id).await?;

    let (owner, event) = tokio::time::timeout(Duration::from_secs(10), async {
        let child_event = async {
            loop {
                let event = child_thread
                    .next_event()
                    .await
                    .expect("child event stream should remain open")
                    .msg;
                if matches!(
                    event,
                    EventMsg::ElicitationRequest(_) | EventMsg::TurnComplete(_)
                ) {
                    break event;
                }
            }
        };
        let root_event = async {
            loop {
                let event = test
                    .codex
                    .next_event()
                    .await
                    .expect("root event stream should remain open")
                    .msg;
                if matches!(event, EventMsg::ElicitationRequest(_)) {
                    break event;
                }
            }
        };
        tokio::select! {
            event = child_event => ("child", event),
            event = root_event => ("root", event),
        }
    })
    .await
    .expect("an MCP elicitation should be delivered");
    let EventMsg::ElicitationRequest(request) = event else {
        panic!(
            "the shared MCP server should route its elicitation to the child thread; owner={owner}, event={event:?}, output={:?}",
            child_result.function_call_output_text(CALL)
        );
    };
    assert_eq!(
        owner, "child",
        "elicitation must not be delivered to the root"
    );
    assert_eq!(request.server_name, MCP_SERVER_NAME);
    assert!(
        matches!(&request.id, codex_protocol::mcp::RequestId::String(id) if id.starts_with("codex-mcp-elicitation-")),
        "expected the MCP server elicitation, got {request:?}"
    );
    child_thread
        .submit(Op::ResolveElicitation {
            server_name: request.server_name,
            request_id: request.id,
            decision: ElicitationAction::Accept,
            content: Some(json!({ "confirmed": true })),
            meta: None,
        })
        .await?;
    let output = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(output) = child_result.function_call_output_text(CALL) {
                break output;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("child should continue after the elicitation response");
    assert!(
        output.contains("accepted"),
        "the child should receive its accepted elicitation result: {output}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_shutdown_cancels_unanswered_elicitation_and_recovers_root() -> Result<()> {
    const ROOT: &str = "spawn a child for an unanswered MCP elicitation";
    const CHILD: &str = "wait for the unanswered MCP elicitation test";
    const SPAWN: &str = "spawn-unanswered-elicitation-child";

    let server = start_mock_server().await;
    mount_idle_child_responses(&server, ROOT, CHILD, SPAWN, "unanswered-elicitation").await?;

    let temp_dir = tempfile::tempdir()?;
    let pid_file = temp_dir.path().join("unanswered-elicitation-mcp.pid");
    let pid_file_for_config = pid_file.clone();
    let command = stdio_server_bin()?;
    let test = test_codex()
        .with_config(move |config| {
            configure_mcp_with_env(
                config,
                command,
                HashMap::from([(
                    "MCP_TEST_PID_FILE".to_string(),
                    pid_file_for_config.to_string_lossy().into_owned(),
                )]),
            );
        })
        .build(&server)
        .await?;
    let mut created_threads = test.thread_manager.subscribe_thread_created();
    test.codex
        .start_or_steer_turn(
            TurnInputRequest::user_input(vec![UserInput::Text {
                text: ROOT.to_string(),
                text_elements: Vec::new(),
            }])
            .with_thread_settings(ThreadSettingsOverrides {
                approval_policy: Some(AskForApproval::OnRequest),
                permission_profile: Some(PermissionProfile::Disabled),
                ..Default::default()
            }),
        )
        .await?;
    let child_thread_id = tokio::time::timeout(Duration::from_secs(10), created_threads.recv())
        .await
        .expect("child thread should be created")?;
    let child_thread = test.thread_manager.get_thread(child_thread_id).await?;
    let old_pid = wait_for_pid_file(&pid_file).await?;

    let pending_call = tokio::spawn({
        let child_thread = Arc::clone(&child_thread);
        async move {
            child_thread
                .call_mcp_tool(
                    MCP_SERVER_NAME,
                    "request_elicitation",
                    /*arguments*/ None,
                    /*meta*/ None,
                )
                .await
        }
    });
    let event = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let event = child_thread
                .next_event()
                .await
                .expect("child event stream should remain open")
                .msg;
            if matches!(event, EventMsg::ElicitationRequest(_)) {
                break event;
            }
        }
    })
    .await
    .expect("the child should receive the MCP elicitation");
    assert!(matches!(event, EventMsg::ElicitationRequest(_)));

    child_thread.shutdown_and_wait().await?;
    let call_result = tokio::time::timeout(Duration::from_secs(2), pending_call)
        .await
        .expect("the pending elicitation call should stop when its child shuts down")
        .expect("the pending call task should not panic");
    assert!(
        call_result.is_err(),
        "an unanswered elicitation must not survive its child session"
    );
    wait_for_process_exit_after_grace(&old_pid)
        .await
        .context("the retired unanswered-elicitation MCP process should exit")?;

    let result = test
        .codex
        .call_mcp_tool(
            MCP_SERVER_NAME,
            "shared_counter",
            /*arguments*/ None,
            /*meta*/ None,
        )
        .await?;
    let replacement_pid = result
        .structured_content
        .as_ref()
        .and_then(|content| content.get("pid"))
        .and_then(Value::as_u64)
        .expect("replacement counter response should include a PID")
        .to_string();
    assert_ne!(replacement_pid, old_pid);
    assert_eq!(
        result
            .structured_content
            .as_ref()
            .and_then(|content| content.get("count"))
            .and_then(Value::as_u64),
        Some(1)
    );

    test.codex.shutdown_and_wait().await?;
    wait_for_process_exit_after_grace(&replacement_pid)
        .await
        .context("the replacement unanswered-elicitation MCP process should exit")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_child_call_recovers_tree_without_routing_late_elicitation_to_root() -> Result<()>
{
    const ROOT: &str = "spawn a child for a delayed shared MCP elicitation";
    const CHILD: &str = "wait for the delayed shared MCP elicitation test";
    const SPAWN: &str = "spawn-delayed-elicitation-child";

    let server = start_mock_server().await;
    let _root_spawn = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, ROOT),
        sse(vec![
            ev_response_created("delayed-elicitation-root-spawn"),
            ev_function_call_with_namespace(
                SPAWN,
                MULTI_AGENT_V1_NAMESPACE,
                "spawn_agent",
                &serde_json::to_string(&json!({ "message": CHILD }))?,
            ),
            ev_completed("delayed-elicitation-root-spawn"),
        ]),
    )
    .await;
    let _child_response = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, CHILD) && !body_contains(request, SPAWN),
        sse(vec![
            ev_response_created("delayed-elicitation-child"),
            ev_assistant_message("delayed-elicitation-child-message", "child ready"),
            ev_completed("delayed-elicitation-child"),
        ]),
    )
    .await;
    let _root_final = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, SPAWN),
        sse(vec![
            ev_response_created("delayed-elicitation-root-final"),
            ev_assistant_message("delayed-elicitation-root-message", "root ready"),
            ev_completed("delayed-elicitation-root-final"),
        ]),
    )
    .await;
    let temp_dir = tempfile::tempdir()?;
    let pid_file = temp_dir.path().join("delayed-elicitation-mcp.pid");
    let pid_file_for_config = pid_file.clone();
    let command = stdio_server_bin()?;
    let test = test_codex()
        .with_config(move |config| {
            configure_mcp_with_env_and_timeout(
                config,
                command,
                HashMap::from([(
                    "MCP_TEST_PID_FILE".to_string(),
                    pid_file_for_config.to_string_lossy().into_owned(),
                )]),
                Some(Duration::from_millis(500)),
            );
        })
        .build(&server)
        .await?;
    let mut created_threads = test.thread_manager.subscribe_thread_created();
    test.submit_turn(ROOT).await?;
    let child_thread_id = tokio::time::timeout(Duration::from_secs(10), created_threads.recv())
        .await
        .expect("child thread should be created")?;
    let child_thread = test.thread_manager.get_thread(child_thread_id).await?;
    let old_pid = wait_for_pid_file(&pid_file).await?;

    let error = child_thread
        .call_mcp_tool(
            MCP_SERVER_NAME,
            "delayed_elicitation",
            Some(json!({ "delay_ms": 1_000 })),
            /*meta*/ None,
        )
        .await
        .expect_err("the delayed elicitation tool should exceed its configured timeout");
    let error_chain = format!("{error:#}");
    assert!(
        error_chain.contains("timed out"),
        "expected an MCP operation timeout, got {error_chain}"
    );
    wait_for_process_exit(&old_pid).await?;

    let root_elicitation = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let event = test
                .codex
                .next_event()
                .await
                .expect("root event stream should remain open")
                .msg;
            if matches!(event, EventMsg::ElicitationRequest(_)) {
                break event;
            }
        }
    })
    .await;
    assert!(
        root_elicitation.is_err(),
        "the root must not receive a late elicitation from the timed-out child call: {root_elicitation:?}"
    );

    let replacement_pid = wait_for_replacement_pid_file(&pid_file, &old_pid).await?;
    let result = test
        .codex
        .call_mcp_tool(
            MCP_SERVER_NAME,
            "shared_counter",
            /*arguments*/ None,
            /*meta*/ None,
        )
        .await?;
    let replacement_pid_number = replacement_pid.parse::<u32>()?;
    assert_eq!(
        result.structured_content,
        Some(json!({ "count": 1, "pid": replacement_pid_number }))
    );

    child_thread.shutdown_and_wait().await?;
    test.codex.shutdown_and_wait().await?;
    wait_for_process_exit(&replacement_pid).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_child_resource_request_recovers_tree_without_late_elicitation() -> Result<()> {
    const ROOT: &str = "spawn a child for a delayed MCP resource request";
    const CHILD: &str = "wait for the delayed MCP resource test";
    const SPAWN: &str = "spawn-delayed-resource-child";

    let server = start_mock_server().await;
    mount_idle_child_responses(&server, ROOT, CHILD, SPAWN, "delayed-resource").await?;

    let temp_dir = tempfile::tempdir()?;
    let pid_file = temp_dir.path().join("delayed-resource-mcp.pid");
    let pid_file_for_config = pid_file.clone();
    let command = stdio_server_bin()?;
    let test = test_codex()
        .with_config(move |config| {
            configure_mcp_with_env_and_timeout(
                config,
                command,
                HashMap::from([
                    (
                        "MCP_TEST_PID_FILE".to_string(),
                        pid_file_for_config.to_string_lossy().into_owned(),
                    ),
                    (
                        "MCP_TEST_DELAYED_RESOURCE_ELICITATION_MS".to_string(),
                        "1000".to_string(),
                    ),
                ]),
                Some(Duration::from_millis(500)),
            );
        })
        .build(&server)
        .await?;
    let mut created_threads = test.thread_manager.subscribe_thread_created();
    test.submit_turn(ROOT).await?;
    let child_thread_id = tokio::time::timeout(Duration::from_secs(10), created_threads.recv())
        .await
        .expect("child thread should be created")?;
    let child_thread = test.thread_manager.get_thread(child_thread_id).await?;
    let old_pid = wait_for_pid_file(&pid_file).await?;

    let error = child_thread
        .read_mcp_resource(MCP_SERVER_NAME, ReadResourceRequestParams::new(MEMO_URI))
        .await
        .expect_err("the delayed resource request should exceed its configured timeout");
    let error_chain = format!("{error:#}");
    assert!(
        error_chain.contains("timed out"),
        "expected an MCP operation timeout, got {error_chain}"
    );
    wait_for_process_exit(&old_pid).await?;

    let root_elicitation = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let event = test
                .codex
                .next_event()
                .await
                .expect("root event stream should remain open")
                .msg;
            if matches!(event, EventMsg::ElicitationRequest(_)) {
                break event;
            }
        }
    })
    .await;
    assert!(
        root_elicitation.is_err(),
        "the root must not receive a late elicitation from the timed-out resource request: {root_elicitation:?}"
    );

    let replacement_pid = wait_for_replacement_pid_file(&pid_file, &old_pid).await?;
    let result = test
        .codex
        .call_mcp_tool(
            MCP_SERVER_NAME,
            "shared_counter",
            /*arguments*/ None,
            /*meta*/ None,
        )
        .await?;
    assert_eq!(
        result.structured_content,
        Some(json!({
            "count": 1,
            "pid": replacement_pid.parse::<u32>()?,
        }))
    );

    child_thread.shutdown_and_wait().await?;
    test.codex.shutdown_and_wait().await?;
    wait_for_process_exit(&replacement_pid).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_server_lives_until_the_last_agent_shuts_down() -> Result<()> {
    const ROOT: &str = "spawn a child for shared MCP lifecycle testing";
    const CHILD: &str = "inspect the shared MCP process before shutdown";
    const SPAWN: &str = "spawn-lifecycle-child";

    let server = start_mock_server().await;
    let root_response = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, ROOT),
        sse(vec![
            ev_response_created("lifecycle-root-spawn"),
            ev_function_call_with_namespace(
                SPAWN,
                MULTI_AGENT_V1_NAMESPACE,
                "spawn_agent",
                &serde_json::to_string(&json!({ "message": CHILD }))?,
            ),
            ev_completed("lifecycle-root-spawn"),
        ]),
    )
    .await;
    let child_response = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, CHILD) && !body_contains(request, SPAWN),
        sse(vec![
            ev_response_created("lifecycle-child"),
            ev_assistant_message("lifecycle-child-message", "child done"),
            ev_completed("lifecycle-child"),
        ]),
    )
    .await;
    let _root_final = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, SPAWN),
        sse(vec![
            ev_response_created("lifecycle-root-final"),
            ev_assistant_message("lifecycle-root-message", "root done"),
            ev_completed("lifecycle-root-final"),
        ]),
    )
    .await;

    let temp_dir = tempfile::tempdir()?;
    let pid_file = temp_dir.path().join("shared-mcp.pid");
    let pid_file_for_config = pid_file.clone();
    let command = stdio_server_bin()?;
    let test = test_codex()
        .with_config(move |config| {
            configure_mcp_with_env(
                config,
                command,
                HashMap::from([(
                    "MCP_TEST_PID_FILE".to_string(),
                    pid_file_for_config.to_string_lossy().into_owned(),
                )]),
            );
        })
        .build(&server)
        .await?;
    let mut created_threads = test.thread_manager.subscribe_thread_created();
    test.submit_turn(ROOT).await?;
    let child_thread_id = tokio::time::timeout(Duration::from_secs(10), created_threads.recv())
        .await
        .expect("child thread should be created")?;
    let child_thread = test.thread_manager.get_thread(child_thread_id).await?;
    let pid = wait_for_pid_file(&pid_file).await?;
    let child_labels = process_labels(&child_response, "child");
    let root_labels = process_labels(&root_response, "root");
    assert!(!child_labels.is_empty() && !root_labels.is_empty());
    // Match recording happens before the route predicate, so concurrent requests can appear in
    // both captures. Every observed request must still use the same physical MCP server.
    assert!(
        child_labels
            .iter()
            .chain(&root_labels)
            .all(|label| label == &root_labels[0]),
        "child={child_labels:?}, root={root_labels:?}"
    );

    child_thread.shutdown_and_wait().await?;
    assert!(
        process_is_alive(&pid)?,
        "the root lease should keep the shared MCP process alive"
    );
    test.codex.shutdown_and_wait().await?;
    wait_for_process_exit(&pid).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crashed_shared_server_can_be_replaced_for_the_tree() -> Result<()> {
    const ROOT: &str = "spawn a child for shared MCP crash recovery";
    const CHILD: &str = "wait for the shared MCP crash test";
    const SPAWN: &str = "spawn-crash-child";

    let server = start_mock_server().await;
    let _root_spawn = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, ROOT),
        sse(vec![
            ev_response_created("crash-root-spawn"),
            ev_function_call_with_namespace(
                SPAWN,
                MULTI_AGENT_V1_NAMESPACE,
                "spawn_agent",
                &serde_json::to_string(&json!({ "message": CHILD }))?,
            ),
            ev_completed("crash-root-spawn"),
        ]),
    )
    .await;
    let _child_response = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, CHILD) && !body_contains(request, SPAWN),
        sse(vec![
            ev_response_created("crash-child"),
            ev_assistant_message("crash-child-message", "child ready"),
            ev_completed("crash-child"),
        ]),
    )
    .await;
    let _root_final = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, SPAWN),
        sse(vec![
            ev_response_created("crash-root-final"),
            ev_assistant_message("crash-root-message", "root ready"),
            ev_completed("crash-root-final"),
        ]),
    )
    .await;
    let temp_dir = tempfile::tempdir()?;
    let pid_file = temp_dir.path().join("crash-mcp.pid");
    let pid_file_for_config = pid_file.clone();
    let command = stdio_server_bin()?;
    let test = test_codex()
        .with_config(move |config| {
            configure_mcp_with_env(
                config,
                command,
                HashMap::from([(
                    "MCP_TEST_PID_FILE".to_string(),
                    pid_file_for_config.to_string_lossy().into_owned(),
                )]),
            );
        })
        .build(&server)
        .await?;
    let mut created_threads = test.thread_manager.subscribe_thread_created();
    test.submit_turn(ROOT).await?;
    let child_thread_id = tokio::time::timeout(Duration::from_secs(10), created_threads.recv())
        .await
        .expect("child thread should be created")?;
    let child_thread = test.thread_manager.get_thread(child_thread_id).await?;
    let crashed_pid = wait_for_pid_file(&pid_file).await?;

    child_thread
        .call_mcp_tool(
            MCP_SERVER_NAME,
            "crash",
            /*arguments*/ None,
            /*meta*/ None,
        )
        .await
        .expect_err("crashing the shared server should fail the child call");
    wait_for_process_exit(&crashed_pid).await?;
    let replacement_pid = wait_for_replacement_pid_file(&pid_file, &crashed_pid).await?;
    assert_ne!(replacement_pid, crashed_pid);
    let result = test
        .codex
        .call_mcp_tool(
            MCP_SERVER_NAME,
            "shared_counter",
            /*arguments*/ None,
            /*meta*/ None,
        )
        .await?;
    let replacement_pid_number = replacement_pid.parse::<u32>()?;
    assert_eq!(
        result.structured_content,
        Some(json!({ "count": 1, "pid": replacement_pid_number }))
    );

    child_thread.shutdown_and_wait().await?;
    test.codex.shutdown_and_wait().await?;
    wait_for_process_exit(&replacement_pid).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_root_refresh_replaces_the_shared_tree_connection() -> Result<()> {
    const ROOT: &str = "spawn a child for shared MCP refresh testing";
    const CHILD: &str = "wait for the shared MCP refresh test";
    const SPAWN: &str = "spawn-refresh-child";
    const REFRESH: &str = "refresh the shared MCP server";

    let server = start_mock_server().await;
    let _root_spawn = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, ROOT),
        sse(vec![
            ev_response_created("tree-refresh-root-spawn"),
            ev_function_call_with_namespace(
                SPAWN,
                MULTI_AGENT_V1_NAMESPACE,
                "spawn_agent",
                &serde_json::to_string(&json!({ "message": CHILD }))?,
            ),
            ev_completed("tree-refresh-root-spawn"),
        ]),
    )
    .await;
    let _child_response = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, CHILD) && !body_contains(request, SPAWN),
        sse(vec![
            ev_response_created("tree-refresh-child"),
            ev_assistant_message("tree-refresh-child-message", "child ready"),
            ev_completed("tree-refresh-child"),
        ]),
    )
    .await;
    let _root_final = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, SPAWN),
        sse(vec![
            ev_response_created("tree-refresh-root-final"),
            ev_assistant_message("tree-refresh-root-message", "root ready"),
            ev_completed("tree-refresh-root-final"),
        ]),
    )
    .await;
    let _refresh_response = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, REFRESH),
        sse(vec![
            ev_response_created("tree-refresh-response"),
            ev_assistant_message("tree-refresh-message", "refreshed"),
            ev_completed("tree-refresh-response"),
        ]),
    )
    .await;

    let command = stdio_server_bin()?;
    let test = test_codex()
        .with_config(move |config| configure_mcp(config, command))
        .build(&server)
        .await?;
    let mut created_threads = test.thread_manager.subscribe_thread_created();
    test.submit_turn(ROOT).await?;
    let child_thread_id = tokio::time::timeout(Duration::from_secs(10), created_threads.recv())
        .await
        .expect("child thread should be created")?;
    let child_thread = test.thread_manager.get_thread(child_thread_id).await?;

    let root_before = test
        .codex
        .call_mcp_tool(
            MCP_SERVER_NAME,
            "shared_counter",
            /*arguments*/ None,
            /*meta*/ None,
        )
        .await?;
    let child_before = child_thread
        .call_mcp_tool(
            MCP_SERVER_NAME,
            "shared_counter",
            /*arguments*/ None,
            /*meta*/ None,
        )
        .await?;
    let old_pid = root_before
        .structured_content
        .as_ref()
        .and_then(|value| value.get("pid"))
        .and_then(Value::as_u64)
        .expect("root counter result should contain a PID");
    assert_eq!(
        root_before.structured_content,
        Some(json!({ "count": 1, "pid": old_pid }))
    );
    assert_eq!(
        child_before.structured_content,
        Some(json!({ "count": 2, "pid": old_pid }))
    );

    test.codex.submit(Op::RefreshMcpServers).await?;
    test.submit_turn(REFRESH).await?;
    let root_after = test
        .codex
        .call_mcp_tool(
            MCP_SERVER_NAME,
            "shared_counter",
            /*arguments*/ None,
            /*meta*/ None,
        )
        .await?;
    let new_pid = root_after
        .structured_content
        .as_ref()
        .and_then(|value| value.get("pid"))
        .and_then(Value::as_u64)
        .expect("refreshed root counter result should contain a PID");
    assert_ne!(new_pid, old_pid);
    assert_eq!(
        root_after.structured_content,
        Some(json!({ "count": 1, "pid": new_pid }))
    );
    let child_after = child_thread
        .call_mcp_tool(
            MCP_SERVER_NAME,
            "shared_counter",
            /*arguments*/ None,
            /*meta*/ None,
        )
        .await?;
    assert_eq!(
        child_after.structured_content,
        Some(json!({ "count": 2, "pid": new_pid }))
    );

    let old_pid = old_pid.to_string();
    let new_pid = new_pid.to_string();
    child_thread.shutdown_and_wait().await?;
    wait_for_process_exit(&old_pid).await?;
    assert!(
        process_is_alive(&new_pid)?,
        "the refreshed root lease should keep the replacement process alive"
    );
    test.codex.shutdown_and_wait().await?;
    wait_for_process_exit(&new_pid).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn calls_from_different_agents_enter_the_shared_server_serially() -> Result<()> {
    const ROOT: &str = "spawn a child for shared MCP serialization";
    const CHILD: &str = "wait for the shared MCP serialization test";
    const SPAWN: &str = "spawn-serialization-child";

    let server = start_mock_server().await;
    let _root_spawn = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, ROOT),
        sse(vec![
            ev_response_created("serialization-root-spawn"),
            ev_function_call_with_namespace(
                SPAWN,
                MULTI_AGENT_V1_NAMESPACE,
                "spawn_agent",
                &serde_json::to_string(&json!({ "message": CHILD }))?,
            ),
            ev_completed("serialization-root-spawn"),
        ]),
    )
    .await;
    let _child_response = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, CHILD) && !body_contains(request, SPAWN),
        sse(vec![
            ev_response_created("serialization-child"),
            ev_assistant_message("serialization-child-message", "child ready"),
            ev_completed("serialization-child"),
        ]),
    )
    .await;
    let _root_final = mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, SPAWN),
        sse(vec![
            ev_response_created("serialization-root-final"),
            ev_assistant_message("serialization-root-message", "root ready"),
            ev_completed("serialization-root-final"),
        ]),
    )
    .await;

    let command = stdio_server_bin()?;
    let test = test_codex()
        .with_config(move |config| configure_mcp(config, command))
        .build(&server)
        .await?;
    let mut created_threads = test.thread_manager.subscribe_thread_created();
    test.submit_turn(ROOT).await?;
    let child_thread_id = tokio::time::timeout(Duration::from_secs(10), created_threads.recv())
        .await
        .expect("child thread should be created")?;
    let child_thread = test.thread_manager.get_thread(child_thread_id).await?;

    let temp_dir = tempfile::tempdir()?;
    let root_entered = temp_dir.path().join("root-entered");
    let child_entered = temp_dir.path().join("child-entered");
    let release_root = temp_dir.path().join("release-root");
    let root_call = tokio::spawn({
        let root = Arc::clone(&test.codex);
        let root_entered = root_entered.clone();
        let release_root = release_root.clone();
        async move {
            root.call_mcp_tool(
                MCP_SERVER_NAME,
                "serialized_probe",
                Some(json!({
                    "label": "root",
                    "entered_file": root_entered,
                    "release_file": release_root,
                })),
                /*meta*/ None,
            )
            .await
        }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !root_entered.is_file() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("root call should enter the MCP server");

    let child_call = tokio::spawn({
        let child_thread = Arc::clone(&child_thread);
        let child_entered = child_entered.clone();
        async move {
            child_thread
                .call_mcp_tool(
                    MCP_SERVER_NAME,
                    "serialized_probe",
                    Some(json!({
                        "label": "child",
                        "entered_file": child_entered,
                    })),
                    /*meta*/ None,
                )
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !child_entered.is_file(),
        "a different session route must not enter while the root route is active"
    );
    fs::write(&release_root, b"release")?;

    let root_result = root_call.await??;
    let child_result = child_call.await??;
    assert_eq!(
        root_result.structured_content,
        Some(json!({ "label": "root", "maximum_active": 1 }))
    );
    assert_eq!(
        child_result.structured_content,
        Some(json!({ "label": "child", "maximum_active": 1 }))
    );
    Ok(())
}
