use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::connection_pool::McpConnectionIdentity;
use crate::connection_pool::McpConnectionPool;
use crate::connection_pool::McpConnectionPoolMode;
use crate::connection_pool::McpConnectionStartupIdentity;
use crate::connection_pool::McpPooledClient;
use crate::elicitation::ElicitationRequestManager;
use crate::elicitation::ElicitationRequestRouter;
use crate::mcp::tests::test_elicitation_config;
use crate::request_router::McpSessionRoute;
use crate::rmcp_client::AsyncManagedClient;
use crate::rmcp_client::CodexAppsStartupReconnect;
use crate::rmcp_client::ManagedClient;
use crate::rmcp_client::ManagedClientFuture;
use crate::rmcp_client::StartupOutcomeError;
use crate::runtime::McpRuntimeContext;
use crate::server::EffectiveMcpServer;
use codex_config::McpServerConfig;
use codex_config::McpServerTransportConfig;
use codex_config::types::AuthKeyringBackendKind;
use codex_config::types::OAuthCredentialsStoreMode;
use codex_connectors::ConnectorRuntimeContextKey;
use codex_exec_server_test_support::environment_manager_without_environments;
use codex_login::CodexAuth;
use codex_protocol::mcp::ClientMcpExtensions;
use codex_protocol::mcp::McpServerInfo;
use codex_protocol::mcp::OPENAI_FORM_EXTENSION_ID;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_rmcp_client::Elicitation;
use codex_rmcp_client::ElicitationResponse;
use codex_rmcp_client::InProcessTransportFactory;
use codex_rmcp_client::RmcpClient;
use futures::FutureExt;
use pretty_assertions::assert_eq;
use rmcp::model::ElicitationAction;
use rmcp::model::ElicitationCapability;
use rmcp::model::FormElicitationCapability;
use rmcp::model::RequestId;
use tokio::io::DuplexStream;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

fn config(command: &str) -> McpServerConfig {
    McpServerConfig {
        transport: McpServerTransportConfig::Stdio {
            command: command.to_string(),
            args: Vec::new(),
            env: None,
            env_vars: Vec::new(),
            cwd: None,
        },
        auth: Default::default(),
        environment_id: codex_config::DEFAULT_MCP_SERVER_ENVIRONMENT_ID.to_string(),
        enabled: true,
        required: false,
        supports_parallel_tool_calls: false,
        omit_tools_from: None,
        disabled_reason: None,
        startup_timeout_sec: None,
        tool_timeout_sec: None,
        default_tools_approval_mode: None,
        enabled_tools: None,
        disabled_tools: None,
        scopes: None,
        oauth: None,
        oauth_resource: None,
        tools: HashMap::new(),
    }
}

fn form_extensions() -> ClientMcpExtensions {
    ClientMcpExtensions::new([(OPENAI_FORM_EXTENSION_ID.to_string(), serde_json::json!({}))])
}

fn identity(command: &str, cwd: &str) -> McpConnectionIdentity {
    identity_with(
        config(command),
        cwd,
        &Ok(None),
        ConnectorRuntimeContextKey::personal(
            /*account_id*/ None, /*chatgpt_user_id*/ None,
        ),
        /*has_runtime_auth*/ false,
        &ElicitationCapability::default(),
        ClientMcpExtensions::default(),
    )
}

fn identity_with(
    config: McpServerConfig,
    cwd: &str,
    resolved_environment: &Result<Option<Arc<codex_exec_server::Environment>>, String>,
    connector_context: ConnectorRuntimeContextKey,
    has_runtime_auth: bool,
    elicitation_capability: &ElicitationCapability,
    client_mcp_extensions: ClientMcpExtensions,
) -> McpConnectionIdentity {
    identity_with_catalog_limit(
        config,
        cwd,
        resolved_environment,
        connector_context,
        has_runtime_auth,
        elicitation_capability,
        client_mcp_extensions,
        crate::pagination::MAX_MCP_CATALOG_ITEMS,
    )
}

#[allow(clippy::too_many_arguments)]
fn identity_with_catalog_limit(
    config: McpServerConfig,
    cwd: &str,
    resolved_environment: &Result<Option<Arc<codex_exec_server::Environment>>, String>,
    connector_context: ConnectorRuntimeContextKey,
    has_runtime_auth: bool,
    elicitation_capability: &ElicitationCapability,
    client_mcp_extensions: ClientMcpExtensions,
    catalog_item_limit: usize,
) -> McpConnectionIdentity {
    let server = EffectiveMcpServer::configured(config);
    let runtime_context = McpRuntimeContext::new(
        Arc::new(environment_manager_without_environments()),
        PathBuf::from(cwd),
    );
    let auth = has_runtime_auth.then(|| CodexAuth::from_api_key("test-runtime-auth"));
    let runtime_auth_provider = auth
        .as_ref()
        .map(codex_model_provider::auth_provider_from_auth);
    McpConnectionIdentity::new(
        "test",
        &server,
        /*host_plugin_root*/ None,
        OAuthCredentialsStoreMode::default(),
        AuthKeyringBackendKind::default(),
        resolved_environment,
        &runtime_context,
        runtime_auth_provider.as_ref(),
        auth.as_ref(),
        Some((PathBuf::from("/codex-home"), connector_context)),
        elicitation_capability.clone(),
        client_mcp_extensions,
        /*previous_identity*/ None,
        /*effective_protocol_mode*/ Some(crate::McpProtocolMode::Legacy),
        catalog_item_limit,
    )
}

fn identity_with_auth(auth: &CodexAuth) -> McpConnectionIdentity {
    let server = EffectiveMcpServer::configured(config("server"));
    let runtime_context = McpRuntimeContext::new(
        Arc::new(environment_manager_without_environments()),
        PathBuf::from("/one"),
    );
    let provider = codex_model_provider::auth_provider_from_auth(auth);
    McpConnectionIdentity::new(
        "test",
        &server,
        /*host_plugin_root*/ None,
        OAuthCredentialsStoreMode::default(),
        AuthKeyringBackendKind::default(),
        &Ok(None),
        &runtime_context,
        Some(&provider),
        Some(auth),
        Some((
            PathBuf::from("/codex-home"),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None,
            ),
        )),
        ElicitationCapability::default(),
        ClientMcpExtensions::default(),
        /*previous_identity*/ None,
        /*effective_protocol_mode*/ Some(crate::McpProtocolMode::Legacy),
        crate::pagination::MAX_MCP_CATALOG_ITEMS,
    )
}

fn identity_with_agent_plugin(agent_plugin: bool) -> McpConnectionIdentity {
    let server = EffectiveMcpServer::configured(config("server")).with_agent_plugin(agent_plugin);
    let runtime_context = McpRuntimeContext::new(
        Arc::new(environment_manager_without_environments()),
        PathBuf::from("/one"),
    );
    McpConnectionIdentity::new(
        "test",
        &server,
        /*host_plugin_root*/ None,
        OAuthCredentialsStoreMode::default(),
        AuthKeyringBackendKind::default(),
        &Ok(None),
        &runtime_context,
        /*runtime_auth_provider*/ None,
        /*auth*/ None,
        Some((
            PathBuf::from("/codex-home"),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None,
            ),
        )),
        ElicitationCapability::default(),
        ClientMcpExtensions::default(),
        /*previous_identity*/ None,
        /*effective_protocol_mode*/ Some(crate::McpProtocolMode::Legacy),
        crate::pagination::MAX_MCP_CATALOG_ITEMS,
    )
}

#[tokio::test]
async fn agent_plugin_is_part_of_the_connection_pool_identity() {
    let pool = McpConnectionPool::default();
    let route = route();
    let standard_identity = identity_with_agent_plugin(/*agent_plugin*/ false);
    let agent_plugin_identity = identity_with_agent_plugin(/*agent_plugin*/ true);

    assert!(!standard_identity.has_same_connection_config(&agent_plugin_identity));
    let standard = pool.acquire(
        standard_identity,
        McpConnectionPoolMode::Reuse,
        &route,
        client,
    );
    let agent_plugin = pool.acquire(
        agent_plugin_identity,
        McpConnectionPoolMode::Reuse,
        &route,
        client,
    );

    assert!(!standard.ptr_eq(&agent_plugin));
    assert_ne!(standard.connection_id(), agent_plugin.connection_id());
}

#[tokio::test]
async fn catalog_item_limit_is_part_of_the_connection_pool_identity() {
    let pool = McpConnectionPool::default();
    let route = route();
    let connector_context = ConnectorRuntimeContextKey::personal(
        /*account_id*/ None, /*chatgpt_user_id*/ None,
    );
    let standard_identity = identity_with_catalog_limit(
        config("server"),
        "/one",
        &Ok(None),
        connector_context.clone(),
        /*has_runtime_auth*/ false,
        &ElicitationCapability::default(),
        ClientMcpExtensions::default(),
        crate::pagination::MAX_MCP_CATALOG_ITEMS,
    );
    let elevated_identity = identity_with_catalog_limit(
        config("server"),
        "/one",
        &Ok(None),
        connector_context,
        /*has_runtime_auth*/ false,
        &ElicitationCapability::default(),
        ClientMcpExtensions::default(),
        crate::pagination::MAX_CODEX_APPS_TOOL_CATALOG_ITEMS,
    );

    assert!(!standard_identity.has_same_connection_config(&elevated_identity));
    let standard = pool.acquire(
        standard_identity,
        McpConnectionPoolMode::Reuse,
        &route,
        client,
    );
    let elevated = pool.acquire(
        elevated_identity,
        McpConnectionPoolMode::Reuse,
        &route,
        client,
    );

    assert!(!standard.ptr_eq(&elevated));
    assert_ne!(standard.connection_id(), elevated.connection_id());
}

#[tokio::test]
async fn client_mcp_extensions_are_part_of_the_connection_pool_identity() {
    let pool = McpConnectionPool::default();
    let route = route();
    let connector_context = ConnectorRuntimeContextKey::personal(
        /*account_id*/ None, /*chatgpt_user_id*/ None,
    );
    let default_identity = identity_with(
        config("server"),
        "/one",
        &Ok(None),
        connector_context.clone(),
        /*has_runtime_auth*/ false,
        &ElicitationCapability::default(),
        ClientMcpExtensions::default(),
    );
    let form_identity = identity_with(
        config("server"),
        "/one",
        &Ok(None),
        connector_context,
        /*has_runtime_auth*/ false,
        &ElicitationCapability::default(),
        form_extensions(),
    );

    assert!(!default_identity.has_same_connection_config(&form_identity));
    let default_connection = pool.acquire(
        default_identity,
        McpConnectionPoolMode::Reuse,
        &route,
        client,
    );
    let form_connection = pool.acquire(form_identity, McpConnectionPoolMode::Reuse, &route, client);

    assert!(!default_connection.ptr_eq(&form_connection));
    assert_ne!(
        default_connection.connection_id(),
        form_connection.connection_id()
    );
}

fn route() -> Arc<McpSessionRoute> {
    route_with(AskForApproval::Never, PermissionProfile::default())
}

fn route_with(
    approval_policy: AskForApproval,
    permission_profile: PermissionProfile,
) -> Arc<McpSessionRoute> {
    route_with_events(approval_policy, permission_profile).0
}

fn route_with_events(
    approval_policy: AskForApproval,
    permission_profile: PermissionProfile,
) -> (
    Arc<McpSessionRoute>,
    ElicitationRequestManager,
    async_channel::Receiver<Event>,
) {
    let manager = ElicitationRequestManager::new(
        test_elicitation_config("test", approval_policy, permission_profile),
        /*reviewer*/ None,
        /*lifecycle*/ None,
        ElicitationRequestRouter::default(),
    );
    let (tx, rx) = async_channel::unbounded();
    (
        Arc::new(McpSessionRoute::new(
            "test-submit".to_string(),
            manager.clone(),
            Some(tx),
        )),
        manager,
        rx,
    )
}

fn client(request_router: crate::request_router::McpConnectionRequestRouter) -> AsyncManagedClient {
    client_with_startup(
        request_router,
        async { Err(StartupOutcomeError::Cancelled) }
            .boxed()
            .shared(),
    )
}

fn client_with_startup(
    request_router: crate::request_router::McpConnectionRequestRouter,
    client: ManagedClientFuture,
) -> AsyncManagedClient {
    AsyncManagedClient {
        client,
        is_codex_apps_mcp_server: false,
        cached_server_info: None,
        codex_apps_tools_cache_context: None,
        tool_catalog_cache_context: None,
        startup_complete: Arc::new(AtomicBool::new(true)),
        startup_reconnect: None,
        cancel_token: CancellationToken::new(),
        request_router,
    }
}

struct TestInProcessTransportFactory;

impl InProcessTransportFactory for TestInProcessTransportFactory {
    fn open(&self) -> futures::future::BoxFuture<'static, io::Result<DuplexStream>> {
        async {
            let (client_stream, _server_stream) = tokio::io::duplex(1);
            Ok(client_stream)
        }
        .boxed()
    }
}

async fn test_managed_client(label: &str) -> anyhow::Result<ManagedClient> {
    Ok(ManagedClient {
        client: Arc::new(
            RmcpClient::new_in_process_client(Arc::new(TestInProcessTransportFactory)).await?,
        ),
        server_info: McpServerInfo {
            name: label.to_string(),
            title: None,
            version: "1".to_string(),
            description: None,
            icons: None,
            website_url: None,
        },
        tools: Vec::new(),
        tool_timeout: None,
        server_instructions: None,
        server_supports_sandbox_state_meta_capability: false,
        codex_apps_tools_cache_context: None,
    })
}

fn ready_client(
    request_router: crate::request_router::McpConnectionRequestRouter,
    managed: ManagedClient,
) -> AsyncManagedClient {
    client_with_startup(
        request_router,
        futures::future::ready(Ok(managed)).boxed().shared(),
    )
}

#[tokio::test]
async fn startup_wait_retries_a_superseded_connection_generation() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let root_route = route();
    let replacement_route = route();
    let old_started = Arc::new(Notify::new());
    let release_old = Arc::new(Notify::new());
    let old_started_for_client = Arc::clone(&old_started);
    let release_old_for_client = Arc::clone(&release_old);
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &root_route,
        move |request_router| {
            let old_started = Arc::clone(&old_started_for_client);
            let release_old = Arc::clone(&release_old_for_client);
            client_with_startup(
                request_router,
                async move {
                    old_started.notify_one();
                    release_old.notified().await;
                    Err(StartupOutcomeError::Failed {
                        error: "superseded startup".to_string(),
                        is_authentication_required: false,
                    })
                }
                .boxed()
                .shared(),
            )
        },
    );
    let startup = {
        let lease = lease.clone();
        tokio::spawn(async move { lease.await_current_startup(root_route).await })
    };
    old_started.notified().await;

    let replacement = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Replace,
        &replacement_route,
        |request_router| {
            client_with_startup(
                request_router,
                async {
                    Err(StartupOutcomeError::Failed {
                        error: "current startup".to_string(),
                        is_authentication_required: false,
                    })
                }
                .boxed()
                .shared(),
            )
        },
    );
    let error = match tokio::time::timeout(Duration::from_secs(1), startup).await?? {
        Ok(_) => anyhow::bail!("the current generation should report its startup failure"),
        Err(error) => error,
    };
    release_old.notify_one();
    assert!(error.to_string().contains("current startup"));
    assert!(lease.ptr_eq(&replacement));
    Ok(())
}

#[tokio::test]
async fn cancelling_exclusive_startup_does_not_start_a_replacement() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let route = route();
    let starts = Arc::new(AtomicUsize::new(0));
    let startup_started = Arc::new(Notify::new());
    let starts_for_factory = Arc::clone(&starts);
    let startup_started_for_factory = Arc::clone(&startup_started);
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route,
        move |request_router| {
            starts_for_factory.fetch_add(1, Ordering::AcqRel);
            let startup_started = Arc::clone(&startup_started_for_factory);
            client_with_startup(
                request_router,
                async move {
                    startup_started.notify_one();
                    std::future::pending().await
                }
                .boxed()
                .shared(),
            )
        },
    );
    let startup = {
        let lease = lease.clone();
        tokio::spawn(async move { lease.await_current_startup(route).await })
    };
    startup_started.notified().await;

    startup.abort();
    let error = match startup.await {
        Err(error) => error,
        Ok(_) => anyhow::bail!("startup waiter should be cancelled"),
    };
    assert!(error.is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), async {
        while !lease.is_connection_cancelled() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    tokio::time::sleep(Duration::from_millis(25)).await;

    assert_eq!(starts.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn reconnect_elicitation_uses_the_triggering_session_route() -> anyhow::Result<()> {
    let request_router = crate::request_router::McpConnectionRequestRouter::default();
    let sender = Arc::new(request_router.make_sender("test".to_string()));
    let reconnect_finished = Arc::new(Notify::new());
    let reconnect_finished_for_factory = Arc::clone(&reconnect_finished);
    let reconnect_factory = Arc::new(move || {
        let reconnect_finished = Arc::clone(&reconnect_finished_for_factory);
        let sender = sender.clone();
        async move {
            let _ = sender(
                RequestId::String("reconnect-elicitation".into()),
                Elicitation::OpenAiForm {
                    meta: None,
                    message: "reconnect".to_string(),
                    requested_schema: serde_json::json!({ "type": "object" }),
                },
            )
            .await
            .map_err(StartupOutcomeError::from)?;
            reconnect_finished.notify_one();
            Err(StartupOutcomeError::Failed {
                error: "test reconnect completed".to_string(),
                is_authentication_required: false,
            })
        }
        .boxed()
        .shared()
    });
    let async_client = AsyncManagedClient {
        client: async {
            Err(StartupOutcomeError::Failed {
                error: "initial startup failed".to_string(),
                is_authentication_required: false,
            })
        }
        .boxed()
        .shared(),
        is_codex_apps_mcp_server: true,
        cached_server_info: None,
        codex_apps_tools_cache_context: None,
        tool_catalog_cache_context: None,
        startup_complete: Arc::new(AtomicBool::new(true)),
        startup_reconnect: Some(Arc::new(CodexAppsStartupReconnect::new(
            reconnect_factory,
            "test".to_string(),
            request_router.clone(),
        ))),
        cancel_token: CancellationToken::new(),
        request_router,
    };
    let lease = crate::connection_pool::McpConnectionLease::from(async_client);
    let (route, manager, events) =
        route_with_events(AskForApproval::OnRequest, PermissionProfile::default());

    lease.reconnect_failed_startup(Arc::clone(&route)).await;
    let event = tokio::time::timeout(Duration::from_secs(1), events.recv()).await??;
    let EventMsg::ElicitationRequest(request) = event.msg else {
        anyhow::bail!("expected reconnect elicitation, got {:?}", event.msg);
    };
    let codex_protocol::mcp::RequestId::String(id) = request.id else {
        anyhow::bail!("expected string elicitation id");
    };
    manager
        .resolve(
            request.server_name,
            RequestId::String(id.into()),
            ElicitationResponse {
                action: ElicitationAction::Accept,
                content: Some(serde_json::json!({})),
                meta: None,
            },
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(1), reconnect_finished.notified()).await?;
    Ok(())
}

#[tokio::test]
async fn initial_startup_elicitation_uses_the_starting_session_route() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let (route, manager, events) =
        route_with_events(AskForApproval::OnRequest, PermissionProfile::default());
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route,
        |request_router| {
            let sender = request_router.make_sender("test".to_string());
            client_with_startup(
                request_router,
                async move {
                    let _ = sender(
                        RequestId::String("startup-elicitation".into()),
                        Elicitation::OpenAiForm {
                            meta: None,
                            message: "startup".to_string(),
                            requested_schema: serde_json::json!({ "type": "object" }),
                        },
                    )
                    .await
                    .map_err(StartupOutcomeError::from)?;
                    Err(StartupOutcomeError::Failed {
                        error: "test startup completed".to_string(),
                        is_authentication_required: false,
                    })
                }
                .boxed()
                .shared(),
            )
        },
    );
    let startup = {
        let route = Arc::clone(&route);
        tokio::spawn(async move { lease.await_current_startup(route).await })
    };

    let event = tokio::time::timeout(Duration::from_secs(1), events.recv()).await??;
    let EventMsg::ElicitationRequest(request) = event.msg else {
        anyhow::bail!("expected startup elicitation, got {:?}", event.msg);
    };
    let codex_protocol::mcp::RequestId::String(id) = request.id else {
        anyhow::bail!("expected string elicitation id");
    };
    manager
        .resolve(
            request.server_name,
            RequestId::String(id.into()),
            ElicitationResponse {
                action: ElicitationAction::Accept,
                content: Some(serde_json::json!({})),
                meta: None,
            },
        )
        .await?;
    let error = match startup.await? {
        Ok(_) => anyhow::bail!("test startup should end with its sentinel failure"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("test startup completed"));
    Ok(())
}

#[test]
fn compatible_acquisitions_reuse_one_client() {
    let pool = McpConnectionPool::default();
    let first = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let second = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );

    assert!(first.ptr_eq(&second));
}

#[test]
fn different_server_names_do_not_reuse_one_client() {
    let pool = McpConnectionPool::default();
    let first = pool.acquire_named(
        "first".to_string(),
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let second = pool.acquire_named(
        "second".to_string(),
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );

    assert!(!first.ptr_eq(&second));
}

#[test]
fn incompatible_startup_inputs_do_not_reuse_clients() {
    let pool = McpConnectionPool::default();
    let first = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let different_command = pool.acquire(
        identity("other-server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let different_cwd = pool.acquire(
        identity("server", "/two"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );

    assert!(!first.ptr_eq(&different_command));
    assert!(!first.ptr_eq(&different_cwd));
}

#[test]
fn environment_configuration_is_part_of_connection_identity() {
    let mut first_literal_environment = config("server");
    let mut second_literal_environment = config("server");
    let mut environment_binding = config("server");
    let McpServerTransportConfig::Stdio { env, .. } = &mut first_literal_environment.transport
    else {
        panic!("test server should use stdio");
    };
    *env = Some(HashMap::from([("TOKEN".to_string(), "one".to_string())]));
    let McpServerTransportConfig::Stdio { env, .. } = &mut second_literal_environment.transport
    else {
        panic!("test server should use stdio");
    };
    *env = Some(HashMap::from([("TOKEN".to_string(), "two".to_string())]));
    let McpServerTransportConfig::Stdio { env_vars, .. } = &mut environment_binding.transport
    else {
        panic!("test server should use stdio");
    };
    env_vars.push(codex_config::McpServerEnvVar::Name("TOKEN".to_string()));

    assert!(
        identity_with(
            first_literal_environment,
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None
            ),
            /*has_runtime_auth*/ false,
            &ElicitationCapability::default(),
            ClientMcpExtensions::default(),
        ) != identity_with(
            second_literal_environment,
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None
            ),
            /*has_runtime_auth*/ false,
            &ElicitationCapability::default(),
            ClientMcpExtensions::default(),
        )
    );
    assert!(
        identity("server", "/one")
            != identity_with(
                environment_binding,
                "/one",
                &Ok(None),
                ConnectorRuntimeContextKey::personal(
                    /*account_id*/ None, /*chatgpt_user_id*/ None
                ),
                /*has_runtime_auth*/ false,
                &ElicitationCapability::default(),
                ClientMcpExtensions::default(),
            )
    );
}

#[test]
fn configured_environment_id_is_part_of_connection_identity() {
    let first = config("server");
    let mut second = config("server");
    second.environment_id = "remote".to_string();

    assert!(
        identity_with(
            first,
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None
            ),
            /*has_runtime_auth*/ false,
            &ElicitationCapability::default(),
            ClientMcpExtensions::default(),
        ) != identity_with(
            second,
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None
            ),
            /*has_runtime_auth*/ false,
            &ElicitationCapability::default(),
            ClientMcpExtensions::default(),
        )
    );
}

#[test]
fn equivalent_map_configurations_have_one_connection_identity() {
    let mut first = config("server");
    let mut second = config("server");
    let McpServerTransportConfig::Stdio { env: first_env, .. } = &mut first.transport else {
        panic!("test server should use stdio");
    };
    let McpServerTransportConfig::Stdio {
        env: second_env, ..
    } = &mut second.transport
    else {
        panic!("test server should use stdio");
    };
    *first_env = Some(HashMap::from([
        ("ALPHA".to_string(), "one".to_string()),
        ("BETA".to_string(), "two".to_string()),
    ]));
    *second_env = Some(HashMap::from([
        ("BETA".to_string(), "two".to_string()),
        ("ALPHA".to_string(), "one".to_string()),
    ]));

    assert!(
        identity_with(
            first,
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None
            ),
            /*has_runtime_auth*/ false,
            &ElicitationCapability::default(),
            ClientMcpExtensions::default(),
        ) == identity_with(
            second,
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None
            ),
            /*has_runtime_auth*/ false,
            &ElicitationCapability::default(),
            ClientMcpExtensions::default(),
        )
    );
}

#[test]
fn authorization_and_client_capabilities_are_part_of_connection_identity() {
    let default_capability = ElicitationCapability::default();
    let form_capability = ElicitationCapability::new().with_form(FormElicitationCapability::new());
    let baseline = identity_with(
        config("server"),
        "/one",
        &Ok(None),
        ConnectorRuntimeContextKey::personal(
            /*account_id*/ None, /*chatgpt_user_id*/ None,
        ),
        /*has_runtime_auth*/ false,
        &default_capability,
        ClientMcpExtensions::default(),
    );

    for incompatible in [
        identity_with(
            config("server"),
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None,
            ),
            /*has_runtime_auth*/ true,
            &default_capability,
            ClientMcpExtensions::default(),
        ),
        identity_with(
            config("server"),
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None,
            ),
            /*has_runtime_auth*/ false,
            &form_capability,
            ClientMcpExtensions::default(),
        ),
        identity_with(
            config("server"),
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None,
            ),
            /*has_runtime_auth*/ false,
            &default_capability,
            form_extensions(),
        ),
        identity_with(
            config("server"),
            "/one",
            &Ok(None),
            ConnectorRuntimeContextKey::workspace(
                /*account_id*/ None, /*chatgpt_user_id*/ None,
            ),
            /*has_runtime_auth*/ false,
            &default_capability,
            ClientMcpExtensions::default(),
        ),
    ] {
        assert!(baseline != incompatible);
    }
}

#[test]
fn effective_runtime_credentials_are_part_of_connection_identity() {
    let first = CodexAuth::from_api_key("first");
    let second = CodexAuth::from_api_key("second");

    assert!(identity_with_auth(&first) != identity_with_auth(&second));
}

#[tokio::test]
async fn resolved_execution_environment_is_part_of_connection_identity() {
    let first_environment = Arc::new(codex_exec_server::Environment::default_for_tests());
    let second_environment = Arc::new(codex_exec_server::Environment::default_for_tests());
    let first = Ok(Some(first_environment));
    let second = Ok(Some(second_environment));

    assert!(
        identity_with(
            config("server"),
            "/one",
            &first,
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None
            ),
            /*has_runtime_auth*/ false,
            &ElicitationCapability::default(),
            ClientMcpExtensions::default(),
        ) != identity_with(
            config("server"),
            "/one",
            &second,
            ConnectorRuntimeContextKey::personal(
                /*account_id*/ None, /*chatgpt_user_id*/ None
            ),
            /*has_runtime_auth*/ false,
            &ElicitationCapability::default(),
            ClientMcpExtensions::default(),
        )
    );
}

#[tokio::test]
async fn connection_identity_retains_its_resolved_environment() {
    let environment = Arc::new(codex_exec_server::Environment::default_for_tests());
    let weak = Arc::downgrade(&environment);
    let identity = identity_with(
        config("server"),
        "/one",
        &Ok(Some(Arc::clone(&environment))),
        ConnectorRuntimeContextKey::personal(
            /*account_id*/ None, /*chatgpt_user_id*/ None,
        ),
        /*has_runtime_auth*/ false,
        &ElicitationCapability::default(),
        ClientMcpExtensions::default(),
    );
    drop(environment);

    assert!(weak.upgrade().is_some());
    drop(identity);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn dropping_last_lease_releases_connection_identity() {
    let pool = McpConnectionPool::default();
    let environment = Arc::new(codex_exec_server::Environment::default_for_tests());
    let weak = Arc::downgrade(&environment);
    let identity = identity_with(
        config("server"),
        "/one",
        &Ok(Some(Arc::clone(&environment))),
        ConnectorRuntimeContextKey::personal(
            /*account_id*/ None, /*chatgpt_user_id*/ None,
        ),
        /*has_runtime_auth*/ false,
        &ElicitationCapability::default(),
        ClientMcpExtensions::default(),
    );
    let lease = pool.acquire_named(
        "test".to_string(),
        identity,
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    drop(environment);

    assert!(weak.upgrade().is_some());
    drop(lease);
    assert!(weak.upgrade().is_none());
}

#[test]
fn approval_policy_and_permission_profile_do_not_prevent_reuse() {
    let pool = McpConnectionPool::default();
    let never = route_with(AskForApproval::Never, PermissionProfile::Disabled);
    let on_request = route_with(AskForApproval::OnRequest, PermissionProfile::default());
    let first = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &never,
        client,
    );
    let second = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &on_request,
        client,
    );

    assert!(first.ptr_eq(&second));
}

#[test]
fn reused_connection_refreshes_oauth_contention_for_every_lease() {
    let pool = McpConnectionPool::default();
    let original = identity("server", "/one");
    let mut refreshed = original.clone();
    refreshed.oauth_store_was_contended = true;

    let first = pool.acquire(original, McpConnectionPoolMode::Reuse, &route(), client);
    let second = pool.acquire(refreshed, McpConnectionPoolMode::Reuse, &route(), client);

    assert!(first.ptr_eq(&second));
    assert!(
        first
            .connection_identity()
            .expect("first connection identity")
            .oauth_store_was_contended
    );
    assert!(
        second
            .connection_identity()
            .expect("second connection identity")
            .oauth_store_was_contended
    );
}

#[test]
fn replacement_becomes_preferred_without_revoking_old_lease() {
    let pool = McpConnectionPool::default();
    let old = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let first_generation = old.current_connection_id();
    let replacement = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Replace,
        &route(),
        client,
    );
    let reused = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );

    assert!(old.ptr_eq(&replacement));
    assert!(replacement.ptr_eq(&reused));
    assert_ne!(first_generation, replacement.current_connection_id());
    assert_eq!(
        replacement.current_connection_id(),
        reused.current_connection_id()
    );
    assert!(!old.is_connection_cancelled());
}

#[test]
fn acquisition_rechecks_pending_startup_after_generation_replacement() {
    let pool = McpConnectionPool::default();
    let first_route = route();
    let replacement_route = route();
    let recovered_route = route();
    let first_startup = McpConnectionStartupIdentity {
        protocol_mode: Some(crate::McpProtocolMode::Legacy),
        timeout: Duration::from_secs(30),
    };
    let replacement_startup = McpConnectionStartupIdentity {
        protocol_mode: Some(crate::McpProtocolMode::Legacy),
        timeout: Duration::from_secs(45),
    };
    let pending_factory = |request_router| {
        let pending = client(request_router);
        pending.startup_complete.store(false, Ordering::Release);
        pending
    };
    let first = pool.acquire_named_with_startup_identity(
        "test".to_string(),
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        Some(first_startup),
        &first_route,
        pending_factory,
    );
    let first_generation = first.connection_id();

    let replacement = pool.acquire_named_with_startup_identity(
        "test".to_string(),
        identity("server", "/one"),
        McpConnectionPoolMode::Replace,
        Some(replacement_startup),
        &replacement_route,
        pending_factory,
    );
    let replacement_generation = replacement.connection_id();
    let recovered = pool.acquire_named_with_startup_identity(
        "test".to_string(),
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        Some(first_startup),
        &recovered_route,
        pending_factory,
    );

    assert_ne!(first_generation, replacement_generation);
    assert_ne!(replacement_generation, recovered.connection_id());
    assert_eq!(
        recovered
            .current()
            .expect("recovered lease should be active")
            .startup_identity,
        Some(first_startup)
    );
}

#[tokio::test]
async fn captured_binding_retains_its_exact_generation_across_replacement() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let session_route = route();
    let old_rmcp =
        Arc::new(RmcpClient::new_in_process_client(Arc::new(TestInProcessTransportFactory)).await?);
    let old_managed = ManagedClient {
        client: Arc::clone(&old_rmcp),
        server_info: McpServerInfo {
            name: "old".to_string(),
            title: None,
            version: "1".to_string(),
            description: None,
            icons: None,
            website_url: None,
        },
        tools: Vec::new(),
        tool_timeout: None,
        server_instructions: None,
        server_supports_sandbox_state_meta_capability: false,
        codex_apps_tools_cache_context: None,
    };
    let old = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &session_route,
        move |request_router| ready_client(request_router, old_managed.clone()),
    );
    let old_cancelled = old.current()?.client.cancel_token.clone();
    let (binding, _) = old
        .capture_ready_client_and_tools(
            Arc::clone(&session_route),
            /*catalog_override*/ None,
            /*tool_timeout*/ None,
        )
        .await
        .expect("ready generation should be captured");

    let replacement_rmcp =
        Arc::new(RmcpClient::new_in_process_client(Arc::new(TestInProcessTransportFactory)).await?);
    let replacement_managed = ManagedClient {
        client: replacement_rmcp,
        server_info: McpServerInfo {
            name: "replacement".to_string(),
            title: None,
            version: "1".to_string(),
            description: None,
            icons: None,
            website_url: None,
        },
        tools: Vec::new(),
        tool_timeout: None,
        server_instructions: None,
        server_supports_sandbox_state_meta_capability: false,
        codex_apps_tools_cache_context: None,
    };
    let _replacement = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Replace,
        &session_route,
        move |request_router| ready_client(request_router, replacement_managed.clone()),
    );

    assert!(!old_cancelled.is_cancelled());
    binding
        .run(move |managed| async move {
            assert!(Arc::ptr_eq(&managed.client, &old_rmcp));
            Ok(())
        })
        .await?;
    drop(binding);
    assert!(old_cancelled.is_cancelled());
    Ok(())
}

#[tokio::test]
async fn abandoning_a_bound_operation_retires_the_captured_generation() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let session_route = route();
    let managed = test_managed_client("abandoned").await?;
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &session_route,
        move |request_router| ready_client(request_router, managed.clone()),
    );
    let old_connection_id = lease.connection_id();
    let old_cancelled = lease.connection_cancel_token();
    let (binding, _) = lease
        .capture_ready_client_and_tools(
            Arc::clone(&session_route),
            /*catalog_override*/ None,
            /*tool_timeout*/ None,
        )
        .await
        .expect("ready generation should be captured");
    let operation_started = Arc::new(Notify::new());
    let operation_started_for_task = Arc::clone(&operation_started);
    let operation = tokio::spawn(async move {
        binding
            .run(move |_| async move {
                operation_started_for_task.notify_one();
                std::future::pending::<anyhow::Result<()>>().await
            })
            .await
    });
    operation_started.notified().await;
    operation.abort();
    assert!(
        operation
            .await
            .expect_err("bound operation should be cancelled")
            .is_cancelled()
    );

    tokio::time::timeout(Duration::from_secs(1), async {
        while !old_cancelled.is_cancelled() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_ne!(lease.connection_id(), old_connection_id);
    Ok(())
}

#[test]
fn sequential_replacements_keep_every_lease_on_one_generation() {
    let pool = McpConnectionPool::default();
    let root = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let child = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let first_token = root.connection_cancel_token();

    let refreshed_root = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Replace,
        &route(),
        client,
    );
    let second_token = refreshed_root.connection_cancel_token();
    assert!(first_token.is_cancelled());

    let refreshed_child = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Replace,
        &route(),
        client,
    );
    assert!(second_token.is_cancelled());
    let current = root.connection_id();
    assert_eq!(child.connection_id(), current);
    assert_eq!(refreshed_root.connection_id(), current);
    assert_eq!(refreshed_child.connection_id(), current);
}

#[test]
fn simultaneous_replacements_converge_every_lease_on_one_generation() {
    let pool = McpConnectionPool::default();
    let root = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let child = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let barrier = Arc::new(Barrier::new(2));
    // Both refresh threads must reach the barrier before either is joined.
    #[allow(clippy::needless_collect)]
    let refreshes = (0..2)
        .map(|_| {
            let pool = pool.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                pool.acquire(
                    identity("server", "/one"),
                    McpConnectionPoolMode::Replace,
                    &route(),
                    client,
                )
            })
        })
        .collect::<Vec<_>>();
    let refreshes = refreshes
        .into_iter()
        .map(|refresh| refresh.join().expect("refresh thread should finish"))
        .collect::<Vec<_>>();

    let current = root.connection_id();
    assert_eq!(child.connection_id(), current);
    assert!(
        refreshes
            .iter()
            .all(|refresh| refresh.connection_id() == current)
    );
}

#[tokio::test]
async fn queued_operation_converges_on_the_replacement_generation() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let first_route = route();
    let queued_route = route();
    let replacement_route = route();
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &first_route,
        client,
    );
    let queued_lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &queued_route,
        client,
    );
    let active = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let active_for_operation = Arc::clone(&active);
    let release_for_operation = Arc::clone(&release);
    let first = {
        let lease = lease.clone();
        tokio::spawn(async move {
            lease
                .run(first_route, move |_| async move {
                    active_for_operation.notify_one();
                    release_for_operation.notified().await;
                })
                .await
        })
    };
    active.notified().await;
    let queued = tokio::spawn(async move {
        queued_lease
            .run(queued_route, |client| async move { client.connection_id() })
            .await
    });
    tokio::task::yield_now().await;

    let replacement = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Replace,
        &replacement_route,
        client,
    );
    let replacement_id = replacement.connection_id();
    release.notify_one();

    assert_eq!(queued.await??, replacement_id);
    first.await??;
    Ok(())
}

#[tokio::test]
async fn closing_an_active_route_retires_without_wedging_a_sibling() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let first_route = route();
    let second_route = route();
    let first = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &first_route,
        client,
    );
    let second = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &second_route,
        client,
    );
    let started = Arc::new(tokio::sync::Notify::new());
    let operation = {
        let started = Arc::clone(&started);
        let first_route = Arc::clone(&first_route);
        let operation_lease = first.clone();
        tokio::spawn(async move {
            operation_lease
                .run(first_route, move |_| async move {
                    started.notify_one();
                    std::future::pending::<()>().await;
                })
                .await
        })
    };
    started.notified().await;
    first.unregister_route(&first_route);
    assert!(!first.release());
    assert!(operation.await?.is_err());

    let value = second.run(second_route, |_| async move { 7 }).await?;
    assert_eq!(value, 7);
    Ok(())
}

#[tokio::test]
async fn closing_an_active_route_returns_before_unresponsive_shutdown() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let session_route = route();
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &session_route,
        |request_router| {
            client_with_startup(request_router, std::future::pending().boxed().shared())
        },
    );
    let started = Arc::new(Notify::new());
    let operation = {
        let started = Arc::clone(&started);
        let session_route = Arc::clone(&session_route);
        tokio::spawn(async move {
            lease
                .run(session_route, move |_| async move {
                    started.notify_one();
                    std::future::pending::<()>().await;
                })
                .await
        })
    };
    started.notified().await;
    session_route.close();

    let result = tokio::time::timeout(Duration::from_secs(1), operation).await??;
    assert_eq!(
        result
            .expect_err("the closed route should stop the operation")
            .to_string(),
        "MCP session route closed"
    );
    Ok(())
}

#[tokio::test]
async fn failure_recovery_preserves_the_replaced_generations_factory() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let session_route = route();
    let first_starts = Arc::new(AtomicUsize::new(0));
    let replacement_starts = Arc::new(AtomicUsize::new(0));
    let first_startup = McpConnectionStartupIdentity {
        protocol_mode: Some(crate::McpProtocolMode::Legacy),
        timeout: Duration::from_secs(/*secs*/ 30),
    };
    let replacement_startup = McpConnectionStartupIdentity {
        protocol_mode: Some(crate::McpProtocolMode::Legacy),
        timeout: Duration::from_secs(/*secs*/ 45),
    };
    let lease = pool.acquire_named_with_startup_identity(
        "test".to_string(),
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        Some(first_startup),
        &session_route,
        {
            let starts = Arc::clone(&first_starts);
            move |request_router| {
                starts.fetch_add(/*val*/ 1, Ordering::SeqCst);
                client_with_startup(
                    request_router,
                    async {
                        test_managed_client("first")
                            .await
                            .map_err(StartupOutcomeError::from)
                    }
                    .boxed()
                    .shared(),
                )
            }
        },
    );
    let stale_connection = lease.current()?;
    let replacement = pool.acquire_named_with_startup_identity(
        "test".to_string(),
        identity("server", "/one"),
        McpConnectionPoolMode::Replace,
        Some(replacement_startup),
        &session_route,
        {
            let starts = Arc::clone(&replacement_starts);
            move |request_router| {
                starts.fetch_add(/*val*/ 1, Ordering::SeqCst);
                client_with_startup(
                    request_router,
                    async {
                        test_managed_client("replacement")
                            .await
                            .map_err(StartupOutcomeError::from)
                    }
                    .boxed()
                    .shared(),
                )
            }
        },
    );
    let replacement_connection = replacement.current()?;
    let replacement_client = replacement
        .await_current_startup(Arc::clone(&session_route))
        .await?;

    McpPooledClient {
        connection: stale_connection,
        slot: Arc::downgrade(&lease.inner.slot),
        route: Arc::clone(&session_route),
    }
    .retire_after_failure();
    assert_eq!(lease.connection_id(), replacement_connection.id);

    McpPooledClient {
        connection: Arc::clone(&replacement_connection),
        slot: Arc::downgrade(&lease.inner.slot),
        route: Arc::clone(&session_route),
    }
    .retire_after_failure();
    let recovered = tokio::time::timeout(
        Duration::from_secs(/*secs*/ 5),
        lease.await_current_startup(session_route),
    )
    .await??;
    let recovered_connection = lease.current()?;
    assert_ne!(recovered_connection.id, replacement_connection.id);
    assert!(!Arc::ptr_eq(&recovered.client, &replacement_client.client));
    assert_eq!(
        (
            recovered.server_info,
            recovered_connection.startup_identity,
            first_starts.load(Ordering::SeqCst),
            replacement_starts.load(Ordering::SeqCst),
        ),
        (
            replacement_client.server_info,
            Some(replacement_startup),
            1,
            2,
        ),
    );
    Ok(())
}

#[tokio::test]
async fn failure_retirement_replaces_an_exclusive_session_connection() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let session_route = route();
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &session_route,
        client,
    );
    let old_connection = lease.current()?;
    let old_token = old_connection.client.cancel_token.clone();
    let old_id = lease.connection_id();
    McpPooledClient {
        connection: old_connection,
        slot: Arc::downgrade(&lease.inner.slot),
        route: Arc::clone(&session_route),
    }
    .retire_after_failure();

    assert!(old_token.is_cancelled());
    assert_ne!(lease.connection_id(), old_id);
    let result = lease.run(session_route, |_| async move { 7 }).await?;
    assert_eq!(result, 7);
    Ok(())
}

#[tokio::test]
async fn failure_retirement_does_not_replace_after_the_final_session_releases() {
    let pool = McpConnectionPool::default();
    let session_route = route();
    let create_count = Arc::new(AtomicUsize::new(0));
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &session_route,
        {
            let create_count = Arc::clone(&create_count);
            move |request_router| {
                create_count.fetch_add(1, Ordering::SeqCst);
                client(request_router)
            }
        },
    );
    let old_connection = lease.current().expect("connection should be live");
    let pooled_client = McpPooledClient {
        connection: old_connection,
        slot: Arc::downgrade(&lease.inner.slot),
        route: Arc::clone(&session_route),
    };

    lease.unregister_route(&session_route);
    assert!(lease.release());
    pooled_client.retire_after_failure();

    assert_eq!(create_count.load(Ordering::SeqCst), 1);
}

#[test]
fn session_lease_tracks_sibling_ownership_but_not_operation_clones() {
    let pool = McpConnectionPool::default();
    let first = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    let operation_clone = first.clone();
    assert!(first.is_exclusive());

    let sibling = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    assert!(!first.is_exclusive());
    assert!(!sibling.is_exclusive());

    drop(sibling);
    assert!(first.is_exclusive());
    drop(operation_clone);
    assert!(first.is_exclusive());
}

#[test]
fn repeated_child_acquire_and_release_preserves_one_live_root_connection() {
    let pool = McpConnectionPool::default();
    let root_route = route();
    let root = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &root_route,
        client,
    );
    let connection_id = root.connection_id();

    for _ in 0..100 {
        let child_route = route();
        let child = pool.acquire(
            identity("server", "/one"),
            McpConnectionPoolMode::Reuse,
            &child_route,
            client,
        );
        assert_eq!(child.connection_id(), connection_id);
        child.unregister_route(&child_route);
        assert!(!child.release());
    }

    assert!(root.is_exclusive());
    assert_eq!(root.connection_id(), connection_id);
    assert!(!root.is_connection_cancelled());
}

#[test]
fn last_lease_drop_removes_the_client() {
    let pool = McpConnectionPool::default();
    let cancel_token = {
        let leased = pool.acquire(
            identity("server", "/one"),
            McpConnectionPoolMode::Reuse,
            &route(),
            client,
        );
        leased.connection_cancel_token()
    };
    assert!(cancel_token.is_cancelled());

    let replacement = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        client,
    );
    assert!(!replacement.is_connection_cancelled());
}

#[test]
fn released_but_retained_lease_is_not_reused() {
    let pool = McpConnectionPool::default();
    let create_count = Arc::new(AtomicUsize::new(0));
    let retained = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        {
            let create_count = Arc::clone(&create_count);
            move |request_router| {
                create_count.fetch_add(1, Ordering::SeqCst);
                client(request_router)
            }
        },
    );
    assert!(retained.release());

    let replacement = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &route(),
        {
            let create_count = Arc::clone(&create_count);
            move |request_router| {
                create_count.fetch_add(1, Ordering::SeqCst);
                client(request_router)
            }
        },
    );

    assert_eq!(create_count.load(Ordering::SeqCst), 2);
    assert!(!retained.ptr_eq(&replacement));
}

#[test]
fn simultaneous_acquisition_starts_one_client() {
    const THREADS: usize = 8;
    let pool = McpConnectionPool::default();
    let barrier = Arc::new(Barrier::new(THREADS));
    let create_count = Arc::new(AtomicUsize::new(0));
    // Every acquisition thread must reach the barrier before the first join.
    #[allow(clippy::needless_collect)]
    let handles = (0..THREADS)
        .map(|_| {
            let pool = pool.clone();
            let barrier = Arc::clone(&barrier);
            let create_count = Arc::clone(&create_count);
            std::thread::spawn(move || {
                barrier.wait();
                pool.acquire(
                    identity("server", "/one"),
                    McpConnectionPoolMode::Reuse,
                    &route(),
                    move |request_router| {
                        create_count.fetch_add(1, Ordering::SeqCst);
                        client(request_router)
                    },
                )
            })
        })
        .collect::<Vec<_>>();
    let clients = handles
        .into_iter()
        .map(|handle| handle.join().expect("acquisition thread should finish"))
        .collect::<Vec<_>>();

    assert_eq!(create_count.load(Ordering::SeqCst), 1);
    assert!(clients.iter().all(|client| client.ptr_eq(&clients[0])));
}

#[tokio::test]
async fn cancelling_a_dispatched_request_replaces_the_connection_for_siblings() -> anyhow::Result<()>
{
    let pool = McpConnectionPool::default();
    let root_route = route();
    let child_route = route();
    let create_count = Arc::new(AtomicUsize::new(0));
    let root = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &root_route,
        {
            let create_count = Arc::clone(&create_count);
            move |request_router| {
                create_count.fetch_add(1, Ordering::SeqCst);
                client(request_router)
            }
        },
    );
    let child = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &child_route,
        client,
    );
    let original_connection = root.connection_id();
    let original_cancel = root.connection_cancel_token();
    let request_started = Arc::new(Notify::new());

    let caller = {
        let root = root.clone();
        let request_started = Arc::clone(&request_started);
        tokio::spawn(async move {
            root.run(root_route, move |client| async move {
                request_started.notify_one();
                client.cancel_token.cancelled().await;
            })
            .await
        })
    };
    request_started.notified().await;
    caller.abort();

    tokio::time::timeout(Duration::from_secs(1), original_cancel.cancelled()).await?;
    tokio::time::timeout(Duration::from_secs(1), async {
        while child.connection_id() == original_connection {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    child
        .run(child_route, |_| async move {})
        .await
        .expect("sibling request should use the replacement connection");
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn superseded_failure_replacement_does_not_start_after_its_final_check() -> anyhow::Result<()>
{
    let pool = McpConnectionPool::default();
    let root_route = route();
    let refresh_route = route();
    let create_count = Arc::new(AtomicUsize::new(0));
    let stale_startup_polled = Arc::new(AtomicBool::new(false));
    let root = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &root_route,
        {
            let create_count = Arc::clone(&create_count);
            let stale_startup_polled = Arc::clone(&stale_startup_polled);
            move |request_router| {
                let creation = create_count.fetch_add(1, Ordering::SeqCst);
                if creation == 1 {
                    let stale_startup_polled = Arc::clone(&stale_startup_polled);
                    client_with_startup(
                        request_router,
                        async move {
                            stale_startup_polled.store(true, Ordering::SeqCst);
                            std::future::pending().await
                        }
                        .boxed()
                        .shared(),
                    )
                } else {
                    client(request_router)
                }
            }
        },
    );
    let old_connection = root.current()?;
    let (failure_replacement, startup_route) = root
        .inner
        .slot
        .replace_if_current(&old_connection, Some(&root_route))
        .expect("the failed current generation should be replaced");
    let startup_reached = Arc::new(Notify::new());
    let release_startup = Arc::new(Notify::new());
    McpPooledClient {
        connection: failure_replacement,
        slot: Arc::downgrade(&root.inner.slot),
        route: startup_route,
    }
    .start_in_background_after_check({
        let startup_reached = Arc::clone(&startup_reached);
        let release_startup = Arc::clone(&release_startup);
        async move {
            startup_reached.notify_one();
            release_startup.notified().await;
        }
    });
    startup_reached.notified().await;

    let refreshed = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Replace,
        &refresh_route,
        client,
    );
    release_startup.notify_one();
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }

    assert!(root.ptr_eq(&refreshed));
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
    assert!(!stale_startup_polled.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn route_close_retirement_bounds_unresponsive_shutdown() -> anyhow::Result<()> {
    let pool = McpConnectionPool::default();
    let session_route = route();
    let lease = pool.acquire(
        identity("server", "/one"),
        McpConnectionPoolMode::Reuse,
        &session_route,
        |request_router| {
            client_with_startup(request_router, std::future::pending().boxed().shared())
        },
    );
    let pooled_client = McpPooledClient {
        connection: lease.current()?,
        slot: Arc::downgrade(&lease.inner.slot),
        route: Arc::clone(&session_route),
    };
    session_route.close();
    let retirement = tokio::spawn(async move {
        pooled_client.retire_after_route_close().await;
    });

    tokio::time::advance(Duration::from_secs(11)).await;
    retirement.await?;
    Ok(())
}
