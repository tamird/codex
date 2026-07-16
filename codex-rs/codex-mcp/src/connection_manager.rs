//! Aggregates MCP server connections for Codex.
//!
//! [`McpConnectionSet`] is the private connection set behind
//! [`crate::McpRuntime`] and [`crate::McpBinding`]. It coordinates startup status
//! events, keeps server metadata, and aggregates tools and resources across
//! running RMCP clients.

#[path = "connection_manager/required.rs"]
mod required;
#[path = "connection_manager/resources.rs"]
mod resources;
#[path = "connection_manager/startup.rs"]
mod startup;
#[path = "connection_manager/status.rs"]
mod status;
#[path = "connection_manager/tool_catalog.rs"]
mod tool_catalog;

use startup::chatgpt_auth_provider_for_server;
use startup::emit_update;
use startup::mcp_init_error_display;
use startup::mcp_startup_failure_reason;
use startup::should_share_codex_apps_tools_cache;
pub(crate) use tool_catalog::StableMcpBindingIdentity;
pub use tool_catalog::tool_is_model_visible;

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use crate::binding::call_tool_result_from_rmcp;
use crate::catalog::McpServerSource;
use crate::connection_pool::McpConnectionLease;
use crate::connection_pool::McpConnectionPool;
use crate::connection_pool::McpConnectionPoolMode;
use crate::connection_pool::McpConnectionStartupIdentity;
use crate::connection_pool::McpPooledClient;
use crate::elicitation::ElicitationRequestManager;
use crate::elicitation::ElicitationRequestRouter;
use crate::mcp::CODEX_APPS_MCP_SERVER_NAME;
use crate::mcp::ToolPluginProvenance;
use crate::pagination::MAX_CODEX_APPS_TOOL_CATALOG_ITEMS;
use crate::pagination::MAX_MCP_CATALOG_ITEMS;
use crate::request_router::McpSessionRoute;
use crate::rmcp_client::AsyncManagedClient;
use crate::rmcp_client::DEFAULT_STARTUP_TIMEOUT;
use crate::rmcp_client::DEFAULT_TOOL_TIMEOUT;
use crate::rmcp_client::StartupOutcomeError;
use crate::runtime::McpPublicationGate;
use crate::runtime::McpRuntimeInput;
use crate::runtime::McpStartupPolicy;
use crate::server::McpServerConnectionIdentity;
use crate::server::McpServerMetadata;
use crate::tool_catalog_cache::McpToolCatalogCacheContext;
use crate::tools::ToolFilter;
use crate::tools::ToolInfo;
use crate::trusted_access::ENTITLEMENT_CONTEXT_KEY;
use crate::trusted_access::TrustedAccessContext;
use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use codex_config::McpServerTransportConfig;
use codex_protocol::mcp::CallToolResult;
use codex_protocol::mcp::McpServerInfo;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::McpStartupCompleteEvent;
use codex_protocol::protocol::McpStartupFailure;
use codex_protocol::protocol::McpStartupFailureReason;
use codex_protocol::protocol::McpStartupStatus;
use codex_protocol::protocol::McpStartupUpdateEvent;
use codex_rmcp_client::determine_streamable_http_auth_status_from_credentials;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::warn;

#[derive(Clone)]
struct McpServerView {
    connection: McpConnectionLease,
    startup_trigger: Option<watch::Sender<bool>>,
    startup_status_published: Option<watch::Receiver<bool>>,
    metadata: McpServerMetadata,
    tool_filter: ToolFilter,
    tool_timeout: Option<Duration>,
    catalog_item_limit: usize,
}

impl McpServerView {
    async fn trigger_startup(&self) {
        if let Some(startup_trigger) = &self.startup_trigger {
            startup_trigger.send_replace(true);
        }
        if let Some(startup_status_published) = &self.startup_status_published {
            // Startup status must reach the session before a tool result can overtake it.
            let mut startup_status_published = startup_status_published.clone();
            let _ = startup_status_published
                .wait_for(|published| *published)
                .await;
        }
    }

    fn startup_is_dormant(&self) -> bool {
        self.startup_trigger
            .as_ref()
            .is_some_and(|startup_trigger| !*startup_trigger.borrow())
    }
}

/// A published view over a set of running MCP server connections.
pub(crate) struct McpConnectionSet {
    servers: HashMap<String, McpServerView>,
    disabled_servers: Vec<String>,
    protocol_mode: crate::McpProtocolMode,
    required_servers: Vec<String>,
    optional_startup_deadline: OnceLock<tokio::time::Instant>,
    tool_catalog_revision: Arc<RwLock<u64>>,
    codex_apps_tools_override: RwLock<Option<(u64, Vec<ToolInfo>)>>,
    codex_apps_refresh_lock: Mutex<()>,
    tool_plugin_provenance: Arc<ToolPluginProvenance>,
    prefix_mcp_tool_names: bool,
    non_prefixed_mcp_tool_servers: Vec<String>,
    elicitation_requests: ElicitationRequestManager,
    pub(crate) trusted_access: Option<TrustedAccessContext>,
    session_route: Arc<McpSessionRoute>,
    startup_cancellation_token: CancellationToken,
    connection_pool: McpConnectionPool,
}

impl Drop for McpConnectionSet {
    fn drop(&mut self) {
        // Startup tasks retain leases. Cancel this view's tasks so dropping the last view
        // cannot leave a pending startup holding its own connection alive indefinitely.
        self.startup_cancellation_token.cancel();
        self.session_route.close();
        for view in self.servers.values() {
            view.connection.unregister_route(&self.session_route);
        }
    }
}

impl McpConnectionSet {
    /// Creates an MCP connection manager. Threadless callers can pass no `tx_event`; startup
    /// notifications are then skipped and interactive elicitations are declined.
    pub async fn new(
        previous: Option<&Self>,
        publication_gate: McpPublicationGate,
        input: McpRuntimeInput,
        elicitation_router: ElicitationRequestRouter,
    ) -> Self {
        let trusted_access = TrustedAccessContext::from_runtime(&input);
        let McpRuntimeInput {
            startup_policy,
            config,
            plugins_available: _,
            ready_selected_capability_roots: _,
            mcp_servers,
            submit_id,
            tx_event,
            startup_cancellation_token,
            connection_pool,
            connection_pool_mode,
            runtime_context,
            codex_apps_tools_cache,
            tool_catalog_cache,
            codex_apps_tools_cache_key,
            client_mcp_extensions,
            auth,
            auth_manager,
            elicitation_reviewer,
            elicitation_lifecycle,
        } = input;
        let store_mode = config.mcp_oauth_credentials_store_mode;
        let keyring_backend_kind = config.auth_keyring_backend_kind;
        let codex_home = config.codex_home.clone();
        let prefix_mcp_tool_names = config.prefix_mcp_tool_names;
        let non_prefixed_mcp_tool_servers = config.non_prefixed_mcp_tool_servers.clone();
        let protocol_mode = config.protocol_mode;
        let client_elicitation_capability = config.client_elicitation_capability.clone();
        let tool_plugin_provenance = crate::mcp::tool_plugin_provenance(&config);
        let auth = auth.as_ref();
        let mut servers = HashMap::new();
        let disabled_servers = mcp_servers
            .iter()
            .filter(|(_, server)| !server.enabled())
            .map(|(name, _)| name.clone())
            .collect();
        let mut required_servers = mcp_servers
            .iter()
            .filter(|(_, server)| server.enabled() && server.required())
            .map(|(server_name, _)| server_name.clone())
            .collect::<Vec<_>>();
        required_servers.sort();
        let reused_ready = Vec::new();
        let mut join_set = JoinSet::new();
        // Explicit reconnects have no previous set and must replace their clients eagerly.
        let allow_deferred_startup =
            startup_policy == McpStartupPolicy::LazyWhenCached && previous.is_some();
        let reusable_previous = previous.filter(|previous| {
            !previous.servers.is_empty()
                && previous.elicitation_requests.update(
                    Arc::clone(&config),
                    elicitation_reviewer.clone(),
                    elicitation_lifecycle.clone(),
                )
        });
        let connection_pool = reusable_previous
            .map(|previous| previous.connection_pool.clone())
            .unwrap_or(connection_pool);
        let elicitation_requests = if let Some(previous) = reusable_previous {
            previous.elicitation_requests.clone()
        } else {
            ElicitationRequestManager::new(
                Arc::clone(&config),
                elicitation_reviewer,
                elicitation_lifecycle,
                elicitation_router,
            )
        };
        let session_route = Arc::new(McpSessionRoute::new(
            submit_id.clone(),
            elicitation_requests.clone(),
            tx_event.clone(),
        ));
        let tool_plugin_provenance = Arc::new(tool_plugin_provenance);
        let startup_submit_id = submit_id;
        let static_chatgpt_auth_provider = auth
            .filter(|auth| auth.uses_codex_backend())
            .map(codex_model_provider::auth_provider_from_auth);
        let codex_apps_auth_provider = auth_manager.and_then(|auth_manager| {
            auth.filter(|auth| auth.uses_codex_backend()).map(|auth| {
                codex_model_provider::auth_provider_from_auth_manager(auth_manager, auth)
            })
        });
        for (server_name, server) in mcp_servers
            .into_iter()
            .filter(|(_, server)| server.enabled())
        {
            let registration = config.mcp_server_catalog.server(&server_name);
            let is_host_owned_codex_apps = registration.is_some_and(|server| {
                server
                    .source()
                    .is_host_owned_apps(&server_name, server.config())
            });
            let host_plugin_root = registration.and_then(|server| match server.source() {
                McpServerSource::Plugin(plugin) => plugin.host_root(),
                McpServerSource::SelectedPlugin(_)
                | McpServerSource::Config
                | McpServerSource::Compatibility { .. }
                | McpServerSource::Extension { .. } => None,
            });
            let catalog_item_limit = if is_host_owned_codex_apps {
                MAX_CODEX_APPS_TOOL_CATALOG_ITEMS
            } else {
                MAX_MCP_CATALOG_ITEMS
            };
            let metadata = McpServerMetadata::from(&server);
            let configured_config = server.config().clone();
            let configured_tool_filter = ToolFilter::from_config(&configured_config);
            let startup_timeout = configured_config
                .startup_timeout_sec
                .unwrap_or(DEFAULT_STARTUP_TIMEOUT);
            let configured_tool_timeout = Some(
                configured_config
                    .tool_timeout_sec
                    .unwrap_or(DEFAULT_TOOL_TIMEOUT),
            );
            let resolved_environment =
                runtime_context.resolve_server_environment(&server_name, &configured_config);
            // For built-in Codex Apps, `CODEX_CONNECTORS_TOKEN` is a debug
            // override: it supplies runtime auth but bypasses the shared tools
            // cache.
            let uses_env_bearer_token = match &configured_config.transport {
                McpServerTransportConfig::StreamableHttp {
                    bearer_token_env_var,
                    ..
                } => bearer_token_env_var.is_some(),
                McpServerTransportConfig::Stdio { .. } => false,
            };
            let shares_codex_apps_tools_cache = is_host_owned_codex_apps
                && should_share_codex_apps_tools_cache(&server_name, uses_env_bearer_token);
            let codex_apps_tools_cache_context = shares_codex_apps_tools_cache.then(|| {
                codex_apps_tools_cache
                    .context(codex_home.clone(), codex_apps_tools_cache_key.clone())
            });
            // The reserved Codex Apps registration follows the shared
            // AuthManager across refreshes. In the hosted-plugin path, this
            // is the ChatGPT /ps/mcp connection. User-configured MCP
            // registrations keep their existing configured auth path.
            let chatgpt_auth_provider = if server_name == CODEX_APPS_MCP_SERVER_NAME {
                codex_apps_auth_provider
                    .clone()
                    .or_else(|| static_chatgpt_auth_provider.clone())
            } else {
                static_chatgpt_auth_provider.clone()
            };
            // If Codex Apps has an env bearer token, that is its auth path. Do
            // not also attach the ambient CodexAuth provider.
            let runtime_auth_provider =
                if server_name == CODEX_APPS_MCP_SERVER_NAME && uses_env_bearer_token {
                    None
                } else {
                    chatgpt_auth_provider_for_server(&server, chatgpt_auth_provider)
                };
            let expected_protocol_mode = match &configured_config.transport {
                McpServerTransportConfig::StreamableHttp { .. } => Some(protocol_mode),
                McpServerTransportConfig::Stdio { .. }
                    if protocol_mode == crate::McpProtocolMode::Legacy =>
                {
                    Some(crate::McpProtocolMode::Legacy)
                }
                McpServerTransportConfig::Stdio { env, .. } => match env
                    .as_ref()
                    .and_then(|variables| variables.get("CODEX_MCP_PROTOCOL_VERSION"))
                {
                    None => Some(crate::McpProtocolMode::Legacy),
                    Some(version)
                        if version == rmcp::model::ProtocolVersion::V_2026_07_28.as_str() =>
                    {
                        Some(protocol_mode)
                    }
                    Some(_) => None,
                },
            };
            let previous_identity = previous
                .and_then(|previous| previous.servers.get(&server_name))
                .and_then(|view| view.connection.connection_identity());
            let connection_identity = McpServerConnectionIdentity::new(
                &server_name,
                &server,
                host_plugin_root,
                store_mode,
                keyring_backend_kind,
                &resolved_environment,
                &runtime_context,
                runtime_auth_provider.as_ref(),
                auth,
                shares_codex_apps_tools_cache
                    .then(|| (codex_home.clone(), codex_apps_tools_cache_key.clone())),
                client_elicitation_capability.clone(),
                client_mcp_extensions.clone(),
                previous_identity.as_ref(),
                expected_protocol_mode,
                catalog_item_limit,
            );
            let startup_identity = McpConnectionStartupIdentity {
                protocol_mode: expected_protocol_mode,
                timeout: startup_timeout,
            };
            let cancel_token = startup_cancellation_token.child_token();
            let tool_catalog_cache_context = if server_name == CODEX_APPS_MCP_SERVER_NAME {
                None
            } else if let Ok(environment) = resolved_environment.as_ref() {
                tool_catalog_cache.context(
                    &server_name,
                    &configured_config,
                    &runtime_context,
                    environment.as_ref(),
                    (&client_elicitation_capability, &client_mcp_extensions),
                    Some((
                        &connection_identity,
                        protocol_mode,
                        server.is_agent_plugin(),
                    )),
                )
            } else {
                None
            };
            let has_runtime_auth = runtime_auth_provider.is_some();
            let factory_server_name = server_name.clone();
            let factory_server = server.clone();
            let factory_codex_apps_tools_cache_context = codex_apps_tools_cache_context.clone();
            let factory_tool_catalog_cache_context = tool_catalog_cache_context.clone();
            let factory_runtime_context = runtime_context.clone();
            let factory_resolved_environment = resolved_environment.clone();
            let factory_runtime_auth_provider = runtime_auth_provider.clone();
            let factory_client_elicitation_capability = client_elicitation_capability.clone();
            let factory_client_mcp_extensions = client_mcp_extensions.clone();
            let unchanged_auth_failure =
                if let Some((previous, previous_view)) = reusable_previous.and_then(|previous| {
                    previous
                        .servers
                        .get(&server_name)
                        .map(|view| (previous, view))
                }) && previous_view.connection.connection_identity().as_ref()
                    == Some(&connection_identity)
                    && connection_identity.oauth_store_was_contended
                    && previous.protocol_mode == protocol_mode
                    && previous_view.connection.startup_complete()
                {
                    previous_view
                        .connection
                        .await_current_startup(Arc::clone(&previous.session_route))
                        .await
                        .err()
                        .is_some_and(|error| error.is_authentication_required())
                } else {
                    false
                };
            let contended_auth_failure_reason = unchanged_auth_failure
                .then(|| connection_identity.oauth_credentials().ok().flatten())
                .flatten()
                .map(|_| McpStartupFailureReason::ReauthenticationRequired);
            let server_connection_pool_mode = match connection_pool_mode {
                McpConnectionPoolMode::Replace => McpConnectionPoolMode::Replace,
                McpConnectionPoolMode::Reuse => {
                    match reusable_previous.and_then(|previous| {
                        previous
                            .servers
                            .get(&server_name)
                            .map(|view| (view, Arc::clone(&previous.session_route)))
                    }) {
                        Some((previous_view, _))
                            if previous_view.catalog_item_limit != catalog_item_limit =>
                        {
                            McpConnectionPoolMode::Replace
                        }
                        Some(_) if unchanged_auth_failure => McpConnectionPoolMode::Reuse,
                        Some((previous_view, _)) if previous_view.startup_is_dormant() => {
                            if previous_view
                                .connection
                                .is_reusable_connection(
                                    &connection_identity,
                                    Some(startup_identity),
                                )
                                .await
                            {
                                McpConnectionPoolMode::Reuse
                            } else {
                                McpConnectionPoolMode::Replace
                            }
                        }
                        Some((previous_view, _))
                            if !previous_view.connection.startup_complete() =>
                        {
                            if previous_view
                                .connection
                                .is_reusable_connection(
                                    &connection_identity,
                                    Some(startup_identity),
                                )
                                .await
                            {
                                McpConnectionPoolMode::Reuse
                            } else {
                                McpConnectionPoolMode::Replace
                            }
                        }
                        Some((previous_view, _))
                            if previous_view.connection.has_recoverable_failed_startup() =>
                        {
                            if previous_view
                                .connection
                                .has_same_connection_identity(&connection_identity)
                            {
                                McpConnectionPoolMode::Reuse
                            } else {
                                McpConnectionPoolMode::Replace
                            }
                        }
                        Some((previous_view, _))
                            if !previous_view
                                .connection
                                .is_reusable_connection(
                                    &connection_identity,
                                    Some(startup_identity),
                                )
                                .await =>
                        {
                            McpConnectionPoolMode::Replace
                        }
                        Some((previous_view, previous_session_route))
                            if !previous_view
                                .connection
                                .await_current_startup(Arc::clone(&previous_session_route))
                                .await
                                .is_ok_and(|client| {
                                    expected_protocol_mode.is_some_and(|expected| {
                                        client.client.protocol_mode() == expected
                                    })
                                }) =>
                        {
                            McpConnectionPoolMode::Replace
                        }
                        Some(_) => McpConnectionPoolMode::Reuse,
                        None => match connection_pool
                            .preferred_connection_is_reusable(
                                &server_name,
                                &connection_identity,
                                Some(startup_identity),
                            )
                            .await
                        {
                            Some(false) => McpConnectionPoolMode::Replace,
                            Some(true) | None => McpConnectionPoolMode::Reuse,
                        },
                    }
                }
            };
            let defer_startup = !unchanged_auth_failure
                && allow_deferred_startup
                && !tool_plugin_provenance.is_selected_plugin_mcp_server(&server_name)
                && tool_catalog_cache_context
                    .as_ref()
                    .and_then(McpToolCatalogCacheContext::current_tools)
                    .is_some_and(|tools| {
                        tools.into_iter().any(|tool| {
                            configured_tool_filter.allows(&tool.tool.name)
                                && tool_is_model_visible(&tool)
                        })
                    });
            let (
                startup_trigger,
                startup_receiver,
                startup_status_published,
                startup_status_receiver,
            ) = if defer_startup {
                let (trigger, receiver) = watch::channel(false);
                let (status_published, status_receiver) = watch::channel(false);
                (
                    Some(trigger),
                    Some(receiver),
                    Some(status_published),
                    Some(status_receiver),
                )
            } else {
                (None, None, None, None)
            };
            let connection = connection_pool.acquire_named_with_startup_identity(
                server_name.clone(),
                connection_identity,
                server_connection_pool_mode,
                Some(startup_identity),
                &session_route,
                move |request_router| {
                    AsyncManagedClient::new(
                        factory_server_name.clone(),
                        factory_server.clone(),
                        store_mode,
                        keyring_backend_kind,
                        CancellationToken::new(),
                        request_router,
                        factory_codex_apps_tools_cache_context.clone(),
                        factory_tool_catalog_cache_context.clone(),
                        factory_runtime_context.clone(),
                        factory_resolved_environment.clone(),
                        factory_runtime_auth_provider.clone(),
                        factory_client_elicitation_capability.clone(),
                        factory_client_mcp_extensions.clone(),
                        protocol_mode,
                        catalog_item_limit,
                    )
                },
            );
            servers.insert(
                server_name.clone(),
                McpServerView {
                    connection: connection.clone(),
                    startup_trigger,
                    startup_status_published: startup_status_receiver,
                    metadata,
                    tool_filter: configured_tool_filter,
                    tool_timeout: configured_tool_timeout,
                    catalog_item_limit,
                },
            );
            let tx_event = tx_event.clone();
            let submit_id = startup_submit_id.clone();
            let publication_gate = publication_gate.clone();
            let startup_route = Arc::clone(&session_route);
            let startup = async move {
                let mut startup_receiver = startup_receiver;
                let deferred_startup = startup_receiver.is_some();
                if let Some(startup_receiver) = startup_receiver.as_mut()
                    && tokio::select! {
                        started = startup_receiver.wait_for(|started| *started) => started.is_err(),
                        () = startup_route.closed() => true,
                    }
                {
                    return (server_name, Err(StartupOutcomeError::Cancelled));
                }
                if !publication_gate.wait().await {
                    return (server_name, Err(StartupOutcomeError::Cancelled));
                }
                if let Some(tx_event) = tx_event.as_ref() {
                    let _ = emit_update(
                        submit_id.as_str(),
                        tx_event,
                        McpStartupUpdateEvent {
                            server: server_name.clone(),
                            status: McpStartupStatus::Starting,
                        },
                    )
                    .await;
                }
                let mut outcome = if let Some(startup_receiver) = startup_receiver.as_mut() {
                    // The trigger is never reset. Waiting for false therefore detects the view's
                    // sender being dropped when a refresh replaces this coordinator.
                    let outcome = tokio::select! {
                        outcome = connection.await_current_startup(Arc::clone(&startup_route)) => {
                            Some(outcome)
                        }
                        _ = startup_receiver.wait_for(|started| !*started) => None,
                    };
                    let Some(outcome) = outcome else {
                        return (server_name, Err(StartupOutcomeError::Cancelled));
                    };
                    outcome
                } else {
                    tokio::select! {
                        outcome = connection.await_current_startup(Arc::clone(&startup_route)) => {
                            outcome
                        }
                        () = cancel_token.cancelled() => Err(StartupOutcomeError::Cancelled),
                    }
                };
                if !deferred_startup && cancel_token.is_cancelled() {
                    outcome = Err(StartupOutcomeError::Cancelled);
                }
                if let Some(tx_event) = tx_event.as_ref() {
                    let auth_state = match &outcome {
                        Err(error)
                            if error.is_authentication_required()
                                && !has_runtime_auth
                                && !unchanged_auth_failure =>
                        {
                            match &configured_config.transport {
                                McpServerTransportConfig::StreamableHttp {
                                    url,
                                    bearer_token_env_var,
                                    http_headers,
                                    env_http_headers,
                                    ..
                                } => {
                                    match determine_streamable_http_auth_status_from_credentials(
                                        configured_config
                                            .oauth_credential_name(&server_name)
                                            .as_ref(),
                                        url,
                                        bearer_token_env_var.as_deref(),
                                        http_headers.clone(),
                                        env_http_headers.clone(),
                                        store_mode,
                                        keyring_backend_kind,
                                    ) {
                                        Ok(auth_state) => auth_state,
                                        Err(error) => {
                                            warn!(
                                                "failed to read stored auth status for MCP server `{server_name}`: {error:?}"
                                            );
                                            None
                                        }
                                    }
                                }
                                McpServerTransportConfig::Stdio { .. } => None,
                            }
                        }
                        Ok(_) | Err(_) => None,
                    };
                    if !deferred_startup && cancel_token.is_cancelled() {
                        outcome = Err(StartupOutcomeError::Cancelled);
                    }
                    let status = match &outcome {
                        Ok(_) => McpStartupStatus::Ready,
                        Err(StartupOutcomeError::Cancelled) => McpStartupStatus::Cancelled,
                        Err(error) => {
                            let reason = contended_auth_failure_reason
                                .or_else(|| mcp_startup_failure_reason(auth_state, error));
                            let error_str = mcp_init_error_display(
                                server_name.as_str(),
                                Some(&configured_config),
                                error,
                                reason,
                            );
                            McpStartupStatus::Failed {
                                error: error_str,
                                reason,
                            }
                        }
                    };

                    let _ = emit_update(
                        submit_id.as_str(),
                        tx_event,
                        McpStartupUpdateEvent {
                            server: server_name.clone(),
                            status,
                        },
                    )
                    .await;
                }
                if let Some(startup_status_published) = startup_status_published {
                    startup_status_published.send_replace(true);
                }
                if !deferred_startup && cancel_token.is_cancelled() {
                    outcome = Err(StartupOutcomeError::Cancelled);
                }

                if !unchanged_auth_failure
                    && matches!(&outcome, Err(StartupOutcomeError::Failed { .. }))
                {
                    connection
                        .reconnect_failed_startup(Arc::clone(&startup_route))
                        .await;
                }

                (server_name, outcome)
            };
            if defer_startup {
                // Dormant servers must not hold the initial startup summary open.
                tokio::spawn(startup);
            } else {
                join_set.spawn(startup);
            }
        }
        let manager = Self {
            servers,
            disabled_servers,
            protocol_mode,
            required_servers,
            optional_startup_deadline: OnceLock::new(),
            tool_catalog_revision: Arc::new(RwLock::new(0)),
            codex_apps_tools_override: RwLock::new(None),
            codex_apps_refresh_lock: Mutex::new(()),
            tool_plugin_provenance,
            prefix_mcp_tool_names,
            non_prefixed_mcp_tool_servers,
            elicitation_requests: elicitation_requests.clone(),
            trusted_access,
            session_route,
            startup_cancellation_token: startup_cancellation_token.clone(),
            connection_pool,
        };
        let summary_publication_gate = publication_gate;
        tokio::spawn(async move {
            let outcomes = join_set.join_all().await;
            if let Some(tx_event) = tx_event {
                if !summary_publication_gate.wait().await {
                    return;
                }
                let mut summary = McpStartupCompleteEvent {
                    ready: reused_ready,
                    ..Default::default()
                };
                for server_name in &summary.ready {
                    let _ = emit_update(
                        startup_submit_id.as_str(),
                        &tx_event,
                        McpStartupUpdateEvent {
                            server: server_name.clone(),
                            status: McpStartupStatus::Ready,
                        },
                    )
                    .await;
                }
                for (server_name, outcome) in outcomes {
                    match outcome {
                        Ok(_) => summary.ready.push(server_name),
                        Err(StartupOutcomeError::Cancelled) => summary.cancelled.push(server_name),
                        Err(StartupOutcomeError::Failed { error, .. }) => {
                            summary.failed.push(McpStartupFailure {
                                server: server_name,
                                error,
                            })
                        }
                    }
                }
                let _ = tx_event
                    .send(Event {
                        id: startup_submit_id,
                        msg: EventMsg::McpStartupComplete(summary),
                    })
                    .await;
            }
        });
        manager
    }

    pub fn empty(prefix_mcp_tool_names: bool) -> Self {
        let elicitation_requests = ElicitationRequestManager::default();
        let session_route = Arc::new(McpSessionRoute::new(
            String::new(),
            elicitation_requests.clone(),
            /*tx_event*/ None,
        ));
        Self {
            servers: HashMap::new(),
            disabled_servers: Vec::new(),
            protocol_mode: crate::McpProtocolMode::Legacy,
            required_servers: Vec::new(),
            optional_startup_deadline: OnceLock::new(),
            tool_catalog_revision: Arc::new(RwLock::new(0)),
            codex_apps_tools_override: RwLock::new(None),
            codex_apps_refresh_lock: Mutex::new(()),
            tool_plugin_provenance: Arc::new(ToolPluginProvenance::default()),
            prefix_mcp_tool_names,
            non_prefixed_mcp_tool_servers: Vec::new(),
            elicitation_requests,
            session_route,
            startup_cancellation_token: CancellationToken::new(),
            connection_pool: McpConnectionPool::default(),
            trusted_access: None,
        }
    }

    pub fn has_servers(&self) -> bool {
        !self.servers.is_empty()
    }

    pub(crate) fn session_route(&self) -> Arc<McpSessionRoute> {
        Arc::clone(&self.session_route)
    }

    pub(crate) fn contains_server(&self, server_name: &str) -> bool {
        self.servers.contains_key(server_name)
    }

    pub(crate) async fn run_client_request_by_name<T, F, Fut>(
        &self,
        server: &str,
        operation: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(McpPooledClient, Option<Duration>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let view = self
            .servers
            .get(server)
            .ok_or_else(|| anyhow!("unknown MCP server '{server}'"))?;
        view.trigger_startup().await;
        let timeout = view.tool_timeout;
        view.connection
            .run_mcp_request(Arc::clone(&self.session_route), move |client| {
                operation(client, timeout)
            })
            .await
    }

    pub(crate) async fn authentication_failed_servers(&self) -> Vec<String> {
        let mut failed_servers = Vec::new();
        for (server_name, view) in &self.servers {
            if view.connection.authentication_failed().await {
                failed_servers.push(server_name.clone());
            }
        }
        failed_servers
    }

    pub(crate) async fn updated_oauth_credentials_after_auth_failure(
        &self,
        config: &crate::McpConfig,
    ) -> Vec<String> {
        let mut candidates = Vec::new();
        for server_name in self.authentication_failed_servers().await {
            if let Some(view) = self.servers.get(&server_name)
                && let Some(identity) = view.connection.connection_identity()
                && let Some(server) = config.mcp_server_catalog.server(&server_name)
            {
                candidates.push((server_name, identity, server.config().clone()));
            }
        }
        if candidates.is_empty() {
            return Vec::new();
        }

        match tokio::task::spawn_blocking(move || {
            candidates
                .into_iter()
                .filter_map(|(server_name, identity, config)| {
                    identity
                        .oauth_credentials_changed(&server_name, &config)
                        .then_some(server_name)
                })
                .collect()
        })
        .await
        {
            Ok(recovered_servers) => recovered_servers,
            Err(error) => {
                warn!(%error, "failed to inspect stored MCP OAuth credentials");
                Vec::new()
            }
        }
    }

    pub(crate) async fn wait_for_server_startup(&self, server_name: &str) -> bool {
        let Some(view) = self.servers.get(server_name) else {
            return false;
        };
        view.trigger_startup().await;
        view.connection
            .await_current_startup_preserving_connection(Arc::clone(&self.session_route))
            .await
            .is_ok()
    }

    /// Stop all MCP clients owned by this manager and terminate stdio server processes.
    pub async fn shutdown(&self) {
        let connections = self
            .servers
            .values()
            .map(|view| view.connection.clone())
            .collect::<Vec<_>>();
        let session_route = Arc::clone(&self.session_route);
        self.startup_cancellation_token.cancel();
        self.session_route.close();
        // Keep cleanup alive if an interrupt cancels the refresh that requested it.
        let shutdown_task = tokio::spawn(async move {
            let mut final_connections = Vec::new();
            for connection in connections {
                connection.unregister_route(&session_route);
                if connection.release() {
                    final_connections.push(connection);
                }
            }
            let mut shutdowns = JoinSet::new();
            for connection in final_connections {
                shutdowns.spawn(async move {
                    if tokio::time::timeout(Duration::from_secs(10), connection.shutdown())
                        .await
                        .is_err()
                    {
                        warn!("timed out shutting down MCP client");
                    }
                });
            }
            while let Some(result) = shutdowns.join_next().await {
                if let Err(error) = result {
                    warn!("MCP client shutdown task failed: {error}");
                }
            }
        });
        if let Err(error) = shutdown_task.await {
            warn!("MCP client shutdown task failed: {error}");
        }
    }

    pub(crate) fn cancel_startup(&self) {
        self.startup_cancellation_token.cancel();
    }

    pub fn plugin_id_for_mcp_server_name(&self, server_name: &str) -> Option<&str> {
        self.tool_plugin_provenance
            .plugin_id_for_mcp_server_name(server_name)
    }

    pub fn is_selected_plugin_mcp_server(&self, server_name: &str) -> bool {
        self.tool_plugin_provenance
            .is_selected_plugin_mcp_server(server_name)
    }

    pub async fn wait_for_server_ready(&self, server_name: &str, timeout: Duration) -> bool {
        let Some(view) = self.servers.get(server_name) else {
            return false;
        };
        tokio::time::timeout(timeout, async {
            view.trigger_startup().await;
            view.connection
                .await_current_startup_preserving_connection(Arc::clone(&self.session_route))
                .await
                .is_ok()
        })
        .await
        .unwrap_or(false)
    }

    /// Invoke the tool indicated by the (server, tool) pair.
    #[allow(clippy::too_many_arguments)]
    pub async fn call_tool(
        &self,
        server: &str,
        tool: &str,
        environment_id: Option<&str>,
        arguments: Option<serde_json::Value>,
        mut meta: Option<serde_json::Value>,
        requested_timeout: Option<Duration>,
        wait_for_server: bool,
    ) -> Result<CallToolResult> {
        let view = self
            .servers
            .get(server)
            .ok_or_else(|| anyhow!("unknown MCP server '{server}'"))?;
        if let Some(environment_id) = environment_id
            && view.metadata.environment_id != environment_id
        {
            bail!(
                "MCP server `{server}` is running in environment `{}`, expected `{environment_id}`",
                view.metadata.environment_id
            );
        }
        if !view.tool_filter.allows(tool) {
            return Err(anyhow!(
                "tool '{tool}' is disabled for MCP server '{server}'"
            ));
        }
        let effective_timeout = match (view.tool_timeout, requested_timeout) {
            (Some(server_timeout), Some(requested_timeout)) => {
                Some(server_timeout.min(requested_timeout))
            }
            (server_timeout, requested_timeout) => server_timeout.or(requested_timeout),
        };
        // Direct callers cannot supply host-owned entitlement metadata, even for unlisted tools.
        if let Some(serde_json::Value::Object(meta)) = meta.as_mut() {
            meta.remove(ENTITLEMENT_CONTEXT_KEY);
        }
        if wait_for_server {
            view.trigger_startup().await;
        }
        let tool = tool.to_string();
        let server = server.to_string();
        let result: rmcp::model::CallToolResult = view
            .connection
            .run_mcp_request(Arc::clone(&self.session_route), move |client| async move {
                let managed = if wait_for_server {
                    client.client().await.context("failed to get client")?
                } else {
                    let managed = client
                        .ready_client()
                        .ok_or_else(|| anyhow!("MCP server '{server}' is not connected"))?;
                    if managed.client.is_closed().await {
                        bail!("MCP server '{server}' is not connected");
                    }
                    managed
                };
                managed
                    .client
                    .call_tool(tool.clone(), arguments, meta, effective_timeout)
                    .await
                    .with_context(|| format!("tool call failed for `{server}/{tool}`"))
            })
            .await?;

        Ok(call_tool_result_from_rmcp(result))
    }

    /// Returns presentation metadata from the current connection.
    /// Codex Apps metadata may come from its existing cache; regular MCP server information is
    /// connection-specific, so pending regular clients are awaited.
    pub(crate) async fn list_available_server_infos(&self) -> HashMap<String, McpServerInfo> {
        let mut server_infos = HashMap::new();
        for (server_name, view) in &self.servers {
            if !view.connection.startup_complete()
                && let Some(server_info) = view.connection.cached_server_info()
            {
                server_infos.insert(server_name.clone(), server_info);
                continue;
            }
            view.trigger_startup().await;
            match view
                .connection
                .await_current_startup(Arc::clone(&self.session_route))
                .await
            {
                Ok(managed_client) => {
                    server_infos.insert(server_name.clone(), managed_client.server_info);
                }
                Err(_) => {
                    if let Some(server_info) = view.connection.cached_server_info() {
                        server_infos.insert(server_name.clone(), server_info);
                    }
                }
            }
        }
        server_infos
    }
}

#[cfg(test)]
#[path = "connection_manager_tests.rs"]
mod tests;
