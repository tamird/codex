use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use codex_connectors::ConnectorRuntimeFetchSource;
use futures::future::join_all;
use tracing::Instrument;
use tracing::instrument;
use tracing::trace;
use tracing::trace_span;

use super::McpConnectionSet;
use super::McpServerMetadata;
use crate::binding::McpBinding;
use crate::binding::PreparedMcpCall;
use crate::binding_clients::McpBindingClients;
use crate::connection_pool::McpPooledBindingClient;
use crate::connection_pool::StableMcpConnectionState;
use crate::mcp::CODEX_APPS_MCP_SERVER_NAME;
use crate::rmcp_client::CODEX_APPS_REFRESH_DURATION_METRIC;
use crate::rmcp_client::MCP_TOOLS_LIST_DURATION_METRIC;
use crate::rmcp_client::list_tools_for_client_uncached;
use crate::rmcp_client::prepare_codex_apps_tools_for_model;
use crate::runtime::emit_duration;
use crate::tools::ToolInfo;
use crate::tools::filter_tools;
use crate::tools::normalize_tools_for_model_with_prefix;

const MCP_UI_META_KEY: &str = "ui";
const MCP_UI_VISIBILITY_META_KEY: &str = "visibility";
const MCP_UI_MODEL_VISIBILITY: &str = "model";
/// Tool-catalog and physical-connection identity for a reusable model-step binding.
///
/// A pooled connection replacement can preserve the catalog revision, so both values are
/// required before an existing binding can be returned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StableMcpBindingIdentity {
    catalog_revision: u64,
    connection_ids: Vec<(String, u64)>,
}

#[cfg(test)]
impl StableMcpBindingIdentity {
    pub(crate) fn for_test(catalog_revision: u64, connection_ids: Vec<(String, u64)>) -> Self {
        Self {
            catalog_revision,
            connection_ids,
        }
    }
}

/// Returns whether a tool may be included in model-facing tool declarations.
///
/// Tools without visibility metadata remain visible. Tools with visibility
/// metadata are hidden unless they explicitly include `model`.
///
/// <https://github.com/modelcontextprotocol/ext-apps/blob/main/specification/2026-01-26/apps.mdx#resource-discovery>
pub fn tool_is_model_visible(tool: &ToolInfo) -> bool {
    let Some(visibility) = tool
        .tool
        .meta
        .as_deref()
        .and_then(|meta| meta.get(MCP_UI_META_KEY))
        .and_then(serde_json::Value::as_object)
        .and_then(|ui| ui.get(MCP_UI_VISIBILITY_META_KEY))
        .and_then(serde_json::Value::as_array)
    else {
        return true;
    };
    visibility
        .iter()
        .any(|target| target.as_str() == Some(MCP_UI_MODEL_VISIBILITY))
}

impl McpConnectionSet {
    pub(crate) async fn stable_binding_identity(&self) -> Option<StableMcpBindingIdentity> {
        let mut connection_ids = Vec::with_capacity(self.servers.len());
        for (server_name, view) in &self.servers {
            match view.connection.stable_connection_state().await {
                StableMcpConnectionState::Ready(connection_id) => {
                    connection_ids.push((server_name.clone(), connection_id));
                }
                StableMcpConnectionState::TerminalFailure
                    if server_name != CODEX_APPS_MCP_SERVER_NAME
                        && self.required_servers.binary_search(server_name).is_err() =>
                {
                    continue;
                }
                StableMcpConnectionState::TerminalFailure
                | StableMcpConnectionState::PendingOrClosed => return None,
            }
        }
        connection_ids.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Some(StableMcpBindingIdentity {
            catalog_revision: *self.tool_catalog_revision.read().await,
            connection_ids,
        })
    }

    #[cfg(test)]
    pub(crate) async fn stable_catalog_revision(&self) -> Option<u64> {
        self.stable_binding_identity()
            .await
            .map(|identity| identity.catalog_revision)
    }

    /// Returns all tools with model-visible names normalized.
    #[instrument(level = "trace", skip_all, fields(mcp_server_count = self.servers.len()))]
    pub async fn list_all_tools(&self) -> Vec<ToolInfo> {
        let mut tools = Vec::new();
        let mut available_server_count = 0;
        let mut unavailable_server_count = 0;
        let server_results = join_all(self.servers.iter().map(|(server_name, view)| async move {
            let cached_tools = if view.connection.startup_complete() {
                None
            } else {
                view.connection.cached_tools_or(/*fallback*/ None)
            };
            let server_tools = if let Some((_connection_id, tools)) = cached_tools {
                let tools = filter_tools(tools, &view.tool_filter);
                if server_name == CODEX_APPS_MCP_SERVER_NAME {
                    prepare_codex_apps_tools_for_model(tools, &self.tool_plugin_provenance)
                } else {
                    crate::rmcp_client::prepare_regular_mcp_tools_for_model(
                        tools,
                        &self.tool_plugin_provenance,
                    )
                }
            } else {
                let has_cached_tools = view.connection.has_cached_tools();
                if !has_cached_tools {
                    view.trigger_startup().await;
                }
                view.connection
                    .reconnect_failed_startup(view.session_route())
                    .await;
                let startup_complete = view.connection.startup_complete();
                let catalog_override = if server_name == CODEX_APPS_MCP_SERVER_NAME {
                    self.codex_apps_tools_override.read().await.clone()
                } else {
                    None
                };
                let provenance = Arc::clone(&self.tool_plugin_provenance);
                let tool_filter = view.tool_filter.clone();
                let Ok(Some(server_tools)) = view
                    .connection
                    .run(view.session_route(), move |client| async move {
                        let tools = match catalog_override {
                            Some((connection_id, tools))
                                if connection_id == client.connection_id() =>
                            {
                                client.prepare_tools(tools, provenance.as_ref())
                            }
                            _ => client.listed_tools(provenance.as_ref()).await?,
                        };
                        Some(filter_tools(tools, &tool_filter))
                    })
                    .instrument(trace_span!(
                        "list_tools_for_server",
                        server_name = %server_name,
                        has_cached_tools,
                        startup_complete
                    ))
                    .await
                else {
                    trace!(
                        server_name = %server_name,
                        has_cached_tools,
                        startup_complete,
                        "MCP server tools unavailable while building tool list"
                    );
                    return None;
                };
                server_tools
            };
            Some(
                server_tools
                    .into_iter()
                    .map(|tool| Self::with_server_metadata(tool, &view.metadata))
                    .collect::<Vec<_>>(),
            )
        }))
        .await;
        for server_tools in server_results {
            match server_tools {
                Some(server_tools) => {
                    available_server_count += 1;
                    tools.extend(server_tools);
                }
                None => unavailable_server_count += 1,
            }
        }
        let tools = normalize_tools_for_model_with_prefix(
            tools,
            self.prefix_mcp_tool_names,
            &self.non_prefixed_mcp_tool_servers,
        );
        trace!(
            available_server_count,
            unavailable_server_count,
            tool_count = tools.len(),
            "built MCP tool list"
        );
        tools
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "catalog capture must remain serialized with catalog replacement"
    )]
    pub(crate) async fn capture_binding_with_metadata(
        self: &Arc<Self>,
        config: Arc<crate::McpConfig>,
        plugins_available: bool,
        required_servers: &[String],
    ) -> McpBinding {
        let revision = self.tool_catalog_revision.read().await;
        let mut listed_tools = Vec::new();
        let mut clients = std::collections::HashMap::new();
        let optional_mcp_startup_grace = config.optional_mcp_startup_grace;
        let server_snapshots = join_all(self.servers.iter().map(|(server_name, view)| async move {
            if !view.connection.startup_complete()
            {
                let required = self.required_servers.binary_search(server_name).is_ok();
                // Keep the catalog that lets us skip startup even if it expires during the wait.
                let cached_tools = view.connection.cached_tools_or(/*fallback*/ None);
                let has_cached_tools = cached_tools.is_some();
                let must_wait_for_startup = (required
                    && (!view.startup_is_dormant() || !has_cached_tools))
                    || self.is_selected_plugin_mcp_server(server_name)
                    || required_servers
                        .iter()
                        .any(|required| required == server_name)
                    || (server_name == CODEX_APPS_MCP_SERVER_NAME && !has_cached_tools);
                if !must_wait_for_startup && has_cached_tools {
                    return (server_name, view, cached_tools);
                }
                if !must_wait_for_startup && optional_mcp_startup_grace.is_zero() {
                    view.connection.optional_startup_deadline(
                        tokio::time::Instant::now(),
                        optional_mcp_startup_grace,
                    );
                } else if !must_wait_for_startup {
                    let optional_startup_deadline = if view.startup_is_dormant() {
                        tokio::time::Instant::now() + optional_mcp_startup_grace
                    } else {
                        *self.optional_startup_deadline.get_or_init(|| {
                            tokio::time::Instant::now() + optional_mcp_startup_grace
                        })
                    };
                    let startup_deadline = view.connection.optional_startup_deadline(
                        optional_startup_deadline,
                        optional_mcp_startup_grace,
                    );
                    if tokio::time::timeout_at(startup_deadline, async {
                        view.trigger_startup().await;
                        view.connection
                            .await_current_startup_preserving_connection(view.session_route())
                            .await
                    })
                    .await
                    .is_err()
                    {
                        trace!(server_name = %server_name, "omitting pending optional MCP server");
                    }
                    return (server_name, view, cached_tools);
                }
                view.trigger_startup().await;
                let _ = view
                    .connection
                    .await_current_startup_preserving_connection(view.session_route())
                    .await;
                return (server_name, view, cached_tools);
            }
            (server_name, view, None)
        }))
        .await;
        let server_results = join_all(server_snapshots.into_iter().map(|(server_name, view, cached_tools)| async move {
            let (client, server_tools) = if !view.connection.startup_complete()
            {
                let (_connection_id, tools) = view.connection.cached_tools_or(cached_tools)?;
                (None, tools)
            } else {
                let catalog_override = if server_name == CODEX_APPS_MCP_SERVER_NAME {
                    self.codex_apps_tools_override.read().await.clone()
                } else {
                    None
                };
                let Some((client, server_tools)) = view
                    .connection
                    .capture_ready_client_and_tools(
                        view.session_route(),
                        catalog_override,
                        view.tool_timeout,
                    )
                    .await
                else {
                    trace!(server_name = %server_name, "omitting MCP server without an exact ready client");
                    return None;
                };
                (Some(client), server_tools)
            };
            let server_tools = filter_tools(server_tools, &view.tool_filter);
            let server_tools = if server_name == CODEX_APPS_MCP_SERVER_NAME {
                prepare_codex_apps_tools_for_model(server_tools, &self.tool_plugin_provenance)
            } else {
                crate::rmcp_client::prepare_regular_mcp_tools_for_model(
                    server_tools,
                    &self.tool_plugin_provenance,
                )
            };
            let server_tools = server_tools
                .into_iter()
                .map(|mut tool| {
                    if client.is_none()
                        && let Some(annotations) = tool.tool.annotations.as_mut()
                    {
                        annotations.read_only_hint = None;
                    }
                    Self::with_server_metadata(tool, &view.metadata)
                })
                .collect::<Vec<_>>();
            Some((server_name.clone(), client, server_tools))
        }))
        .await;
        for (server_name, client, server_tools) in server_results.into_iter().flatten() {
            if let Some(client) = client {
                clients.insert(server_name, client);
            }
            listed_tools.extend(server_tools);
        }
        let clients = Arc::new(McpBindingClients::new(clients));
        let listed_tools = normalize_tools_for_model_with_prefix(
            listed_tools,
            self.prefix_mcp_tool_names,
            &self.non_prefixed_mcp_tool_servers,
        );
        let mut tools = Vec::with_capacity(listed_tools.len());
        let mut calls = std::collections::HashMap::with_capacity(listed_tools.len());
        for tool_info in listed_tools {
            let model_visible = crate::tool_is_model_visible(&tool_info);
            let Some(client) = clients.client(&tool_info.server_name) else {
                if model_visible {
                    tools.push(tool_info);
                }
                continue;
            };
            let Some(call) = self.prepare_call(&tool_info, client, Arc::clone(&config), *revision)
            else {
                trace!(
                    server_name = %tool_info.server_name,
                    tool_name = %tool_info.tool.name,
                    "omitting MCP tool without an exact ready client"
                );
                continue;
            };
            calls.insert(
                (
                    tool_info.server_name.clone(),
                    tool_info.tool.name.to_string(),
                ),
                call,
            );
            if model_visible {
                tools.push(tool_info);
            }
        }
        McpBinding::new(
            Arc::clone(self),
            clients,
            config,
            plugins_available,
            tools,
            calls,
        )
    }

    fn prepare_call(
        self: &Arc<Self>,
        tool_info: &ToolInfo,
        client: McpPooledBindingClient,
        config: Arc<crate::McpConfig>,
        tool_catalog_revision: u64,
    ) -> Option<PreparedMcpCall> {
        let server_name = &tool_info.server_name;
        let view = self.servers.get(server_name)?;
        PreparedMcpCall::new(
            Arc::clone(self),
            client,
            config,
            tool_catalog_revision,
            Arc::clone(&self.tool_catalog_revision),
            tool_info.clone(),
            view.metadata.clone(),
            self.plugin_id_for_mcp_server_name(server_name)
                .map(str::to_string),
            self.is_selected_plugin_mcp_server(server_name),
        )
    }

    /// Force-refresh Codex Apps tools and publish one new exact catalog revision.
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "catalog publication must remain serialized with captured tool calls"
    )]
    pub async fn hard_refresh_codex_apps_tools_cache(&self) -> Result<Vec<ToolInfo>> {
        let _refresh = self.codex_apps_refresh_lock.lock().await;
        let refresh_start = Instant::now();
        let view = self
            .servers
            .get(CODEX_APPS_MCP_SERVER_NAME)
            .ok_or_else(|| anyhow!("unknown MCP server '{CODEX_APPS_MCP_SERVER_NAME}'"))?;
        view.trigger_startup().await;
        let client = view.connection.clone();
        let list_start = Instant::now();
        let tool_timeout = view.tool_timeout;
        let catalog_item_limit = view.catalog_item_limit;
        let (connection_id, managed_client, fetch_ticket, client_tools) = client
            .run_mcp_request(view.session_route(), move |client| async move {
                let connection_id = client.connection_id();
                let managed_client = client.client().await.context("failed to get client")?;
                let fetch_ticket =
                    managed_client
                        .codex_apps_tools_cache_context
                        .as_ref()
                        .map(|cache_context| {
                            cache_context.begin_fetch(ConnectorRuntimeFetchSource::HardRefresh)
                        });
                let client_tools = list_tools_for_client_uncached(
                    CODEX_APPS_MCP_SERVER_NAME,
                    /*is_codex_apps_mcp_server*/ true,
                    /*codex_apps_refresh_trigger*/ "explicit",
                    &managed_client.client,
                    tool_timeout,
                    catalog_item_limit,
                    managed_client.server_instructions.as_deref(),
                )
                .await
                .with_context(|| {
                    format!("failed to refresh tools for MCP server '{CODEX_APPS_MCP_SERVER_NAME}'")
                })?;
                Ok((connection_id, managed_client, fetch_ticket, client_tools))
            })
            .await
            .context("failed to get client")?;

        let mut tool_catalog_revision = self.tool_catalog_revision.write().await;
        let tools = match (
            managed_client.codex_apps_tools_cache_context.as_ref(),
            fetch_ticket,
        ) {
            (Some(cache_context), Some(fetch_ticket)) => cache_context.publish_if_newest_accepted(
                fetch_ticket,
                &managed_client.server_info,
                client_tools.clone(),
            ),
            (None, None) => client_tools.clone(),
            _ => unreachable!("Codex Apps fetch ticket requires cache context"),
        };
        *self.codex_apps_tools_override.write().await = Some((connection_id, client_tools));
        *tool_catalog_revision += 1;
        drop(tool_catalog_revision);
        emit_duration(
            MCP_TOOLS_LIST_DURATION_METRIC,
            list_start.elapsed(),
            &[("cache", "miss")],
        );
        let tools = prepare_codex_apps_tools_for_model(
            filter_tools(tools, &view.tool_filter),
            &self.tool_plugin_provenance,
        )
        .into_iter()
        .map(|tool| Self::with_server_metadata(tool, &view.metadata));
        let tools = normalize_tools_for_model_with_prefix(
            tools,
            self.prefix_mcp_tool_names,
            &self.non_prefixed_mcp_tool_servers,
        );
        emit_duration(
            CODEX_APPS_REFRESH_DURATION_METRIC,
            refresh_start.elapsed(),
            &[("path", "legacy"), ("trigger", "explicit")],
        );
        Ok(tools)
    }

    fn with_server_metadata(mut tool: ToolInfo, metadata: &McpServerMetadata) -> ToolInfo {
        tool.supports_parallel_tool_calls = metadata.supports_parallel_tool_calls;
        tool.server_origin = metadata
            .origin
            .as_ref()
            .map(|origin| origin.as_str().to_string());
        tool
    }
}
