use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use rmcp::model::ListResourceTemplatesResult;
use rmcp::model::ListResourcesResult;
use rmcp::model::PaginatedRequestParams;
use rmcp::model::ReadResourceRequestParams;
use rmcp::model::ReadResourceResult;
use rmcp::model::Resource;
use rmcp::model::ResourceTemplate;
use tokio::task::JoinSet;
use tracing::warn;

use super::McpConnectionSet;
use crate::pagination::collect_paginated;
impl McpConnectionSet {
    /// Returns resources from servers selected by `include_server`.
    pub async fn list_all_resources(
        &self,
        include_server: impl Fn(&str) -> bool,
    ) -> HashMap<String, Vec<Resource>> {
        let mut join_set = JoinSet::new();
        for (server_name, view) in self
            .servers
            .iter()
            .filter(|(server_name, _)| include_server(server_name))
        {
            let server_name = server_name.clone();
            view.trigger_startup().await;
            let connection = view.connection.clone();
            let session_route = Arc::clone(&self.session_route);
            let timeout = view.tool_timeout;
            join_set.spawn(async move {
                let result = connection
                    .run_mcp_request(session_route, move |client| async move {
                        let managed = client.client().await.context("failed to get client")?;
                        let client = Arc::clone(&managed.client);
                        collect_paginated("resources/list", timeout, |params| {
                            let client = Arc::clone(&client);
                            async move {
                                let response = client.list_resources(params, timeout).await?;
                                Ok((response.resources, response.next_cursor))
                            }
                        })
                        .await
                    })
                    .await;
                (server_name, result)
            });
        }

        let mut resources = HashMap::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((server_name, Ok(server_resources))) => {
                    resources.insert(server_name, server_resources);
                }
                Ok((server_name, Err(error))) => {
                    warn!("Failed to list resources for MCP server '{server_name}': {error:#}");
                }
                Err(error) => {
                    warn!("Task panic when listing resources for MCP server: {error:#}");
                }
            }
        }
        resources
    }

    /// Returns resource templates from servers selected by `include_server`.
    pub async fn list_all_resource_templates(
        &self,
        include_server: impl Fn(&str) -> bool,
    ) -> HashMap<String, Vec<ResourceTemplate>> {
        let mut join_set = JoinSet::new();
        for (server_name, view) in self
            .servers
            .iter()
            .filter(|(server_name, _)| include_server(server_name))
        {
            let server_name = server_name.clone();
            view.trigger_startup().await;
            let connection = view.connection.clone();
            let session_route = Arc::clone(&self.session_route);
            let timeout = view.tool_timeout;
            join_set.spawn(async move {
                let result = connection
                    .run_mcp_request(session_route, move |client| async move {
                        let managed = client.client().await.context("failed to get client")?;
                        let client = Arc::clone(&managed.client);
                        collect_paginated("resources/templates/list", timeout, |params| {
                            let client = Arc::clone(&client);
                            async move {
                                let response =
                                    client.list_resource_templates(params, timeout).await?;
                                Ok((response.resource_templates, response.next_cursor))
                            }
                        })
                        .await
                    })
                    .await;
                (server_name, result)
            });
        }

        let mut templates = HashMap::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((server_name, Ok(server_templates))) => {
                    templates.insert(server_name, server_templates);
                }
                Ok((server_name, Err(error))) => {
                    warn!(
                        "Failed to list resource templates for MCP server '{server_name}': {error:#}"
                    );
                }
                Err(error) => {
                    warn!("Task panic when listing resource templates for MCP server: {error:#}");
                }
            }
        }
        templates
    }

    pub async fn list_resources(
        &self,
        server: &str,
        params: Option<PaginatedRequestParams>,
    ) -> Result<ListResourcesResult> {
        let view = self
            .servers
            .get(server)
            .ok_or_else(|| anyhow!("unknown MCP server '{server}'"))?;
        view.trigger_startup().await;
        let server = server.to_string();
        let timeout = view.tool_timeout;
        view.connection
            .run_mcp_request(Arc::clone(&self.session_route), move |client| async move {
                let managed = client.client().await.context("failed to get client")?;
                managed
                    .client
                    .list_resources(params, timeout)
                    .await
                    .with_context(|| format!("resources/list failed for `{server}`"))
            })
            .await
    }

    pub async fn list_resource_templates(
        &self,
        server: &str,
        params: Option<PaginatedRequestParams>,
    ) -> Result<ListResourceTemplatesResult> {
        let view = self
            .servers
            .get(server)
            .ok_or_else(|| anyhow!("unknown MCP server '{server}'"))?;
        view.trigger_startup().await;
        let server = server.to_string();
        let timeout = view.tool_timeout;
        view.connection
            .run_mcp_request(Arc::clone(&self.session_route), move |client| async move {
                let managed = client.client().await.context("failed to get client")?;
                managed
                    .client
                    .list_resource_templates(params, timeout)
                    .await
                    .with_context(|| format!("resources/templates/list failed for `{server}`"))
            })
            .await
    }

    pub async fn read_resource(
        &self,
        server: &str,
        params: ReadResourceRequestParams,
    ) -> Result<ReadResourceResult> {
        let view = self
            .servers
            .get(server)
            .ok_or_else(|| anyhow!("unknown MCP server '{server}'"))?;
        view.trigger_startup().await;
        let server = server.to_string();
        let timeout = view.tool_timeout;
        view.connection
            .run_mcp_request(Arc::clone(&self.session_route), move |client| async move {
                let managed = client.client().await.context("failed to get client")?;
                let uri = params.uri.clone();
                managed
                    .client
                    .read_resource(params, timeout)
                    .await
                    .with_context(|| format!("resources/read failed for `{server}` ({uri})"))
            })
            .await
    }
}
