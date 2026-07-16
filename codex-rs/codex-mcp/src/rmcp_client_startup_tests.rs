use super::StartServerTaskParams;
use super::start_server_task;
use crate::CODEX_APPS_MCP_SERVER_NAME;
use crate::request_router::McpConnectionRequestRouter;
use crate::tools::ToolInfo;
use codex_connectors::ConnectorRuntimeContextKey;
use codex_connectors::ConnectorRuntimeManager;
use codex_protocol::mcp::ClientMcpExtensions;
use codex_rmcp_client::InProcessTransportFactory;
use codex_rmcp_client::RmcpClient;
use futures::FutureExt;
use futures::future::BoxFuture;
use pretty_assertions::assert_eq;
use rmcp::RoleServer;
use rmcp::ServerHandler;
use rmcp::ServiceExt;
use rmcp::model::ElicitationCapability;
use rmcp::model::ListToolsResult;
use rmcp::model::PaginatedRequestParams;
use rmcp::model::ServerCapabilities;
use rmcp::model::ServerInfo;
use rmcp::model::Tool;
use rmcp::service::RequestContext;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::DuplexStream;
use tokio::sync::Notify;

#[derive(Clone)]
struct StartupCatalogServer {
    tool: Tool,
    list_started: Arc<Notify>,
    release_list: Arc<Notify>,
}

impl ServerHandler for StartupCatalogServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, rmcp::ErrorData> {
        self.list_started.notify_one();
        self.release_list.notified().await;
        Ok(ListToolsResult::with_all_items(vec![self.tool.clone()]))
    }
}

impl InProcessTransportFactory for StartupCatalogServer {
    fn open(&self) -> BoxFuture<'static, io::Result<DuplexStream>> {
        let server = self.clone();
        async move {
            let (client_stream, server_stream) = tokio::io::duplex(/*max_buf_size*/ 4096);
            tokio::spawn(async move {
                let server = server
                    .serve(server_stream)
                    .await
                    .expect("serve startup test MCP server");
                server
                    .waiting()
                    .await
                    .expect("wait for startup test MCP server");
            });
            Ok(client_stream)
        }
        .boxed()
    }
}

#[tokio::test]
async fn stale_apps_startup_keeps_its_physical_clients_tools() -> anyhow::Result<()> {
    let cache = ConnectorRuntimeManager::<ToolInfo>::new_without_cache().context(
        PathBuf::from("/codex-home"),
        ConnectorRuntimeContextKey::personal(
            /*account_id*/ None, /*chatgpt_user_id*/ None,
        ),
    );
    let first_tool = Tool::new(
        "first_only",
        "First generation tool",
        Arc::new(Default::default()),
    );
    let second_tool = Tool::new(
        "second_only",
        "Second generation tool",
        Arc::new(Default::default()),
    );
    let first_listing = Arc::new(Notify::new());
    let release_first = Arc::new(Notify::new());
    let release_second = Arc::new(Notify::new());
    let first_client = Arc::new(
        RmcpClient::new_in_process_client(Arc::new(StartupCatalogServer {
            tool: first_tool.clone(),
            list_started: Arc::clone(&first_listing),
            release_list: Arc::clone(&release_first),
        }))
        .await?,
    );
    let second_client = Arc::new(
        RmcpClient::new_in_process_client(Arc::new(StartupCatalogServer {
            tool: second_tool.clone(),
            list_started: Arc::new(Notify::new()),
            release_list: Arc::clone(&release_second),
        }))
        .await?,
    );
    let startup_params = || StartServerTaskParams {
        is_codex_apps_mcp_server: true,
        startup_timeout: Some(Duration::from_secs(/*secs*/ 5)),
        request_router: McpConnectionRequestRouter::default(),
        codex_apps_tools_cache_context: Some(cache.clone()),
        tool_catalog_cache_context: None,
        tool_catalog_fetch_ticket: None,
        client_elicitation_capability: ElicitationCapability::default(),
        client_mcp_extensions: ClientMcpExtensions::default(),
        catalog_item_limit: crate::pagination::MAX_MCP_CATALOG_ITEMS,
    };
    let first_startup = tokio::spawn(start_server_task(
        CODEX_APPS_MCP_SERVER_NAME.to_string(),
        Arc::clone(&first_client),
        startup_params(),
    ));
    // The first fetch ticket already exists when tools/list reaches the server.
    tokio::time::timeout(Duration::from_secs(/*secs*/ 5), first_listing.notified()).await?;
    release_second.notify_one();
    let second = start_server_task(
        CODEX_APPS_MCP_SERVER_NAME.to_string(),
        Arc::clone(&second_client),
        startup_params(),
    )
    .await?;
    let published_before_stale = cache
        .current_tools()
        .expect("second startup publishes its catalog");
    assert_eq!(
        published_before_stale
            .iter()
            .map(|info| &info.tool)
            .collect::<Vec<_>>(),
        vec![&second_tool]
    );

    release_first.notify_one();
    let first = tokio::time::timeout(Duration::from_secs(/*secs*/ 5), first_startup).await??;
    let first = first?;
    assert!(Arc::ptr_eq(&first.client, &first_client));
    assert!(Arc::ptr_eq(&second.client, &second_client));
    let published_after_stale = cache
        .current_tools()
        .expect("stale startup retains shared catalog");
    assert_eq!(
        (
            first
                .tools
                .iter()
                .map(|info| &info.tool)
                .collect::<Vec<_>>(),
            second
                .tools
                .iter()
                .map(|info| &info.tool)
                .collect::<Vec<_>>(),
            published_after_stale
                .iter()
                .map(|info| &info.tool)
                .collect::<Vec<_>>(),
        ),
        (vec![&first_tool], vec![&second_tool], vec![&second_tool]),
    );
    Ok(())
}
