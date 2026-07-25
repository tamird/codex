use super::*;
use codex_app_server_protocol::ClientNotification;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::JSONRPCError;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::JSONRPCResponse;
use futures::SinkExt;
use futures::StreamExt;
use std::sync::Mutex;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum BlockedThreadListPage {
    First,
    Second,
    SecondError,
}

pub(super) type BlockedThreadList = (
    ThreadId,
    oneshot::Sender<()>,
    oneshot::Receiver<()>,
    BlockedThreadListPage,
);

/// Records JSON-RPC requests while forwarding them to the real embedded app server.
pub(super) async fn start_recording_app_server(
    config: &Config,
    mut blocked_thread_list: Option<BlockedThreadList>,
) -> Result<(
    AppServerSession,
    Arc<Mutex<Vec<String>>>,
    JoinHandle<Result<()>>,
)> {
    let state_db =
        crate::init_state_db_for_app_server_target(config, &crate::AppServerTarget::Embedded)
            .await?;
    let embedded = crate::start_embedded_app_server(
        codex_arg0::Arg0DispatchPaths::default(),
        config.clone(),
        Vec::new(),
        codex_config::LoaderOverrides::without_managed_config_for_tests(),
        /*strict_config*/ false,
        codex_config::CloudConfigBundleLoader::default(),
        codex_feedback::CodexFeedback::new(),
        /*log_db*/ None,
        state_db,
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    )
    .await?;
    let codex_home = config.codex_home.display().to_string();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let request_sink = Arc::clone(&requests);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let websocket_url = format!("ws://{}", listener.local_addr()?);
    let proxy = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut websocket = accept_async(stream).await?;
        while let Some(frame) = websocket.next().await {
            let Message::Text(text) = frame? else {
                continue;
            };
            let message = serde_json::from_str::<JSONRPCMessage>(&text)?;
            match message {
                JSONRPCMessage::Request(request) if request.method == "initialize" => {
                    websocket
                        .send(Message::Text(
                            serde_json::to_string(&JSONRPCMessage::Response(JSONRPCResponse {
                                id: request.id,
                                result: serde_json::json!({
                                    "userAgent": "codex-tui-test",
                                    "codexHome": codex_home,
                                }),
                            }))?
                            .into(),
                        ))
                        .await?;
                }
                JSONRPCMessage::Request(request) => {
                    request_sink
                        .lock()
                        .expect("request recorder lock")
                        .push(request.method.clone());
                    let request_id = request.id.clone();
                    let mut request =
                        serde_json::from_value::<ClientRequest>(serde_json::to_value(request)?)?;
                    if let ClientRequest::ThreadList { params, .. } = &mut request
                        && let Some((root_thread_id, _, _, blocked_page)) =
                            blocked_thread_list.as_ref()
                    {
                        std::assert_eq!(
                            params.source_kinds.as_deref(),
                            Some(&[
                                codex_app_server_protocol::ThreadSourceKind::SubAgentThreadSpawn,
                            ][..])
                        );
                        let expected_ancestor =
                            params.use_state_db_only.then(|| root_thread_id.to_string());
                        std::assert_eq!(
                            params.ancestor_thread_id.as_deref(),
                            expected_ancestor.as_deref()
                        );
                        let is_second_page = matches!(
                            *blocked_page,
                            BlockedThreadListPage::Second | BlockedThreadListPage::SecondError
                        );
                        if is_second_page {
                            params.limit = Some(1);
                        }
                        if params.cursor.is_some() == is_second_page {
                            let (_, started, release, blocked_page) =
                                blocked_thread_list.take().expect("blocked thread list");
                            if blocked_page == BlockedThreadListPage::SecondError {
                                params.cursor = Some("not-a-cursor".to_string());
                            }
                            let _ = started.send(());
                            let _ = release.await;
                        }
                    }
                    let response = match embedded.request(request).await? {
                        Ok(result) => JSONRPCMessage::Response(JSONRPCResponse {
                            id: request_id,
                            result,
                        }),
                        Err(error) => JSONRPCMessage::Error(JSONRPCError {
                            id: request_id,
                            error,
                        }),
                    };
                    websocket
                        .send(Message::Text(serde_json::to_string(&response)?.into()))
                        .await?;
                }
                JSONRPCMessage::Notification(notification)
                    if notification.method == "initialized" => {}
                JSONRPCMessage::Notification(notification) => {
                    embedded
                        .notify(serde_json::from_value::<ClientNotification>(
                            serde_json::to_value(notification)?,
                        )?)
                        .await?;
                }
                JSONRPCMessage::Response(_) | JSONRPCMessage::Error(_) => {}
            }
        }
        embedded.shutdown().await?;
        Ok(())
    });
    let app_server = crate::connect_remote_app_server(crate::RemoteAppServerEndpoint::WebSocket {
        websocket_url,
        auth_token: None,
    })
    .await?;

    Ok((
        AppServerSession::new(
            app_server,
            crate::app_server_session::ThreadParamsMode::Embedded,
        ),
        requests,
        proxy,
    ))
}
