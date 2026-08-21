//! Source-owned, one-shot fork transfer. Preparing a pane never starts a temporary runtime.

use super::persisted_resume_settings::PersistedResumeSettings;
use super::*;
use codex_app_server_protocol::ThreadForkImportParams;
use codex_app_server_protocol::ThreadForkPrepareResponse;
use codex_history::CodexHarnessMetadata;
use codex_protocol::mcp::ClientMcpExtensions;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::HistoryPosition;
use codex_rollout::ResponseItemEnvelope;
use codex_thread_store::FrozenRolloutSegment;
use codex_thread_store::PreparedFork;
use codex_uds::UnixListener;
use codex_uds::UnixStream;
use codex_utils_path_uri::LegacyAppPathString;
use std::borrow::Cow;
use std::io;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::OwnedSemaphorePermit;

// Bound the entire seed, including any compatibility history, to the storage interactive scan
// budget. Permit at most two pending handoffs per app-server; reject rather than truncate.
const MAX_HANDOFF_BYTES: usize = 64 * 1024 * 1024;
const HANDOFF_TTL: Duration = Duration::from_secs(120);
const HANDOFF_VERSION: u32 = 1;

/// Selects local creation, owner-side snapshot export, or receiver-side snapshot import.
pub(super) enum ForkHandoff {
    Local,
    Export(OwnedSemaphorePermit),
    Import(Box<ImportedFork>),
}

/// Validated snapshot and source settings retained through durable child creation.
pub(super) struct ImportedFork {
    pub(super) source: StoredThread,
    pub(super) prepared: PreparedFork,
    pub(super) settings: PersistedResumeSettings,
}

/// Bounded transfer of the captured history and settings, independent of the parent's runtime.
#[derive(serde::Serialize, serde::Deserialize)]
struct ForkSeed<'a> {
    /// Rejects transfers encoded with a different handoff protocol.
    version: u32,
    params: ThreadForkParams,
    source: StoredThread,
    history_base: Option<HistoryPosition>,
    frozen_segment: Option<FrozenRolloutSegment>,
    model_context: Arc<Vec<RolloutItem>>,
    shared_model_response_items: Option<Vec<ResponseItemSeed<'a>>>,
    copied_history: Option<Arc<Vec<RolloutItem>>>,
    settings: PersistedResumeSettings,
}

/// Keeps harness provenance separate from model payloads, borrowing both for bounded export.
#[derive(serde::Serialize, serde::Deserialize)]
struct ResponseItemSeed<'a> {
    item: Cow<'a, ResponseItem>,
    metadata: Option<Cow<'a, CodexHarnessMetadata>>,
}

/// Removes the private socket when the transfer completes, expires, or is cancelled.
struct HandoffSocket(PathBuf);

impl Drop for HandoffSocket {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.0)
            && error.kind() != io::ErrorKind::NotFound
        {
            tracing::warn!("failed to remove fork handoff socket: {error}");
        }
    }
}

/// Enforces the transfer size limit during serialization rather than after allocation.
struct SeedBuffer(Vec<u8>);

impl io::Write for SeedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_HANDOFF_BYTES.saturating_sub(self.0.len()) {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "fork handoff exceeds 64 MiB",
            ));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ThreadRequestProcessor {
    // Construct the boxed future here: boxing at the dispatcher still leaves large construction
    // temporaries in its poll frame, even for requests that never use the handoff branches.
    pub(crate) fn thread_fork_prepare(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadForkParams,
    ) -> impl std::future::Future<Output = Result<Option<ClientResponsePayload>, JSONRPCErrorError>>
    + Send
    + '_ {
        Box::pin(async move {
            validate_handoff_params(&params)?;
            let source = ThreadId::from_string(&params.thread_id)
                .map_err(|error| invalid_request(error.to_string()))?;
            let parent = self.thread_manager.get_thread(source).await.map_err(|_| {
                invalid_request(
                    "fork preparation must be requested from the app-server owning the source",
                )
            })?;
            let permit = Arc::clone(&self.fork_handoff_slots)
                .try_acquire_owned()
                .map_err(|_| {
                    invalid_request(
                        "two fork handoffs are already pending; wait for a pane to start",
                    )
                })?;
            parent.flush_rollout().await.map_err(handoff_error)?;
            self.thread_fork_inner(
                request_id,
                params,
                /*app_server_client_name*/ None,
                /*app_server_client_version*/ None,
                ClientMcpExtensions::default(),
                ForkHandoff::Export(permit),
            )
            .await
            .map(|()| None)
        })
    }

    pub(super) async fn prepare_legacy_handoff(
        &self,
        source: &StoredThread,
        ephemeral: bool,
    ) -> Result<PreparedFork, JSONRPCErrorError> {
        let store = self
            .thread_store
            .as_any()
            .downcast_ref::<LocalThreadStore>()
            .ok_or_else(|| invalid_request("fork handoff requires local thread storage"))?;
        if !ephemeral {
            return store
                .prepare_legacy_fork_handoff(source.thread_id)
                .await
                .map_err(handoff_error);
        }
        let context = Arc::new(
            self.thread_store
                .load_latest_model_context(StoreLoadThreadHistoryParams {
                    thread_id: source.thread_id,
                    include_archived: true,
                })
                .await
                .map_err(handoff_error)?
                .items,
        );
        Ok(PreparedFork::new(
            source.thread_id,
            /*history_base*/ None,
            /*frozen_segment*/ None,
            Arc::clone(&context),
            Arc::clone(&context),
            context,
            /*interrupt_if_open*/ true,
            (),
        ))
    }

    pub(super) async fn publish_fork_handoff(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadForkParams,
        source: StoredThread,
        mut prepared: PreparedFork,
        permit: OwnedSemaphorePermit,
    ) -> Result<(), JSONRPCErrorError> {
        let store = self
            .thread_store
            .as_any()
            .downcast_ref::<LocalThreadStore>()
            .ok_or_else(|| invalid_request("fork handoff requires local thread storage"))?;
        let parent = self
            .thread_manager
            .get_thread(source.thread_id)
            .await
            .map_err(handoff_error)?;
        let settings = parent.thread_settings_snapshot().await;
        let reservation: Box<dyn std::fmt::Debug + Send> = if params.ephemeral {
            // Context-only imports have no dependency on a source file, even if the source exits.
            prepared.history_base = None;
            prepared.frozen_segment = None;
            prepared.copied_history = None;
            for item in Arc::make_mut(&mut prepared.model_context) {
                if let RolloutItem::SessionMeta(meta) = item {
                    meta.meta.history_base = None;
                    meta.meta.subagent_history_start_ordinal = None;
                }
            }
            Box::new(())
        } else {
            Box::new(
                store
                    .reserve_exported_fork(source.thread_id)
                    .await
                    .map_err(handoff_error)?,
            )
        };
        let seed = ForkSeed {
            version: HANDOFF_VERSION,
            params,
            source,
            history_base: prepared.history_base,
            frozen_segment: prepared.frozen_segment.clone(),
            // Latest-only, excludeTurns preparation uses this same context for response replay
            // and settings. Serialize it once instead of serializing three copies of one Arc.
            model_context: Arc::clone(&prepared.model_context),
            shared_model_response_items: prepared.shared_model_response_items.as_ref().map(
                |items| {
                    items
                        .iter()
                        .map(|envelope| ResponseItemSeed {
                            item: Cow::Borrowed(&envelope.item),
                            metadata: envelope.metadata.as_ref().map(Cow::Borrowed),
                        })
                        .collect()
                },
            ),
            copied_history: prepared.copied_history.clone(),
            settings: PersistedResumeSettings {
                approval_policy: settings.approval_policy,
                approvals_reviewer: Some(settings.approvals_reviewer),
                active_permission_profile: settings.active_permission_profile,
            },
        };
        let mut bytes = SeedBuffer(Vec::new());
        serde_json::to_writer(&mut bytes, &seed).map_err(handoff_error)?;
        drop(seed);
        let control = crate::app_server_control_socket_path(&self.config.codex_home)
            .map_err(handoff_error)?;
        let directory = control
            .as_path()
            .parent()
            .ok_or_else(|| internal_error("control socket has no directory"))?;
        codex_uds::prepare_private_socket_directory(directory)
            .await
            .map_err(handoff_error)?;
        let path = directory.join(format!("fork-{}.sock", uuid::Uuid::new_v4().simple()));
        let mut listener = UnixListener::bind(&path).await.map_err(handoff_error)?;
        let socket = HandoffSocket(path.clone());
        let response = ThreadForkPrepareResponse {
            socket_path: LegacyAppPathString::from_path(&path),
        };
        self.background_tasks.spawn(async move {
            let _socket = socket;
            let _permit = permit;
            let _prepared = prepared;
            let _reservation = reservation;
            let result = tokio::time::timeout(HANDOFF_TTL, async move {
                let mut stream = listener.accept().await?;
                stream
                    .write_u32(u32::try_from(bytes.0.len()).map_err(io::Error::other)?)
                    .await?;
                stream.write_all(&bytes.0).await?;
                // Retain the process-local lease until the child owns independent protection and
                // validates the snapshot. Disconnect/expiry is safe: the child validates after SH.
                if stream.read_u8().await? != 1 {
                    return Err(io::Error::other("fork handoff was not claimed"));
                }
                Ok::<(), io::Error>(())
            })
            .await;
            if !matches!(result, Ok(Ok(()))) {
                tracing::debug!(?result, "fork handoff ended before the child claimed it");
            }
        });
        self.outgoing.send_response(request_id, response).await;
        Ok(())
    }

    pub(crate) fn thread_fork_import(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadForkImportParams,
        app_server_client_name: Option<String>,
        app_server_client_version: Option<String>,
        client_mcp_extensions: ClientMcpExtensions,
    ) -> impl std::future::Future<Output = Result<Option<ClientResponsePayload>, JSONRPCErrorError>>
    + Send
    + '_ {
        Box::pin(async move {
            let path = params
                .socket_path
                .to_inferred_path_uri()
                .ok_or_else(|| {
                    invalid_request("fork handoff socket must be an absolute local path")
                })?
                .to_abs_path()
                .map_err(handoff_error)?;
            let control = crate::app_server_control_socket_path(&self.config.codex_home)
                .map_err(handoff_error)?;
            let name = path
                .as_path()
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_prefix("fork-"))
                .and_then(|name| name.strip_suffix(".sock"));
            if path.as_path().parent() != control.as_path().parent()
                || name.is_none_or(|name| uuid::Uuid::parse_str(name).is_err())
            {
                return Err(invalid_request(
                    "fork handoff socket is outside the private control directory",
                ));
            }
            let (seed, mut stream) = tokio::time::timeout(HANDOFF_TTL, async {
                let mut stream = UnixStream::connect(path.as_path()).await?;
                let length = usize::try_from(stream.read_u32().await?).map_err(io::Error::other)?;
                if length > MAX_HANDOFF_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::FileTooLarge,
                        "fork handoff exceeds 64 MiB",
                    ));
                }
                let mut bytes = vec![0; length];
                stream.read_exact(&mut bytes).await?;
                // Tagged rollout variants must not deserialize directly from bytes with arbitrary_precision.
                let value: serde_json::Value =
                    serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                let seed: ForkSeed<'static> =
                    serde_json::from_value(value).map_err(io::Error::other)?;
                Ok::<_, io::Error>((seed, stream))
            })
            .await
            .map_err(handoff_error)?
            .map_err(handoff_error)?;
            validate_handoff_params(&seed.params)?;
            if seed.version != HANDOFF_VERSION
                || seed.params.thread_id != seed.source.thread_id.to_string()
                || seed.source.history.is_some()
            {
                return Err(invalid_request(
                    "fork handoff version or source identity does not match",
                ));
            }
            let store = self
                .thread_store
                .as_any()
                .downcast_ref::<LocalThreadStore>()
                .ok_or_else(|| invalid_request("fork handoff requires local thread storage"))?;
            let reservation: Box<dyn std::fmt::Debug + Send> = if seed.params.ephemeral {
                let references_source = seed.history_base.is_some()
                    || seed.frozen_segment.is_some()
                    || seed.copied_history.is_some()
                    || seed.model_context.iter().any(|item| {
                        matches!(item, RolloutItem::RolloutReference(_))
                            || matches!(
                                item,
                                RolloutItem::SessionMeta(meta) if meta.meta.history_base.is_some()
                            )
                    });
                if references_source {
                    return Err(invalid_request(
                        "ephemeral fork handoff must be self-contained",
                    ));
                }
                Box::new(())
            } else {
                let reservation = store
                    .reserve_imported_fork(seed.source.thread_id)
                    .await
                    .map_err(handoff_error)?;
                let frozen = seed
                    .frozen_segment
                    .as_ref()
                    .ok_or_else(|| invalid_request("durable fork handoff has no frozen prefix"))?;
                store
                    .validate_imported_fork(seed.source.thread_id, frozen)
                    .await
                    .map_err(handoff_error)?;
                Box::new(reservation)
            };
            if !seed.model_context.iter().any(|item| {
                matches!(
                    item,
                    RolloutItem::SessionMeta(meta) if meta.meta.id == seed.source.thread_id
                )
            }) {
                return Err(invalid_request(
                    "fork handoff is missing its source metadata",
                ));
            }
            stream.write_u8(1).await.map_err(handoff_error)?;
            let mut prepared = PreparedFork::new(
                seed.source.thread_id,
                seed.history_base,
                seed.frozen_segment,
                Arc::clone(&seed.model_context),
                Arc::clone(&seed.model_context),
                seed.model_context,
                /*interrupt_if_open*/ true,
                reservation,
            );
            prepared.shared_model_response_items = seed.shared_model_response_items.map(|items| {
                Arc::new(
                    items
                        .into_iter()
                        .map(|envelope| ResponseItemEnvelope {
                            item: envelope.item.into_owned(),
                            metadata: envelope.metadata.map(Cow::into_owned),
                        })
                        .collect(),
                )
            });
            prepared.copied_history = seed.copied_history;
            prepared.model_state_origin = codex_thread_store::ForkModelStateOrigin::Snapshot;
            self.thread_fork_inner(
                request_id,
                seed.params,
                app_server_client_name,
                app_server_client_version,
                client_mcp_extensions,
                ForkHandoff::Import(Box::new(ImportedFork {
                    source: seed.source,
                    prepared,
                    settings: seed.settings,
                })),
            )
            .await
            .map(|()| None)
        })
    }
}

fn validate_handoff_params(params: &ThreadForkParams) -> Result<(), JSONRPCErrorError> {
    if params.last_turn_id.is_some()
        || params.before_turn_id.is_some()
        || params.path.is_some()
        || !params.exclude_turns
        || params.defer_goal_continuation
    {
        return Err(invalid_request(
            "fork handoff requires a loaded threadId, latest history, excludeTurns, and no goal continuation",
        ));
    }
    Ok(())
}

fn handoff_error(error: impl std::fmt::Display) -> JSONRPCErrorError {
    internal_error(format!("fork handoff failed: {error}"))
}
