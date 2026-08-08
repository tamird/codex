use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::session::TurnInput;
use crate::session::session::Session;
use crate::session::turn::RunTurnProviderStartup;
use crate::session::turn::run_hooks_and_record_inputs;
use crate::session::turn::run_turn;
use crate::session::turn_context::TurnContext;
use crate::session_startup_prewarm::SessionStartupPrewarmResolution;
use crate::state::TaskKind;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::TurnStartedEvent;
use codex_thread_store::PersistContext;
use tracing::Instrument;
use tracing::trace_span;

use super::SessionTask;
use super::SessionTaskResult;

#[derive(Default)]
pub(crate) struct RegularTask;

impl RegularTask {
    pub(crate) fn new() -> Self {
        Self
    }
}

impl SessionTask for RegularTask {
    fn kind(&self) -> TaskKind {
        TaskKind::Regular
    }

    fn span_name(&self) -> &'static str {
        "session_task.turn"
    }

    async fn run(
        self: Arc<Self>,
        sess: Arc<Session>,
        ctx: Arc<TurnContext>,
        input: Vec<TurnInput>,
        cancellation_token: CancellationToken,
    ) -> SessionTaskResult {
        let run_turn_span = trace_span!("run_turn");
        // Regular turns emit `TurnStarted` inline so first-turn lifecycle does
        // not wait on startup prewarm resolution.
        let provider_startup = async {
            let event = EventMsg::TurnStarted(TurnStartedEvent {
                turn_id: ctx.sub_id.clone(),
                trace_id: ctx.trace_id.clone(),
                started_at: ctx.turn_timing_state.started_at_unix_secs().await,
                model_context_window: ctx.model_context_window(),
                collaboration_mode_kind: ctx.mode(),
            });
            sess.send_event(ctx.as_ref(), event).await;
            sess.set_server_reasoning_included(/*included*/ false).await;
            if ctx.model_routing_retry_at.is_some() {
                return Some(RunTurnProviderStartup::DeferredForRoutingCooldown);
            }
            match sess
                .consume_startup_prewarm_for_regular_turn(&cancellation_token)
                .await
            {
                SessionStartupPrewarmResolution::Cancelled => None,
                SessionStartupPrewarmResolution::Unavailable { .. } => {
                    Some(RunTurnProviderStartup::Ready(None))
                }
                SessionStartupPrewarmResolution::Ready(prewarmed_client_session) => Some(
                    RunTurnProviderStartup::Ready(Some(prewarmed_client_session)),
                ),
            }
        }
        .instrument(trace_span!("regular_task.prepare_run_turn"))
        .await;
        let Some(provider_startup) = provider_startup else {
            run_hooks_and_record_inputs(&sess, &ctx, &input, PersistContext::Standard).await;
            return Ok(None);
        };
        let mut next_input = input;
        let mut provider_startup = Some(provider_startup);
        loop {
            let last_agent_message = run_turn(
                Arc::clone(&sess),
                Arc::clone(&ctx),
                next_input,
                provider_startup
                    .take()
                    .unwrap_or(RunTurnProviderStartup::Ready(None)),
                cancellation_token.child_token(),
            )
            .instrument(run_turn_span.clone())
            .await?;
            // Terminal errors are already reported. Let task completion preserve pending
            // input instead of restarting the failed turn for that same input.
            if ctx.terminal_error.lock().await.is_some() {
                return Ok(last_agent_message);
            }
            if !sess.input_queue.has_pending_input(&sess.active_turn).await {
                return Ok(last_agent_message);
            }
            next_input = Vec::new();
        }
    }
}
