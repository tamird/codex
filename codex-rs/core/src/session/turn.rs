use std::collections::HashMap;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::client::ModelClientSession;
use crate::client_common::Prompt;
use crate::client_common::ResponseEvent;
use crate::compact::CompactionReporting;
use crate::compact::InitialContextInjection;
use crate::compact::run_inline_auto_compact_task;
use crate::compact_remote::run_inline_remote_auto_compact_task;
use crate::compact_remote_v2::run_inline_remote_auto_compact_task as run_inline_remote_auto_compact_task_v2;
use crate::connectors;
use crate::context::ContextualUserFragment;
use crate::context::InterruptedResponseRecord;
use crate::context::world_state::WorldState;
use crate::environment_selection::TurnEnvironmentSnapshot;
use crate::feedback_tags;
use crate::hook_runtime::PostCompactHookOutcome;
use crate::hook_runtime::drain_async_hook_results;
use crate::hook_runtime::inspect_pending_input;
use crate::hook_runtime::record_additional_contexts;
use crate::hook_runtime::record_pending_input;
use crate::hook_runtime::run_legacy_after_agent_hook;
use crate::hook_runtime::run_pending_session_start_hooks;
use crate::hook_runtime::run_post_compact_hooks;
use crate::hook_runtime::run_turn_stop_hooks;
use crate::mcp_skill_dependencies::maybe_prompt_and_install_mcp_dependencies;
use crate::mentions::build_connector_slug_counts;
use crate::mentions::collect_explicit_app_ids;
use crate::mentions::collect_explicit_plugin_mentions;
use crate::mentions::collect_tool_mentions_from_messages;
use crate::plugins::build_plugin_injections;
use crate::responses_metadata::CodexResponsesMetadata;
use crate::responses_metadata::CodexResponsesRequestKind;
use crate::responses_retry::ResponsesStreamRequest;
use crate::responses_retry::ResponsesStreamRetryState;
use crate::responses_retry::handle_retryable_response_stream_error;
use crate::session::PreviousTurnSettings;
use crate::session::TurnInput;
use crate::session::model_routing::classify_model_routing_failure;
use crate::session::session::Session;
use crate::session::step_context::StepContext;
use crate::session::turn_context::TurnContext;
use crate::session_startup_prewarm::SessionStartupPrewarmResolution;
use crate::skills::emit_explicit_skill_invocations;
use crate::stream_events_utils::HandleOutputCtx;
use crate::stream_events_utils::InFlightFuture;
use crate::stream_events_utils::TurnItemContributorPolicy;
use crate::stream_events_utils::finalize_non_tool_response_item;
use crate::stream_events_utils::handle_non_tool_response_item;
use crate::stream_events_utils::handle_output_item_done;
use crate::stream_events_utils::last_assistant_message_from_item;
use crate::stream_events_utils::mark_thread_memory_mode_polluted_if_external_context;
use crate::stream_events_utils::raw_assistant_output_text_from_item;
use crate::stream_events_utils::record_completed_response_item_with_finalized_facts;
use crate::tasks::emit_compact_metric;
use crate::tools::ToolRouter;
use crate::tools::context::SharedTurnDiffTracker;
use crate::tools::handlers::is_set_workspace_cwd_tool;
use crate::tools::parallel::ToolCallRuntime;
use crate::tools::registry::ToolArgumentDiffConsumer;
use crate::tools::router::ToolSuggestCandidates;
use crate::tools::router::ToolSuggestPresentation;
use crate::tools::spec_plan::build_tool_router;
use crate::tools::spec_plan::tool_suggest_enabled;
use crate::turn_diff_tracker::TurnDiffTracker;
use crate::turn_timing::record_turn_ttft_metric;
use crate::util::error_or_panic;
use codex_analytics::AppInvocation;
use codex_analytics::CompactionPhase;
use codex_analytics::CompactionReason;
use codex_analytics::CompactionTrigger;
use codex_analytics::InvocationType;
use codex_analytics::TurnResolvedConfigFact;
use codex_analytics::build_track_events_context;
use codex_async_utils::OrCancelExt;
use codex_connectors::AppToolPolicyEvaluator;
use codex_core_plugins::RecommendedPluginCandidatesInput;
use codex_extension_api::ExtensionData;
use codex_extension_api::TurnInputContext;
use codex_extension_api::TurnInputEnvironment;
use codex_features::Feature;
use codex_file_system::FindUpErrorPolicy;
use codex_file_system::find_nearest_ancestor_with_markers;
use codex_login::CodexAuth;
use codex_model_provider::RemoteCompactionSupport;
use codex_protocol::ResponseItemId;
use codex_protocol::config_types::AutoCompactTokenLimitScope;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::ServiceTier;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::items::PlanItem;
use codex_protocol::items::TurnItem;
use codex_protocol::items::build_hook_prompt_message;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::ContentItem;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::MessagePhase;
use codex_protocol::models::ReasoningItemContent;
use codex_protocol::models::ReasoningItemReasoningSummary;
use codex_protocol::models::ResponseInputItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AgentMessageContentDeltaEvent;
use codex_protocol::protocol::AgentReasoningSectionBreakEvent;
use codex_protocol::protocol::CodexErrorInfo;
use codex_protocol::protocol::ErrorEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::InternalSessionSource;
use codex_protocol::protocol::PlanDeltaEvent;
use codex_protocol::protocol::RawResponseCompletedEvent;
use codex_protocol::protocol::ReasoningContentDeltaEvent;
use codex_protocol::protocol::ReasoningRawContentDeltaEvent;
use codex_protocol::protocol::SafetyBufferingEvent;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::TurnDiffEvent;
use codex_protocol::protocol::WarningEvent;
use codex_protocol::user_input::UserInput;
use codex_skills::ToolMentionKind;
use codex_skills::app_id_from_path;
use codex_skills::build_skill_name_counts;
use codex_skills::collect_explicit_skill_mentions;
use codex_skills::tool_kind_for_path;
use codex_skills_extension::HostSkillPrompts;
use codex_skills_extension::InjectedHostSkillPrompts;
use codex_thread_store::PersistContext;
use codex_tools::DiscoverableTool;
use codex_tools::ToolName;
use codex_tools::filter_request_plugin_install_discoverable_tools_for_client;
use codex_utils_path_uri::PathUri;
use codex_utils_stream_parser::AssistantTextChunk;
use codex_utils_stream_parser::AssistantTextStreamParser;
use codex_utils_stream_parser::ProposedPlanSegment;
use codex_utils_stream_parser::extract_proposed_plan_text;
use codex_utils_stream_parser::strip_citations;
use futures::prelude::*;
use futures::stream::FuturesOrdered;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing::error;
use tracing::field;
use tracing::info;
use tracing::instrument;
use tracing::trace;
use tracing::trace_span;
use tracing::warn;

const POST_SAMPLING_TOKEN_ESTIMATE_TARGET: &str = "codex_core::post_sampling_token_estimate";

/// Provider startup state for a regular turn.
///
/// A cooling routing candidate defers provider session creation, startup prewarm consumption, and
/// pre-sampling compaction until the accepted input has crossed the rollout durability barrier.
pub(crate) enum RunTurnProviderStartup {
    Ready(Option<Box<ModelClientSession>>),
    DeferredForRoutingCooldown,
}

/// Takes initial turn input and runs a loop where, at each sampling request,
/// the model replies with either:
///
/// - requested function calls
/// - an assistant message
///
/// While it is possible for the model to return multiple of these items in a
/// single sampling request, in practice, we generally one item per sampling request:
///
/// - If the model requests a function call, we execute it and send the output
///   back to the model in the next sampling request.
/// - If the model sends only an assistant message, we record it in the
///   conversation history and consider the turn complete.
///
pub(crate) async fn run_turn(
    sess: Arc<Session>,
    mut turn_context: Arc<TurnContext>,
    input: Vec<TurnInput>,
    provider_startup: RunTurnProviderStartup,
    cancellation_token: CancellationToken,
) -> CodexResult<Option<String>> {
    // Record results from hooks that finished after the previous turn before this turn's user prompt.
    drain_async_hook_results(&sess, &turn_context, /*before_user_prompt*/ true).await;

    let deferred_provider_startup = matches!(
        provider_startup,
        RunTurnProviderStartup::DeferredForRoutingCooldown
    );
    let mut client_session = match provider_startup {
        RunTurnProviderStartup::Ready(prewarmed_client_session) => Some(
            prewarmed_client_session
                .map(|client_session| *client_session)
                .unwrap_or_else(|| sess.services.model_client.new_session()),
        ),
        RunTurnProviderStartup::DeferredForRoutingCooldown => None,
    };
    let mut attempted_routing_candidates = HashSet::new();
    if let Some(candidate) = turn_context.model_routing_candidate.clone() {
        attempted_routing_candidates.insert(candidate);
    }
    let mut initial_routing_change_pending = turn_context.model_profile.is_some();
    let mut interrupted_response_recorded = false;
    // TODO(ccunningham): Pre-turn compaction runs before context updates and the
    // new user message are recorded. Estimate pending incoming items (context
    // diffs/full reinjection + user input) and trigger compaction preemptively
    // when they would push the thread over the compaction threshold.
    if let Some(client_session) = client_session.as_mut()
        && let Err(err) =
            run_pre_sampling_compact(&sess, &turn_context, client_session, &cancellation_token)
                .await
    {
        if matches!(err.details(), CodexErrorDetails::TurnAborted) {
            run_hooks_and_record_inputs(&sess, &turn_context, &input, PersistContext::Standard)
                .await;
            return Err(err);
        }
        if matches!(err.details(), CodexErrorDetails::ToolCollision(_)) {
            return Err(err);
        }
        let error = err.to_codex_protocol_error();
        sess.emit_turn_error_lifecycle(turn_context.as_ref(), error.clone())
            .await;
        error!("Failed to run pre-sampling compact");
        return Ok(None);
    }

    let user_input = turn_user_input(&input);
    let (required_servers, mentioned_plugins) =
        match required_mcp_servers_for_input(&sess, turn_context.as_ref(), &user_input)
            .or_cancel(&cancellation_token)
            .await
        {
            Ok(requirements) => requirements,
            Err(err) => {
                run_hooks_and_record_inputs(&sess, &turn_context, &input, PersistContext::Standard)
                    .await;
                return Err(err.into());
            }
        };

    // run_turn owns the step used to seed context and make the first sampling request.
    let mut first_step_context = match sess
        .capture_step_context_with_required_mcp_servers(
            Arc::clone(&turn_context),
            &cancellation_token,
            &required_servers,
        )
        .await
    {
        Ok(step_context) => step_context,
        Err(err) if matches!(err.details(), CodexErrorDetails::TurnAborted) => {
            run_hooks_and_record_inputs(&sess, &turn_context, &input, PersistContext::Standard)
                .await;
            return Err(err);
        }
        Err(err) => return Err(err),
    };
    // Keep the exact model-visible state used by this turn and its inline compactions.
    let (world_state, display_roots) = tokio::join!(
        sess.record_context_updates_and_set_reference_context_item(first_step_context.as_ref()),
        async {
            if first_step_context
                .turn
                .config
                .features
                .enabled(Feature::CwdRelativeTurnDiffs)
            {
                first_step_context
                    .environments
                    .turn_environments()
                    .map(|environment| {
                        (
                            environment.selection().environment_id,
                            environment.cwd().clone(),
                        )
                    })
                    .collect()
            } else {
                turn_diff_display_roots(first_step_context.as_ref()).await
            }
        },
    );
    let mut world_state = world_state?;

    let Some((injection_items, explicitly_enabled_connectors)) = build_skills_and_plugins(
        &sess,
        first_step_context.as_ref(),
        &user_input,
        &mentioned_plugins,
        &cancellation_token,
    )
    .await
    else {
        return Ok(None);
    };

    if run_pending_session_start_hooks(&sess, &turn_context).await {
        return Ok(None);
    }
    let mut can_drain_pending_input = input.is_empty();
    if run_hooks_and_record_inputs(&sess, &turn_context, &input, PersistContext::TurnStart).await {
        return Ok(None);
    }

    if deferred_provider_startup {
        sess.flush_rollout().await?;
        if !sess
            .wait_for_model_routing_retry(turn_context.model_routing_retry_at, &cancellation_token)
            .await
        {
            return Err(CodexErr::TurnAborted);
        }
        let prewarmed_client_session = match sess
            .consume_startup_prewarm_for_regular_turn(&cancellation_token)
            .await
        {
            SessionStartupPrewarmResolution::Cancelled => {
                return Err(CodexErr::TurnAborted);
            }
            SessionStartupPrewarmResolution::Unavailable { .. } => None,
            SessionStartupPrewarmResolution::Ready(prewarmed_client_session) => {
                Some(*prewarmed_client_session)
            }
        };
        let active_client_session = client_session.insert(
            prewarmed_client_session.unwrap_or_else(|| sess.services.model_client.new_session()),
        );
        if let Err(err) = run_pre_sampling_compact(
            &sess,
            &turn_context,
            active_client_session,
            &cancellation_token,
        )
        .await
        {
            if matches!(err.details(), CodexErrorDetails::TurnAborted) {
                return Err(err);
            }
            if matches!(err.details(), CodexErrorDetails::ToolCollision(_)) {
                return Err(err);
            }
            let error = err.to_codex_protocol_error();
            sess.emit_turn_error_lifecycle(turn_context.as_ref(), error.clone())
                .await;
            error!("Failed to run pre-sampling compact");
            return Ok(None);
        }
        first_step_context = sess
            .capture_step_context_with_required_mcp_servers(
                Arc::clone(&turn_context),
                &cancellation_token,
                &required_servers,
            )
            .await?;
        world_state = sess
            .record_context_updates_and_set_reference_context_item(first_step_context.as_ref())
            .await?;
    }

    let Some(mut client_session) = client_session else {
        return Err(CodexErr::Fatal(
            "provider startup did not complete before the first sampling request".to_string(),
        ));
    };

    sess.merge_connector_selection(explicitly_enabled_connectors.clone())
        .await;
    sess.set_previous_turn_settings(Some(PreviousTurnSettings {
        model: turn_context.model_info().slug.clone(),
        comp_hash: turn_context.model_info().comp_hash.clone(),
        realtime_active: Some(turn_context.realtime_active),
    }))
    .await;
    for response_item in injection_items {
        sess.record_conversation_items(&turn_context, std::slice::from_ref(&response_item))
            .await;
    }

    track_turn_resolved_config_analytics(&sess, &turn_context, &input).await;

    let mut last_agent_message: Option<String> = None;
    let mut stop_hook_active = false;
    // Although from the perspective of codex.rs, TurnDiffTracker has the lifecycle of a Task which contains
    // many turns, from the perspective of the user, it is a single turn.
    let turn_diff_tracker = Arc::new(tokio::sync::Mutex::new(
        TurnDiffTracker::with_environment_display_roots(display_roots),
    ));

    // `ModelClientSession` is turn-scoped and caches WebSocket + sticky routing state, so we reuse
    // one instance across retries within this turn.
    // Pending input is drained into history before building the next model request.
    // However, we defer that drain until after sampling in two cases:
    // 1. At the start of a turn, so the fresh turn input in `input` gets sampled first.
    // 2. After auto-compact, when model/tool continuation needs to resume before any steer.

    let mut next_step_context = Some(first_step_context);
    'turn: loop {
        // Note that pending_input would be something like a message the user
        // submitted through the UI while the model was running. Though the UI
        // may support this, the model might not.
        let pending_input = if can_drain_pending_input {
            sess.input_queue
                .get_pending_input(&sess.active_turn)
                .await
                .0
        } else {
            Vec::new()
        };

        if run_hooks_and_record_inputs(
            &sess,
            &turn_context,
            &pending_input,
            PersistContext::Standard,
        )
        .await
        {
            break;
        }

        let window_id = sess.current_window_id().await;
        super::rollout_budget::maybe_record_reminder(
            sess.as_ref(),
            turn_context.as_ref(),
            &window_id,
        )
        .await;

        // Capture once so context, advertised tools, and tool calls share one request view.
        // Pending steering is authoritative for the next request. A context captured before the
        // steering arrived cannot contain MCP servers mentioned by that steering.
        let prepared_step_context = next_step_context.take();
        let step_context = match (prepared_step_context, pending_input.is_empty()) {
            (Some(step_context), true) => step_context,
            (None, true) => {
                sess.capture_step_context(Arc::clone(&turn_context), &cancellation_token)
                    .await?
            }
            (prepared_step_context, false) => {
                let pending_user_input = turn_user_input(&pending_input);
                let (mut required_servers, _) = required_mcp_servers_for_input(
                    &sess,
                    turn_context.as_ref(),
                    &pending_user_input,
                )
                .or_cancel(&cancellation_token)
                .await?;
                if let Some(prepared_step_context) = prepared_step_context {
                    for server in &prepared_step_context.required_mcp_servers {
                        if !required_servers.contains(server) {
                            required_servers.push(server.clone());
                        }
                    }
                }
                sess.capture_step_context_with_required_mcp_servers(
                    Arc::clone(&turn_context),
                    &cancellation_token,
                    &required_servers,
                )
                .await?
            }
        };
        let sampling_request_result = async {
            super::time_reminder::maybe_record_current_time_reminder(
                sess.as_ref(),
                turn_context.as_ref(),
                &window_id,
            )
            .await?;

            world_state = sess
                .record_step_world_state_if_changed(&world_state, step_context.as_ref())
                .await?;

            // Construct the input that we will send to the model.
            let sampling_request_input: Vec<ResponseItem> = async {
                sess.clone_history()
                    .await
                    .for_prompt(&step_context.settings.model_info.input_modalities)
            }
            .instrument(trace_span!("run_turn.prepare_sampling_request_input"))
            .await;

            let responses_metadata = sess
                .responses_metadata(turn_context.as_ref(), CodexResponsesRequestKind::Turn)
                .await;
            run_sampling_request(
                Arc::clone(&sess),
                Arc::clone(&step_context),
                Arc::clone(&turn_context.extension_data),
                Arc::clone(&turn_diff_tracker),
                &mut client_session,
                &responses_metadata,
                sampling_request_input,
                cancellation_token.child_token(),
                &mut interrupted_response_recorded,
            )
            .await
        }
        .await;
        match sampling_request_result {
            Ok((sampling_request_output, sampling_request_input)) => {
                if turn_context.model_profile.is_some() {
                    if initial_routing_change_pending
                        && attempted_routing_candidates.len() == 1
                        && let (Some(previous), Some(reason)) = (
                            turn_context.model_routing_previous_candidate.as_ref(),
                            turn_context.model_routing_selection_reason,
                        )
                    {
                        sess.notify_model_routing_candidate_change(previous, &turn_context, reason)
                            .await;
                    }
                    sess.record_model_routing_success(turn_context.as_ref())
                        .await;
                    initial_routing_change_pending = false;
                }
                let SamplingRequestResult {
                    needs_follow_up: model_needs_follow_up,
                    last_agent_message: sampling_request_last_agent_message,
                    refresh_turn_context,
                } = sampling_request_output;
                if refresh_turn_context {
                    let refreshed_turn_context = sess
                        .refresh_active_turn_context(turn_context.as_ref())
                        .await;
                    let refreshed_step_context = sess
                        .capture_step_context(
                            Arc::clone(&refreshed_turn_context),
                            &cancellation_token,
                        )
                        .await?;
                    let display_roots =
                        turn_diff_display_roots(refreshed_step_context.as_ref()).await;
                    turn_diff_tracker
                        .lock()
                        .await
                        .set_environment_display_roots(display_roots);
                    world_state = sess
                        .record_context_updates_and_set_reference_context_item(
                            refreshed_step_context.as_ref(),
                        )
                        .await?;
                    turn_context = refreshed_turn_context;
                    next_step_context = Some(refreshed_step_context);
                }
                if model_needs_follow_up {
                    sess.input_queue
                        .accept_mailbox_delivery_for_current_turn(
                            &sess.active_turn,
                            &turn_context.sub_id,
                        )
                        .await;
                }
                can_drain_pending_input = true;
                // Process async hooks only after sampling and its tools have finished.
                drain_async_hook_results(&sess, &turn_context, /*before_user_prompt*/ false).await;
                let (has_pending_input, token_status) = async {
                    let has_pending_input =
                        sess.input_queue.has_pending_input(&sess.active_turn).await;
                    let token_status = super::context_window::context_window_token_status(
                        sess.as_ref(),
                        turn_context.as_ref(),
                    )
                    .await;
                    (has_pending_input, token_status)
                }
                .instrument(trace_span!("run_turn.collect_post_sampling_state"))
                .await;
                let needs_follow_up = model_needs_follow_up || has_pending_input;
                let token_limit_reached = token_status.token_limit_reached;

                trace!(
                    turn_id = %turn_context.sub_id,
                    total_usage_tokens = token_status.active_context_tokens,
                    auto_compact_scope_tokens = token_status.auto_compact_scope_tokens,
                    auto_compact_scope_limit = ?token_status.auto_compact_scope_limit,
                    auto_compact_limit_scope = ?turn_context.config.model_auto_compact_token_limit_scope,
                    auto_compact_window_prefill_tokens = ?token_status.auto_compact_window_prefill_tokens,
                    full_context_window_limit = ?token_status.full_context_window_limit,
                    full_context_window_limit_reached = token_status.full_context_window_limit_reached,
                    token_limit_reached,
                    model_needs_follow_up,
                    has_pending_input,
                    needs_follow_up,
                    "post sampling token usage"
                );
                if tracing::event_enabled!(
                    target: POST_SAMPLING_TOKEN_ESTIMATE_TARGET,
                    tracing::Level::TRACE,
                    turn_id,
                    estimated_token_count,
                    message
                ) {
                    let estimated_token_count =
                        sess.get_estimated_token_count(turn_context.as_ref()).await;
                    trace!(
                        target: POST_SAMPLING_TOKEN_ESTIMATE_TARGET,
                        turn_id = %turn_context.sub_id,
                        estimated_token_count = ?estimated_token_count,
                        "post sampling token estimate"
                    );
                }

                let should_roll_over = needs_follow_up
                    && (sess.take_new_context_window_request().await || token_limit_reached);
                let allow_auto_compact_fallback = !should_roll_over && !token_limit_reached;
                super::token_budget::maybe_record(
                    sess.as_ref(),
                    turn_context.as_ref(),
                    token_status.base_window_tokens_remaining,
                    allow_auto_compact_fallback,
                )
                .await;

                // as long as compaction works well in getting us way below the token limit, we shouldn't worry about being in an infinite loop.
                if should_roll_over {
                    if let Err(err) = run_auto_compact(
                        &sess,
                        Arc::clone(&step_context),
                        /*fallback_step_context*/ None,
                        &mut client_session,
                        InitialContextInjection::BeforeLastUserMessage {
                            world_state: Arc::clone(&world_state),
                            step_context: Arc::clone(&step_context),
                        },
                        CompactionReason::ContextLimit,
                        CompactionPhase::MidTurn,
                        CompactionReporting::Immediate,
                    )
                    .await
                    {
                        if matches!(err.details(), CodexErrorDetails::TurnAborted) {
                            return Err(err);
                        }
                        let error = err.to_codex_protocol_error();
                        sess.emit_turn_error_lifecycle(turn_context.as_ref(), error.clone())
                            .await;
                        return Ok(None);
                    }
                    if run_pending_session_start_hooks(&sess, &turn_context).await {
                        return Ok(None);
                    }
                    can_drain_pending_input = !model_needs_follow_up;
                    continue;
                }

                if !needs_follow_up {
                    last_agent_message = sampling_request_last_agent_message;
                    let stop_outcome = run_turn_stop_hooks(
                        &sess,
                        &step_context,
                        stop_hook_active,
                        last_agent_message.clone(),
                    )
                    .await;
                    if matches!(
                        turn_context.session_source,
                        SessionSource::Internal(InternalSessionSource::MemoryConsolidation)
                    ) && (stop_outcome.should_block || stop_outcome.should_stop)
                    {
                        // Do not feed managed rejections back into an unattended memory loop.
                        return Err(CodexErr::InvalidRequest(
                            "Memory consolidation was rejected by a Stop hook.".to_string(),
                        ));
                    }
                    if stop_outcome.should_block {
                        if let Some(hook_prompt_message) =
                            build_hook_prompt_message(&stop_outcome.continuation_fragments)
                        {
                            sess.record_response_item_and_emit_turn_item(
                                &turn_context,
                                hook_prompt_message,
                            )
                            .await;
                            sess.input_queue
                                .accept_mailbox_delivery_for_current_turn(
                                    &sess.active_turn,
                                    &turn_context.sub_id,
                                )
                                .await;
                            stop_hook_active = true;
                            continue;
                        } else {
                            sess.send_event(
                                &turn_context,
                                EventMsg::Warning(WarningEvent {
                                    message: "Stop hook requested continuation without a prompt; ignoring the block.".to_string(),
                                }),
                            )
                            .await;
                        }
                    }
                    if stop_outcome.should_stop {
                        break;
                    }
                    if run_legacy_after_agent_hook(
                        &sess,
                        &turn_context,
                        &sampling_request_input,
                        last_agent_message.clone(),
                    )
                    .await
                    {
                        return Ok(None);
                    }
                    break;
                }
                continue;
            }
            Err(failure) if matches!(failure.error.details(), CodexErrorDetails::TurnAborted) => {
                return Err(failure.error);
            }
            Err(failure)
                if matches!(
                    failure.error.details(),
                    CodexErrorDetails::InvalidImageRequest()
                ) =>
            {
                let codex_error = failure.error;
                sess.track_turn_codex_error(turn_context.as_ref(), &codex_error);
                let error = CodexErrorInfo::BadRequest;
                sess.emit_turn_error_lifecycle(turn_context.as_ref(), error.clone())
                    .await;
                let event = EventMsg::Error(ErrorEvent {
                    misalignment: None,
                    message: "Invalid image in your last message. Please remove it and try again."
                        .to_string(),
                    codex_error_info: Some(error),
                });
                sess.send_event(&turn_context, event).await;
                break;
            }
            Err(failure) => {
                let routing_failure = classify_model_routing_failure(failure.error.details());
                if let Some(routing_failure) = routing_failure.as_ref() {
                    sess.record_model_routing_failure(turn_context.as_ref(), routing_failure)
                        .await;
                }
                if failure.reroute_safe
                    && let Some(routing_failure) = routing_failure
                    && let Some(profile_name) = turn_context.model_profile.clone()
                {
                    let mut last_compaction_error = None;
                    while let Some(selection) = sess
                        .select_model_routing_context(
                            turn_context.as_ref(),
                            &profile_name,
                            &attempted_routing_candidates,
                        )
                        .await
                    {
                        if !sess
                            .wait_for_model_routing_retry(selection.retry_at, &cancellation_token)
                            .await
                        {
                            return Err(CodexErr::TurnAborted);
                        }
                        let mut routed = selection.context;
                        routed.model_routing_previous_candidate = None;
                        routed.model_routing_selection_reason = None;
                        let routed = Arc::new(routed);
                        if let Some(candidate) = routed.model_routing_candidate.clone() {
                            attempted_routing_candidates.insert(candidate);
                        }
                        sess.services
                            .thread_extension_data
                            .insert(routed.model_info().as_ref().clone());
                        let routed_step_context = match sess
                            .capture_step_context_with_required_mcp_servers(
                                Arc::clone(&routed),
                                &cancellation_token,
                                &step_context.required_mcp_servers,
                            )
                            .await
                        {
                            Ok(routed_step_context) => routed_step_context,
                            Err(err) => {
                                sess.services
                                    .thread_extension_data
                                    .insert(turn_context.model_info().as_ref().clone());
                                return Err(err);
                            }
                        };
                        client_session.reset_for_model_reroute();
                        let compacted = match maybe_run_model_reroute_inline_compact(
                            &sess,
                            &turn_context,
                            &routed_step_context,
                            &world_state,
                            &mut client_session,
                        )
                        .await
                        {
                            Ok(compacted) => compacted,
                            Err(err)
                                if matches!(
                                    err.details(),
                                    CodexErrorDetails::Interrupted | CodexErrorDetails::TurnAborted
                                ) =>
                            {
                                sess.services
                                    .thread_extension_data
                                    .insert(turn_context.model_info().as_ref().clone());
                                return Err(err);
                            }
                            Err(err) => {
                                if let Some(compaction_failure) =
                                    classify_model_routing_failure(err.details())
                                {
                                    sess.record_model_routing_failure(
                                        routed.as_ref(),
                                        &compaction_failure,
                                    )
                                    .await;
                                    sess.services
                                        .thread_extension_data
                                        .insert(turn_context.model_info().as_ref().clone());
                                    client_session.reset_for_model_reroute();
                                    last_compaction_error = Some(err);
                                    continue;
                                }
                                sess.services
                                    .thread_extension_data
                                    .insert(turn_context.model_info().as_ref().clone());
                                info!("Turn error during model reroute compaction: {err:#}");
                                let error = err.to_codex_protocol_error();
                                sess.emit_turn_error_lifecycle(
                                    turn_context.as_ref(),
                                    error.clone(),
                                )
                                .await;
                                sess.track_turn_codex_error(routed.as_ref(), &err);
                                let event =
                                    EventMsg::Error(err.to_error_event(/*message_prefix*/ None));
                                sess.send_event(&turn_context, event).await;
                                return Ok(None);
                            }
                        };
                        if compacted {
                            // Compaction is its own provider request. Sampling starts a clean route
                            // segment and rebuilds from the compacted local history.
                            client_session.reset_for_model_reroute();
                            sess.notify_model_routing_change(
                                turn_context.as_ref(),
                                &routed,
                                routing_failure.reason,
                            )
                            .await;
                            sess.set_previous_turn_settings(Some(PreviousTurnSettings {
                                model: routed.model_info().slug.clone(),
                                comp_hash: routed.model_info().comp_hash.clone(),
                                realtime_active: Some(routed.realtime_active),
                            }))
                            .await;
                            turn_context = Arc::clone(&routed);
                            if let PostCompactHookOutcome::Stopped = run_post_compact_hooks(
                                &sess,
                                &turn_context,
                                CompactionTrigger::Auto,
                            )
                            .await
                            {
                                return Err(CodexErr::TurnAborted);
                            }
                        }
                        let routed_step_context = if compacted {
                            sess.capture_step_context_with_required_mcp_servers(
                                Arc::clone(&routed),
                                &cancellation_token,
                                &step_context.required_mcp_servers,
                            )
                            .await?
                        } else {
                            routed_step_context
                        };
                        world_state = match sess
                            .record_context_updates_and_set_reference_context_item(
                                routed_step_context.as_ref(),
                            )
                            .await
                        {
                            Ok(world_state) => world_state,
                            Err(err) if !compacted => {
                                sess.services
                                    .thread_extension_data
                                    .insert(turn_context.model_info().as_ref().clone());
                                return Err(err);
                            }
                            Err(err) => return Err(err),
                        };
                        if !compacted {
                            sess.notify_model_routing_change(
                                turn_context.as_ref(),
                                &routed,
                                routing_failure.reason,
                            )
                            .await;
                            sess.set_previous_turn_settings(Some(PreviousTurnSettings {
                                model: routed.model_info().slug.clone(),
                                comp_hash: routed.model_info().comp_hash.clone(),
                                realtime_active: Some(routed.realtime_active),
                            }))
                            .await;
                            turn_context = routed;
                        }
                        if failure.interrupted_response && !interrupted_response_recorded {
                            record_interrupted_response(sess.as_ref(), turn_context.as_ref()).await;
                            interrupted_response_recorded = true;
                        }
                        next_step_context = Some(routed_step_context);
                        // The failed request no longer owns the mailbox boundary. Steering that
                        // arrived during X belongs in Y's next sampling request in the same user turn.
                        can_drain_pending_input = true;
                        continue 'turn;
                    }
                    if let Some(err) = last_compaction_error {
                        sess.services
                            .thread_extension_data
                            .insert(turn_context.model_info().as_ref().clone());
                        info!("Turn error during model reroute compaction: {err:#}");
                        let error = err.to_codex_protocol_error();
                        sess.emit_turn_error_lifecycle(turn_context.as_ref(), error.clone())
                            .await;
                        sess.track_turn_codex_error(turn_context.as_ref(), &err);
                        let event = EventMsg::Error(err.to_error_event(/*message_prefix*/ None));
                        sess.send_event(&turn_context, event).await;
                        return Ok(None);
                    }
                }
                let e = failure.error;
                info!("Turn error: {e:#}");
                let error = e.to_codex_protocol_error();
                sess.emit_turn_error_lifecycle(turn_context.as_ref(), error.clone())
                    .await;
                sess.track_turn_codex_error(turn_context.as_ref(), &e);
                let event = EventMsg::Error(e.to_error_event(/*message_prefix*/ None));
                sess.send_event(&turn_context, event).await;
                // let the user continue the conversation
                break;
            }
        }
    }

    Ok(last_agent_message)
}

struct SamplingRequestFailure {
    error: CodexErr,
    reroute_safe: bool,
    interrupted_response: bool,
}

impl From<CodexErr> for SamplingRequestFailure {
    fn from(error: CodexErr) -> Self {
        Self {
            error,
            reroute_safe: false,
            interrupted_response: false,
        }
    }
}

#[instrument(level = "trace", skip_all)]
async fn turn_diff_display_roots(step_context: &StepContext) -> Vec<(String, PathUri)> {
    let mut display_roots = Vec::new();
    for turn_environment in step_context.environments.turn_environments() {
        let cwd = turn_environment.cwd();
        // A turn cwd is expected to be a directory. If it is a file, the failed `<cwd>/.git` probe
        // is ignored and ancestor search continues from its parent.
        let root = find_nearest_ancestor_with_markers(
            turn_environment.environment.get_filesystem().as_ref(),
            cwd,
            vec![".git".to_string()],
            FindUpErrorPolicy::Ignore,
            /*sandbox*/ None,
        )
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| cwd.clone());
        display_roots.push((turn_environment.selection.environment_id.clone(), root));
    }
    display_roots
}

#[instrument(level = "trace", skip_all)]
pub(crate) async fn run_hooks_and_record_inputs(
    sess: &Arc<Session>,
    turn_context: &Arc<TurnContext>,
    input: &[TurnInput],
    persist_context: PersistContext,
) -> bool {
    let mut blocked_input = false;
    let mut accepted_user_input = false;
    for input_item in input {
        let hook_outcome = inspect_pending_input(sess, turn_context, input_item).await;
        if hook_outcome.should_stop {
            blocked_input = true;
            record_additional_contexts(sess, turn_context, hook_outcome.additional_contexts).await;
        } else {
            if matches!(input_item, TurnInput::UserInput { content, .. } if !content.is_empty()) {
                accepted_user_input = true;
            }
            record_pending_input(
                sess,
                turn_context,
                input_item.clone(),
                hook_outcome.additional_contexts,
                persist_context,
            )
            .await;
        }
    }
    blocked_input && !accepted_user_input
}

fn turn_user_input(input: &[TurnInput]) -> Vec<UserInput> {
    input
        .iter()
        .filter_map(|item| match item {
            TurnInput::UserInput { content, .. } => Some(content.as_slice()),
            TurnInput::ResponseItem(_)
            | TurnInput::FunctionCallOutput(_)
            | TurnInput::InterAgentCommunication(_) => None,
        })
        .flatten()
        .cloned()
        .collect()
}

async fn required_mcp_servers_for_input(
    sess: &Arc<Session>,
    turn_context: &TurnContext,
    user_input: &[UserInput],
) -> (Vec<String>, Vec<crate::plugins::PluginCapabilitySummary>) {
    if crate::guardian::is_basic_session_source(&turn_context.session_source) {
        return (Vec::new(), Vec::new());
    }

    // Plugin capabilities depend on authentication, so project them only after
    // the runtime has aligned the plugin manager with its current account.
    sess.refresh_mcp_if_dirty().await;
    let loaded_plugins = sess
        .services
        .plugins_manager
        .plugins_for_config(&turn_context.config.plugins_config_input())
        .await;
    let current_config = sess.services.mcp_runtime.current_config();
    let mentioned_plugins =
        collect_explicit_plugin_mentions(user_input, loaded_plugins.capability_summaries());
    let mut required_servers = mentioned_plugins
        .iter()
        .flat_map(|plugin| plugin.mcp_server_names.iter().cloned())
        .collect::<HashSet<_>>();

    let messages = user_input
        .iter()
        .filter_map(|input| match input {
            UserInput::Text { text, .. } => Some(text.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mentions = collect_tool_mentions_from_messages(&messages);
    let paths = user_input
        .iter()
        .filter_map(|input| match input {
            UserInput::Mention { path, .. } => Some(path.clone()),
            _ => None,
        })
        .chain(mentions.paths);
    required_servers.extend(paths.filter_map(|path| {
        path.strip_prefix("mcp://")
            .filter(|server| !server.is_empty())
            .map(str::to_string)
    }));

    let connector_slug_counts = if turn_context.apps_enabled() && !mentions.plain_names.is_empty() {
        let cached_connectors =
            connectors::list_cached_accessible_connectors_from_mcp_tools(&turn_context.config)
                .await;
        let accessible_connectors = match cached_connectors {
            Some(connectors) => connectors,
            None => sess
                .services
                .mcp_runtime
                .current_binding()
                .await
                .map(|binding| connectors::accessible_connectors_from_mcp_tools(binding.tools()))
                .unwrap_or_default(),
        };
        let connector_ids = current_config
            .iter()
            .flat_map(|config| config.connector_snapshot.connector_ids())
            .map(|connector_id| connector_id.0.clone());
        build_connector_slug_counts(
            &codex_connectors::merge::merge_plugin_connectors_with_accessible(
                connector_ids,
                accessible_connectors,
            ),
        )
    } else {
        HashMap::new()
    };
    let skills_snapshot = turn_context.skills_snapshot();
    let skills_outcome = skills_snapshot.outcome();
    let mentioned_skills =
        collect_explicit_skill_mentions(user_input, skills_outcome, &connector_slug_counts);
    for skill in mentioned_skills {
        if let Some(dependencies) = skill.dependencies {
            required_servers.extend(
                dependencies
                    .tools
                    .into_iter()
                    .filter(|tool| tool.r#type.eq_ignore_ascii_case("mcp"))
                    .map(|tool| tool.value),
            );
        }
        if let Some(plugin_id) = skill.plugin_id.as_deref()
            && let Some(plugin) = loaded_plugins
                .capability_summaries()
                .iter()
                .find(|plugin| plugin.config_name == plugin_id)
        {
            required_servers.extend(plugin.mcp_server_names.iter().cloned());
        }
    }

    (required_servers.into_iter().collect(), mentioned_plugins)
}

#[instrument(level = "trace", skip_all)]
async fn build_skills_and_plugins(
    sess: &Arc<Session>,
    step_context: &StepContext,
    user_input: &[UserInput],
    mentioned_plugins: &[crate::plugins::PluginCapabilitySummary],
    cancellation_token: &CancellationToken,
) -> Option<(Vec<ResponseItem>, HashSet<String>)> {
    let turn_context = step_context.turn.as_ref();
    // Guardian input embeds the parent transcript as untrusted evidence. Do not interpret skill or
    // plugin mentions from that generated prompt as requests to inject additional instructions.
    if crate::guardian::is_basic_session_source(&turn_context.session_source) {
        return Some((Vec::new(), HashSet::new()));
    }

    let tracking = build_track_events_context(
        turn_context.model_info().slug.clone(),
        sess.thread_id.to_string(),
        turn_context.sub_id.clone(),
        turn_context.originator.clone(),
    );
    let connector_snapshot = step_context.mcp.config().connector_snapshot.clone();
    let mcp_tools = if turn_context.apps_enabled() || !mentioned_plugins.is_empty() {
        // Plugin mentions need raw MCP/app inventory even when app tools
        // are normally hidden so we can describe the plugin's currently
        // usable capabilities for this turn.
        step_context.mcp.tools()
    } else {
        &[]
    };
    let available_connectors = if turn_context.apps_enabled() {
        let connectors = codex_connectors::merge::merge_plugin_connectors_with_accessible(
            connector_snapshot
                .connector_ids()
                .iter()
                .map(|connector_id| connector_id.0.clone()),
            connectors::accessible_connectors_from_mcp_tools(mcp_tools),
        );
        AppToolPolicyEvaluator::new(&turn_context.config.config_layer_stack)
            .apply_app_enabled_state(connectors)
    } else {
        Vec::new()
    };
    let skills_snapshot = turn_context.skills_snapshot();
    let skills_outcome = skills_snapshot.outcome();
    let connector_slug_counts = build_connector_slug_counts(&available_connectors);
    let extension_injection_items =
        build_extension_turn_input_items(sess, step_context, user_input, cancellation_token)
            .await?;
    let skill_name_counts_lower =
        build_skill_name_counts(&skills_outcome.skills, &skills_outcome.disabled_paths).1;
    let mentioned_skills =
        collect_explicit_skill_mentions(user_input, skills_outcome, &connector_slug_counts);
    maybe_prompt_and_install_mcp_dependencies(
        sess,
        turn_context,
        cancellation_token,
        &mentioned_skills,
        Some(sess.mcp_elicitation_reviewer()),
    )
    .await;

    let injected_host_skill_prompts = turn_context
        .extension_data
        .get::<InjectedHostSkillPrompts>();
    let HostSkillPrompts {
        fragments,
        injected: injected_host_skills,
        warnings: host_skill_warnings,
    } = skills_snapshot.load_skill_prompts(&mentioned_skills).await;
    emit_explicit_skill_invocations(
        sess,
        turn_context,
        &mentioned_skills,
        &injected_host_skills,
        tracking.clone(),
    )
    .await;
    for message in host_skill_warnings {
        sess.send_event(turn_context, EventMsg::Warning(WarningEvent { message }))
            .await;
    }
    let skill_items = fragments
        .into_iter()
        .map(ContextualUserFragment::into_boxed_response_item)
        .collect::<Vec<_>>();
    let skill_connector_ids = collect_explicit_app_ids_from_skill_items(
        &skill_items,
        &available_connectors,
        &skill_name_counts_lower,
    );
    let plugin_items = build_plugin_injections(mentioned_plugins, mcp_tools, &available_connectors);
    let mut explicitly_enabled_connectors = collect_explicit_app_ids(user_input);
    explicitly_enabled_connectors.extend(skill_connector_ids);
    let connector_names_by_id = available_connectors
        .iter()
        .map(|connector| (connector.id.as_str(), connector.name.as_str()))
        .collect::<HashMap<&str, &str>>();
    let mentioned_app_invocations = explicitly_enabled_connectors
        .iter()
        .map(|connector_id| AppInvocation {
            connector_id: Some(connector_id.clone()),
            app_name: connector_names_by_id
                .get(connector_id.as_str())
                .map(|name| (*name).to_string()),
            invocation_type: Some(InvocationType::Explicit),
        })
        .collect::<Vec<_>>();
    sess.services
        .analytics_events_client
        .track_app_mentioned(tracking.clone(), mentioned_app_invocations);
    for summary in mentioned_plugins {
        if let Some(plugin) = sess
            .services
            .plugins_manager
            .telemetry_metadata_for_capability_summary(summary)
        {
            sess.services
                .analytics_events_client
                .track_plugin_used(tracking.clone(), plugin);
        }
    }

    let mut injection_items = match injected_host_skill_prompts {
        Some(injected_host_skill_prompts) => skill_items
            .into_iter()
            .zip(injected_host_skills.iter())
            .filter_map(|(item, skill)| {
                (!injected_host_skill_prompts
                    .contains_path(&skill.path_to_skills_md.to_string_lossy()))
                .then_some(item)
            })
            .collect(),
        None => skill_items,
    };
    injection_items.extend(plugin_items);
    injection_items.extend(extension_injection_items);
    Some((injection_items, explicitly_enabled_connectors))
}

#[tracing::instrument(
    level = "trace",
    skip_all,
    fields(user_input_count = user_input.len())
)]
async fn build_extension_turn_input_items(
    sess: &Arc<Session>,
    step_context: &StepContext,
    user_input: &[UserInput],
    cancellation_token: &CancellationToken,
) -> Option<Vec<ResponseItem>> {
    let turn_context = step_context.turn.as_ref();
    let contributors = sess.services.extensions.turn_input_contributors().to_vec();
    if contributors.is_empty() {
        return Some(Vec::new());
    }

    let environments = step_context
        .environments
        .turn_environments()
        .enumerate()
        .map(|(index, environment)| TurnInputEnvironment {
            _lifetime: PhantomData,
            environment_id: environment.selection.environment_id.clone(),
            cwd: environment.cwd().clone(),
            is_primary: index == 0,
        })
        .collect::<Vec<_>>();

    let input = TurnInputContext {
        turn_id: turn_context.sub_id.to_string(),
        user_input: user_input.to_vec(),
        environments,
    };
    let extension_metrics =
        super::extension_metrics::from_session_telemetry(turn_context.session_telemetry.clone());

    let mut items = Vec::new();
    for contributor in contributors {
        let contributed_fragments = contributor
            .contribute(
                input.clone(),
                Some(Arc::clone(&extension_metrics)),
                &sess.services.session_extension_data,
                &sess.services.thread_extension_data,
                turn_context.extension_data.as_ref(),
            )
            .or_cancel(cancellation_token)
            .await
            .ok()?;
        items.extend(
            contributed_fragments
                .into_iter()
                .map(ContextualUserFragment::into_boxed_response_item),
        );
    }

    Some(items)
}

#[tracing::instrument(
    level = "trace",
    skip_all,
    fields(input_count = input.len())
)]
async fn track_turn_resolved_config_analytics(
    sess: &Session,
    turn_context: &TurnContext,
    input: &[TurnInput],
) {
    let thread_config = sess.thread_config_snapshot().await;
    let is_first_turn = {
        let mut state = sess.state.lock().await;
        state.take_next_turn_is_first()
    };
    sess.services
        .analytics_events_client
        .track_turn_resolved_config(TurnResolvedConfigFact {
            turn_id: turn_context.sub_id.clone(),
            thread_id: sess.thread_id.to_string(),
            turn_metadata: turn_context.turn_metadata_state.clone(),
            num_input_images: input
                .iter()
                .filter_map(|item| match item {
                    TurnInput::UserInput { content, .. } => Some(content.as_slice()),
                    TurnInput::ResponseItem(_)
                    | TurnInput::FunctionCallOutput(_)
                    | TurnInput::InterAgentCommunication(_) => None,
                })
                .flatten()
                .filter(|item| {
                    matches!(item, UserInput::Image { .. } | UserInput::LocalImage { .. })
                })
                .count(),
            submission_type: None,
            ephemeral: thread_config.ephemeral,
            session_source: thread_config.session_source,
            model: turn_context.model_info().slug.clone(),
            model_provider: turn_context.config.model_provider_id.clone(),
            permission_profile: turn_context.permission_profile(),
            #[allow(deprecated)]
            permission_profile_cwd: turn_context.cwd.to_path_buf(),
            reasoning_effort: turn_context.reasoning_effort().cloned(),
            reasoning_summary: Some(turn_context.reasoning_summary()),
            service_tier: turn_context
                .config
                .service_tier
                .as_deref()
                .and_then(ServiceTier::from_request_value),
            approval_policy: turn_context.approval_policy(),
            approvals_reviewer: turn_context.config.approvals_reviewer,
            sandbox_network_access: turn_context.network_sandbox_policy().is_enabled(),
            collaboration_mode: turn_context.mode(),
            personality: turn_context.personality(),
            workspace_kind: turn_context.turn_metadata_state.workspace_kind(),
            is_first_turn,
        });
}

#[instrument(level = "trace", skip_all)]
async fn run_pre_sampling_compact(
    sess: &Arc<Session>,
    turn_context: &Arc<TurnContext>,
    client_session: &mut ModelClientSession,
    cancellation_token: &CancellationToken,
) -> CodexResult<()> {
    maybe_run_previous_model_inline_compact(sess, turn_context, client_session, cancellation_token)
        .await?;
    let token_status =
        super::context_window::context_window_token_status(sess.as_ref(), turn_context.as_ref())
            .await;
    // Compact if the configured auto-compaction budget or usable context window is exhausted.
    if token_status.token_limit_reached {
        // Pre-turn compaction runs before run_turn creates the normal sampling step.
        let step_context = sess
            .capture_step_context(Arc::clone(turn_context), cancellation_token)
            .await?;
        run_auto_compact(
            sess,
            step_context,
            /*fallback_step_context*/ None,
            client_session,
            InitialContextInjection::DoNotInject,
            CompactionReason::ContextLimit,
            CompactionPhase::PreTurn,
            CompactionReporting::Immediate,
        )
        .await?;
    }
    Ok(())
}

/// Returns true only when both turns declare compaction compatibility hashes and they differ.
/// A missing hash does not provide enough information to trigger compaction.
fn comp_hash_changed(previous: Option<&str>, current: Option<&str>) -> bool {
    previous
        .zip(current)
        .is_some_and(|(previous, current)| previous != current)
}

/// Captures the current model's request-scoped state for retrying previous-model compaction.
///
/// Returns `None` when the active authentication does not use the Codex backend, the provider is
/// not OpenAI, or the previous and current model are the same.
async fn capture_current_model_fallback_step_context(
    sess: &Arc<Session>,
    turn_context: &Arc<TurnContext>,
    previous_model: &str,
    cancellation_token: &CancellationToken,
) -> CodexResult<Option<Arc<StepContext>>> {
    let uses_codex_backend = turn_context
        .auth_manager
        .as_deref()
        .is_some_and(codex_login::AuthManager::current_auth_uses_codex_backend);
    if !uses_codex_backend
        || !turn_context.provider.info().is_openai()
        || previous_model == turn_context.model_info().slug
    {
        return Ok(None);
    }
    sess.capture_speculative_step_context(Arc::clone(turn_context), cancellation_token)
        .await
        .map(Some)
}

/// Runs pre-sampling compaction against the previous model when its compaction compatibility
/// hash changed or when switching to a smaller context-window model.
///
/// Returns `Err(_)` only when compaction was attempted and failed.
async fn maybe_run_previous_model_inline_compact(
    sess: &Arc<Session>,
    turn_context: &Arc<TurnContext>,
    client_session: &mut ModelClientSession,
    cancellation_token: &CancellationToken,
) -> CodexResult<()> {
    let Some(previous_turn_settings) = sess.previous_turn_settings().await else {
        return Ok(());
    };
    let should_compact_for_comp_hash_change = comp_hash_changed(
        previous_turn_settings.comp_hash.as_deref(),
        turn_context.model_info().comp_hash.as_deref(),
    );
    let previous_model = previous_turn_settings.model;
    let previous_model_turn_context = Arc::new(
        turn_context
            .with_model(previous_model.clone(), &sess.services.models_manager)
            .await,
    );

    if should_compact_for_comp_hash_change {
        let step_context = sess
            .capture_step_context(Arc::clone(&previous_model_turn_context), cancellation_token)
            .await?;
        let fallback_step_context = capture_current_model_fallback_step_context(
            sess,
            turn_context,
            previous_model.as_str(),
            cancellation_token,
        )
        .await?;
        run_auto_compact(
            sess,
            step_context,
            fallback_step_context,
            client_session,
            InitialContextInjection::DoNotInject,
            CompactionReason::CompHashChanged,
            CompactionPhase::PreTurn,
            CompactionReporting::Immediate,
        )
        .await?;
        return Ok(());
    }

    let Some(old_context_window) = previous_model_turn_context.model_context_window() else {
        return Ok(());
    };
    let Some(new_context_window) = turn_context.model_context_window() else {
        return Ok(());
    };
    let active_context_tokens = sess.get_total_token_usage().await;
    let previous_model_limit_reached = match turn_context
        .config
        .model_auto_compact_token_limit_scope
    {
        AutoCompactTokenLimitScope::Total => {
            let new_auto_compact_limit = turn_context
                .model_info()
                .auto_compact_token_limit()
                .unwrap_or(i64::MAX);
            active_context_tokens > new_auto_compact_limit
                || active_context_tokens >= new_context_window
        }
        AutoCompactTokenLimitScope::BodyAfterPrefix => active_context_tokens >= new_context_window,
    };
    let should_run = previous_model_limit_reached
        && previous_model_turn_context.model_info().slug != turn_context.model_info().slug
        && old_context_window > new_context_window;
    if should_run {
        let step_context = sess
            .capture_step_context(Arc::clone(&previous_model_turn_context), cancellation_token)
            .await?;
        let fallback_step_context = capture_current_model_fallback_step_context(
            sess,
            turn_context,
            previous_model.as_str(),
            cancellation_token,
        )
        .await?;
        run_auto_compact(
            sess,
            step_context,
            fallback_step_context,
            client_session,
            InitialContextInjection::DoNotInject,
            CompactionReason::ModelDownshift,
            CompactionPhase::PreTurn,
            CompactionReporting::Immediate,
        )
        .await?;
    }
    Ok(())
}

/// Compacts a routing checkpoint directly with the replacement request configuration.
///
/// The failed configuration is not retried for compaction. The replacement configuration starts
/// a new provider route segment and compacts the locally reconstructed history before it becomes
/// the task's previous-turn configuration.
async fn maybe_run_model_reroute_inline_compact(
    sess: &Arc<Session>,
    previous_turn_context: &Arc<TurnContext>,
    routed_step_context: &Arc<StepContext>,
    world_state: &Arc<WorldState>,
    client_session: &mut ModelClientSession,
) -> CodexResult<bool> {
    let routed_turn_context = &routed_step_context.turn;
    let reason = if comp_hash_changed(
        previous_turn_context.model_info().comp_hash.as_deref(),
        routed_turn_context.model_info().comp_hash.as_deref(),
    ) {
        Some(CompactionReason::CompHashChanged)
    } else {
        let active_context_tokens = sess.get_total_token_usage().await;
        let routed_limit_reached = match routed_turn_context
            .config
            .model_auto_compact_token_limit_scope
        {
            AutoCompactTokenLimitScope::Total => {
                let auto_compact_limit = routed_turn_context
                    .model_info()
                    .auto_compact_token_limit()
                    .unwrap_or(i64::MAX);
                let context_window = routed_turn_context
                    .model_context_window()
                    .unwrap_or(i64::MAX);
                active_context_tokens > auto_compact_limit
                    || active_context_tokens >= context_window
            }
            AutoCompactTokenLimitScope::BodyAfterPrefix => routed_turn_context
                .model_context_window()
                .is_some_and(|context_window| active_context_tokens >= context_window),
        };
        let is_model_downshift = previous_turn_context.model_info().slug
            != routed_turn_context.model_info().slug
            && previous_turn_context
                .model_context_window()
                .zip(routed_turn_context.model_context_window())
                .is_some_and(|(previous, routed)| previous > routed);
        (routed_limit_reached && is_model_downshift).then_some(CompactionReason::ModelDownshift)
    };
    let Some(reason) = reason else {
        return Ok(false);
    };
    run_auto_compact(
        sess,
        Arc::clone(routed_step_context),
        /*fallback_step_context*/ None,
        client_session,
        InitialContextInjection::BeforeLastUserMessage {
            world_state: Arc::clone(world_state),
            step_context: Arc::clone(routed_step_context),
        },
        reason,
        CompactionPhase::MidTurn,
        CompactionReporting::CandidateEvaluation,
    )
    .await?;
    Ok(true)
}

#[instrument(
    level = "trace",
    skip_all,
    fields(reason = ?reason, phase = ?phase)
)]
#[allow(clippy::too_many_arguments)]
async fn run_auto_compact(
    sess: &Arc<Session>,
    step_context: Arc<StepContext>,
    fallback_step_context: Option<Arc<StepContext>>,
    client_session: &mut ModelClientSession,
    initial_context_injection: InitialContextInjection,
    reason: CompactionReason,
    phase: CompactionPhase,
    reporting: CompactionReporting,
) -> CodexResult<()> {
    let turn_context = &step_context.turn;
    let _profile_guard = turn_context.turn_timing_state.begin_compaction();
    if turn_context.config.features.enabled(Feature::TokenBudget) {
        // Compaction is the reset request, so force a new context window
        // instead of consuming a pending `new_context` tool request.
        crate::compact_token_budget::run_inline_auto_compact_task(
            Arc::clone(sess),
            step_context,
            initial_context_injection,
            reporting,
        )
        .await?;
        return Ok(());
    }

    match turn_context.provider.capabilities().remote_compaction {
        RemoteCompactionSupport::V2
            if turn_context
                .config
                .features
                .enabled(Feature::RemoteCompactionV2) =>
        {
            emit_compact_metric(
                &sess.services.session_telemetry,
                "remote_v2",
                /*manual*/ false,
            );
            run_inline_remote_auto_compact_task_v2(
                Arc::clone(sess),
                step_context,
                fallback_step_context,
                client_session,
                initial_context_injection,
                reason,
                phase,
                reporting,
            )
            .await?;
        }
        RemoteCompactionSupport::V2 => {
            emit_compact_metric(
                &sess.services.session_telemetry,
                "remote",
                /*manual*/ false,
            );
            run_inline_remote_auto_compact_task(
                Arc::clone(sess),
                step_context,
                fallback_step_context,
                client_session.turn_state(),
                initial_context_injection,
                reason,
                phase,
                reporting,
            )
            .await?;
        }
        RemoteCompactionSupport::Unsupported => {
            emit_compact_metric(
                &sess.services.session_telemetry,
                "local",
                /*manual*/ false,
            );
            run_inline_auto_compact_task(
                Arc::clone(sess),
                Arc::clone(turn_context),
                client_session,
                initial_context_injection,
                reason,
                phase,
                reporting,
            )
            .await?;
        }
    }
    Ok(())
}

pub(super) fn collect_explicit_app_ids_from_skill_items(
    skill_items: &[ResponseItem],
    connectors: &[connectors::AppInfo],
    skill_name_counts_lower: &HashMap<String, usize>,
) -> HashSet<String> {
    if skill_items.is_empty() || connectors.is_empty() {
        return HashSet::new();
    }

    let skill_messages = skill_items
        .iter()
        .filter_map(|item| match item {
            ResponseItem::Message { content, .. } => {
                content.iter().find_map(|content_item| match content_item {
                    ContentItem::InputText { text } => Some(text.clone()),
                    _ => None,
                })
            }
            _ => None,
        })
        .collect::<Vec<String>>();
    if skill_messages.is_empty() {
        return HashSet::new();
    }

    let mentions = collect_tool_mentions_from_messages(&skill_messages);
    let mention_names_lower = mentions
        .plain_names
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<HashSet<String>>();
    let mut connector_ids = mentions
        .paths
        .iter()
        .filter(|path| tool_kind_for_path(path) == ToolMentionKind::App)
        .filter_map(|path| app_id_from_path(path).map(str::to_string))
        .collect::<HashSet<String>>();

    let connector_slug_counts = build_connector_slug_counts(connectors);
    for connector in connectors {
        let slug = codex_connectors::metadata::connector_mention_slug(connector);
        let connector_count = connector_slug_counts.get(&slug).copied().unwrap_or(0);
        let skill_count = skill_name_counts_lower.get(&slug).copied().unwrap_or(0);
        if connector_count == 1 && skill_count == 0 && mention_names_lower.contains(&slug) {
            connector_ids.insert(connector.id.clone());
        }
    }

    connector_ids
}

#[instrument(level = "trace", skip_all)]
pub(crate) fn build_prompt(
    input: Vec<ResponseItem>,
    step_context: &StepContext,
    base_instructions: BaseInstructions,
) -> Prompt {
    let turn_context = &step_context.turn;
    Prompt {
        input,
        tools: step_context.tool_router.model_visible_specs(),
        parallel_tool_calls: true,
        base_instructions,
        output_schema: turn_context.final_output_json_schema.clone(),
        output_schema_strict: !crate::guardian::is_basic_session_source(
            &turn_context.session_source,
        ),
        cyber_access_program: turn_context.cyber_access_program,
    }
}

#[allow(clippy::too_many_arguments)]
#[allow(deprecated)]
#[instrument(level = "trace",
    skip_all,
    fields(
        turn_id = %step_context.turn.sub_id,
        model = %step_context.settings.model_info.slug,
        cwd = %step_context.turn.cwd.display()
    )
)]
async fn run_sampling_request(
    sess: Arc<Session>,
    step_context: Arc<StepContext>,
    turn_store: Arc<codex_extension_api::ExtensionData>,
    turn_diff_tracker: SharedTurnDiffTracker,
    client_session: &mut ModelClientSession,
    responses_metadata: &CodexResponsesMetadata,
    input: Vec<ResponseItem>,
    cancellation_token: CancellationToken,
    interrupted_response_recorded: &mut bool,
) -> Result<(SamplingRequestResult, Vec<ResponseItem>), SamplingRequestFailure> {
    let turn_context = Arc::clone(&step_context.turn);
    let base_instructions = sess.get_base_instructions().await;

    let tool_runtime = ToolCallRuntime::new(
        Arc::clone(&sess),
        Arc::clone(&step_context),
        Arc::clone(&turn_diff_tracker),
    );
    let _code_mode_worker = sess.services.code_mode_service.start_turn_worker(
        &sess,
        Arc::clone(&step_context),
        Arc::clone(&turn_diff_tracker),
    );
    let max_retries = turn_context.provider.info().stream_max_retries();
    let mut retry_state = ResponsesStreamRetryState::default();
    let mut initial_input = Some(input);
    let mut original_input = None;
    let mut executed_tool_calls_by_output = HashMap::new();
    loop {
        // A routing profile must not hide unsafe partial output behind a later model change.
        // Ordinary same-model retries rebuild the request from committed local history.
        let reroute_safe = Arc::new(AtomicBool::new(true));
        let interrupted_response = Arc::new(AtomicBool::new(false));
        let prompt_input = if let Some(input) = initial_input.take() {
            input
        } else {
            sess.clone_history()
                .await
                .for_prompt(&step_context.settings.model_info.input_modalities)
        };
        let mut prompt_input = prompt_input;
        if let Some(executed_tool_calls) = sess.services.executed_tool_calls.as_ref()
            && executed_tool_calls
                .attach_pending_to_prompt(&mut prompt_input, &mut executed_tool_calls_by_output)
        {
            codex_protocol::models::bound_executed_tool_calls_for_prompt(&mut prompt_input);
        }
        let prompt = build_prompt(
            prompt_input,
            step_context.as_ref(),
            base_instructions.clone(),
        );
        let err = match try_run_sampling_request(
            tool_runtime.clone(),
            Arc::clone(&sess),
            Arc::clone(&step_context),
            Arc::clone(&turn_store),
            client_session,
            responses_metadata,
            Arc::clone(&turn_diff_tracker),
            &prompt,
            cancellation_token.child_token(),
            Arc::clone(&reroute_safe),
            Arc::clone(&interrupted_response),
        )
        .await
        {
            Ok(output) => {
                return Ok((output, original_input.unwrap_or(prompt.input)));
            }
            Err(err) => match err.details() {
                CodexErrorDetails::ContextWindowExceeded => {
                    sess.set_total_tokens_full(&turn_context).await;
                    return Err(SamplingRequestFailure {
                        error: err,
                        reroute_safe: reroute_safe.load(Ordering::Relaxed),
                        interrupted_response: interrupted_response.load(Ordering::Relaxed),
                    });
                }
                CodexErrorDetails::UsageLimitReached(e) => {
                    let rate_limits = e.rate_limits.clone();
                    if let Some(rate_limits) = rate_limits {
                        sess.update_rate_limits(&turn_context, *rate_limits).await;
                    }
                    return Err(SamplingRequestFailure {
                        error: err,
                        reroute_safe: reroute_safe.load(Ordering::Relaxed),
                        interrupted_response: interrupted_response.load(Ordering::Relaxed),
                    });
                }
                _ if turn_context.model_profile.is_some()
                    && classify_model_routing_failure(err.details()).is_some() =>
                {
                    return Err(SamplingRequestFailure {
                        error: err,
                        reroute_safe: reroute_safe.load(Ordering::Relaxed),
                        interrupted_response: interrupted_response.load(Ordering::Relaxed),
                    });
                }
                _ => err,
            },
        };
        let attempt_reroute_safe = reroute_safe.load(Ordering::Relaxed);

        if original_input.is_none() {
            original_input = Some(prompt.input);
        }

        let routing_profile_blocks_retry =
            turn_context.model_profile.is_some() && !attempt_reroute_safe;
        if routing_profile_blocks_retry || !err.is_retryable() {
            return Err(SamplingRequestFailure {
                error: err,
                reroute_safe: attempt_reroute_safe,
                interrupted_response: interrupted_response.load(Ordering::Relaxed),
            });
        }

        handle_retryable_response_stream_error(
            &mut retry_state,
            max_retries,
            err,
            client_session,
            &sess,
            &turn_context,
            ResponsesStreamRequest::Sampling,
        )
        .await
        .map_err(|error| SamplingRequestFailure {
            error,
            reroute_safe: attempt_reroute_safe,
            interrupted_response: interrupted_response.load(Ordering::Relaxed),
        })?;
        if interrupted_response.swap(false, Ordering::Relaxed) && !*interrupted_response_recorded {
            record_interrupted_response(sess.as_ref(), turn_context.as_ref()).await;
            *interrupted_response_recorded = true;
        }
        turn_context.turn_timing_state.record_sampling_retry();
    }
}

pub(crate) struct PreparedToolRecommendations {
    auth: Option<CodexAuth>,
    endpoint_candidates: Option<Vec<DiscoverableTool>>,
}

#[instrument(level = "trace", skip_all)]
pub(crate) async fn prepare_tool_recommendations(
    sess: &Session,
    turn_context: &TurnContext,
) -> PreparedToolRecommendations {
    let loaded_plugins = sess
        .services
        .plugins_manager
        .plugins_for_config(&turn_context.config.plugins_config_input())
        .instrument(trace_span!("built_tools.load_plugins"))
        .await;
    let tool_suggest_is_enabled = tool_suggest_enabled(turn_context);
    let auth = if tool_suggest_is_enabled {
        sess.services.auth_manager.auth().await
    } else {
        None
    };
    let endpoint_candidates = if tool_suggest_is_enabled {
        let plugins_config = turn_context.config.plugins_config_input();
        sess.services
            .plugins_manager
            .recommended_plugin_candidates_for_config(RecommendedPluginCandidatesInput {
                plugins_config: &plugins_config,
                loaded_plugins: &loaded_plugins,
                auth: auth.as_ref(),
                disabled_tools: &turn_context.config.tool_suggest.disabled_tools,
                app_server_client_name: turn_context.app_server_client_name.as_deref(),
            })
            .await
    } else {
        None
    };

    PreparedToolRecommendations {
        auth,
        endpoint_candidates,
    }
}

#[instrument(level = "trace",
    skip_all,
    fields(
        turn_id = %turn_context.sub_id,
        model = %model_info.slug,
        apps_enabled = turn_context.apps_enabled()
    )
)]
pub(crate) async fn built_tools(
    sess: &Session,
    turn_context: &TurnContext,
    model_info: &codex_protocol::openai_models::ModelInfo,
    environments: &TurnEnvironmentSnapshot,
    mcp: &Arc<codex_mcp::McpBinding>,
    step_store: &ExtensionData,
    prepared_recommendations: PreparedToolRecommendations,
) -> CodexResult<Arc<ToolRouter>> {
    let all_mcp_tools = mcp.tools();
    let connector_snapshot = mcp.config().connector_snapshot.clone();

    let apps_enabled = turn_context.apps_enabled();
    let accessible_connectors =
        apps_enabled.then(|| connectors::accessible_connectors_from_mcp_tools(all_mcp_tools));
    let tool_suggest_is_enabled = tool_suggest_enabled(turn_context);
    let PreparedToolRecommendations {
        auth,
        endpoint_candidates: endpoint_recommended_plugin_candidates,
    } = prepared_recommendations;
    let tool_suggest_candidates =
        if let Some(recommended_plugin_candidates) = endpoint_recommended_plugin_candidates {
            Some(ToolSuggestCandidates {
                tools: recommended_plugin_candidates,
                presentation: ToolSuggestPresentation::RecommendationContext,
            })
        } else {
            let loaded_plugin_app_connector_ids = connector_snapshot
                .connector_ids()
                .iter()
                .map(|connector_id| connector_id.0.clone())
                .collect::<Vec<_>>();
            async {
                if apps_enabled && tool_suggest_is_enabled {
                    if let Some(accessible_connectors) = accessible_connectors.as_ref() {
                        match connectors::list_tool_suggest_discoverable_tools_with_auth(
                            &turn_context.config,
                            sess.services.plugins_manager.as_ref(),
                            auth.as_ref(),
                            accessible_connectors.as_slice(),
                            &loaded_plugin_app_connector_ids,
                        )
                        .await
                        .map(|discoverable_tools| {
                            filter_request_plugin_install_discoverable_tools_for_client(
                                discoverable_tools,
                                turn_context.app_server_client_name.as_deref(),
                            )
                        }) {
                            Ok(discoverable_tools) if discoverable_tools.is_empty() => None,
                            Ok(discoverable_tools) => Some(ToolSuggestCandidates {
                                tools: discoverable_tools,
                                presentation: ToolSuggestPresentation::ListTool,
                            }),
                            Err(err) => {
                                warn!("failed to load discoverable tool suggestions: {err:#}");
                                None
                            }
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            .instrument(trace_span!("built_tools.load_discoverable_tools"))
            .await
        };
    Ok(Arc::new(build_tool_router(
        sess,
        turn_context,
        model_info,
        environments,
        mcp,
        apps_enabled,
        step_store,
        tool_suggest_candidates.as_ref(),
    )?))
}

#[derive(Debug)]
struct SamplingRequestResult {
    needs_follow_up: bool,
    last_agent_message: Option<String>,
    refresh_turn_context: bool,
}

/// Ephemeral per-response state for streaming a single proposed plan.
/// This is intentionally not persisted or stored in session/state since it
/// only exists while a response is actively streaming. The final plan text
/// is extracted from the completed assistant message.
/// Tracks a single proposed plan item across a streaming response.
struct ProposedPlanItemState {
    item_id: String,
    started: bool,
    completed: bool,
}

/// Aggregated state used only while streaming a plan-mode response.
/// Includes per-item parsers, deferred agent message bookkeeping, and the plan item lifecycle.
struct PlanModeStreamState {
    /// Agent message items started by the model but deferred until we see non-plan text.
    pending_agent_message_items: HashMap<String, TurnItem>,
    /// Agent message items whose start notification has been emitted.
    started_agent_message_items: HashSet<String>,
    /// Leading whitespace buffered until we see non-whitespace text for an item.
    leading_whitespace_by_item: HashMap<String, String>,
    /// Tracks plan item lifecycle while streaming plan output.
    plan_item_state: ProposedPlanItemState,
}

impl PlanModeStreamState {
    fn new(turn_id: &str) -> Self {
        Self {
            pending_agent_message_items: HashMap::new(),
            started_agent_message_items: HashSet::new(),
            leading_whitespace_by_item: HashMap::new(),
            plan_item_state: ProposedPlanItemState::new(turn_id),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct AssistantMessageStreamParsers {
    plan_mode: bool,
    parsers_by_item: HashMap<String, AssistantTextStreamParser>,
}

type ParsedAssistantTextDelta = AssistantTextChunk;

impl AssistantMessageStreamParsers {
    pub(super) fn new(plan_mode: bool) -> Self {
        Self {
            plan_mode,
            parsers_by_item: HashMap::new(),
        }
    }

    fn parser_mut(&mut self, item_id: &str) -> &mut AssistantTextStreamParser {
        let plan_mode = self.plan_mode;
        self.parsers_by_item
            .entry(item_id.to_string())
            .or_insert_with(|| AssistantTextStreamParser::new(plan_mode))
    }

    pub(super) fn seed_item_text(&mut self, item_id: &str, text: &str) -> ParsedAssistantTextDelta {
        if text.is_empty() {
            return ParsedAssistantTextDelta::default();
        }
        self.parser_mut(item_id).push_str(text)
    }

    pub(super) fn parse_delta(&mut self, item_id: &str, delta: &str) -> ParsedAssistantTextDelta {
        self.parser_mut(item_id).push_str(delta)
    }

    pub(super) fn finish_item(&mut self, item_id: &str) -> ParsedAssistantTextDelta {
        let Some(mut parser) = self.parsers_by_item.remove(item_id) else {
            return ParsedAssistantTextDelta::default();
        };
        parser.finish()
    }

    fn drain_finished(&mut self) -> Vec<(String, ParsedAssistantTextDelta)> {
        let parsers_by_item = std::mem::take(&mut self.parsers_by_item);
        parsers_by_item
            .into_iter()
            .map(|(item_id, mut parser)| (item_id, parser.finish()))
            .collect()
    }
}

impl ProposedPlanItemState {
    fn new(turn_id: &str) -> Self {
        Self {
            item_id: format!("{turn_id}-plan"),
            started: false,
            completed: false,
        }
    }

    async fn start(&mut self, sess: &Session, turn_context: &TurnContext) {
        if self.started || self.completed {
            return;
        }
        self.started = true;
        let item = TurnItem::Plan(PlanItem {
            id: self.item_id.clone(),
            text: String::new(),
        });
        sess.emit_turn_item_started(turn_context, &item).await;
    }

    async fn push_delta(&mut self, sess: &Session, turn_context: &TurnContext, delta: &str) {
        if self.completed {
            return;
        }
        if delta.is_empty() {
            return;
        }
        let event = PlanDeltaEvent {
            thread_id: sess.thread_id.to_string(),
            turn_id: turn_context.sub_id.clone(),
            item_id: self.item_id.clone(),
            delta: delta.to_string(),
        };
        sess.send_event(turn_context, EventMsg::PlanDelta(event))
            .await;
    }

    async fn complete_with_text(
        &mut self,
        sess: &Session,
        turn_context: &TurnContext,
        text: String,
    ) {
        if self.completed || !self.started {
            return;
        }
        self.completed = true;
        let item = TurnItem::Plan(PlanItem {
            id: self.item_id.clone(),
            text,
        });
        sess.emit_turn_item_completed(turn_context, item).await;
    }
}

/// In plan mode we defer agent message starts until the parser emits non-plan
/// text. The parser buffers each line until it can rule out a tag prefix, so
/// plan-only outputs never show up as empty assistant messages.
async fn maybe_emit_pending_agent_message_start(
    sess: &Session,
    turn_context: &TurnContext,
    state: &mut PlanModeStreamState,
    item_id: &str,
) {
    if state.started_agent_message_items.contains(item_id) {
        return;
    }
    if let Some(item) = state.pending_agent_message_items.remove(item_id) {
        sess.emit_turn_item_started(turn_context, &item).await;
        state
            .started_agent_message_items
            .insert(item_id.to_string());
    }
}

/// Agent messages are text-only today; concatenate all text entries.
pub(super) fn agent_message_text(item: &codex_protocol::items::AgentMessageItem) -> String {
    item.content
        .iter()
        .map(|entry| match entry {
            codex_protocol::items::AgentMessageContent::Text { text } => text.as_str(),
        })
        .collect()
}

pub(super) fn realtime_text_for_event(msg: &EventMsg) -> Option<(String, Option<MessagePhase>)> {
    match msg {
        EventMsg::AgentMessage(event) => Some((event.message.clone(), event.phase.clone())),
        EventMsg::ItemCompleted(event) => match &event.item {
            TurnItem::AgentMessage(item) => Some((agent_message_text(item), item.phase.clone())),
            _ => None,
        },
        EventMsg::ExecApprovalRequest(_)
        | EventMsg::RequestPermissions(_)
        | EventMsg::ApplyPatchApprovalRequest(_)
        | EventMsg::RequestUserInput(_)
        | EventMsg::ElicitationRequest(_) => {
            let message = if matches!(
                msg,
                EventMsg::RequestUserInput(_) | EventMsg::ElicitationRequest(_)
            ) {
                "I need your input. Please respond in the app."
            } else {
                "I need your approval to continue. Please review the request in the app."
            };
            serde_json::to_string(msg)
                .ok()
                .map(|request| (format!("{message}\n\n{request}"), None))
        }
        EventMsg::Error(_)
        | EventMsg::Warning(_)
        | EventMsg::AuthRecoveryStarted(_)
        | EventMsg::AuthRecoveryCompleted(_)
        | EventMsg::GuardianWarning(_)
        | EventMsg::RealtimeConversationStarted(_)
        | EventMsg::RealtimeConversationSdp(_)
        | EventMsg::RealtimeConversationRealtime(_)
        | EventMsg::RealtimeConversationClosed(_)
        | EventMsg::ModelReroute(_)
        | EventMsg::ModelVerification(_)
        | EventMsg::TurnModerationMetadata(_)
        | EventMsg::SafetyBuffering(_)
        | EventMsg::ContextCompacted(_)
        | EventMsg::ThreadRolledBack(_)
        | EventMsg::TurnStarted(_)
        | EventMsg::ThreadSettingsApplied(_)
        | EventMsg::TurnComplete(_)
        | EventMsg::TokenCount(_)
        | EventMsg::UserMessage(_)
        | EventMsg::AgentReasoning(_)
        | EventMsg::AgentReasoningRawContent(_)
        | EventMsg::AgentReasoningSectionBreak(_)
        | EventMsg::SessionConfigured(_)
        | EventMsg::EnvironmentConnected(_)
        | EventMsg::EnvironmentDisconnected(_)
        | EventMsg::ThreadGoalUpdated(_)
        | EventMsg::ThreadQueueChanged(_)
        | EventMsg::McpStartupUpdate(_)
        | EventMsg::McpStartupComplete(_)
        | EventMsg::McpToolCallBegin(_)
        | EventMsg::McpToolCallEnd(_)
        | EventMsg::WebSearchBegin(_)
        | EventMsg::WebSearchEnd(_)
        | EventMsg::ExecCommandBegin(_)
        | EventMsg::ExecCommandOutputDelta(_)
        | EventMsg::TerminalInteraction(_)
        | EventMsg::ExecCommandEnd(_)
        | EventMsg::PatchApplyBegin(_)
        | EventMsg::PatchApplyUpdated(_)
        | EventMsg::PatchApplyEnd(_)
        | EventMsg::ImageGenerationBegin(_)
        | EventMsg::ImageGenerationEnd(_)
        | EventMsg::ViewImageToolCall(_)
        | EventMsg::DynamicToolCallRequest(_)
        | EventMsg::DynamicToolCallResponse(_)
        | EventMsg::GuardianAssessment(_)
        | EventMsg::DeprecationNotice(_)
        | EventMsg::StreamError(_)
        | EventMsg::TurnDiff(_)
        | EventMsg::RealtimeConversationListVoicesResponse(_)
        | EventMsg::PlanUpdate(_)
        | EventMsg::TurnAborted(_)
        | EventMsg::ShutdownComplete
        | EventMsg::EnteredReviewMode(_)
        | EventMsg::ExitedReviewMode(_)
        | EventMsg::RawResponseItem(_)
        | EventMsg::RawResponseCompleted(_)
        | EventMsg::ItemStarted(_)
        | EventMsg::HookStarted(_)
        | EventMsg::HookCompleted(_)
        | EventMsg::AgentMessageContentDelta(_)
        | EventMsg::PlanDelta(_)
        | EventMsg::ReasoningContentDelta(_)
        | EventMsg::ReasoningRawContentDelta(_)
        | EventMsg::CollabAgentSpawnBegin(_)
        | EventMsg::CollabAgentSpawnEnd(_)
        | EventMsg::CollabAgentInteractionBegin(_)
        | EventMsg::CollabAgentInteractionEnd(_)
        | EventMsg::CollabWaitingBegin(_)
        | EventMsg::CollabWaitingEnd(_)
        | EventMsg::CollabCloseBegin(_)
        | EventMsg::CollabCloseEnd(_)
        | EventMsg::CollabResumeBegin(_)
        | EventMsg::CollabResumeEnd(_)
        | EventMsg::SubAgentActivity(_) => None,
    }
}

/// Split the stream into normal assistant text vs. proposed plan content.
/// Normal text becomes AgentMessage deltas; plan content becomes PlanDelta +
/// TurnItem::Plan.
async fn handle_plan_segments(
    sess: &Session,
    turn_context: &TurnContext,
    state: &mut PlanModeStreamState,
    item_id: &str,
    segments: Vec<ProposedPlanSegment>,
) {
    for segment in segments {
        match segment {
            ProposedPlanSegment::Normal(delta) => {
                if delta.is_empty() {
                    continue;
                }
                let has_non_whitespace = delta.chars().any(|ch| !ch.is_whitespace());
                if !has_non_whitespace && !state.started_agent_message_items.contains(item_id) {
                    let entry = state
                        .leading_whitespace_by_item
                        .entry(item_id.to_string())
                        .or_default();
                    entry.push_str(&delta);
                    continue;
                }
                let delta = if !state.started_agent_message_items.contains(item_id) {
                    if let Some(prefix) = state.leading_whitespace_by_item.remove(item_id) {
                        format!("{prefix}{delta}")
                    } else {
                        delta
                    }
                } else {
                    delta
                };
                maybe_emit_pending_agent_message_start(sess, turn_context, state, item_id).await;

                let event = AgentMessageContentDeltaEvent {
                    thread_id: sess.thread_id.to_string(),
                    turn_id: turn_context.sub_id.clone(),
                    item_id: item_id.to_string(),
                    delta,
                };
                sess.send_event(turn_context, EventMsg::AgentMessageContentDelta(event))
                    .await;
            }
            ProposedPlanSegment::ProposedPlanStart => {
                if !state.plan_item_state.completed {
                    state.plan_item_state.start(sess, turn_context).await;
                }
            }
            ProposedPlanSegment::ProposedPlanDelta(delta) => {
                if !state.plan_item_state.completed {
                    if !state.plan_item_state.started {
                        state.plan_item_state.start(sess, turn_context).await;
                    }
                    state
                        .plan_item_state
                        .push_delta(sess, turn_context, &delta)
                        .await;
                }
            }
            ProposedPlanSegment::ProposedPlanEnd => {}
        }
    }
}

async fn emit_streamed_assistant_text_delta(
    sess: &Session,
    turn_context: &TurnContext,
    plan_mode_state: Option<&mut PlanModeStreamState>,
    item_id: &str,
    parsed: ParsedAssistantTextDelta,
) {
    if parsed.is_empty() {
        return;
    }
    if !parsed.citations.is_empty() {
        // Citation extraction is intentionally local for now; we strip citations from display text
        // but do not yet surface them in protocol events.
        let _citations = parsed.citations;
    }
    if let Some(state) = plan_mode_state {
        if !parsed.plan_segments.is_empty() {
            handle_plan_segments(sess, turn_context, state, item_id, parsed.plan_segments).await;
        }
        return;
    }
    if parsed.visible_text.is_empty() {
        return;
    }
    let event = AgentMessageContentDeltaEvent {
        thread_id: sess.thread_id.to_string(),
        turn_id: turn_context.sub_id.clone(),
        item_id: item_id.to_string(),
        delta: parsed.visible_text,
    };
    sess.send_event(turn_context, EventMsg::AgentMessageContentDelta(event))
        .await;
}

/// Flush buffered assistant text parser state when an assistant message item ends.
async fn flush_assistant_text_segments_for_item(
    sess: &Session,
    turn_context: &TurnContext,
    plan_mode_state: Option<&mut PlanModeStreamState>,
    parsers: &mut AssistantMessageStreamParsers,
    item_id: &str,
) {
    let parsed = parsers.finish_item(item_id);
    emit_streamed_assistant_text_delta(sess, turn_context, plan_mode_state, item_id, parsed).await;
}

/// Flush any remaining buffered assistant text parser state at response completion.
async fn flush_assistant_text_segments_all(
    sess: &Session,
    turn_context: &TurnContext,
    mut plan_mode_state: Option<&mut PlanModeStreamState>,
    parsers: &mut AssistantMessageStreamParsers,
) {
    for (item_id, parsed) in parsers.drain_finished() {
        emit_streamed_assistant_text_delta(
            sess,
            turn_context,
            plan_mode_state.as_deref_mut(),
            &item_id,
            parsed,
        )
        .await;
    }
}

/// Emit completion for plan items by parsing the finalized assistant message.
async fn maybe_complete_plan_item_from_message(
    sess: &Session,
    turn_context: &TurnContext,
    state: &mut PlanModeStreamState,
    item: &ResponseItem,
) {
    if let ResponseItem::Message { role, content, .. } = item
        && role == "assistant"
    {
        let mut text = String::new();
        for entry in content {
            if let ContentItem::OutputText { text: chunk } = entry {
                text.push_str(chunk);
            }
        }
        if let Some(plan_text) = extract_proposed_plan_text(&text) {
            let (plan_text, _citations) = strip_citations(&plan_text);
            if !state.plan_item_state.started {
                state.plan_item_state.start(sess, turn_context).await;
            }
            state
                .plan_item_state
                .complete_with_text(sess, turn_context, plan_text)
                .await;
        }
    }
}

/// Emit a completed agent message in plan mode, respecting deferred starts.
async fn emit_agent_message_in_plan_mode(
    sess: &Session,
    turn_context: &TurnContext,
    agent_message: codex_protocol::items::AgentMessageItem,
    state: &mut PlanModeStreamState,
) {
    let agent_message_id = agent_message.id.clone();
    let text = agent_message_text(&agent_message);
    if text.trim().is_empty() {
        state.pending_agent_message_items.remove(&agent_message_id);
        state.started_agent_message_items.remove(&agent_message_id);
        return;
    }

    maybe_emit_pending_agent_message_start(sess, turn_context, state, &agent_message_id).await;

    if !state
        .started_agent_message_items
        .contains(&agent_message_id)
    {
        let start_item = state
            .pending_agent_message_items
            .remove(&agent_message_id)
            .unwrap_or_else(|| {
                TurnItem::AgentMessage(codex_protocol::items::AgentMessageItem {
                    id: agent_message_id.clone(),
                    content: Vec::new(),
                    phase: None,
                    memory_citation: None,
                    delivery: None,
                })
            });
        sess.emit_turn_item_started(turn_context, &start_item).await;
        state
            .started_agent_message_items
            .insert(agent_message_id.clone());
    }

    sess.emit_turn_item_completed(turn_context, TurnItem::AgentMessage(agent_message))
        .await;
    state.started_agent_message_items.remove(&agent_message_id);
}

/// Emit completion for a plan-mode turn item, handling agent messages specially.
async fn emit_turn_item_in_plan_mode(
    sess: &Session,
    turn_context: &TurnContext,
    turn_item: TurnItem,
    previously_active_item: Option<&TurnItem>,
    state: &mut PlanModeStreamState,
) {
    match turn_item {
        TurnItem::AgentMessage(agent_message) => {
            emit_agent_message_in_plan_mode(sess, turn_context, agent_message, state).await;
        }
        _ => {
            if previously_active_item.is_none() {
                sess.emit_turn_item_started(turn_context, &turn_item).await;
            }
            sess.emit_turn_item_completed(turn_context, turn_item).await;
        }
    }
}

/// Handle a completed assistant response item in plan mode, returning true if handled.
async fn handle_assistant_item_done_in_plan_mode(
    sess: &Session,
    turn_context: &TurnContext,
    turn_store: &codex_extension_api::ExtensionData,
    item: &ResponseItem,
    state: &mut PlanModeStreamState,
    previously_active_item: Option<&TurnItem>,
    last_agent_message: &mut Option<String>,
) -> bool {
    if let ResponseItem::Message { role, .. } = item
        && role == "assistant"
    {
        maybe_complete_plan_item_from_message(sess, turn_context, state, item).await;

        let mut finalized_facts = None;
        if let Some(finalized_turn_item) = finalize_non_tool_response_item(
            sess,
            TurnItemContributorPolicy::Run(turn_store),
            item,
            /*plan_mode*/ true,
        )
        .await
        {
            finalized_facts = Some(finalized_turn_item.facts.clone());
            emit_turn_item_in_plan_mode(
                sess,
                turn_context,
                finalized_turn_item.turn_item,
                previously_active_item,
                state,
            )
            .await;
        }
        let final_last_agent_message = finalized_facts
            .as_ref()
            .and_then(|facts| facts.last_agent_message.clone());

        record_completed_response_item_with_finalized_facts(
            sess,
            turn_context,
            item,
            finalized_facts.as_ref(),
        )
        .await;
        if let Some(agent_message) = final_last_agent_message {
            *last_agent_message = Some(agent_message);
        }
        return true;
    }
    false
}

/// Finalizes assistant text from an interrupted provider route segment.
///
/// The text is part of the conversation history and client-visible turn item lifecycle, but it is
/// not the final answer for the user turn. In particular, it must not defer queued steering to the
/// next user turn.
async fn finalize_interrupted_assistant_item(
    sess: &Session,
    turn_context: &TurnContext,
    turn_store: &codex_extension_api::ExtensionData,
    item: &ResponseItem,
    previously_streamed_item: Option<&TurnItem>,
    plan_mode_state: Option<&mut PlanModeStreamState>,
) -> bool {
    let plan_mode = plan_mode_state.is_some();
    let Some(mut finalized_turn_item) = finalize_non_tool_response_item(
        sess,
        TurnItemContributorPolicy::Run(turn_store),
        item,
        plan_mode,
    )
    .await
    else {
        return false;
    };
    finalized_turn_item
        .facts
        .defers_mailbox_delivery_to_next_turn = false;

    if let Some(plan_mode_state) = plan_mode_state {
        emit_turn_item_in_plan_mode(
            sess,
            turn_context,
            finalized_turn_item.turn_item,
            previously_streamed_item,
            plan_mode_state,
        )
        .await;
    } else {
        if previously_streamed_item.is_none() {
            sess.emit_turn_item_started(turn_context, &finalized_turn_item.turn_item)
                .await;
        }
        sess.emit_turn_item_completed(turn_context, finalized_turn_item.turn_item)
            .await;
    }
    record_completed_response_item_with_finalized_facts(
        sess,
        turn_context,
        item,
        Some(&finalized_turn_item.facts),
    )
    .await;
    true
}

async fn record_interrupted_response(sess: &Session, turn_context: &TurnContext) {
    let interrupted_response: ResponseItem =
        ContextualUserFragment::into(InterruptedResponseRecord);
    sess.record_conversation_items(turn_context, &[interrupted_response])
        .await;
}

/// Closes a client lifecycle without adding an incomplete provider-managed item to model history.
///
/// Reasoning may lack encrypted completion state, and hosted tools do not share one valid synthetic
/// output type. The bounded interrupted-response record describes the failure for the replacement
/// route segment instead.
async fn finalize_interrupted_client_item(
    sess: &Session,
    turn_context: &TurnContext,
    turn_store: &codex_extension_api::ExtensionData,
    item: &ResponseItem,
    previously_streamed_item: Option<&TurnItem>,
    plan_mode: bool,
) -> bool {
    let finalized_turn_item = finalize_non_tool_response_item(
        sess,
        TurnItemContributorPolicy::Run(turn_store),
        item,
        plan_mode,
    )
    .await;
    let Some(finalized_turn_item) = finalized_turn_item else {
        return previously_streamed_item.is_none();
    };
    if previously_streamed_item.is_none() {
        sess.emit_turn_item_started(turn_context, &finalized_turn_item.turn_item)
            .await;
    }
    sess.emit_turn_item_completed(turn_context, finalized_turn_item.turn_item)
        .await;
    true
}

const MAX_INTERRUPTED_TOOL_INPUT_BYTES: usize = 8 * 1024;
const INTERRUPTED_TOOL_CALL_OUTPUT: &str =
    "Tool call was not executed because the model request ended before the call completed.";

/// Records an incomplete harness-owned call as a non-executed exchange.
///
/// The call is never passed to `ToolRouter`. Pairing it with a fixed output preserves the model's
/// attempted action without implying that truncated arguments were valid or that a side effect
/// occurred.
async fn record_interrupted_tool_call(
    sess: &Session,
    turn_context: &TurnContext,
    item: ResponseItem,
) {
    let output = match &item {
        ResponseItem::FunctionCall {
            call_id,
            name,
            namespace,
            ..
        } => ResponseItem::FunctionCallOutput {
            id: None,
            call_id: Some(call_id.clone()),
            name: Some(name.clone()),
            namespace: namespace.clone(),
            output: FunctionCallOutputPayload::from_text(INTERRUPTED_TOOL_CALL_OUTPUT.to_string()),
            internal_chat_message_metadata_passthrough: None,
        },
        ResponseItem::CustomToolCall { call_id, name, .. } => ResponseItem::CustomToolCallOutput {
            id: None,
            call_id: call_id.clone(),
            name: Some(name.clone()),
            output: FunctionCallOutputPayload::from_text(INTERRUPTED_TOOL_CALL_OUTPUT.to_string()),
            internal_chat_message_metadata_passthrough: None,
        },
        _ => return,
    };
    sess.record_conversation_items(turn_context, &[item, output])
        .await;
}

#[instrument(level = "trace", skip_all)]
async fn drain_in_flight(
    in_flight: &mut FuturesOrdered<InFlightFuture<'static>>,
    sess: Arc<Session>,
    turn_context: Arc<TurnContext>,
) -> CodexResult<()> {
    while let Some(res) = in_flight.next().await {
        match res {
            Ok(envelope) => {
                mark_thread_memory_mode_polluted_if_external_context(
                    sess.as_ref(),
                    turn_context.as_ref(),
                    &envelope.item,
                )
                .await;
                sess.record_annotated_conversation_items(&turn_context, vec![envelope])
                    .await;
            }
            Err(err) => {
                error_or_panic(format!("in-flight tool future failed during drain: {err}"));
                return Err(err);
            }
        }
    }
    Ok(())
}

fn assign_missing_streamed_response_item_id(
    item: &mut ResponseItem,
    active_item: Option<&TurnItem>,
) {
    if item.id().is_some_and(|id| !id.is_empty()) {
        return;
    }

    let active_item_id = active_item
        .map(|item| ResponseItemId::from_server(item.id()))
        .filter(|item_id| !item_id.is_empty());
    item.set_id(active_item_id);
    Session::assign_missing_response_item_id(item);
}

/// Extends the active assistant message with text already sent to the client.
///
/// If the provider fails before `OutputItemDone`, this accumulated item is finalized into local
/// history before a model reroute. The replacement route segment can then continue after the exact
/// assistant prefix instead of replaying it.
fn append_partial_assistant_text(active_item: &mut Option<ResponseItem>, delta: &str) {
    let Some(ResponseItem::Message { role, content, .. }) = active_item.as_mut() else {
        return;
    };
    if role != "assistant" || delta.is_empty() {
        return;
    }
    if let Some(ContentItem::OutputText { text }) = content.last_mut() {
        text.push_str(delta);
    } else {
        content.push(ContentItem::OutputText {
            text: delta.to_string(),
        });
    }
}

/// Accumulates only the bounded input needed to explain an interrupted, undispatched tool call.
fn append_partial_tool_call_input(active_item: &mut Option<ResponseItem>, delta: &str) {
    let input = match active_item.as_mut() {
        Some(ResponseItem::FunctionCall { arguments, .. }) => arguments,
        Some(ResponseItem::CustomToolCall { input, .. }) => input,
        _ => return,
    };
    truncate_to_char_boundary(input, MAX_INTERRUPTED_TOOL_INPUT_BYTES);
    if input.len() >= MAX_INTERRUPTED_TOOL_INPUT_BYTES || delta.is_empty() {
        return;
    }
    let remaining = MAX_INTERRUPTED_TOOL_INPUT_BYTES - input.len();
    let mut end = remaining.min(delta.len());
    while !delta.is_char_boundary(end) {
        end -= 1;
    }
    input.push_str(&delta[..end]);
}

fn bound_partial_tool_call_input(item: &mut ResponseItem) {
    let input = match item {
        ResponseItem::FunctionCall { arguments, .. } => arguments,
        ResponseItem::CustomToolCall { input, .. } => input,
        _ => return,
    };
    truncate_to_char_boundary(input, MAX_INTERRUPTED_TOOL_INPUT_BYTES);
}

fn truncate_to_char_boundary(value: &mut String, max_bytes: usize) {
    if value.len() <= max_bytes {
        return;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
}

fn append_partial_reasoning_summary(
    active_item: &mut Option<ResponseItem>,
    summary_index: i64,
    delta: &str,
) {
    let Ok(summary_index) = usize::try_from(summary_index) else {
        return;
    };
    let Some(ResponseItem::Reasoning { summary, .. }) = active_item.as_mut() else {
        return;
    };
    while summary.len() <= summary_index {
        summary.push(ReasoningItemReasoningSummary::SummaryText {
            text: String::new(),
        });
    }
    let ReasoningItemReasoningSummary::SummaryText { text } = &mut summary[summary_index];
    text.push_str(delta);
}

fn append_partial_reasoning_content(
    active_item: &mut Option<ResponseItem>,
    content_index: i64,
    delta: &str,
) {
    let Ok(content_index) = usize::try_from(content_index) else {
        return;
    };
    let Some(ResponseItem::Reasoning { content, .. }) = active_item.as_mut() else {
        return;
    };
    let content = content.get_or_insert_default();
    while content.len() <= content_index {
        content.push(ReasoningItemContent::ReasoningText {
            text: String::new(),
        });
    }
    match &mut content[content_index] {
        ReasoningItemContent::ReasoningText { text } | ReasoningItemContent::Text { text } => {
            text.push_str(delta);
        }
    }
}

#[allow(clippy::too_many_arguments)]
#[instrument(level = "trace",
    skip_all,
    fields(
        turn_id = %step_context.turn.sub_id,
        model = %step_context.settings.model_info.slug
    )
)]
async fn try_run_sampling_request(
    tool_runtime: ToolCallRuntime,
    sess: Arc<Session>,
    step_context: Arc<StepContext>,
    turn_store: Arc<codex_extension_api::ExtensionData>,
    client_session: &mut ModelClientSession,
    responses_metadata: &CodexResponsesMetadata,
    turn_diff_tracker: SharedTurnDiffTracker,
    prompt: &Prompt,
    cancellation_token: CancellationToken,
    reroute_safe: Arc<AtomicBool>,
    interrupted_response: Arc<AtomicBool>,
) -> CodexResult<SamplingRequestResult> {
    let turn_context = Arc::clone(&step_context.turn);
    feedback_tags!(
        model = step_context.settings.model_info.slug.clone(),
        approval_policy = turn_context.approval_policy(),
        sandbox_policy = &turn_context.sandbox_policy(),
        effort = step_context.settings.reasoning_effort(),
        auth_mode = sess.services.auth_manager.auth_mode(),
        features = sess.features.enabled_features(),
    );
    let inference_trace = sess.services.rollout_thread_trace.inference_trace_context(
        turn_context.sub_id.as_str(),
        step_context.settings.model_info.slug.as_str(),
        turn_context.provider.info().name.as_str(),
    );
    let sampling_timing_guard = turn_context.turn_timing_state.begin_sampling();
    let uses_sequential_cutoff_reasoning_summaries = turn_context
        .config
        .features
        .enabled(Feature::ConcurrentReasoningSummaries)
        && turn_context.provider.info().is_openai();
    let mut stream = client_session
        .stream(
            prompt,
            &step_context.settings.model_info,
            &step_context.session_telemetry,
            step_context.settings.reasoning_effort().cloned(),
            step_context.settings.reasoning_summary,
            step_context.settings.service_tier.clone(),
            responses_metadata,
            &inference_trace,
        )
        .instrument(trace_span!("stream_request"))
        .or_cancel(&cancellation_token)
        .await??;
    let mut in_flight: FuturesOrdered<InFlightFuture<'static>> = FuturesOrdered::new();
    let mut tool_call_count = 0usize;
    let mut workspace_cwd_call_seen = false;
    let mut needs_follow_up = false;
    let mut last_agent_message: Option<String> = None;
    let mut active_item: Option<TurnItem> = None;
    let mut active_response_item: Option<ResponseItem> = None;
    let mut active_tool_argument_diff_consumer: Option<(
        String,
        Box<dyn ToolArgumentDiffConsumer>,
    )> = None;
    let mut should_emit_turn_diff = false;
    let mut should_emit_token_count = false;
    const MAX_ANALYTICS_TOOL_CALL_IDS_PER_RESPONSE: usize = 256;
    let mut analytics_tool_call_ids = Vec::new();
    let reasoning_effort = step_context
        .settings
        .reasoning_effort()
        .or(step_context
            .settings
            .model_info
            .default_reasoning_level
            .as_ref())
        .map(std::string::ToString::to_string)
        .unwrap_or_else(|| "default".to_string());
    let plan_mode = turn_context.mode() == ModeKind::Plan;
    let mut assistant_message_stream_parsers = AssistantMessageStreamParsers::new(plan_mode);
    let mut plan_mode_state = plan_mode.then(|| PlanModeStreamState::new(&turn_context.sub_id));
    let defer_streamed_turn_items_for_contributors =
        !sess.services.extensions.turn_item_contributors().is_empty();
    let mut active_item_is_streaming_to_client = false;
    let mut completed_response_item_before_failure = false;
    let receiving_span = trace_span!("receiving_stream");
    let outcome: CodexResult<SamplingRequestResult> = loop {
        let handle_responses = trace_span!(
            parent: &receiving_span,
            "handle_responses",
            otel.name = field::Empty,
            tool_name = field::Empty,
            from = field::Empty,
            codex.request.reasoning_effort = %reasoning_effort,
            gen_ai.usage.input_tokens = field::Empty,
            gen_ai.usage.cache_read.input_tokens = field::Empty,
            gen_ai.usage.cache_write.input_tokens = field::Empty,
            gen_ai.usage.output_tokens = field::Empty,
            codex.usage.reasoning_output_tokens = field::Empty,
            codex.usage.total_tokens = field::Empty,
        );

        let event = match stream
            .next()
            .instrument(trace_span!(parent: &handle_responses, "receiving"))
            .or_cancel(&cancellation_token)
            .await
        {
            Ok(event) => event,
            Err(codex_async_utils::CancelErr::Cancelled) => {
                break Err(CodexErr::TurnAborted);
            }
        };

        let event = match event {
            Some(Ok(event)) => event,
            Some(Err(err)) => break Err(err),
            None => {
                break Err(CodexErr::Stream(
                    "stream closed before response.completed".into(),
                ));
            }
        };

        sess.services
            .session_telemetry
            .record_responses(&handle_responses, &event);
        record_turn_ttft_metric(&turn_context, &event).await;

        match event {
            ResponseEvent::Created => {}
            ResponseEvent::OutputItemDone(mut item) => {
                completed_response_item_before_failure = true;
                assign_missing_streamed_response_item_id(&mut item, active_item.as_ref());
                active_response_item = None;
                reroute_safe.store(true, Ordering::Relaxed);
                if analytics_tool_call_ids.len() < MAX_ANALYTICS_TOOL_CALL_IDS_PER_RESPONSE {
                    let call_id = match &item {
                        ResponseItem::FunctionCall { call_id, .. }
                        | ResponseItem::CustomToolCall { call_id, .. } => Some(call_id.as_str()),
                        ResponseItem::ToolSearchCall { call_id, .. }
                        | ResponseItem::LocalShellCall { call_id, .. } => call_id.as_deref(),
                        ResponseItem::WebSearchCall { id, .. }
                        | ResponseItem::ImageGenerationCall { id, .. } => {
                            id.as_ref().map(codex_protocol::ResponseItemId::as_str)
                        }
                        _ => None,
                    };
                    if let Some(call_id) = call_id {
                        analytics_tool_call_ids.push(call_id.to_string());
                    }
                }
                if let Some((_, mut consumer)) = active_tool_argument_diff_consumer.take()
                    && let Ok(Some(event)) = consumer.finish()
                {
                    sess.send_event(&turn_context, event).await;
                }
                let previously_active_item = active_item.take();
                let previously_streamed_item = if active_item_is_streaming_to_client {
                    previously_active_item
                } else {
                    None
                };
                active_item_is_streaming_to_client = false;
                if let Some(previous) = previously_streamed_item.as_ref()
                    && matches!(previous, TurnItem::AgentMessage(_))
                {
                    let item_id = previous.id();
                    flush_assistant_text_segments_for_item(
                        &sess,
                        &turn_context,
                        plan_mode_state.as_mut(),
                        &mut assistant_message_stream_parsers,
                        &item_id,
                    )
                    .await;
                }
                if let Some(state) = plan_mode_state.as_mut()
                    && handle_assistant_item_done_in_plan_mode(
                        &sess,
                        &turn_context,
                        turn_store.as_ref(),
                        &item,
                        state,
                        previously_streamed_item.as_ref(),
                        &mut last_agent_message,
                    )
                    .await
                {
                    continue;
                }

                let mut ctx = HandleOutputCtx {
                    sess: sess.clone(),
                    turn_context: turn_context.clone(),
                    turn_store: Arc::clone(&turn_store),
                    tool_runtime: tool_runtime.clone(),
                    cancellation_token: cancellation_token.child_token(),
                };

                let preempt_for_mailbox_mail = match &item {
                    ResponseItem::Message { role, phase, .. } => {
                        role == "assistant" && matches!(phase, Some(MessagePhase::Commentary))
                    }
                    ResponseItem::Reasoning { .. } => true,
                    ResponseItem::AgentMessage { .. } => false,
                    ResponseItem::AdditionalTools { .. }
                    | ResponseItem::LocalShellCall { .. }
                    | ResponseItem::FunctionCall { .. }
                    | ResponseItem::ToolSearchCall { .. }
                    | ResponseItem::FunctionCallOutput { .. }
                    | ResponseItem::CustomToolCall { .. }
                    | ResponseItem::CustomToolCallOutput { .. }
                    | ResponseItem::ToolSearchOutput { .. }
                    | ResponseItem::WebSearchCall { .. }
                    | ResponseItem::ImageGenerationCall { .. }
                    | ResponseItem::Compaction { .. }
                    | ResponseItem::CompactionTrigger { .. }
                    | ResponseItem::ContextCompaction { .. }
                    | ResponseItem::Other => false,
                };

                let output_result =
                    match handle_output_item_done(&mut ctx, item, previously_streamed_item)
                        .instrument(handle_responses)
                        .await
                    {
                        Ok(output_result) => output_result,
                        Err(err) => break Err(err),
                    };
                if let Some(tool_future) = output_result.tool_future {
                    in_flight.push_back(tool_future);
                }
                if let Some(tool_name) = output_result.tool_name {
                    tool_call_count += 1;
                    workspace_cwd_call_seen |= is_set_workspace_cwd_tool(&tool_name);
                }
                if let Some(agent_message) = output_result.last_agent_message {
                    last_agent_message = Some(agent_message);
                }
                needs_follow_up |= output_result.needs_follow_up;
                // todo: remove before stabilizing multi-agent v2
                if preempt_for_mailbox_mail && sess.input_queue.has_pending_mailbox_items().await {
                    break Ok(SamplingRequestResult {
                        needs_follow_up: true,
                        last_agent_message,
                        refresh_turn_context: false,
                    });
                }
            }
            ResponseEvent::OutputItemAdded(mut item) => {
                assign_missing_streamed_response_item_id(&mut item, /*active_item*/ None);
                bound_partial_tool_call_input(&mut item);
                active_response_item = Some(item.clone());
                reroute_safe.store(false, Ordering::Relaxed);
                if let ResponseItem::CustomToolCall {
                    call_id,
                    name,
                    namespace,
                    ..
                } = &item
                {
                    let tool_name = ToolName::new(namespace.clone(), name.as_str());
                    active_tool_argument_diff_consumer = tool_runtime
                        .create_diff_consumer(&tool_name)
                        .map(|consumer| (call_id.clone(), consumer));
                } else if matches!(&item, ResponseItem::FunctionCall { .. }) {
                    active_tool_argument_diff_consumer = None;
                }
                if let Some(turn_item) = handle_non_tool_response_item(
                    sess.as_ref(),
                    TurnItemContributorPolicy::Skip,
                    &item,
                    plan_mode,
                )
                .await
                {
                    let mut turn_item = turn_item;
                    let stream_item_to_client = !defer_streamed_turn_items_for_contributors;
                    let mut seeded_parsed: Option<ParsedAssistantTextDelta> = None;
                    let mut seeded_item_id: Option<String> = None;
                    if stream_item_to_client
                        && matches!(turn_item, TurnItem::AgentMessage(_))
                        && let Some(raw_text) = raw_assistant_output_text_from_item(&item)
                    {
                        let item_id = turn_item.id();
                        let mut seeded =
                            assistant_message_stream_parsers.seed_item_text(&item_id, &raw_text);
                        if let TurnItem::AgentMessage(agent_message) = &mut turn_item {
                            agent_message.content =
                                vec![codex_protocol::items::AgentMessageContent::Text {
                                    text: if plan_mode {
                                        String::new()
                                    } else {
                                        std::mem::take(&mut seeded.visible_text)
                                    },
                                }];
                        }
                        seeded_parsed = plan_mode.then_some(seeded);
                        seeded_item_id = Some(item_id);
                    }
                    if stream_item_to_client {
                        if let Some(state) = plan_mode_state.as_mut()
                            && matches!(turn_item, TurnItem::AgentMessage(_))
                        {
                            let item_id = turn_item.id();
                            state
                                .pending_agent_message_items
                                .insert(item_id, turn_item.clone());
                        } else {
                            sess.emit_turn_item_started(&turn_context, &turn_item).await;
                        }
                        if let (Some(state), Some(item_id), Some(parsed)) = (
                            plan_mode_state.as_mut(),
                            seeded_item_id.as_deref(),
                            seeded_parsed,
                        ) {
                            emit_streamed_assistant_text_delta(
                                &sess,
                                &turn_context,
                                Some(state),
                                item_id,
                                parsed,
                            )
                            .await;
                        }
                    }
                    active_item = Some(turn_item);
                    active_item_is_streaming_to_client = stream_item_to_client;
                }
            }
            ResponseEvent::ServerModel(server_model) => {
                if !turn_context
                    .server_model_warning_emitted
                    .load(Ordering::Relaxed)
                    && sess
                        .maybe_warn_on_server_model_mismatch(&step_context, server_model)
                        .await
                {
                    turn_context
                        .server_model_warning_emitted
                        .store(true, Ordering::Relaxed);
                }
            }
            ResponseEvent::ModelVerifications(verifications) => {
                if !turn_context
                    .model_verification_emitted
                    .swap(true, Ordering::Relaxed)
                {
                    sess.emit_model_verification(&turn_context, verifications)
                        .await;
                }
            }
            ResponseEvent::TurnModerationMetadata(metadata) => {
                sess.emit_turn_moderation_metadata(&turn_context, metadata)
                    .await;
            }
            ResponseEvent::SafetyBuffering(buffering) => {
                sess.send_event(
                    &turn_context,
                    EventMsg::SafetyBuffering(SafetyBufferingEvent {
                        model: step_context.settings.model_info.slug.clone(),
                        use_cases: buffering.use_cases,
                        reasons: buffering.reasons,
                        show_buffering_ui: buffering.show_buffering_ui,
                        faster_model: buffering.faster_model,
                    }),
                )
                .await;
            }
            ResponseEvent::ServerReasoningIncluded(included) => {
                sess.set_server_reasoning_included(included).await;
            }
            ResponseEvent::RateLimits(snapshot) => {
                // Update internal state with latest rate limits, but defer sending until
                // token usage is available to avoid duplicate TokenCount events.
                sess.record_rate_limits_info(snapshot).await;
                should_emit_token_count = true;
            }
            ResponseEvent::ModelsEtag(etag) => {
                // Update internal state with latest models etag
                sess.services
                    .models_manager
                    .refresh_if_new_etag(etag, turn_context.config.http_client_factory())
                    .await;
            }
            ResponseEvent::Completed {
                response_id,
                token_usage,
                usage_metadata,
                end_turn,
            } => {
                sess.services
                    .analytics_events_client
                    .track_code_mode_tool_call(
                        codex_analytics::CodeModeToolCallFact::SamplingResponseCompleted {
                            thread_id: sess.thread_id.to_string(),
                            turn_id: turn_context.sub_id.clone(),
                            response_id: response_id.clone(),
                            tool_call_ids: std::mem::take(&mut analytics_tool_call_ids),
                        },
                    );
                flush_assistant_text_segments_all(
                    &sess,
                    &turn_context,
                    plan_mode_state.as_mut(),
                    &mut assistant_message_stream_parsers,
                )
                .await;
                sess.send_event(
                    &turn_context,
                    EventMsg::RawResponseCompleted(RawResponseCompletedEvent {
                        response_id,
                        token_usage: token_usage.clone(),
                        usage_metadata,
                    }),
                )
                .await;
                let budget_result = sess
                    .record_token_usage_info(&turn_context, token_usage.as_ref())
                    .await;
                should_emit_token_count = true;
                should_emit_turn_diff = true;
                if let Err(err) = budget_result {
                    break Err(err);
                }
                if let Some(false) = end_turn {
                    needs_follow_up = true;
                }
                break Ok(SamplingRequestResult {
                    needs_follow_up,
                    last_agent_message,
                    refresh_turn_context: false,
                });
            }
            ResponseEvent::OutputTextDelta(delta) => {
                reroute_safe.store(false, Ordering::Relaxed);
                append_partial_assistant_text(&mut active_response_item, &delta);
                // In review child threads, suppress assistant text deltas; the
                // UI will show a selection popup from the final ReviewOutput.
                if let Some(active) = active_item.as_ref() {
                    if !active_item_is_streaming_to_client {
                        continue;
                    }
                    let item_id = active.id();
                    if matches!(active, TurnItem::AgentMessage(_)) {
                        let parsed = assistant_message_stream_parsers.parse_delta(&item_id, &delta);
                        emit_streamed_assistant_text_delta(
                            &sess,
                            &turn_context,
                            plan_mode_state.as_mut(),
                            &item_id,
                            parsed,
                        )
                        .await;
                    } else {
                        let event = AgentMessageContentDeltaEvent {
                            thread_id: sess.thread_id.to_string(),
                            turn_id: turn_context.sub_id.clone(),
                            item_id,
                            delta,
                        };
                        sess.send_event(&turn_context, EventMsg::AgentMessageContentDelta(event))
                            .await;
                    }
                } else {
                    error_or_panic("OutputTextDelta without active item".to_string());
                }
            }
            ResponseEvent::ToolCallInputDelta {
                item_id: _,
                call_id,
                delta,
            } => {
                reroute_safe.store(false, Ordering::Relaxed);
                append_partial_tool_call_input(&mut active_response_item, &delta);
                let Some((active_call_id, consumer)) = active_tool_argument_diff_consumer.as_mut()
                else {
                    continue;
                };
                let call_id = match call_id {
                    Some(call_id) if call_id.as_str() != active_call_id.as_str() => continue,
                    Some(call_id) => call_id,
                    None => active_call_id.clone(),
                };
                if let Some(event) = consumer.consume_diff(turn_context.as_ref(), call_id, &delta) {
                    sess.send_event(&turn_context, event).await;
                }
            }
            ResponseEvent::ReasoningSummaryDelta {
                delta,
                summary_index,
            } => {
                reroute_safe.store(false, Ordering::Relaxed);
                if uses_sequential_cutoff_reasoning_summaries {
                    continue;
                }
                append_partial_reasoning_summary(&mut active_response_item, summary_index, &delta);
                if let Some(active) = active_item.as_ref() {
                    if !active_item_is_streaming_to_client {
                        continue;
                    }
                    let event = ReasoningContentDeltaEvent {
                        thread_id: sess.thread_id.to_string(),
                        turn_id: turn_context.sub_id.clone(),
                        item_id: active.id(),
                        delta,
                        summary_index,
                    };
                    sess.send_event(&turn_context, EventMsg::ReasoningContentDelta(event))
                        .await;
                } else {
                    error_or_panic("ReasoningSummaryDelta without active item".to_string());
                }
            }
            ResponseEvent::ReasoningSummaryPartAdded { summary_index } => {
                reroute_safe.store(false, Ordering::Relaxed);
                if uses_sequential_cutoff_reasoning_summaries {
                    continue;
                }
                if let Some(active) = active_item.as_ref() {
                    if !active_item_is_streaming_to_client {
                        continue;
                    }
                    let event =
                        EventMsg::AgentReasoningSectionBreak(AgentReasoningSectionBreakEvent {
                            item_id: active.id(),
                            summary_index,
                        });
                    sess.send_event(&turn_context, event).await;
                } else {
                    error_or_panic("ReasoningSummaryPartAdded without active item".to_string());
                }
            }
            ResponseEvent::ReasoningSummaryDone {
                item_id,
                text,
                summary_index,
            } => {
                reroute_safe.store(false, Ordering::Relaxed);
                if !uses_sequential_cutoff_reasoning_summaries {
                    continue;
                }
                append_partial_reasoning_summary(&mut active_response_item, summary_index, &text);
                let Some(active) = active_item.as_ref() else {
                    continue;
                };
                if !active_item_is_streaming_to_client || active.id() != item_id {
                    continue;
                }
                if summary_index > 0 {
                    sess.send_event(
                        &turn_context,
                        EventMsg::AgentReasoningSectionBreak(AgentReasoningSectionBreakEvent {
                            item_id: item_id.clone(),
                            summary_index,
                        }),
                    )
                    .await;
                }
                let event = ReasoningContentDeltaEvent {
                    thread_id: sess.thread_id.to_string(),
                    turn_id: turn_context.sub_id.clone(),
                    item_id,
                    delta: text,
                    summary_index,
                };
                sess.send_event(&turn_context, EventMsg::ReasoningContentDelta(event))
                    .await;
            }
            ResponseEvent::ReasoningContentDelta {
                delta,
                content_index,
            } => {
                reroute_safe.store(false, Ordering::Relaxed);
                append_partial_reasoning_content(&mut active_response_item, content_index, &delta);
                if let Some(active) = active_item.as_ref() {
                    if !active_item_is_streaming_to_client {
                        continue;
                    }
                    let event = ReasoningRawContentDeltaEvent {
                        thread_id: sess.thread_id.to_string(),
                        turn_id: turn_context.sub_id.clone(),
                        item_id: active.id(),
                        delta,
                        content_index,
                    };
                    sess.send_event(&turn_context, EventMsg::ReasoningRawContentDelta(event))
                        .await;
                } else {
                    error_or_panic("ReasoningRawContentDelta without active item".to_string());
                }
            }
        }
    };
    drop(sampling_timing_guard);

    let can_recover_failed_stream = outcome.as_ref().err().is_some_and(|err| {
        err.is_retryable() || classify_model_routing_failure(err.details()).is_some()
    });
    if turn_context.model_profile.is_some() && can_recover_failed_stream {
        if completed_response_item_before_failure {
            interrupted_response.store(true, Ordering::Relaxed);
        }
        let checkpoint_ready = match active_response_item.take() {
            None => reroute_safe.load(Ordering::Relaxed),
            Some(item) if matches!(&item, ResponseItem::Message { role, .. } if role == "assistant") =>
            {
                if let Some(active) = active_item.as_ref()
                    && active_item_is_streaming_to_client
                {
                    flush_assistant_text_segments_for_item(
                        &sess,
                        &turn_context,
                        plan_mode_state.as_mut(),
                        &mut assistant_message_stream_parsers,
                        &active.id(),
                    )
                    .await;
                }
                let previously_active_item = active_item.take();
                let previously_streamed_item = if active_item_is_streaming_to_client {
                    previously_active_item
                } else {
                    None
                };
                let finalized = finalize_interrupted_assistant_item(
                    sess.as_ref(),
                    turn_context.as_ref(),
                    turn_store.as_ref(),
                    &item,
                    previously_streamed_item.as_ref(),
                    plan_mode_state.as_mut(),
                )
                .await;
                if !finalized {
                    error!("failed to finalize interrupted assistant message");
                } else {
                    interrupted_response.store(true, Ordering::Relaxed);
                }
                finalized
            }
            Some(item @ ResponseItem::Reasoning { .. }) => {
                let previously_active_item = active_item.take();
                let previously_streamed_item = if active_item_is_streaming_to_client {
                    previously_active_item
                } else {
                    None
                };
                let finalized = finalize_interrupted_client_item(
                    sess.as_ref(),
                    turn_context.as_ref(),
                    turn_store.as_ref(),
                    &item,
                    previously_streamed_item.as_ref(),
                    plan_mode,
                )
                .await;
                if !finalized {
                    error!("failed to finalize interrupted reasoning item");
                } else {
                    interrupted_response.store(true, Ordering::Relaxed);
                }
                finalized
            }
            Some(item @ ResponseItem::FunctionCall { .. }) => {
                drop(active_tool_argument_diff_consumer.take());
                drop(active_item.take());
                record_interrupted_tool_call(sess.as_ref(), turn_context.as_ref(), item).await;
                interrupted_response.store(true, Ordering::Relaxed);
                true
            }
            Some(item @ ResponseItem::CustomToolCall { .. }) => {
                drop(active_tool_argument_diff_consumer.take());
                drop(active_item.take());
                record_interrupted_tool_call(sess.as_ref(), turn_context.as_ref(), item).await;
                interrupted_response.store(true, Ordering::Relaxed);
                true
            }
            Some(item) => {
                let previously_active_item = active_item.take();
                let previously_streamed_item = if active_item_is_streaming_to_client {
                    previously_active_item
                } else {
                    None
                };
                let finalized = finalize_interrupted_client_item(
                    sess.as_ref(),
                    turn_context.as_ref(),
                    turn_store.as_ref(),
                    &item,
                    previously_streamed_item.as_ref(),
                    plan_mode,
                )
                .await;
                if finalized {
                    interrupted_response.store(true, Ordering::Relaxed);
                }
                finalized
            }
        };
        reroute_safe.store(checkpoint_ready, Ordering::Relaxed);
    }

    flush_assistant_text_segments_all(
        &sess,
        &turn_context,
        plan_mode_state.as_mut(),
        &mut assistant_message_stream_parsers,
    )
    .await;

    let tool_blocking_timing_guard = if in_flight.is_empty() {
        None
    } else {
        Some(turn_context.turn_timing_state.begin_tool_blocking())
    };
    if workspace_cwd_call_seen && tool_call_count != 1 {
        step_context.reject_context_transition_mixed_with_sibling_tool();
    }
    if let Err(err) = drain_in_flight(&mut in_flight, sess.clone(), turn_context.clone()).await {
        reroute_safe.store(false, Ordering::Relaxed);
        return Err(err);
    }
    drop(tool_blocking_timing_guard);

    if should_emit_token_count {
        // A tool call such as request_user_input can intentionally pause the turn. Emit token
        // counts only after pending tools resolve so clients do not see progress events while the
        // turn is waiting on the user. This also needs to happen before returning cancellation so
        // token usage already recorded from the completed response is still persisted.
        sess.send_token_count_event(&turn_context).await;
    }

    if cancellation_token.is_cancelled() {
        return Err(CodexErr::TurnAborted);
    }

    if should_emit_turn_diff {
        let unified_diff = {
            let tracker = turn_diff_tracker.lock().await;
            tracker.get_unified_diff()
        };
        if let Some(unified_diff) = unified_diff {
            let msg = EventMsg::TurnDiff(TurnDiffEvent { unified_diff });
            sess.clone().send_event(&turn_context, msg).await;
        }
    }

    outcome.map(|mut outcome| {
        outcome.refresh_turn_context = step_context.turn_context_refresh_requested();
        outcome
    })
}

pub(crate) fn get_last_assistant_message_from_turn<'a>(
    responses: impl DoubleEndedIterator<Item = &'a ResponseItem>,
) -> Option<String> {
    for item in responses.rev() {
        if let Some(message) = last_assistant_message_from_item(item, /*plan_mode*/ false) {
            return Some(message);
        }
    }
    None
}

#[cfg(test)]
#[path = "turn_tests.rs"]
mod tests;
