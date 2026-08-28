//! Per-thread custom-model routing at reconstructable sampling checkpoints.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use codex_models_manager::ModelRoutingCandidate;
use codex_models_manager::routing::CandidateSelection;
use codex_models_manager::routing::ModelRoutingState;
use codex_models_manager::routing::RoutingFailureClass;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::WarningEvent;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::warn;

use super::context_transition::ContextTransitionTarget;
use super::session::Session;
use super::turn_context::TurnContext;

#[derive(Clone, Copy)]
pub(super) enum ModelRoutingOwner<'a> {
    Admission,
    Active(&'a ContextTransitionTarget),
}

/// Explains why a custom-model routing profile changed its active request configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ModelRoutingReason {
    TemporaryAvailability,
    UsageLimit,
    ModelUnavailable,
    PreferredCandidateRecovered,
    ProfileConfigurationChanged,
}

pub(super) struct ModelRoutingFailure {
    pub(super) reason: ModelRoutingReason,
    pub(super) minimum_retry_at: Option<DateTime<Utc>>,
    pub(super) class: RoutingFailureClass,
}

pub(super) struct ModelRoutingSelection {
    pub(super) context: TurnContext,
    pub(super) last_success: Option<ModelRoutingCandidate>,
    pub(super) profile_changed: bool,
    /// Earliest time the selected concrete candidate may issue a provider request.
    pub(super) retry_at: Option<DateTime<Utc>>,
}

impl Session {
    async fn with_model_routing_state<T>(
        &self,
        owner: ModelRoutingOwner<'_>,
        update: impl FnOnce(&mut ModelRoutingState) -> T,
    ) -> Option<T> {
        let active = match owner {
            ModelRoutingOwner::Admission => None,
            ModelRoutingOwner::Active(_) => Some(self.active_turn.lock().await),
        };
        let mut state = self.state.lock().await;
        if let ModelRoutingOwner::Active(target) = owner {
            target
                .check(
                    active
                        .as_ref()
                        .and_then(|active| active.as_ref())
                        .and_then(|turn| turn.task.as_ref()),
                )
                .ok()?;
        }
        Some(update(&mut state.model_routing))
    }

    async fn model_routing_now(&self) -> DateTime<Utc> {
        match self
            .services
            .time_provider
            .current_time(self.thread_id())
            .await
        {
            Ok(now) => now,
            Err(err) => {
                warn!("failed to read model routing clock: {err}");
                Utc::now()
            }
        }
    }

    /// Waits until a selected candidate's request gate opens while the owning turn remains
    /// interruptible.
    pub(crate) async fn wait_for_model_routing_retry(
        &self,
        retry_at: Option<DateTime<Utc>>,
        cancellation_token: &CancellationToken,
    ) -> bool {
        let Some(retry_at) = retry_at else {
            return true;
        };
        let wait = retry_at
            .signed_duration_since(self.model_routing_now().await)
            .to_std()
            .unwrap_or_default();
        if wait.is_zero() {
            return true;
        }
        info!(
            wait_seconds = wait.as_secs(),
            "waiting for the next model routing candidate cooldown"
        );
        let provider_wait = self.services.time_provider.sleep(self.thread_id(), wait);
        let provider_result = tokio::select! {
            _ = cancellation_token.cancelled() => return false,
            result = provider_wait => result,
        };
        if let Err(err) = provider_result {
            warn!("model routing clock failed to wait for a candidate cooldown: {err}");
            let remaining = retry_at
                .signed_duration_since(self.model_routing_now().await)
                .to_std()
                .unwrap_or_default();
            tokio::select! {
                _ = cancellation_token.cancelled() => return false,
                _ = tokio::time::sleep(remaining) => {}
            }
        }
        true
    }

    pub(super) async fn select_model_routing_context(
        &self,
        base: &TurnContext,
        profile_name: &str,
        attempted: &HashSet<ModelRoutingCandidate>,
        owner: ModelRoutingOwner<'_>,
    ) -> Option<ModelRoutingSelection> {
        let custom_model = base.config.custom_models.get(profile_name)?;
        let profile = custom_model.routing_profile.as_ref()?;
        let trust_candidate_constraints = custom_model.trust_candidate_constraints;
        let now = self.model_routing_now().await;
        let mut attempted = attempted.clone();
        let mut last_rejected = None;
        let (profile_changed, previous_success) = self
            .with_model_routing_state(owner, |routing| {
                let previous_success = routing.last_success().cloned();
                let profile_changed = routing.reconcile_profile(profile_name)
                    | routing.reconcile(&profile.candidates);
                (profile_changed, previous_success)
            })
            .await?;
        loop {
            let selection = self
                .with_model_routing_state(owner, |routing| {
                    routing.select_candidate(&profile.candidates, &attempted, now)
                })
                .await?;
            let (candidate, retry_at) = match selection {
                CandidateSelection::Ready(candidate) => (candidate, None),
                CandidateSelection::CoolingDown {
                    candidate,
                    retry_at,
                } => (candidate, Some(retry_at)),
                CandidateSelection::Exhausted => {
                    let candidate = last_rejected?;
                    let context = base
                        .with_unchecked_routing_candidate(
                            profile_name,
                            &candidate,
                            &self.services.models_manager,
                        )
                        .await;
                    return Some(ModelRoutingSelection {
                        context,
                        last_success: previous_success,
                        profile_changed,
                        retry_at: None,
                    });
                }
            };
            let context = if trust_candidate_constraints {
                Some(
                    base.with_unchecked_routing_candidate(
                        profile_name,
                        &candidate,
                        &self.services.models_manager,
                    )
                    .await,
                )
            } else {
                base.with_routing_candidate(profile_name, &candidate, &self.services.models_manager)
                    .await
            };
            if let Some(context) = context {
                return Some(ModelRoutingSelection {
                    context,
                    last_success: previous_success,
                    profile_changed,
                    retry_at,
                });
            }
            attempted.insert(candidate.clone());
            last_rejected = Some(candidate);
        }
    }

    pub(super) async fn record_model_routing_failure(
        &self,
        turn_context: &Arc<TurnContext>,
        done: &Arc<Notify>,
        cancellation_token: &CancellationToken,
        failure: &ModelRoutingFailure,
    ) {
        let Some(candidate) = turn_context.model_routing_candidate.as_ref() else {
            return;
        };
        let Some(target) = self
            .capture_context_transition(turn_context, done, cancellation_token)
            .await
        else {
            return;
        };
        let now = self.model_routing_now().await;
        let _ = self
            .with_model_routing_state(ModelRoutingOwner::Active(&target), |routing| {
                routing.record_failure(candidate, failure.class, now, failure.minimum_retry_at);
            })
            .await;
    }

    pub(super) async fn record_model_routing_success(
        &self,
        turn_context: &Arc<TurnContext>,
        done: &Arc<Notify>,
        cancellation_token: &CancellationToken,
    ) {
        let Some(candidate) = turn_context.model_routing_candidate.as_ref() else {
            return;
        };
        let Some(target) = self
            .capture_context_transition(turn_context, done, cancellation_token)
            .await
        else {
            return;
        };
        let _ = self
            .with_model_routing_state(ModelRoutingOwner::Active(&target), |routing| {
                routing.record_success(candidate);
            })
            .await;
    }

    pub(super) async fn notify_model_routing_change(
        &self,
        from: &TurnContext,
        to: &Arc<TurnContext>,
        reason: ModelRoutingReason,
    ) {
        info!(
            profile = to.model_profile.as_deref(),
            from_model = from.model_info().slug,
            to_model = to.model_info().slug,
            from_reasoning_effort = ?from.reasoning_effort(),
            to_reasoning_effort = ?to.reasoning_effort(),
            from_service_tier = ?from.config.service_tier,
            to_service_tier = ?to.config.service_tier,
            ?reason,
            "model routing profile changed its active request configuration"
        );
        self.send_event(
            to,
            EventMsg::Warning(WarningEvent {
                message: format!(
                    "Model profile `{}` changed its active request configuration because the previous configuration was unavailable.",
                    to.model_profile.as_deref().unwrap_or("custom")
                ),
            }),
        )
        .await;
    }

    pub(super) async fn notify_model_routing_candidate_change(
        &self,
        from: &ModelRoutingCandidate,
        to: &Arc<TurnContext>,
        reason: ModelRoutingReason,
    ) {
        info!(
            profile = to.model_profile.as_deref(),
            from_model = from.model,
            to_model = to.model_info().slug,
            from_reasoning_effort = ?from.reasoning_effort,
            to_reasoning_effort = ?to.reasoning_effort(),
            from_service_tier = ?from.service_tier,
            to_service_tier = ?to.config.service_tier,
            ?reason,
            "model routing profile updated its active request configuration"
        );
        self.send_event(
            to,
            EventMsg::Warning(WarningEvent {
                message: format!(
                    "Model profile `{}` updated its active request configuration.",
                    to.model_profile.as_deref().unwrap_or("custom")
                ),
            }),
        )
        .await;
    }
}

pub(super) fn classify_model_routing_failure(
    details: &CodexErrorDetails,
) -> Option<ModelRoutingFailure> {
    match details {
        CodexErrorDetails::ServerOverloaded => Some(ModelRoutingFailure {
            reason: ModelRoutingReason::TemporaryAvailability,
            minimum_retry_at: None,
            class: RoutingFailureClass::TemporaryAvailability,
        }),
        CodexErrorDetails::UsageLimitReached(error) => Some(ModelRoutingFailure {
            reason: ModelRoutingReason::UsageLimit,
            minimum_retry_at: error.resets_at,
            class: RoutingFailureClass::TemporaryAvailability,
        }),
        CodexErrorDetails::ModelUnavailable(_) => Some(ModelRoutingFailure {
            reason: ModelRoutingReason::ModelUnavailable,
            minimum_retry_at: None,
            class: RoutingFailureClass::ModelUnavailable,
        }),
        _ => None,
    }
}
