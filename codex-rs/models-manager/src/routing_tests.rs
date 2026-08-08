use std::collections::HashSet;

use chrono::TimeZone;
use chrono::Utc;
use codex_protocol::openai_models::ReasoningEffort;
use pretty_assertions::assert_eq;

use super::CandidateSelection;
use super::ModelRoutingState;
use super::RoutingFailureClass;
use crate::ModelRoutingCandidate;

fn candidate(model: &str) -> ModelRoutingCandidate {
    ModelRoutingCandidate {
        model: model.to_string(),
        reasoning_effort: None,
        service_tier: None,
    }
}

fn now() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 8, 12, 0, 0)
        .single()
        .expect("valid timestamp")
}

#[test]
fn temporary_failure_falls_back_then_reprobes_after_cooldown() {
    let preferred = candidate("test-primary");
    let fallback = candidate("test-fallback");
    let candidates = vec![preferred.clone(), fallback.clone()];
    let mut state = ModelRoutingState::default();
    let mut attempted = HashSet::new();

    assert_eq!(
        state.select_candidate(&candidates, &attempted, now()),
        CandidateSelection::Ready(preferred.clone())
    );
    attempted.insert(preferred.clone());
    state.record_failure(
        &preferred,
        RoutingFailureClass::TemporaryAvailability,
        now(),
        /*minimum_retry_at*/ None,
    );
    assert_eq!(
        state.select_candidate(&candidates, &attempted, now()),
        CandidateSelection::Ready(fallback.clone())
    );
    state.record_success(&fallback);

    assert_eq!(
        state.select_candidate(&candidates, &HashSet::new(), now()),
        CandidateSelection::Ready(fallback.clone())
    );
    assert_eq!(
        state.select_candidate(
            &candidates,
            &HashSet::new(),
            now() + chrono::Duration::seconds(30)
        ),
        CandidateSelection::Ready(preferred)
    );
}

#[test]
fn cooldowns_exponentially_increase_and_honor_server_minimum() {
    let route_candidate = candidate("test-primary");
    let mut state = ModelRoutingState::default();

    let first = state.record_failure(
        &route_candidate,
        RoutingFailureClass::TemporaryAvailability,
        now(),
        /*minimum_retry_at*/ None,
    );
    let second = state.record_failure(
        &route_candidate,
        RoutingFailureClass::TemporaryAvailability,
        now(),
        Some(now() + chrono::Duration::minutes(3)),
    );
    assert_eq!(first, now() + chrono::Duration::seconds(30));
    assert_eq!(second, now() + chrono::Duration::minutes(3));

    let unavailable = candidate("test-preview");
    let first = state.record_failure(
        &unavailable,
        RoutingFailureClass::ModelUnavailable,
        now(),
        /*minimum_retry_at*/ None,
    );
    let second = state.record_failure(
        &unavailable,
        RoutingFailureClass::ModelUnavailable,
        now(),
        /*minimum_retry_at*/ None,
    );
    assert_eq!(first, now() + chrono::Duration::minutes(15));
    assert_eq!(second, now() + chrono::Duration::minutes(30));
}

#[test]
fn cooldowns_are_capped_and_failure_classes_have_independent_backoff() {
    let candidate = candidate("model-primary");
    let mut state = ModelRoutingState::default();

    let mut short_retry_at = now();
    for _ in 0..20 {
        short_retry_at = state.record_failure(
            &candidate,
            RoutingFailureClass::TemporaryAvailability,
            now(),
            /*minimum_retry_at*/ None,
        );
    }
    assert_eq!(short_retry_at, now() + chrono::Duration::minutes(10));

    let first_long_retry_at = state.record_failure(
        &candidate,
        RoutingFailureClass::ModelUnavailable,
        now(),
        /*minimum_retry_at*/ None,
    );
    assert_eq!(first_long_retry_at, now() + chrono::Duration::minutes(15));

    let mut long_retry_at = first_long_retry_at;
    for _ in 0..20 {
        long_retry_at = state.record_failure(
            &candidate,
            RoutingFailureClass::ModelUnavailable,
            now(),
            /*minimum_retry_at*/ None,
        );
    }
    assert_eq!(long_retry_at, now() + chrono::Duration::hours(12));
}

#[test]
fn minimum_retry_time_never_shortens_local_cooldown() {
    let candidate = candidate("model-primary");
    let mut state = ModelRoutingState::default();

    let retry_at = state.record_failure(
        &candidate,
        RoutingFailureClass::TemporaryAvailability,
        now(),
        Some(now() + chrono::Duration::seconds(5)),
    );

    assert_eq!(retry_at, now() + chrono::Duration::seconds(30));
}

#[test]
fn later_failures_preserve_an_existing_minimum_retry_time() {
    let candidate = candidate("model-primary");
    let mut state = ModelRoutingState::default();
    state.record_failure(
        &candidate,
        RoutingFailureClass::TemporaryAvailability,
        now(),
        Some(now() + chrono::Duration::hours(1)),
    );

    let retry_at = state.record_failure(
        &candidate,
        RoutingFailureClass::TemporaryAvailability,
        now() + chrono::Duration::minutes(1),
        /*minimum_retry_at*/ None,
    );

    assert_eq!(retry_at, now() + chrono::Duration::hours(1));
}

#[test]
fn success_decays_backoff_and_updates_last_success() {
    let candidate = candidate("model-primary");
    let mut state = ModelRoutingState::default();
    state.record_failure(
        &candidate,
        RoutingFailureClass::TemporaryAvailability,
        now(),
        /*minimum_retry_at*/ None,
    );
    state.record_failure(
        &candidate,
        RoutingFailureClass::TemporaryAvailability,
        now(),
        /*minimum_retry_at*/ None,
    );

    state.record_success(&candidate);

    assert_eq!(state.last_success(), Some(&candidate));
    assert_eq!(
        state.record_failure(
            &candidate,
            RoutingFailureClass::TemporaryAvailability,
            now(),
            /*minimum_retry_at*/ None,
        ),
        now() + chrono::Duration::seconds(60)
    );

    state.record_success(&candidate);
    state.record_success(&candidate);
    assert_eq!(
        state.record_failure(
            &candidate,
            RoutingFailureClass::TemporaryAvailability,
            now(),
            /*minimum_retry_at*/ None,
        ),
        now() + chrono::Duration::seconds(30)
    );
}

#[test]
fn quiet_intervals_decay_old_failure_streaks() {
    let temporary = candidate("model-temporary");
    let unavailable = candidate("model-unavailable");
    let mut state = ModelRoutingState::default();

    for _ in 0..3 {
        state.record_failure(
            &temporary,
            RoutingFailureClass::TemporaryAvailability,
            now(),
            /*minimum_retry_at*/ None,
        );
    }
    assert_eq!(
        state.record_failure(
            &temporary,
            RoutingFailureClass::TemporaryAvailability,
            now() + chrono::Duration::minutes(30),
            /*minimum_retry_at*/ None,
        ),
        now() + chrono::Duration::minutes(30) + chrono::Duration::seconds(30)
    );

    for _ in 0..2 {
        state.record_failure(
            &unavailable,
            RoutingFailureClass::ModelUnavailable,
            now(),
            /*minimum_retry_at*/ None,
        );
    }
    assert_eq!(
        state.record_failure(
            &unavailable,
            RoutingFailureClass::ModelUnavailable,
            now() + chrono::Duration::hours(24),
            /*minimum_retry_at*/ None,
        ),
        now() + chrono::Duration::hours(24) + chrono::Duration::minutes(15)
    );
}

#[test]
fn health_is_keyed_by_the_exact_request_tuple() {
    let base = candidate("model-shared");
    let high_effort = ModelRoutingCandidate {
        reasoning_effort: Some(ReasoningEffort::High),
        ..base.clone()
    };
    let priority_tier = ModelRoutingCandidate {
        service_tier: Some("tier-priority".to_string()),
        ..base.clone()
    };
    let candidates = vec![base.clone(), high_effort.clone(), priority_tier.clone()];
    let mut state = ModelRoutingState::default();
    state.record_failure(
        &base,
        RoutingFailureClass::TemporaryAvailability,
        now(),
        /*minimum_retry_at*/ None,
    );

    assert_eq!(
        state.select_candidate(&candidates, &HashSet::new(), now()),
        CandidateSelection::Ready(high_effort)
    );

    let attempted = HashSet::from([base, priority_tier]);
    assert_eq!(
        state.select_candidate(&candidates, &attempted, now()),
        CandidateSelection::Ready(candidates[1].clone())
    );
}

#[test]
fn selection_reports_exhaustion_when_every_candidate_was_attempted() {
    let candidates = vec![candidate("model-primary"), candidate("model-fallback")];
    let attempted = candidates.iter().cloned().collect();
    let mut state = ModelRoutingState::default();

    assert_eq!(
        state.select_candidate(&candidates, &attempted, now()),
        CandidateSelection::Exhausted
    );
}

#[test]
fn selection_returns_earliest_retry_instead_of_bypassing_cooldowns() {
    let primary = candidate("model-primary");
    let fallback = candidate("model-fallback");
    let candidates = vec![primary.clone(), fallback.clone()];
    let mut state = ModelRoutingState::default();
    state.record_failure(
        &primary,
        RoutingFailureClass::TemporaryAvailability,
        now(),
        Some(now() + chrono::Duration::minutes(2)),
    );
    state.record_failure(
        &fallback,
        RoutingFailureClass::TemporaryAvailability,
        now(),
        /*minimum_retry_at*/ None,
    );

    assert_eq!(
        state.select_candidate(&candidates, &HashSet::new(), now()),
        CandidateSelection::CoolingDown {
            candidate: fallback.clone(),
            retry_at: now() + chrono::Duration::seconds(30),
        }
    );
    assert_eq!(
        state.select_candidate(
            &candidates,
            &HashSet::new(),
            now() + chrono::Duration::seconds(30),
        ),
        CandidateSelection::Ready(fallback)
    );
}

#[test]
fn cooling_candidate_uses_profile_order_to_break_retry_time_ties() {
    let primary = candidate("model-primary");
    let fallback = candidate("model-fallback");
    let candidates = vec![primary.clone(), fallback];
    let mut state = ModelRoutingState::default();
    for candidate in &candidates {
        state.record_failure(
            candidate,
            RoutingFailureClass::TemporaryAvailability,
            now(),
            /*minimum_retry_at*/ None,
        );
    }

    assert_eq!(
        state.select_candidate(&candidates, &HashSet::new(), now()),
        CandidateSelection::CoolingDown {
            candidate: primary,
            retry_at: now() + chrono::Duration::seconds(30),
        }
    );
}

#[test]
fn reconciliation_retains_unchanged_health_and_forgets_removed_candidates() {
    let preferred = candidate("test-primary");
    let fallback = candidate("test-fallback");
    let replacement = candidate("test-replacement");
    let mut state = ModelRoutingState::default();
    state.record_failure(
        &preferred,
        RoutingFailureClass::TemporaryAvailability,
        now(),
        /*minimum_retry_at*/ None,
    );
    state.record_success(&fallback);

    state.reconcile(&[preferred.clone(), replacement.clone()]);

    assert_eq!(
        state.select_candidate(&[preferred, replacement.clone()], &HashSet::new(), now()),
        CandidateSelection::Ready(replacement)
    );
}

#[test]
fn reconciliation_reports_profile_changes_and_preserves_unchanged_health() {
    let preferred = candidate("model-primary");
    let fallback = candidate("model-fallback");
    let added = candidate("model-added");
    let mut state = ModelRoutingState::default();
    state.record_failure(
        &preferred,
        RoutingFailureClass::TemporaryAvailability,
        now(),
        /*minimum_retry_at*/ None,
    );

    assert!(!state.reconcile(&[preferred.clone(), fallback.clone()]));
    assert!(!state.reconcile(&[preferred.clone(), fallback.clone()]));
    assert!(state.reconcile(&[fallback.clone(), preferred.clone()]));
    assert!(state.reconcile(&[preferred.clone(), fallback.clone(), added]));
    assert!(state.reconcile(&[preferred.clone(), fallback.clone()]));
    assert_eq!(
        state.select_candidate(&[preferred, fallback.clone()], &HashSet::new(), now()),
        CandidateSelection::Ready(fallback)
    );
}

#[test]
fn selecting_a_different_profile_name_resets_candidate_health() {
    let preferred = candidate("model-primary");
    let fallback = candidate("model-fallback");
    let candidates = vec![preferred.clone(), fallback.clone()];
    let mut state = ModelRoutingState::default();
    state.reconcile_profile("profile-one");
    state.record_failure(
        &preferred,
        RoutingFailureClass::TemporaryAvailability,
        now(),
        /*minimum_retry_at*/ None,
    );
    state.record_success(&fallback);

    assert!(state.reconcile_profile("profile-two"));
    assert_eq!(state.last_success(), None);
    assert_eq!(
        state.select_candidate(&candidates, &HashSet::new(), now()),
        CandidateSelection::Ready(preferred)
    );
}

#[test]
fn validated_profile_rename_preserves_candidate_health() {
    let preferred = candidate("model-primary");
    let fallback = candidate("model-fallback");
    let candidates = vec![preferred.clone(), fallback.clone()];
    let mut state = ModelRoutingState::default();
    state.reconcile_profile("profile-before");
    state.record_failure(
        &preferred,
        RoutingFailureClass::TemporaryAvailability,
        now(),
        /*minimum_retry_at*/ None,
    );
    state.record_success(&fallback);

    state.rename_profile("profile-before", "profile-after");

    assert!(!state.reconcile_profile("profile-after"));
    assert_eq!(state.last_success(), Some(&fallback));
    assert_eq!(
        state.select_candidate(&candidates, &HashSet::new(), now()),
        CandidateSelection::Ready(fallback)
    );
}
