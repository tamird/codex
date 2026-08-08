//! Candidate health and selection for routed custom model aliases.

use std::collections::HashMap;
use std::collections::HashSet;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;

use crate::ModelRoutingCandidate;

const TEMPORARY_INITIAL_COOLDOWN: Duration = Duration::seconds(30);
const TEMPORARY_MAX_COOLDOWN: Duration = Duration::minutes(10);
const TEMPORARY_FAILURE_DECAY: Duration = Duration::minutes(10);
const MODEL_UNAVAILABLE_INITIAL_COOLDOWN: Duration = Duration::minutes(15);
const MODEL_UNAVAILABLE_MAX_COOLDOWN: Duration = Duration::hours(12);
const MODEL_UNAVAILABLE_FAILURE_DECAY: Duration = Duration::hours(12);

/// Semantic failure classes that affect when a routing candidate may be probed again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingFailureClass {
    TemporaryAvailability,
    ModelUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CandidateHealth {
    failure_class: RoutingFailureClass,
    consecutive_failures: u32,
    last_failure_at: DateTime<Utc>,
    retry_at: Option<DateTime<Utc>>,
}

/// Result of applying candidate order, this turn's attempts, and stored cooldowns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidateSelection {
    /// The candidate may be requested now.
    Ready(ModelRoutingCandidate),
    /// The earliest untried candidate may not be requested before `retry_at`.
    CoolingDown {
        candidate: ModelRoutingCandidate,
        retry_at: DateTime<Utc>,
    },
    /// Every configured candidate was already attempted in this user turn.
    Exhausted,
}

/// Per-thread health observations for the candidates in one selected profile.
#[derive(Debug, Default)]
pub struct ModelRoutingState {
    health: HashMap<ModelRoutingCandidate, CandidateHealth>,
    last_success: Option<ModelRoutingCandidate>,
    profile_name: Option<String>,
    profile_candidates: Vec<ModelRoutingCandidate>,
}

impl ModelRoutingState {
    /// Returns the candidate most recently recorded as successful.
    pub fn last_success(&self) -> Option<&ModelRoutingCandidate> {
        self.last_success.as_ref()
    }

    /// Resets observations when the thread explicitly selects a different profile name.
    pub fn reconcile_profile(&mut self, profile_name: &str) -> bool {
        let changed = self
            .profile_name
            .as_deref()
            .is_some_and(|previous| previous != profile_name);
        if changed {
            self.health.clear();
            self.last_success = None;
            self.profile_candidates.clear();
        }
        self.profile_name = Some(profile_name.to_string());
        changed
    }

    /// Preserves observations across a validated one-to-one config rename.
    pub fn rename_profile(&mut self, previous: &str, next: &str) {
        if self.profile_name.as_deref() == Some(previous) {
            self.profile_name = Some(next.to_string());
        }
    }

    /// Retains health for unchanged candidates and forgets removed candidates.
    pub fn reconcile(&mut self, candidates: &[ModelRoutingCandidate]) -> bool {
        let profile_changed =
            !self.profile_candidates.is_empty() && self.profile_candidates.as_slice() != candidates;
        self.profile_candidates = candidates.to_vec();
        let candidates = candidates.iter().cloned().collect::<HashSet<_>>();
        self.health
            .retain(|candidate, _| candidates.contains(candidate));
        if self
            .last_success
            .as_ref()
            .is_some_and(|candidate| !candidates.contains(candidate))
        {
            self.last_success = None;
        }
        profile_changed
    }

    /// Selects the highest-ranked candidate that has not been tried in this turn.
    ///
    /// Cooldowns are hard request gates. Callers may wait until `retry_at`, but must not send a
    /// cooling candidate merely because every configured candidate is unavailable.
    pub fn select_candidate(
        &mut self,
        candidates: &[ModelRoutingCandidate],
        attempted: &HashSet<ModelRoutingCandidate>,
        now: DateTime<Utc>,
    ) -> CandidateSelection {
        self.reconcile(candidates);
        if let Some(candidate) = candidates.iter().find(|candidate| {
            !attempted.contains(*candidate)
                && self
                    .health
                    .get(*candidate)
                    .and_then(|health| health.retry_at)
                    .is_none_or(|retry_at| retry_at <= now)
        }) {
            return CandidateSelection::Ready(candidate.clone());
        }

        let earliest_retry = candidates
            .iter()
            .enumerate()
            .filter(|(_, candidate)| !attempted.contains(*candidate))
            .filter_map(|(index, candidate)| {
                self.health
                    .get(candidate)
                    .and_then(|health| health.retry_at)
                    .map(|retry_at| (index, candidate, retry_at))
            })
            .min_by_key(|(index, _, retry_at)| (*retry_at, *index));
        if let Some((_, candidate, retry_at)) = earliest_retry {
            return CandidateSelection::CoolingDown {
                candidate: candidate.clone(),
                retry_at,
            };
        }

        CandidateSelection::Exhausted
    }

    /// Records a failed attempt and returns the next time the candidate may be probed.
    pub fn record_failure(
        &mut self,
        candidate: &ModelRoutingCandidate,
        class: RoutingFailureClass,
        now: DateTime<Utc>,
        minimum_retry_at: Option<DateTime<Utc>>,
    ) -> DateTime<Utc> {
        let previous_health = self.health.get(candidate);
        let (initial, maximum, decay_interval) = backoff_policy(class);
        let previous_failures = previous_health.map_or(0, |health| {
            if health.failure_class == class {
                let elapsed = now
                    .signed_duration_since(health.last_failure_at)
                    .max(Duration::zero());
                let decay_steps = elapsed.num_seconds() / decay_interval.num_seconds();
                health
                    .consecutive_failures
                    .saturating_sub(u32::try_from(decay_steps).unwrap_or(u32::MAX))
            } else {
                0
            }
        });
        let consecutive_failures = previous_failures.saturating_add(1);
        let exponent = consecutive_failures.saturating_sub(1).min(30);
        let multiplier = 1_i32.checked_shl(exponent).unwrap_or(i32::MAX);
        let cooldown = initial
            .checked_mul(multiplier)
            .unwrap_or(maximum)
            .min(maximum);
        let retry_at = (now + cooldown).max(minimum_retry_at.unwrap_or(now)).max(
            previous_health
                .and_then(|health| health.retry_at)
                .unwrap_or(now),
        );
        self.health.insert(
            candidate.clone(),
            CandidateHealth {
                failure_class: class,
                consecutive_failures,
                last_failure_at: now,
                retry_at: Some(retry_at),
            },
        );
        retry_at
    }

    /// Opens the candidate's retry gate and decays one failure step after a successful request.
    pub fn record_success(&mut self, candidate: &ModelRoutingCandidate) {
        if let Some(health) = self.health.get_mut(candidate) {
            if health.consecutive_failures <= 1 {
                self.health.remove(candidate);
            } else {
                health.consecutive_failures -= 1;
                health.retry_at = None;
            }
        }
        self.last_success = Some(candidate.clone());
    }
}

fn backoff_policy(class: RoutingFailureClass) -> (Duration, Duration, Duration) {
    match class {
        RoutingFailureClass::TemporaryAvailability => (
            TEMPORARY_INITIAL_COOLDOWN,
            TEMPORARY_MAX_COOLDOWN,
            TEMPORARY_FAILURE_DECAY,
        ),
        RoutingFailureClass::ModelUnavailable => (
            MODEL_UNAVAILABLE_INITIAL_COOLDOWN,
            MODEL_UNAVAILABLE_MAX_COOLDOWN,
            MODEL_UNAVAILABLE_FAILURE_DECAY,
        ),
    }
}

#[cfg(test)]
#[path = "routing_tests.rs"]
mod tests;
