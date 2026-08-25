use super::Deployment;
use super::PERFORMANCE_WINDOW_INTERVAL;
use super::PerformanceSummary;
use super::PerformanceWindow;
use super::valid_build_cohort;
use super::valid_build_revision;
use crate::app_event::AppEvent;
use crate::version::CODEX_CLI_VERSION;
use pretty_assertions::assert_eq;
use std::time::Duration;
use std::time::Instant;

#[test]
fn accepts_only_bounded_lowercase_git_revisions() {
    assert!(valid_build_revision("abcdef0"));
    assert!(valid_build_revision(
        "0123456789abcdef0123456789abcdef01234567"
    ));
    assert!(valid_build_revision("abcdef0-dirty"));

    assert!(!valid_build_revision("abcdef"));
    assert!(!valid_build_revision("ABCDEF0"));
    assert!(!valid_build_revision("abcdef0-dirty/other"));
    assert!(!valid_build_revision(
        "0123456789abcdef0123456789abcdef012345678"
    ));
}

#[test]
fn captures_static_application_event_variants_without_consuming_payloads() {
    let event = AppEvent::NewSession {
        name: Some("private session name".to_string()),
    };
    let event_kind: &'static str = (&event).into();

    assert_eq!(event_kind, "NewSession");
    let AppEvent::NewSession { name } = event else {
        panic!("borrowed variant conversion consumed the event");
    };
    assert_eq!(name, Some("private session name".to_string()));

    let event = AppEvent::OpenAgentPicker;
    let event_kind: &'static str = (&event).into();
    assert_eq!(event_kind, "OpenAgentPicker");
}

#[test]
fn accepts_only_bounded_safe_cohort_labels() {
    assert!(valid_build_cohort("candidate-1"));
    assert!(valid_build_cohort("last_known_good"));

    assert!(!valid_build_cohort(""));
    assert!(!valid_build_cohort("candidate/current"));
    assert!(!valid_build_cohort(
        "candidate_label_that_is_too_long_for_a_bounded_metric"
    ));
}

#[test]
fn validates_build_attribution_without_reading_process_environment() {
    assert_eq!(
        Deployment::new("abcdef0-dirty".to_string(), Some("canary-1".to_string())),
        Some(Deployment {
            revision: "abcdef0-dirty".to_string(),
            cohort: "canary-1".to_string(),
        })
    );
    assert_eq!(
        Deployment::new("abcdef0".to_string(), Some("unsafe/cohort".to_string())),
        Some(Deployment {
            revision: "abcdef0".to_string(),
            cohort: "default".to_string(),
        })
    );
    assert_eq!(
        Deployment::new("not-a-revision".to_string(), /*cohort*/ None),
        None
    );
}

#[test]
fn identifies_frankendex_revisions_in_displayed_versions() {
    let deployment = Deployment::new("abcdef012345-dirty".to_string(), /*cohort*/ None)
        .expect("valid deployment attribution");

    assert_eq!(
        deployment.display_version(),
        format!("{CODEX_CLI_VERSION}+frankendex.abcdef012345-dirty"),
    );
}

#[test]
fn retains_deployment_attribution_across_completed_windows() {
    let started_at = Instant::now();
    let deployment = Deployment::new("abcdef0".to_string(), Some("canary".to_string()))
        .expect("valid deployment attribution");
    let mut window = PerformanceWindow::with_deployment(started_at, Some(deployment.clone()));

    assert!(
        window
            .record(
                "draw",
                Duration::from_millis(1),
                Duration::from_millis(16),
                started_at + PERFORMANCE_WINDOW_INTERVAL,
            )
            .is_some()
    );
    assert_eq!(window.deployment, Some(deployment));
}

#[test]
fn summarizes_events_only_after_the_activity_window_elapses() {
    let started_at = Instant::now();
    let mut window = PerformanceWindow::new(started_at);
    let slow_threshold = Duration::from_millis(16);

    assert_eq!(
        window.record(
            "key",
            Duration::from_micros(500),
            slow_threshold,
            started_at + Duration::from_secs(1),
        ),
        None
    );
    assert_eq!(
        window.record(
            "draw",
            Duration::from_millis(16),
            slow_threshold,
            started_at + Duration::from_secs(2),
        ),
        None
    );

    assert_eq!(
        window.record(
            "application",
            Duration::from_millis(4),
            slow_threshold,
            started_at + PERFORMANCE_WINDOW_INTERVAL,
        ),
        Some(PerformanceSummary {
            window_duration: PERFORMANCE_WINDOW_INTERVAL,
            event_count: 3,
            draw_count: 1,
            slow_event_count: 1,
            slow_draw_count: 1,
            total_duration: Duration::from_micros(20_500),
            max_duration: Duration::from_millis(16),
            duration_buckets: [1, 0, 1, 0, 1],
        })
    );
}

#[test]
fn starts_a_fresh_window_after_emitting_a_summary() {
    let started_at = Instant::now();
    let mut window = PerformanceWindow::new(started_at);
    let slow_threshold = Duration::from_millis(16);

    assert!(
        window
            .record(
                "draw",
                Duration::from_millis(20),
                slow_threshold,
                started_at + PERFORMANCE_WINDOW_INTERVAL,
            )
            .is_some()
    );
    assert_eq!(
        window.record(
            "paste",
            Duration::from_millis(2),
            slow_threshold,
            started_at + PERFORMANCE_WINDOW_INTERVAL + Duration::from_secs(1),
        ),
        None
    );

    assert_eq!(
        window.record(
            "resize",
            Duration::from_millis(9),
            slow_threshold,
            started_at + PERFORMANCE_WINDOW_INTERVAL + PERFORMANCE_WINDOW_INTERVAL,
        ),
        Some(PerformanceSummary {
            window_duration: PERFORMANCE_WINDOW_INTERVAL,
            event_count: 2,
            draw_count: 0,
            slow_event_count: 0,
            slow_draw_count: 0,
            total_duration: Duration::from_millis(11),
            max_duration: Duration::from_millis(9),
            duration_buckets: [0, 1, 0, 1, 0],
        })
    );
}

#[test]
fn assigns_events_to_fixed_latency_buckets() {
    let started_at = Instant::now();
    let mut window = PerformanceWindow::new(started_at);
    let slow_threshold = Duration::from_secs(1);

    for (index, duration) in [
        Duration::from_micros(999),
        Duration::from_millis(1),
        Duration::from_millis(4),
        Duration::from_millis(8),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            window.record(
                "key",
                duration,
                slow_threshold,
                started_at + Duration::from_secs(index as u64),
            ),
            None
        );
    }

    let summary = window
        .record(
            "key",
            Duration::from_millis(16),
            slow_threshold,
            started_at + PERFORMANCE_WINDOW_INTERVAL,
        )
        .expect("emit completed latency window");
    assert_eq!(summary.duration_buckets, [1, 1, 1, 1, 1]);
}

#[test]
fn saturates_event_counters_and_total_duration() {
    let started_at = Instant::now();
    let mut window = PerformanceWindow::new(started_at);
    window.event_count = u64::MAX;
    window.draw_count = u64::MAX;
    window.slow_event_count = u64::MAX;
    window.slow_draw_count = u64::MAX;
    window.duration_buckets[4] = u64::MAX;
    window.total_duration = Duration::MAX;

    assert_eq!(
        window.record(
            "draw",
            Duration::from_secs(1),
            Duration::from_millis(16),
            started_at + PERFORMANCE_WINDOW_INTERVAL,
        ),
        Some(PerformanceSummary {
            window_duration: PERFORMANCE_WINDOW_INTERVAL,
            event_count: u64::MAX,
            draw_count: u64::MAX,
            slow_event_count: u64::MAX,
            slow_draw_count: u64::MAX,
            total_duration: Duration::MAX,
            max_duration: Duration::from_secs(1),
            duration_buckets: [0, 0, 0, 0, u64::MAX],
        })
    );
}
