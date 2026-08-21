use crate::agent::control::SpawnAgentForkMode;
use crate::agent::control::SpawnAgentOptions;
use crate::agent::next_thread_spawn_depth;
use crate::session::session::Session;
use chrono::Utc;
use codex_extension_api::ThreadIdleCause;
use codex_history::RolloutItem;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadGoal;
use codex_protocol::protocol::ThreadGoalStatus;
use codex_protocol::protocol::ThreadGoalUpdatedEvent;
use codex_protocol::protocol::WarningEvent;
use codex_protocol::user_input::UserInput;
use serde::Serialize;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::warn;

pub(crate) const GOAL_SUPERVISOR_ROLE_NAME: &str = "goal_supervisor";
const MIN_SUPERVISOR_SNOOZE_SECONDS: u64 = 1;
const MAX_SUPERVISOR_SNOOZE_REASON_CHARS: usize = 120;
const INITIAL_SUPERVISOR_FAILURE_RETRY_SECONDS: u64 = 60;
const MAX_SUPERVISOR_FAILURE_RETRY_SECONDS: u64 = 60 * 60;
const SUPERVISOR_FAILURE_JITTER_DIVISOR: u64 = 5;

pub(crate) struct GoalSupervisorRuntimeState {
    transition_lock: Arc<Mutex<()>>,
    active_helper_id: Mutex<Option<ThreadId>>,
    active_goal_id: Mutex<Option<String>>,
    snoozed_until: Mutex<Option<SupervisorWakeDeadline>>,
    scheduled_wakeup: Mutex<Option<ScheduledSupervisorWakeup>>,
    wakeup_generation: AtomicU64,
    failure_backoff: Mutex<SupervisorFailureBackoff>,
    last_action: Mutex<Option<SupervisorActionRecord>>,
    snooze_records: Mutex<Vec<SupervisorSnoozeRecord>>,
}

impl GoalSupervisorRuntimeState {
    pub(crate) fn new() -> Self {
        Self {
            transition_lock: Arc::new(Mutex::new(())),
            active_helper_id: Mutex::new(None),
            active_goal_id: Mutex::new(None),
            snoozed_until: Mutex::new(None),
            scheduled_wakeup: Mutex::new(None),
            wakeup_generation: AtomicU64::new(0),
            failure_backoff: Mutex::new(SupervisorFailureBackoff::default()),
            last_action: Mutex::new(None),
            snooze_records: Mutex::new(Vec::new()),
        }
    }
}

/// One in-memory supervisor deadline scoped to the goal that created it.
#[derive(Clone, Debug)]
struct SupervisorWakeDeadline {
    goal_id: String,
    deadline: Instant,
}

/// The one live timer responsible for an in-memory supervisor deadline.
#[derive(Clone, Debug)]
struct ScheduledSupervisorWakeup {
    wake: SupervisorWakeDeadline,
    generation: u64,
    abort_handle: tokio::task::AbortHandle,
}

/// Consecutive implicit supervisor failures for one active goal.
#[derive(Debug, Default)]
struct SupervisorFailureBackoff {
    goal_id: Option<String>,
    consecutive_failures: u32,
    last_warned_base_delay_seconds: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum SupervisorActionKind {
    CompactParentContext,
    FollowupTask,
    Snooze,
}

#[derive(Clone, Debug)]
struct SupervisorActionRecord {
    goal_id: Option<String>,
    kind: SupervisorActionKind,
    sent_at: chrono::DateTime<Utc>,
    delivered_parent_message: Option<InterAgentCommunication>,
    snoozed_seconds: Option<u64>,
}

#[derive(Clone, Debug)]
struct SupervisorSnoozeRecord {
    goal_id: String,
    sent_at: chrono::DateTime<Utc>,
    snoozed_seconds: u64,
}

pub(crate) fn is_goal_supervisor_helper_source(session_source: &SessionSource) -> bool {
    matches!(
        session_source,
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            agent_role: Some(agent_role),
            ..
        }) if agent_role == GOAL_SUPERVISOR_ROLE_NAME
    )
}

pub(crate) async fn restart_active_helper_for_execution_settings_change(session: &Arc<Session>) {
    let transition = Arc::clone(&session.goal_supervisor_runtime.transition_lock)
        .lock_owned()
        .await;
    let active_helper_id = *session
        .goal_supervisor_runtime
        .active_helper_id
        .lock()
        .await;
    let Some(active_helper_id) = active_helper_id else {
        return;
    };
    if !matches!(
        session
            .services
            .agent_control
            .get_status(active_helper_id)
            .await,
        AgentStatus::PendingInit | AgentStatus::Running
    ) {
        return;
    }
    reset_failure_backoff(session).await;
    let active_goal_id = session
        .goal_supervisor_runtime
        .active_goal_id
        .lock()
        .await
        .clone();
    *session
        .goal_supervisor_runtime
        .active_helper_id
        .lock()
        .await = None;
    *session.goal_supervisor_runtime.active_goal_id.lock().await = None;
    if let Err(err) = session
        .services
        .agent_control
        .shutdown_live_agent(active_helper_id)
        .await
    {
        warn!(
            helper_thread_id = %active_helper_id,
            "failed to stop goal supervisor after execution settings changed: {err}"
        );
        *session
            .goal_supervisor_runtime
            .active_helper_id
            .lock()
            .await = Some(active_helper_id);
        *session.goal_supervisor_runtime.active_goal_id.lock().await = active_goal_id;
        return;
    }
    drop(transition);
    let session = Arc::clone(session);
    tokio::spawn(restart_active_goal_supervisor_task(session));
}

// Box this future to break the recursive type cycle between spawning a helper session loop and
// processing a later settings update from that loop.
fn restart_active_goal_supervisor_task(
    session: Arc<Session>,
) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
    Box::pin(async move {
        let Some(state_db) = session.services.state_db.as_ref() else {
            return;
        };
        let goal = match state_db
            .thread_goals()
            .get_thread_goal(session.thread_id)
            .await
        {
            Ok(Some(goal)) if goal.status == codex_state::ThreadGoalStatus::Active => goal,
            Ok(Some(_)) | Ok(None) => return,
            Err(err) => {
                warn!(
                    thread_id = %session.thread_id,
                    "failed to read active goal after execution settings changed: {err}"
                );
                return;
            }
        };
        let goal_id = goal.goal_id.clone();
        if let Err(err) = maybe_start_supervisor_checkin(
            &session,
            goal_id.as_str(),
            &protocol_goal_from_state(goal),
        )
        .await
        {
            warn!(
                thread_id = %session.thread_id,
                "failed to restart goal supervisor after execution settings changed: {err}"
            );
        }
    })
}

pub(crate) async fn maybe_start_supervisor_checkin(
    session: &Arc<Session>,
    goal_id: &str,
    goal: &ThreadGoal,
) -> anyhow::Result<()> {
    let _transition = Arc::clone(&session.goal_supervisor_runtime.transition_lock)
        .lock_owned()
        .await;
    maybe_start_supervisor_checkin_locked(session, goal_id, goal).await
}

async fn maybe_start_supervisor_checkin_locked(
    session: &Arc<Session>,
    goal_id: &str,
    goal: &ThreadGoal,
) -> anyhow::Result<()> {
    if session.active_turn.lock().await.is_some() || session.has_pending_turn_start_work().await {
        return Ok(());
    }

    let active_helper_id = *session
        .goal_supervisor_runtime
        .active_helper_id
        .lock()
        .await;
    if let Some(helper_id) = active_helper_id {
        let active_goal_id = session
            .goal_supervisor_runtime
            .active_goal_id
            .lock()
            .await
            .clone();
        let status = session.services.agent_control.get_status(helper_id).await;
        if matches!(status, AgentStatus::PendingInit | AgentStatus::Running)
            && active_goal_id.as_deref() == Some(goal_id)
        {
            return Ok(());
        }
        if active_goal_id.as_deref() == Some(goal_id) {
            let _ = defer_failed_supervisor_helper_locked(session, helper_id, status).await?;
            return Ok(());
        }
        finish_supervisor_helper_locked(session, helper_id).await?;
    }

    let now = Instant::now();
    let snoozed_until = session
        .goal_supervisor_runtime
        .snoozed_until
        .lock()
        .await
        .clone();
    if let Some(snoozed_until) = snoozed_until
        && snoozed_until.goal_id == goal_id
        && now < snoozed_until.deadline
    {
        schedule_supervisor_wakeup(session, snoozed_until).await;
        return Ok(());
    }
    if let Some(delay) = persisted_snooze_delay(session, goal_id).await? {
        let wake = SupervisorWakeDeadline {
            goal_id: goal_id.to_string(),
            deadline: Instant::now() + delay,
        };
        *session.goal_supervisor_runtime.snoozed_until.lock().await = Some(wake.clone());
        schedule_supervisor_wakeup(session, wake).await;
        return Ok(());
    }

    invalidate_supervisor_wakeup(session).await;
    *session.goal_supervisor_runtime.snoozed_until.lock().await = None;
    let helper_id = match spawn_supervisor_helper(session, goal).await {
        Ok(helper_id) => helper_id,
        Err(err) => {
            schedule_supervisor_failure_retry_locked(session, goal_id, &err.to_string()).await;
            return Err(err);
        }
    };
    *session
        .goal_supervisor_runtime
        .active_helper_id
        .lock()
        .await = Some(helper_id);
    *session.goal_supervisor_runtime.active_goal_id.lock().await = Some(goal_id.to_string());
    Ok(())
}

pub(crate) async fn maybe_start_supervisor_checkin_after_goal_resume(
    session: &Arc<Session>,
    goal_id: &str,
    goal: &ThreadGoal,
) -> anyhow::Result<()> {
    let _transition = Arc::clone(&session.goal_supervisor_runtime.transition_lock)
        .lock_owned()
        .await;
    let active_helper_id = *session
        .goal_supervisor_runtime
        .active_helper_id
        .lock()
        .await;
    let active_goal_id = session
        .goal_supervisor_runtime
        .active_goal_id
        .lock()
        .await
        .clone();
    if let Some(helper_id) = active_helper_id
        && active_goal_id.as_deref() == Some(goal_id)
    {
        let status = session.services.agent_control.get_status(helper_id).await;
        if !matches!(status, AgentStatus::PendingInit | AgentStatus::Running) {
            finish_supervisor_helper_locked(session, helper_id).await?;
        }
    }
    invalidate_supervisor_wakeup(session).await;
    *session.goal_supervisor_runtime.snoozed_until.lock().await = None;
    reset_failure_backoff(session).await;
    if let Some(state_db) = session.services.state_db.as_ref() {
        state_db
            .thread_goals()
            .set_thread_goal_supervisor_snoozed_until_ms(
                session.thread_id,
                goal_id,
                /*snoozed_until_ms*/ None,
            )
            .await?;
    }
    maybe_start_supervisor_checkin_locked(session, goal_id, goal).await
}

pub(crate) async fn defer_failed_supervisor_helper(
    session: &Arc<Session>,
    helper_thread_id: ThreadId,
    terminal_status: AgentStatus,
) -> anyhow::Result<bool> {
    let _transition = Arc::clone(&session.goal_supervisor_runtime.transition_lock)
        .lock_owned()
        .await;
    defer_failed_supervisor_helper_locked(session, helper_thread_id, terminal_status).await
}

async fn defer_failed_supervisor_helper_locked(
    session: &Arc<Session>,
    helper_thread_id: ThreadId,
    terminal_status: AgentStatus,
) -> anyhow::Result<bool> {
    let active_helper_id = *session
        .goal_supervisor_runtime
        .active_helper_id
        .lock()
        .await;
    if active_helper_id != Some(helper_thread_id) {
        return Ok(false);
    }
    let active_goal_id = session
        .goal_supervisor_runtime
        .active_goal_id
        .lock()
        .await
        .clone();
    let Some(active_goal_id) = active_goal_id else {
        return finish_supervisor_helper_locked(session, helper_thread_id).await;
    };

    let finish_result = finish_supervisor_helper_locked(session, helper_thread_id).await;
    schedule_supervisor_failure_retry_locked(
        session,
        active_goal_id.as_str(),
        &supervisor_terminal_status_description(&terminal_status),
    )
    .await;
    finish_result
}

// Both startup failures and terminal helpers use the same goal-scoped retry policy. In
// particular, a failed spawn has no helper whose completion could schedule a later check-in.
async fn schedule_supervisor_failure_retry_locked(
    session: &Arc<Session>,
    goal_id: &str,
    failure: &str,
) {
    let retry_active_goal = if let Some(state_db) = session.services.state_db.as_ref() {
        match state_db
            .thread_goals()
            .get_thread_goal(session.thread_id)
            .await
        {
            Ok(Some(goal)) => {
                goal.goal_id == goal_id && goal.status == codex_state::ThreadGoalStatus::Active
            }
            Ok(None) => false,
            Err(err) => {
                warn!(
                    thread_id = %session.thread_id,
                    goal_id,
                    "failed to verify active goal before scheduling supervisor retry: {err}"
                );
                true
            }
        }
    } else {
        true
    };
    if !retry_active_goal {
        invalidate_supervisor_wakeup(session).await;
        *session.goal_supervisor_runtime.snoozed_until.lock().await = None;
        reset_failure_backoff(session).await;
        return;
    }

    let retry = next_failure_retry(session, goal_id).await;
    let retry_delay_ms = i64::try_from(retry.delay.as_millis()).unwrap_or(i64::MAX);
    let persisted_deadline_ms = Utc::now().timestamp_millis().saturating_add(retry_delay_ms);
    if let Some(state_db) = session.services.state_db.as_ref()
        && let Err(err) = state_db
            .thread_goals()
            .set_thread_goal_supervisor_snoozed_until_ms(
                session.thread_id,
                goal_id,
                Some(persisted_deadline_ms),
            )
            .await
    {
        warn!(
            thread_id = %session.thread_id,
            goal_id,
            "failed to persist goal supervisor failure retry deadline: {err}"
        );
    }
    let wake = SupervisorWakeDeadline {
        goal_id: goal_id.to_string(),
        deadline: Instant::now() + retry.delay,
    };
    *session.goal_supervisor_runtime.snoozed_until.lock().await = Some(wake.clone());

    schedule_supervisor_wakeup(session, wake).await;
    if retry.should_warn {
        session
            .send_event_raw(Event {
                id: format!("goal-supervisor-retry-{}", ThreadId::new()),
                msg: EventMsg::Warning(WarningEvent {
                    message: format!(
                        "Goal supervisor check-in failed: {failure}. Retrying in {}.",
                        format_retry_duration(retry.delay),
                    ),
                }),
            })
            .await;
    }
}

pub(crate) async fn finish_supervisor_helper(
    session: &Arc<Session>,
    helper_thread_id: ThreadId,
) -> anyhow::Result<bool> {
    let _transition = Arc::clone(&session.goal_supervisor_runtime.transition_lock)
        .lock_owned()
        .await;
    finish_supervisor_helper_locked(session, helper_thread_id).await
}

pub(crate) async fn finish_supervisor_helper_after_followup(
    session: &Arc<Session>,
    helper_thread_id: ThreadId,
) -> anyhow::Result<bool> {
    let transition = Arc::clone(&session.goal_supervisor_runtime.transition_lock)
        .lock_owned()
        .await;
    let finished = finish_supervisor_helper_locked(session, helper_thread_id).await?;
    drop(transition);
    if finished {
        tokio::spawn(restart_active_goal_supervisor_task(Arc::clone(session)));
    }
    Ok(finished)
}

async fn finish_supervisor_helper_locked(
    session: &Arc<Session>,
    helper_thread_id: ThreadId,
) -> anyhow::Result<bool> {
    if *session
        .goal_supervisor_runtime
        .active_helper_id
        .lock()
        .await
        != Some(helper_thread_id)
    {
        return Ok(false);
    }
    session
        .services
        .agent_control
        .finish_internal_helper_thread(helper_thread_id)
        .await
        .map_err(anyhow::Error::msg)?;
    *session
        .goal_supervisor_runtime
        .active_helper_id
        .lock()
        .await = None;
    *session.goal_supervisor_runtime.active_goal_id.lock().await = None;
    Ok(true)
}

struct SupervisorFailureRetry {
    delay: Duration,
    should_warn: bool,
}

async fn next_failure_retry(session: &Session, goal_id: &str) -> SupervisorFailureRetry {
    let mut backoff = session.goal_supervisor_runtime.failure_backoff.lock().await;
    if backoff.goal_id.as_deref() != Some(goal_id) {
        *backoff = SupervisorFailureBackoff {
            goal_id: Some(goal_id.to_string()),
            ..SupervisorFailureBackoff::default()
        };
    }
    backoff.consecutive_failures = backoff.consecutive_failures.saturating_add(1);
    let (base_delay_seconds, delay) =
        supervisor_failure_retry_delay(session.thread_id, goal_id, backoff.consecutive_failures);
    let should_warn = backoff.last_warned_base_delay_seconds != Some(base_delay_seconds);
    if should_warn {
        backoff.last_warned_base_delay_seconds = Some(base_delay_seconds);
    }
    SupervisorFailureRetry { delay, should_warn }
}

async fn reset_failure_backoff(session: &Session) {
    *session.goal_supervisor_runtime.failure_backoff.lock().await =
        SupervisorFailureBackoff::default();
}

fn supervisor_failure_retry_delay(
    thread_id: ThreadId,
    goal_id: &str,
    consecutive_failures: u32,
) -> (u64, Duration) {
    let exponent = consecutive_failures.saturating_sub(1).min(63);
    let base_delay_seconds = INITIAL_SUPERVISOR_FAILURE_RETRY_SECONDS
        .saturating_mul(1_u64.checked_shl(exponent).unwrap_or(u64::MAX))
        .min(MAX_SUPERVISOR_FAILURE_RETRY_SECONDS);
    let jitter_span = base_delay_seconds / SUPERVISOR_FAILURE_JITTER_DIVISOR;
    let jitter = stable_failure_jitter(thread_id, goal_id, consecutive_failures)
        % jitter_span.saturating_add(1);
    (
        base_delay_seconds,
        Duration::from_secs(base_delay_seconds.saturating_sub(jitter)),
    )
}

fn stable_failure_jitter(thread_id: ThreadId, goal_id: &str, consecutive_failures: u32) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in thread_id
        .to_string()
        .bytes()
        .chain([0])
        .chain(goal_id.bytes())
        .chain([0])
        .chain(consecutive_failures.to_le_bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn supervisor_terminal_status_description(status: &AgentStatus) -> String {
    match status {
        AgentStatus::Completed(Some(message)) if !message.trim().is_empty() => {
            format!("completed without a terminal supervisor action: {message}")
        }
        AgentStatus::Completed(_) => "completed without a terminal supervisor action".to_string(),
        AgentStatus::Errored(message) => message.clone(),
        AgentStatus::Interrupted => "the check-in was interrupted".to_string(),
        AgentStatus::Shutdown => "the check-in shut down unexpectedly".to_string(),
        AgentStatus::NotFound => "the check-in disappeared".to_string(),
        AgentStatus::PendingInit | AgentStatus::Running => {
            format!("unexpected terminal status {status:?}")
        }
    }
}

fn format_retry_duration(delay: Duration) -> String {
    let total_seconds = delay.as_secs();
    let minutes = total_seconds / 60;
    let seconds = total_seconds % 60;
    match (minutes, seconds) {
        (0, seconds) => format!("{seconds}s"),
        (minutes, 0) => format!("{minutes}m"),
        (minutes, seconds) => format!("{minutes}m {seconds}s"),
    }
}

pub(crate) async fn has_running_supervisor_helper(session: &Arc<Session>) -> bool {
    let Some(helper_id) = *session
        .goal_supervisor_runtime
        .active_helper_id
        .lock()
        .await
    else {
        return false;
    };
    matches!(
        session.services.agent_control.get_status(helper_id).await,
        AgentStatus::PendingInit | AgentStatus::Running
    )
}

pub(crate) async fn snooze_supervisor_helper(
    session: &Arc<Session>,
    helper_thread_id: ThreadId,
    delay_seconds: u64,
    reason: Option<&str>,
) -> anyhow::Result<Option<u64>> {
    let _transition = Arc::clone(&session.goal_supervisor_runtime.transition_lock)
        .lock_owned()
        .await;
    let active_helper_id = *session
        .goal_supervisor_runtime
        .active_helper_id
        .lock()
        .await;
    if active_helper_id != Some(helper_thread_id) {
        return Ok(None);
    }
    let active_goal_id = session
        .goal_supervisor_runtime
        .active_goal_id
        .lock()
        .await
        .clone();
    let Some(active_goal_id) = active_goal_id else {
        let _ = finish_supervisor_helper_locked(session, helper_thread_id).await?;
        return Ok(None);
    };
    let delay_seconds = delay_seconds.max(MIN_SUPERVISOR_SNOOZE_SECONDS);
    if let Some(state_db) = session.services.state_db.as_ref()
        && let Some(goal) = state_db
            .thread_goals()
            .get_thread_goal(session.thread_id)
            .await?
    {
        if goal.goal_id != active_goal_id {
            let _ = finish_supervisor_helper_locked(session, helper_thread_id).await?;
            return Ok(None);
        }
        state_db
            .thread_goals()
            .set_thread_goal_supervisor_snoozed_until_ms(
                session.thread_id,
                &active_goal_id,
                Some(Utc::now().timestamp_millis() + (delay_seconds as i64 * 1000)),
            )
            .await?;
    }
    let wake = SupervisorWakeDeadline {
        goal_id: active_goal_id.clone(),
        deadline: Instant::now() + Duration::from_secs(delay_seconds),
    };
    *session.goal_supervisor_runtime.snoozed_until.lock().await = Some(wake.clone());
    let parent_path = session
        .session_source()
        .await
        .get_agent_path()
        .unwrap_or_else(AgentPath::root);
    let supervisor_path = parent_path
        .join(GOAL_SUPERVISOR_ROLE_NAME)
        .map_err(anyhow::Error::msg)?;
    let reason = reason
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
        .map(|reason| {
            reason
                .split_whitespace()
                .flat_map(|word| word.chars().chain(std::iter::once(' ')))
                .filter(|character| !character.is_control())
                .take(MAX_SUPERVISOR_SNOOZE_REASON_CHARS)
                .collect::<String>()
                .trim_end()
                .to_string()
        })
        .filter(|reason| !reason.is_empty());
    let content = match reason {
        Some(reason) => format!("Snooze {delay_seconds}s: {reason}"),
        None => format!("Snooze {delay_seconds}s"),
    };
    let communication = InterAgentCommunication::new(
        supervisor_path,
        parent_path,
        Vec::new(),
        content,
        /*trigger_turn*/ false,
    );
    let turn_context = session
        .new_turn_with_default_settings(
            format!("goal-supervisor-snooze-{helper_thread_id}"),
            Default::default(),
        )
        .await;
    let response_item: ResponseItem = communication.to_response_input_item().into();
    session
        .record_conversation_items(&turn_context, &[response_item])
        .await;
    record_snooze_action(session, &active_goal_id, delay_seconds).await;
    let _ = finish_supervisor_helper_locked(session, helper_thread_id).await?;
    schedule_supervisor_wakeup(session, wake).await;
    Ok(Some(delay_seconds))
}

pub(crate) async fn record_followup_action(
    session: &Arc<Session>,
    delivered_parent_message: &InterAgentCommunication,
) {
    let goal_id = session
        .goal_supervisor_runtime
        .active_goal_id
        .lock()
        .await
        .clone();
    record_action(
        session,
        SupervisorActionRecord {
            goal_id,
            kind: SupervisorActionKind::FollowupTask,
            sent_at: Utc::now(),
            delivered_parent_message: Some(delivered_parent_message.clone()),
            snoozed_seconds: None,
        },
    )
    .await;
}

pub(crate) async fn record_compact_parent_context_action(session: &Arc<Session>) {
    let goal_id = session
        .goal_supervisor_runtime
        .active_goal_id
        .lock()
        .await
        .clone();
    record_action(
        session,
        SupervisorActionRecord {
            goal_id,
            kind: SupervisorActionKind::CompactParentContext,
            sent_at: Utc::now(),
            delivered_parent_message: None,
            snoozed_seconds: None,
        },
    )
    .await;
}

async fn record_snooze_action(session: &Arc<Session>, goal_id: &str, snoozed_seconds: u64) {
    let sent_at = Utc::now();
    {
        let mut snooze_records = session.goal_supervisor_runtime.snooze_records.lock().await;
        snooze_records.retain(|record| record.goal_id == goal_id);
        snooze_records.push(SupervisorSnoozeRecord {
            goal_id: goal_id.to_string(),
            sent_at,
            snoozed_seconds,
        });
    }
    record_action(
        session,
        SupervisorActionRecord {
            goal_id: Some(goal_id.to_string()),
            kind: SupervisorActionKind::Snooze,
            sent_at,
            delivered_parent_message: None,
            snoozed_seconds: Some(snoozed_seconds),
        },
    )
    .await;
}

async fn record_action(session: &Arc<Session>, action: SupervisorActionRecord) {
    reset_failure_backoff(session).await;
    *session.goal_supervisor_runtime.last_action.lock().await = Some(action);
}

pub(crate) async fn complete_supervised_goal(
    session: &Arc<Session>,
    helper_thread_id: ThreadId,
) -> anyhow::Result<Option<ThreadGoal>> {
    let _transition = Arc::clone(&session.goal_supervisor_runtime.transition_lock)
        .lock_owned()
        .await;
    let active_helper_id = *session
        .goal_supervisor_runtime
        .active_helper_id
        .lock()
        .await;
    if active_helper_id != Some(helper_thread_id) {
        return Ok(None);
    }
    let active_goal_id = session
        .goal_supervisor_runtime
        .active_goal_id
        .lock()
        .await
        .clone();
    let Some(active_goal_id) = active_goal_id else {
        let _ = finish_supervisor_helper_locked(session, helper_thread_id).await?;
        return Ok(None);
    };
    let Some(state_db) = session.services.state_db.as_ref() else {
        return Ok(None);
    };
    let updated = state_db
        .thread_goals()
        .update_thread_goal(
            session.thread_id,
            codex_state::GoalUpdate {
                objective: None,
                status: Some(codex_state::ThreadGoalStatus::Complete),
                token_budget: None,
                expected_goal_id: Some(active_goal_id.clone()),
            },
        )
        .await?
        .map(protocol_goal_from_state);
    if let Some(goal) = updated.as_ref() {
        invalidate_supervisor_wakeup(session).await;
        *session.goal_supervisor_runtime.snoozed_until.lock().await = None;
        reset_failure_backoff(session).await;
        state_db
            .thread_goals()
            .set_thread_goal_supervisor_snoozed_until_ms(
                session.thread_id,
                &active_goal_id,
                /*snoozed_until_ms*/ None,
            )
            .await?;
        session
            .send_event_raw(Event {
                id: format!("goal-supervisor-complete-{}", session.thread_id),
                msg: EventMsg::ThreadGoalUpdated(ThreadGoalUpdatedEvent {
                    thread_id: session.thread_id,
                    turn_id: None,
                    goal: goal.clone(),
                }),
            })
            .await;
    }
    let _ = finish_supervisor_helper_locked(session, helper_thread_id).await?;
    Ok(updated)
}

async fn spawn_supervisor_helper(session: &Session, goal: &ThreadGoal) -> anyhow::Result<ThreadId> {
    let mut helper_config = session.effective_session_config().await;
    helper_config.ephemeral = true;
    let parent_source = session
        .services
        .agent_control
        .get_agent_config_snapshot(session.thread_id)
        .await
        .map(|snapshot| snapshot.session_source)
        .unwrap_or(SessionSource::Cli);
    let depth = next_thread_spawn_depth(&parent_source);
    let supervisor_path = parent_source
        .get_agent_path()
        .unwrap_or_else(AgentPath::root)
        .join("goal_supervisor")
        .map_err(anyhow::Error::msg)?;
    if let Some(helper_thread_id) = session
        .services
        .agent_control
        .reconcile_goal_supervisor_state(session.thread_id, &supervisor_path)
        .await
        .map_err(anyhow::Error::msg)?
    {
        return Ok(helper_thread_id);
    }
    let session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: session.thread_id,
        depth,
        agent_path: Some(supervisor_path),
        agent_nickname: None,
        agent_role: Some(GOAL_SUPERVISOR_ROLE_NAME.to_string()),
    });
    let prompt = supervisor_helper_prompt(session, goal);
    let helper = session
        .services
        .agent_control
        .spawn_agent_with_metadata(
            helper_config,
            vec![UserInput::Text {
                text: prompt,
                text_elements: Vec::new(),
            }],
            Some(session_source),
            SpawnAgentOptions {
                fork_parent_spawn_call_id: None,
                fork_mode: Some(SpawnAgentForkMode::FullHistory),
                parent_thread_id: Some(session.thread_id),
                parent_turn_id: None,
                root_turn_id: None,
                environments: None,
                multi_agent_v2_usage_hints: None,
                cyber_access_program: None,
                initial_task_message: None,
            },
        )
        .await
        .map_err(anyhow::Error::msg)?;
    Ok(helper.thread_id)
}

#[cfg(test)]
pub(crate) async fn spawn_supervisor_helper_for_test(
    session: &Session,
    goal: &ThreadGoal,
) -> anyhow::Result<ThreadId> {
    spawn_supervisor_helper(session, goal).await
}

fn supervisor_helper_prompt(session: &Session, goal: &ThreadGoal) -> String {
    format!(
        "# Goal Supervisor Assignment\n\nParent agent id: {}\n\nActive goal objective:\n\n{}\n\nEvaluate whether the parent should continue now, snooze, compact, or mark the goal complete.",
        session.thread_id, goal.objective
    )
}

pub(crate) async fn supervisor_continuity_context_item(
    session: &Arc<Session>,
    goal_id: &str,
    goal: &ThreadGoal,
    source_items: &[RolloutItem],
) -> RolloutItem {
    let previous_supervisor_action = session
        .goal_supervisor_runtime
        .last_action
        .lock()
        .await
        .clone()
        .filter(|action| action.goal_id.as_deref() == Some(goal_id));
    let last_parent_message_at = source_items.iter().rev().find_map(|item| match item {
        RolloutItem::EventMsg(EventMsg::TurnComplete(event)) => event.completed_at,
        _ => None,
    });
    let snooze_records = session
        .goal_supervisor_runtime
        .snooze_records
        .lock()
        .await
        .clone();
    let snooze_records = snooze_records
        .into_iter()
        .filter(|record| record.goal_id == goal_id)
        .collect::<Vec<_>>();
    let (snooze_count_since_goal_created, snoozed_seconds_since_goal_created) = snooze_records
        .iter()
        .fold((0_u64, 0_u64), |(count, seconds), record| {
            (
                count.saturating_add(1),
                seconds.saturating_add(record.snoozed_seconds),
            )
        });
    let snooze_records = snooze_records.into_iter().filter(|record| {
        last_parent_message_at.is_none_or(|completed_at| record.sent_at.timestamp() >= completed_at)
    });
    let (snooze_count_since_last_parent_message, snoozed_seconds_since_last_parent_message) =
        snooze_records.fold((0_u64, 0_u64), |(count, seconds), record| {
            (
                count.saturating_add(1),
                seconds.saturating_add(record.snoozed_seconds),
            )
        });
    let continuity = serde_json::json!({
        "supervisor_identity": "/root/goal_supervisor",
        "activation_reason": "thread_idle",
        "previous_supervisor_action": previous_supervisor_action.as_ref().map(|action| serde_json::json!({
            "kind": action.kind,
            "sent_at_utc": action.sent_at.to_rfc3339(),
            "delivered_parent_message": action.delivered_parent_message,
            "snoozed_seconds": action.snoozed_seconds,
        })),
        "goal_timing": {
            "goal_created_at_utc": chrono::DateTime::<Utc>::from_timestamp(goal.created_at, 0).map(|created_at| created_at.to_rfc3339()),
            "seconds_since_goal_created": Utc::now().timestamp().saturating_sub(goal.created_at),
            "snooze_count_since_goal_created": snooze_count_since_goal_created,
            "snoozed_seconds_since_goal_created": snoozed_seconds_since_goal_created,
        },
        "parent_timing": {
            "last_parent_message_at_utc": last_parent_message_at.and_then(|completed_at| chrono::DateTime::<Utc>::from_timestamp(completed_at, 0)).map(|completed_at| completed_at.to_rfc3339()),
            "snooze_count_since_last_parent_message": snooze_count_since_last_parent_message,
            "snoozed_seconds_since_last_parent_message": snoozed_seconds_since_last_parent_message,
        },
    });
    let continuity = match serde_json::to_string_pretty(&continuity) {
        Ok(continuity) => continuity,
        Err(err) => format!("failed to serialize goal supervisor continuity: {err}"),
    };
    RolloutItem::ResponseItem(
        ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: format!("# Goal Supervisor Continuity\n\n{continuity}"),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        }
        .into(),
    )
}

async fn persisted_snooze_delay(
    session: &Arc<Session>,
    goal_id: &str,
) -> anyhow::Result<Option<Duration>> {
    let Some(state_db) = session.services.state_db.as_ref() else {
        return Ok(None);
    };
    let Some(snoozed_until_ms) = state_db
        .thread_goals()
        .get_thread_goal_supervisor_snoozed_until_ms(session.thread_id, goal_id)
        .await?
    else {
        return Ok(None);
    };
    let now_ms = Utc::now().timestamp_millis();
    if snoozed_until_ms <= now_ms {
        state_db
            .thread_goals()
            .set_thread_goal_supervisor_snoozed_until_ms(
                session.thread_id,
                goal_id,
                /*snoozed_until_ms*/ None,
            )
            .await?;
        return Ok(None);
    }
    Ok(Some(Duration::from_millis(
        (snoozed_until_ms - now_ms) as u64,
    )))
}

async fn invalidate_supervisor_wakeup(session: &Session) {
    session
        .goal_supervisor_runtime
        .wakeup_generation
        .fetch_add(1, Ordering::SeqCst);
    if let Some(scheduled) = session
        .goal_supervisor_runtime
        .scheduled_wakeup
        .lock()
        .await
        .take()
    {
        scheduled.abort_handle.abort();
    }
}

async fn schedule_supervisor_wakeup(session: &Arc<Session>, wake: SupervisorWakeDeadline) {
    let mut scheduled = session
        .goal_supervisor_runtime
        .scheduled_wakeup
        .lock()
        .await;
    if scheduled.as_ref().is_some_and(|scheduled| {
        scheduled.wake.goal_id == wake.goal_id && scheduled.wake.deadline == wake.deadline
    }) {
        return;
    }
    if let Some(scheduled) = scheduled.take() {
        scheduled.abort_handle.abort();
    }
    let generation = session
        .goal_supervisor_runtime
        .wakeup_generation
        .fetch_add(1, Ordering::SeqCst)
        .wrapping_add(1);

    let session = Arc::clone(session);
    let task_wake = wake.clone();
    let scheduled_task = tokio::spawn(async move {
        let delay = task_wake.deadline.saturating_duration_since(Instant::now());
        if let Err(err) = session
            .services
            .time_provider
            .sleep(session.thread_id, delay)
            .await
        {
            warn!(
                thread_id = %session.thread_id,
                "failed to wait for goal supervisor wakeup: {err}"
            );
            clear_scheduled_supervisor_wakeup(&session, generation).await;
            return;
        }
        fire_scheduled_supervisor_wakeup(&session, generation).await;
    });
    *scheduled = Some(ScheduledSupervisorWakeup {
        wake,
        generation,
        abort_handle: scheduled_task.abort_handle(),
    });
}

async fn fire_scheduled_supervisor_wakeup(session: &Arc<Session>, generation: u64) {
    if session
        .goal_supervisor_runtime
        .wakeup_generation
        .compare_exchange(
            generation,
            generation.wrapping_add(1),
            Ordering::SeqCst,
            Ordering::SeqCst,
        )
        .is_err()
    {
        return;
    }
    if !clear_scheduled_supervisor_wakeup(session, generation).await {
        return;
    }
    session
        .emit_thread_idle_lifecycle_if_idle(ThreadIdleCause::Completed)
        .await;
}

async fn clear_scheduled_supervisor_wakeup(session: &Session, generation: u64) -> bool {
    let mut scheduled = session
        .goal_supervisor_runtime
        .scheduled_wakeup
        .lock()
        .await;
    if scheduled
        .as_ref()
        .is_none_or(|scheduled| scheduled.generation != generation)
    {
        return false;
    }
    *scheduled = None;
    true
}

#[cfg(test)]
pub(crate) async fn scheduled_supervisor_wakeup_generation_for_test(
    session: &Session,
) -> Option<u64> {
    session
        .goal_supervisor_runtime
        .scheduled_wakeup
        .lock()
        .await
        .as_ref()
        .map(|scheduled| scheduled.generation)
}

#[cfg(test)]
pub(crate) async fn hold_supervisor_transition_for_test(
    session: &Session,
) -> tokio::sync::OwnedMutexGuard<()> {
    Arc::clone(&session.goal_supervisor_runtime.transition_lock)
        .lock_owned()
        .await
}

#[cfg(test)]
pub(crate) async fn supervisor_failure_count_for_test(session: &Session) -> u32 {
    session
        .goal_supervisor_runtime
        .failure_backoff
        .lock()
        .await
        .consecutive_failures
}

#[cfg(test)]
pub(crate) async fn fire_scheduled_supervisor_wakeup_for_test(session: &Arc<Session>) {
    let generation = scheduled_supervisor_wakeup_generation_for_test(session).await;
    if let Some(generation) = generation {
        if let Some(deadline) = session
            .goal_supervisor_runtime
            .snoozed_until
            .lock()
            .await
            .as_mut()
        {
            deadline.deadline = Instant::now();
        }
        fire_scheduled_supervisor_wakeup(session, generation).await;
    }
}

#[cfg(test)]
pub(crate) fn supervisor_wakeup_generation_for_test(session: &Session) -> u64 {
    session
        .goal_supervisor_runtime
        .wakeup_generation
        .load(Ordering::SeqCst)
}

#[cfg(test)]
pub(crate) async fn fire_supervisor_wakeup_generation_for_test(
    session: &Arc<Session>,
    generation: u64,
) {
    fire_scheduled_supervisor_wakeup(session, generation).await;
}

pub(crate) fn protocol_goal_from_state(goal: codex_state::ThreadGoal) -> ThreadGoal {
    ThreadGoal {
        thread_id: goal.thread_id,
        objective: goal.objective,
        status: protocol_status_from_state(goal.status),
        token_budget: goal.token_budget,
        tokens_used: goal.tokens_used,
        time_used_seconds: goal.time_used_seconds,
        created_at: goal.created_at.timestamp(),
        updated_at: goal.updated_at.timestamp(),
    }
}

fn protocol_status_from_state(status: codex_state::ThreadGoalStatus) -> ThreadGoalStatus {
    match status {
        codex_state::ThreadGoalStatus::Active => ThreadGoalStatus::Active,
        codex_state::ThreadGoalStatus::Paused => ThreadGoalStatus::Paused,
        codex_state::ThreadGoalStatus::Blocked => ThreadGoalStatus::Blocked,
        codex_state::ThreadGoalStatus::UsageLimited => ThreadGoalStatus::UsageLimited,
        codex_state::ThreadGoalStatus::BudgetLimited => ThreadGoalStatus::BudgetLimited,
        codex_state::ThreadGoalStatus::Complete => ThreadGoalStatus::Complete,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_retry_uses_deterministic_exponential_tiers_with_a_cap() {
        let thread_id = ThreadId::new();
        let expected_base_seconds = [60, 120, 240, 480, 960, 1_920, 3_600, 3_600];

        for (index, expected_base_seconds) in expected_base_seconds.into_iter().enumerate() {
            let consecutive_failures = u32::try_from(index + 1).expect("small test index");
            let (base_seconds, delay) =
                supervisor_failure_retry_delay(thread_id, "goal-1", consecutive_failures);
            let (_, repeated_delay) =
                supervisor_failure_retry_delay(thread_id, "goal-1", consecutive_failures);

            assert_eq!(base_seconds, expected_base_seconds);
            assert_eq!(delay, repeated_delay, "jitter should be deterministic");
            assert!(
                delay.as_secs()
                    >= expected_base_seconds
                        - expected_base_seconds / SUPERVISOR_FAILURE_JITTER_DIVISOR
            );
            assert!(delay.as_secs() <= expected_base_seconds);
            assert!(delay <= Duration::from_secs(MAX_SUPERVISOR_FAILURE_RETRY_SECONDS));
        }
    }
}
