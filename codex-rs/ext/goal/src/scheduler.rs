use std::collections::HashMap;
use std::fs::File;
use std::fs::TryLockError;
use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadSource;
use codex_state::ActiveGoalSupervisorSchedule;
use codex_state::ListActiveGoalSupervisorSchedulesParams;
use codex_state::StateRuntime;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tracing::debug;
use tracing::warn;

const DEFAULT_SCAN_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_MAX_RETRY_INTERVAL: Duration = Duration::from_secs(5 * 60);
const DEFAULT_PAGE_SIZE: usize = 256;
const DEFAULT_MAX_CONCURRENT_ACTIVATIONS: usize = 4;

pub type GoalActivationFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>>;
pub type GoalActivator =
    Arc<dyn Fn(ActiveGoalSupervisorSchedule) -> GoalActivationFuture + Send + Sync + 'static>;

/// Runtime limits for the process-level active-goal scheduler.
#[derive(Clone, Debug)]
pub struct GoalSchedulerOptions {
    pub scan_interval: Duration,
    pub retry_interval: Duration,
    pub max_retry_interval: Duration,
    pub page_size: usize,
    pub max_concurrent_activations: usize,
}

impl Default for GoalSchedulerOptions {
    fn default() -> Self {
        Self {
            scan_interval: DEFAULT_SCAN_INTERVAL,
            retry_interval: DEFAULT_RETRY_INTERVAL,
            max_retry_interval: DEFAULT_MAX_RETRY_INTERVAL,
            page_size: DEFAULT_PAGE_SIZE,
            max_concurrent_activations: DEFAULT_MAX_CONCURRENT_ACTIVATIONS,
        }
    }
}

/// Owns the background task that restores persisted goal supervisor deadlines.
pub struct GoalSchedulerHandle {
    task: JoinHandle<()>,
}

impl GoalSchedulerHandle {
    pub fn start(state_db: Arc<StateRuntime>, activator: GoalActivator) -> Self {
        Self::start_with_options(state_db, activator, GoalSchedulerOptions::default())
    }

    pub fn start_with_options(
        state_db: Arc<StateRuntime>,
        activator: GoalActivator,
        options: GoalSchedulerOptions,
    ) -> Self {
        let task = tokio::spawn(run_scheduler(state_db, activator, options));
        Self { task }
    }

    pub fn stop(&self) {
        self.task.abort();
    }
}

impl Drop for GoalSchedulerHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct ScheduledActivation {
    schedule: ActiveGoalSupervisorSchedule,
    task: JoinHandle<()>,
}

async fn run_scheduler(
    state_db: Arc<StateRuntime>,
    activator: GoalActivator,
    options: GoalSchedulerOptions,
) {
    loop {
        match try_acquire_scheduler_ownership(state_db.sqlite().home()) {
            Ok(Some(_ownership)) => {
                debug!(
                    sqlite_home = %state_db.sqlite().home().display(),
                    "acquired active goal scheduler ownership"
                );
                run_as_owner(state_db, activator, options).await;
                return;
            }
            Ok(None) => {}
            Err(err) => {
                warn!(
                    sqlite_home = %state_db.sqlite().home().display(),
                    "failed to acquire active goal scheduler ownership: {err}"
                );
            }
        }
        tokio::time::sleep(options.scan_interval).await;
    }
}

fn try_acquire_scheduler_ownership(sqlite_home: &Path) -> io::Result<Option<File>> {
    // Lock the existing SQLite home directory rather than a SQLite file. This keeps scheduler
    // ownership independent from SQLite's byte-range locks and adds no persistent lock file.
    let file = File::open(sqlite_home)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(TryLockError::WouldBlock) => Ok(None),
        Err(TryLockError::Error(err)) => Err(err),
    }
}

async fn run_as_owner(
    state_db: Arc<StateRuntime>,
    activator: GoalActivator,
    options: GoalSchedulerOptions,
) {
    let activation_permits = Arc::new(Semaphore::new(options.max_concurrent_activations.max(1)));
    let mut scheduled = HashMap::<ThreadId, ScheduledActivation>::new();

    loop {
        match scan_active_schedules(state_db.as_ref(), options.page_size).await {
            Ok(current_schedules) => {
                scheduled.retain(|thread_id, activation| {
                    let unchanged = current_schedules
                        .get(thread_id)
                        .is_some_and(|current| current == &activation.schedule);
                    if !unchanged {
                        activation.task.abort();
                    }
                    unchanged
                });

                for (thread_id, schedule) in current_schedules {
                    if scheduled.contains_key(&thread_id) {
                        continue;
                    }
                    let task = tokio::spawn(run_scheduled_activation(
                        Arc::clone(&state_db),
                        Arc::clone(&activator),
                        Arc::clone(&activation_permits),
                        schedule.clone(),
                        options.clone(),
                    ));
                    scheduled.insert(thread_id, ScheduledActivation { schedule, task });
                }
            }
            Err(err) => warn!("failed to scan active goal supervisor schedules: {err}"),
        }

        tokio::time::sleep(options.scan_interval).await;
    }
}

async fn scan_active_schedules(
    state_db: &StateRuntime,
    page_size: usize,
) -> Result<HashMap<ThreadId, ActiveGoalSupervisorSchedule>, String> {
    let mut schedules = HashMap::new();
    let mut cursor = None;
    loop {
        let page = state_db
            .thread_goals()
            .list_active_goal_supervisor_schedules(ListActiveGoalSupervisorSchedulesParams {
                after_thread_id: cursor,
                limit: page_size,
            })
            .await
            .map_err(|err| err.to_string())?;
        for schedule in page.data {
            if !is_subagent_thread(state_db, schedule.thread_id).await? {
                schedules.insert(schedule.thread_id, schedule);
            }
        }
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        if cursor == Some(next_cursor) {
            warn!(thread_id = %next_cursor, "active goal scheduler scan cursor did not advance");
            break;
        }
        cursor = Some(next_cursor);
    }
    Ok(schedules)
}

async fn is_subagent_thread(state_db: &StateRuntime, thread_id: ThreadId) -> Result<bool, String> {
    let Some(metadata) = state_db
        .get_thread(thread_id)
        .await
        .map_err(|err| err.to_string())?
    else {
        return Ok(false);
    };

    if matches!(metadata.thread_source, Some(ThreadSource::Subagent)) {
        return Ok(true);
    }

    let source = serde_json::from_str::<SessionSource>(&metadata.source)
        .or_else(|_| serde_json::from_value(serde_json::Value::String(metadata.source)));
    Ok(matches!(source, Ok(SessionSource::SubAgent(_))))
}

async fn run_scheduled_activation(
    state_db: Arc<StateRuntime>,
    activator: GoalActivator,
    activation_permits: Arc<Semaphore>,
    schedule: ActiveGoalSupervisorSchedule,
    options: GoalSchedulerOptions,
) {
    wait_until_deadline(&schedule, options.scan_interval).await;
    let mut retry_interval = options.retry_interval;
    loop {
        let current = match state_db
            .thread_goals()
            .get_active_goal_supervisor_schedule(schedule.thread_id)
            .await
        {
            Ok(current) => current,
            Err(err) => {
                warn!(
                    thread_id = %schedule.thread_id,
                    "failed to verify active goal supervisor schedule: {err}"
                );
                tokio::time::sleep(retry_interval).await;
                retry_interval = next_retry_interval(retry_interval, options.max_retry_interval);
                continue;
            }
        };
        if current.as_ref() != Some(&schedule) {
            return;
        }

        match is_subagent_thread(state_db.as_ref(), schedule.thread_id).await {
            Ok(true) => return,
            Ok(false) => {}
            Err(err) => {
                warn!(
                    thread_id = %schedule.thread_id,
                    "failed to verify persisted goal thread source: {err}"
                );
                tokio::time::sleep(retry_interval).await;
                retry_interval = next_retry_interval(retry_interval, options.max_retry_interval);
                continue;
            }
        }

        let permit = match Arc::clone(&activation_permits).acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return,
        };
        let result = activator(schedule.clone()).await;
        drop(permit);
        if let Err(err) = result {
            warn!(
                thread_id = %schedule.thread_id,
                goal_id = %schedule.goal_id,
                "failed to activate persisted goal supervisor schedule: {err}"
            );
        }
        // Activation can be a no-op while the thread is temporarily busy. Keep retrying until the
        // supervisor records a new snooze deadline or changes the goal status.
        tokio::time::sleep(retry_interval).await;
        retry_interval = next_retry_interval(retry_interval, options.max_retry_interval);
    }
}

async fn wait_until_deadline(schedule: &ActiveGoalSupervisorSchedule, max_sleep: Duration) {
    let Some(deadline_ms) = schedule.snoozed_until_ms else {
        return;
    };
    loop {
        let remaining_ms = deadline_ms.saturating_sub(Utc::now().timestamp_millis());
        let Ok(remaining_ms) = u64::try_from(remaining_ms) else {
            return;
        };
        let remaining = Duration::from_millis(remaining_ms);
        if remaining.is_zero() {
            return;
        }
        tokio::time::sleep(remaining.min(max_sleep)).await;
    }
}

fn next_retry_interval(current: Duration, maximum: Duration) -> Duration {
    current.saturating_mul(2).min(maximum)
}
