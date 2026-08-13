use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use codex_goal_extension::GoalActivator;
use codex_goal_extension::GoalSchedulerHandle;
use codex_goal_extension::GoalSchedulerOptions;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_state::ActiveGoalSupervisorSchedule;
use codex_state::SqliteConfig;
use codex_state::StateRuntime;
use codex_state::ThreadGoalStatus;
use codex_utils_absolute_path::test_support::PathExt;
use tempfile::TempDir;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;

fn scheduler_options() -> GoalSchedulerOptions {
    GoalSchedulerOptions {
        scan_interval: Duration::from_millis(10),
        retry_interval: Duration::from_millis(10),
        max_retry_interval: Duration::from_millis(40),
        page_size: 1,
        max_concurrent_activations: 2,
    }
}

async fn test_runtime() -> anyhow::Result<(TempDir, Arc<StateRuntime>)> {
    let sqlite_home = tempfile::tempdir()?;
    let state_db = StateRuntime::init(
        SqliteConfig::new_for_testing(sqlite_home.path().abs()),
        "test-provider".to_string(),
    )
    .await?;
    Ok((sqlite_home, state_db))
}

fn test_thread_id(suffix: u128) -> anyhow::Result<ThreadId> {
    Ok(ThreadId::from_string(
        format!("00000000-0000-0000-0000-{suffix:012x}").as_str(),
    )?)
}

fn recording_activator(tx: mpsc::UnboundedSender<ActiveGoalSupervisorSchedule>) -> GoalActivator {
    Arc::new(move |schedule| {
        let tx = tx.clone();
        Box::pin(async move {
            tx.send(schedule)
                .map_err(|_| "activation receiver closed".to_string())
        })
    })
}

fn gated_recording_activator(
    tx: mpsc::UnboundedSender<ActiveGoalSupervisorSchedule>,
    completion_gates: Arc<Semaphore>,
) -> GoalActivator {
    Arc::new(move |schedule| {
        let tx = tx.clone();
        let completion_gates = Arc::clone(&completion_gates);
        Box::pin(async move {
            tx.send(schedule)
                .map_err(|_| "activation receiver closed".to_string())?;
            let permit = completion_gates
                .acquire_owned()
                .await
                .map_err(|_| "activation completion gate closed".to_string())?;
            permit.forget();
            Ok(())
        })
    })
}

fn recording_terminal_activator(
    state_db: Arc<StateRuntime>,
    tx: mpsc::UnboundedSender<ActiveGoalSupervisorSchedule>,
) -> GoalActivator {
    Arc::new(move |schedule| {
        let state_db = Arc::clone(&state_db);
        let tx = tx.clone();
        Box::pin(async move {
            tx.send(schedule.clone())
                .map_err(|_| "activation receiver closed".to_string())?;
            state_db
                .thread_goals()
                .update_thread_goal(
                    schedule.thread_id,
                    codex_state::GoalUpdate {
                        objective: None,
                        status: Some(ThreadGoalStatus::Paused),
                        token_budget: None,
                        expected_goal_id: Some(schedule.goal_id),
                    },
                )
                .await
                .map_err(|err| err.to_string())?;
            Ok(())
        })
    })
}

#[tokio::test]
async fn scheduler_runs_immediate_and_future_deadlines_once() -> anyhow::Result<()> {
    let (_codex_home, state_db) = test_runtime().await?;
    let immediate_thread_id = test_thread_id(/*suffix*/ 1)?;
    let future_thread_id = test_thread_id(/*suffix*/ 2)?;
    state_db
        .thread_goals()
        .replace_thread_goal(
            immediate_thread_id,
            "run now",
            ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await
        .expect("immediate goal should persist");
    let future_goal = state_db
        .thread_goals()
        .replace_thread_goal(
            future_thread_id,
            "run later",
            ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await
        .expect("future goal should persist");
    state_db
        .thread_goals()
        .set_thread_goal_supervisor_snoozed_until_ms(
            future_thread_id,
            &future_goal.goal_id,
            Some(Utc::now().timestamp_millis() + 150),
        )
        .await
        .expect("future deadline should persist");

    let (tx, mut rx) = mpsc::unbounded_channel();
    let scheduler = GoalSchedulerHandle::start_with_options(
        Arc::clone(&state_db),
        recording_terminal_activator(Arc::clone(&state_db), tx),
        scheduler_options(),
    );

    let immediate = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("immediate goal should activate promptly")
        .expect("activation channel should remain open");
    assert_eq!(immediate_thread_id, immediate.thread_id);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), rx.recv())
            .await
            .is_err(),
        "future deadline must not activate early"
    );
    let future = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("future goal should activate")
        .expect("activation channel should remain open");
    assert_eq!(future_thread_id, future.thread_id);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), rx.recv())
            .await
            .is_err(),
        "changed schedules must not activate twice"
    );
    scheduler.stop();
    Ok(())
}

#[tokio::test]
async fn scheduler_ignores_subagent_goals_with_legacy_thread_metadata() -> anyhow::Result<()> {
    let (_codex_home, state_db) = test_runtime().await?;
    let root_thread_id = test_thread_id(/*suffix*/ 7)?;
    let review_thread_id = test_thread_id(/*suffix*/ 8)?;
    let spawned_thread_id = test_thread_id(/*suffix*/ 9)?;

    for (thread_id, source) in [
        (root_thread_id, SessionSource::Cli),
        (
            review_thread_id,
            SessionSource::SubAgent(SubAgentSource::Review),
        ),
        (
            spawned_thread_id,
            SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id: root_thread_id,
                depth: 1,
                agent_path: None,
                agent_nickname: None,
                agent_role: None,
            }),
        ),
    ] {
        let metadata = codex_state::ThreadMetadataBuilder::new(
            thread_id,
            state_db
                .sqlite()
                .home()
                .join(format!("rollout-{thread_id}.jsonl")),
            Utc::now(),
            source,
        )
        .build("test-provider");
        assert!(metadata.thread_source.is_none());
        state_db.upsert_thread(&metadata).await?;
        state_db
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "persisted active goal",
                ThreadGoalStatus::Active,
                /*token_budget*/ None,
            )
            .await?;
    }

    let (tx, mut rx) = mpsc::unbounded_channel();
    let scheduler = GoalSchedulerHandle::start_with_options(
        Arc::clone(&state_db),
        recording_terminal_activator(Arc::clone(&state_db), tx),
        scheduler_options(),
    );

    let activation = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("the root goal should activate")
        .expect("activation channel should remain open");
    assert_eq!(root_thread_id, activation.thread_id);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "subagent goals must not activate"
    );

    scheduler.stop();
    Ok(())
}

#[tokio::test]
async fn scheduler_rechecks_goal_before_deadline() -> anyhow::Result<()> {
    let (_codex_home, state_db) = test_runtime().await?;
    let thread_id = test_thread_id(/*suffix*/ 3)?;
    let goal = state_db
        .thread_goals()
        .replace_thread_goal(
            thread_id,
            "pause before wakeup",
            ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await
        .expect("goal should persist");
    state_db
        .thread_goals()
        .set_thread_goal_supervisor_snoozed_until_ms(
            thread_id,
            &goal.goal_id,
            Some(Utc::now().timestamp_millis() + 100),
        )
        .await
        .expect("deadline should persist");

    let (tx, mut rx) = mpsc::unbounded_channel();
    let scheduler = GoalSchedulerHandle::start_with_options(
        Arc::clone(&state_db),
        recording_activator(tx),
        scheduler_options(),
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    state_db
        .thread_goals()
        .update_thread_goal(
            thread_id,
            codex_state::GoalUpdate {
                objective: None,
                status: Some(ThreadGoalStatus::Paused),
                token_budget: None,
                expected_goal_id: Some(goal.goal_id),
            },
        )
        .await
        .expect("goal pause should succeed")
        .expect("goal should still exist");

    assert!(
        tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .is_err(),
        "paused goal must not activate"
    );
    scheduler.stop();
    Ok(())
}

#[tokio::test]
async fn one_process_owns_each_goal_database_scheduler() -> anyhow::Result<()> {
    let (sqlite_home, state_db) = test_runtime().await?;
    let second_state_db = StateRuntime::init(
        SqliteConfig::new_for_testing(sqlite_home.path().abs()),
        "test-provider".to_string(),
    )
    .await?;
    let thread_id = test_thread_id(/*suffix*/ 4)?;
    state_db
        .thread_goals()
        .replace_thread_goal(
            thread_id,
            "activate once",
            ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await
        .expect("goal should persist");

    let (tx, mut rx) = mpsc::unbounded_channel();
    let first = GoalSchedulerHandle::start_with_options(
        Arc::clone(&state_db),
        recording_terminal_activator(Arc::clone(&state_db), tx.clone()),
        scheduler_options(),
    );
    let second = GoalSchedulerHandle::start_with_options(
        Arc::clone(&second_state_db),
        recording_terminal_activator(second_state_db, tx),
        scheduler_options(),
    );

    let activation = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("one scheduler should activate the goal")
        .expect("activation channel should remain open");
    assert_eq!(thread_id, activation.thread_id);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "the second process contender must not duplicate activation"
    );
    first.stop();
    second.stop();
    Ok(())
}

#[tokio::test]
async fn scheduler_retries_until_the_schedule_changes() -> anyhow::Result<()> {
    let (_codex_home, state_db) = test_runtime().await?;
    let thread_id = test_thread_id(/*suffix*/ 6)?;
    let goal = state_db
        .thread_goals()
        .replace_thread_goal(
            thread_id,
            "retry transient no-op activation",
            ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await
        .expect("goal should persist");

    let (tx, mut rx) = mpsc::unbounded_channel();
    let completion_gates = Arc::new(Semaphore::new(0));
    let scheduler = GoalSchedulerHandle::start_with_options(
        Arc::clone(&state_db),
        gated_recording_activator(tx, Arc::clone(&completion_gates)),
        scheduler_options(),
    );

    for expected_attempt in 1..=2 {
        let activation = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("activation attempt {expected_attempt} should run"))
            .expect("activation channel should remain open");
        assert_eq!(thread_id, activation.thread_id);
        if expected_attempt == 1 {
            completion_gates.add_permits(1);
        }
    }

    state_db
        .thread_goals()
        .update_thread_goal(
            thread_id,
            codex_state::GoalUpdate {
                objective: None,
                status: Some(ThreadGoalStatus::Paused),
                token_budget: None,
                expected_goal_id: Some(goal.goal_id),
            },
        )
        .await
        .expect("goal pause should succeed")
        .expect("goal should still exist");
    completion_gates.add_permits(1);

    assert!(
        tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "a changed schedule must stop activation retries"
    );
    scheduler.stop();
    Ok(())
}

#[tokio::test]
async fn scheduler_recovers_an_overdue_deadline_after_restart() -> anyhow::Result<()> {
    let (_codex_home, state_db) = test_runtime().await?;
    let thread_id = test_thread_id(/*suffix*/ 5)?;
    let goal = state_db
        .thread_goals()
        .replace_thread_goal(
            thread_id,
            "survive restart",
            ThreadGoalStatus::Active,
            /*token_budget*/ None,
        )
        .await
        .expect("goal should persist");
    state_db
        .thread_goals()
        .set_thread_goal_supervisor_snoozed_until_ms(
            thread_id,
            &goal.goal_id,
            Some(Utc::now().timestamp_millis() + 100),
        )
        .await
        .expect("deadline should persist");

    let (first_tx, mut first_rx) = mpsc::unbounded_channel();
    let first = GoalSchedulerHandle::start_with_options(
        Arc::clone(&state_db),
        recording_activator(first_tx),
        scheduler_options(),
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    first.stop();
    drop(first);
    assert!(first_rx.try_recv().is_err());

    tokio::time::sleep(Duration::from_millis(100)).await;
    let (second_tx, mut second_rx) = mpsc::unbounded_channel();
    let second = GoalSchedulerHandle::start_with_options(
        Arc::clone(&state_db),
        recording_activator(second_tx),
        scheduler_options(),
    );
    let activation = tokio::time::timeout(Duration::from_secs(1), second_rx.recv())
        .await
        .expect("replacement scheduler should recover the deadline")
        .expect("activation channel should remain open");
    assert_eq!(thread_id, activation.thread_id);
    second.stop();
    Ok(())
}
