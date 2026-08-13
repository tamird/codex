use super::*;
use crate::model::ThreadGoalRow;
use uuid::Uuid;

const GOAL_SUPERVISOR_STATE_TABLE_SQL: &str = r#"
CREATE TABLE thread_goal_supervisor_state (
    thread_id TEXT PRIMARY KEY NOT NULL,
    goal_id TEXT NOT NULL,
    snoozed_until_ms INTEGER,
    updated_at_ms INTEGER NOT NULL
)
"#;

#[derive(Clone)]
pub struct GoalStore {
    pool: Arc<SqlitePool>,
}

/// One active goal whose root thread may need a process-owned supervisor wakeup.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ActiveGoalSupervisorSchedule {
    /// Root thread that owns the active goal.
    pub thread_id: ThreadId,
    /// Goal identity used to discard snooze state from a replaced goal.
    pub goal_id: String,
    /// Millisecond timestamp used by schedulers to detect a changed goal.
    pub goal_updated_at_ms: i64,
    /// Earliest millisecond timestamp at which the supervisor may run again.
    pub snoozed_until_ms: Option<i64>,
}

/// Bounded scan parameters for active goal supervisor schedules.
pub struct ListActiveGoalSupervisorSchedulesParams {
    /// Exclusive stable-thread cursor for the next page.
    pub after_thread_id: Option<ThreadId>,
    /// Requested page size, clamped by the store to `1..=1000`.
    pub limit: usize,
}

/// One page from an active goal supervisor schedule scan.
pub struct ActiveGoalSupervisorSchedulesPage {
    /// Active, non-deferred schedules in stable-thread order.
    pub data: Vec<ActiveGoalSupervisorSchedule>,
    /// Exclusive cursor for the next page, or `None` when this page is terminal.
    pub next_cursor: Option<ThreadId>,
}

impl GoalStore {
    pub(crate) fn new(pool: Arc<SqlitePool>) -> Self {
        Self { pool }
    }

    pub(crate) async fn close(&self) {
        self.pool.close().await;
    }

    pub(crate) async fn ensure_thread_goal_supervisor_state_table(&self) -> anyhow::Result<()> {
        sqlx::query(
            r#"
CREATE TABLE IF NOT EXISTS thread_goal_supervisor_state (
    thread_id TEXT PRIMARY KEY NOT NULL,
    goal_id TEXT NOT NULL,
    snoozed_until_ms INTEGER,
    updated_at_ms INTEGER NOT NULL
)
            "#,
        )
        .execute(self.pool.as_ref())
        .await?;
        let mut connection = self.pool.acquire().await?;
        validate_goal_supervisor_state_table(&mut connection).await?;
        Ok(())
    }

    pub(crate) async fn upsert_frodex_goal_supervisor_state_rows(
        &self,
        supervisor_rows: Vec<(String, String, Option<i64>, i64)>,
    ) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        for (thread_id, goal_id, snoozed_until_ms, updated_at_ms) in supervisor_rows {
            sqlx::query(
                r#"
INSERT INTO thread_goal_supervisor_state (
    thread_id,
    goal_id,
    snoozed_until_ms,
    updated_at_ms
) VALUES (?, ?, ?, ?)
ON CONFLICT(thread_id) DO UPDATE SET
    goal_id = excluded.goal_id,
    snoozed_until_ms = excluded.snoozed_until_ms,
    updated_at_ms = excluded.updated_at_ms
WHERE excluded.updated_at_ms >= thread_goal_supervisor_state.updated_at_ms
                "#,
            )
            .bind(thread_id)
            .bind(goal_id)
            .bind(snoozed_until_ms)
            .bind(updated_at_ms)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }
}

fn normalize_goal_supervisor_table_sql(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_ascii_whitespace() && *character != ';')
        .flat_map(char::to_uppercase)
        .collect::<String>()
        .replace("IFNOTEXISTS", "")
}

async fn validate_goal_supervisor_state_table(
    connection: &mut SqliteConnection,
) -> anyhow::Result<()> {
    let table_sql = sqlx::query_scalar::<_, String>(
        "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'thread_goal_supervisor_state'",
    )
    .fetch_optional(&mut *connection)
    .await?
    .context("goals database is missing thread_goal_supervisor_state")?;
    if normalize_goal_supervisor_table_sql(&table_sql)
        != normalize_goal_supervisor_table_sql(GOAL_SUPERVISOR_STATE_TABLE_SQL)
    {
        anyhow::bail!("goals database thread_goal_supervisor_state has an incompatible schema");
    }

    let columns = sqlx::query("PRAGMA table_xinfo('thread_goal_supervisor_state')")
        .fetch_all(&mut *connection)
        .await?;
    let expected = [
        (0_i64, "thread_id", "TEXT", true, 1_i64),
        (1_i64, "goal_id", "TEXT", true, 0_i64),
        (2_i64, "snoozed_until_ms", "INTEGER", false, 0_i64),
        (3_i64, "updated_at_ms", "INTEGER", true, 0_i64),
    ];
    if columns.len() != expected.len() {
        anyhow::bail!("goals database thread_goal_supervisor_state has incompatible columns");
    }
    for (column, (cid, name, data_type, not_null, primary_key)) in columns.iter().zip(expected) {
        let default_value: Option<String> = column.try_get("dflt_value")?;
        if column.try_get::<i64, _>("cid")? != cid
            || column.try_get::<String, _>("name")? != name
            || column.try_get::<String, _>("type")?.to_ascii_uppercase() != data_type
            || column.try_get::<bool, _>("notnull")? != not_null
            || column.try_get::<i64, _>("pk")? != primary_key
            || column.try_get::<i64, _>("hidden")? != 0
            || default_value.is_some()
        {
            anyhow::bail!(
                "goals database thread_goal_supervisor_state has an incompatible {name} column"
            );
        }
    }
    if !sqlx::query("PRAGMA foreign_key_list('thread_goal_supervisor_state')")
        .fetch_all(&mut *connection)
        .await?
        .is_empty()
    {
        anyhow::bail!("goals database thread_goal_supervisor_state has foreign keys");
    }
    let auxiliary_objects = sqlx::query_scalar::<_, String>(
        r#"
SELECT name
FROM sqlite_schema
WHERE tbl_name = 'thread_goal_supervisor_state'
  AND ((type = 'index' AND sql IS NOT NULL) OR type = 'trigger')
        "#,
    )
    .fetch_all(&mut *connection)
    .await?;
    if !auxiliary_objects.is_empty() {
        anyhow::bail!(
            "goals database thread_goal_supervisor_state has incompatible indexes or triggers"
        );
    }
    let indexes = sqlx::query("PRAGMA index_list('thread_goal_supervisor_state')")
        .fetch_all(&mut *connection)
        .await?;
    if indexes.len() != 1
        || !indexes[0].try_get::<bool, _>("unique")?
        || indexes[0].try_get::<String, _>("origin")? != "pk"
        || indexes[0].try_get::<bool, _>("partial")?
    {
        anyhow::bail!("goals database thread_goal_supervisor_state has incompatible indexes");
    }
    Ok(())
}

pub struct GoalUpdate {
    pub objective: Option<String>,
    pub status: Option<crate::ThreadGoalStatus>,
    pub token_budget: Option<Option<i64>>,
    pub expected_goal_id: Option<String>,
}

pub enum GoalAccountingOutcome {
    Unchanged(Option<crate::ThreadGoal>),
    Updated(crate::ThreadGoal),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GoalAccountingMode {
    ActiveStatusOnly,
    ActiveOnly,
    ActiveOrComplete,
    ActiveOrStopped,
}

impl GoalStore {
    pub async fn get_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<Option<crate::ThreadGoal>> {
        let row = sqlx::query(
            r#"
SELECT
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
FROM thread_goals
WHERE thread_id = ?
            "#,
        )
        .bind(thread_id.to_string())
        .fetch_optional(self.pool.as_ref())
        .await?;

        row.map(|row| thread_goal_from_row(&row)).transpose()
    }

    pub async fn replace_thread_goal_snapshot(
        &self,
        goal: &crate::ThreadGoal,
    ) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            r#"
INSERT INTO thread_goals (
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(thread_id) DO UPDATE SET
    goal_id = excluded.goal_id,
    objective = excluded.objective,
    status = excluded.status,
    token_budget = excluded.token_budget,
    tokens_used = excluded.tokens_used,
    time_used_seconds = excluded.time_used_seconds,
    created_at_ms = excluded.created_at_ms,
    updated_at_ms = excluded.updated_at_ms
            "#,
        )
        .bind(goal.thread_id.to_string())
        .bind(&goal.goal_id)
        .bind(&goal.objective)
        .bind(goal.status.as_str())
        .bind(goal.token_budget)
        .bind(goal.tokens_used)
        .bind(goal.time_used_seconds)
        .bind(datetime_to_epoch_millis(goal.created_at))
        .bind(datetime_to_epoch_millis(goal.updated_at))
        .execute(&mut *transaction)
        .await?;

        sqlx::query(
            r#"
INSERT INTO thread_goal_continuation_deferrals (thread_id)
VALUES (?)
ON CONFLICT(thread_id) DO NOTHING
            "#,
        )
        .bind(goal.thread_id.to_string())
        .execute(&mut *transaction)
        .await?;

        transaction.commit().await?;

        Ok(())
    }

    pub async fn has_thread_goal_continuation_deferral(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<bool> {
        sqlx::query_scalar(
            r#"
SELECT EXISTS(
    SELECT 1
    FROM thread_goal_continuation_deferrals
    WHERE thread_id = ?
)
            "#,
        )
        .bind(thread_id.to_string())
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(Into::into)
    }

    pub async fn clear_thread_goal_continuation_deferral(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM thread_goal_continuation_deferrals WHERE thread_id = ?")
            .bind(thread_id.to_string())
            .execute(self.pool.as_ref())
            .await?;

        Ok(())
    }

    pub async fn get_active_goal_supervisor_schedule(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<Option<ActiveGoalSupervisorSchedule>> {
        let row = sqlx::query(
            r#"
SELECT
    goals.thread_id,
    goals.goal_id,
    goals.updated_at_ms AS goal_updated_at_ms,
    supervisor.snoozed_until_ms
FROM thread_goals AS goals
LEFT JOIN thread_goal_supervisor_state AS supervisor
    ON supervisor.thread_id = goals.thread_id
    AND supervisor.goal_id = goals.goal_id
WHERE goals.thread_id = ?
  AND goals.status = 'active'
  AND NOT EXISTS (
      SELECT 1
      FROM thread_goal_continuation_deferrals AS deferrals
      WHERE deferrals.thread_id = goals.thread_id
  )
            "#,
        )
        .bind(thread_id.to_string())
        .fetch_optional(self.pool.as_ref())
        .await?;

        row.map(|row| active_goal_supervisor_schedule_from_row(&row))
            .transpose()
    }

    pub async fn list_active_goal_supervisor_schedules(
        &self,
        params: ListActiveGoalSupervisorSchedulesParams,
    ) -> anyhow::Result<ActiveGoalSupervisorSchedulesPage> {
        let limit = params.limit.clamp(1, 1_000);
        let after_thread_id = params
            .after_thread_id
            .map(|thread_id| thread_id.to_string());
        let mut rows = sqlx::query(
            r#"
SELECT
    goals.thread_id,
    goals.goal_id,
    goals.updated_at_ms AS goal_updated_at_ms,
    supervisor.snoozed_until_ms
FROM thread_goals AS goals
LEFT JOIN thread_goal_supervisor_state AS supervisor
    ON supervisor.thread_id = goals.thread_id
    AND supervisor.goal_id = goals.goal_id
WHERE goals.status = 'active'
  AND NOT EXISTS (
      SELECT 1
      FROM thread_goal_continuation_deferrals AS deferrals
      WHERE deferrals.thread_id = goals.thread_id
  )
  AND (? IS NULL OR goals.thread_id > ?)
ORDER BY goals.thread_id ASC
LIMIT ?
            "#,
        )
        .bind(after_thread_id.as_deref())
        .bind(after_thread_id.as_deref())
        .bind(i64::try_from(limit.saturating_add(1))?)
        .fetch_all(self.pool.as_ref())
        .await?;

        let has_more = rows.len() > limit;
        rows.truncate(limit);
        let data = rows
            .iter()
            .map(active_goal_supervisor_schedule_from_row)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let next_cursor = has_more
            .then(|| data.last().map(|schedule| schedule.thread_id))
            .flatten();
        Ok(ActiveGoalSupervisorSchedulesPage { data, next_cursor })
    }

    pub async fn replace_thread_goal(
        &self,
        thread_id: ThreadId,
        objective: &str,
        status: crate::ThreadGoalStatus,
        token_budget: Option<i64>,
    ) -> anyhow::Result<crate::ThreadGoal> {
        let goal_id = Uuid::new_v4().to_string();
        let now_ms = datetime_to_epoch_millis(Utc::now());
        let status = status_after_budget_limit(status, /*tokens_used*/ 0, token_budget);
        let row = sqlx::query(
            r#"
INSERT INTO thread_goals (
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
) VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)
ON CONFLICT(thread_id) DO UPDATE SET
    goal_id = excluded.goal_id,
    objective = excluded.objective,
    status = excluded.status,
    token_budget = excluded.token_budget,
    tokens_used = 0,
    time_used_seconds = 0,
    created_at_ms = excluded.created_at_ms,
    updated_at_ms = excluded.updated_at_ms
RETURNING
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
            "#,
        )
        .bind(thread_id.to_string())
        .bind(goal_id)
        .bind(objective)
        .bind(status.as_str())
        .bind(token_budget)
        .bind(now_ms)
        .bind(now_ms)
        .fetch_one(self.pool.as_ref())
        .await?;

        thread_goal_from_row(&row)
    }

    pub async fn insert_thread_goal(
        &self,
        thread_id: ThreadId,
        objective: &str,
        status: crate::ThreadGoalStatus,
        token_budget: Option<i64>,
    ) -> anyhow::Result<Option<crate::ThreadGoal>> {
        let goal_id = Uuid::new_v4().to_string();
        let now_ms = datetime_to_epoch_millis(Utc::now());
        let status = status_after_budget_limit(status, /*tokens_used*/ 0, token_budget);
        let row = sqlx::query(
            r#"
INSERT INTO thread_goals (
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
) VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)
ON CONFLICT(thread_id) DO UPDATE SET
    goal_id = excluded.goal_id,
    objective = excluded.objective,
    status = excluded.status,
    token_budget = excluded.token_budget,
    tokens_used = 0,
    time_used_seconds = 0,
    created_at_ms = excluded.created_at_ms,
    updated_at_ms = excluded.updated_at_ms
WHERE thread_goals.status = 'complete'
RETURNING
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
            "#,
        )
        .bind(thread_id.to_string())
        .bind(goal_id)
        .bind(objective)
        .bind(status.as_str())
        .bind(token_budget)
        .bind(now_ms)
        .bind(now_ms)
        .fetch_optional(self.pool.as_ref())
        .await?;

        row.map(|row| thread_goal_from_row(&row)).transpose()
    }

    pub async fn update_thread_goal(
        &self,
        thread_id: ThreadId,
        update: GoalUpdate,
    ) -> anyhow::Result<Option<crate::ThreadGoal>> {
        let GoalUpdate {
            objective,
            status,
            token_budget,
            expected_goal_id,
        } = update;
        let objective = objective.as_deref();
        let expected_goal_id = expected_goal_id.as_deref();
        let now_ms = datetime_to_epoch_millis(Utc::now());
        let result = match (status, token_budget) {
            (Some(status), Some(token_budget)) => {
                sqlx::query(
                    r#"
UPDATE thread_goals
SET
    objective = COALESCE(?, objective),
    status = CASE
        WHEN status = ? AND ? IN (?, ?) THEN status
        WHEN ? = 'active' AND ? IS NOT NULL AND tokens_used >= ? THEN ?
        ELSE ?
    END,
    token_budget = ?,
    updated_at_ms = ?
WHERE thread_id = ?
  AND (? IS NULL OR goal_id = ?)
            "#,
                )
                .bind(objective)
                .bind(crate::ThreadGoalStatus::BudgetLimited.as_str())
                .bind(status.as_str())
                .bind(crate::ThreadGoalStatus::Paused.as_str())
                .bind(crate::ThreadGoalStatus::Blocked.as_str())
                .bind(status.as_str())
                .bind(token_budget)
                .bind(token_budget)
                .bind(crate::ThreadGoalStatus::BudgetLimited.as_str())
                .bind(status.as_str())
                .bind(token_budget)
                .bind(now_ms)
                .bind(thread_id.to_string())
                .bind(expected_goal_id)
                .bind(expected_goal_id)
                .execute(self.pool.as_ref())
                .await?
            }
            (Some(status), None) => {
                sqlx::query(
                    r#"
UPDATE thread_goals
SET
    objective = COALESCE(?, objective),
    status = CASE
        WHEN status = ? AND ? IN (?, ?) THEN status
        WHEN ? = 'active' AND token_budget IS NOT NULL AND tokens_used >= token_budget THEN ?
        ELSE ?
    END,
    updated_at_ms = ?
WHERE thread_id = ?
  AND (? IS NULL OR goal_id = ?)
            "#,
                )
                .bind(objective)
                .bind(crate::ThreadGoalStatus::BudgetLimited.as_str())
                .bind(status.as_str())
                .bind(crate::ThreadGoalStatus::Paused.as_str())
                .bind(crate::ThreadGoalStatus::Blocked.as_str())
                .bind(status.as_str())
                .bind(crate::ThreadGoalStatus::BudgetLimited.as_str())
                .bind(status.as_str())
                .bind(now_ms)
                .bind(thread_id.to_string())
                .bind(expected_goal_id)
                .bind(expected_goal_id)
                .execute(self.pool.as_ref())
                .await?
            }
            (None, Some(token_budget)) => {
                sqlx::query(
                    r#"
UPDATE thread_goals
SET
    objective = COALESCE(?, objective),
    token_budget = ?,
    status = CASE
        WHEN status = 'active' AND ? IS NOT NULL AND tokens_used >= ? THEN ?
        ELSE status
    END,
    updated_at_ms = ?
WHERE thread_id = ?
  AND (? IS NULL OR goal_id = ?)
            "#,
                )
                .bind(objective)
                .bind(token_budget)
                .bind(token_budget)
                .bind(token_budget)
                .bind(crate::ThreadGoalStatus::BudgetLimited.as_str())
                .bind(now_ms)
                .bind(thread_id.to_string())
                .bind(expected_goal_id)
                .bind(expected_goal_id)
                .execute(self.pool.as_ref())
                .await?
            }
            (None, None) => {
                if let Some(objective) = objective {
                    sqlx::query(
                        r#"
UPDATE thread_goals
SET
    objective = ?,
    updated_at_ms = ?
WHERE thread_id = ?
  AND (? IS NULL OR goal_id = ?)
            "#,
                    )
                    .bind(objective)
                    .bind(now_ms)
                    .bind(thread_id.to_string())
                    .bind(expected_goal_id)
                    .bind(expected_goal_id)
                    .execute(self.pool.as_ref())
                    .await?
                } else {
                    let goal = self.get_thread_goal(thread_id).await?;
                    return Ok(match (goal, expected_goal_id) {
                        (Some(goal), Some(expected_goal_id))
                            if goal.goal_id != expected_goal_id =>
                        {
                            None
                        }
                        (goal, _) => goal,
                    });
                }
            }
        };

        if result.rows_affected() == 0 {
            return Ok(None);
        }

        self.get_thread_goal(thread_id).await
    }

    pub async fn pause_active_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<Option<crate::ThreadGoal>> {
        self.update_active_thread_goal_status(thread_id, crate::ThreadGoalStatus::Paused)
            .await
    }

    pub async fn usage_limit_active_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<Option<crate::ThreadGoal>> {
        self.update_active_thread_goal_status(thread_id, crate::ThreadGoalStatus::UsageLimited)
            .await
    }

    async fn update_active_thread_goal_status(
        &self,
        thread_id: ThreadId,
        status: crate::ThreadGoalStatus,
    ) -> anyhow::Result<Option<crate::ThreadGoal>> {
        let now_ms = datetime_to_epoch_millis(Utc::now());
        let result = sqlx::query(
            r#"
UPDATE thread_goals
SET
    status = ?,
    updated_at_ms = ?
WHERE thread_id = ?
  AND (
      status = 'active'
      OR (
          ? = 'usage_limited'
          AND status = 'budget_limited'
      )
  )
            "#,
        )
        .bind(status.as_str())
        .bind(now_ms)
        .bind(thread_id.to_string())
        .bind(status.as_str())
        .execute(self.pool.as_ref())
        .await?;

        if result.rows_affected() == 0 {
            return Ok(None);
        }

        self.get_thread_goal(thread_id).await
    }

    pub async fn delete_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<Option<crate::ThreadGoal>> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            r#"
DELETE FROM thread_goal_supervisor_state
WHERE thread_id = ?
            "#,
        )
        .bind(thread_id.to_string())
        .execute(&mut *transaction)
        .await?;
        let row = sqlx::query(
            r#"
DELETE FROM thread_goals
WHERE thread_id = ?
RETURNING
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
            "#,
        )
        .bind(thread_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        let goal = row.map(|row| thread_goal_from_row(&row)).transpose()?;
        transaction.commit().await?;
        Ok(goal)
    }

    pub async fn get_thread_goal_supervisor_snoozed_until_ms(
        &self,
        thread_id: ThreadId,
        goal_id: &str,
    ) -> anyhow::Result<Option<i64>> {
        let row = sqlx::query(
            r#"
SELECT snoozed_until_ms
FROM thread_goal_supervisor_state
WHERE thread_id = ? AND goal_id = ?
            "#,
        )
        .bind(thread_id.to_string())
        .bind(goal_id)
        .fetch_optional(self.pool.as_ref())
        .await?;

        Ok(row
            .map(|row| row.try_get::<Option<i64>, _>("snoozed_until_ms"))
            .transpose()?
            .flatten())
    }

    pub async fn set_thread_goal_supervisor_snoozed_until_ms(
        &self,
        thread_id: ThreadId,
        goal_id: &str,
        snoozed_until_ms: Option<i64>,
    ) -> anyhow::Result<()> {
        let now_ms = datetime_to_epoch_millis(Utc::now());
        match snoozed_until_ms {
            Some(snoozed_until_ms) => {
                sqlx::query(
                    r#"
INSERT INTO thread_goal_supervisor_state (
    thread_id,
    goal_id,
    snoozed_until_ms,
    updated_at_ms
) VALUES (?, ?, ?, ?)
ON CONFLICT(thread_id) DO UPDATE SET
    goal_id = excluded.goal_id,
    snoozed_until_ms = excluded.snoozed_until_ms,
    updated_at_ms = excluded.updated_at_ms
                    "#,
                )
                .bind(thread_id.to_string())
                .bind(goal_id)
                .bind(snoozed_until_ms)
                .bind(now_ms)
                .execute(self.pool.as_ref())
                .await?;
            }
            None => {
                sqlx::query(
                    r#"
DELETE FROM thread_goal_supervisor_state
WHERE thread_id = ? AND goal_id = ?
                    "#,
                )
                .bind(thread_id.to_string())
                .bind(goal_id)
                .execute(self.pool.as_ref())
                .await?;
            }
        }
        Ok(())
    }

    pub async fn account_thread_goal_usage(
        &self,
        thread_id: ThreadId,
        time_delta_seconds: i64,
        token_delta: i64,
        mode: GoalAccountingMode,
        expected_goal_id: Option<&str>,
    ) -> anyhow::Result<GoalAccountingOutcome> {
        let time_delta_seconds = time_delta_seconds.max(0);
        let token_delta = token_delta.max(0);
        if time_delta_seconds == 0 && token_delta == 0 {
            return Ok(GoalAccountingOutcome::Unchanged(
                self.get_thread_goal(thread_id).await?,
            ));
        }

        let now_ms = datetime_to_epoch_millis(Utc::now());
        let active_or_stopped_status_filter =
            "status IN ('active', 'paused', 'blocked', 'usage_limited', 'budget_limited')";
        let status_filter = match mode {
            GoalAccountingMode::ActiveStatusOnly => "status = 'active'",
            GoalAccountingMode::ActiveOnly => "status IN ('active', 'budget_limited')",
            GoalAccountingMode::ActiveOrComplete => {
                "status IN ('active', 'budget_limited', 'complete')"
            }
            GoalAccountingMode::ActiveOrStopped => active_or_stopped_status_filter,
        };
        let budget_limit_status_filter = match mode {
            GoalAccountingMode::ActiveStatusOnly
            | GoalAccountingMode::ActiveOnly
            | GoalAccountingMode::ActiveOrComplete => "status = 'active'",
            GoalAccountingMode::ActiveOrStopped => active_or_stopped_status_filter,
        };
        let mut builder = QueryBuilder::<Sqlite>::new(
            r#"
UPDATE thread_goals
SET
    time_used_seconds = time_used_seconds +
            "#,
        );
        builder.push_bind(time_delta_seconds);
        builder.push(
            r#",
    tokens_used = tokens_used +
            "#,
        );
        builder.push_bind(token_delta);
        builder.push(
            r#",
    status = CASE
        WHEN
            "#,
        );
        builder.push(budget_limit_status_filter);
        builder.push(
            r#"
            AND token_budget IS NOT NULL
            AND tokens_used +
            "#,
        );
        builder.push_bind(token_delta);
        builder.push(
            r#"
                >= token_budget
            THEN
            "#,
        );
        builder.push_bind(crate::ThreadGoalStatus::BudgetLimited.as_str());
        builder.push(
            r#"
        ELSE status
    END,
    updated_at_ms =
            "#,
        );
        builder.push_bind(now_ms);
        builder.push(
            r#"
WHERE thread_id =
            "#,
        );
        builder.push_bind(thread_id.to_string());
        builder.push(" AND ");
        builder.push(status_filter);
        if let Some(expected_goal_id) = expected_goal_id {
            builder.push(" AND goal_id = ").push_bind(expected_goal_id);
        }
        builder.push(
            r#"
RETURNING
    thread_id,
    goal_id,
    objective,
    status,
    token_budget,
    tokens_used,
    time_used_seconds,
    created_at_ms,
    updated_at_ms
            "#,
        );

        let row = builder.build().fetch_optional(self.pool.as_ref()).await?;

        let Some(row) = row else {
            return Ok(GoalAccountingOutcome::Unchanged(
                self.get_thread_goal(thread_id).await?,
            ));
        };

        let updated = thread_goal_from_row(&row)?;
        Ok(GoalAccountingOutcome::Updated(updated))
    }
}

fn active_goal_supervisor_schedule_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> anyhow::Result<ActiveGoalSupervisorSchedule> {
    Ok(ActiveGoalSupervisorSchedule {
        thread_id: ThreadId::from_string(row.try_get::<String, _>("thread_id")?.as_str())?,
        goal_id: row.try_get("goal_id")?,
        goal_updated_at_ms: row.try_get("goal_updated_at_ms")?,
        snoozed_until_ms: row.try_get("snoozed_until_ms")?,
    })
}

fn thread_goal_from_row(row: &sqlx::sqlite::SqliteRow) -> anyhow::Result<crate::ThreadGoal> {
    ThreadGoalRow::try_from_row(row).and_then(crate::ThreadGoal::try_from)
}

fn status_after_budget_limit(
    status: crate::ThreadGoalStatus,
    tokens_used: i64,
    token_budget: Option<i64>,
) -> crate::ThreadGoalStatus {
    if status == crate::ThreadGoalStatus::Active
        && token_budget.is_some_and(|budget| tokens_used >= budget)
    {
        crate::ThreadGoalStatus::BudgetLimited
    } else {
        status
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::test_support::test_thread_metadata;
    use crate::runtime::test_support::unique_temp_dir;
    use codex_utils_absolute_path::test_support::PathExt;
    use pretty_assertions::assert_eq;

    async fn test_runtime() -> std::sync::Arc<StateRuntime> {
        StateRuntime::init(
            crate::SqliteConfig::new_for_testing(unique_temp_dir().as_path().abs()),
            "test-provider".to_string(),
        )
        .await
        .expect("state db should initialize")
    }

    fn test_thread_id() -> ThreadId {
        ThreadId::from_string("00000000-0000-0000-0000-000000000123").expect("valid thread id")
    }

    async fn upsert_test_thread(runtime: &StateRuntime, thread_id: ThreadId) {
        let sqlite_home = runtime.sqlite().home();
        let metadata = test_thread_metadata(sqlite_home, thread_id, sqlite_home.join("workspace"));
        runtime
            .upsert_thread(&metadata)
            .await
            .expect("test thread should be upserted");
    }

    #[tokio::test]
    async fn replace_update_and_get_thread_goal() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;

        let goal = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "optimize the benchmark",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(100_000),
            )
            .await
            .expect("goal replacement should succeed");
        assert_eq!(
            Some(goal.clone()),
            runtime
                .thread_goals()
                .get_thread_goal(thread_id)
                .await
                .unwrap()
        );
        let metadata = runtime
            .get_thread(thread_id)
            .await
            .expect("thread metadata should load")
            .expect("thread should exist");
        assert_eq!(metadata.preview.as_deref(), Some("hello"));

        let updated = runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(crate::ThreadGoalStatus::Paused),
                    token_budget: Some(Some(200_000)),
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");
        let expected = crate::ThreadGoal {
            status: crate::ThreadGoalStatus::Paused,
            token_budget: Some(200_000),
            updated_at: updated.updated_at,
            ..goal.clone()
        };
        assert_eq!(expected, updated);

        let replaced = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "ship the new result",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ None,
            )
            .await
            .expect("goal replacement should succeed");
        assert_eq!("ship the new result", replaced.objective);
        assert_eq!(crate::ThreadGoalStatus::Active, replaced.status);
        assert_eq!(None, replaced.token_budget);
        assert_eq!(0, replaced.tokens_used);
        assert_eq!(0, replaced.time_used_seconds);

        assert_eq!(
            Some(replaced),
            runtime
                .thread_goals()
                .delete_thread_goal(thread_id)
                .await
                .unwrap()
        );
        assert_eq!(
            None,
            runtime
                .thread_goals()
                .get_thread_goal(thread_id)
                .await
                .unwrap()
        );
        assert_eq!(
            None,
            runtime
                .thread_goals()
                .delete_thread_goal(thread_id)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn replace_thread_goal_applies_budget_limit_immediately() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;

        let replaced = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "stay within budget",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(0),
            )
            .await
            .expect("goal replacement should succeed");

        assert_eq!(crate::ThreadGoalStatus::BudgetLimited, replaced.status);
        assert_eq!(Some(0), replaced.token_budget);
        assert_eq!(0, replaced.tokens_used);
        assert_eq!(0, replaced.time_used_seconds);
    }

    #[tokio::test]
    async fn insert_thread_goal_does_not_replace_existing_goal() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;

        let inserted = runtime
            .thread_goals()
            .insert_thread_goal(
                thread_id,
                "optimize the benchmark",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(100_000),
            )
            .await
            .expect("goal insertion should succeed")
            .expect("goal should be inserted");

        let duplicate = runtime
            .thread_goals()
            .insert_thread_goal(
                thread_id,
                "replace the benchmark",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(200_000),
            )
            .await
            .expect("duplicate insert should not fail");

        assert_eq!(None, duplicate);
        assert_eq!(
            Some(inserted),
            runtime
                .thread_goals()
                .get_thread_goal(thread_id)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn insert_thread_goal_applies_budget_limit_immediately() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;

        let inserted = runtime
            .thread_goals()
            .insert_thread_goal(
                thread_id,
                "stay within budget",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(0),
            )
            .await
            .expect("goal insertion should succeed")
            .expect("goal should be inserted");

        assert_eq!(crate::ThreadGoalStatus::BudgetLimited, inserted.status);
        assert_eq!(Some(0), inserted.token_budget);
        assert_eq!(0, inserted.tokens_used);
        assert_eq!(0, inserted.time_used_seconds);
    }

    #[tokio::test]
    async fn update_thread_goal_ignores_replaced_goal_version() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;

        let original = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "old objective",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(100),
            )
            .await
            .expect("goal replacement should succeed");
        let replacement = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "new objective",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(10),
            )
            .await
            .expect("goal replacement should succeed");

        let stale_update = runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(crate::ThreadGoalStatus::Complete),
                    token_budget: None,
                    expected_goal_id: Some(original.goal_id),
                },
            )
            .await
            .expect("goal update should succeed");

        assert_eq!(None, stale_update);
        assert_eq!(
            Some(replacement.clone()),
            runtime
                .thread_goals()
                .get_thread_goal(thread_id)
                .await
                .expect("goal read should succeed")
        );

        let fresh_update = runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(crate::ThreadGoalStatus::Complete),
                    token_budget: None,
                    expected_goal_id: Some(replacement.goal_id),
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("fresh update should match the replacement goal");
        assert_eq!(crate::ThreadGoalStatus::Complete, fresh_update.status);
    }

    #[tokio::test]
    async fn usage_accounting_ignores_replaced_goal_version() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;

        let original = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "old objective",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(100),
            )
            .await
            .expect("goal replacement should succeed");
        let replacement = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "new objective",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(10),
            )
            .await
            .expect("goal replacement should succeed");

        let outcome = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 5,
                /*token_delta*/ 5,
                GoalAccountingMode::ActiveOnly,
                Some(original.goal_id.as_str()),
            )
            .await
            .expect("usage accounting should succeed");

        let GoalAccountingOutcome::Unchanged(Some(goal)) = outcome else {
            panic!("stale goal version should not be updated");
        };
        assert_ne!(replacement.goal_id, original.goal_id);
        assert_eq!(replacement.created_at, goal.created_at);
        assert_eq!("new objective", goal.objective);
        assert_eq!(0, goal.tokens_used);
        assert_eq!(0, goal.time_used_seconds);
    }

    #[tokio::test]
    async fn update_thread_goal_objective_preserves_usage_and_created_at() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;

        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "draft the report",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(100),
            )
            .await
            .expect("goal replacement should succeed");
        let outcome = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 12,
                /*token_delta*/ 30,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(accounted) = outcome else {
            panic!("active goal should account usage");
        };

        let updated = runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: Some("draft the report clearly".to_string()),
                    status: Some(crate::ThreadGoalStatus::Paused),
                    token_budget: Some(Some(200)),
                    expected_goal_id: Some(accounted.goal_id.clone()),
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");
        let expected = crate::ThreadGoal {
            objective: "draft the report clearly".to_string(),
            status: crate::ThreadGoalStatus::Paused,
            token_budget: Some(200),
            updated_at: updated.updated_at,
            ..accounted
        };
        assert_eq!(expected, updated);
    }

    #[tokio::test]
    async fn concurrent_partial_updates_preserve_independent_fields() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "optimize the benchmark",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(100_000),
            )
            .await
            .expect("goal replacement should succeed");

        let status_update = runtime.thread_goals().update_thread_goal(
            thread_id,
            GoalUpdate {
                objective: None,
                status: Some(crate::ThreadGoalStatus::Paused),
                token_budget: None,
                expected_goal_id: None,
            },
        );
        let budget_update = runtime.thread_goals().update_thread_goal(
            thread_id,
            GoalUpdate {
                objective: None,
                status: None,
                token_budget: Some(Some(200_000)),
                expected_goal_id: None,
            },
        );
        let (status_update, budget_update) = tokio::join!(status_update, budget_update);
        status_update.expect("status update should succeed");
        budget_update.expect("budget update should succeed");

        let goal = runtime
            .thread_goals()
            .get_thread_goal(thread_id)
            .await
            .expect("goal read should succeed")
            .expect("goal should exist");
        assert_eq!(crate::ThreadGoalStatus::Paused, goal.status);
        assert_eq!(Some(200_000), goal.token_budget);
    }

    #[tokio::test]
    async fn pause_active_thread_goal_does_not_clobber_terminal_status() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        let goal = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "optimize the benchmark",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(100_000),
            )
            .await
            .expect("goal replacement should succeed");

        let paused = runtime
            .thread_goals()
            .pause_active_thread_goal(thread_id)
            .await
            .expect("active pause should succeed")
            .expect("active goal should be paused");
        let expected = crate::ThreadGoal {
            status: crate::ThreadGoalStatus::Paused,
            updated_at: paused.updated_at,
            ..goal
        };
        assert_eq!(expected, paused);

        let complete = runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(crate::ThreadGoalStatus::Complete),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");
        let pause_result = runtime
            .thread_goals()
            .pause_active_thread_goal(thread_id)
            .await
            .expect("terminal pause attempt should succeed");
        assert_eq!(None, pause_result);
        assert_eq!(
            Some(complete),
            runtime
                .thread_goals()
                .get_thread_goal(thread_id)
                .await
                .expect("goal read should succeed")
        );
    }

    #[tokio::test]
    async fn usage_limit_active_thread_goal_updates_active_or_budget_limited_goals() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        let goal = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "optimize the benchmark",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ None,
            )
            .await
            .expect("goal replacement should succeed");

        let usage_limited = runtime
            .thread_goals()
            .usage_limit_active_thread_goal(thread_id)
            .await
            .expect("usage limiting should succeed")
            .expect("active goal should become usage limited");
        let expected = crate::ThreadGoal {
            status: crate::ThreadGoalStatus::UsageLimited,
            updated_at: usage_limited.updated_at,
            ..goal
        };
        assert_eq!(expected, usage_limited);

        let second_update = runtime
            .thread_goals()
            .usage_limit_active_thread_goal(thread_id)
            .await
            .expect("repeated usage limiting should succeed");
        assert_eq!(None, second_update);

        let budget_limited = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "keep the usage failure visible",
                crate::ThreadGoalStatus::BudgetLimited,
                /*token_budget*/ Some(1),
            )
            .await
            .expect("goal replacement should succeed");
        let usage_limited = runtime
            .thread_goals()
            .usage_limit_active_thread_goal(thread_id)
            .await
            .expect("usage limiting should succeed")
            .expect("budget-limited goal should become usage limited");
        let expected = crate::ThreadGoal {
            status: crate::ThreadGoalStatus::UsageLimited,
            updated_at: usage_limited.updated_at,
            ..budget_limited
        };
        assert_eq!(expected, usage_limited);
    }

    #[tokio::test]
    async fn usage_accounting_updates_active_goals_and_accounts_budget_limited_in_flight_usage() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "stay within budget",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(20),
            )
            .await
            .expect("goal replacement should succeed");

        let outcome = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 7,
                /*token_delta*/ 5,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(goal) = outcome else {
            panic!("active goal should be updated");
        };
        assert_eq!(crate::ThreadGoalStatus::Active, goal.status);
        assert_eq!(5, goal.tokens_used);
        assert_eq!(7, goal.time_used_seconds);

        let outcome = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 3,
                /*token_delta*/ 15,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(goal) = outcome else {
            panic!("budget crossing should update the goal");
        };
        assert_eq!(crate::ThreadGoalStatus::BudgetLimited, goal.status);
        assert_eq!(20, goal.tokens_used);
        assert_eq!(10, goal.time_used_seconds);

        let outcome = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 5,
                /*token_delta*/ 5,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(goal) = outcome else {
            panic!("budget-limited goal should still account in-flight active usage");
        };
        assert_eq!(crate::ThreadGoalStatus::BudgetLimited, goal.status);
        assert_eq!(25, goal.tokens_used);
        assert_eq!(15, goal.time_used_seconds);
    }

    #[tokio::test]
    async fn active_status_only_usage_accounting_does_not_update_budget_limited_goals() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "stay stopped",
                crate::ThreadGoalStatus::BudgetLimited,
                /*token_budget*/ Some(20),
            )
            .await
            .expect("goal replacement should succeed");

        let outcome = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 5,
                /*token_delta*/ 5,
                GoalAccountingMode::ActiveStatusOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Unchanged(Some(goal)) = outcome else {
            panic!("budget-limited goal should not be updated");
        };
        assert_eq!(crate::ThreadGoalStatus::BudgetLimited, goal.status);
        assert_eq!(0, goal.tokens_used);
        assert_eq!(0, goal.time_used_seconds);
    }

    #[tokio::test]
    async fn stopped_usage_accounting_promotes_paused_goal_over_budget() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "stop before overrun",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(20),
            )
            .await
            .expect("goal replacement should succeed");
        runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                crate::GoalUpdate {
                    objective: None,
                    status: Some(crate::ThreadGoalStatus::Paused),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed");

        let outcome = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 3,
                /*token_delta*/ 25,
                GoalAccountingMode::ActiveOrStopped,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(goal) = outcome else {
            panic!("stopped goal should account final usage");
        };
        assert_eq!(crate::ThreadGoalStatus::BudgetLimited, goal.status);
        assert_eq!(25, goal.tokens_used);
        assert_eq!(3, goal.time_used_seconds);
    }

    #[tokio::test]
    async fn budget_updates_immediately_stop_active_goals_already_over_budget() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "stay within budget",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(100),
            )
            .await
            .expect("goal replacement should succeed");
        runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 1,
                /*token_delta*/ 50,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");

        let lowered = runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: None,
                    token_budget: Some(Some(40)),
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");

        assert_eq!(crate::ThreadGoalStatus::BudgetLimited, lowered.status);
        assert_eq!(Some(40), lowered.token_budget);
        assert_eq!(50, lowered.tokens_used);
    }

    #[tokio::test]
    async fn activating_goal_already_over_budget_keeps_it_budget_limited() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "stay within budget",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(40),
            )
            .await
            .expect("goal replacement should succeed");
        runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 1,
                /*token_delta*/ 50,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");

        let reactivated = runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: Some("stay within budget, with clearer wording".to_string()),
                    status: Some(crate::ThreadGoalStatus::Active),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");

        assert_eq!(crate::ThreadGoalStatus::BudgetLimited, reactivated.status);
        assert_eq!(
            "stay within budget, with clearer wording",
            reactivated.objective
        );
        assert_eq!(Some(40), reactivated.token_budget);
        assert_eq!(50, reactivated.tokens_used);
    }

    #[tokio::test]
    async fn pausing_budget_limited_goal_preserves_terminal_status() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "stay within budget",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(40),
            )
            .await
            .expect("goal replacement should succeed");
        runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 1,
                /*token_delta*/ 50,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");

        let paused = runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(crate::ThreadGoalStatus::Paused),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");

        assert_eq!(crate::ThreadGoalStatus::BudgetLimited, paused.status);
        assert_eq!(Some(40), paused.token_budget);
        assert_eq!(50, paused.tokens_used);
    }

    #[tokio::test]
    async fn blocking_budget_limited_goal_preserves_terminal_status() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "stay within budget",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(40),
            )
            .await
            .expect("goal replacement should succeed");
        let outcome = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 1,
                /*token_delta*/ 50,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(budget_limited) = outcome else {
            panic!("budget crossing should update the goal");
        };

        let blocked = runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(crate::ThreadGoalStatus::Blocked),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");

        let expected = crate::ThreadGoal {
            updated_at: blocked.updated_at,
            ..budget_limited
        };
        assert_eq!(expected, blocked);
    }

    #[tokio::test]
    async fn usage_accounting_can_finalize_completed_goal_for_completing_turn() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "finish the report",
                crate::ThreadGoalStatus::Complete,
                /*token_budget*/ Some(1_000),
            )
            .await
            .expect("goal replacement should succeed");

        let active_only = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 30,
                /*token_delta*/ 200,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Unchanged(Some(goal)) = active_only else {
            panic!("completed goal should not be updated by active-only accounting");
        };
        assert_eq!(crate::ThreadGoalStatus::Complete, goal.status);
        assert_eq!(0, goal.tokens_used);
        assert_eq!(0, goal.time_used_seconds);

        let completing_turn = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 30,
                /*token_delta*/ 200,
                GoalAccountingMode::ActiveOrComplete,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(goal) = completing_turn else {
            panic!("completed goal should be updated for final accounting");
        };
        assert_eq!(crate::ThreadGoalStatus::Complete, goal.status);
        assert_eq!(200, goal.tokens_used);
        assert_eq!(30, goal.time_used_seconds);
    }

    #[tokio::test]
    async fn usage_accounting_can_finalize_stopped_goal_for_in_flight_turn() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "finish the report",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(1_000),
            )
            .await
            .expect("goal replacement should succeed");
        runtime
            .thread_goals()
            .update_thread_goal(
                thread_id,
                GoalUpdate {
                    objective: None,
                    status: Some(crate::ThreadGoalStatus::Paused),
                    token_budget: None,
                    expected_goal_id: None,
                },
            )
            .await
            .expect("goal update should succeed")
            .expect("goal should exist");

        let active_only = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 30,
                /*token_delta*/ 200,
                GoalAccountingMode::ActiveOnly,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Unchanged(Some(goal)) = active_only else {
            panic!("paused goal should not be updated by active-only accounting");
        };
        assert_eq!(crate::ThreadGoalStatus::Paused, goal.status);
        assert_eq!(0, goal.tokens_used);
        assert_eq!(0, goal.time_used_seconds);

        let in_flight_turn = runtime
            .thread_goals()
            .account_thread_goal_usage(
                thread_id,
                /*time_delta_seconds*/ 30,
                /*token_delta*/ 200,
                GoalAccountingMode::ActiveOrStopped,
                /*expected_goal_id*/ None,
            )
            .await
            .expect("usage accounting should succeed");
        let GoalAccountingOutcome::Updated(goal) = in_flight_turn else {
            panic!("stopped goal should be updated for in-flight accounting");
        };
        assert_eq!(crate::ThreadGoalStatus::Paused, goal.status);
        assert_eq!(200, goal.tokens_used);
        assert_eq!(30, goal.time_used_seconds);
    }

    #[tokio::test]
    async fn usage_accounting_adds_concurrent_token_deltas() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "count every token",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ Some(1_000),
            )
            .await
            .expect("goal replacement should succeed");

        let first = runtime.thread_goals().account_thread_goal_usage(
            thread_id,
            /*time_delta_seconds*/ 4,
            /*token_delta*/ 40,
            GoalAccountingMode::ActiveOnly,
            /*expected_goal_id*/ None,
        );
        let second = runtime.thread_goals().account_thread_goal_usage(
            thread_id,
            /*time_delta_seconds*/ 6,
            /*token_delta*/ 60,
            GoalAccountingMode::ActiveOnly,
            /*expected_goal_id*/ None,
        );
        let (first, second) = tokio::join!(first, second);
        first.expect("first usage accounting should succeed");
        second.expect("second usage accounting should succeed");

        let goal = runtime
            .thread_goals()
            .get_thread_goal(thread_id)
            .await
            .expect("goal read should succeed")
            .expect("goal should exist");
        assert_eq!(100, goal.tokens_used);
        assert_eq!(10, goal.time_used_seconds);
    }

    #[tokio::test]
    async fn supervisor_snooze_state_is_scoped_to_current_goal_id() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        let goal = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "persist supervisor snooze",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ None,
            )
            .await
            .expect("goal replacement should succeed");

        runtime
            .thread_goals()
            .set_thread_goal_supervisor_snoozed_until_ms(thread_id, &goal.goal_id, Some(123_456))
            .await
            .expect("supervisor snooze should persist");
        assert_eq!(
            Some(123_456),
            runtime
                .thread_goals()
                .get_thread_goal_supervisor_snoozed_until_ms(thread_id, &goal.goal_id)
                .await
                .expect("supervisor snooze should read")
        );
        assert_eq!(
            None,
            runtime
                .thread_goals()
                .get_thread_goal_supervisor_snoozed_until_ms(thread_id, "stale-goal-id")
                .await
                .expect("stale supervisor snooze should be ignored")
        );

        sqlx::query(
            "UPDATE thread_goal_supervisor_state SET snoozed_until_ms = NULL WHERE thread_id = ? AND goal_id = ?",
        )
        .bind(thread_id.to_string())
        .bind(&goal.goal_id)
        .execute(runtime.thread_goals().pool.as_ref())
        .await
        .expect("nullable supervisor state should persist");
        assert_eq!(
            None,
            runtime
                .thread_goals()
                .get_thread_goal_supervisor_snoozed_until_ms(thread_id, &goal.goal_id)
                .await
                .expect("nullable supervisor state should read as no deadline")
        );

        runtime
            .thread_goals()
            .set_thread_goal_supervisor_snoozed_until_ms(
                thread_id,
                &goal.goal_id,
                /*snoozed_until_ms*/ None,
            )
            .await
            .expect("supervisor snooze should clear");
        assert_eq!(
            None,
            runtime
                .thread_goals()
                .get_thread_goal_supervisor_snoozed_until_ms(thread_id, &goal.goal_id)
                .await
                .expect("supervisor snooze should be cleared")
        );
    }

    #[tokio::test]
    async fn supervisor_state_table_rejects_an_existing_incompatible_schema() {
        let runtime = test_runtime().await;
        sqlx::query("DROP TABLE thread_goal_supervisor_state")
            .execute(runtime.thread_goals().pool.as_ref())
            .await
            .expect("runtime table should drop");
        sqlx::query(
            r#"
CREATE TABLE thread_goal_supervisor_state (
    thread_id TEXT PRIMARY KEY NOT NULL,
    goal_id TEXT UNIQUE NOT NULL,
    snoozed_until_ms INTEGER,
    updated_at_ms INTEGER NOT NULL
)
            "#,
        )
        .execute(runtime.thread_goals().pool.as_ref())
        .await
        .expect("incompatible runtime table should be created");

        runtime
            .thread_goals()
            .ensure_thread_goal_supervisor_state_table()
            .await
            .expect_err("an incompatible destination table must fail closed");
    }

    #[tokio::test]
    async fn legacy_supervisor_state_batch_rolls_back_when_one_row_fails() {
        let runtime = test_runtime().await;
        let rejected_thread_id = "00000000-0000-0000-0000-000000000402";
        sqlx::query(
            "CREATE TRIGGER reject_second_supervisor_row BEFORE INSERT ON thread_goal_supervisor_state WHEN NEW.thread_id = '00000000-0000-0000-0000-000000000402' BEGIN SELECT RAISE(ABORT, 'injected transfer failure'); END",
        )
        .execute(runtime.thread_goals().pool.as_ref())
        .await
        .expect("transfer failure trigger should be created");

        runtime
            .thread_goals()
            .upsert_frodex_goal_supervisor_state_rows(vec![
                (
                    "00000000-0000-0000-0000-000000000401".to_string(),
                    "first-goal".to_string(),
                    Some(401_000),
                    401,
                ),
                (
                    rejected_thread_id.to_string(),
                    "second-goal".to_string(),
                    Some(402_000),
                    402,
                ),
            ])
            .await
            .expect_err("the second row should abort the destination transaction");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM thread_goal_supervisor_state")
                .fetch_one(runtime.thread_goals().pool.as_ref())
                .await
                .unwrap(),
            0,
            "the first row must roll back with the rejected row"
        );
    }

    #[tokio::test]
    async fn deleting_a_goal_and_its_supervisor_state_is_atomic() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        let goal = runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "preserve supervisor state when deletion fails",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ None,
            )
            .await
            .expect("goal replacement should succeed");
        runtime
            .thread_goals()
            .set_thread_goal_supervisor_snoozed_until_ms(thread_id, &goal.goal_id, Some(123_456))
            .await
            .expect("supervisor state should persist");
        sqlx::query(
            "CREATE TRIGGER fail_goal_delete BEFORE DELETE ON thread_goals BEGIN SELECT RAISE(ABORT, 'injected delete failure'); END",
        )
        .execute(runtime.thread_goals().pool.as_ref())
        .await
        .expect("delete failure trigger should be created");

        runtime
            .thread_goals()
            .delete_thread_goal(thread_id)
            .await
            .expect_err("injected goal delete failure should abort the transaction");
        assert_eq!(
            Some(123_456),
            runtime
                .thread_goals()
                .get_thread_goal_supervisor_snoozed_until_ms(thread_id, &goal.goal_id)
                .await
                .expect("supervisor state should remain after rollback")
        );
        assert_eq!(
            Some(goal),
            runtime
                .thread_goals()
                .get_thread_goal(thread_id)
                .await
                .expect("goal should remain after rollback")
        );
    }

    #[tokio::test]
    async fn active_goal_supervisor_schedules_are_bounded_and_ignore_stale_state() {
        let runtime = test_runtime().await;
        let first_thread_id = ThreadId::from_string("00000000-0000-0000-0000-000000000123")
            .expect("valid first thread id");
        let paused_thread_id = ThreadId::from_string("00000000-0000-0000-0000-000000000124")
            .expect("valid paused thread id");
        let last_thread_id = ThreadId::from_string("00000000-0000-0000-0000-000000000125")
            .expect("valid last thread id");
        let deferred_thread_id = ThreadId::from_string("00000000-0000-0000-0000-000000000126")
            .expect("valid deferred thread id");
        for thread_id in [
            first_thread_id,
            paused_thread_id,
            last_thread_id,
            deferred_thread_id,
        ] {
            upsert_test_thread(&runtime, thread_id).await;
        }

        let first_goal = runtime
            .thread_goals()
            .replace_thread_goal(
                first_thread_id,
                "wake later",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ None,
            )
            .await
            .expect("first goal should persist");
        runtime
            .thread_goals()
            .set_thread_goal_supervisor_snoozed_until_ms(
                first_thread_id,
                &first_goal.goal_id,
                Some(123_456),
            )
            .await
            .expect("first deadline should persist");

        runtime
            .thread_goals()
            .replace_thread_goal(
                paused_thread_id,
                "do not wake",
                crate::ThreadGoalStatus::Paused,
                /*token_budget*/ None,
            )
            .await
            .expect("paused goal should persist");

        let stale_goal = runtime
            .thread_goals()
            .replace_thread_goal(
                last_thread_id,
                "old goal",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ None,
            )
            .await
            .expect("stale goal should persist");
        runtime
            .thread_goals()
            .set_thread_goal_supervisor_snoozed_until_ms(
                last_thread_id,
                &stale_goal.goal_id,
                Some(999_999),
            )
            .await
            .expect("stale deadline should persist");
        let last_goal = runtime
            .thread_goals()
            .replace_thread_goal(
                last_thread_id,
                "current goal",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ None,
            )
            .await
            .expect("replacement goal should persist");
        let deferred_goal = runtime
            .thread_goals()
            .replace_thread_goal(
                deferred_thread_id,
                "wait for the next explicit turn",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ None,
            )
            .await
            .expect("deferred goal should persist");
        runtime
            .thread_goals()
            .replace_thread_goal_snapshot(&deferred_goal)
            .await
            .expect("goal continuation should be deferred");

        let first_page = runtime
            .thread_goals()
            .list_active_goal_supervisor_schedules(ListActiveGoalSupervisorSchedulesParams {
                after_thread_id: None,
                limit: 1,
            })
            .await
            .expect("first page should load");
        assert_eq!(Some(first_thread_id), first_page.next_cursor);
        assert_eq!(
            vec![ActiveGoalSupervisorSchedule {
                thread_id: first_thread_id,
                goal_id: first_goal.goal_id,
                goal_updated_at_ms: first_goal.updated_at.timestamp_millis(),
                snoozed_until_ms: Some(123_456),
            }],
            first_page.data
        );

        let last_page = runtime
            .thread_goals()
            .list_active_goal_supervisor_schedules(ListActiveGoalSupervisorSchedulesParams {
                after_thread_id: first_page.next_cursor,
                limit: 1,
            })
            .await
            .expect("last page should load");
        assert_eq!(None, last_page.next_cursor);
        assert_eq!(
            vec![ActiveGoalSupervisorSchedule {
                thread_id: last_thread_id,
                goal_id: last_goal.goal_id,
                goal_updated_at_ms: last_goal.updated_at.timestamp_millis(),
                snoozed_until_ms: None,
            }],
            last_page.data
        );
        assert_eq!(
            None,
            runtime
                .thread_goals()
                .get_active_goal_supervisor_schedule(paused_thread_id)
                .await
                .expect("paused schedule lookup should succeed")
        );
        assert_eq!(
            None,
            runtime
                .thread_goals()
                .get_active_goal_supervisor_schedule(deferred_thread_id)
                .await
                .expect("deferred schedule lookup should succeed")
        );
    }

    #[tokio::test]
    async fn deleting_thread_deletes_goal() {
        let runtime = test_runtime().await;
        let thread_id = test_thread_id();
        upsert_test_thread(&runtime, thread_id).await;
        runtime
            .thread_goals()
            .replace_thread_goal(
                thread_id,
                "clean up with the thread",
                crate::ThreadGoalStatus::Active,
                /*token_budget*/ None,
            )
            .await
            .expect("goal replacement should succeed");

        runtime
            .delete_thread(thread_id)
            .await
            .expect("thread deletion should succeed");

        assert_eq!(
            None,
            runtime
                .thread_goals()
                .get_thread_goal(thread_id)
                .await
                .expect("goal read should succeed")
        );
    }
}
