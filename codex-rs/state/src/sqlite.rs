//! Shared SQLite connection configuration.

#![expect(
    clippy::disallowed_methods,
    reason = "this is the centralized SQLite connection shim"
)]

use crate::DbTelemetry;
use crate::migrations::repair_frodex_agent_path_migration_collision;
use crate::migrations::repair_frodex_goal_supervisor_state_migration;
use crate::migrations::repair_legacy_recency_migration_version;
use crate::runtime::RuntimeDbInitError;
use crate::telemetry;
use crate::telemetry::DbKind;
use codex_utils_absolute_path::AbsolutePathBuf;
use log::LevelFilter;
use sqlx::ConnectOptions;
use sqlx::Error;
use sqlx::Sqlite;
use sqlx::SqlitePool;
use sqlx::Transaction;
use sqlx::migrate::Migrator;
use sqlx::sqlite::SqliteAutoVacuum;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqliteJournalMode;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::sqlite::SqliteSynchronous;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use tracing::warn;

const LOGS_DB_FILENAME: &str = "logs_2.sqlite";
const GOALS_DB_FILENAME: &str = "goals_1.sqlite";
const MEMORIES_DB_FILENAME: &str = "memories_1.sqlite";
const QUEUE_DB_FILENAME: &str = "queue_1.sqlite";
const STATE_DB_FILENAME: &str = "state_5.sqlite";
const THREAD_HISTORY_DB_FILENAME: &str = "thread_history_1.sqlite";

/// State migration boundaries used by rollback fault-injection tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StateMigrationStep {
    GoalSupervisorCompatibility,
    AgentPathCompatibility,
    RecencyCompatibility,
    OfficialMigrations,
}

fn ensure_transactional_migrations(migrator: &Migrator) -> anyhow::Result<()> {
    if migrator.no_tx || migrator.migrations.iter().any(|migration| migration.no_tx) {
        anyhow::bail!("database schema migrations must all support the startup transaction");
    }
    Ok(())
}

fn is_sqlite_writer_contention(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(database_error) = error else {
        return false;
    };
    database_error
        .code()
        .and_then(|code| code.parse::<i32>().ok())
        .is_some_and(|code| matches!(code & 0xff, 5 | 6))
}

async fn begin_runtime_migration_transaction<F>(
    pool: &SqlitePool,
    mut on_contention: F,
) -> anyhow::Result<Transaction<'static, Sqlite>>
where
    F: FnMut(),
{
    loop {
        match pool.begin_with("BEGIN IMMEDIATE").await {
            Ok(transaction) => return Ok(transaction),
            Err(error) if is_sqlite_writer_contention(&error) => {
                on_contention();
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn migrate_state_database_with_hooks<F, G>(
    pool: &SqlitePool,
    migrator: &Migrator,
    mut after_step: F,
    on_contention: G,
) -> anyhow::Result<()>
where
    F: FnMut(StateMigrationStep) -> anyhow::Result<()>,
    G: FnMut(),
{
    ensure_transactional_migrations(migrator)?;

    let mut transaction = begin_runtime_migration_transaction(pool, on_contention).await?;
    let migration_result = async {
        repair_frodex_goal_supervisor_state_migration(&mut transaction).await?;
        after_step(StateMigrationStep::GoalSupervisorCompatibility)?;
        let repaired_agent_path =
            repair_frodex_agent_path_migration_collision(&mut transaction, migrator).await?;
        after_step(StateMigrationStep::AgentPathCompatibility)?;
        repair_legacy_recency_migration_version(&mut transaction, migrator).await?;
        after_step(StateMigrationStep::RecencyCompatibility)?;
        migrator
            .run_direct(None, &mut *transaction, false)
            .await
            .map_err(anyhow::Error::from)?;
        after_step(StateMigrationStep::OfficialMigrations)?;
        Ok::<_, anyhow::Error>(repaired_agent_path)
    }
    .await;

    let repaired_agent_path = match migration_result {
        Ok(repaired_agent_path) => {
            transaction.commit().await?;
            repaired_agent_path
        }
        Err(error) => {
            if let Err(rollback_error) = transaction.rollback().await {
                return Err(error.context(format!(
                    "state migration failed and its rollback also failed: {rollback_error}"
                )));
            }
            return Err(error);
        }
    };

    if repaired_agent_path {
        warn!(
            migration_version = 48,
            "repaired released Frodex state migration collision"
        );
    }
    Ok(())
}

async fn migrate_state_database(pool: &SqlitePool, migrator: &Migrator) -> anyhow::Result<()> {
    migrate_state_database_with_hooks(pool, migrator, |_| Ok(()), || {}).await
}

#[cfg(test)]
pub(crate) async fn migrate_state_database_with_fault(
    pool: &SqlitePool,
    migrator: &Migrator,
    fault: StateMigrationStep,
) -> anyhow::Result<()> {
    migrate_state_database_with_hooks(
        pool,
        migrator,
        |completed| {
            if completed == fault {
                anyhow::bail!("injected state migration failure after {completed:?}");
            }
            Ok(())
        },
        || {},
    )
    .await
}

#[cfg(test)]
pub(crate) async fn migrate_state_database_with_contention_hook<F>(
    pool: &SqlitePool,
    migrator: &Migrator,
    on_contention: F,
) -> anyhow::Result<()>
where
    F: FnMut(),
{
    migrate_state_database_with_hooks(pool, migrator, |_| Ok(()), on_contention).await
}

#[derive(Clone, Copy)]
struct RuntimeDbSpec {
    label: &'static str,
    filename: &'static str,
    kind: DbKind,
    open_phase: &'static str,
    migrate_phase: &'static str,
}

impl RuntimeDbSpec {
    fn path(self, codex_home: &Path) -> PathBuf {
        codex_home.join(self.filename)
    }
}

const STATE_DB: RuntimeDbSpec = RuntimeDbSpec {
    label: "state DB",
    filename: STATE_DB_FILENAME,
    kind: DbKind::State,
    open_phase: "open_state",
    migrate_phase: "migrate_state",
};

const LOGS_DB: RuntimeDbSpec = RuntimeDbSpec {
    label: "log DB",
    filename: LOGS_DB_FILENAME,
    kind: DbKind::Logs,
    open_phase: "open_logs",
    migrate_phase: "migrate_logs",
};

const GOALS_DB: RuntimeDbSpec = RuntimeDbSpec {
    label: "goals DB",
    filename: GOALS_DB_FILENAME,
    kind: DbKind::Goals,
    open_phase: "open_goals",
    migrate_phase: "migrate_goals",
};

const MEMORIES_DB: RuntimeDbSpec = RuntimeDbSpec {
    label: "memories DB",
    filename: MEMORIES_DB_FILENAME,
    kind: DbKind::Memories,
    open_phase: "open_memories",
    migrate_phase: "migrate_memories",
};

const QUEUE_DB: RuntimeDbSpec = RuntimeDbSpec {
    label: "queue DB",
    filename: QUEUE_DB_FILENAME,
    kind: DbKind::Queue,
    open_phase: "open_queue",
    migrate_phase: "migrate_queue",
};

const THREAD_HISTORY_DB: RuntimeDbSpec = RuntimeDbSpec {
    label: "thread history DB",
    filename: THREAD_HISTORY_DB_FILENAME,
    kind: DbKind::ThreadHistory,
    open_phase: "open_thread_history",
    migrate_phase: "migrate_thread_history",
};

const RUNTIME_DBS: [RuntimeDbSpec; 6] = [
    STATE_DB,
    LOGS_DB,
    GOALS_DB,
    MEMORIES_DB,
    QUEUE_DB,
    THREAD_HISTORY_DB,
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeDbPath {
    pub label: &'static str,
    pub path: PathBuf,
}

/// Resolved configuration shared by all Codex SQLite connections.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqliteConfig {
    sqlite_home: AbsolutePathBuf,
}

impl SqliteConfig {
    pub fn from_sqlite_home(sqlite_home: AbsolutePathBuf) -> Self {
        Self { sqlite_home }
    }

    pub fn new_for_testing(sqlite_home: AbsolutePathBuf) -> Self {
        Self::from_sqlite_home(sqlite_home)
    }

    pub fn home(&self) -> &Path {
        self.sqlite_home.as_path()
    }

    /// Return the path to the primary state database.
    pub fn state_db_path(&self) -> PathBuf {
        STATE_DB.path(self.home())
    }

    /// Return the path to the logs database.
    pub fn logs_db_path(&self) -> PathBuf {
        LOGS_DB.path(self.home())
    }

    /// Return the path to the goals database.
    pub fn goals_db_path(&self) -> PathBuf {
        GOALS_DB.path(self.home())
    }

    /// Return the path to the memories database.
    pub fn memories_db_path(&self) -> PathBuf {
        MEMORIES_DB.path(self.home())
    }

    /// Return the path to the durable user-message queue database.
    pub fn queue_db_path(&self) -> PathBuf {
        QUEUE_DB.path(self.home())
    }

    /// Return the path to the paginated thread-history database.
    pub fn thread_history_db_path(&self) -> PathBuf {
        THREAD_HISTORY_DB.path(self.home())
    }

    /// Return the paths to every database managed by the state runtime.
    pub fn runtime_db_paths(&self) -> Vec<RuntimeDbPath> {
        RUNTIME_DBS
            .iter()
            .map(|spec| RuntimeDbPath {
                label: spec.label,
                path: spec.path(self.home()),
            })
            .collect()
    }

    pub(super) async fn open_state_db(
        &self,
        migrator: &Migrator,
        telemetry_override: Option<&dyn DbTelemetry>,
    ) -> anyhow::Result<SqlitePool> {
        // New state DBs should use incremental auto-vacuum, but retrofitting an
        // existing DB requires a full VACUUM. Do not attempt that during process
        // startup: it is maintenance work that can contend with foreground writers.
        self.open_runtime_db(STATE_DB, migrator, telemetry_override)
            .await
    }

    pub(super) async fn open_logs_db(
        &self,
        migrator: &Migrator,
        telemetry_override: Option<&dyn DbTelemetry>,
    ) -> anyhow::Result<SqlitePool> {
        self.open_runtime_db(LOGS_DB, migrator, telemetry_override)
            .await
    }

    pub(super) async fn open_goals_db(
        &self,
        migrator: &Migrator,
        telemetry_override: Option<&dyn DbTelemetry>,
    ) -> anyhow::Result<SqlitePool> {
        self.open_runtime_db(GOALS_DB, migrator, telemetry_override)
            .await
    }

    pub(super) async fn open_memories_db(
        &self,
        migrator: &Migrator,
        telemetry_override: Option<&dyn DbTelemetry>,
    ) -> anyhow::Result<SqlitePool> {
        self.open_runtime_db(MEMORIES_DB, migrator, telemetry_override)
            .await
    }

    pub(super) async fn open_queue_db(
        &self,
        migrator: &Migrator,
        telemetry_override: Option<&dyn DbTelemetry>,
    ) -> anyhow::Result<SqlitePool> {
        self.open_runtime_db(QUEUE_DB, migrator, telemetry_override)
            .await
    }

    pub(super) async fn open_thread_history_db(
        &self,
        migrator: &Migrator,
        telemetry_override: Option<&dyn DbTelemetry>,
    ) -> anyhow::Result<SqlitePool> {
        self.open_runtime_db(THREAD_HISTORY_DB, migrator, telemetry_override)
            .await
    }

    async fn open_runtime_db(
        &self,
        spec: RuntimeDbSpec,
        migrator: &Migrator,
        telemetry_override: Option<&dyn DbTelemetry>,
    ) -> anyhow::Result<SqlitePool> {
        let path = spec.path(self.home());
        let started = Instant::now();
        let pool_result = loop {
            match self.open_read_write_pool(&path).await {
                Err(error)
                    if matches!(spec.kind, DbKind::State | DbKind::ThreadHistory)
                        && is_sqlite_writer_contention(&error) =>
                {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                result => break result.map_err(anyhow::Error::from),
            }
        };
        telemetry::record_init_result(
            telemetry_override,
            spec.kind,
            spec.open_phase,
            started.elapsed(),
            &pool_result,
        );
        let pool = pool_result.map_err(|source| {
            RuntimeDbInitError::new(spec.label, "open", path.as_path(), source)
        })?;
        let started = Instant::now();
        let migrate_result = async {
            if matches!(spec.kind, DbKind::State) {
                migrate_state_database(&pool, migrator).await
            } else if matches!(spec.kind, DbKind::ThreadHistory) {
                // Read applied versions only after reserving SQLite's writer. Otherwise two
                // app-servers can both decide migration 1 must create thread_turns.
                ensure_transactional_migrations(migrator)?;
                let mut transaction = begin_runtime_migration_transaction(&pool, || {}).await?;
                migrator
                    .run_direct(/*target*/ None, &mut *transaction, /*skip*/ false)
                    .await?;
                transaction.commit().await?;
                Ok(())
            } else {
                migrator.run(&pool).await.map_err(anyhow::Error::from)
            }
        }
        .await;
        telemetry::record_init_result(
            telemetry_override,
            spec.kind,
            spec.migrate_phase,
            started.elapsed(),
            &migrate_result,
        );
        if let Err(source) = migrate_result {
            pool.close().await;
            return Err(
                RuntimeDbInitError::new(spec.label, "migrate", path.as_path(), source).into(),
            );
        }
        Ok(pool)
    }

    /// Open a writable Codex SQLite database, creating it if necessary.
    pub async fn open_read_write_pool(&self, path: &Path) -> Result<SqlitePool, Error> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .auto_vacuum(SqliteAutoVacuum::Incremental)
            .busy_timeout(Duration::from_secs(5))
            .log_statements(LevelFilter::Off);
        SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
    }

    /// Open an existing Codex SQLite database without creating or modifying it.
    pub async fn open_read_only_pool(
        &self,
        path: &Path,
        busy_timeout: Option<Duration>,
    ) -> Result<SqlitePool, Error> {
        let mut options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false)
            .read_only(true)
            .log_statements(LevelFilter::Off);
        if let Some(busy_timeout) = busy_timeout {
            options = options.busy_timeout(busy_timeout);
        }
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
    }
}

#[cfg(test)]
#[path = "thread_history_migration_tests.rs"]
mod thread_history_migration_tests;
