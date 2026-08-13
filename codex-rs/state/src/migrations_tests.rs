use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use sqlx::Connection;
use sqlx::Row;
use sqlx::SqlSafeStr;
use sqlx::migrate::MigrateError;
use sqlx::migrate::Migration;
use sqlx::migrate::MigrationType;
use sqlx::migrate::Migrator;
use std::borrow::Cow;

use super::FRODEX_GOAL_SUPERVISOR_MIGRATION_CHECKSUM;
use super::FRODEX_GOAL_SUPERVISOR_MIGRATION_DESCRIPTION;
use super::STATE_MIGRATOR;
use super::THREAD_HISTORY_MIGRATOR;
use super::repair_frodex_agent_path_migration_collision as repair_frodex_agent_path_migration_collision_on;
use super::repair_frodex_goal_supervisor_state_migration as repair_frodex_goal_supervisor_state_migration_on;
use super::repair_legacy_recency_migration_version as repair_legacy_recency_migration_version_on;
use super::runtime_state_migrator;
use crate::PINNED_THREAD_SECTION_ID;
use crate::PINNED_THREAD_SECTION_NAME;
use crate::sqlite::StateMigrationStep;
use crate::sqlite::migrate_state_database_with_fault;

const CUSTOM_THREAD_SECTION_ID: &str = "01984de2-8f74-7c91-a3b2-5c5e937cf317";
const FRODEX_GOAL_SUPERVISOR_MIGRATION_SQL: &str = "CREATE TABLE thread_goal_supervisor_state (\n    thread_id TEXT PRIMARY KEY NOT NULL REFERENCES threads(id) ON DELETE CASCADE,\n    goal_id TEXT NOT NULL,\n    snoozed_until_ms INTEGER,\n    updated_at_ms INTEGER NOT NULL\n);\n";
const STATE_MIGRATION_SUBPROCESS_HOME: &str = "CODEX_STATE_MIGRATION_SUBPROCESS_HOME";

async fn repair_frodex_goal_supervisor_state_migration(
    pool: &sqlx::SqlitePool,
) -> anyhow::Result<()> {
    let mut connection = pool.acquire().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    repair_frodex_goal_supervisor_state_migration_on(&mut transaction).await?;
    transaction.commit().await?;
    Ok(())
}

async fn repair_frodex_agent_path_migration_collision(
    pool: &sqlx::SqlitePool,
    migrator: &Migrator,
) -> anyhow::Result<()> {
    let mut connection = pool.acquire().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    repair_frodex_agent_path_migration_collision_on(&mut transaction, migrator).await?;
    transaction.commit().await?;
    Ok(())
}

async fn repair_legacy_recency_migration_version(
    pool: &sqlx::SqlitePool,
    migrator: &Migrator,
) -> anyhow::Result<()> {
    let mut connection = pool.acquire().await?;
    repair_legacy_recency_migration_version_on(&mut connection, migrator).await
}

fn migrator_through(version: i64) -> Migrator {
    Migrator {
        migrations: Cow::Owned(
            STATE_MIGRATOR
                .migrations
                .iter()
                .filter(|migration| migration.version <= version)
                .cloned()
                .collect(),
        ),
        ignore_missing: STATE_MIGRATOR.ignore_missing,
        locking: STATE_MIGRATOR.locking,
        table_name: STATE_MIGRATOR.table_name.clone(),
        create_schemas: STATE_MIGRATOR.create_schemas.clone(),
        no_tx: STATE_MIGRATOR.no_tx,
    }
}

fn migrator_with_frodex_goal_supervisor_collision(version: i64, sql: &'static str) -> Migrator {
    let mut migrations = STATE_MIGRATOR
        .migrations
        .iter()
        .filter(|migration| migration.version < version)
        .cloned()
        .collect::<Vec<_>>();
    let migration_type = STATE_MIGRATOR
        .migrations
        .first()
        .expect("state migrations should not be empty")
        .migration_type;
    migrations.push(Migration::new(
        version,
        Cow::Borrowed(FRODEX_GOAL_SUPERVISOR_MIGRATION_DESCRIPTION),
        migration_type,
        sqlx::SqlStr::from_static(sql),
        /*no_tx*/ false,
    ));
    Migrator::with_migrations(migrations)
}

async fn apply_legacy_recency_migration(pool: &sqlx::SqlitePool) {
    migrator_through(/*version*/ 37)
        .run(pool)
        .await
        .expect("pre-recency migrations should apply");
    let recency_migration = STATE_MIGRATOR
        .migrations
        .iter()
        .find(|migration| migration.version == 39)
        .expect("recency migration should exist");
    let mut legacy_migrations = STATE_MIGRATOR
        .migrations
        .iter()
        .filter(|migration| migration.version <= 37)
        .cloned()
        .collect::<Vec<_>>();
    legacy_migrations.push(Migration::new(
        38,
        recency_migration.description.clone(),
        recency_migration.migration_type,
        recency_migration.sql.clone(),
        recency_migration.no_tx,
    ));
    Migrator::with_migrations(legacy_migrations)
        .run(pool)
        .await
        .expect("legacy recency migration should apply as version 38");
}

async fn new_migration_test_pool() -> (std::path::PathBuf, crate::SqliteConfig, sqlx::SqlitePool) {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let pool = sqlite
        .open_read_write_pool(&sqlite.state_db_path())
        .await
        .expect("sqlite database should open");
    (sqlite_home, sqlite, pool)
}

async fn state_database_signature(
    pool: &sqlx::SqlitePool,
) -> (
    Vec<(String, String, String)>,
    Vec<(i64, String, bool, Vec<u8>)>,
) {
    let schema = sqlx::query_as::<_, (String, String, String)>(
        r#"
SELECT type, name, COALESCE(sql, '')
FROM sqlite_schema
WHERE name NOT LIKE 'sqlite_autoindex_%'
ORDER BY type, name
        "#,
    )
    .fetch_all(pool)
    .await
    .expect("state schema should load");
    let ledger_exists = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = '_sqlx_migrations'",
    )
    .fetch_one(pool)
    .await
    .expect("migration ledger existence should load")
        != 0;
    let ledger = if ledger_exists {
        sqlx::query_as::<_, (i64, String, bool, Vec<u8>)>(
            "SELECT version, description, success, checksum FROM _sqlx_migrations ORDER BY version",
        )
        .fetch_all(pool)
        .await
        .expect("migration ledger should load")
    } else {
        Vec::new()
    };
    (schema, ledger)
}

#[tokio::test]
async fn repairs_exact_frodex_goal_supervisor_migration_33_and_34() {
    for version in [33_i64, 34_i64] {
        let (sqlite_home, _sqlite, pool) = new_migration_test_pool().await;
        let legacy_migrator = migrator_with_frodex_goal_supervisor_collision(
            version,
            FRODEX_GOAL_SUPERVISOR_MIGRATION_SQL,
        );
        let legacy = legacy_migrator
            .migrations
            .last()
            .expect("legacy migration should exist");
        assert_eq!(
            legacy.checksum.as_ref(),
            FRODEX_GOAL_SUPERVISOR_MIGRATION_CHECKSUM
        );
        legacy_migrator
            .run(&pool)
            .await
            .expect("released Frodex migration should apply");

        repair_frodex_goal_supervisor_state_migration(&pool)
            .await
            .expect("released Frodex collision should repair");
        STATE_MIGRATOR
            .run(&pool)
            .await
            .expect("official migrations should apply after repair");

        let applied = sqlx::query_as::<_, (i64, String, Vec<u8>)>(
            "SELECT version, description, checksum FROM _sqlx_migrations WHERE version IN (33, 34) ORDER BY version",
        )
        .fetch_all(&pool)
        .await
        .expect("official migration rows should load");
        let expected = STATE_MIGRATOR
            .migrations
            .iter()
            .filter(|migration| matches!(migration.version, 33 | 34))
            .map(|migration| {
                (
                    migration.version,
                    migration.description.to_string(),
                    migration.checksum.to_vec(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(applied, expected);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'thread_goal_supervisor_state'",
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            1,
            "the source table remains until goals transfer"
        );

        pool.close().await;
        std::fs::remove_dir_all(sqlite_home).expect("sqlite home should be removed");
    }
}

#[tokio::test]
async fn concurrent_state_starts_repair_goal_supervisor_collision_once() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let first_sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let second_sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let legacy_pool = first_sqlite
        .open_read_write_pool(&first_sqlite.state_db_path())
        .await
        .expect("legacy state database should open");
    migrator_with_frodex_goal_supervisor_collision(
        /*version*/ 33,
        FRODEX_GOAL_SUPERVISOR_MIGRATION_SQL,
    )
    .run(&legacy_pool)
    .await
    .expect("released Frodex migration should apply");
    legacy_pool.close().await;

    let first_migrator = runtime_state_migrator();
    let second_migrator = runtime_state_migrator();
    let (first, second) = tokio::join!(
        first_sqlite.open_state_db(&first_migrator, /*telemetry_override*/ None),
        second_sqlite.open_state_db(&second_migrator, /*telemetry_override*/ None),
    );
    let first = first.expect("first repaired state open should succeed");
    let second = second.expect("second repaired state open should succeed");
    let official_descriptions = sqlx::query_scalar::<_, String>(
        "SELECT description FROM _sqlx_migrations WHERE version IN (33, 34) ORDER BY version",
    )
    .fetch_all(&first)
    .await
    .expect("official migration descriptions should load");
    assert_eq!(
        official_descriptions,
        vec![
            "thread goal stopped statuses".to_string(),
            "drop thread goals".to_string(),
        ]
    );
    let source_table_exists = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'thread_goal_supervisor_state'",
    )
    .fetch_one(&first)
    .await
    .expect("legacy source table should count");
    assert_eq!(source_table_exists, 1);
    assert!(!sqlite_home.join(".state_5.sqlite.migration.lock").exists());
    first.close().await;
    second.close().await;
}

#[tokio::test]
async fn concurrent_state_start_waits_past_busy_timeout_for_writer() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let holder_sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let waiting_sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let holder_pool = holder_sqlite
        .open_state_db(&runtime_state_migrator(), /*telemetry_override*/ None)
        .await
        .expect("initial state database should migrate");
    let waiting_pool = waiting_sqlite
        .open_read_write_pool(&waiting_sqlite.state_db_path())
        .await
        .expect("waiting state pool should open before migration contention");
    let mut holder_connection = holder_pool
        .acquire()
        .await
        .expect("holder connection should open");
    let holder_transaction = holder_connection
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("holder should acquire the writer slot");
    let waiting_migrator = runtime_state_migrator();
    let waiting_pool_for_migration = waiting_pool.clone();
    let contention_seen = std::sync::Arc::new(tokio::sync::Notify::new());
    let contention_seen_by_task = contention_seen.clone();
    let waiting_open = tokio::spawn(async move {
        crate::sqlite::migrate_state_database_with_contention_hook(
            &waiting_pool_for_migration,
            &waiting_migrator,
            || contention_seen_by_task.notify_one(),
        )
        .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(7),
        contention_seen.notified(),
    )
    .await
    .expect("waiting state migration should observe writer contention");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    holder_transaction
        .commit()
        .await
        .expect("holder should release the writer slot");

    tokio::time::timeout(std::time::Duration::from_secs(3), waiting_open)
        .await
        .expect("waiting state open should retry after its configured busy timeout")
        .expect("waiting state task should complete")
        .expect("waiting state open should succeed");
    waiting_pool.close().await;
    drop(holder_connection);
    holder_pool.close().await;
}

#[tokio::test]
async fn state_migration_subprocess_opens_database() {
    let Some(sqlite_home) = std::env::var_os(STATE_MIGRATION_SUBPROCESS_HOME) else {
        return;
    };
    let sqlite_home = std::path::PathBuf::from(sqlite_home);
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let pool = sqlite
        .open_state_db(&runtime_state_migrator(), /*telemetry_override*/ None)
        .await
        .expect("subprocess state open should succeed after writer release");
    pool.close().await;
}

#[tokio::test]
async fn concurrent_process_state_start_waits_for_writer_release() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let pool = sqlite
        .open_state_db(&runtime_state_migrator(), /*telemetry_override*/ None)
        .await
        .expect("initial state database should migrate");
    let mut connection = pool.acquire().await.expect("holder connection should open");
    let transaction = connection
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("holder should acquire the writer slot");
    let child = std::process::Command::new(
        std::env::current_exe().expect("state test executable should resolve"),
    )
    .arg("--exact")
    .arg("migrations::tests::state_migration_subprocess_opens_database")
    .arg("--nocapture")
    .env(STATE_MIGRATION_SUBPROCESS_HOME, &sqlite_home)
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::piped())
    .spawn()
    .expect("state migration subprocess should spawn");

    tokio::time::sleep(std::time::Duration::from_millis(5_100)).await;
    transaction
        .commit()
        .await
        .expect("holder should release the writer slot");
    drop(connection);
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        tokio::task::spawn_blocking(move || child.wait_with_output()),
    )
    .await
    .expect("subprocess should finish after writer release")
    .expect("subprocess waiter should complete")
    .expect("subprocess output should load");
    assert!(
        output.status.success(),
        "subprocess failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    pool.close().await;
}

#[tokio::test]
async fn frodex_goal_supervisor_migration_repair_rejects_unknown_checksum_without_mutation() {
    let (sqlite_home, _sqlite, pool) = new_migration_test_pool().await;
    let legacy_migrator = migrator_with_frodex_goal_supervisor_collision(
        /*version*/ 33,
        "CREATE TABLE thread_goal_supervisor_state (\n    thread_id TEXT PRIMARY KEY NOT NULL REFERENCES threads(id) ON DELETE CASCADE,\n    goal_id TEXT NOT NULL,\n    snoozed_until_ms INTEGER,\n    updated_at_ms INTEGER NOT NULL\n); \n",
    );
    legacy_migrator
        .run(&pool)
        .await
        .expect("unknown legacy migration should apply");
    let before = sqlx::query_as::<_, (String, bool, Vec<u8>)>(
        "SELECT description, success, checksum FROM _sqlx_migrations WHERE version = 33",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let error = repair_frodex_goal_supervisor_state_migration(&pool)
        .await
        .expect_err("unknown checksum must fail closed");
    assert!(
        error
            .to_string()
            .contains("does not match the released Frodex migration")
    );
    let after = sqlx::query_as::<_, (String, bool, Vec<u8>)>(
        "SELECT description, success, checksum FROM _sqlx_migrations WHERE version = 33",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(after, before);

    pool.close().await;
    std::fs::remove_dir_all(sqlite_home).expect("sqlite home should be removed");
}

#[tokio::test]
async fn frodex_goal_supervisor_migration_repair_rejects_missing_or_incompatible_schema() {
    for mutation in [
        "DROP TABLE thread_goal_supervisor_state",
        "ALTER TABLE thread_goal_supervisor_state ADD COLUMN extra TEXT",
    ] {
        let (sqlite_home, _sqlite, pool) = new_migration_test_pool().await;
        migrator_with_frodex_goal_supervisor_collision(
            /*version*/ 33,
            FRODEX_GOAL_SUPERVISOR_MIGRATION_SQL,
        )
        .run(&pool)
        .await
        .expect("released Frodex migration should apply");
        sqlx::query(mutation).execute(&pool).await.unwrap();

        repair_frodex_goal_supervisor_state_migration(&pool)
            .await
            .expect_err("incompatible schema must fail closed");
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 33 AND description = ?",
            )
            .bind(FRODEX_GOAL_SUPERVISOR_MIGRATION_DESCRIPTION)
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );

        pool.close().await;
        std::fs::remove_dir_all(sqlite_home).expect("sqlite home should be removed");
    }
}

#[tokio::test]
async fn frodex_goal_supervisor_migration_repair_rejects_wrong_key_and_auxiliary_objects() {
    let incompatible_schemas = [
        r#"
CREATE TABLE thread_goal_supervisor_state (
    thread_id TEXT NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    goal_id TEXT PRIMARY KEY NOT NULL,
    snoozed_until_ms INTEGER,
    updated_at_ms INTEGER NOT NULL
)
        "#,
        r#"
CREATE TABLE thread_goal_supervisor_state (
    thread_id TEXT PRIMARY KEY NOT NULL REFERENCES threads(id) ON DELETE SET NULL,
    goal_id TEXT NOT NULL,
    snoozed_until_ms INTEGER,
    updated_at_ms INTEGER NOT NULL
)
        "#,
    ];
    for schema in incompatible_schemas {
        let (sqlite_home, _sqlite, pool) = new_migration_test_pool().await;
        migrator_with_frodex_goal_supervisor_collision(
            /*version*/ 33,
            FRODEX_GOAL_SUPERVISOR_MIGRATION_SQL,
        )
        .run(&pool)
        .await
        .unwrap();
        sqlx::query("DROP TABLE thread_goal_supervisor_state")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(schema.to_string()))
            .execute(&pool)
            .await
            .unwrap();
        repair_frodex_goal_supervisor_state_migration(&pool)
            .await
            .expect_err("wrong keys must fail closed");
        pool.close().await;
        std::fs::remove_dir_all(sqlite_home).unwrap();
    }

    let (sqlite_home, _sqlite, pool) = new_migration_test_pool().await;
    migrator_with_frodex_goal_supervisor_collision(
        /*version*/ 33,
        FRODEX_GOAL_SUPERVISOR_MIGRATION_SQL,
    )
    .run(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER incompatible_supervisor_trigger AFTER INSERT ON thread_goal_supervisor_state BEGIN SELECT 1; END",
    )
    .execute(&pool)
    .await
    .unwrap();
    repair_frodex_goal_supervisor_state_migration(&pool)
        .await
        .expect_err("auxiliary triggers must fail closed");
    pool.close().await;
    std::fs::remove_dir_all(sqlite_home).unwrap();
}

#[tokio::test]
async fn frodex_goal_supervisor_migration_repair_rejects_unreleased_table_constraints() {
    let incompatible_schemas = [
        r#"
CREATE TABLE thread_goal_supervisor_state (
    thread_id TEXT PRIMARY KEY NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    goal_id TEXT UNIQUE NOT NULL,
    snoozed_until_ms INTEGER,
    updated_at_ms INTEGER NOT NULL
)
        "#,
        r#"
CREATE TABLE thread_goal_supervisor_state (
    thread_id TEXT PRIMARY KEY NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    goal_id TEXT NOT NULL CHECK(length(goal_id) > 0),
    snoozed_until_ms INTEGER,
    updated_at_ms INTEGER NOT NULL
)
        "#,
        r#"
CREATE TABLE thread_goal_supervisor_state (
    thread_id TEXT PRIMARY KEY NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    goal_id TEXT COLLATE NOCASE NOT NULL,
    snoozed_until_ms INTEGER,
    updated_at_ms INTEGER NOT NULL
)
        "#,
        r#"
CREATE TABLE thread_goal_supervisor_state (
    thread_id TEXT PRIMARY KEY NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    goal_id TEXT NOT NULL,
    snoozed_until_ms INTEGER,
    updated_at_ms INTEGER NOT NULL
)
STRICT
        "#,
    ];
    for schema in incompatible_schemas {
        let (sqlite_home, _sqlite, pool) = new_migration_test_pool().await;
        migrator_with_frodex_goal_supervisor_collision(
            /*version*/ 33,
            FRODEX_GOAL_SUPERVISOR_MIGRATION_SQL,
        )
        .run(&pool)
        .await
        .expect("released Frodex migration should apply");
        sqlx::query("DROP TABLE thread_goal_supervisor_state")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(schema.to_string()))
            .execute(&pool)
            .await
            .expect("incompatible table should be valid SQLite");

        repair_frodex_goal_supervisor_state_migration(&pool)
            .await
            .expect_err("unreleased constraints must fail closed");
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 33 AND description = ?",
            )
            .bind(FRODEX_GOAL_SUPERVISOR_MIGRATION_DESCRIPTION)
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );
        pool.close().await;
        std::fs::remove_dir_all(sqlite_home).unwrap();
    }
}

#[tokio::test]
async fn frodex_goal_supervisor_migration_repair_rejects_multiple_candidates() {
    let (sqlite_home, _sqlite, pool) = new_migration_test_pool().await;
    migrator_with_frodex_goal_supervisor_collision(
        /*version*/ 33,
        FRODEX_GOAL_SUPERVISOR_MIGRATION_SQL,
    )
    .run(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES (34, ?, 1, ?, 0)",
    )
    .bind(FRODEX_GOAL_SUPERVISOR_MIGRATION_DESCRIPTION)
    .bind(FRODEX_GOAL_SUPERVISOR_MIGRATION_CHECKSUM.as_slice())
    .execute(&pool)
    .await
    .unwrap();
    repair_frodex_goal_supervisor_state_migration(&pool)
        .await
        .expect_err("multiple candidates must fail closed");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM _sqlx_migrations WHERE description = ?",
        )
        .bind(FRODEX_GOAL_SUPERVISOR_MIGRATION_DESCRIPTION)
        .fetch_one(&pool)
        .await
        .unwrap(),
        2
    );
    pool.close().await;
    std::fs::remove_dir_all(sqlite_home).unwrap();
}

#[tokio::test]
async fn frodex_goal_supervisor_migration_repair_rejects_unsuccessful_ledger_row() {
    let (sqlite_home, _sqlite, pool) = new_migration_test_pool().await;
    migrator_with_frodex_goal_supervisor_collision(
        /*version*/ 33,
        FRODEX_GOAL_SUPERVISOR_MIGRATION_SQL,
    )
    .run(&pool)
    .await
    .expect("released Frodex migration should apply");
    sqlx::query("UPDATE _sqlx_migrations SET success = 0 WHERE version = 33")
        .execute(&pool)
        .await
        .unwrap();

    repair_frodex_goal_supervisor_state_migration(&pool)
        .await
        .expect_err("unsuccessful ledger row must fail closed");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT success FROM _sqlx_migrations WHERE version = 33")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );

    pool.close().await;
    std::fs::remove_dir_all(sqlite_home).expect("sqlite home should be removed");
}

#[tokio::test]
async fn frodex_goal_supervisor_migration_repair_authenticates_complete_predecessor_ledger() {
    for mutation in [
        "UPDATE _sqlx_migrations SET checksum = X'00' WHERE version = 32",
        "UPDATE _sqlx_migrations SET success = 0 WHERE version = 32",
        "DELETE FROM _sqlx_migrations WHERE version = 32",
    ] {
        let (sqlite_home, _sqlite, pool) = new_migration_test_pool().await;
        migrator_with_frodex_goal_supervisor_collision(
            /*version*/ 33,
            FRODEX_GOAL_SUPERVISOR_MIGRATION_SQL,
        )
        .run(&pool)
        .await
        .expect("released Frodex migration should apply");
        sqlx::query(mutation).execute(&pool).await.unwrap();

        repair_frodex_goal_supervisor_state_migration(&pool)
            .await
            .expect_err("an unauthenticated predecessor ledger must fail closed");
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 33 AND description = ?",
            )
            .bind(FRODEX_GOAL_SUPERVISOR_MIGRATION_DESCRIPTION)
            .fetch_one(&pool)
            .await
            .unwrap(),
            1,
            "the collision row must remain unchanged"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'thread_goal_supervisor_state'",
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            1,
            "the source table must remain unchanged"
        );
        pool.close().await;
        std::fs::remove_dir_all(sqlite_home).expect("sqlite home should be removed");
    }
}

fn released_frodex_state_migrator() -> Migrator {
    let mut migrations = STATE_MIGRATOR
        .migrations
        .iter()
        .filter(|migration| migration.version <= 47)
        .cloned()
        .collect::<Vec<_>>();
    migrations.push(Migration::new(
        48,
        Cow::Borrowed("threads agent path index"),
        MigrationType::Simple,
        "CREATE INDEX idx_threads_agent_path\n    ON threads(agent_path)\n    WHERE agent_path IS NOT NULL;\n"
            .into_sql_str(),
        false,
    ));
    Migrator {
        migrations: Cow::Owned(migrations),
        ignore_missing: STATE_MIGRATOR.ignore_missing,
        locking: STATE_MIGRATOR.locking,
        table_name: STATE_MIGRATOR.table_name.clone(),
        create_schemas: STATE_MIGRATOR.create_schemas.clone(),
        no_tx: STATE_MIGRATOR.no_tx,
    }
}

#[tokio::test]
async fn repairs_released_frodex_migration_48_for_upstream_compatibility() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let state_path = sqlite.state_db_path();
    let legacy_pool = sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("legacy state database should open");
    released_frodex_state_migrator()
        .run(&legacy_pool)
        .await
        .expect("released Frodex migrations should apply");
    legacy_pool.close().await;

    let repaired_pool = sqlite
        .open_state_db(&runtime_state_migrator(), /*telemetry_override*/ None)
        .await
        .expect("released Frodex state database should repair");
    let applied_48 = sqlx::query_as::<_, (String, Vec<u8>)>(
        "SELECT description, checksum FROM _sqlx_migrations WHERE version = 48",
    )
    .fetch_one(&repaired_pool)
    .await
    .expect("migration 48 should be recorded");
    let upstream_48 = STATE_MIGRATOR
        .migrations
        .iter()
        .find(|migration| migration.version == 48)
        .expect("upstream migration 48 should be embedded");
    assert_eq!(
        applied_48,
        (
            upstream_48.description.to_string(),
            upstream_48.checksum.to_vec(),
        )
    );
    let appearance_shape = sqlx::query_as::<_, (String, i64, Option<String>, i64)>(
        r#"
SELECT type, "notnull", dflt_value, pk
FROM pragma_table_info('thread_sections')
WHERE name = 'appearance'
        "#,
    )
    .fetch_one(&repaired_pool)
    .await
    .expect("upstream appearance column should exist");
    assert_eq!(appearance_shape, ("TEXT".to_string(), 0, None, 0));
    let agent_path_indexes = sqlx::query_scalar::<_, String>(
        r#"
SELECT name
FROM pragma_index_list('threads')
WHERE name IN ('idx_threads_agent_path', 'frodex_idx_threads_agent_path')
ORDER BY name
        "#,
    )
    .fetch_all(&repaired_pool)
    .await
    .expect("agent-path indexes should load");
    assert!(agent_path_indexes.is_empty());
    let integrity = sqlx::query_scalar::<_, String>("PRAGMA quick_check")
        .fetch_one(&repaired_pool)
        .await
        .expect("repaired database should pass quick_check");
    assert_eq!(integrity, "ok");
    STATE_MIGRATOR
        .run(&repaired_pool)
        .await
        .expect("official migrator should accept the repaired database");
    repaired_pool.close().await;
}

#[tokio::test]
async fn official_migration_48_then_concurrent_frodex_starts_are_idempotent() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let first_sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let second_sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let state_path = first_sqlite.state_db_path();
    let official_pool = first_sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("official state database should open");
    STATE_MIGRATOR
        .run(&official_pool)
        .await
        .expect("official migrations should apply");
    official_pool.close().await;

    let first_migrator = runtime_state_migrator();
    let second_migrator = runtime_state_migrator();
    let (first, second) = tokio::join!(
        first_sqlite.open_state_db(&first_migrator, /*telemetry_override*/ None),
        second_sqlite.open_state_db(&second_migrator, /*telemetry_override*/ None),
    );
    let first = first.expect("first Frodex state open should succeed");
    let second = second.expect("second Frodex state open should succeed");
    let matching_indexes = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM pragma_index_list('threads') WHERE name IN ('idx_threads_agent_path', 'frodex_idx_threads_agent_path')",
    )
    .fetch_one(&first)
    .await
    .expect("agent-path indexes should count");
    assert_eq!(matching_indexes, 0);
    first.close().await;
    second.close().await;
}

#[tokio::test]
async fn concurrent_frodex_starts_repair_released_migration_48_once() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let first_sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let second_sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let state_path = first_sqlite.state_db_path();
    let legacy_pool = first_sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("legacy state database should open");
    released_frodex_state_migrator()
        .run(&legacy_pool)
        .await
        .expect("released Frodex migrations should apply");
    legacy_pool.close().await;

    let first_migrator = runtime_state_migrator();
    let second_migrator = runtime_state_migrator();
    let (first, second) = tokio::join!(
        first_sqlite.open_state_db(&first_migrator, /*telemetry_override*/ None),
        second_sqlite.open_state_db(&second_migrator, /*telemetry_override*/ None),
    );
    let first = first.expect("first repaired Frodex state open should succeed");
    let second = second.expect("second repaired Frodex state open should succeed");
    let applied_description = sqlx::query_scalar::<_, String>(
        "SELECT description FROM _sqlx_migrations WHERE version = 48",
    )
    .fetch_one(&first)
    .await
    .expect("migration 48 should be recorded");
    assert_eq!(applied_description, "thread section appearance");
    let matching_indexes = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM pragma_index_list('threads') WHERE name IN ('idx_threads_agent_path', 'frodex_idx_threads_agent_path')",
    )
    .fetch_one(&first)
    .await
    .expect("agent-path indexes should count");
    assert_eq!(matching_indexes, 0);
    assert!(
        !sqlite_home.join(".state_5.sqlite.migration.lock").exists(),
        "state migration must not create a Frodex lock file"
    );
    first.close().await;
    second.close().await;
}

#[tokio::test]
async fn state_migration_faults_roll_back_compatibility_repairs_and_official_migrations() {
    for fault in [
        StateMigrationStep::GoalSupervisorCompatibility,
        StateMigrationStep::AgentPathCompatibility,
        StateMigrationStep::RecencyCompatibility,
        StateMigrationStep::OfficialMigrations,
    ] {
        let (sqlite_home, _sqlite, pool) = new_migration_test_pool().await;
        match fault {
            StateMigrationStep::GoalSupervisorCompatibility => {
                migrator_with_frodex_goal_supervisor_collision(
                    /*version*/ 33,
                    FRODEX_GOAL_SUPERVISOR_MIGRATION_SQL,
                )
                .run(&pool)
                .await
                .expect("released Frodex migration should apply");
            }
            StateMigrationStep::RecencyCompatibility => {
                apply_legacy_recency_migration(&pool).await;
            }
            StateMigrationStep::AgentPathCompatibility | StateMigrationStep::OfficialMigrations => {
                released_frodex_state_migrator()
                    .run(&pool)
                    .await
                    .expect("released Frodex migrations should apply");
            }
        }
        let before = state_database_signature(&pool).await;

        migrate_state_database_with_fault(&pool, &runtime_state_migrator(), fault)
            .await
            .expect_err("injected state migration failure should abort startup");

        assert_eq!(
            state_database_signature(&pool).await,
            before,
            "failure after {fault:?} must roll back every state migration change"
        );
        pool.close().await;
        std::fs::remove_dir_all(sqlite_home).expect("sqlite home should be removed");
    }
}

#[tokio::test]
async fn fresh_state_database_creates_no_frodex_migration_lock() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());

    let pool = sqlite
        .open_state_db(&runtime_state_migrator(), /*telemetry_override*/ None)
        .await
        .expect("fresh state database should migrate");

    assert!(!sqlite_home.join(".state_5.sqlite.migration.lock").exists());
    pool.close().await;
}

#[tokio::test]
async fn nontransactional_state_migrations_fail_before_schema_mutation() {
    for global_no_tx in [false, true] {
        let (sqlite_home, sqlite, pool) = new_migration_test_pool().await;
        let migration = Migration::new(
            1,
            Cow::Borrowed("nontransactional test migration"),
            MigrationType::Simple,
            "CREATE TABLE must_not_exist (id INTEGER PRIMARY KEY);".into_sql_str(),
            /*no_tx*/ !global_no_tx,
        );
        let mut migrator = Migrator::with_migrations(vec![migration]);
        migrator.no_tx = global_no_tx;
        let before = state_database_signature(&pool).await;

        let error = sqlite
            .open_state_db(&migrator, /*telemetry_override*/ None)
            .await
            .expect_err("nontransactional state migration should be rejected");

        assert!(
            error
                .to_string()
                .contains("must all support the startup transaction")
        );
        assert_eq!(state_database_signature(&pool).await, before);
        pool.close().await;
        std::fs::remove_dir_all(sqlite_home).expect("sqlite home should be removed");
    }
}

#[tokio::test]
async fn unknown_migration_48_checksum_still_fails_closed() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let state_path = sqlite.state_db_path();
    let pool = sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("state database should open");
    released_frodex_state_migrator()
        .run(&pool)
        .await
        .expect("released Frodex migrations should apply");
    sqlx::query("UPDATE _sqlx_migrations SET checksum = ? WHERE version = 48")
        .bind(vec![0_u8; 48])
        .execute(&pool)
        .await
        .expect("test checksum should update");
    pool.close().await;

    let error = sqlite
        .open_state_db(&runtime_state_migrator(), /*telemetry_override*/ None)
        .await
        .expect_err("unknown migration checksum should fail");
    assert!(error.chain().any(|source| matches!(
        source.downcast_ref::<MigrateError>(),
        Some(MigrateError::VersionMismatch(48))
    )));
}

#[tokio::test]
async fn unknown_migration_48_predecessor_fails_without_mutation() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let state_path = sqlite.state_db_path();
    let pool = sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("state database should open");
    released_frodex_state_migrator()
        .run(&pool)
        .await
        .expect("released Frodex migrations should apply");
    sqlx::query("UPDATE _sqlx_migrations SET checksum = ? WHERE version = 47")
        .bind(vec![0_u8; 48])
        .execute(&pool)
        .await
        .expect("test predecessor checksum should update");
    pool.close().await;

    sqlite
        .open_state_db(&runtime_state_migrator(), /*telemetry_override*/ None)
        .await
        .expect_err("unknown predecessor checksum should fail before repair");

    let pool = sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("failed state database should reopen");
    let appearance_columns = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM pragma_table_info('thread_sections') WHERE name = 'appearance'",
    )
    .fetch_one(&pool)
    .await
    .expect("appearance columns should count");
    let legacy_indexes = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM pragma_index_list('threads') WHERE name = 'idx_threads_agent_path'",
    )
    .fetch_one(&pool)
    .await
    .expect("legacy indexes should count");
    let description = sqlx::query_scalar::<_, String>(
        "SELECT description FROM _sqlx_migrations WHERE version = 48",
    )
    .fetch_one(&pool)
    .await
    .expect("legacy migration 48 should load");
    assert_eq!(appearance_columns, 0);
    assert_eq!(legacy_indexes, 1);
    assert_eq!(description, "threads agent path index");
    pool.close().await;
}

#[tokio::test]
async fn missing_goal_supervisor_predecessors_fail_without_mutation() {
    for corrupt_table in [false, true] {
        let sqlite_home = crate::runtime::test_support::unique_temp_dir();
        tokio::fs::create_dir_all(&sqlite_home)
            .await
            .expect("sqlite home should be created");
        let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
            let _ = std::fs::remove_dir_all(sqlite_home);
        });
        let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
        let state_path = sqlite.state_db_path();
        let pool = sqlite
            .open_read_write_pool(&state_path)
            .await
            .expect("state database should open");
        released_frodex_state_migrator()
            .run(&pool)
            .await
            .expect("released Frodex migrations should apply");
        if corrupt_table {
            sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 33")
                .execute(&pool)
                .await
                .expect("one goal supervisor predecessor row should be removed");
        } else {
            sqlx::query("DELETE FROM _sqlx_migrations WHERE version IN (33, 34)")
                .execute(&pool)
                .await
                .expect("both goal supervisor predecessor rows should be removed");
        }
        if corrupt_table {
            sqlx::query("CREATE TABLE thread_goal_supervisor_state (thread_id TEXT PRIMARY KEY)")
                .execute(&pool)
                .await
                .expect("incompatible legacy table should install");
        } else {
            sqlx::query(super::FRODEX_GOAL_SUPERVISOR_STATE_TABLE_SQL)
                .execute(&pool)
                .await
                .expect("released legacy table should install");
        }

        repair_frodex_agent_path_migration_collision(&pool, &runtime_state_migrator())
            .await
            .expect_err("unauthenticated predecessor exception should fail closed");
        let appearance_columns = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM pragma_table_info('thread_sections') WHERE name = 'appearance'",
        )
        .fetch_one(&pool)
        .await
        .expect("appearance columns should count");
        let legacy_indexes = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM pragma_index_list('threads') WHERE name = 'idx_threads_agent_path'",
        )
        .fetch_one(&pool)
        .await
        .expect("legacy indexes should count");
        let description = sqlx::query_scalar::<_, String>(
            "SELECT description FROM _sqlx_migrations WHERE version = 48",
        )
        .fetch_one(&pool)
        .await
        .expect("legacy migration 48 should load");
        assert_eq!(appearance_columns, 0);
        assert_eq!(legacy_indexes, 1);
        assert_eq!(description, "threads agent path index");
        pool.close().await;
    }
}

#[tokio::test]
async fn released_frodex_migration_48_with_missing_index_fails_closed() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let state_path = sqlite.state_db_path();
    let pool = sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("state database should open");
    released_frodex_state_migrator()
        .run(&pool)
        .await
        .expect("released Frodex migrations should apply");
    drop(pool);
    let pool = sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("released state database should reopen without stale schema cache");
    sqlx::query("DROP INDEX idx_threads_agent_path")
        .execute(&pool)
        .await
        .expect("test index should drop");
    let error = repair_frodex_agent_path_migration_collision(&pool, &runtime_state_migrator())
        .await
        .expect_err("missing legacy index should fail repair");
    assert!(
        error
            .to_string()
            .contains("refusing to repair Frodex migration 48")
    );
    pool.close().await;
}

#[tokio::test]
async fn released_frodex_migration_48_with_alternate_index_sql_fails_closed() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let state_path = sqlite.state_db_path();
    let pool = sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("state database should open");
    released_frodex_state_migrator()
        .run(&pool)
        .await
        .expect("released Frodex migrations should apply");
    drop(pool);
    let pool = sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("released state database should reopen without stale schema cache");
    sqlx::query("DROP INDEX idx_threads_agent_path")
        .execute(&pool)
        .await
        .expect("released index should drop");
    sqlx::query(
        "CREATE INDEX idx_threads_agent_path ON threads(agent_path DESC) WHERE agent_path IS NOT NULL",
    )
    .execute(&pool)
    .await
    .expect("alternate index should install");

    repair_frodex_agent_path_migration_collision(&pool, &runtime_state_migrator())
        .await
        .expect_err("alternate index SQL should fail closed");
    let appearance_columns = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM pragma_table_info('thread_sections') WHERE name = 'appearance'",
    )
    .fetch_one(&pool)
    .await
    .expect("appearance columns should count");
    let index_sql = sqlx::query_scalar::<_, String>(
        "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = 'idx_threads_agent_path'",
    )
    .fetch_one(&pool)
    .await
    .expect("alternate index SQL should load");
    let description = sqlx::query_scalar::<_, String>(
        "SELECT description FROM _sqlx_migrations WHERE version = 48",
    )
    .fetch_one(&pool)
    .await
    .expect("legacy migration 48 should load");
    assert_eq!(appearance_columns, 0);
    assert!(index_sql.contains("agent_path DESC"));
    assert_eq!(description, "threads agent path index");
    pool.close().await;
}

#[tokio::test]
async fn released_frodex_migration_48_with_upstream_column_repairs_ledger_only() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let state_path = sqlite.state_db_path();
    let pool = sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("state database should open");
    released_frodex_state_migrator()
        .run(&pool)
        .await
        .expect("released Frodex migrations should apply");
    sqlx::query("ALTER TABLE thread_sections ADD COLUMN appearance TEXT")
        .execute(&pool)
        .await
        .expect("upstream column should be added to the fixture");

    repair_frodex_agent_path_migration_collision(&pool, &runtime_state_migrator())
        .await
        .expect("compatible upstream column should repair");
    STATE_MIGRATOR
        .run(&pool)
        .await
        .expect("official migrator should accept the repaired ledger");
    let description = sqlx::query_scalar::<_, String>(
        "SELECT description FROM _sqlx_migrations WHERE version = 48",
    )
    .fetch_one(&pool)
    .await
    .expect("migration 48 should load");
    assert_eq!(description, "thread section appearance");
    let matching_indexes = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM pragma_index_list('threads') WHERE name IN ('idx_threads_agent_path', 'frodex_idx_threads_agent_path')",
    )
    .fetch_one(&pool)
    .await
    .expect("agent-path indexes should count");
    assert_eq!(matching_indexes, 0);
    pool.close().await;
}

#[tokio::test]
async fn released_frodex_migration_48_with_wrong_upstream_column_fails_closed() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let state_path = sqlite.state_db_path();
    let pool = sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("state database should open");
    released_frodex_state_migrator()
        .run(&pool)
        .await
        .expect("released Frodex migrations should apply");
    sqlx::query("ALTER TABLE thread_sections ADD COLUMN appearance INTEGER NOT NULL DEFAULT 0")
        .execute(&pool)
        .await
        .expect("wrong upstream column should be added to the fixture");

    repair_frodex_agent_path_migration_collision(&pool, &runtime_state_migrator())
        .await
        .expect_err("wrong upstream column should fail repair");
    let description = sqlx::query_scalar::<_, String>(
        "SELECT description FROM _sqlx_migrations WHERE version = 48",
    )
    .fetch_one(&pool)
    .await
    .expect("legacy migration 48 should load");
    let indexes = sqlx::query_scalar::<_, String>(
        "SELECT name FROM pragma_index_list('threads') WHERE name LIKE '%idx_threads_agent_path'",
    )
    .fetch_all(&pool)
    .await
    .expect("agent-path indexes should load");
    assert_eq!(description, "threads agent path index");
    assert_eq!(indexes, vec!["idx_threads_agent_path".to_string()]);
    pool.close().await;
}

#[tokio::test]
async fn failed_ledger_rewrite_rolls_back_the_complete_migration_48_repair() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let state_path = sqlite.state_db_path();
    let pool = sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("state database should open");
    released_frodex_state_migrator()
        .run(&pool)
        .await
        .expect("released Frodex migrations should apply");
    sqlx::query(
        r#"
CREATE TRIGGER block_frodex_migration_repair
BEFORE UPDATE OF description, checksum ON _sqlx_migrations
WHEN OLD.version = 48
BEGIN
    SELECT RAISE(ABORT, 'blocked migration repair');
END
        "#,
    )
    .execute(&pool)
    .await
    .expect("repair-blocking trigger should install");

    repair_frodex_agent_path_migration_collision(&pool, &runtime_state_migrator())
        .await
        .expect_err("blocked ledger rewrite should fail repair");
    let appearance_columns = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM pragma_table_info('thread_sections') WHERE name = 'appearance'",
    )
    .fetch_one(&pool)
    .await
    .expect("appearance columns should count");
    let indexes = sqlx::query_scalar::<_, String>(
        r#"
SELECT name
FROM pragma_index_list('threads')
WHERE name IN ('idx_threads_agent_path', 'frodex_idx_threads_agent_path')
ORDER BY name
        "#,
    )
    .fetch_all(&pool)
    .await
    .expect("agent-path indexes should load");
    let description = sqlx::query_scalar::<_, String>(
        "SELECT description FROM _sqlx_migrations WHERE version = 48",
    )
    .fetch_one(&pool)
    .await
    .expect("legacy migration 48 should load");
    assert_eq!(appearance_columns, 0);
    assert_eq!(indexes, vec!["idx_threads_agent_path".to_string()]);
    assert_eq!(description, "threads agent path index");
    pool.close().await;
}

#[tokio::test]
async fn thread_section_migration_preserves_legacy_pin_compatibility() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let state_path = sqlite.state_db_path();
    let pool = sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("sqlite database should open");
    migrator_through(/*version*/ 44)
        .run(&pool)
        .await
        .expect("released thread migrations should apply");

    for thread_id in [
        "00000000-0000-0000-0000-000000000043",
        "00000000-0000-0000-0000-000000000044",
    ] {
        if thread_id.ends_with("44") {
            sqlx::query("UPDATE threads SET is_pinned = 1 WHERE id = ?")
                .bind("00000000-0000-0000-0000-000000000043")
                .execute(&pool)
                .await
                .expect("legacy pin should remain writable before section migration");
            STATE_MIGRATOR
                .run(&pool)
                .await
                .expect("section migration should apply");
        }
        sqlx::query(
            r#"
INSERT INTO threads (
    id,
    rollout_path,
    created_at,
    updated_at,
    created_at_ms,
    updated_at_ms,
    source,
    model_provider,
    cwd,
    title,
    sandbox_policy,
    approval_mode
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(thread_id)
        .bind("/tmp/legacy.jsonl")
        .bind(1_700_000_000_i64)
        .bind(1_700_000_000_i64)
        .bind(1_700_000_000_000_i64)
        .bind(1_700_000_000_000_i64)
        .bind("cli")
        .bind("openai")
        .bind("/tmp")
        .bind("")
        .bind("read-only")
        .bind("on-request")
        .execute(&pool)
        .await
        .expect("legacy thread insert should succeed");
    }

    let registered_sections = sqlx::query_as::<_, (String, String, Option<String>)>(
        "SELECT id, name, appearance FROM thread_sections ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("independent thread sections should load");
    assert_eq!(
        registered_sections,
        vec![(
            PINNED_THREAD_SECTION_ID.to_string(),
            PINNED_THREAD_SECTION_NAME.to_string(),
            None,
        )]
    );

    let threads = sqlx::query_as::<_, (i64, Option<String>)>(
        "SELECT is_pinned, thread_section_id FROM threads ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("legacy and section-aware thread metadata should load");
    assert_eq!(threads, vec![(1, None), (0, None)]);

    sqlx::query("INSERT INTO thread_sections (id, name) VALUES (?, ?)")
        .bind(CUSTOM_THREAD_SECTION_ID)
        .bind("Custom section")
        .execute(&pool)
        .await
        .expect("custom sections should have independent persisted identities");

    let thread_id = "00000000-0000-0000-0000-000000000043";
    sqlx::query("UPDATE threads SET thread_section_id = ? WHERE id = ?")
        .bind(CUSTOM_THREAD_SECTION_ID)
        .bind(thread_id)
        .execute(&pool)
        .await
        .expect("threads should reference independently persisted sections");
    sqlx::query("UPDATE threads SET is_pinned = 0 WHERE id = ?")
        .bind(thread_id)
        .execute(&pool)
        .await
        .expect("released binaries should still update the legacy pin column");
    let thread = sqlx::query_as::<_, (i64, Option<String>)>(
        "SELECT is_pinned, thread_section_id FROM threads WHERE id = ?",
    )
    .bind(thread_id)
    .fetch_one(&pool)
    .await
    .expect("legacy pin updates should not overwrite the authoritative section");
    assert_eq!(thread, (0, Some(CUSTOM_THREAD_SECTION_ID.to_string())));

    sqlx::query("UPDATE threads SET thread_section_id = NULL WHERE id = ?")
        .bind(thread_id)
        .execute(&pool)
        .await
        .expect("threads should be removable from sections");

    let registered_sections =
        sqlx::query_as::<_, (String, String)>("SELECT id, name FROM thread_sections ORDER BY id")
            .fetch_all(&pool)
            .await
            .expect("empty sections should remain independently discoverable");
    assert_eq!(
        registered_sections,
        vec![
            (
                CUSTOM_THREAD_SECTION_ID.to_string(),
                "Custom section".to_string(),
            ),
            (
                PINNED_THREAD_SECTION_ID.to_string(),
                PINNED_THREAD_SECTION_NAME.to_string(),
            ),
        ]
    );

    let mut released_pin_migrator = migrator_through(/*version*/ 44);
    released_pin_migrator.ignore_missing = true;
    released_pin_migrator
        .run(&pool)
        .await
        .expect("released pin-capable binaries should tolerate newer migrations");

    pool.close().await;
}

#[tokio::test]
async fn thread_artifact_migration_preserves_existing_section_metadata() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let pool = sqlite
        .open_read_write_pool(&sqlite.state_db_path())
        .await
        .expect("sqlite database should open");
    migrator_through(/*version*/ 50)
        .run(&pool)
        .await
        .expect("released thread migrations should apply");
    sqlx::query("UPDATE thread_sections SET appearance = ? WHERE id = ?")
        .bind(r#"{"icon":"pin"}"#)
        .bind(PINNED_THREAD_SECTION_ID)
        .execute(&pool)
        .await
        .expect("released section appearance should remain writable");

    STATE_MIGRATOR
        .run(&pool)
        .await
        .expect("artifact migration should apply without rewriting released migrations");
    let section = sqlx::query_as::<_, (String, String, Option<String>)>(
        "SELECT id, name, appearance FROM thread_sections WHERE id = ?",
    )
    .bind(PINNED_THREAD_SECTION_ID)
    .fetch_one(&pool)
    .await
    .expect("existing section metadata should remain available");
    assert_eq!(
        section,
        (
            PINNED_THREAD_SECTION_ID.to_string(),
            PINNED_THREAD_SECTION_NAME.to_string(),
            Some(r#"{"icon":"pin"}"#.to_string()),
        )
    );

    let artifact_tables = sqlx::query_scalar::<_, String>(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'thread_artifacts'",
    )
    .fetch_all(&pool)
    .await
    .expect("artifact table should exist");
    assert_eq!(artifact_tables, vec!["thread_artifacts"]);

    let mut released_migrator = migrator_through(/*version*/ 50);
    released_migrator.ignore_missing = true;
    released_migrator
        .run(&pool)
        .await
        .expect("released binaries should tolerate the additive artifact migration");
}

#[tokio::test]
async fn thread_section_order_migration_backfills_stably() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let pool = sqlite
        .open_read_write_pool(&sqlite.state_db_path())
        .await
        .expect("sqlite database should open");
    migrator_through(/*version*/ 45)
        .run(&pool)
        .await
        .expect("pre-ordering migrations should apply");

    sqlx::query("INSERT INTO thread_sections (id, name) VALUES (?, ?)")
        .bind(CUSTOM_THREAD_SECTION_ID)
        .bind("Custom section")
        .execute(&pool)
        .await
        .expect("custom section should exist before threads reference it");

    let older = "00000000-0000-0000-0000-000000000071";
    let newer = "00000000-0000-0000-0000-000000000072";
    let pinned = "00000000-0000-0000-0000-000000000073";
    let unsectioned = "00000000-0000-0000-0000-000000000074";
    for (thread_id, recency_at_ms, section) in [
        (older, 1_700_000_001_000_i64, Some(CUSTOM_THREAD_SECTION_ID)),
        (newer, 1_700_000_002_000, Some(CUSTOM_THREAD_SECTION_ID)),
        (pinned, 1_700_000_003_000, Some(PINNED_THREAD_SECTION_ID)),
        (unsectioned, 1_700_000_004_000, None),
    ] {
        sqlx::query(
            r#"
INSERT INTO threads (
    id, rollout_path, created_at, updated_at, recency_at,
    created_at_ms, updated_at_ms, recency_at_ms, source,
    model_provider, cwd, title, preview, sandbox_policy, approval_mode, thread_section_id
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(thread_id)
        .bind("/tmp/legacy.jsonl")
        .bind(recency_at_ms / 1000)
        .bind(recency_at_ms / 1000)
        .bind(recency_at_ms / 1000)
        .bind(recency_at_ms)
        .bind(recency_at_ms)
        .bind(recency_at_ms)
        .bind("cli")
        .bind("openai")
        .bind("/tmp")
        .bind("")
        .bind("preview")
        .bind("read-only")
        .bind("on-request")
        .bind(section)
        .execute(&pool)
        .await
        .expect("legacy section row should insert");
    }

    STATE_MIGRATOR
        .run(&pool)
        .await
        .expect("section ordering migration should apply");
    let custom_order = sqlx::query_scalar::<_, String>(
        "SELECT id FROM threads WHERE thread_section_id = ? ORDER BY section_position, id",
    )
    .bind(CUSTOM_THREAD_SECTION_ID)
    .fetch_all(&pool)
    .await
    .expect("backfilled custom order should load");
    assert_eq!(custom_order, vec![newer.to_string(), older.to_string()]);
    let positions =
        sqlx::query_scalar::<_, Option<i64>>("SELECT section_position FROM threads ORDER BY id")
            .fetch_all(&pool)
            .await
            .expect("section positions should load");
    assert_eq!(
        positions,
        vec![Some(2_000_000), Some(1_000_000), Some(1_000_000), None]
    );
    let entered = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT section_entered_at_ms FROM threads ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("section entry timestamps should load");
    assert_eq!(
        entered,
        vec![
            Some(1_700_000_001_000),
            Some(1_700_000_002_000),
            Some(1_700_000_003_000),
            None,
        ]
    );

    let section_position_index = sqlx::query_scalar::<_, String>(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?",
    )
    .bind("idx_threads_section_position")
    .fetch_optional(&pool)
    .await
    .expect("section position index should remain inspectable");
    assert_eq!(
        section_position_index,
        Some("idx_threads_section_position".to_string())
    );

    pool.close().await;
}

#[tokio::test]
async fn thread_item_update_ordinals_allow_older_writers() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let pre_update_ordinal_migrator = Migrator {
        migrations: Cow::Owned(
            THREAD_HISTORY_MIGRATOR
                .migrations
                .iter()
                .filter(|migration| migration.version < 4)
                .cloned()
                .collect(),
        ),
        ignore_missing: THREAD_HISTORY_MIGRATOR.ignore_missing,
        locking: THREAD_HISTORY_MIGRATOR.locking,
        table_name: THREAD_HISTORY_MIGRATOR.table_name.clone(),
        create_schemas: THREAD_HISTORY_MIGRATOR.create_schemas.clone(),
        no_tx: THREAD_HISTORY_MIGRATOR.no_tx,
    };
    let pool = sqlite
        .open_thread_history_db(
            &pre_update_ordinal_migrator,
            /*telemetry_override*/ None,
        )
        .await
        .expect("pre-update-ordinal migrations should apply");
    sqlx::query(
        r#"
INSERT INTO thread_items (thread_id, turn_id, item_id, rollout_ordinal, created_at_ms, item_type, item_json) VALUES
    ('thread-1', 'turn-1', 'existing-item-1', 11, 1_100, 'userMessage', '{}'),
    ('thread-1', 'turn-1', 'existing-item-2', 12, 1_200, 'userMessage', '{}')
        "#,
    )
    .execute(&pool)
    .await
    .expect("pre-migration items should be inserted");
    THREAD_HISTORY_MIGRATOR
        .run(&pool)
        .await
        .expect("update-ordinal migration should apply");
    sqlx::query(
        r#"
INSERT INTO thread_items (thread_id, turn_id, item_id, rollout_ordinal, created_at_ms, item_type, item_json) VALUES
    ('thread-1', 'turn-1', 'old-writer-item-1', 13, 1_300, 'userMessage', '{}'),
    ('thread-1', 'turn-1', 'old-writer-item-2', 14, 1_400, 'userMessage', '{}')
        "#,
    )
    .execute(&pool)
    .await
    .expect("older writers should be able to append multiple items after migration");
    let ordinals = sqlx::query_as::<_, (i64, i64)>(
        "SELECT rollout_ordinal, updated_at_ordinal FROM thread_items WHERE thread_id = ? ORDER BY rollout_ordinal",
    )
    .bind("thread-1")
    .fetch_all(&pool)
    .await
    .expect("old-writer items should load");
    assert_eq!(ordinals, vec![(11, 11), (12, 12), (13, 0), (14, 0)]);

    pool.close().await;
}

#[tokio::test]
async fn realtime_items_preserve_older_thread_history_writers() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let older_migrator = Migrator {
        migrations: Cow::Owned(
            THREAD_HISTORY_MIGRATOR
                .migrations
                .iter()
                .filter(|migration| migration.version < 5)
                .cloned()
                .collect(),
        ),
        ignore_missing: true,
        locking: THREAD_HISTORY_MIGRATOR.locking,
        table_name: THREAD_HISTORY_MIGRATOR.table_name.clone(),
        create_schemas: THREAD_HISTORY_MIGRATOR.create_schemas.clone(),
        no_tx: THREAD_HISTORY_MIGRATOR.no_tx,
    };
    let pool = sqlite
        .open_thread_history_db(&older_migrator, /*telemetry_override*/ None)
        .await
        .expect("existing thread history migrations should apply");
    sqlx::query(
        "INSERT INTO thread_items (thread_id, turn_id, item_id, rollout_ordinal, created_at_ms, item_json) VALUES ('thread-1', 'turn-1', 'existing-item', 1, 100, '{}')",
    )
    .execute(&pool)
    .await
    .expect("existing turn-scoped item should be inserted");

    THREAD_HISTORY_MIGRATOR
        .run(&pool)
        .await
        .expect("realtime item migration should apply");
    let turn_id_not_null = sqlx::query_scalar::<_, i64>(
        "SELECT \"notnull\" FROM pragma_table_info('thread_items') WHERE name = 'turn_id'",
    )
    .fetch_one(&pool)
    .await
    .expect("existing thread item schema should remain inspectable");
    assert_eq!(turn_id_not_null, 1);

    sqlx::query(
        "INSERT INTO thread_realtime_items (thread_id, item_id, rollout_ordinal, created_at_ms, item_type, item_json) VALUES ('thread-1', 'realtime-item', 2, 200, 'realtime_session_started', '{}')",
    )
    .execute(&pool)
    .await
    .expect("thread-scoped realtime item should be inserted separately");
    sqlx::query(
        "INSERT INTO thread_history_projection_state (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES ('thread-1', 0, 0)",
    )
    .execute(&pool)
    .await
    .expect("thread projection checkpoint should be inserted");

    let older_pool = sqlite
        .open_thread_history_db(&older_migrator, /*telemetry_override*/ None)
        .await
        .expect("older binaries should tolerate the additive realtime migration");
    sqlx::query(
        "INSERT INTO thread_items (thread_id, turn_id, item_id, rollout_ordinal, created_at_ms, item_json) VALUES ('thread-1', 'turn-1', 'older-writer-item', 3, 300, '{}')",
    )
    .execute(&older_pool)
    .await
    .expect("older binaries should continue writing ordinary turn-scoped items");
    let ordinary_items = sqlx::query_as::<_, (String, String)>(
        "SELECT item_id, turn_id FROM thread_items WHERE thread_id = ? ORDER BY rollout_ordinal",
    )
    .bind("thread-1")
    .fetch_all(&older_pool)
    .await
    .expect("older binaries should never observe turnless realtime items");
    assert_eq!(
        ordinary_items,
        vec![
            ("existing-item".to_string(), "turn-1".to_string()),
            ("older-writer-item".to_string(), "turn-1".to_string()),
        ]
    );
    sqlx::query("DELETE FROM thread_history_projection_state WHERE thread_id = ?")
        .bind("thread-1")
        .execute(&older_pool)
        .await
        .expect("older binaries should delete their known projection checkpoint");
    let remaining_realtime_items = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM thread_realtime_items WHERE thread_id = ?",
    )
    .bind("thread-1")
    .fetch_one(&pool)
    .await
    .expect("read realtime items after an older writer deleted the thread");
    assert_eq!(remaining_realtime_items, 0);

    older_pool.close().await;
    pool.close().await;
}

#[tokio::test]
async fn agent_job_tables_are_dropped_when_upgrading() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let state_path = sqlite.state_db_path();
    let pool = sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("sqlite database should open");
    migrator_through(/*version*/ 15)
        .run(&pool)
        .await
        .expect("agent job migrations should apply");

    sqlx::query(
        r#"
INSERT INTO agent_jobs (
    id,
    name,
    status,
    instruction,
    input_headers_json,
    input_csv_path,
    output_csv_path,
    created_at,
    updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind("job-1")
    .bind("legacy job")
    .bind("running")
    .bind("process rows")
    .bind(r#"["path"]"#)
    .bind("/tmp/input.csv")
    .bind("/tmp/output.csv")
    .bind(1_700_000_000_i64)
    .bind(1_700_000_000_i64)
    .execute(&pool)
    .await
    .expect("legacy agent job should insert");
    sqlx::query(
        r#"
INSERT INTO agent_job_items (
    job_id,
    item_id,
    row_index,
    row_json,
    status,
    result_json,
    created_at,
    updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind("job-1")
    .bind("item-1")
    .bind(0_i64)
    .bind(r#"{"path":"secret.csv"}"#)
    .bind("completed")
    .bind(r#"{"result":"legacy"}"#)
    .bind(1_700_000_000_i64)
    .bind(1_700_000_000_i64)
    .execute(&pool)
    .await
    .expect("legacy agent job item should insert");

    STATE_MIGRATOR
        .run(&pool)
        .await
        .expect("current migrations should apply");

    let agent_job_tables = sqlx::query_scalar::<_, String>(
        r#"
SELECT name
FROM sqlite_master
WHERE type = 'table' AND name IN ('agent_jobs', 'agent_job_items')
ORDER BY name
        "#,
    )
    .fetch_all(&pool)
    .await
    .expect("remaining agent job tables should load");
    assert_eq!(agent_job_tables, Vec::<String>::new());

    pool.close().await;
}

#[tokio::test]
async fn recency_migration_backfills_and_seeds_old_binary_inserts() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let state_path = sqlite.state_db_path();
    let pool = sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("sqlite database should open");
    migrator_through(/*version*/ 37)
        .run(&pool)
        .await
        .expect("pre-recency migrations should apply");

    sqlx::query(
        r#"
INSERT INTO threads (
    id,
    rollout_path,
    created_at,
    updated_at,
    created_at_ms,
    updated_at_ms,
    source,
    model_provider,
    cwd,
    title,
    sandbox_policy,
    approval_mode
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind("00000000-0000-0000-0000-000000000001")
    .bind("/tmp/first.jsonl")
    .bind(1_700_000_000_i64)
    .bind(1_700_000_100_i64)
    .bind(1_700_000_000_123_i64)
    .bind(1_700_000_100_456_i64)
    .bind("cli")
    .bind("openai")
    .bind("/tmp")
    .bind("")
    .bind("read-only")
    .bind("on-request")
    .execute(&pool)
    .await
    .expect("legacy row should insert");

    STATE_MIGRATOR
        .run(&pool)
        .await
        .expect("recency migration should apply");

    let backfilled = sqlx::query(
        "SELECT updated_at, updated_at_ms, recency_at, recency_at_ms FROM threads WHERE id = ?",
    )
    .bind("00000000-0000-0000-0000-000000000001")
    .fetch_one(&pool)
    .await
    .expect("backfilled row should load");
    assert_eq!(backfilled.get::<i64, _>("recency_at"), 1_700_000_100);
    assert_eq!(backfilled.get::<i64, _>("recency_at_ms"), 1_700_000_100_456);

    sqlx::query(
        r#"
INSERT INTO threads (
    id,
    rollout_path,
    created_at,
    updated_at,
    created_at_ms,
    updated_at_ms,
    source,
    model_provider,
    cwd,
    title,
    sandbox_policy,
    approval_mode
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind("00000000-0000-0000-0000-000000000002")
    .bind("/tmp/second.jsonl")
    .bind(1_700_000_200_i64)
    .bind(1_700_000_300_i64)
    .bind(1_700_000_200_123_i64)
    .bind(1_700_000_300_456_i64)
    .bind("cli")
    .bind("openai")
    .bind("/tmp")
    .bind("")
    .bind("read-only")
    .bind("on-request")
    .execute(&pool)
    .await
    .expect("old-binary row should insert");

    let seeded = sqlx::query("SELECT recency_at, recency_at_ms FROM threads WHERE id = ?")
        .bind("00000000-0000-0000-0000-000000000002")
        .fetch_one(&pool)
        .await
        .expect("old-binary row should load");
    assert_eq!(seeded.get::<i64, _>("recency_at"), 1_700_000_300);
    assert_eq!(seeded.get::<i64, _>("recency_at_ms"), 1_700_000_300_456);

    pool.close().await;
}

#[tokio::test]
async fn repairs_recency_migration_that_was_applied_as_version_38() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let state_path = sqlite.state_db_path();
    let pool = sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("sqlite database should open");
    apply_legacy_recency_migration(&pool).await;

    repair_legacy_recency_migration_version(&pool, &STATE_MIGRATOR)
        .await
        .expect("legacy migration history should be repaired");
    STATE_MIGRATOR
        .run(&pool)
        .await
        .expect("current migrations should apply after repair");

    let applied = sqlx::query(
        "SELECT version, checksum FROM _sqlx_migrations WHERE version >= 38 ORDER BY version",
    )
    .fetch_all(&pool)
    .await
    .expect("applied migrations should load")
    .into_iter()
    .map(|row| {
        (
            row.get::<i64, _>("version"),
            row.get::<Vec<u8>, _>("checksum"),
        )
    })
    .collect::<Vec<_>>();
    let expected = STATE_MIGRATOR
        .migrations
        .iter()
        .filter(|migration| migration.version >= 38)
        .map(|migration| (migration.version, migration.checksum.to_vec()))
        .collect::<Vec<_>>();
    assert_eq!(applied, expected);

    pool.close().await;
}

#[tokio::test]
async fn repair_recency_migration_succeeds_while_another_connection_holds_writer_slot() {
    let sqlite_home = crate::runtime::test_support::unique_temp_dir();
    tokio::fs::create_dir_all(&sqlite_home)
        .await
        .expect("sqlite home should be created");
    let _cleanup = scopeguard::guard(sqlite_home.clone(), |sqlite_home| {
        let _ = std::fs::remove_dir_all(sqlite_home);
    });
    let sqlite = crate::SqliteConfig::new_for_testing(sqlite_home.as_path().abs());
    let state_path = sqlite.state_db_path();
    let pool = sqlite
        .open_read_write_pool(&state_path)
        .await
        .expect("database should open");
    STATE_MIGRATOR
        .run(&pool)
        .await
        .expect("current migrations should apply");
    let read_pool = sqlite
        .open_read_only_pool(&state_path, /*busy_timeout*/ None)
        .await
        .expect("read-only pool should open");
    let mut write_connection = pool.acquire().await.expect("write connection should open");
    let write_transaction = write_connection
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("write transaction should acquire the writer slot");

    let repair_result = repair_legacy_recency_migration_version(&read_pool, &STATE_MIGRATOR).await;

    write_transaction
        .rollback()
        .await
        .expect("write transaction should roll back");
    drop(write_connection);
    read_pool.close().await;
    pool.close().await;
    repair_result.expect("current migration history should not need the writer slot");
}
