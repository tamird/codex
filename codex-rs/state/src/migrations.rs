use std::borrow::Cow;

use anyhow::Context;
use anyhow::bail;
use sqlx::Connection;
use sqlx::Row;
use sqlx::SqliteConnection;
use sqlx::SqlitePool;
use sqlx::migrate::Migrator;

const FRODEX_GOAL_SUPERVISOR_MIGRATION_DESCRIPTION: &str = "thread goal supervisor state";
const FRODEX_GOAL_SUPERVISOR_MIGRATION_CHECKSUM: [u8; 48] = [
    0xa3, 0x76, 0x8f, 0x53, 0x74, 0x09, 0x87, 0xd9, 0xb5, 0xf6, 0xa6, 0xdc, 0xee, 0x57, 0xab, 0x73,
    0x6d, 0xb4, 0x72, 0xc0, 0xce, 0xfb, 0x42, 0x42, 0x15, 0x8b, 0x5c, 0x12, 0x3f, 0x6a, 0x73, 0x4c,
    0x9d, 0xa7, 0xf1, 0x56, 0x4d, 0x2c, 0x5c, 0x89, 0x37, 0x73, 0x4f, 0xff, 0x31, 0xc4, 0xb5, 0x17,
];
const FRODEX_GOAL_SUPERVISOR_STATE_TABLE_SQL: &str = r#"
CREATE TABLE thread_goal_supervisor_state (
    thread_id TEXT PRIMARY KEY NOT NULL REFERENCES threads(id) ON DELETE CASCADE,
    goal_id TEXT NOT NULL,
    snoozed_until_ms INTEGER,
    updated_at_ms INTEGER NOT NULL
)
"#;

pub(crate) static STATE_MIGRATOR: Migrator = sqlx::migrate!("./migrations");
pub(crate) static LOGS_MIGRATOR: Migrator = sqlx::migrate!("./logs_migrations");
pub(crate) static GOALS_MIGRATOR: Migrator = sqlx::migrate!("./goals_migrations");
pub(crate) static MEMORIES_MIGRATOR: Migrator = sqlx::migrate!("./memory_migrations");
pub(crate) static QUEUE_MIGRATOR: Migrator = sqlx::migrate!("./queue_migrations");
pub(crate) static THREAD_HISTORY_MIGRATOR: Migrator = sqlx::migrate!("./thread_history_migrations");

/// Allow an older Codex binary to open a database that has already been
/// migrated by a newer binary running in parallel.
///
/// We intentionally ignore applied migration versions that are newer than the
/// embedded migration set. Known migration versions are still validated by
/// checksum, so this only relaxes the "database is ahead of me" case.
fn runtime_migrator(base: &'static Migrator) -> Migrator {
    Migrator {
        migrations: Cow::Borrowed(base.migrations.as_ref()),
        ignore_missing: true,
        locking: base.locking,
        no_tx: base.no_tx,
        table_name: base.table_name.clone(),
        create_schemas: base.create_schemas.clone(),
    }
}

pub(crate) fn runtime_state_migrator() -> Migrator {
    runtime_migrator(&STATE_MIGRATOR)
}

pub(crate) fn runtime_logs_migrator() -> Migrator {
    runtime_migrator(&LOGS_MIGRATOR)
}

pub(crate) fn runtime_goals_migrator() -> Migrator {
    runtime_migrator(&GOALS_MIGRATOR)
}

pub(crate) fn runtime_memories_migrator() -> Migrator {
    runtime_migrator(&MEMORIES_MIGRATOR)
}

pub(crate) fn runtime_queue_migrator() -> Migrator {
    runtime_migrator(&QUEUE_MIGRATOR)
}

// The paginated history projector will call this when it takes ownership of opening the database.
#[allow(dead_code)]
pub(crate) fn runtime_thread_history_migrator() -> Migrator {
    runtime_migrator(&THREAD_HISTORY_MIGRATOR)
}

/// Removes only the authenticated Frodex migration 33/34 collision so the unchanged upstream
/// migrator can apply those versions. The legacy table remains until its rows are durably copied
/// to the goals database.
pub(crate) async fn repair_frodex_goal_supervisor_state_migration(
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let migrations_table_exists = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = '_sqlx_migrations'",
    )
    .fetch_one(pool)
    .await?
        != 0;
    if !migrations_table_exists {
        return Ok(());
    }
    let candidate_exists = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM _sqlx_migrations WHERE version IN (33, 34) AND description = ?",
    )
    .bind(FRODEX_GOAL_SUPERVISOR_MIGRATION_DESCRIPTION)
    .fetch_one(pool)
    .await?
        != 0;
    if !candidate_exists {
        return Ok(());
    }

    let mut connection = pool.acquire().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let migrations_table_exists = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = '_sqlx_migrations'",
    )
    .fetch_one(&mut *transaction)
    .await?
        != 0;
    if !migrations_table_exists {
        transaction.commit().await?;
        return Ok(());
    }

    let candidate_rows = sqlx::query(
        r#"
SELECT version, description, success, checksum
FROM _sqlx_migrations
WHERE version IN (33, 34)
  AND description = ?
ORDER BY version
        "#,
    )
    .bind(FRODEX_GOAL_SUPERVISOR_MIGRATION_DESCRIPTION)
    .fetch_all(&mut *transaction)
    .await?;
    if candidate_rows.is_empty() {
        transaction.commit().await?;
        return Ok(());
    }
    if candidate_rows.len() != 1 {
        bail!("multiple Frodex goal supervisor migration candidates are present");
    }

    let candidate = &candidate_rows[0];
    let version: i64 = candidate.try_get("version")?;
    let success: bool = candidate.try_get("success")?;
    let checksum: Vec<u8> = candidate.try_get("checksum")?;
    if !success || checksum.as_slice() != FRODEX_GOAL_SUPERVISOR_MIGRATION_CHECKSUM {
        bail!(
            "migration {version} named {FRODEX_GOAL_SUPERVISOR_MIGRATION_DESCRIPTION:?} does not match the released Frodex migration"
        );
    }

    validate_frodex_goal_supervisor_state_table_on(&mut transaction).await?;
    validate_frodex_goal_supervisor_ledger_on(&mut transaction, version).await?;

    let result = sqlx::query(
        r#"
DELETE FROM _sqlx_migrations
WHERE version = ?
  AND description = ?
  AND success = 1
  AND checksum = ?
        "#,
    )
    .bind(version)
    .bind(FRODEX_GOAL_SUPERVISOR_MIGRATION_DESCRIPTION)
    .bind(FRODEX_GOAL_SUPERVISOR_MIGRATION_CHECKSUM.as_slice())
    .execute(&mut *transaction)
    .await
    .context("removing authenticated Frodex goal supervisor migration row")?;
    if result.rows_affected() != 1 {
        bail!("authenticated Frodex goal supervisor migration row changed during repair");
    }
    transaction.commit().await?;
    Ok(())
}

async fn validate_frodex_goal_supervisor_ledger_on(
    connection: &mut SqliteConnection,
    candidate_version: i64,
) -> anyhow::Result<()> {
    if !matches!(candidate_version, 33 | 34) {
        bail!("unsupported Frodex goal supervisor migration version {candidate_version}");
    }

    let rows = sqlx::query(
        "SELECT version, description, success, checksum FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(&mut *connection)
    .await?;
    let expected_predecessors = STATE_MIGRATOR
        .migrations
        .iter()
        .filter(|migration| migration.version < candidate_version)
        .collect::<Vec<_>>();
    if rows.len() != expected_predecessors.len() + 1 {
        bail!(
            "migration {candidate_version} Frodex collision does not have the complete official predecessor ledger"
        );
    }
    for (row, expected) in rows
        .iter()
        .take(expected_predecessors.len())
        .zip(expected_predecessors)
    {
        let version: i64 = row.try_get("version")?;
        let description: String = row.try_get("description")?;
        let success: bool = row.try_get("success")?;
        let checksum: Vec<u8> = row.try_get("checksum")?;
        if version != expected.version
            || !success
            || description != expected.description
            || checksum.as_slice() != expected.checksum.as_ref()
        {
            bail!(
                "migration {candidate_version} Frodex collision does not have the complete official predecessor ledger"
            );
        }
    }
    let candidate = rows
        .last()
        .context("Frodex goal supervisor migration candidate disappeared")?;
    if candidate.try_get::<i64, _>("version")? != candidate_version
        || candidate.try_get::<String, _>("description")?
            != FRODEX_GOAL_SUPERVISOR_MIGRATION_DESCRIPTION
        || !candidate.try_get::<bool, _>("success")?
        || candidate.try_get::<Vec<u8>, _>("checksum")?.as_slice()
            != FRODEX_GOAL_SUPERVISOR_MIGRATION_CHECKSUM
    {
        bail!("authenticated Frodex goal supervisor migration candidate changed during repair");
    }
    Ok(())
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_ascii_whitespace() && *character != ';')
        .flat_map(char::to_uppercase)
        .collect()
}

pub(crate) async fn validate_frodex_goal_supervisor_state_table_on(
    connection: &mut SqliteConnection,
) -> anyhow::Result<()> {
    let table_sql = sqlx::query_scalar::<_, String>(
        "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'thread_goal_supervisor_state'",
    )
    .fetch_optional(&mut *connection)
    .await?
    .context("authenticated Frodex migration is missing thread_goal_supervisor_state")?;
    if normalize_schema_sql(&table_sql)
        != normalize_schema_sql(FRODEX_GOAL_SUPERVISOR_STATE_TABLE_SQL)
    {
        bail!("thread_goal_supervisor_state does not match the released Frodex schema");
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
        bail!("thread_goal_supervisor_state has incompatible columns");
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
            bail!("thread_goal_supervisor_state has an incompatible {name} column");
        }
    }

    let foreign_keys = sqlx::query("PRAGMA foreign_key_list('thread_goal_supervisor_state')")
        .fetch_all(&mut *connection)
        .await?;
    if foreign_keys.len() != 1 {
        bail!("thread_goal_supervisor_state has incompatible foreign keys");
    }
    let foreign_key = &foreign_keys[0];
    if foreign_key.try_get::<i64, _>("id")? != 0
        || foreign_key.try_get::<i64, _>("seq")? != 0
        || foreign_key.try_get::<String, _>("table")? != "threads"
        || foreign_key.try_get::<String, _>("from")? != "thread_id"
        || foreign_key.try_get::<String, _>("to")? != "id"
        || foreign_key.try_get::<String, _>("on_update")? != "NO ACTION"
        || foreign_key.try_get::<String, _>("on_delete")? != "CASCADE"
        || foreign_key.try_get::<String, _>("match")? != "NONE"
    {
        bail!("thread_goal_supervisor_state has an incompatible foreign key");
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
        bail!("thread_goal_supervisor_state has incompatible indexes or triggers");
    }
    let indexes = sqlx::query("PRAGMA index_list('thread_goal_supervisor_state')")
        .fetch_all(&mut *connection)
        .await?;
    if indexes.len() != 1
        || !indexes[0].try_get::<bool, _>("unique")?
        || indexes[0].try_get::<String, _>("origin")? != "pk"
        || indexes[0].try_get::<bool, _>("partial")?
    {
        bail!("thread_goal_supervisor_state has incompatible indexes");
    }
    Ok(())
}

pub(crate) async fn repair_legacy_recency_migration_version(
    pool: &SqlitePool,
    migrator: &Migrator,
) -> anyhow::Result<()> {
    let Some(recency_migration) = migrator
        .migrations
        .iter()
        .find(|migration| migration.version == 39)
    else {
        return Ok(());
    };
    let migrations_table_exists = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations'",
    )
    .fetch_optional(pool)
    .await?
    .is_some();
    if !migrations_table_exists {
        return Ok(());
    }

    let legacy_recency_needs_repair = sqlx::query_scalar::<_, i64>(
        r#"
SELECT 1
FROM _sqlx_migrations
WHERE version = ?
  AND checksum = ?
  AND NOT EXISTS (
      SELECT 1 FROM _sqlx_migrations WHERE version = ?
  )
        "#,
    )
    .bind(38_i64)
    .bind(recency_migration.checksum.as_ref())
    .bind(recency_migration.version)
    .fetch_optional(pool)
    .await?
    .is_some();
    if !legacy_recency_needs_repair {
        return Ok(());
    }

    sqlx::query(
        r#"
UPDATE _sqlx_migrations
SET version = ?, description = ?
WHERE version = ?
  AND checksum = ?
  AND NOT EXISTS (
      SELECT 1 FROM _sqlx_migrations WHERE version = ?
  )
        "#,
    )
    .bind(recency_migration.version)
    .bind(recency_migration.description.as_ref())
    .bind(38_i64)
    .bind(recency_migration.checksum.as_ref())
    .bind(recency_migration.version)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
#[path = "migrations_tests.rs"]
mod tests;
