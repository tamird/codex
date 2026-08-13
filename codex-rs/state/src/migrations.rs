use std::borrow::Cow;

use anyhow::Context;
use anyhow::bail;
use sqlx::Row;
use sqlx::SqliteConnection;
use sqlx::migrate::Migrator;

const LEGACY_FRODEX_AGENT_PATH_MIGRATION_VERSION: i64 = 48;
const LEGACY_FRODEX_AGENT_PATH_MIGRATION_DESCRIPTION: &str = "threads agent path index";
const LEGACY_FRODEX_AGENT_PATH_MIGRATION_CHECKSUM_HEX: &str = "6e2da6fd82ca71d665d712527262760e81a468eed448e940173e94d330286527508503b739159ed4424e8e0a8f9036d5";
const LEGACY_FRODEX_AGENT_PATH_INDEX: &str = "idx_threads_agent_path";
const LEGACY_FRODEX_AGENT_PATH_INDEX_SQL: &str = r#"
CREATE INDEX idx_threads_agent_path
    ON threads(agent_path)
    WHERE agent_path IS NOT NULL
"#;
const UPSTREAM_THREAD_SECTION_APPEARANCE_SQL: &str =
    "ALTER TABLE thread_sections ADD COLUMN appearance TEXT;\n";

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
    connection: &mut SqliteConnection,
) -> anyhow::Result<()> {
    let migrations_table_exists = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = '_sqlx_migrations'",
    )
    .fetch_one(&mut *connection)
    .await?
        != 0;
    if !migrations_table_exists {
        return Ok(());
    }
    let candidate_exists = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM _sqlx_migrations WHERE version IN (33, 34) AND description = ?",
    )
    .bind(FRODEX_GOAL_SUPERVISOR_MIGRATION_DESCRIPTION)
    .fetch_one(&mut *connection)
    .await?
        != 0;
    if !candidate_exists {
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
    .fetch_all(&mut *connection)
    .await?;
    if candidate_rows.is_empty() {
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

    validate_frodex_goal_supervisor_state_table_on(connection).await?;
    validate_frodex_goal_supervisor_ledger_on(connection, version).await?;

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
    .execute(&mut *connection)
    .await
    .context("removing authenticated Frodex goal supervisor migration row")?;
    if result.rows_affected() != 1 {
        bail!("authenticated Frodex goal supervisor migration row changed during repair");
    }
    Ok(())
}

async fn validate_agent_path_index(
    connection: &mut SqliteConnection,
    index_name: &str,
    forced_query: &'static str,
) -> anyhow::Result<()> {
    let index_shape = sqlx::query_as::<_, (i64, i64, i64)>(
        r#"
SELECT
    (SELECT COUNT(*) FROM pragma_index_info(?)),
    (SELECT COUNT(*)
     FROM pragma_index_list('threads')
     WHERE name = ? AND partial = 1),
    (SELECT COUNT(*)
     FROM pragma_index_list('threads')
     WHERE name = ? AND "unique" = 1)
        "#,
    )
    .bind(index_name)
    .bind(index_name)
    .bind(index_name)
    .fetch_one(&mut *connection)
    .await?;
    let index_column = sqlx::query_scalar::<_, String>("SELECT name FROM pragma_index_info(?)")
        .bind(index_name)
        .fetch_optional(&mut *connection)
        .await?;
    if index_shape != (1, 1, 0) || index_column.as_deref() != Some("agent_path") {
        anyhow::bail!("index {index_name} does not match the Frodex agent-path index schema");
    }
    let index_sql = sqlx::query_scalar::<_, String>(
        "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = ?",
    )
    .bind(index_name)
    .fetch_optional(&mut *connection)
    .await?
    .context("released Frodex agent-path index is missing its SQL definition")?;
    if normalize_schema_sql(&index_sql) != normalize_schema_sql(LEGACY_FRODEX_AGENT_PATH_INDEX_SQL)
    {
        anyhow::bail!("index {index_name} does not match the released Frodex index SQL");
    }
    sqlx::query(forced_query)
        .bind("__frodex_index_validation__")
        .fetch_all(&mut *connection)
        .await
        .with_context(|| format!("index {index_name} cannot satisfy the agent-path query"))?;
    Ok(())
}

/// Convert the released Frodex migration-48 ledger entry to upstream migration 48.
///
/// Frodex alpha.6 and official Codex alpha.7 assigned version 48 to different SQL while sharing
/// `state_5.sqlite`. This repair recognizes only the exact released Frodex checksum and schema,
/// applies upstream's migration in the same writer transaction, and restores the upstream ledger
/// entry. Every other checksum mismatch remains SQLx's responsibility and fails closed.
pub(crate) async fn repair_frodex_agent_path_migration_collision(
    connection: &mut SqliteConnection,
    migrator: &Migrator,
) -> anyhow::Result<bool> {
    let Some(upstream_migration) = migrator
        .migrations
        .iter()
        .find(|migration| migration.version == LEGACY_FRODEX_AGENT_PATH_MIGRATION_VERSION)
    else {
        return Ok(false);
    };
    if upstream_migration.description != "thread section appearance"
        || upstream_migration.sql.as_str() != UPSTREAM_THREAD_SECTION_APPEARANCE_SQL
    {
        anyhow::bail!("embedded state migration 48 does not match upstream appearance migration");
    }
    let migrations_table_exists = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = '_sqlx_migrations'",
    )
    .fetch_optional(&mut *connection)
    .await?
    .is_some();
    if !migrations_table_exists {
        return Ok(false);
    }
    let legacy_row_exists = sqlx::query_scalar::<_, i64>(
        r#"
SELECT 1
FROM _sqlx_migrations
WHERE version = ?
  AND success = 1
  AND description = ?
  AND lower(hex(checksum)) = ?
        "#,
    )
    .bind(LEGACY_FRODEX_AGENT_PATH_MIGRATION_VERSION)
    .bind(LEGACY_FRODEX_AGENT_PATH_MIGRATION_DESCRIPTION)
    .bind(LEGACY_FRODEX_AGENT_PATH_MIGRATION_CHECKSUM_HEX)
    .fetch_optional(&mut *connection)
    .await?
    .is_some();
    if !legacy_row_exists {
        return Ok(false);
    }

    validate_frodex_agent_path_predecessor_ledger(connection, migrator).await?;
    validate_agent_path_index(
        connection,
        LEGACY_FRODEX_AGENT_PATH_INDEX,
        "EXPLAIN QUERY PLAN SELECT id FROM threads INDEXED BY idx_threads_agent_path WHERE agent_path = ?",
    )
    .await
    .context("refusing to repair Frodex migration 48")?;

    let appearance_shape = sqlx::query_as::<_, (String, i64, Option<String>, i64)>(
        r#"
SELECT type, "notnull", dflt_value, pk
FROM pragma_table_info('thread_sections')
WHERE name = 'appearance'
        "#,
    )
    .fetch_optional(&mut *connection)
    .await?;
    match appearance_shape {
        None => {
            sqlx::query(UPSTREAM_THREAD_SECTION_APPEARANCE_SQL)
                .execute(&mut *connection)
                .await
                .context("applying upstream state migration 48 during Frodex repair")?;
        }
        Some((column_type, not_null, default_value, primary_key))
            if column_type.eq_ignore_ascii_case("TEXT")
                && not_null == 0
                && default_value.is_none()
                && primary_key == 0 => {}
        Some(_) => anyhow::bail!(
            "refusing to repair Frodex migration 48 because thread_sections.appearance does not match upstream"
        ),
    }

    sqlx::query("DROP INDEX idx_threads_agent_path")
        .execute(&mut *connection)
        .await?;

    let updated = sqlx::query(
        r#"
UPDATE _sqlx_migrations
SET description = ?, checksum = ?
WHERE version = ?
  AND success = 1
  AND description = ?
  AND lower(hex(checksum)) = ?
        "#,
    )
    .bind(upstream_migration.description.as_ref())
    .bind(upstream_migration.checksum.as_ref())
    .bind(LEGACY_FRODEX_AGENT_PATH_MIGRATION_VERSION)
    .bind(LEGACY_FRODEX_AGENT_PATH_MIGRATION_DESCRIPTION)
    .bind(LEGACY_FRODEX_AGENT_PATH_MIGRATION_CHECKSUM_HEX)
    .execute(&mut *connection)
    .await?;
    if updated.rows_affected() != 1 {
        anyhow::bail!("Frodex migration 48 repair did not update exactly one ledger row");
    }
    Ok(true)
}

async fn validate_frodex_agent_path_predecessor_ledger(
    connection: &mut SqliteConnection,
    migrator: &Migrator,
) -> anyhow::Result<()> {
    let rows = sqlx::query(
        "SELECT version, description, success, checksum FROM _sqlx_migrations WHERE version < 48 ORDER BY version",
    )
    .fetch_all(&mut *connection)
    .await?;
    let expected_predecessors = migrator
        .migrations
        .iter()
        .filter(|migration| migration.version < LEGACY_FRODEX_AGENT_PATH_MIGRATION_VERSION)
        .collect::<Vec<_>>();
    let mut row_index = 0;
    let mut missing_goal_supervisor_version = None;
    for expected in expected_predecessors {
        let row = rows.get(row_index);
        let row_version = row
            .map(|row| row.try_get::<i64, _>("version"))
            .transpose()?;
        if row_version == Some(expected.version) {
            let row = row.context("migration predecessor row disappeared")?;
            let description: String = row.try_get("description")?;
            let success: bool = row.try_get("success")?;
            let checksum: Vec<u8> = row.try_get("checksum")?;
            if !success
                || description != expected.description
                || checksum.as_slice() != expected.checksum.as_ref()
            {
                bail!(
                    "refusing to repair Frodex migration 48 because predecessor migration {} is not official",
                    expected.version
                );
            }
            row_index += 1;
            continue;
        }
        if !matches!(expected.version, 33 | 34) || missing_goal_supervisor_version.is_some() {
            bail!(
                "refusing to repair Frodex migration 48 because predecessor migration {} is missing",
                expected.version
            );
        }
        missing_goal_supervisor_version = Some(expected.version);
    }
    if row_index != rows.len() {
        bail!("refusing to repair Frodex migration 48 because its predecessor ledger is unknown");
    }
    if missing_goal_supervisor_version.is_some() {
        validate_frodex_goal_supervisor_state_table_on(connection)
            .await
            .context(
                "refusing to repair Frodex migration 48 with an unauthenticated goal supervisor predecessor",
            )?;
    }
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
    connection: &mut SqliteConnection,
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
    .fetch_optional(&mut *connection)
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
    .fetch_optional(&mut *connection)
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
    .execute(&mut *connection)
    .await?;
    Ok(())
}

#[cfg(test)]
#[path = "migrations_tests.rs"]
mod tests;
