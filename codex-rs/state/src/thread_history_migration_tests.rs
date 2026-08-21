use std::sync::Arc;

use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn concurrent_history_database_initialization_applies_each_migration_once() {
    for existing_schema in [false, true] {
        let home = crate::runtime::test_support::unique_temp_dir();
        tokio::fs::create_dir_all(&home).await.expect("home");
        let _cleanup = scopeguard::guard(home.clone(), |home| {
            let _ = std::fs::remove_dir_all(home);
        });
        let sqlite = crate::SqliteConfig::new_for_testing(home.as_path().abs());
        let pool = sqlite
            .open_read_write_pool(&sqlite.thread_history_db_path())
            .await
            .expect("WAL file");
        if existing_schema {
            let mut migrator = crate::migrations::runtime_thread_history_migrator();
            migrator.migrations = std::borrow::Cow::Owned(
                migrator
                    .migrations
                    .iter()
                    .filter(|migration| migration.version == 1)
                    .cloned()
                    .collect(),
            );
            migrator.run(&pool).await.expect("old history schema");
        }
        pool.close().await;
        let barrier = Arc::new(tokio::sync::Barrier::new(8));
        let mut requests = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let sqlite = sqlite.clone();
            let barrier = barrier.clone();
            requests.spawn(async move {
                barrier.wait().await;
                crate::open_thread_history_db(&sqlite).await
            });
        }
        while let Some(result) = requests.join_next().await {
            let pool = result
                .expect("initializer task")
                .expect("concurrent history initialization");
            let versions: Vec<(i64, bool)> =
                sqlx::query_as("SELECT version, success FROM _sqlx_migrations ORDER BY version")
                    .fetch_all(&pool)
                    .await
                    .expect("applied migrations");
            let expected = crate::migrations::THREAD_HISTORY_MIGRATOR
                .migrations
                .iter()
                .map(|migration| (migration.version, true))
                .collect::<Vec<_>>();
            assert_eq!(versions, expected);
            let state_tables: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('threads', 'thread_goal_supervisor_state')")
                .fetch_one(&pool).await.expect("schema isolation");
            assert_eq!(
                state_tables, 0,
                "state compatibility repairs must not run in the history database"
            );
            pool.close().await;
        }
    }
}
