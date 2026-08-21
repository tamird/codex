use super::*;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use pretty_assertions::assert_eq;

#[test]
fn goal_supervisor_reconciliation_does_not_migrate_indexed_children() {
    run_goal_supervisor_test(
        "goal_supervisor_reconciliation_does_not_migrate_indexed_children",
        async {
            let (_home, config) = test_config().await;
            let state_db = init_state_db(&config).await.expect("state db");
            let store = Arc::new(LocalThreadStore::new(
                LocalThreadStoreConfig::from_config(&config),
                Some(state_db.clone()),
            ));
            store.start_automatic_rollout_migration();
            let auth = AuthManager::from_auth_for_testing(CodexAuth::from_api_key("dummy"));
            let manager = ThreadManager::new(
                &config,
                auth.clone(),
                crate::thread_manager::build_models_manager(&config, auth),
                crate::CodexAppsToolsCache::default(),
                SessionSource::Exec,
                Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
                empty_extension_registry(),
                Arc::new(crate::test_support::EmptyUserInstructionsProvider),
                /*analytics_events_client*/ None,
                store,
                crate::thread_manager::local_agent_graph_store_from_state_db(Some(&state_db)),
                uuid::Uuid::new_v4().to_string(),
                /*attestation_provider*/ None,
                /*external_time_provider*/ None,
            );
            let control = manager.agent_control();
            let parent_id = ThreadId::new();
            let supervisor_path = AgentPath::root().join("goal_supervisor").expect("path");
            let directory = config.codex_home.join("sessions/2025/01/03");
            std::fs::create_dir_all(&directory).expect("session directory");
            let parent_metadata = codex_state::ThreadMetadataBuilder::new(
                parent_id,
                directory.join(format!("{parent_id}.jsonl")).to_path_buf(),
                chrono::Utc::now(),
                SessionSource::Exec,
            )
            .build("openai");
            state_db
                .upsert_thread(&parent_metadata)
                .await
                .expect("parent metadata");
            let mut children = Vec::new();
            for index in 0..4 {
                let child_id = ThreadId::new();
                let source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    parent_thread_id: if index == 3 {
                        ThreadId::new()
                    } else {
                        parent_id
                    },
                    depth: 1,
                    agent_path: None,
                    agent_nickname: None,
                    agent_role: match index {
                        2 | 3 => {
                            Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME.to_string())
                        }
                        1 => Some("worker".to_string()),
                        _ => None,
                    },
                });
                let path = directory.join(format!("rollout-2025-01-03T12-00-00-{child_id}.jsonl"));
                let meta = RolloutLine {
                    timestamp: "2025-01-03T12:00:00Z".to_string(),
                    ordinal: None,
                    item: RolloutItem::SessionMeta(SessionMetaLine {
                        meta: SessionMeta {
                            session_id: child_id.into(),
                            id: child_id,
                            timestamp: "2025-01-03T12:00:00Z".to_string(),
                            cwd: config.cwd.to_path_buf(),
                            source: source.clone(),
                            history_mode: ThreadHistoryMode::Legacy,
                            ..Default::default()
                        },
                        git: None,
                    }),
                };
                let mut bytes = serde_json::to_vec(&meta).expect("session metadata");
                bytes.push(b'\n');
                std::fs::write(&path, &bytes).expect("legacy rollout");
                let metadata = codex_state::ThreadMetadataBuilder::new(
                    child_id,
                    path.to_path_buf(),
                    chrono::Utc::now(),
                    source,
                )
                .build("openai");
                state_db
                    .upsert_thread(&metadata)
                    .await
                    .expect("child metadata");
                state_db
                    .upsert_thread_spawn_edge(
                        parent_id,
                        child_id,
                        DirectionalThreadSpawnEdgeStatus::Open,
                    )
                    .await
                    .expect("child edge");
                children.push((child_id, path, bytes));
            }
            let job = codex_rollout::try_acquire_rollout_maintenance_job_lock(&config.codex_home)
                .expect("maintenance lock")
                .expect("exclusive migration job");
            let result = timeout(
                Duration::from_secs(2),
                control.reconcile_goal_supervisor_state(parent_id, &supervisor_path),
            )
            .await;
            drop(job);
            assert_eq!(
                result
                    .expect("metadata must not wait for migration")
                    .expect("reconcile"),
                None
            );
            let open = state_db
                .list_thread_spawn_children_with_status(
                    parent_id,
                    DirectionalThreadSpawnEdgeStatus::Open,
                )
                .await
                .expect("open children");
            for (index, (child_id, path, bytes)) in children.iter().enumerate() {
                assert_eq!(open.contains(child_id), index != 2);
                assert_eq!(std::fs::read(path).expect("retained source"), *bytes);
            }
        },
    );
}
