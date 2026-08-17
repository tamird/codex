use super::super::ResumeModelSettings;
use crate::legacy_core::config::Config;
use crate::legacy_core::config::ConfigBuilder;
use app_test_support::create_fake_paginated_rollout;
use app_test_support::create_fake_rollout;
use app_test_support::rollout_path;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_features::Feature;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::models::AgentMessageInputContent;
use codex_protocol::models::InternalChatMessageMetadataPassthrough;
use codex_protocol::models::ResponseItem;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use color_eyre::eyre::Result;
use pretty_assertions::assert_eq;
use std::time::Duration;
use tempfile::TempDir;

async fn build_config(temp_dir: &TempDir) -> Config {
    ConfigBuilder::default()
        .codex_home(temp_dir.path().to_path_buf())
        .build()
        .await
        .expect("config should build")
}

fn poison_legacy_rollout_for_goal_supervisor_repair(
    codex_home: &std::path::Path,
    filename_ts: &str,
    thread_id: ThreadId,
) -> Result<()> {
    let path = rollout_path(codex_home, filename_ts, thread_id.to_string().as_str());
    let mut lines = std::fs::read_to_string(path.as_path())?
        .lines()
        .map(serde_json::from_str::<RolloutLine>)
        .collect::<Result<Vec<_>, _>>()?;
    let RolloutItem::SessionMeta(session_meta) = &mut lines[0].item else {
        panic!("expected session metadata");
    };
    session_meta.meta.segment_id = Some(SegmentId::new());
    session_meta.meta.cli_version = "0.148.0-alpha.6+frodex.0".to_string();
    for (ordinal, line) in lines.iter_mut().enumerate() {
        line.ordinal = Some(ordinal as u64);
    }
    let delivery_ordinal = lines.len() as u64;
    lines.push(RolloutLine {
        timestamp: "2026-08-17T08:00:01Z".to_string(),
        ordinal: Some(delivery_ordinal),
        item: RolloutItem::InterAgentCommunicationMetadata { trigger_turn: true },
    });
    lines.push(RolloutLine {
        timestamp: "2026-08-17T08:00:02Z".to_string(),
        ordinal: Some(delivery_ordinal + 1),
        item: RolloutItem::ResponseItem(
            ResponseItem::AgentMessage {
                id: Some(codex_protocol::ResponseItemId::from_server(
                    "amsg_01900000-0000-7000-8000-000000000017".to_string(),
                )),
                author: "/root/goal_supervisor".to_string(),
                recipient: "/root".to_string(),
                content: vec![
                    AgentMessageInputContent::InputText {
                        text: "Message Type: NEW_TASK\nTask name: /root\nSender: /root/goal_supervisor\nPayload:\n"
                            .to_string(),
                    },
                    AgentMessageInputContent::EncryptedContent {
                        encrypted_content: "synthetic poisoned supervisor instruction".to_string(),
                    },
                ],
                internal_chat_message_metadata_passthrough: Some(
                    InternalChatMessageMetadataPassthrough {
                        turn_id: Some(
                            "01900000-0000-7000-8000-000000000018".to_string(),
                        ),
                        ..Default::default()
                    },
                ),
            }
            .into(),
        ),
    });
    let mut bytes = Vec::new();
    for line in lines {
        serde_json::to_writer(&mut bytes, &line)?;
        bytes.push(b'\n');
    }
    std::fs::write(path, bytes)?;
    Ok(())
}

#[tokio::test]
async fn legacy_resume_preserves_history_mode_after_picker_server_replacement() -> Result<()> {
    let codex_home = tempfile::tempdir().expect("tempdir");
    let config = build_config(&codex_home).await;
    let thread_id = ThreadId::from_string(
        &create_fake_rollout(
            codex_home.path(),
            "2025-01-05T12-00-00",
            "2025-01-05T12:00:00Z",
            "Saved user message",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create source rollout"),
    )?;
    let mut picker_app_server = crate::start_embedded_app_server_for_picker(&config).await?;
    let history_mode = picker_app_server
        .thread_read(thread_id, /*include_turns*/ false)
        .await?
        .history_mode;
    picker_app_server.shutdown().await?;

    let mut app_server = crate::start_embedded_app_server_for_picker(&config).await?;
    app_server.remember_thread_history_mode(thread_id, history_mode);
    let next_request_id = app_server.next_request_id;
    let resumed = app_server
        .resume_thread(config, thread_id, ResumeModelSettings::RestoreFromThread)
        .await?;

    assert_eq!(app_server.next_request_id, next_request_id + 2);
    assert!(!resumed.turns.is_empty());
    app_server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn embedded_legacy_resume_does_not_reenter_rollout_maintenance_lock() -> Result<()> {
    let codex_home = tempfile::tempdir().expect("tempdir");
    let config = build_config(&codex_home).await;
    let filename_ts = "2026-08-17T08-00-00";
    let thread_id = ThreadId::from_string(
        &create_fake_rollout(
            codex_home.path(),
            filename_ts,
            "2026-08-17T08:00:00Z",
            "Saved user message",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create source rollout"),
    )?;
    poison_legacy_rollout_for_goal_supervisor_repair(codex_home.path(), filename_ts, thread_id)?;
    let mut app_server = crate::start_embedded_app_server_for_picker(&config).await?;
    app_server.remember_thread_history_mode(thread_id, ThreadHistoryMode::Legacy);

    let resumed = tokio::time::timeout(
        Duration::from_secs(2),
        app_server.resume_thread(config, thread_id, ResumeModelSettings::RestoreFromThread),
    )
    .await
    .expect("embedded resume must not wait for its own maintenance lock")?;

    assert_eq!(resumed.session.thread_id, thread_id);
    assert!(!resumed.turns.is_empty());
    app_server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn background_migration_disables_cached_legacy_resume_shortcut() -> Result<()> {
    let codex_home = tempfile::tempdir().expect("tempdir");
    let mut config = build_config(&codex_home).await;
    config
        .features
        .disable(Feature::BackgroundPaginatedRolloutMigration)?;
    let legacy_thread_id = ThreadId::from_string(
        &create_fake_rollout(
            codex_home.path(),
            "2025-01-05T12-00-00",
            "2025-01-05T12:00:00Z",
            "Saved legacy user message",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create legacy rollout"),
    )?;
    let thread_id = ThreadId::from_string(
        &create_fake_paginated_rollout(
            codex_home.path(),
            "2025-01-05T12-00-01",
            "2025-01-05T12:00:01Z",
            "Saved paginated user message",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create paginated rollout"),
    )?;
    let mut migration_config = config.clone();
    migration_config
        .features
        .enable(Feature::BackgroundPaginatedRolloutMigration)?;

    for (startup_config, resume_config) in
        [(&config, &migration_config), (&migration_config, &config)]
    {
        // Keep the real worker disabled while exercising both startup/request mismatches.
        let mut app_server = crate::start_embedded_app_server_for_picker(&config)
            .await?
            .with_startup_config(startup_config);
        app_server.remember_thread_history_mode(legacy_thread_id, ThreadHistoryMode::Legacy);
        let next_request_id = app_server.next_request_id;
        let legacy = app_server
            .resume_thread(
                resume_config.clone(),
                legacy_thread_id,
                ResumeModelSettings::RestoreFromThread,
            )
            .await?;
        assert_eq!(app_server.next_request_id, next_request_id + 2);
        assert!(!legacy.turns.is_empty());

        app_server.remember_thread_history_mode(thread_id, ThreadHistoryMode::Legacy);
        let next_request_id = app_server.next_request_id;

        let resumed = app_server
            .resume_thread(
                resume_config.clone(),
                thread_id,
                ResumeModelSettings::RestoreFromThread,
            )
            .await?;

        assert!(app_server.next_request_id > next_request_id + 1);
        assert_eq!(resumed.session.thread_id, thread_id);
        assert_eq!(
            app_server
                .history_pagination
                .get(&thread_id)
                .map(|state| state.history_mode),
            Some(ThreadHistoryMode::Paginated)
        );
        app_server.shutdown().await?;
    }

    Ok(())
}

#[tokio::test]
async fn rollout_maintenance_contention_disables_cached_legacy_resume_shortcut() -> Result<()> {
    let codex_home = tempfile::tempdir().expect("tempdir");
    let config = build_config(&codex_home).await;
    let thread_id = ThreadId::from_string(
        &create_fake_paginated_rollout(
            codex_home.path(),
            "2025-01-05T12-00-00",
            "2025-01-05T12:00:00Z",
            "Saved paginated user message",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create paginated rollout"),
    )?;
    let mut app_server = crate::start_embedded_app_server_for_picker(&config).await?;
    app_server.remember_thread_history_mode(thread_id, ThreadHistoryMode::Legacy);
    let maintenance_guard = codex_rollout::try_acquire_rollout_maintenance_lock(codex_home.path())?
        .expect("acquire rollout maintenance lock");
    let release_maintenance = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(maintenance_guard);
    });
    let next_request_id = app_server.next_request_id;

    let resumed = app_server
        .resume_thread(config, thread_id, ResumeModelSettings::RestoreFromThread)
        .await?;
    release_maintenance.await.expect("release maintenance lock");

    assert_eq!(app_server.next_request_id, next_request_id + 3);
    assert_eq!(resumed.session.thread_id, thread_id);
    assert_eq!(
        app_server
            .history_pagination
            .get(&thread_id)
            .map(|state| state.history_mode),
        Some(ThreadHistoryMode::Paginated)
    );

    app_server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn stale_legacy_history_mode_is_revalidated_before_resume() -> Result<()> {
    let codex_home = tempfile::tempdir().expect("tempdir");
    let config = build_config(&codex_home).await;
    let thread_id = ThreadId::from_string(
        &create_fake_paginated_rollout(
            codex_home.path(),
            "2025-01-05T12-00-00",
            "2025-01-05T12:00:00Z",
            "Saved paginated user message",
            Some(config.model_provider_id.as_str()),
            /*git_info*/ None,
        )
        .expect("create paginated rollout"),
    )?;
    let mut app_server = crate::start_embedded_app_server_for_picker(&config).await?;
    app_server.remember_thread_history_mode(thread_id, ThreadHistoryMode::Legacy);
    let next_request_id = app_server.next_request_id;

    let resumed = app_server
        .resume_thread(
            config.clone(),
            thread_id,
            ResumeModelSettings::RestoreFromThread,
        )
        .await?;
    assert_eq!(resumed.session.thread_id, thread_id);
    assert!(app_server.next_request_id >= next_request_id + 4);
    assert_eq!(
        app_server
            .history_pagination
            .get(&thread_id)
            .map(|state| state.history_mode),
        Some(ThreadHistoryMode::Paginated)
    );

    let missing_thread_id = ThreadId::new();
    app_server.remember_thread_history_mode(missing_thread_id, ThreadHistoryMode::Legacy);
    let next_request_id = app_server.next_request_id;
    assert!(
        app_server
            .resume_thread(
                config,
                missing_thread_id,
                ResumeModelSettings::RestoreFromThread
            )
            .await
            .is_err()
    );
    assert_eq!(app_server.next_request_id, next_request_id + 2);

    app_server.shutdown().await?;
    Ok(())
}
