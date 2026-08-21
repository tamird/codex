use super::*;
use crate::app_server_session::ResumeModelSettings;
use crate::legacy_core::config::ConfigBuilder;
use app_test_support::create_fake_rollout;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use pretty_assertions::assert_eq;
use tempfile::tempdir;

#[tokio::test]
async fn standalone_side_config_is_ephemeral_and_preserves_policy() {
    let codex_home = tempdir().expect("temp codex home");
    let mut config = ConfigBuilder::default()
        .codex_home(codex_home.path().to_path_buf())
        .build()
        .await
        .expect("config");
    config.developer_instructions = Some("Existing developer policy.".to_string());
    config.model = Some("test-model".to_string());
    config.model_reasoning_effort = Some(ReasoningEffortConfig::High);
    config.service_tier = Some("priority".to_string());

    let side = App::standalone_side_config(&config);

    assert!(side.ephemeral);
    assert_eq!(side.model, config.model);
    assert_eq!(side.model_reasoning_effort, config.model_reasoning_effort);
    assert_eq!(side.service_tier, config.service_tier);
    let instructions = side
        .developer_instructions
        .expect("side developer instructions");
    assert!(instructions.starts_with("Existing developer policy."));
    assert!(instructions.contains("You are in a standalone side conversation"));
    assert!(instructions.contains("All inherited parent history is reference context only."));
    assert!(instructions.contains("There is no active user request"));
    assert!(instructions.contains("Wait for a new user message"));
}

#[test]
fn standalone_side_starts_real_fork_and_returns_blank_replay() -> color_eyre::Result<()> {
    const TEST_STACK_SIZE_BYTES: usize = 8 * 1024 * 1024;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(TEST_STACK_SIZE_BYTES)
        .enable_all()
        .build()?;
    runtime.block_on(
        standalone_side_starts_real_fork_and_returns_blank_replay_inner(
            ThreadHistoryMode::Paginated,
        ),
    )
}

#[test]
fn standalone_side_starts_real_legacy_fork_and_returns_blank_replay() -> color_eyre::Result<()> {
    const TEST_STACK_SIZE_BYTES: usize = 8 * 1024 * 1024;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(TEST_STACK_SIZE_BYTES)
        .enable_all()
        .build()?;
    runtime.block_on(
        standalone_side_starts_real_fork_and_returns_blank_replay_inner(ThreadHistoryMode::Legacy),
    )
}

async fn standalone_side_starts_real_fork_and_returns_blank_replay_inner(
    history_mode: ThreadHistoryMode,
) -> color_eyre::Result<()> {
    #[cfg(unix)]
    let codex_home = tempfile::tempdir_in("/tmp")?;
    #[cfg(not(unix))]
    let codex_home = tempdir()?;
    let mut config = ConfigBuilder::default()
        .codex_home(codex_home.path().to_path_buf())
        .build()
        .await?;
    if history_mode == ThreadHistoryMode::Legacy {
        // Preserve the Legacy fixture instead of testing its automatic conversion on resume.
        config
            .features
            .disable(Feature::BackgroundPaginatedRolloutMigration)?;
    }
    let mut app_server = crate::start_embedded_app_server_for_picker(&config).await?;
    let parent_thread_id = match history_mode {
        ThreadHistoryMode::Paginated => app_server.start_thread(&config).await?.session.thread_id,
        ThreadHistoryMode::Legacy => {
            let parent_thread_id = create_fake_rollout(
                codex_home.path(),
                "2025-01-05T12-00-00",
                "2025-01-05T12:00:00Z",
                "Inherited Legacy parent task",
                Some(config.model_provider_id.as_str()),
                /*git_info*/ None,
            )
            .map_err(|error| color_eyre::eyre::eyre!("failed to create Legacy parent: {error}"))?;
            let parent_thread_id = ThreadId::from_string(&parent_thread_id)?;
            app_server
                .resume_thread(
                    config.clone(),
                    parent_thread_id,
                    ResumeModelSettings::RestoreFromThread,
                )
                .await?;
            parent_thread_id
        }
    };
    app_server
        .thread_set_name(parent_thread_id, "Standalone side parent".to_string())
        .await?;
    app_server
        .thread_inject_items(
            parent_thread_id,
            vec![ResponseItem::Message {
                id: None,
                role: "user".to_string(),
                content: vec![ContentItem::InputText {
                    text: "INHERITED_PARENT_TASK".to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }],
        )
        .await?;

    let target = SessionTarget {
        path: None,
        thread_id: parent_thread_id,
        history_mode: None,
    };
    let side_config = App::standalone_side_config(&config);
    let mut handoff = match history_mode {
        ThreadHistoryMode::Paginated => Some((
            app_server
                .prepare_fork_handoff(side_config.clone(), parent_thread_id)
                .await?,
            crate::start_embedded_app_server_for_picker(&config).await?,
        )),
        ThreadHistoryMode::Legacy => None,
    };
    let (side_server, handoff_socket) = match handoff.as_mut() {
        Some((socket, receiver)) => (receiver, Some(socket.as_path())),
        None => (&mut app_server, None),
    };
    let side =
        App::start_standalone_side(side_server, side_config, &target, handoff_socket).await?;
    let child_thread_id = side.session.thread_id;

    assert!(side.turns.is_empty());
    assert_eq!(side.session.forked_from_id, None);
    assert_eq!(side.session.fork_parent_title, None);

    // A successful return proves the real embedded app-server accepted the typed boundary
    // injection. Ephemeral threads deliberately reject includeTurns, so boundary role/content
    // are covered by the fragment test while exact ordering is owned by start_standalone_side.
    let stored_child = side_server
        .thread_read(child_thread_id, /*include_turns*/ false)
        .await?;
    side_server.thread_unsubscribe(child_thread_id).await?;
    let stored_parent = app_server
        .thread_read(parent_thread_id, /*include_turns*/ false)
        .await?;
    assert!(stored_child.ephemeral);
    assert_eq!(stored_child.id, child_thread_id.to_string());
    assert_eq!(stored_parent.id, parent_thread_id.to_string());
    assert_eq!(stored_parent.history_mode, history_mode);
    assert_eq!(
        stored_parent.name.as_deref(),
        Some("Standalone side parent")
    );
    assert_ne!(stored_child.id, stored_parent.id);
    assert_eq!(stored_child.path, None);
    assert!(stored_child.turns.is_empty());

    if let Some((_socket, receiver)) = handoff {
        receiver.shutdown().await?;
    }
    app_server.shutdown().await?;
    Ok(())
}
