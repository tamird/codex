use super::*;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use pretty_assertions::assert_eq;

fn stored_root(config: &Config) -> StoredThread {
    let now = chrono::Utc::now();
    StoredThread {
        thread_id: ThreadId::new(),
        extra_config: None,
        rollout_path: Some(
            config
                .codex_home
                .join("sessions/original.jsonl")
                .to_path_buf(),
        ),
        forked_from_id: None,
        parent_thread_id: None,
        preview: "original root".to_string(),
        name: None,
        model_provider: config.model_provider_id.clone(),
        model: Some("original-model".to_string()),
        reasoning_effort: Some(ReasoningEffort::High),
        created_at: now,
        updated_at: now,
        recency_at: now,
        archived_at: None,
        section: None,
        section_position: None,
        section_entered_at: None,
        project_id: None,
        cwd: config.cwd.as_path().join("original-workspace"),
        cli_version: "test".to_string(),
        source: SessionSource::Cli,
        history_mode: ThreadHistoryMode::Legacy,
        thread_source: Some(ThreadSource::User),
        agent_nickname: None,
        agent_role: None,
        agent_path: None,
        git_info: None,
        approval_mode: config.permissions.approval_policy.value(),
        permission_profile: config.permissions.permission_profile().clone(),
        token_usage: None,
        first_user_message: None,
        history: None,
    }
}

fn session_history(thread_id: ThreadId) -> Vec<RolloutItem> {
    vec![RolloutItem::SessionMeta(SessionMetaLine {
        meta: SessionMeta {
            id: thread_id,
            session_id: SessionId::from(thread_id),
            source: SessionSource::Cli,
            thread_source: Some(ThreadSource::User),
            ..Default::default()
        },
        git: None,
    })]
}

#[tokio::test]
async fn cold_root_config_preserves_original_provider_model_reasoning_and_permissions() {
    let mut config = crate::config::test_config().await;
    config.ephemeral = true;
    config.workspace_roots_explicit = true;
    config.workspace_roots = vec![config.cwd.clone()];
    config
        .permissions
        .set_workspace_roots(config.workspace_roots.clone());
    let original_provider = config.model_provider.clone();
    config
        .model_providers
        .insert("original-provider".to_string(), original_provider.clone());
    let mut stored_thread = stored_root(&config);
    stored_thread.model_provider = "original-provider".to_string();
    let original_workspace_roots = vec![
        stored_thread
            .cwd
            .clone()
            .try_into()
            .expect("the original working directory should be absolute"),
        stored_thread
            .cwd
            .join("additional-root")
            .try_into()
            .expect("the additional original workspace root should be absolute"),
    ];

    let restored = restore_cold_root_config(
        config,
        &stored_thread,
        Some(original_workspace_roots.clone()),
    )
    .await
    .expect("a persisted root should restore its original configuration");

    assert_eq!(restored.model_provider_id, "original-provider");
    assert!(!restored.ephemeral);
    assert_eq!(restored.model_provider, original_provider);
    assert_eq!(restored.model.as_deref(), Some("original-model"));
    assert_eq!(restored.model_reasoning_effort, Some(ReasoningEffort::High));
    assert_eq!(restored.cwd.as_path(), stored_thread.cwd.as_path());
    assert!(restored.workspace_roots_explicit);
    assert_eq!(restored.workspace_roots, original_workspace_roots);
    assert_eq!(
        restored.permissions.workspace_roots(),
        restored.workspace_roots.as_slice()
    );
    assert_eq!(
        restored.permissions.approval_policy.value(),
        stored_thread.approval_mode
    );
    assert_eq!(
        restored.permissions.permission_profile(),
        &stored_thread.permission_profile
    );
}

#[tokio::test]
async fn cold_root_config_rejects_an_unavailable_original_provider() {
    let config = crate::config::test_config().await;
    let mut stored_thread = stored_root(&config);
    stored_thread.model_provider = "unavailable-provider".to_string();

    let error = restore_cold_root_config(config, &stored_thread, /*workspace_roots*/ None)
        .await
        .expect_err("adoption must not silently replace the original provider");

    assert!(matches!(
        error.details(),
        CodexErrorDetails::InvalidRequest(message)
            if message.contains("unavailable-provider")
                && message.contains(&stored_thread.thread_id.to_string())
    ));
}

#[test]
fn normalize_adopted_root_preserves_thread_id_and_sets_parent_identity() {
    let thread_id = ThreadId::new();
    let parent_thread_id = ThreadId::new();
    let parent_session_id = SessionId::from(parent_thread_id);
    let agent_path = AgentPath::root()
        .join("adopted_worker")
        .expect("adopted agent path should be valid");
    let metadata = AgentMetadata {
        agent_id: Some(thread_id),
        agent_path: Some(agent_path.clone()),
        agent_nickname: Some("Worker".to_string()),
        agent_role: Some("worker".to_string()),
        ..Default::default()
    };
    let source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: Some(agent_path.clone()),
        agent_nickname: metadata.agent_nickname.clone(),
        agent_role: metadata.agent_role.clone(),
    });
    let mut history = session_history(thread_id);

    normalize_resumed_session_metadata(
        &mut history,
        thread_id,
        &source,
        Some(parent_thread_id),
        Some(&metadata),
        parent_session_id,
    )
    .expect("adopted session metadata should normalize");

    let RolloutItem::SessionMeta(line) = &history[0] else {
        panic!("first rollout item must remain session metadata");
    };
    assert_eq!(line.meta.id, thread_id);
    assert_eq!(line.meta.session_id, parent_session_id);
    assert_eq!(line.meta.parent_thread_id, Some(parent_thread_id));
    assert_eq!(line.meta.source, source);
    assert_eq!(line.meta.thread_source, Some(ThreadSource::Subagent));
    assert_eq!(line.meta.agent_path, Some(agent_path.to_string()));
    assert_eq!(line.meta.agent_nickname, Some("Worker".to_string()));
    assert_eq!(line.meta.agent_role, Some("worker".to_string()));
}

#[test]
fn normalize_promoted_agent_restores_independent_root_identity() {
    let thread_id = ThreadId::new();
    let parent_thread_id = ThreadId::new();
    let mut history = session_history(thread_id);
    let RolloutItem::SessionMeta(line) = &mut history[0] else {
        panic!("first rollout item must remain session metadata");
    };
    line.meta.session_id = SessionId::from(parent_thread_id);
    line.meta.parent_thread_id = Some(parent_thread_id);
    line.meta.source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth: 1,
        agent_path: None,
        agent_nickname: Some("Worker".to_string()),
        agent_role: Some("worker".to_string()),
    });
    line.meta.thread_source = Some(ThreadSource::Subagent);
    line.meta.agent_path = Some("/root/adopted_worker".to_string());
    line.meta.agent_nickname = Some("Worker".to_string());
    line.meta.agent_role = Some("worker".to_string());

    normalize_resumed_session_metadata(
        &mut history,
        thread_id,
        &SessionSource::Cli,
        /*parent_thread_id*/ None,
        /*agent_metadata*/ None,
        SessionId::from(thread_id),
    )
    .expect("promoted session metadata should normalize");

    let RolloutItem::SessionMeta(line) = &history[0] else {
        panic!("first rollout item must remain session metadata");
    };
    assert_eq!(line.meta.id, thread_id);
    assert_eq!(line.meta.session_id, SessionId::from(thread_id));
    assert_eq!(line.meta.parent_thread_id, None);
    assert_eq!(line.meta.source, SessionSource::Cli);
    assert_eq!(line.meta.thread_source, Some(ThreadSource::User));
    assert_eq!(line.meta.agent_path, None);
    assert_eq!(line.meta.agent_nickname, None);
    assert_eq!(line.meta.agent_role, None);
}

#[test]
fn normalize_promoted_original_root_preserves_original_thread_source() {
    let thread_id = ThreadId::new();
    let mut history = session_history(thread_id);
    let RolloutItem::SessionMeta(line) = &mut history[0] else {
        panic!("first rollout item must remain session metadata");
    };
    line.meta.thread_source = None;

    normalize_resumed_session_metadata(
        &mut history,
        thread_id,
        &SessionSource::Cli,
        /*parent_thread_id*/ None,
        /*agent_metadata*/ None,
        SessionId::from(thread_id),
    )
    .expect("an originally independent root should restore its original thread source");

    let RolloutItem::SessionMeta(line) = &history[0] else {
        panic!("first rollout item must remain session metadata");
    };
    assert_eq!(line.meta.source, SessionSource::Cli);
    assert_eq!(line.meta.thread_source, None);
}

#[test]
fn normalize_resumed_session_metadata_rejects_missing_metadata() {
    let thread_id = ThreadId::new();

    let err = normalize_resumed_session_metadata(
        &mut [],
        thread_id,
        &SessionSource::Cli,
        /*parent_thread_id*/ None,
        /*agent_metadata*/ None,
        SessionId::from(thread_id),
    )
    .expect_err("a transfer without session metadata must fail");

    assert!(matches!(
        err.details(),
        CodexErrorDetails::InvalidRequest(_)
    ));
}

#[test]
fn normalize_resumed_session_metadata_rejects_mismatched_thread_id() {
    let actual_thread_id = ThreadId::new();
    let requested_thread_id = ThreadId::new();
    let mut history = session_history(actual_thread_id);

    let err = normalize_resumed_session_metadata(
        &mut history,
        requested_thread_id,
        &SessionSource::Cli,
        /*parent_thread_id*/ None,
        /*agent_metadata*/ None,
        SessionId::from(requested_thread_id),
    )
    .expect_err("a transfer must not accept another thread's session metadata");

    assert!(matches!(
        err.details(),
        CodexErrorDetails::InvalidRequest(_)
    ));
}
