use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use codex_app_server_protocol::build_turns_from_rollout_items;
use codex_extension_items::ExtensionItem;
use codex_extension_items::image_generation::ImageGenerationFailure;
use codex_extension_items::image_generation::ImageGenerationItem;
use codex_protocol::AgentPath;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::config_types::ReasoningSummary;
use codex_protocol::items::ReasoningItem;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::mcp::McpResourceOrigin;
use codex_protocol::mcp::McpResourceOriginCheckpoint;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AgentMessageEvent;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ImageGenerationEndEvent;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SandboxPolicy;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadRolledBackEvent;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnContextItem;
use codex_protocol::protocol::TurnStartedEvent;
use codex_protocol::protocol::UserMessageEvent;
use codex_protocol::user_input::UserInput;
use codex_rollout::CompactedItem;
use codex_rollout::RolloutConfig;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_rollout::RolloutRecorder;
use pretty_assertions::assert_eq;
use serde_json::json;
use sha2::Digest;
use sha2::Sha256;
use tempfile::TempDir;

use super::LocalThreadStore;
use super::RolloutMigrationFailureReason;
use super::RolloutMigrationMode;
use super::RolloutMigrationOptions;
use super::RolloutMigrationPaths;
use super::RolloutMigrationProgress;
use super::RolloutMigrationRateLimiter;
use super::RolloutMigrationStatus;
use super::compressed_staged_rollout_path;
#[cfg(unix)]
use super::decompress_rollout_to_path;
use super::decompressed_staged_rollout_path;
use super::lineage::LegacyLineagePredecessor;
use super::lineage::hash_file;
use super::lineage::plan_legacy_lineage;
use super::lineage_journal::LineageMigrationJournal;
use super::lineage_journal::LineageMigrationPhase;
use super::lineage_journal::read_lineage_migration_journal;
use super::lineage_journal::write_lineage_migration_journal;
use super::lineage_stage::measure_legacy_lineage;
use super::lineage_stage::stage_legacy_lineage;
use super::migration_journal_path;
use super::rewritten_staged_rollout_path;
use super::staged_rollout_path;
use super::telemetry::RolloutMigrationTrigger;
use super::thread_history;
use super::write_migration_journal;
use crate::ItemSortKey;
use crate::ListItemsParams;
use crate::ListThreadsParams;
use crate::ListTurnsParams;
use crate::LoadThreadHistoryParams;
use crate::SortDirection;
use crate::StoredTurnItemsView;
use crate::ThreadMetadataPatch;
use crate::ThreadSortKey;
use crate::ThreadStore;
use crate::ThreadStoreError;
use crate::TurnPage;
use crate::UpdateThreadMetadataParams;
use crate::local::test_support::test_config;

const TIMESTAMP: &str = "2025-01-03T12:00:00Z";

fn write_rollout(
    home: &Path,
    thread_id: ThreadId,
    source: SessionSource,
    items: Vec<RolloutItem>,
) -> PathBuf {
    write_rollout_with_fork(home, thread_id, source, /*forked_from_id*/ None, items)
}

fn write_rollout_with_fork(
    home: &Path,
    thread_id: ThreadId,
    source: SessionSource,
    forked_from_id: Option<ThreadId>,
    items: Vec<RolloutItem>,
) -> PathBuf {
    let directory = home.join("sessions/2025/01/03");
    fs::create_dir_all(&directory).expect("create rollout directory");
    let path = directory.join(format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl"));
    let mut file = fs::File::create(&path).expect("create legacy rollout");
    let metadata = SessionMeta {
        session_id: thread_id.into(),
        id: thread_id,
        forked_from_id,
        timestamp: TIMESTAMP.to_string(),
        cwd: home.to_path_buf(),
        originator: "test-originator".to_string(),
        cli_version: "0.0.0".to_string(),
        source,
        model_provider: Some("test-provider".to_string()),
        ..SessionMeta::default()
    };
    let items = std::iter::once(RolloutItem::SessionMeta(SessionMetaLine {
        meta: metadata,
        git: None,
    }))
    .chain(items);
    for item in items {
        let line = RolloutLine {
            timestamp: TIMESTAMP.to_string(),
            ordinal: None,
            item,
        };
        writeln!(
            file,
            "{}",
            serde_json::to_string(&line).expect("serialize legacy record")
        )
        .expect("write legacy record");
    }
    path
}

fn write_legacy_segment(
    path: &Path,
    home: &Path,
    thread_id: ThreadId,
    segment_id: SegmentId,
    items: Vec<RolloutItem>,
) {
    fs::create_dir_all(path.parent().expect("segment parent")).expect("create segment directory");
    let mut file = fs::File::create(path).expect("create legacy segment");
    let metadata = SessionMeta {
        session_id: thread_id.into(),
        id: thread_id,
        segment_id: Some(segment_id),
        timestamp: TIMESTAMP.to_string(),
        cwd: home.to_path_buf(),
        originator: "test-originator".to_string(),
        cli_version: "0.0.0".to_string(),
        source: SessionSource::Cli,
        model_provider: Some("test-provider".to_string()),
        ..SessionMeta::default()
    };
    for item in std::iter::once(RolloutItem::SessionMeta(SessionMetaLine {
        meta: metadata,
        git: None,
    }))
    .chain(items)
    {
        let line = RolloutLine {
            timestamp: TIMESTAMP.to_string(),
            ordinal: None,
            item,
        };
        writeln!(
            file,
            "{}",
            serde_json::to_string(&line).expect("serialize legacy segment record")
        )
        .expect("write legacy segment record");
    }
}

fn write_paginated_segment(
    path: &Path,
    home: &Path,
    thread_id: ThreadId,
    segment_id: SegmentId,
    start_ordinal: u64,
    items: Vec<RolloutItem>,
) -> u64 {
    fs::create_dir_all(path.parent().expect("segment parent")).expect("create segment directory");
    let mut file = fs::File::create(path).expect("create Paginated segment");
    let metadata = SessionMeta {
        session_id: thread_id.into(),
        id: thread_id,
        segment_id: Some(segment_id),
        timestamp: TIMESTAMP.to_string(),
        cwd: home.to_path_buf(),
        originator: "test-originator".to_string(),
        cli_version: "0.0.0".to_string(),
        source: SessionSource::Cli,
        model_provider: Some("test-provider".to_string()),
        history_mode: ThreadHistoryMode::Paginated,
        ..SessionMeta::default()
    };
    let mut next_ordinal = start_ordinal;
    for item in std::iter::once(RolloutItem::SessionMeta(SessionMetaLine {
        meta: metadata,
        git: None,
    }))
    .chain(items)
    {
        let line = RolloutLine {
            timestamp: TIMESTAMP.to_string(),
            ordinal: Some(next_ordinal),
            item,
        };
        next_ordinal = next_ordinal.checked_add(1).expect("test ordinal");
        writeln!(
            file,
            "{}",
            serde_json::to_string(&line).expect("serialize Paginated segment record")
        )
        .expect("write Paginated segment record");
    }
    next_ordinal
}

fn set_paginated_subagent_history_start(path: &Path, boundary: u64) {
    let text = fs::read_to_string(path).expect("read Paginated rollout");
    let mut lines = text.lines();
    let mut first: RolloutLine = serde_json::from_str(lines.next().expect("session metadata line"))
        .expect("parse session metadata line");
    let RolloutItem::SessionMeta(metadata) = &mut first.item else {
        panic!("first rollout item must be session metadata");
    };
    metadata.meta.source = SessionSource::SubAgent(SubAgentSource::Other("test".to_string()));
    metadata.meta.subagent_history_start_ordinal = Some(boundary);
    let mut output = serde_json::to_string(&first).expect("serialize session metadata");
    output.push('\n');
    for line in lines {
        output.push_str(line);
        output.push('\n');
    }
    fs::write(path, output).expect("rewrite Paginated session metadata");
}

fn segment_reference(path: PathBuf, thread_id: ThreadId, segment_id: SegmentId) -> RolloutItem {
    RolloutItem::RolloutReference(RolloutReferenceItem {
        rollout_id: Some(thread_id),
        rollout_path: path,
        thread_id: Some(thread_id),
        rollout_timestamp: None,
        segment_id: Some(segment_id),
        max_depth: codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    })
}

fn move_to_archived(home: &Path, path: PathBuf) -> PathBuf {
    let directory = home.join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir_all(&directory).expect("create archived rollout directory");
    let archived_path = directory.join(path.file_name().expect("rollout filename"));
    fs::rename(path, &archived_path).expect("archive rollout");
    archived_path
}

fn compress_rollout(path: &Path) -> PathBuf {
    let compressed_path = path.with_extension("jsonl.zst");
    let compressed = zstd::stream::encode_all(
        fs::File::open(path).expect("open rollout"),
        /*level*/ 3,
    )
    .expect("compress rollout");
    fs::write(&compressed_path, compressed).expect("write compressed rollout");
    fs::remove_file(path).expect("remove plain rollout");
    compressed_path
}

fn user_message(text: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
        message: text.to_string(),
        ..UserMessageEvent::default()
    }))
}

fn agent_message(text: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
        message: text.to_string(),
        phase: None,
        memory_citation: None,
        delivery: None,
    }))
}

fn input_response_message(role: &str, text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: role.to_string(),
        content: vec![ContentItem::InputText {
            text: text.to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

fn rollout_response_item(item: ResponseItem) -> RolloutItem {
    RolloutItem::ResponseItem(item.into())
}

fn exec_completion(turn_id: &str, call_id: &str) -> RolloutItem {
    serde_json::from_value(json!({
        "type": "event_msg",
        "payload": {
            "type": "exec_command_end",
            "call_id": call_id,
            "turn_id": turn_id,
            "command": ["echo", "ok"],
            "cwd": "file:///tmp",
            "parsed_cmd": [],
            "source": "agent",
            "stdout": "ok",
            "stderr": "",
            "aggregated_output": "ok",
            "exit_code": 0,
            "duration": {"secs": 0, "nanos": 0},
            "formatted_output": "ok",
            "status": "completed"
        }
    }))
    .expect("build legacy exec completion")
}

fn item_completed(turn_id: &str, item_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
        thread_id: ThreadId::new(),
        turn_id: turn_id.to_string(),
        item: TurnItem::Reasoning(ReasoningItem {
            id: item_id.to_string(),
            summary_text: vec!["summary".to_string()],
            raw_content: Vec::new(),
        }),
        started_at_ms: None,
        completed_at_ms: 1_735_905_601_000,
    }))
}

fn turn_started(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_id: turn_id.to_string(),
        trace_id: None,
        started_at: Some(1_735_905_600),
        model_context_window: Some(258_400),
        collaboration_mode_kind: Default::default(),
    }))
}

fn turn_complete(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
        turn_id: turn_id.to_string(),
        last_agent_message: None,
        error: None,
        started_at: Some(1_735_905_600),
        completed_at: Some(1_735_905_601),
        duration_ms: Some(1_000),
        time_to_first_token_ms: Some(100),
    }))
}

fn completed_user_message(
    thread_id: ThreadId,
    turn_id: &str,
    item_id: &str,
    text: &str,
) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
        thread_id,
        turn_id: turn_id.to_string(),
        item: TurnItem::UserMessage(UserMessageItem {
            id: item_id.to_string(),
            client_id: None,
            content: vec![UserInput::Text {
                text: text.to_string(),
                text_elements: Vec::new(),
            }],
        }),
        started_at_ms: None,
        completed_at_ms: 1_735_905_601_000,
    }))
}

fn started(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
        turn_id: turn_id.to_string(),
        trace_id: None,
        started_at: Some(1_735_905_600),
        model_context_window: None,
        collaboration_mode_kind: Default::default(),
    }))
}

fn compacted(replacement_history: Vec<ResponseItem>) -> RolloutItem {
    RolloutItem::Compacted(CompactedItem {
        message: "checkpoint".to_string(),
        replacement_history: Some(replacement_history.into_iter().map(Into::into).collect()),
        mcp_resource_origins: None,
        window_number: Some(1),
        first_window_id: None,
        previous_window_id: None,
        window_id: None,
        segment_state_checkpoint: None,
    })
}

fn completed(turn_id: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
        turn_id: turn_id.to_string(),
        last_agent_message: None,
        error: None,
        started_at: Some(1_735_905_600),
        completed_at: Some(1_735_905_601),
        duration_ms: Some(1_000),
        time_to_first_token_ms: None,
    }))
}

fn bounded_subagent_items(cwd: &Path) -> Vec<RolloutItem> {
    vec![
        RolloutItem::Compacted(CompactedItem {
            message: "superseded checkpoint".repeat(1024),
            replacement_history: Some(Vec::new()),
            mcp_resource_origins: None,
            window_number: Some(1),
            first_window_id: None,
            previous_window_id: None,
            window_id: None,
            segment_state_checkpoint: None,
        }),
        RolloutItem::Compacted(CompactedItem {
            message: "latest checkpoint".to_string(),
            replacement_history: Some(vec![
                ResponseItem::Message {
                    id: None,
                    role: "user".to_string(),
                    content: vec![ContentItem::InputText {
                        text: "latest compacted context".to_string(),
                    }],
                    phase: None,
                    internal_chat_message_metadata_passthrough: None,
                }
                .into(),
            ]),
            mcp_resource_origins: None,
            window_number: Some(2),
            first_window_id: None,
            previous_window_id: None,
            window_id: None,
            segment_state_checkpoint: None,
        }),
        started("child-turn"),
        RolloutItem::TurnContext(TurnContextItem {
            turn_id: Some("child-turn".to_string()),
            cwd: serde_json::from_value(json!(cwd)).expect("absolute cwd"),
            workspace_roots: None,
            current_date: None,
            timezone: None,
            approval_policy: AskForApproval::Never,
            approvals_reviewer: None,
            sandbox_policy: SandboxPolicy::new_read_only_policy(),
            permission_profile: None,
            active_permission_profile: None,
            network: None,
            file_system_sandbox_policy: None,
            model: "test-model".to_string(),
            comp_hash: None,
            personality: None,
            collaboration_mode: None,
            multi_agent_version: None,
            multi_agent_mode: None,
            realtime_active: None,
            cyber_access_program: None,
            effort: None,
            service_tier: None,
            model_profile: None,
            summary: ReasoningSummary::Auto,
        }),
        user_message("child question"),
        agent_message("child answer"),
        completed("child-turn"),
    ]
}

fn read_rollout(path: &Path) -> Vec<RolloutLine> {
    fs::read_to_string(path)
        .expect("read migrated rollout")
        .lines()
        .map(|line| serde_json::from_str(line).expect("parse migrated rollout"))
        .collect()
}

fn decoded_rollout_bytes(path: &Path) -> Vec<u8> {
    if path.extension().is_some_and(|extension| extension == "zst") {
        zstd::stream::decode_all(fs::File::open(path).expect("open compressed rollout"))
            .expect("decode compressed rollout")
    } else {
        fs::read(path).expect("read rollout")
    }
}

fn assert_manifest_target_matches_published_rollout(
    target: &super::RolloutMigrationLineageTarget,
    path: &Path,
) {
    let bytes = decoded_rollout_bytes(path);
    assert_eq!(
        target.byte_count,
        u64::try_from(bytes.len()).expect("payload byte count fits u64")
    );
    assert_eq!(target.sha256, format!("{:x}", Sha256::digest(&bytes)));
    assert_eq!(
        target.record_count,
        u64::try_from(bytes.iter().filter(|byte| **byte == b'\n').count())
            .expect("record count fits u64")
    );
}

fn set_history_base(path: &Path, history_base: HistoryPosition) {
    let contents = fs::read_to_string(path).expect("read rollout");
    let mut lines = contents.lines();
    let mut head: serde_json::Value =
        serde_json::from_str(lines.next().expect("session metadata")).expect("parse metadata");
    head["payload"]["history_base"] =
        serde_json::to_value(history_base).expect("serialize history base");
    let mut updated = serde_json::to_string(&head).expect("serialize metadata");
    for line in lines {
        updated.push('\n');
        updated.push_str(line);
    }
    updated.push('\n');
    fs::write(path, updated).expect("write history base");
}

async fn assert_no_migration_artifacts(home: &Path, path: &Path, thread_id: ThreadId) {
    let journal_path = migration_journal_path(home, thread_id);
    assert!(
        !journal_path.exists(),
        "unexpected journal: {}",
        fs::read_to_string(&journal_path).unwrap_or_else(|error| error.to_string())
    );
    assert!(!staged_rollout_path(path).expect("staged path").exists());
    assert!(
        !rewritten_staged_rollout_path(&staged_rollout_path(path).expect("staged path"))
            .expect("rewritten path")
            .exists()
    );
    assert!(
        !compressed_staged_rollout_path(path)
            .expect("compressed staged path")
            .exists()
    );
    assert!(
        !decompressed_staged_rollout_path(path)
            .expect("decompressed path")
            .exists()
    );
}

async fn projection_checkpoint(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> Option<(u64, u64)> {
    thread_history::projection_state(store, thread_id)
        .await
        .expect("read projection checkpoint")
        .map(|state| (state.next_byte_offset, state.next_ordinal))
}

async fn history_row_counts(store: &LocalThreadStore, thread_id: ThreadId) -> (i64, i64) {
    sqlx::query_as::<_, (i64, i64)>(
        r#"
SELECT
    (SELECT COUNT(*) FROM thread_turns WHERE thread_id = ?),
    (SELECT COUNT(*) FROM thread_items WHERE thread_id = ?)
        "#,
    )
    .bind(thread_id.to_string())
    .bind(thread_id.to_string())
    .fetch_one(store.thread_history_db().await.expect("thread history db"))
    .await
    .expect("read thread history row counts")
}

fn apply_options() -> RolloutMigrationOptions {
    RolloutMigrationOptions {
        mode: RolloutMigrationMode::Apply,
        max_mib_per_second: Some(1024),
        ..RolloutMigrationOptions::default()
    }
}

fn assert_failed_with_reason(
    outcome: &super::RolloutMigrationOutcome,
    failure_reason: RolloutMigrationFailureReason,
) {
    assert_eq!(
        (outcome.status, outcome.failure_reason),
        (RolloutMigrationStatus::Failed, Some(failure_reason))
    );
}

async fn indexed_store(home: &Path) -> LocalThreadStore {
    let config = test_config(home);
    let rollout_config = RolloutConfig {
        codex_home: config.codex_home.clone(),
        sqlite: config.sqlite.clone(),
        cwd: home.to_path_buf(),
        model_provider_id: config.default_model_provider_id.clone(),
        generate_memories: false,
    };
    let state_db = codex_rollout::state_db::try_init(&rollout_config)
        .await
        .expect("backfill legacy thread metadata");
    LocalThreadStore::new(config, Some(state_db))
}

async fn list_active_summary_turns(store: &LocalThreadStore, thread_id: ThreadId) -> TurnPage {
    store
        .list_turns(ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: None,
            page_size: 10,
            sort_direction: SortDirection::Asc,
            items_view: StoredTurnItemsView::Summary,
        })
        .await
        .expect("read projected turns")
}

#[tokio::test]
async fn migration_publishes_canonical_projected_history_and_is_idempotent() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            user_message("first question"),
            agent_message("first answer"),
        ],
    );
    let store = indexed_store(home.path()).await;

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate legacy rollout");
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(
        report.outcomes[0].status,
        RolloutMigrationStatus::Migrated,
        "{:?}",
        report.outcomes[0].message
    );

    let lines = read_rollout(&path);
    assert_eq!(
        lines.iter().map(|line| line.ordinal).collect::<Vec<_>>(),
        (0..lines.len() as u64).map(Some).collect::<Vec<_>>()
    );
    assert!(matches!(
        &lines[0].item,
        RolloutItem::SessionMeta(metadata)
            if metadata.meta.history_mode == ThreadHistoryMode::Paginated
                && metadata.meta.id == thread_id
                && metadata.meta.history_base.is_none()
    ));
    assert_eq!(
        lines
            .iter()
            .filter(|line| matches!(line.item, RolloutItem::EventMsg(EventMsg::ItemCompleted(_))))
            .count(),
        2
    );

    let turns = list_active_summary_turns(&store, thread_id).await;
    assert_eq!(turns.turns.len(), 1);
    assert_eq!(turns.turns[0].items.len(), 2);

    let bytes = fs::read(&path).expect("read first migration");
    let second = store
        .migrate_rollouts(apply_options())
        .await
        .expect("rerun migration");
    assert_eq!(
        second.outcomes[0].status,
        RolloutMigrationStatus::AlreadyPaginated
    );
    assert_eq!(fs::read(&path).expect("read idempotent rollout"), bytes);
}

#[tokio::test]
async fn migration_skips_non_selected_reverted_rollout_and_projects_selected_rollout_id() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let rollout_id = ThreadId::new();
    let old_path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            user_message("selected question"),
            agent_message("selected answer"),
        ],
    );
    let selected_path = old_path.with_file_name(format!(
        "rollout-2025-01-03T12-00-01-{thread_id}_{rollout_id}.jsonl"
    ));
    fs::copy(&old_path, &selected_path).expect("create replacement physical rollout");
    let old_bytes = fs::read(&old_path).expect("read original rollout");
    let store = indexed_store(home.path()).await;
    let state_db = store.state_db.as_ref().expect("state db");
    let mut metadata = state_db
        .get_thread(thread_id)
        .await
        .expect("read thread metadata")
        .expect("thread metadata");
    metadata.rollout_path = selected_path.clone();
    state_db
        .upsert_thread(&metadata)
        .await
        .expect("select replacement physical rollout");

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate selected rollout");

    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(report.outcomes[0].thread_id, Some(thread_id));
    assert_eq!(report.outcomes[0].rollout_path, selected_path);
    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Migrated);
    assert_eq!(
        fs::read(&old_path).expect("read original rollout"),
        old_bytes
    );
    assert!(matches!(
        read_rollout(&old_path).first().map(|line| &line.item),
        Some(RolloutItem::SessionMeta(metadata))
            if metadata.meta.history_mode == ThreadHistoryMode::Legacy
    ));
    assert!(matches!(
        read_rollout(&selected_path).first().map(|line| &line.item),
        Some(RolloutItem::SessionMeta(metadata))
            if metadata.meta.history_mode == ThreadHistoryMode::Paginated
    ));
    assert!(
        thread_history::projection_state(&store, thread_id)
            .await
            .expect("read stable-id projection")
            .is_none()
    );
    let projection = thread_history::projection_state(&store, rollout_id)
        .await
        .expect("read physical rollout projection")
        .expect("physical rollout projection");
    assert_eq!(
        projection.next_byte_offset,
        fs::metadata(&selected_path)
            .expect("selected rollout metadata")
            .len()
    );
    let turns = list_active_summary_turns(&store, thread_id).await;
    assert_eq!(turns.turns.len(), 1);
    assert_eq!(turns.turns[0].items.len(), 2);
}

#[tokio::test]
async fn migration_attributes_corrupt_reverted_filename_to_stable_thread_id() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let rollout_id = ThreadId::new();
    let directory = home.path().join("sessions/2025/01/03");
    fs::create_dir_all(&directory).expect("create rollout directory");
    let path = directory.join(format!(
        "rollout-2025-01-03T12-00-00-{thread_id}_{rollout_id}.jsonl"
    ));
    fs::write(&path, "{not json}\n").expect("write corrupt rollout");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let selected = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![thread_id],
            ..RolloutMigrationOptions::default()
        })
        .await
        .expect("inspect corrupt stable thread");
    assert_eq!(selected.outcomes.len(), 1);
    assert_eq!(selected.outcomes[0].thread_id, Some(thread_id));
    assert_eq!(selected.outcomes[0].status, RolloutMigrationStatus::Failed);

    let physical_only = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![rollout_id],
            ..RolloutMigrationOptions::default()
        })
        .await
        .expect("physical rollout ID must not select stable thread");
    assert!(physical_only.outcomes.is_empty());
}

#[tokio::test]
async fn migration_supports_authenticated_noncanonical_selected_rollout() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let canonical_path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            user_message("imported question"),
            agent_message("imported answer"),
        ],
    );
    let imported_path = canonical_path.with_file_name("rollout-imported.jsonl");
    fs::rename(&canonical_path, &imported_path).expect("rename imported rollout");
    let competing_thread_id = ThreadId::new();
    let competing_path = write_rollout(
        home.path(),
        competing_thread_id,
        SessionSource::Cli,
        vec![user_message("newer competing question")],
    );
    let newer_directory = home.path().join("sessions/2025/01/04");
    fs::create_dir_all(&newer_directory).expect("create newer directory");
    fs::rename(
        &competing_path,
        newer_directory.join(competing_path.file_name().expect("competing filename")),
    )
    .expect("move competing rollout");
    let store = indexed_store(home.path()).await;
    assert_eq!(
        store
            .state_db
            .as_ref()
            .expect("state db")
            .get_thread(thread_id)
            .await
            .expect("read imported thread")
            .expect("imported thread metadata")
            .rollout_path,
        imported_path
    );

    let migrated = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![thread_id],
            ..apply_options()
        })
        .await
        .expect("migrate imported rollout");
    assert_eq!(migrated.outcomes.len(), 1);
    assert_eq!(
        migrated.outcomes[0].status,
        RolloutMigrationStatus::Migrated
    );
    assert!(matches!(
        read_rollout(&imported_path).first().map(|line| &line.item),
        Some(RolloutItem::SessionMeta(metadata))
            if metadata.meta.history_mode == ThreadHistoryMode::Paginated
    ));
    let projection = thread_history::projection_state(&store, thread_id)
        .await
        .expect("read imported projection")
        .expect("imported projection");
    assert_eq!(
        projection.next_byte_offset,
        fs::metadata(&imported_path)
            .expect("imported rollout metadata")
            .len()
    );
    assert_eq!(
        list_active_summary_turns(&store, thread_id)
            .await
            .turns
            .len(),
        1
    );

    thread_history::delete_thread(&store, thread_id)
        .await
        .expect("simulate missing imported projection");
    let journal_path = migration_journal_path(home.path(), thread_id);
    write_migration_journal(&journal_path)
        .await
        .expect("simulate imported recovery journal");
    let recovered = store
        .migrate_rollouts(apply_options())
        .await
        .expect("recover imported rollout");
    assert_eq!(recovered.outcomes.len(), 2);
    assert_eq!(recovered.outcomes[0].thread_id, Some(thread_id));
    assert_eq!(
        recovered.outcomes[0].status,
        RolloutMigrationStatus::Migrated
    );
    assert_eq!(recovered.outcomes[1].thread_id, Some(competing_thread_id));
    assert_no_migration_artifacts(home.path(), &imported_path, thread_id).await;
    assert_eq!(
        list_active_summary_turns(&store, thread_id)
            .await
            .turns
            .len(),
        1
    );
}

#[tokio::test]
async fn migration_refuses_segmented_legacy_history_without_mutation() {
    let home = TempDir::new().expect("create Codex home");
    let parent_id = ThreadId::new();
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![user_message("segmented question")],
    );
    set_history_base(
        &path,
        HistoryPosition {
            thread_id: parent_id,
            end_ordinal_exclusive: 7,
            end_byte_offset: 123,
        },
    );
    let original = fs::read(&path).expect("read segmented rollout");
    let store = indexed_store(home.path()).await;
    thread_history::apply_projection(
        &store,
        thread_id,
        /*start_offset*/ 0,
        /*next_offset*/ 0,
        /*initial_ordinal*/ 0,
        Vec::new(),
    )
    .await
    .expect("seed projection checkpoint");
    let projection_before = projection_checkpoint(&store, thread_id).await;
    let before_mode = store
        .state_db
        .as_ref()
        .expect("state db")
        .get_thread(thread_id)
        .await
        .expect("read thread metadata")
        .expect("thread metadata")
        .history_mode;

    for options in [RolloutMigrationOptions::default(), apply_options()] {
        let report = store
            .migrate_rollouts(RolloutMigrationOptions {
                thread_ids: vec![thread_id],
                ..options
            })
            .await
            .expect("refuse segmented migration");
        assert_eq!(report.outcomes.len(), 1);
        assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Failed);
        assert!(
            report.outcomes[0]
                .message
                .as_deref()
                .is_some_and(|message| message.contains("segmented legacy rollout"))
        );
    }

    assert_eq!(fs::read(&path).expect("read refused rollout"), original);
    assert_eq!(
        projection_checkpoint(&store, thread_id).await,
        projection_before
    );
    assert_eq!(
        store
            .state_db
            .as_ref()
            .expect("state db")
            .get_thread(thread_id)
            .await
            .expect("read thread metadata")
            .expect("thread metadata")
            .history_mode,
        before_mode
    );
    assert_no_migration_artifacts(home.path(), &path, thread_id).await;
}

#[tokio::test]
async fn lineage_migration_plan_authenticates_and_orders_segmented_legacy_sources() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let rollout_file = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
    let segment_ids = [SegmentId::new(), SegmentId::new(), SegmentId::new()];
    let immutable_root = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string());
    let oldest_path = immutable_root
        .join(segment_ids[0].to_string())
        .join(rollout_file.as_str());
    write_legacy_segment(
        oldest_path.as_path(),
        home.path(),
        thread_id,
        segment_ids[0],
        vec![user_message("oldest")],
    );
    let middle_path = immutable_root
        .join(segment_ids[1].to_string())
        .join(rollout_file.as_str());
    write_legacy_segment(
        middle_path.as_path(),
        home.path(),
        thread_id,
        segment_ids[1],
        vec![
            segment_reference(oldest_path.clone(), thread_id, segment_ids[0]),
            user_message("middle"),
        ],
    );
    let active_path = home.path().join("sessions/2025/01/03").join(rollout_file);
    write_legacy_segment(
        active_path.as_path(),
        home.path(),
        thread_id,
        segment_ids[2],
        vec![
            segment_reference(middle_path.clone(), thread_id, segment_ids[1]),
            user_message("active"),
        ],
    );

    let original = [
        fs::read(oldest_path.as_path()).expect("read oldest"),
        fs::read(middle_path.as_path()).expect("read middle"),
        fs::read(active_path.as_path()).expect("read active"),
    ];
    let plan = plan_legacy_lineage(home.path(), active_path.as_path())
        .await
        .expect("plan segmented lineage");

    assert_eq!(plan.selected_thread_id, thread_id);
    assert_eq!(plan.selected_rollout_id, thread_id);
    assert_eq!(plan.sources.len(), 3);
    assert_eq!(plan.targets.len(), 3);
    assert!(
        plan.sources
            .iter()
            .all(|source| source.thread_id == thread_id && source.rollout_id == thread_id)
    );
    assert_eq!(
        plan.sources
            .iter()
            .map(|source| source.path.clone())
            .collect::<Vec<_>>(),
        vec![
            fs::canonicalize(&oldest_path).expect("canonical oldest source"),
            fs::canonicalize(&middle_path).expect("canonical middle source"),
            active_path.clone()
        ]
    );
    assert_eq!(
        plan.sources
            .iter()
            .map(|source| source.segment_id)
            .collect::<Vec<_>>(),
        segment_ids.into_iter().map(Some).collect::<Vec<_>>()
    );
    assert!(plan.sources[0].predecessor.is_none());
    for source in &plan.sources[1..] {
        assert!(matches!(
            source.predecessor,
            Some(LegacyLineagePredecessor::RolloutReference(_))
        ));
    }
    assert!(
        plan.sources
            .iter()
            .all(|source| source.history_mode == ThreadHistoryMode::Legacy
                && source.record_count >= 2
                && source.byte_count > 0
                && source.sha256.len() == 64)
    );
    assert_eq!(
        plan.targets.iter().filter(|target| target.selected).count(),
        1
    );
    assert!(plan.targets.last().is_some_and(|target| target.selected));
    assert_ne!(
        plan.targets.last().expect("selected target").rollout_id,
        plan.selected_rollout_id
    );
    assert!(plan.targets.iter().all(|target| {
        target.thread_id == thread_id
            && target.segment_id.is_some()
            && target.path.starts_with(home.path())
            && !plan.sources.iter().any(|source| source.path == target.path)
    }));
    assert!(plan.targets[0].predecessor_segment_id.is_none());
    for index in 1..plan.targets.len() {
        assert_eq!(
            plan.targets[index].predecessor_segment_id,
            plan.targets[index - 1].segment_id
        );
    }
    assert_eq!(
        [
            fs::read(oldest_path).expect("reread oldest"),
            fs::read(middle_path).expect("reread middle"),
            fs::read(&active_path).expect("reread active"),
        ],
        original
    );

    let repeated = plan_legacy_lineage(home.path(), active_path.as_path())
        .await
        .expect("repeat segmented lineage plan");
    assert_eq!(
        repeated
            .sources
            .iter()
            .map(|source| (
                source.path.clone(),
                source.byte_count,
                source.record_count,
                source.sha256.clone(),
            ))
            .collect::<Vec<_>>(),
        plan.sources
            .iter()
            .map(|source| (
                source.path.clone(),
                source.byte_count,
                source.record_count,
                source.sha256.clone(),
            ))
            .collect::<Vec<_>>()
    );
    assert_eq!(repeated.targets, plan.targets);
}

#[tokio::test]
async fn lineage_migration_targets_are_scoped_to_divergent_selected_lineages() {
    let home = TempDir::new().expect("create Codex home");
    let shared_thread_id = ThreadId::new();
    let shared_segment_id = SegmentId::new();
    let shared_filename = format!("rollout-2025-01-03T12-00-00-{shared_thread_id}.jsonl");
    let shared_path = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(shared_thread_id.to_string())
        .join(shared_segment_id.to_string())
        .join(shared_filename);
    write_legacy_segment(
        shared_path.as_path(),
        home.path(),
        shared_thread_id,
        shared_segment_id,
        vec![user_message("shared predecessor")],
    );

    let selected_paths = [ThreadId::new(), ThreadId::new()].map(|thread_id| {
        let selected_path = home
            .path()
            .join("sessions/2025/01/03")
            .join(format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl"));
        write_legacy_segment(
            selected_path.as_path(),
            home.path(),
            thread_id,
            SegmentId::new(),
            vec![
                segment_reference(shared_path.clone(), shared_thread_id, shared_segment_id),
                user_message(thread_id.to_string().as_str()),
            ],
        );
        selected_path
    });
    let first = plan_legacy_lineage(home.path(), selected_paths[0].as_path())
        .await
        .expect("plan first divergent lineage");
    let second = plan_legacy_lineage(home.path(), selected_paths[1].as_path())
        .await
        .expect("plan second divergent lineage");

    let shared_path = fs::canonicalize(shared_path).expect("canonical shared source");
    assert_eq!(first.sources[0].path, shared_path);
    assert_eq!(second.sources[0].path, shared_path);
    assert_ne!(first.selected_thread_id, second.selected_thread_id);
    assert_ne!(first.targets[0].path, second.targets[0].path);
    assert_ne!(first.targets[0].segment_id, second.targets[0].segment_id);
    assert_ne!(first.targets[0].rollout_id, second.targets[0].rollout_id);
    assert_eq!(
        plan_legacy_lineage(home.path(), selected_paths[0].as_path())
            .await
            .expect("repeat first divergent lineage")
            .targets,
        first.targets
    );
}

#[tokio::test]
async fn lineage_migration_stages_one_contiguous_paginated_ordinal_space() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let segment_ids = [SegmentId::new(), SegmentId::new(), SegmentId::new()];
    let filename = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
    let immutable_root = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string());
    let oldest = immutable_root
        .join(segment_ids[0].to_string())
        .join(filename.as_str());
    write_legacy_segment(
        oldest.as_path(),
        home.path(),
        thread_id,
        segment_ids[0],
        vec![user_message("oldest")],
    );
    let middle = immutable_root
        .join(segment_ids[1].to_string())
        .join(filename.as_str());
    write_legacy_segment(
        middle.as_path(),
        home.path(),
        thread_id,
        segment_ids[1],
        vec![
            segment_reference(oldest, thread_id, segment_ids[0]),
            user_message("middle"),
        ],
    );
    let active = home.path().join("sessions/2025/01/03").join(filename);
    write_legacy_segment(
        active.as_path(),
        home.path(),
        thread_id,
        segment_ids[2],
        vec![
            segment_reference(middle, thread_id, segment_ids[1]),
            user_message("active"),
        ],
    );
    let plan = plan_legacy_lineage(home.path(), active.as_path())
        .await
        .expect("plan lineage");
    let measured = measure_legacy_lineage(&plan)
        .await
        .expect("measure lineage without writing");
    let stage_root = home.path().join("rollout-migrations/staging-a");
    let staged = stage_legacy_lineage(&plan, stage_root.as_path())
        .await
        .expect("stage lineage");

    assert_eq!(staged.len(), 3);
    assert_eq!(measured.len(), staged.len());
    assert_eq!(
        measured
            .iter()
            .map(|target| (
                target.thread_id,
                target.rollout_id,
                target.segment_id,
                target.final_path.clone(),
                target.start_ordinal,
                target.end_ordinal_exclusive,
                target.byte_count,
                target.record_count,
                target.sha256.clone(),
                target.selected,
            ))
            .collect::<Vec<_>>(),
        staged
            .iter()
            .map(|target| (
                target.thread_id,
                target.rollout_id,
                target.segment_id,
                target.final_path.clone(),
                target.start_ordinal,
                target.end_ordinal_exclusive,
                target.byte_count,
                target.record_count,
                target.sha256.clone(),
                target.selected,
            ))
            .collect::<Vec<_>>()
    );
    assert!(staged.last().is_some_and(|target| target.selected));
    assert!(staged.iter().all(|target| {
        target.staged_path.starts_with(stage_root.as_path())
            && target.byte_count > 0
            && target.record_count > 0
            && target.sha256.len() == 64
            && !target.final_path.exists()
    }));
    let mut next_ordinal = 0_u64;
    for (index, target) in staged.iter().enumerate() {
        assert_eq!(target.start_ordinal, next_ordinal);
        let mut reader = codex_rollout::open_rollout_line_reader(target.staged_path.as_path())
            .await
            .expect("open staged target");
        let mut records = Vec::new();
        while let Some(raw) = reader.next_line().await.expect("read staged target") {
            let line =
                serde_json::from_str::<RolloutLine>(raw.as_str()).expect("parse staged line");
            assert_eq!(line.ordinal, Some(next_ordinal));
            next_ordinal += 1;
            records.push(line);
        }
        assert_eq!(target.end_ordinal_exclusive, next_ordinal);
        let RolloutItem::SessionMeta(metadata) = &records[0].item else {
            panic!("target must start with SessionMeta");
        };
        let expected_history_base = index.checked_sub(1).map(|predecessor| HistoryPosition {
            thread_id: staged[predecessor].rollout_id,
            end_ordinal_exclusive: staged[predecessor].end_ordinal_exclusive,
            end_byte_offset: staged[predecessor].byte_count,
        });
        assert_eq!(metadata.meta.history_base, expected_history_base);
        assert!(
            records
                .iter()
                .all(|line| !matches!(line.item, RolloutItem::RolloutReference(_)))
        );
    }

    let repeated = stage_legacy_lineage(
        &plan,
        home.path().join("rollout-migrations/staging-b").as_path(),
    )
    .await
    .expect("repeat stage lineage");
    assert_eq!(
        repeated
            .iter()
            .map(|target| (
                target.start_ordinal,
                target.end_ordinal_exclusive,
                target.byte_count,
                target.record_count,
                target.sha256.clone(),
            ))
            .collect::<Vec<_>>(),
        staged
            .iter()
            .map(|target| (
                target.start_ordinal,
                target.end_ordinal_exclusive,
                target.byte_count,
                target.record_count,
                target.sha256.clone(),
            ))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn migration_rewrites_segmented_paginated_references_as_native_history_base() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let predecessor_segment_id = SegmentId::new();
    let active_segment_id = SegmentId::new();
    let filename = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
    let predecessor_path = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(predecessor_segment_id.to_string())
        .join(filename.as_str());
    let predecessor_end = write_paginated_segment(
        predecessor_path.as_path(),
        home.path(),
        thread_id,
        predecessor_segment_id,
        /*start_ordinal*/ 0,
        vec![user_message("native migration predecessor")],
    );
    let active_path = home.path().join("sessions/2025/01/03").join(filename);
    write_paginated_segment(
        active_path.as_path(),
        home.path(),
        thread_id,
        active_segment_id,
        predecessor_end,
        vec![
            segment_reference(predecessor_path.clone(), thread_id, predecessor_segment_id),
            user_message("native migration active"),
        ],
    );
    let source_bytes = [
        fs::read(predecessor_path.as_path()).expect("read predecessor source"),
        fs::read(active_path.as_path()).expect("read active source"),
    ];
    let store = indexed_store(home.path()).await;

    let dry_run = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![thread_id],
            ..RolloutMigrationOptions::default()
        })
        .await
        .expect("dry-run native migration");
    assert_eq!(dry_run.outcomes.len(), 1);
    assert_eq!(dry_run.outcomes[0].status, RolloutMigrationStatus::Eligible);
    let manifest = dry_run.outcomes[0]
        .manifest
        .as_ref()
        .expect("native migration manifest");
    assert_eq!(manifest.sources.len(), 2);
    assert_eq!(manifest.targets.len(), 2);
    assert!(manifest.reference_dependencies.is_empty());
    assert_eq!(manifest.targets[0].history_base, None);
    assert_eq!(
        manifest.targets[1].history_base,
        Some(HistoryPosition {
            thread_id: manifest.targets[0].rollout_id,
            end_ordinal_exclusive: manifest.targets[0].end_ordinal_exclusive,
            end_byte_offset: manifest.targets[0].byte_count,
        })
    );

    let applied = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![thread_id],
            ..apply_options()
        })
        .await
        .expect("apply native migration");
    assert_eq!(applied.outcomes.len(), 1);
    assert_eq!(
        applied.outcomes[0].status,
        RolloutMigrationStatus::Migrated,
        "{:?}",
        applied.outcomes[0].message
    );
    let selected_path = applied.outcomes[0].rollout_path.clone();
    let selected_text = fs::read_to_string(selected_path.as_path()).expect("read selected target");
    assert!(!selected_text.contains("rollout_reference"));
    let selected_meta = codex_rollout::read_session_meta_line(selected_path.as_path())
        .await
        .expect("read selected metadata");
    assert_eq!(selected_meta.meta.id, thread_id);
    assert_eq!(
        selected_meta.meta.history_mode,
        ThreadHistoryMode::Paginated
    );
    let history_base = selected_meta
        .meta
        .history_base
        .expect("native history base");
    let native_predecessor =
        codex_rollout::find_rollout_path_by_rollout_id(home.path(), history_base.thread_id)
            .await
            .expect("resolve native predecessor")
            .expect("native predecessor exists");
    assert!(
        native_predecessor.starts_with(
            home.path()
                .join(codex_rollout::SESSIONS_SUBDIR)
                .join(codex_rollout::ROLLOUT_SEGMENTS_SUBDIR)
        ),
        "{}",
        native_predecessor.display()
    );
    assert_eq!(
        history_base.end_byte_offset,
        fs::metadata(native_predecessor.as_path())
            .expect("native predecessor metadata")
            .len()
    );
    assert!(
        !fs::read_to_string(native_predecessor.as_path())
            .expect("read native predecessor")
            .contains("rollout_reference")
    );

    let materialized =
        codex_rollout::materialize_rollout_lines(home.path(), selected_path.as_path())
            .await
            .expect("materialize native migration");
    let json = serde_json::to_string(&materialized).expect("serialize native migration");
    assert_eq!(json.matches("native migration predecessor").count(), 1);
    assert_eq!(json.matches("native migration active").count(), 1);
    let mut materialized_ordinals = materialized
        .iter()
        .filter_map(|line| line.ordinal)
        .collect::<Vec<_>>();
    materialized_ordinals.sort_unstable();
    assert_eq!(materialized_ordinals, vec![1, 2, 3]);
    assert_eq!(history_base.end_ordinal_exclusive, 2);

    let repeated = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![thread_id],
            ..apply_options()
        })
        .await
        .expect("repeat native migration");
    assert_eq!(repeated.outcomes.len(), 1);
    assert_eq!(
        repeated.outcomes[0].status,
        RolloutMigrationStatus::AlreadyPaginated
    );
    assert_eq!(repeated.outcomes[0].rollout_path, selected_path);
    assert_eq!(
        [
            fs::read(predecessor_path).expect("reread predecessor source"),
            fs::read(active_path).expect("reread active source"),
        ],
        source_bytes
    );
}

#[tokio::test]
async fn migration_rewrites_paginated_reference_lineage_deeper_than_desktop_bound() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let filename = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
    let mut predecessor = None;
    let mut next_ordinal = 0;
    let mut source_paths = Vec::new();
    for index in 0..4 {
        let segment_id = SegmentId::new();
        let path = if index == 3 {
            home.path().join("sessions/2025/01/03").join(&filename)
        } else {
            home.path()
                .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
                .join(thread_id.to_string())
                .join(segment_id.to_string())
                .join(&filename)
        };
        let turn_id = format!("turn-{index}");
        let mut items = Vec::new();
        if let Some((predecessor_path, predecessor_segment_id)) = predecessor.take() {
            items.push(segment_reference(
                predecessor_path,
                thread_id,
                predecessor_segment_id,
            ));
        }
        items.extend([
            turn_started(turn_id.as_str()),
            completed_user_message(
                thread_id,
                turn_id.as_str(),
                format!("user-{index}").as_str(),
                format!("question-{index}").as_str(),
            ),
            turn_complete(turn_id.as_str()),
        ]);
        next_ordinal = write_paginated_segment(
            path.as_path(),
            home.path(),
            thread_id,
            segment_id,
            next_ordinal,
            items,
        );
        predecessor = Some((path.clone(), segment_id));
        source_paths.push(path);
    }
    let store = indexed_store(home.path()).await;

    let dry_run = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![thread_id],
            ..RolloutMigrationOptions::default()
        })
        .await
        .expect("dry-run deep Paginated lineage migration");
    assert_eq!(dry_run.outcomes[0].status, RolloutMigrationStatus::Eligible);
    assert_eq!(
        dry_run.outcomes[0]
            .manifest
            .as_ref()
            .expect("migration manifest")
            .targets
            .len(),
        4
    );

    let applied = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![thread_id],
            ..apply_options()
        })
        .await
        .expect("apply deep Paginated lineage migration");
    assert_eq!(applied.outcomes[0].status, RolloutMigrationStatus::Migrated);
    let selected_path = applied.outcomes[0].rollout_path.as_path();
    let materialized = codex_rollout::materialize_rollout_lines(home.path(), selected_path)
        .await
        .expect("materialize deep native lineage");
    let materialized = serde_json::to_string(&materialized).expect("serialize native lineage");
    for index in 0..4 {
        assert_eq!(
            materialized
                .matches(format!("question-{index}").as_str())
                .count(),
            1
        );
    }
    assert!(!materialized.contains("rollout_reference"));
    assert_eq!(source_paths.len(), 4);
}

#[tokio::test]
async fn migration_rewrites_a_legacy_reference_after_a_native_history_base() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let rollout_ids = [ThreadId::new(), ThreadId::new()];
    let segment_ids = [SegmentId::new(), SegmentId::new(), SegmentId::new()];
    let active_filename = format!("rollout-2025-01-03T12-00-02-{thread_id}.jsonl");
    let history_root = home
        .path()
        .join(codex_rollout::SESSIONS_SUBDIR)
        .join(codex_rollout::ROLLOUT_SEGMENTS_SUBDIR)
        .join("2025/01/03");
    let oldest_path = history_root.join(format!(
        "rollout-2025-01-03T12-00-00-{thread_id}_{}.jsonl",
        rollout_ids[0]
    ));
    let oldest_end = write_paginated_segment(
        oldest_path.as_path(),
        home.path(),
        thread_id,
        segment_ids[0],
        0,
        vec![user_message("mixed native oldest")],
    );
    let oldest_position = HistoryPosition {
        thread_id: rollout_ids[0],
        end_ordinal_exclusive: oldest_end,
        end_byte_offset: fs::metadata(oldest_path.as_path())
            .expect("oldest metadata")
            .len(),
    };

    let middle_path = history_root.join(format!(
        "rollout-2025-01-03T12-00-01-{thread_id}_{}.jsonl",
        rollout_ids[1]
    ));
    let middle_end = write_paginated_segment(
        middle_path.as_path(),
        home.path(),
        thread_id,
        segment_ids[1],
        oldest_end,
        vec![user_message("mixed native middle")],
    );
    set_history_base(middle_path.as_path(), oldest_position);

    let active_path = home
        .path()
        .join("sessions/2025/01/03")
        .join(active_filename);
    write_paginated_segment(
        active_path.as_path(),
        home.path(),
        thread_id,
        segment_ids[2],
        middle_end,
        vec![
            RolloutItem::RolloutReference(RolloutReferenceItem {
                rollout_id: Some(rollout_ids[1]),
                rollout_path: middle_path.clone(),
                thread_id: Some(thread_id),
                rollout_timestamp: None,
                segment_id: Some(segment_ids[1]),
                max_depth: codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
                nth_user_message: None,
                compacted_replacement_history_filter_texts: None,
            }),
            user_message("mixed compatibility active"),
        ],
    );
    let source_bytes = [
        fs::read(oldest_path.as_path()).expect("read oldest source"),
        fs::read(middle_path.as_path()).expect("read middle source"),
        fs::read(active_path.as_path()).expect("read active source"),
    ];
    let store = indexed_store(home.path()).await;

    let dry_run = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![thread_id],
            ..RolloutMigrationOptions::default()
        })
        .await
        .expect("dry-run mixed migration");
    let manifest = dry_run.outcomes[0]
        .manifest
        .as_ref()
        .expect("mixed migration manifest");
    assert_eq!(manifest.sources.len(), 2);
    assert_eq!(manifest.targets.len(), 2);
    assert_eq!(manifest.targets[0].history_base, Some(oldest_position));
    for index in 1..manifest.targets.len() {
        assert_eq!(
            manifest.targets[index].history_base,
            Some(HistoryPosition {
                thread_id: manifest.targets[index - 1].rollout_id,
                end_ordinal_exclusive: manifest.targets[index - 1].end_ordinal_exclusive,
                end_byte_offset: manifest.targets[index - 1].byte_count,
            })
        );
    }

    let applied = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![thread_id],
            ..apply_options()
        })
        .await
        .expect("apply mixed migration");
    assert_eq!(applied.outcomes[0].status, RolloutMigrationStatus::Migrated);
    let selected_path = applied.outcomes[0].rollout_path.as_path();
    assert!(
        !fs::read_to_string(selected_path)
            .expect("read mixed selected target")
            .contains("rollout_reference")
    );
    let materialized = codex_rollout::materialize_rollout_lines(home.path(), selected_path)
        .await
        .expect("materialize mixed native lineage");
    let json = serde_json::to_string(&materialized).expect("serialize mixed lineage");
    for message in [
        "mixed native oldest",
        "mixed native middle",
        "mixed compatibility active",
    ] {
        assert_eq!(json.matches(message).count(), 1, "{message}");
    }
    assert_eq!(
        [
            fs::read(oldest_path).expect("reread oldest source"),
            fs::read(middle_path).expect("reread middle source"),
            fs::read(active_path).expect("reread active source"),
        ],
        source_bytes
    );
}

#[tokio::test]
async fn native_history_base_migration_translates_subagent_history_boundary() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let segment_ids = [SegmentId::new(), SegmentId::new()];
    let filename = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
    let predecessor = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(segment_ids[0].to_string())
        .join(filename.as_str());
    let predecessor_end = write_paginated_segment(
        predecessor.as_path(),
        home.path(),
        thread_id,
        segment_ids[0],
        0,
        vec![user_message("inherited parent context")],
    );
    let active = home.path().join("sessions/2025/01/03").join(filename);
    write_paginated_segment(
        active.as_path(),
        home.path(),
        thread_id,
        segment_ids[1],
        predecessor_end,
        vec![
            segment_reference(predecessor, thread_id, segment_ids[0]),
            user_message("subagent-owned context"),
        ],
    );
    set_paginated_subagent_history_start(active.as_path(), predecessor_end + 2);
    let store = indexed_store(home.path()).await;

    let report = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![thread_id],
            ..apply_options()
        })
        .await
        .expect("migrate bounded Paginated subagent");
    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Migrated);
    let selected = report.outcomes[0].rollout_path.as_path();
    let metadata = codex_rollout::read_session_meta_line(selected)
        .await
        .expect("read migrated subagent metadata");
    assert_eq!(
        metadata.meta.subagent_history_start_ordinal,
        Some(predecessor_end + 1)
    );
    assert!(
        !fs::read_to_string(selected)
            .expect("read migrated subagent")
            .contains("rollout_reference")
    );
    let materialized = codex_rollout::materialize_rollout_lines(home.path(), selected)
        .await
        .expect("materialize migrated subagent");
    let json = serde_json::to_string(&materialized).expect("serialize subagent history");
    assert_eq!(json.matches("inherited parent context").count(), 1);
    assert!(json.contains("subagent-owned context"));
}

#[tokio::test]
async fn lineage_migration_stages_cross_thread_history_base_without_copying_parent() {
    let home = TempDir::new().expect("create Codex home");
    let parent_id = ThreadId::new();
    write_rollout(
        home.path(),
        parent_id,
        SessionSource::Cli,
        vec![user_message("parent-only marker")],
    );
    let child_id = ThreadId::new();
    let child_path = write_rollout(
        home.path(),
        child_id,
        SessionSource::Cli,
        vec![user_message("child-only marker")],
    );
    let store = indexed_store(home.path()).await;
    let parent_report = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![parent_id],
            ..apply_options()
        })
        .await
        .expect("migrate parent to Paginated");
    assert_eq!(parent_report.outcomes.len(), 1);
    assert_eq!(
        parent_report.outcomes[0].status,
        RolloutMigrationStatus::Migrated
    );
    let parent_path = parent_report.outcomes[0].rollout_path.clone();
    let parent_bytes = fs::read(parent_path.as_path()).expect("read Paginated parent");
    let history_base = store
        .projected_history_position(parent_id)
        .await
        .expect("read parent projection")
        .expect("parent history position");
    set_history_base(child_path.as_path(), history_base);
    let plan = plan_legacy_lineage(home.path(), child_path.as_path())
        .await
        .expect("plan fork lineage");
    store
        .validate_legacy_lineage_plan(&plan)
        .await
        .expect("validate history base");
    let staged = stage_legacy_lineage(
        &plan,
        home.path().join("rollout-migrations/fork-stage").as_path(),
    )
    .await
    .expect("stage fork lineage");

    assert_eq!(plan.history_bases.len(), 1);
    assert_eq!(plan.history_bases[0].path, parent_path);
    assert_eq!(staged.len(), 1);
    assert_eq!(staged[0].thread_id, child_id);
    let child_text =
        fs::read_to_string(staged[0].staged_path.as_path()).expect("read staged child");
    assert!(!child_text.contains("parent-only marker"));
    assert!(child_text.contains("child-only marker"));
    let child_head = serde_json::from_str::<RolloutLine>(
        child_text.lines().next().expect("child session metadata"),
    )
    .expect("parse child session metadata");
    let RolloutItem::SessionMeta(child_meta) = child_head.item else {
        panic!("child target must start with session metadata");
    };
    assert_eq!(child_meta.meta.history_base, Some(history_base));
    assert_eq!(staged[0].start_ordinal, history_base.end_ordinal_exclusive);
    assert_eq!(
        fs::read(parent_path).expect("reread Paginated parent"),
        parent_bytes
    );
}

#[tokio::test]
async fn migration_preserves_paginated_history_base_desktop_view_without_copying_parent() {
    let home = TempDir::new().expect("create Codex home");
    let parent_id = ThreadId::new();
    write_rollout(
        home.path(),
        parent_id,
        SessionSource::Cli,
        vec![user_message("parent marker")],
    );
    let child_id = ThreadId::new();
    let child_source = write_rollout(
        home.path(),
        child_id,
        SessionSource::Cli,
        vec![user_message("child marker")],
    );
    let store = indexed_store(home.path()).await;
    let parent_report = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![parent_id],
            ..apply_options()
        })
        .await
        .expect("migrate parent");
    let parent_path = parent_report.outcomes[0].rollout_path.clone();
    let parent_bytes = fs::read(parent_path.as_path()).expect("read parent target");
    let history_base = store
        .projected_history_position(parent_id)
        .await
        .expect("read parent projection")
        .expect("parent history position");
    set_history_base(child_source.as_path(), history_base);
    let child_source_bytes = fs::read(child_source.as_path()).expect("read updated child source");

    let dry_run = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![child_id],
            ..RolloutMigrationOptions::default()
        })
        .await
        .expect("dry-run child");
    assert_eq!(dry_run.outcomes.len(), 1);
    assert_eq!(
        dry_run.outcomes[0].status,
        RolloutMigrationStatus::Eligible,
        "{:?}",
        dry_run.outcomes[0].message
    );
    let manifest = dry_run.outcomes[0]
        .manifest
        .as_ref()
        .expect("history-base dry-run manifest");
    assert_eq!(manifest.sources.len(), 1);
    assert_eq!(manifest.history_base_dependencies.len(), 1);
    assert!(manifest.reference_dependencies.is_empty());
    assert_eq!(manifest.history_base_dependencies[0].position, history_base);
    assert_eq!(manifest.history_base_dependencies[0].path, parent_path);
    assert_eq!(manifest.dependency_bytes, parent_bytes.len() as u64);
    assert_eq!(
        manifest.targets[0].start_ordinal,
        history_base.end_ordinal_exclusive
    );
    let applied = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![child_id],
            ..apply_options()
        })
        .await
        .expect("migrate child");
    assert_eq!(applied.outcomes.len(), 1);
    assert_eq!(
        applied.outcomes[0].status,
        RolloutMigrationStatus::Migrated,
        "{:?}",
        applied.outcomes[0].message
    );
    let child_target = applied.outcomes[0].rollout_path.as_path();
    let child_target_text = fs::read_to_string(child_target).expect("read child target");
    assert!(!child_target_text.contains("parent marker"));
    assert!(child_target_text.contains("child marker"));
    let child_head = serde_json::from_str::<RolloutLine>(
        child_target_text
            .lines()
            .next()
            .expect("child session metadata"),
    )
    .expect("parse child metadata");
    let RolloutItem::SessionMeta(child_meta) = child_head.item else {
        panic!("child target must start with session metadata");
    };
    assert_eq!(child_meta.meta.history_base, Some(history_base));
    let after = list_active_summary_turns(&store, child_id).await;
    assert_eq!(after.turns.len(), 1);
    assert_eq!(after.turns[0].items.len(), 1);
    let item: serde_json::Value = serde_json::from_slice(&after.turns[0].items[0].item_json)
        .expect("parse post-migration Desktop item");
    assert_eq!(item["content"][0]["text"], "child marker");
    assert_eq!(
        fs::read(parent_path.as_path()).expect("reread parent target"),
        parent_bytes
    );
    assert_eq!(
        fs::read(child_source.as_path()).expect("reread child source"),
        child_source_bytes
    );
}

#[tokio::test]
async fn migration_rejects_history_base_with_an_invalid_paginated_cutoff() {
    let home = TempDir::new().expect("create Codex home");
    let parent_id = ThreadId::new();
    write_rollout(
        home.path(),
        parent_id,
        SessionSource::Cli,
        vec![user_message("parent marker")],
    );
    let child_id = ThreadId::new();
    let child_path = write_rollout(
        home.path(),
        child_id,
        SessionSource::Cli,
        vec![user_message("child marker")],
    );
    let store = indexed_store(home.path()).await;
    store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![parent_id],
            ..apply_options()
        })
        .await
        .expect("migrate parent");
    let mut invalid = store
        .projected_history_position(parent_id)
        .await
        .expect("read parent projection")
        .expect("parent history position");
    invalid.end_byte_offset = invalid
        .end_byte_offset
        .checked_add(1)
        .expect("offset increment");
    set_history_base(child_path.as_path(), invalid);
    let child_bytes = fs::read(child_path.as_path()).expect("read child source");

    let report = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![child_id],
            ..apply_options()
        })
        .await
        .expect("report invalid history base");
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Failed);
    assert!(
        report.outcomes[0]
            .message
            .as_deref()
            .is_some_and(|message| message.contains("cutoff")),
        "{:?}",
        report.outcomes[0].message
    );
    assert_eq!(
        fs::read(child_path.as_path()).expect("reread child source"),
        child_bytes
    );
    assert_no_migration_artifacts(home.path(), child_path.as_path(), child_id).await;
}

#[tokio::test]
async fn migration_rejects_history_base_source_change_after_targets_are_durable() {
    let home = TempDir::new().expect("create Codex home");
    let parent_id = ThreadId::new();
    write_rollout(
        home.path(),
        parent_id,
        SessionSource::Cli,
        vec![user_message("parent marker")],
    );
    let child_id = ThreadId::new();
    let child_path = write_rollout(
        home.path(),
        child_id,
        SessionSource::Cli,
        vec![user_message("child marker")],
    );
    let store = indexed_store(home.path()).await;
    let parent_report = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![parent_id],
            ..apply_options()
        })
        .await
        .expect("migrate parent");
    let parent_path = parent_report.outcomes[0].rollout_path.clone();
    let history_base = store
        .projected_history_position(parent_id)
        .await
        .expect("read parent projection")
        .expect("parent history position");
    set_history_base(child_path.as_path(), history_base);
    let child_bytes = fs::read(child_path.as_path()).expect("read child source");
    let plan = plan_legacy_lineage(home.path(), child_path.as_path())
        .await
        .expect("plan child lineage");
    let journal_path = migration_journal_path(home.path(), child_id);
    let mut limiter =
        RolloutMigrationRateLimiter::new(Some(1024)).expect("create migration limiter");
    let error = store
        .migrate_legacy_lineage_until_phase_for_test(
            child_path.as_path(),
            journal_path.as_path(),
            plan,
            &mut limiter,
            LineageMigrationPhase::TargetsDurable,
        )
        .await
        .expect_err("stop after staged targets are durable");
    assert!(
        error
            .to_string()
            .contains("injected lineage migration stop")
    );
    let mut parent = fs::OpenOptions::new()
        .append(true)
        .open(parent_path.as_path())
        .expect("open parent for append");
    let late_line = RolloutLine {
        timestamp: "2025-01-03T12:00:01Z".to_string(),
        ordinal: Some(history_base.end_ordinal_exclusive),
        item: agent_message("late parent append"),
    };
    writeln!(
        parent,
        "{}",
        serde_json::to_string(&late_line).expect("serialize late parent line")
    )
    .expect("append parent line");
    parent.sync_all().expect("sync parent append");

    let report = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![child_id],
            ..apply_options()
        })
        .await
        .expect("report changed parent");
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Failed);
    assert!(
        report.outcomes[0]
            .message
            .as_deref()
            .is_some_and(|message| message.contains("manifest does not match")),
        "{:?}",
        report.outcomes[0].message
    );
    let selected = store
        .state_db
        .as_ref()
        .expect("state db")
        .get_thread(child_id)
        .await
        .expect("read child metadata")
        .expect("child metadata");
    assert_eq!(selected.rollout_path, child_path);
    assert_eq!(selected.history_mode, ThreadHistoryMode::Legacy);
    assert_eq!(
        fs::read(child_path.as_path()).expect("reread child source"),
        child_bytes
    );
    assert_no_migration_artifacts(home.path(), child_path.as_path(), child_id).await;
}

#[tokio::test]
async fn migration_applies_same_thread_segmented_legacy_lineage_atomically_and_idempotently() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let segment_ids = [SegmentId::new(), SegmentId::new(), SegmentId::new()];
    let filename = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
    let immutable_root = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string());
    let oldest = immutable_root
        .join(segment_ids[0].to_string())
        .join(filename.as_str());
    write_legacy_segment(
        oldest.as_path(),
        home.path(),
        thread_id,
        segment_ids[0],
        vec![user_message("oldest")],
    );
    let middle = immutable_root
        .join(segment_ids[1].to_string())
        .join(filename.as_str());
    write_legacy_segment(
        middle.as_path(),
        home.path(),
        thread_id,
        segment_ids[1],
        vec![
            segment_reference(oldest.clone(), thread_id, segment_ids[0]),
            user_message("middle"),
        ],
    );
    let active = home.path().join("sessions/2025/01/03").join(filename);
    write_legacy_segment(
        active.as_path(),
        home.path(),
        thread_id,
        segment_ids[2],
        vec![
            segment_reference(middle.clone(), thread_id, segment_ids[1]),
            user_message("active"),
        ],
    );
    let source_bytes = [
        fs::read(oldest.as_path()).expect("read oldest source"),
        fs::read(middle.as_path()).expect("read middle source"),
        fs::read(active.as_path()).expect("read active source"),
    ];
    let store = indexed_store(home.path()).await;

    let dry_run = store
        .migrate_rollouts(RolloutMigrationOptions::default())
        .await
        .expect("dry-run segmented migration");
    assert_eq!(dry_run.outcomes.len(), 1);
    assert_eq!(
        dry_run.outcomes[0].status,
        RolloutMigrationStatus::Eligible,
        "{:?}",
        dry_run.outcomes[0].message
    );
    assert!(
        dry_run.outcomes[0]
            .message
            .as_deref()
            .is_some_and(|message| message.contains("authenticated 3 physical source"))
    );
    let manifest = dry_run.outcomes[0]
        .manifest
        .as_ref()
        .expect("dry-run lineage manifest");
    assert_eq!(manifest.version, 2);
    assert_eq!(manifest.selected_thread_id, thread_id);
    assert_eq!(manifest.sources.len(), 3);
    assert!(manifest.history_base_dependencies.is_empty());
    assert!(manifest.reference_dependencies.is_empty());
    assert_eq!(manifest.targets.len(), 3);
    assert_eq!(manifest.minimum_free_bytes, manifest.target_payload_bytes);
    assert!(manifest.target_payload_bytes > 0);
    assert!(manifest.sources_retained_after_apply);
    assert!(manifest.sources.iter().all(|source| {
        source.byte_count > 0 && source.record_count > 0 && source.sha256.len() == 64
    }));
    assert!(manifest.targets.iter().all(|target| {
        target.byte_count > 0
            && target.record_count > 0
            && target.sha256.len() == 64
            && !target.path.exists()
    }));
    assert_eq!(
        manifest
            .targets
            .iter()
            .filter(|target| target.selected)
            .count(),
        1
    );
    let report_json = serde_json::to_value(&dry_run).expect("serialize dry-run report");
    let manifest_json = &report_json["outcomes"][0]["manifest"];
    assert_eq!(manifest_json["version"], 2);
    assert_eq!(manifest_json["input_kind"], "segmented_lineage");
    assert_eq!(manifest_json["selected_thread_id"], thread_id.to_string());
    assert_eq!(manifest_json["sources"].as_array().map(Vec::len), Some(3));
    assert_eq!(manifest_json["targets"].as_array().map(Vec::len), Some(3));
    assert_eq!(
        manifest_json["minimum_free_bytes"],
        manifest_json["target_payload_bytes"]
    );
    assert_eq!(
        manifest_json["publication_phases"],
        json!([
            "planned",
            "targets_durable",
            "projection_durable",
            "selected",
            "verified",
            "complete"
        ])
    );
    assert_eq!(manifest_json["sources_retained_after_apply"], true);
    assert!(
        manifest_json["additional_free_space"]["sqlite_projection"]
            .as_str()
            .is_some_and(|value| value.contains("filesystem-dependent"))
    );
    let repeated_dry_run = store
        .migrate_rollouts(RolloutMigrationOptions::default())
        .await
        .expect("repeat dry-run segmented migration");
    assert_eq!(repeated_dry_run, dry_run);
    assert_no_migration_artifacts(home.path(), active.as_path(), thread_id).await;
    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("apply segmented migration");
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(
        report.outcomes[0].status,
        RolloutMigrationStatus::Migrated,
        "{:?}",
        report.outcomes[0].message
    );
    let selected_path = report.outcomes[0].rollout_path.clone();
    assert_ne!(selected_path, active);
    assert!(selected_path.exists());
    assert_eq!(
        [
            fs::read(oldest.as_path()).expect("reread oldest source"),
            fs::read(middle.as_path()).expect("reread middle source"),
            fs::read(active.as_path()).expect("reread active source"),
        ],
        source_bytes
    );
    let selected = store
        .state_db
        .as_ref()
        .expect("state db")
        .get_thread(thread_id)
        .await
        .expect("read selected thread")
        .expect("selected thread");
    assert_eq!(selected.rollout_path, selected_path);
    assert_eq!(selected.history_mode, ThreadHistoryMode::Paginated);
    let materialized =
        codex_rollout::materialize_rollout_lines(home.path(), selected.rollout_path.as_path())
            .await
            .expect("materialize migrated lineage");
    let materialized_json = serde_json::to_string(&materialized).expect("serialize materialized");
    for marker in ["oldest", "middle", "active"] {
        assert!(materialized_json.contains(marker));
    }
    let ordinals = materialized
        .iter()
        .filter_map(|line| line.ordinal)
        .collect::<Vec<_>>();
    assert_eq!(
        ordinals.iter().copied().collect::<HashSet<_>>().len(),
        ordinals.len()
    );
    assert!(!migration_journal_path(home.path(), thread_id).exists());

    let repeated = store
        .migrate_rollouts(apply_options())
        .await
        .expect("repeat segmented migration");
    assert_eq!(repeated.outcomes.len(), 1);
    assert_eq!(
        repeated.outcomes[0].status,
        RolloutMigrationStatus::AlreadyPaginated
    );
    assert_eq!(repeated.outcomes[0].rollout_path, selected_path);
}

#[tokio::test]
async fn dry_run_manifest_matches_single_rollout_apply_without_artifacts() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            user_message("question"),
            agent_message("answer"),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
            user_message("replacement question"),
            agent_message("replacement answer"),
        ],
    );
    let source_bytes = fs::read(path.as_path()).expect("read Legacy source");
    let store = indexed_store(home.path()).await;

    let dry_run = store
        .migrate_rollouts(RolloutMigrationOptions::default())
        .await
        .expect("dry-run one-file migration");
    assert_eq!(dry_run.outcomes.len(), 1);
    assert_eq!(dry_run.outcomes[0].status, RolloutMigrationStatus::Eligible);
    let manifest = dry_run.outcomes[0]
        .manifest
        .as_ref()
        .expect("one-file dry-run manifest");
    assert_eq!(
        serde_json::to_value(manifest).expect("serialize one-file manifest")["input_kind"],
        "single_rollout"
    );
    assert_eq!(manifest.sources.len(), 1);
    assert_eq!(manifest.targets.len(), 1);
    assert_eq!(manifest.sources[0].path, path);
    assert_eq!(manifest.targets[0].path, path);
    assert!(!manifest.sources_retained_after_apply);
    assert_eq!(
        fs::read(path.as_path()).expect("reread Legacy source after dry-run"),
        source_bytes
    );
    assert_no_migration_artifacts(home.path(), path.as_path(), thread_id).await;

    let apply = store
        .migrate_rollouts(apply_options())
        .await
        .expect("apply one-file migration");
    assert_eq!(apply.outcomes.len(), 1);
    assert_eq!(apply.outcomes[0].status, RolloutMigrationStatus::Migrated);
    assert_eq!(apply.outcomes[0].rollout_path, path);
    let (applied_bytes, applied_sha256) = hash_file(path.as_path())
        .await
        .expect("hash applied rollout");
    assert_eq!(manifest.targets[0].byte_count, applied_bytes);
    assert_eq!(manifest.targets[0].sha256, applied_sha256);
    assert_eq!(
        manifest.targets[0].record_count,
        u64::try_from(read_rollout(path.as_path()).len()).expect("record count fits u64")
    );
    assert_eq!(manifest.targets[0].start_ordinal, 0);
    assert_eq!(
        manifest.targets[0].end_ordinal_exclusive,
        manifest.targets[0].record_count
    );
}

#[tokio::test]
async fn dry_run_manifest_matches_compressed_single_rollout_apply() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = compress_rollout(
        write_rollout(
            home.path(),
            thread_id,
            SessionSource::Cli,
            vec![user_message("question"), agent_message("answer")],
        )
        .as_path(),
    );
    let source_bytes = fs::read(path.as_path()).expect("read compressed Legacy source");
    let store = indexed_store(home.path()).await;

    let dry_run = store
        .migrate_rollouts(RolloutMigrationOptions::default())
        .await
        .expect("dry-run compressed one-file migration");
    let manifest = dry_run.outcomes[0]
        .manifest
        .as_ref()
        .expect("compressed one-file manifest");
    assert!(manifest.targets[0].compressed_on_publication);
    assert_eq!(manifest.targets[0].path, path);
    assert_eq!(
        fs::read(path.as_path()).expect("reread compressed source after dry-run"),
        source_bytes
    );
    assert_no_migration_artifacts(home.path(), path.as_path(), thread_id).await;

    let apply = store
        .migrate_rollouts(apply_options())
        .await
        .expect("apply compressed one-file migration");
    assert_eq!(apply.outcomes[0].status, RolloutMigrationStatus::Migrated);
    assert_manifest_target_matches_published_rollout(&manifest.targets[0], path.as_path());
}

#[tokio::test]
async fn dry_run_manifest_matches_compressed_bounded_subagent_apply() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = compress_rollout(
        write_rollout(
            home.path(),
            thread_id,
            SessionSource::SubAgent(SubAgentSource::Other("test".to_string())),
            bounded_subagent_items(home.path()),
        )
        .as_path(),
    );
    let source_bytes = fs::read(path.as_path()).expect("read compressed subagent source");
    let store = indexed_store(home.path()).await;

    let dry_run = store
        .migrate_rollouts(RolloutMigrationOptions::default())
        .await
        .expect("dry-run compressed bounded subagent");
    let manifest = dry_run.outcomes[0]
        .manifest
        .as_ref()
        .expect("compressed bounded subagent manifest");
    assert_eq!(
        manifest
            .additional_free_space
            .dry_run_decompression_temporary,
        "one decompressed source copy for bounded-context reverse scan"
    );
    assert_eq!(
        fs::read(path.as_path()).expect("reread compressed subagent after dry-run"),
        source_bytes
    );
    assert_no_migration_artifacts(home.path(), path.as_path(), thread_id).await;

    let apply = store
        .migrate_rollouts(apply_options())
        .await
        .expect("apply compressed bounded subagent migration");
    assert_eq!(apply.outcomes[0].status, RolloutMigrationStatus::Migrated);
    assert_manifest_target_matches_published_rollout(&manifest.targets[0], path.as_path());
}

#[tokio::test]
async fn migration_preserves_a_turn_split_across_same_thread_segments() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let segment_ids = [SegmentId::new(), SegmentId::new()];
    let filename = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
    let immutable = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(segment_ids[0].to_string())
        .join(filename.as_str());
    write_legacy_segment(
        immutable.as_path(),
        home.path(),
        thread_id,
        segment_ids[0],
        vec![
            started("split-turn"),
            user_message("question before rotation"),
        ],
    );
    let active = home.path().join("sessions/2025/01/03").join(filename);
    write_legacy_segment(
        active.as_path(),
        home.path(),
        thread_id,
        segment_ids[1],
        vec![
            segment_reference(immutable, thread_id, segment_ids[0]),
            agent_message("answer after rotation"),
            completed("split-turn"),
        ],
    );
    let store = indexed_store(home.path()).await;

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate split turn");
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(
        report.outcomes[0].status,
        RolloutMigrationStatus::Migrated,
        "{:?}",
        report.outcomes[0].message
    );
    let materialized = codex_rollout::materialize_rollout_lines(
        home.path(),
        report.outcomes[0].rollout_path.as_path(),
    )
    .await
    .expect("materialize migrated split turn");
    let started_ids = materialized
        .iter()
        .filter_map(|line| match &line.item {
            RolloutItem::EventMsg(EventMsg::TurnStarted(event)) => Some(event.turn_id.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let completed_ids = materialized
        .iter()
        .filter_map(|line| match &line.item {
            RolloutItem::EventMsg(EventMsg::TurnComplete(event)) => Some(event.turn_id.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let item_turn_ids = materialized
        .iter()
        .filter_map(|line| match &line.item {
            RolloutItem::EventMsg(EventMsg::ItemCompleted(event)) => Some(event.turn_id.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(started_ids, vec!["split-turn"]);
    assert_eq!(completed_ids, vec!["split-turn"]);
    assert_eq!(item_turn_ids, vec!["split-turn", "split-turn"]);
}

#[tokio::test]
async fn segmented_migration_refuses_context_dependent_legacy_item_ids_without_mutation() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let segment_ids = [
        SegmentId::new(),
        SegmentId::new(),
        SegmentId::new(),
        SegmentId::new(),
    ];
    let filename = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
    let mut predecessor = None;
    let mut source_paths = Vec::new();
    for (index, segment_id) in segment_ids.into_iter().enumerate() {
        let path = if index + 1 == segment_ids.len() {
            home.path().join("sessions/2025/01/03").join(&filename)
        } else {
            home.path()
                .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
                .join(thread_id.to_string())
                .join(segment_id.to_string())
                .join(&filename)
        };
        let mut items = Vec::new();
        if let Some((predecessor_path, predecessor_segment_id)) = predecessor.take() {
            items.push(segment_reference(
                predecessor_path,
                thread_id,
                predecessor_segment_id,
            ));
        }
        items.push(started(format!("turn-{index}").as_str()));
        items.push(user_message(format!("question-{index}").as_str()));
        items.push(completed(format!("turn-{index}").as_str()));
        write_legacy_segment(path.as_path(), home.path(), thread_id, segment_id, items);
        predecessor = Some((path.clone(), segment_id));
        source_paths.push(path);
    }
    let source_bytes = source_paths
        .iter()
        .map(|path| fs::read(path).expect("read Legacy source"))
        .collect::<Vec<_>>();
    let selected_path = source_paths.last().expect("selected path").clone();
    let store = indexed_store(home.path()).await;

    let dry_run = store
        .migrate_rollouts(RolloutMigrationOptions::default())
        .await
        .expect("dry-run incompatible segmented migration");
    assert_eq!(dry_run.outcomes.len(), 1);
    assert_eq!(dry_run.outcomes[0].status, RolloutMigrationStatus::Failed);
    assert!(
        dry_run.outcomes[0].message.as_deref().is_some_and(
            |message| message.contains("bounded Legacy Desktop history is not canonical")
        ),
        "{:?}",
        dry_run.outcomes[0].message
    );

    let apply = store
        .migrate_rollouts(apply_options())
        .await
        .expect("apply incompatible segmented migration");
    assert_eq!(apply.outcomes.len(), 1);
    assert_eq!(apply.outcomes[0].status, RolloutMigrationStatus::Failed);
    assert_eq!(
        source_paths
            .iter()
            .map(|path| fs::read(path).expect("reread Legacy source"))
            .collect::<Vec<_>>(),
        source_bytes
    );
    let selected = store
        .state_db
        .as_ref()
        .expect("state db")
        .get_thread(thread_id)
        .await
        .expect("read selected thread")
        .expect("selected thread");
    assert_eq!(selected.rollout_path, selected_path);
    assert_eq!(selected.history_mode, ThreadHistoryMode::Legacy);
    assert_no_migration_artifacts(home.path(), selected_path.as_path(), thread_id).await;
}

#[tokio::test]
async fn migration_recovers_same_thread_lineage_from_every_durable_phase() {
    for phase in [
        LineageMigrationPhase::Planned,
        LineageMigrationPhase::TargetsDurable,
        LineageMigrationPhase::ProjectionDurable,
        LineageMigrationPhase::Selected,
        LineageMigrationPhase::Verified,
        LineageMigrationPhase::Complete,
    ] {
        let home = TempDir::new().expect("create Codex home");
        let thread_id = ThreadId::new();
        let segment_ids = [SegmentId::new(), SegmentId::new()];
        let filename = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
        let immutable = home
            .path()
            .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
            .join(thread_id.to_string())
            .join(segment_ids[0].to_string())
            .join(filename.as_str());
        write_legacy_segment(
            immutable.as_path(),
            home.path(),
            thread_id,
            segment_ids[0],
            vec![user_message("immutable marker")],
        );
        let active = home.path().join("sessions/2025/01/03").join(filename);
        write_legacy_segment(
            active.as_path(),
            home.path(),
            thread_id,
            segment_ids[1],
            vec![
                segment_reference(immutable.clone(), thread_id, segment_ids[0]),
                user_message("active marker"),
            ],
        );
        let source_bytes = [
            fs::read(immutable.as_path()).expect("read immutable source"),
            fs::read(active.as_path()).expect("read active source"),
        ];
        let store = indexed_store(home.path()).await;
        let plan = plan_legacy_lineage(home.path(), active.as_path())
            .await
            .expect("plan lineage");
        let journal_path = migration_journal_path(home.path(), thread_id);
        let mut limiter =
            RolloutMigrationRateLimiter::new(Some(1024)).expect("create migration limiter");
        let error = store
            .migrate_legacy_lineage_until_phase_for_test(
                active.as_path(),
                journal_path.as_path(),
                plan,
                &mut limiter,
                phase,
            )
            .await
            .expect_err("injected phase stop");
        assert!(
            error
                .to_string()
                .contains("injected lineage migration stop")
        );
        assert!(journal_path.exists());
        drop(store);

        let restarted = indexed_store(home.path()).await;
        let recovered = restarted
            .migrate_rollouts(apply_options())
            .await
            .expect("recover lineage migration");
        assert_eq!(recovered.outcomes.len(), 1, "phase {phase:?}");
        assert_eq!(
            recovered.outcomes[0].status,
            RolloutMigrationStatus::Migrated,
            "phase {phase:?}: {:?}",
            recovered.outcomes[0].message
        );
        assert!(!journal_path.exists());
        let selected = restarted
            .state_db
            .as_ref()
            .expect("state db")
            .get_thread(thread_id)
            .await
            .expect("read thread")
            .expect("thread metadata");
        assert_eq!(selected.history_mode, ThreadHistoryMode::Paginated);
        let materialized =
            codex_rollout::materialize_rollout_lines(home.path(), selected.rollout_path.as_path())
                .await
                .expect("materialize recovered lineage");
        let json = serde_json::to_string(&materialized).expect("serialize history");
        assert!(json.contains("immutable marker"));
        assert!(json.contains("active marker"));
        assert_eq!(
            [
                fs::read(immutable.as_path()).expect("reread immutable source"),
                fs::read(active.as_path()).expect("reread active source"),
            ],
            source_bytes
        );
    }
}

#[tokio::test]
async fn native_history_base_migration_recovers_from_every_durable_phase() {
    for phase in [
        LineageMigrationPhase::Planned,
        LineageMigrationPhase::TargetsDurable,
        LineageMigrationPhase::ProjectionDurable,
        LineageMigrationPhase::Selected,
        LineageMigrationPhase::Verified,
        LineageMigrationPhase::Complete,
    ] {
        let home = TempDir::new().expect("create Codex home");
        let thread_id = ThreadId::new();
        let segment_ids = [SegmentId::new(), SegmentId::new()];
        let filename = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
        let immutable = home
            .path()
            .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
            .join(thread_id.to_string())
            .join(segment_ids[0].to_string())
            .join(filename.as_str());
        let predecessor_end = write_paginated_segment(
            immutable.as_path(),
            home.path(),
            thread_id,
            segment_ids[0],
            /*start_ordinal*/ 0,
            vec![user_message("native recovery predecessor")],
        );
        let active = home.path().join("sessions/2025/01/03").join(filename);
        write_paginated_segment(
            active.as_path(),
            home.path(),
            thread_id,
            segment_ids[1],
            predecessor_end,
            vec![
                segment_reference(immutable.clone(), thread_id, segment_ids[0]),
                user_message("native recovery active"),
            ],
        );
        let source_bytes = [
            fs::read(immutable.as_path()).expect("read immutable source"),
            fs::read(active.as_path()).expect("read active source"),
        ];
        let store = indexed_store(home.path()).await;
        let plan = plan_legacy_lineage(home.path(), active.as_path())
            .await
            .expect("plan native migration");
        let journal_path = migration_journal_path(home.path(), thread_id);
        let mut limiter =
            RolloutMigrationRateLimiter::new(Some(1024)).expect("create migration limiter");
        let error = store
            .migrate_legacy_lineage_until_phase_for_test(
                active.as_path(),
                journal_path.as_path(),
                plan,
                &mut limiter,
                phase,
            )
            .await
            .expect_err("injected native migration phase stop");
        assert!(
            error
                .to_string()
                .contains("injected lineage migration stop")
        );
        assert!(journal_path.exists());
        drop(store);

        let restarted = indexed_store(home.path()).await;
        let recovered = restarted
            .migrate_rollouts(RolloutMigrationOptions {
                thread_ids: vec![thread_id],
                ..apply_options()
            })
            .await
            .expect("recover native migration");
        assert_eq!(recovered.outcomes.len(), 1, "phase {phase:?}");
        assert_eq!(
            recovered.outcomes[0].status,
            RolloutMigrationStatus::Migrated,
            "phase {phase:?}: {:?}",
            recovered.outcomes[0].message
        );
        assert!(!journal_path.exists());
        let selected_path = recovered.outcomes[0].rollout_path.as_path();
        let selected_text = fs::read_to_string(selected_path).expect("read recovered target");
        assert!(!selected_text.contains("rollout_reference"));
        let selected_meta = codex_rollout::read_session_meta_line(selected_path)
            .await
            .expect("read recovered metadata");
        let history_base = selected_meta
            .meta
            .history_base
            .expect("recovered history base");
        let predecessor =
            codex_rollout::find_rollout_path_by_rollout_id(home.path(), history_base.thread_id)
                .await
                .expect("resolve recovered predecessor")
                .expect("recovered predecessor exists");
        assert!(
            !fs::read_to_string(predecessor)
                .expect("read recovered predecessor")
                .contains("rollout_reference")
        );
        let materialized = codex_rollout::materialize_rollout_lines(home.path(), selected_path)
            .await
            .expect("materialize recovered native lineage");
        let json = serde_json::to_string(&materialized).expect("serialize recovered history");
        assert_eq!(json.matches("native recovery predecessor").count(), 1);
        assert_eq!(json.matches("native recovery active").count(), 1);
        assert_eq!(
            [
                fs::read(immutable.as_path()).expect("reread immutable source"),
                fs::read(active.as_path()).expect("reread active source"),
            ],
            source_bytes
        );
    }
}

#[tokio::test]
async fn migration_restarts_previous_target_identity_before_selection() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let segment_ids = [SegmentId::new(), SegmentId::new()];
    let filename = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
    let immutable = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(segment_ids[0].to_string())
        .join(filename.as_str());
    write_legacy_segment(
        immutable.as_path(),
        home.path(),
        thread_id,
        segment_ids[0],
        vec![user_message("previous target immutable marker")],
    );
    let active = home.path().join("sessions/2025/01/03").join(filename);
    write_legacy_segment(
        active.as_path(),
        home.path(),
        thread_id,
        segment_ids[1],
        vec![
            segment_reference(immutable, thread_id, segment_ids[0]),
            user_message("previous target active marker"),
        ],
    );
    let store = indexed_store(home.path()).await;
    let plan = plan_legacy_lineage(home.path(), active.as_path())
        .await
        .expect("plan lineage");
    let journal_path = migration_journal_path(home.path(), thread_id);
    let mut limiter =
        RolloutMigrationRateLimiter::new(Some(1024)).expect("create migration limiter");
    let error = store
        .migrate_legacy_lineage_until_phase_for_test(
            active.as_path(),
            journal_path.as_path(),
            plan,
            &mut limiter,
            LineageMigrationPhase::ProjectionDurable,
        )
        .await
        .expect_err("stop before selection");
    assert!(
        error
            .to_string()
            .contains("injected lineage migration stop")
    );
    let journal = read_lineage_migration_journal(journal_path.as_path())
        .await
        .expect("read current journal");
    let first = journal.targets.first().expect("first target");
    fs::create_dir_all(first.path.parent().expect("target parent"))
        .expect("create partial publication parent");
    fs::copy(
        first.staged_path.as_ref().expect("staged target"),
        first.path.as_path(),
    )
    .expect("simulate partial publication");
    let mut journal_json = serde_json::from_slice::<serde_json::Value>(
        fs::read(journal_path.as_path())
            .expect("read journal bytes")
            .as_slice(),
    )
    .expect("parse journal JSON");
    journal_json["version"] = json!(2);
    fs::write(
        journal_path.as_path(),
        serde_json::to_vec(&journal_json).expect("serialize previous journal"),
    )
    .expect("write previous journal");
    drop(store);

    let restarted = indexed_store(home.path()).await;
    let recovered = restarted
        .migrate_rollouts(apply_options())
        .await
        .expect("restart previous target identity");
    assert_eq!(recovered.outcomes.len(), 1);
    assert_eq!(
        recovered.outcomes[0].status,
        RolloutMigrationStatus::Migrated,
        "{:?}",
        recovered.outcomes[0].message
    );
    assert!(!journal_path.exists());
    let materialized = codex_rollout::materialize_rollout_lines(
        home.path(),
        recovered.outcomes[0].rollout_path.as_path(),
    )
    .await
    .expect("materialize recovered migration");
    let text = serde_json::to_string(&materialized).expect("serialize recovered history");
    assert!(text.contains("previous target immutable marker"));
    assert!(text.contains("previous target active marker"));
}

const LINEAGE_CRASH_HOME_ENV: &str = "FRODEX_LINEAGE_CRASH_HOME";
const LINEAGE_CRASH_THREAD_ENV: &str = "FRODEX_LINEAGE_CRASH_THREAD";
const LINEAGE_CRASH_SOURCE_ENV: &str = "FRODEX_LINEAGE_CRASH_SOURCE";
const LINEAGE_CRASH_PHASE_ENV: &str = "FRODEX_LINEAGE_CRASH_PHASE";
const LINEAGE_CRASH_EXIT_CODE: i32 = 86;

fn lineage_phase_name(phase: LineageMigrationPhase) -> &'static str {
    match phase {
        LineageMigrationPhase::Planned => "planned",
        LineageMigrationPhase::TargetsDurable => "targets_durable",
        LineageMigrationPhase::ProjectionDurable => "projection_durable",
        LineageMigrationPhase::Selected => "selected",
        LineageMigrationPhase::Verified => "verified",
        LineageMigrationPhase::Complete => "complete",
    }
}

fn parse_lineage_phase(value: &str) -> LineageMigrationPhase {
    match value {
        "planned" => LineageMigrationPhase::Planned,
        "targets_durable" => LineageMigrationPhase::TargetsDurable,
        "projection_durable" => LineageMigrationPhase::ProjectionDurable,
        "selected" => LineageMigrationPhase::Selected,
        "verified" => LineageMigrationPhase::Verified,
        "complete" => LineageMigrationPhase::Complete,
        _ => panic!("unknown lineage crash phase: {value}"),
    }
}

#[tokio::test]
#[ignore = "subprocess helper for migration_process_death_recovers_every_durable_phase"]
async fn migration_lineage_process_crash_child() {
    let Some(home) = std::env::var_os(LINEAGE_CRASH_HOME_ENV).map(PathBuf::from) else {
        return;
    };
    let thread_id = ThreadId::from_string(
        std::env::var(LINEAGE_CRASH_THREAD_ENV)
            .expect("lineage crash thread id")
            .as_str(),
    )
    .expect("parse lineage crash thread id");
    let source_path = PathBuf::from(
        std::env::var_os(LINEAGE_CRASH_SOURCE_ENV).expect("lineage crash source path"),
    );
    let phase = parse_lineage_phase(
        std::env::var(LINEAGE_CRASH_PHASE_ENV)
            .expect("lineage crash phase")
            .as_str(),
    );
    let store = indexed_store(home.as_path()).await;
    let plan = plan_legacy_lineage(home.as_path(), source_path.as_path())
        .await
        .expect("plan subprocess lineage migration");
    assert_eq!(plan.selected_thread_id, thread_id);
    let journal_path = migration_journal_path(home.as_path(), thread_id);
    let mut limiter =
        RolloutMigrationRateLimiter::new(Some(1024)).expect("create subprocess migration limiter");
    let error = store
        .migrate_legacy_lineage_until_phase_for_test(
            source_path.as_path(),
            journal_path.as_path(),
            plan,
            &mut limiter,
            phase,
        )
        .await
        .expect_err("stop subprocess after durable phase");
    assert!(
        error
            .to_string()
            .contains("injected lineage migration stop")
    );
    assert!(journal_path.exists());

    // `process::exit` runs no Rust destructors. The parent therefore recovers state left by a
    // process that died after the journal phase became durable, rather than a dropped test store.
    std::process::exit(LINEAGE_CRASH_EXIT_CODE);
}

#[tokio::test]
async fn migration_process_death_recovers_every_durable_phase() {
    assert_migration_process_death_recovers_every_durable_phase(ThreadHistoryMode::Legacy).await;
}

#[tokio::test]
async fn native_history_base_migration_process_death_recovers_every_durable_phase() {
    assert_migration_process_death_recovers_every_durable_phase(ThreadHistoryMode::Paginated).await;
}

async fn assert_migration_process_death_recovers_every_durable_phase(
    history_mode: ThreadHistoryMode,
) {
    for phase in [
        LineageMigrationPhase::Planned,
        LineageMigrationPhase::TargetsDurable,
        LineageMigrationPhase::ProjectionDurable,
        LineageMigrationPhase::Selected,
        LineageMigrationPhase::Verified,
        LineageMigrationPhase::Complete,
    ] {
        let home = TempDir::new().expect("create Codex home");
        let thread_id = ThreadId::new();
        let segment_ids = [SegmentId::new(), SegmentId::new()];
        let filename = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
        let immutable = home
            .path()
            .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
            .join(thread_id.to_string())
            .join(segment_ids[0].to_string())
            .join(filename.as_str());
        let predecessor_end = if history_mode == ThreadHistoryMode::Paginated {
            write_paginated_segment(
                immutable.as_path(),
                home.path(),
                thread_id,
                segment_ids[0],
                /*start_ordinal*/ 0,
                vec![user_message("process crash immutable marker")],
            )
        } else {
            write_legacy_segment(
                immutable.as_path(),
                home.path(),
                thread_id,
                segment_ids[0],
                vec![user_message("process crash immutable marker")],
            );
            0
        };
        let active = home.path().join("sessions/2025/01/03").join(filename);
        let active_items = vec![
            segment_reference(immutable.clone(), thread_id, segment_ids[0]),
            user_message("process crash active marker"),
        ];
        if history_mode == ThreadHistoryMode::Paginated {
            write_paginated_segment(
                active.as_path(),
                home.path(),
                thread_id,
                segment_ids[1],
                predecessor_end,
                active_items,
            );
        } else {
            write_legacy_segment(
                active.as_path(),
                home.path(),
                thread_id,
                segment_ids[1],
                active_items,
            );
        }
        let source_bytes = [
            fs::read(immutable.as_path()).expect("read immutable source"),
            fs::read(active.as_path()).expect("read active source"),
        ];
        drop(indexed_store(home.path()).await);

        let output = std::process::Command::new(
            std::env::current_exe().expect("current thread-store test executable"),
        )
        .arg("--exact")
        .arg("local::rollout_migration::tests::migration_lineage_process_crash_child")
        .arg("--ignored")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(LINEAGE_CRASH_HOME_ENV, home.path())
        .env(LINEAGE_CRASH_THREAD_ENV, thread_id.to_string())
        .env(LINEAGE_CRASH_SOURCE_ENV, active.as_path())
        .env(LINEAGE_CRASH_PHASE_ENV, lineage_phase_name(phase))
        .output()
        .expect("run lineage crash subprocess");
        assert_eq!(
            output.status.code(),
            Some(LINEAGE_CRASH_EXIT_CODE),
            "phase {phase:?}; stdout: {}; stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let restarted = indexed_store(home.path()).await;
        let recovered = restarted
            .migrate_rollouts(apply_options())
            .await
            .expect("recover after process death");
        assert_eq!(recovered.outcomes.len(), 1, "phase {phase:?}");
        assert_eq!(
            recovered.outcomes[0].status,
            RolloutMigrationStatus::Migrated,
            "phase {phase:?}: {:?}",
            recovered.outcomes[0].message
        );
        let selected = restarted
            .state_db
            .as_ref()
            .expect("state db")
            .get_thread(thread_id)
            .await
            .expect("read recovered thread")
            .expect("recovered thread metadata");
        let materialized =
            codex_rollout::materialize_rollout_lines(home.path(), selected.rollout_path.as_path())
                .await
                .expect("materialize process-recovered lineage");
        let json = serde_json::to_string(&materialized).expect("serialize recovered lineage");
        assert!(json.contains("process crash immutable marker"));
        assert!(json.contains("process crash active marker"));
        if history_mode == ThreadHistoryMode::Paginated {
            let selected_text = fs::read_to_string(selected.rollout_path.as_path())
                .expect("read recovered selected rollout");
            assert!(!selected_text.contains("rollout_reference"));
            assert!(
                codex_rollout::read_session_meta_line(selected.rollout_path.as_path())
                    .await
                    .expect("read recovered selected metadata")
                    .meta
                    .history_base
                    .is_some()
            );
        }
        assert_eq!(
            [
                fs::read(immutable.as_path()).expect("reread immutable source"),
                fs::read(active.as_path()).expect("reread active source"),
            ],
            source_bytes
        );
    }
}

#[tokio::test]
async fn migration_preserves_archived_compressed_segmented_lineage_and_sources() {
    assert_migration_preserves_archived_compressed_lineage(ThreadHistoryMode::Legacy).await;
}

#[tokio::test]
async fn native_history_base_migration_preserves_archived_compressed_lineage_and_sources() {
    assert_migration_preserves_archived_compressed_lineage(ThreadHistoryMode::Paginated).await;
}

async fn assert_migration_preserves_archived_compressed_lineage(history_mode: ThreadHistoryMode) {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let segment_ids = [SegmentId::new(), SegmentId::new()];
    let filename = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
    let immutable_plain = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(segment_ids[0].to_string())
        .join(filename.as_str());
    let predecessor_end = if history_mode == ThreadHistoryMode::Paginated {
        write_paginated_segment(
            immutable_plain.as_path(),
            home.path(),
            thread_id,
            segment_ids[0],
            0,
            vec![user_message("compressed predecessor")],
        )
    } else {
        write_legacy_segment(
            immutable_plain.as_path(),
            home.path(),
            thread_id,
            segment_ids[0],
            vec![user_message("compressed predecessor")],
        );
        0
    };
    let active_plain = home.path().join("sessions/2025/01/03").join(filename);
    let active_items = vec![
        segment_reference(immutable_plain.clone(), thread_id, segment_ids[0]),
        user_message("archived active"),
    ];
    if history_mode == ThreadHistoryMode::Paginated {
        write_paginated_segment(
            active_plain.as_path(),
            home.path(),
            thread_id,
            segment_ids[1],
            predecessor_end,
            active_items,
        );
    } else {
        write_legacy_segment(
            active_plain.as_path(),
            home.path(),
            thread_id,
            segment_ids[1],
            active_items,
        );
    }
    let immutable = compress_rollout(immutable_plain.as_path());
    let archived_plain = move_to_archived(home.path(), active_plain);
    let archived = compress_rollout(archived_plain.as_path());
    let source_bytes = [
        fs::read(immutable.as_path()).expect("read compressed predecessor"),
        fs::read(archived.as_path()).expect("read compressed active"),
    ];
    let store = indexed_store(home.path()).await;

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate archived compressed lineage");
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(
        report.outcomes[0].status,
        RolloutMigrationStatus::Migrated,
        "{:?}",
        report.outcomes[0].message
    );
    let selected_path = report.outcomes[0].rollout_path.clone();
    assert!(selected_path.starts_with(home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR)));
    assert_eq!(
        selected_path.extension().and_then(|value| value.to_str()),
        Some("zst")
    );
    assert_eq!(
        [
            fs::read(immutable.as_path()).expect("reread compressed predecessor"),
            fs::read(archived.as_path()).expect("reread compressed active"),
        ],
        source_bytes
    );
    if history_mode == ThreadHistoryMode::Paginated {
        let selected_meta = codex_rollout::read_session_meta_line(selected_path.as_path())
            .await
            .expect("read archived native metadata");
        assert!(selected_meta.meta.history_base.is_some());
        let selected_items = RolloutRecorder::load_rollout_items(selected_path.as_path())
            .await
            .expect("read archived native target")
            .0;
        assert!(
            selected_items
                .iter()
                .all(|item| !matches!(item, RolloutItem::RolloutReference(_)))
        );
    }
    let materialized =
        codex_rollout::materialize_rollout_lines(home.path(), selected_path.as_path())
            .await
            .expect("materialize compressed migrated lineage");
    let json = serde_json::to_string(&materialized).expect("serialize history");
    assert!(json.contains("compressed predecessor"));
    assert!(json.contains("archived active"));
}

#[tokio::test]
async fn migration_refuses_source_mutation_after_lineage_journal_is_durable() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let segment_ids = [SegmentId::new(), SegmentId::new()];
    let filename = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
    let immutable = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(segment_ids[0].to_string())
        .join(filename.as_str());
    write_legacy_segment(
        immutable.as_path(),
        home.path(),
        thread_id,
        segment_ids[0],
        vec![user_message("immutable")],
    );
    let active = home.path().join("sessions/2025/01/03").join(filename);
    write_legacy_segment(
        active.as_path(),
        home.path(),
        thread_id,
        segment_ids[1],
        vec![
            segment_reference(immutable, thread_id, segment_ids[0]),
            user_message("active"),
        ],
    );
    let store = indexed_store(home.path()).await;
    let plan = plan_legacy_lineage(home.path(), active.as_path())
        .await
        .expect("plan lineage");
    let journal_path = migration_journal_path(home.path(), thread_id);
    write_lineage_migration_journal(
        journal_path.as_path(),
        &LineageMigrationJournal::from_plan(&plan),
    )
    .await
    .expect("write planned journal");
    fs::OpenOptions::new()
        .append(true)
        .open(active.as_path())
        .expect("open active source")
        .write_all(b"mutated")
        .expect("mutate source");

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("report changed source");
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(
        report.outcomes[0].status,
        RolloutMigrationStatus::Failed,
        "{:?}",
        report.outcomes[0].message
    );
    assert!(
        report.outcomes[0]
            .message
            .as_deref()
            .is_some_and(|message| message.contains("manifest does not match")),
        "{:?}",
        report.outcomes[0].message
    );
    assert!(!journal_path.exists());
    assert!(plan.targets.iter().all(|target| !target.path.exists()));
}

#[tokio::test]
async fn lineage_migration_plan_rejects_reference_cycles() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let rollout_file = format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl");
    let segment_ids = [SegmentId::new(), SegmentId::new()];
    let immutable_path = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(segment_ids[0].to_string())
        .join(rollout_file.as_str());
    let active_path = home.path().join("sessions/2025/01/03").join(rollout_file);
    write_legacy_segment(
        immutable_path.as_path(),
        home.path(),
        thread_id,
        segment_ids[0],
        vec![segment_reference(
            active_path.clone(),
            thread_id,
            segment_ids[1],
        )],
    );
    write_legacy_segment(
        active_path.as_path(),
        home.path(),
        thread_id,
        segment_ids[1],
        vec![segment_reference(immutable_path, thread_id, segment_ids[0])],
    );

    let error = plan_legacy_lineage(home.path(), active_path.as_path())
        .await
        .expect_err("cyclic lineage must fail");
    assert!(error.to_string().contains("contains a cycle"));
}

#[tokio::test]
async fn lineage_migration_plan_rejects_missing_predecessor() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let missing_thread_id = ThreadId::new();
    let active_path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![RolloutItem::RolloutReference(RolloutReferenceItem {
            rollout_id: Some(missing_thread_id),
            rollout_path: home.path().join(format!(
                "sessions/2025/01/03/rollout-2025-01-03T11-00-00-{missing_thread_id}.jsonl"
            )),
            thread_id: Some(missing_thread_id),
            rollout_timestamp: None,
            segment_id: None,
            max_depth: codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        })],
    );

    let error = plan_legacy_lineage(home.path(), active_path.as_path())
        .await
        .expect_err("missing predecessor must fail");
    assert!(error.to_string().contains("could not be resolved"));
}

#[tokio::test]
async fn lineage_migration_plan_rejects_non_leading_reference() {
    let home = TempDir::new().expect("create Codex home");
    let parent_id = ThreadId::new();
    let parent_path = write_rollout(
        home.path(),
        parent_id,
        SessionSource::Cli,
        vec![user_message("parent")],
    );
    let thread_id = ThreadId::new();
    let active_path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            user_message("local record before reference"),
            RolloutItem::RolloutReference(RolloutReferenceItem {
                rollout_id: Some(parent_id),
                rollout_path: parent_path,
                thread_id: Some(parent_id),
                rollout_timestamp: None,
                segment_id: None,
                max_depth: codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
                nth_user_message: None,
                compacted_replacement_history_filter_texts: None,
            }),
        ],
    );

    let error = plan_legacy_lineage(home.path(), active_path.as_path())
        .await
        .expect_err("non-leading reference must fail");
    assert!(error.to_string().contains("non-leading reference"));
}

#[tokio::test]
async fn lineage_migration_journal_round_trips_and_verifies_sources() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![user_message("source")],
    );
    let plan = plan_legacy_lineage(home.path(), path.as_path())
        .await
        .expect("plan lineage");
    let mut journal = LineageMigrationJournal::from_plan(&plan);
    let journal_path = home.path().join("rollout-migrations/lineage.pending");
    write_lineage_migration_journal(journal_path.as_path(), &journal)
        .await
        .expect("write journal");
    let loaded = read_lineage_migration_journal(journal_path.as_path())
        .await
        .expect("read journal");
    assert_eq!(loaded, journal);
    loaded.verify_sources().await.expect("verify sources");

    journal
        .advance(LineageMigrationPhase::TargetsDurable)
        .expect("advance journal");
    write_lineage_migration_journal(journal_path.as_path(), &journal)
        .await
        .expect("replace journal");
    assert_eq!(
        read_lineage_migration_journal(journal_path.as_path())
            .await
            .expect("read replaced journal")
            .phase,
        LineageMigrationPhase::TargetsDurable
    );
    assert!(
        journal
            .advance(LineageMigrationPhase::Planned)
            .expect_err("journal phase cannot move backward")
            .to_string()
            .contains("cannot move backward")
    );
}

#[tokio::test]
async fn lineage_migration_journal_rejects_changed_source() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![user_message("source")],
    );
    let plan = plan_legacy_lineage(home.path(), path.as_path())
        .await
        .expect("plan lineage");
    let journal = LineageMigrationJournal::from_plan(&plan);
    fs::OpenOptions::new()
        .append(true)
        .open(path)
        .expect("open source")
        .write_all(b"changed")
        .expect("change source");
    assert!(
        journal
            .verify_sources()
            .await
            .expect_err("changed source must fail")
            .to_string()
            .contains("source changed after planning")
    );
}

#[tokio::test]
async fn lineage_migration_journal_rejects_unknown_version_and_invalid_json() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![user_message("source")],
    );
    let plan = plan_legacy_lineage(home.path(), path.as_path())
        .await
        .expect("plan lineage");
    let journal = LineageMigrationJournal::from_plan(&plan);
    let mut value = serde_json::to_value(journal).expect("serialize journal");
    value["version"] = serde_json::json!(999);
    let journal_path = home.path().join("lineage.pending");
    fs::write(
        journal_path.as_path(),
        serde_json::to_vec(&value).expect("serialize unknown journal"),
    )
    .expect("write unknown journal");
    assert!(
        read_lineage_migration_journal(journal_path.as_path())
            .await
            .expect_err("unknown version must fail")
            .to_string()
            .contains("unsupported lineage migration journal version")
    );
    fs::write(journal_path.as_path(), b"not json").expect("write invalid journal");
    assert!(
        read_lineage_migration_journal(journal_path.as_path())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn migration_refuses_reference_backed_legacy_history_without_copying_parent() {
    let home = TempDir::new().expect("create Codex home");
    let parent_id = ThreadId::new();
    let parent_path = write_rollout(
        home.path(),
        parent_id,
        SessionSource::Cli,
        vec![user_message("parent question")],
    );
    let child_id = ThreadId::new();
    let child_path = write_rollout(
        home.path(),
        child_id,
        SessionSource::Cli,
        vec![
            RolloutItem::RolloutReference(RolloutReferenceItem {
                rollout_id: Some(parent_id),
                rollout_path: parent_path.clone(),
                thread_id: Some(parent_id),
                rollout_timestamp: None,
                segment_id: None,
                max_depth: codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
                nth_user_message: Some(1),
                compacted_replacement_history_filter_texts: None,
            }),
            user_message("child question"),
        ],
    );
    let parent_original = fs::read(&parent_path).expect("read parent rollout");
    let child_original = fs::read(&child_path).expect("read child rollout");
    let store = indexed_store(home.path()).await;
    thread_history::apply_projection(
        &store,
        child_id,
        /*start_offset*/ 0,
        /*next_offset*/ 0,
        /*initial_ordinal*/ 0,
        Vec::new(),
    )
    .await
    .expect("seed child projection checkpoint");
    let child_projection_before = projection_checkpoint(&store, child_id).await;
    let parent_rows_before = history_row_counts(&store, parent_id).await;
    let child_rows_before = history_row_counts(&store, child_id).await;

    let report = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![child_id],
            ..apply_options()
        })
        .await
        .expect("refuse reference-backed migration");

    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Failed);
    assert!(
        report.outcomes[0]
            .message
            .as_deref()
            .is_some_and(|message| message.contains("reference-backed legacy rollout"))
    );
    assert_eq!(
        fs::read(&parent_path).expect("read parent rollout"),
        parent_original
    );
    assert_eq!(
        fs::read(&child_path).expect("read child rollout"),
        child_original
    );
    assert_eq!(
        projection_checkpoint(&store, child_id).await,
        child_projection_before
    );
    assert_eq!(
        history_row_counts(&store, parent_id).await,
        parent_rows_before
    );
    assert_eq!(
        history_row_counts(&store, child_id).await,
        child_rows_before
    );
    assert_eq!(child_rows_before, (0, 0));
    assert_eq!(
        store
            .state_db
            .as_ref()
            .expect("state db")
            .get_thread(child_id)
            .await
            .expect("read child metadata")
            .expect("child metadata")
            .history_mode,
        ThreadHistoryMode::Legacy
    );
    assert_no_migration_artifacts(home.path(), &child_path, child_id).await;
}

#[tokio::test]
async fn migration_preserves_filtered_cross_thread_immutable_reference() {
    let home = TempDir::new().expect("create Codex home");
    let parent_id = ThreadId::new();
    let parent_segment_id = SegmentId::new();
    let parent_path = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(parent_id.to_string())
        .join(parent_segment_id.to_string())
        .join(format!("rollout-2025-01-03T11-00-00-{parent_id}.jsonl"));
    write_legacy_segment(
        parent_path.as_path(),
        home.path(),
        parent_id,
        parent_segment_id,
        vec![
            user_message("excluded parent marker"),
            user_message("included parent marker"),
        ],
    );
    let child_id = ThreadId::new();
    let child_segment_id = SegmentId::new();
    let child_path = home
        .path()
        .join("sessions/2025/01/03")
        .join(format!("rollout-2025-01-03T12-00-00-{child_id}.jsonl"));
    write_legacy_segment(
        child_path.as_path(),
        home.path(),
        child_id,
        child_segment_id,
        vec![
            RolloutItem::RolloutReference(RolloutReferenceItem {
                rollout_id: Some(parent_id),
                rollout_path: parent_path.clone(),
                thread_id: Some(parent_id),
                rollout_timestamp: None,
                segment_id: Some(parent_segment_id),
                max_depth: codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
                nth_user_message: Some(1),
                compacted_replacement_history_filter_texts: Some(Vec::new()),
            }),
            user_message("child marker"),
        ],
    );
    let source_bytes = [
        fs::read(parent_path.as_path()).expect("read parent source"),
        fs::read(child_path.as_path()).expect("read child source"),
    ];
    let store = indexed_store(home.path()).await;

    let report = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![child_id],
            ..apply_options()
        })
        .await
        .expect("migrate filtered immutable fork");
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(
        report.outcomes[0].status,
        RolloutMigrationStatus::Migrated,
        "{:?}",
        report.outcomes[0].message
    );
    let materialized = codex_rollout::materialize_rollout_lines(
        home.path(),
        report.outcomes[0].rollout_path.as_path(),
    )
    .await
    .expect("materialize migrated fork");
    let json = serde_json::to_string(&materialized).expect("serialize migrated fork");
    assert!(json.contains("excluded parent marker"));
    assert!(!json.contains("included parent marker"), "{json}");
    assert!(json.contains("child marker"));
    assert_eq!(
        [
            fs::read(parent_path).expect("reread parent source"),
            fs::read(child_path).expect("reread child source"),
        ],
        source_bytes
    );
}

#[tokio::test]
async fn migration_retains_filtered_paginated_immutable_reference() {
    let home = TempDir::new().expect("create Codex home");
    let parent_id = ThreadId::new();
    let parent_segment_id = SegmentId::new();
    let parent_path = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(parent_id.to_string())
        .join(parent_segment_id.to_string())
        .join(format!("rollout-2025-01-03T11-00-00-{parent_id}.jsonl"));
    let parent_end = write_paginated_segment(
        parent_path.as_path(),
        home.path(),
        parent_id,
        parent_segment_id,
        /*start_ordinal*/ 0,
        vec![
            started("parent-turn-1"),
            completed_user_message(
                parent_id,
                "parent-turn-1",
                "parent-user-1",
                "included Paginated parent marker",
            ),
            completed("parent-turn-1"),
            started("parent-turn-2"),
            completed_user_message(
                parent_id,
                "parent-turn-2",
                "parent-user-2",
                "excluded Paginated parent marker",
            ),
            completed("parent-turn-2"),
        ],
    );
    assert_eq!(parent_end, 7);
    let parent_bytes = fs::read(parent_path.as_path()).expect("read Paginated parent");
    let child_id = ThreadId::new();
    let child_segment_id = SegmentId::new();
    let child_path = home
        .path()
        .join("sessions/2025/01/03")
        .join(format!("rollout-2025-01-03T12-00-00-{child_id}.jsonl"));
    write_legacy_segment(
        child_path.as_path(),
        home.path(),
        child_id,
        child_segment_id,
        vec![
            RolloutItem::RolloutReference(RolloutReferenceItem {
                rollout_id: Some(parent_id),
                rollout_path: parent_path.clone(),
                thread_id: Some(parent_id),
                rollout_timestamp: None,
                segment_id: Some(parent_segment_id),
                max_depth: codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH,
                nth_user_message: Some(1),
                compacted_replacement_history_filter_texts: Some(Vec::new()),
            }),
            user_message("child marker"),
        ],
    );
    let child_bytes = fs::read(child_path.as_path()).expect("read Legacy child");
    let store = indexed_store(home.path()).await;

    let dry_run = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![child_id],
            ..RolloutMigrationOptions::default()
        })
        .await
        .expect("dry-run child with Paginated reference");
    assert_eq!(dry_run.outcomes.len(), 1);
    assert_eq!(
        dry_run.outcomes[0].status,
        RolloutMigrationStatus::Eligible,
        "{:?}",
        dry_run.outcomes[0].message
    );
    let manifest = dry_run.outcomes[0]
        .manifest
        .as_ref()
        .expect("reference dry-run manifest");
    assert_eq!(manifest.sources.len(), 1);
    assert!(manifest.history_base_dependencies.is_empty());
    assert_eq!(manifest.reference_dependencies.len(), 1);
    assert_eq!(manifest.reference_dependencies[0].path, parent_path);
    assert_eq!(
        manifest.reference_dependencies[0].segment_id,
        parent_segment_id
    );
    assert_eq!(
        manifest.reference_dependencies[0].end_ordinal_exclusive,
        parent_end
    );
    assert_eq!(manifest.dependency_bytes, parent_bytes.len() as u64);
    assert_eq!(manifest.targets[0].start_ordinal, parent_end);
    assert_eq!(manifest.targets[0].history_base, None);
    assert!(!manifest.targets[0].path.exists());

    let report = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![child_id],
            ..apply_options()
        })
        .await
        .expect("migrate child with Paginated reference");
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(
        report.outcomes[0].status,
        RolloutMigrationStatus::Migrated,
        "{:?}",
        report.outcomes[0].message
    );
    let child_target_text =
        fs::read_to_string(report.outcomes[0].rollout_path.as_path()).expect("read child target");
    assert!(!child_target_text.contains("Paginated parent marker"));
    let materialized = codex_rollout::materialize_rollout_lines(
        home.path(),
        report.outcomes[0].rollout_path.as_path(),
    )
    .await
    .expect("materialize migrated child");
    let json = serde_json::to_string(&materialized).expect("serialize migrated child");
    assert!(json.contains("included Paginated parent marker"), "{json}");
    assert!(!json.contains("excluded Paginated parent marker"), "{json}");
    assert!(json.contains("child marker"), "{json}");
    assert_eq!(
        fs::read(parent_path.as_path()).expect("reread Paginated parent"),
        parent_bytes
    );
    assert_eq!(
        fs::read(child_path.as_path()).expect("reread Legacy child"),
        child_bytes
    );
}

#[tokio::test]
async fn migration_retains_compressed_paginated_immutable_reference() {
    let home = TempDir::new().expect("create Codex home");
    let parent_id = ThreadId::new();
    let parent_segment_id = SegmentId::new();
    let parent_path = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(parent_id.to_string())
        .join(parent_segment_id.to_string())
        .join(format!("rollout-2025-01-03T11-00-00-{parent_id}.jsonl"));
    write_paginated_segment(
        parent_path.as_path(),
        home.path(),
        parent_id,
        parent_segment_id,
        /*start_ordinal*/ 0,
        vec![user_message("compressed Paginated parent marker")],
    );
    let parent_path = compress_rollout(parent_path.as_path());
    let parent_bytes = fs::read(parent_path.as_path()).expect("read compressed parent");
    let child_id = ThreadId::new();
    let child_segment_id = SegmentId::new();
    let child_path = home
        .path()
        .join("sessions/2025/01/03")
        .join(format!("rollout-2025-01-03T12-00-00-{child_id}.jsonl"));
    write_legacy_segment(
        child_path.as_path(),
        home.path(),
        child_id,
        child_segment_id,
        vec![
            segment_reference(parent_path.clone(), parent_id, parent_segment_id),
            user_message("child marker"),
        ],
    );
    let child_bytes = fs::read(child_path.as_path()).expect("read Legacy child");
    let store = indexed_store(home.path()).await;

    let report = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![child_id],
            ..apply_options()
        })
        .await
        .expect("migrate child with compressed Paginated reference");
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(
        report.outcomes[0].status,
        RolloutMigrationStatus::Migrated,
        "{:?}",
        report.outcomes[0].message
    );
    let child_target_text =
        fs::read_to_string(report.outcomes[0].rollout_path.as_path()).expect("read child target");
    assert!(!child_target_text.contains("compressed Paginated parent marker"));
    assert!(!child_target_text.contains("rollout_reference"));
    let child_meta =
        codex_rollout::read_session_meta_line(report.outcomes[0].rollout_path.as_path())
            .await
            .expect("read migrated child metadata");
    let history_base = child_meta
        .meta
        .history_base
        .expect("compressed history base");
    let migrated_parent =
        codex_rollout::find_rollout_path_by_rollout_id(home.path(), history_base.thread_id)
            .await
            .expect("resolve compressed native predecessor")
            .expect("compressed native predecessor exists");
    assert_eq!(
        migrated_parent
            .extension()
            .and_then(|extension| extension.to_str()),
        Some("zst")
    );
    let materialized = codex_rollout::materialize_rollout_lines(
        home.path(),
        report.outcomes[0].rollout_path.as_path(),
    )
    .await
    .expect("materialize migrated child");
    let json = serde_json::to_string(&materialized).expect("serialize migrated child");
    assert!(json.contains("compressed Paginated parent marker"));
    assert!(json.contains("child marker"));
    assert_eq!(
        fs::read(parent_path.as_path()).expect("reread compressed parent"),
        parent_bytes
    );
    assert_eq!(
        fs::read(child_path.as_path()).expect("reread Legacy child"),
        child_bytes
    );
}

#[tokio::test]
async fn migration_rejects_paginated_reference_with_non_contiguous_ordinals() {
    let home = TempDir::new().expect("create Codex home");
    let parent_id = ThreadId::new();
    let parent_segment_id = SegmentId::new();
    let parent_path = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(parent_id.to_string())
        .join(parent_segment_id.to_string())
        .join(format!("rollout-2025-01-03T11-00-00-{parent_id}.jsonl"));
    write_paginated_segment(
        parent_path.as_path(),
        home.path(),
        parent_id,
        parent_segment_id,
        /*start_ordinal*/ 0,
        vec![user_message("parent marker")],
    );
    let parent_text = fs::read_to_string(parent_path.as_path()).expect("read parent");
    let mut parent_lines = parent_text
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("parse parent line"))
        .collect::<Vec<_>>();
    parent_lines[1]["ordinal"] = json!(2);
    let corrupted_parent = parent_lines
        .iter()
        .map(|line| serde_json::to_string(line).expect("serialize parent line"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    fs::write(parent_path.as_path(), corrupted_parent.as_bytes()).expect("write corrupt parent");

    let child_id = ThreadId::new();
    let child_segment_id = SegmentId::new();
    let child_path = home
        .path()
        .join("sessions/2025/01/03")
        .join(format!("rollout-2025-01-03T12-00-00-{child_id}.jsonl"));
    write_legacy_segment(
        child_path.as_path(),
        home.path(),
        child_id,
        child_segment_id,
        vec![
            segment_reference(parent_path.clone(), parent_id, parent_segment_id),
            user_message("child marker"),
        ],
    );
    let child_bytes = fs::read(child_path.as_path()).expect("read child source");
    let store = indexed_store(home.path()).await;

    let report = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![child_id],
            ..apply_options()
        })
        .await
        .expect("report invalid Paginated dependency");
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Failed);
    assert!(
        report.outcomes[0]
            .message
            .as_deref()
            .is_some_and(|message| message.contains("non-contiguous ordinal")),
        "{:?}",
        report.outcomes[0].message
    );
    assert_eq!(
        fs::read(child_path.as_path()).expect("reread child source"),
        child_bytes
    );
    assert_no_migration_artifacts(home.path(), child_path.as_path(), child_id).await;
}

#[tokio::test]
async fn migration_rejects_paginated_reference_change_after_targets_are_durable() {
    let home = TempDir::new().expect("create Codex home");
    let parent_id = ThreadId::new();
    let parent_segment_id = SegmentId::new();
    let parent_path = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(parent_id.to_string())
        .join(parent_segment_id.to_string())
        .join(format!("rollout-2025-01-03T11-00-00-{parent_id}.jsonl"));
    let parent_end = write_paginated_segment(
        parent_path.as_path(),
        home.path(),
        parent_id,
        parent_segment_id,
        /*start_ordinal*/ 0,
        vec![user_message("parent marker")],
    );
    let child_id = ThreadId::new();
    let child_segment_id = SegmentId::new();
    let child_path = home
        .path()
        .join("sessions/2025/01/03")
        .join(format!("rollout-2025-01-03T12-00-00-{child_id}.jsonl"));
    write_legacy_segment(
        child_path.as_path(),
        home.path(),
        child_id,
        child_segment_id,
        vec![
            segment_reference(parent_path.clone(), parent_id, parent_segment_id),
            user_message("child marker"),
        ],
    );
    let child_bytes = fs::read(child_path.as_path()).expect("read child source");
    let store = indexed_store(home.path()).await;
    let plan = plan_legacy_lineage(home.path(), child_path.as_path())
        .await
        .expect("plan child lineage");
    let journal_path = migration_journal_path(home.path(), child_id);
    let mut limiter =
        RolloutMigrationRateLimiter::new(Some(1024)).expect("create migration limiter");
    let error = store
        .migrate_legacy_lineage_until_phase_for_test(
            child_path.as_path(),
            journal_path.as_path(),
            plan,
            &mut limiter,
            LineageMigrationPhase::TargetsDurable,
        )
        .await
        .expect_err("stop after staged targets are durable");
    assert!(
        error
            .to_string()
            .contains("injected lineage migration stop")
    );
    let mut parent = fs::OpenOptions::new()
        .append(true)
        .open(parent_path.as_path())
        .expect("open parent for append");
    let late_line = RolloutLine {
        timestamp: "2025-01-03T12:00:01Z".to_string(),
        ordinal: Some(parent_end),
        item: agent_message("late parent append"),
    };
    writeln!(
        parent,
        "{}",
        serde_json::to_string(&late_line).expect("serialize late parent line")
    )
    .expect("append parent line");
    parent.sync_all().expect("sync parent append");

    let report = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![child_id],
            ..apply_options()
        })
        .await
        .expect("report changed Paginated dependency");
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Failed);
    assert!(
        report.outcomes[0]
            .message
            .as_deref()
            .is_some_and(|message| message.contains("manifest does not match")),
        "{:?}",
        report.outcomes[0].message
    );
    let selected = store
        .state_db
        .as_ref()
        .expect("state db")
        .get_thread(child_id)
        .await
        .expect("read child metadata")
        .expect("child metadata");
    assert_eq!(selected.rollout_path, child_path);
    assert_eq!(selected.history_mode, ThreadHistoryMode::Legacy);
    assert_eq!(
        fs::read(child_path.as_path()).expect("reread child source"),
        child_bytes
    );
    assert_no_migration_artifacts(home.path(), child_path.as_path(), child_id).await;
}

#[tokio::test]
async fn migration_refuses_archived_compressed_malformed_reference_without_materialization() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![user_message("archived question")],
    );
    let malformed_reference = json!({
        "timestamp": TIMESTAMP,
        "type": "rollout_reference",
        "payload": {"thread_id": thread_id}
    });
    writeln!(
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open rollout"),
        "{malformed_reference}"
    )
    .expect("append malformed reference");
    let archived = move_to_archived(home.path(), path);
    let compressed = compress_rollout(&archived);
    let original = fs::read(&compressed).expect("read compressed rollout");
    let store = indexed_store(home.path()).await;

    let report = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![thread_id],
            ..apply_options()
        })
        .await
        .expect("refuse compressed reference migration");

    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(
        report.outcomes[0].status,
        RolloutMigrationStatus::Failed,
        "{:?}",
        report.outcomes[0].message
    );
    assert_eq!(
        fs::read(&compressed).expect("read refused compressed rollout"),
        original
    );
    assert!(!archived.exists());
    assert_no_migration_artifacts(home.path(), &compressed, thread_id).await;
}

#[tokio::test]
async fn migration_projects_explicit_and_implicit_legacy_completed_items() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let exec = exec_completion("explicit", "call-1");
    let reasoning = serde_json::from_value(json!({
        "type": "event_msg",
        "payload": {"type": "agent_reasoning", "text": "summary"}
    }))
    .expect("build legacy reasoning summary");
    let raw_reasoning = serde_json::from_value(json!({
        "type": "event_msg",
        "payload": {"type": "agent_reasoning_raw_content", "text": "raw"}
    }))
    .expect("build legacy reasoning content");
    write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            started("explicit"),
            exec,
            completed("explicit"),
            reasoning,
            raw_reasoning,
        ],
    );
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate legacy completion events");

    let turns = list_active_summary_turns(&store, thread_id).await;
    assert_eq!(turns.turns.len(), 2);
    assert_eq!(turns.turns[0].turn_id, "explicit");
    let items = store
        .list_items(ListItemsParams {
            thread_id,
            turn_id: None,
            include_archived: false,
            cursor: None,
            page_size: 10,
            sort_direction: SortDirection::Asc,
            sort_key: ItemSortKey::CreatedAtOrdinal,
            after_updated_at_ordinal: None,
        })
        .await
        .expect("read projected items");
    assert_eq!(items.items.len(), 2);
    assert_eq!(items.items[0].turn_id, "explicit");
    assert_eq!(items.items[1].turn_id, turns.turns[1].turn_id);
    let command: serde_json::Value =
        serde_json::from_slice(&items.items[0].item_json).expect("parse projected command");
    let reasoning: serde_json::Value =
        serde_json::from_slice(&items.items[1].item_json).expect("parse projected reasoning");
    assert_eq!(command["type"], "commandExecution");
    assert_eq!(reasoning["type"], "reasoning");
    assert_eq!(reasoning["summary"], json!(["summary"]));
    assert_eq!(reasoning["content"], json!(["raw"]));
}

#[tokio::test]
async fn migration_preserves_image_generation_failure_metadata() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let expected_item = ImageGenerationItem {
        id: "image-call".to_string(),
        status: "failed".to_string(),
        revised_prompt: Some("paint a blue whale".to_string()),
        result: String::new(),
        transparent_background: None,
        failure: Some(ImageGenerationFailure::UsageLimitExceeded {
            limit_id: "image_gen".to_string(),
            resets_at: Some(1_786_150_800),
        }),
        saved_path: None,
        imagegen_request_id: None,
    };
    let image_completion =
        RolloutItem::EventMsg(EventMsg::ImageGenerationEnd(ImageGenerationEndEvent {
            call_id: expected_item.id.clone(),
            status: expected_item.status.clone(),
            revised_prompt: expected_item.revised_prompt.clone(),
            result: expected_item.result.clone(),
            transparent_background: expected_item.transparent_background,
            failure: expected_item.failure.clone(),
            saved_path: expected_item.saved_path.clone(),
        }));
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            started("image-turn"),
            image_completion,
            completed("image-turn"),
        ],
    );
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate legacy image completion");

    let migrated_item = read_rollout(&path)
        .into_iter()
        .find_map(|line| match line.item {
            RolloutItem::EventMsg(EventMsg::ItemCompleted(event)) => Some(event.item),
            _ => None,
        })
        .expect("migrated image completion");
    let TurnItem::Extension(ExtensionItem::ImageGeneration(migrated_item)) = migrated_item else {
        panic!("expected migrated extension image-generation item");
    };
    assert_eq!(migrated_item, expected_item);
}

#[tokio::test]
async fn migration_keeps_late_completions_in_their_original_turn() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            started("old"),
            user_message("old question"),
            started("current"),
            user_message("current question"),
            exec_completion("old", "call-old"),
            agent_message("current answer"),
            completed("current"),
        ],
    );
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate late completion");

    let started_turn_ids = read_rollout(&path)
        .into_iter()
        .filter_map(|line| match line.item {
            RolloutItem::EventMsg(EventMsg::TurnStarted(event)) => Some(event.turn_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(started_turn_ids, vec!["old", "current"]);

    let items = store
        .list_items(ListItemsParams {
            thread_id,
            turn_id: None,
            include_archived: false,
            cursor: None,
            page_size: 10,
            sort_direction: SortDirection::Asc,
            sort_key: ItemSortKey::CreatedAtOrdinal,
            after_updated_at_ordinal: None,
        })
        .await
        .expect("read projected late completion");
    let item_turn_ids = items
        .items
        .iter()
        .map(|item| item.turn_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(item_turn_ids, vec!["old", "current", "old", "current"]);
}

#[tokio::test]
async fn migration_hoists_delayed_session_meta_before_paginated_history() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(home.path(), thread_id, SessionSource::Cli, Vec::new());
    let existing = fs::read_to_string(&path).expect("read legacy rollout");
    let pre_header = RolloutLine {
        timestamp: TIMESTAMP.to_string(),
        ordinal: None,
        item: user_message("before metadata"),
    };
    fs::write(
        &path,
        format!(
            "{}\n{existing}",
            serde_json::to_string(&pre_header).expect("serialize pre-header record")
        ),
    )
    .expect("write pre-header rollout");
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate delayed session metadata");

    let lines = read_rollout(&path);
    assert_eq!(lines[0].ordinal, Some(0));
    assert!(matches!(
        &lines[0].item,
        RolloutItem::SessionMeta(metadata)
            if metadata.meta.id == thread_id
                && metadata.meta.history_mode == ThreadHistoryMode::Paginated
    ));
    let turns = store
        .list_turns(ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: None,
            page_size: 10,
            sort_direction: SortDirection::Asc,
            items_view: StoredTurnItemsView::NotLoaded,
        })
        .await
        .expect("read migrated turns");
    assert_eq!(turns.turns.len(), 1);

    let second = store
        .migrate_rollouts(apply_options())
        .await
        .expect("rerun migration");
    assert_eq!(
        second.outcomes[0].status,
        RolloutMigrationStatus::AlreadyPaginated
    );
}

#[tokio::test]
async fn migration_preserves_valid_final_record_without_newline() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![user_message("question")],
    );
    let final_line = RolloutLine {
        timestamp: TIMESTAMP.to_string(),
        ordinal: None,
        item: agent_message("answer"),
    };
    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open legacy rollout")
        .write_all(
            serde_json::to_string(&final_line)
                .expect("serialize final record")
                .as_bytes(),
        )
        .expect("append final record");
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate legacy rollout");

    assert_eq!(
        read_rollout(&path)
            .iter()
            .filter(|line| matches!(line.item, RolloutItem::EventMsg(EventMsg::ItemCompleted(_))))
            .count(),
        2
    );
}

#[tokio::test]
async fn migration_applies_historical_rollbacks_before_sqlite_projection() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            started("keep"),
            user_message("keep question"),
            agent_message("keep answer"),
            completed("keep"),
            started("remove"),
            user_message("remove question"),
            agent_message("remove answer"),
            completed("remove"),
            started("shell"),
            completed("shell"),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
            started("replacement"),
            user_message("replacement question"),
            agent_message("replacement answer"),
            completed("replacement"),
        ],
    );
    let store = indexed_store(home.path()).await;

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate rolled-back thread");
    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Migrated);

    let lines = read_rollout(&path);
    assert!(!lines.iter().any(|line| matches!(
        line.item,
        RolloutItem::EventMsg(EventMsg::ThreadRolledBack(_))
    )));
    let turns = list_active_summary_turns(&store, thread_id).await;
    assert_eq!(
        turns
            .turns
            .iter()
            .map(|turn| turn.turn_id.as_str())
            .collect::<Vec<_>>(),
        vec!["keep", "replacement"]
    );
}

#[tokio::test]
async fn migration_rolls_back_response_and_inter_agent_user_boundaries() {
    let home = TempDir::new().expect("create Codex home");
    let response_thread_id = ThreadId::new();
    let response_path = write_rollout(
        home.path(),
        response_thread_id,
        SessionSource::Cli,
        vec![
            rollout_response_item(input_response_message("user", "remove response boundary")),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
            user_message("replacement question"),
        ],
    );
    let communication_thread_id = ThreadId::new();
    let communication_path = write_rollout(
        home.path(),
        communication_thread_id,
        SessionSource::Cli,
        vec![
            RolloutItem::InterAgentCommunication(InterAgentCommunication::new(
                AgentPath::root(),
                AgentPath::root().join("worker").expect("worker path"),
                Vec::new(),
                "remove communication boundary".to_string(),
                /*trigger_turn*/ true,
            )),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
            user_message("replacement question"),
        ],
    );
    let contextual_thread_id = ThreadId::new();
    let contextual_path = write_rollout(
        home.path(),
        contextual_thread_id,
        SessionSource::Cli,
        vec![
            rollout_response_item(input_response_message("user", "keep first boundary")),
            rollout_response_item(input_response_message(
                "developer",
                "<managed_developer_instructions>context only</managed_developer_instructions>",
            )),
            rollout_response_item(input_response_message(
                "developer",
                "<permissions instructions>context only</permissions instructions>",
            )),
            rollout_response_item(input_response_message(
                "user",
                "<environment_context>context only</environment_context>",
            )),
            rollout_response_item(input_response_message("user", "remove real user boundary")),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
            user_message("replacement question"),
        ],
    );
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate legacy rollback boundaries");

    assert!(
        !read_rollout(&response_path)
            .iter()
            .any(|line| matches!(line.item, RolloutItem::ResponseItem(_)))
    );
    assert!(
        !read_rollout(&communication_path)
            .iter()
            .any(|line| matches!(line.item, RolloutItem::InterAgentCommunication(_)))
    );
    assert_eq!(
        read_rollout(&contextual_path)
            .into_iter()
            .filter_map(|line| match line.item {
                RolloutItem::ResponseItem(response) => Some(response.into_item()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![input_response_message("user", "keep first boundary")]
    );
}

#[tokio::test]
async fn migration_drops_trailing_context_when_rollback_arrives_before_next_turn() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            rollout_response_item(input_response_message("user", "keep question")),
            rollout_response_item(input_response_message("user", "remove question")),
            rollout_response_item(input_response_message(
                "user",
                "<turn_aborted>remove this context too</turn_aborted>",
            )),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
            user_message("replacement question"),
        ],
    );
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate trailing rollback context");

    assert_eq!(
        read_rollout(&path)
            .into_iter()
            .filter_map(|line| match line.item {
                RolloutItem::ResponseItem(response) => Some(response.into_item()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![input_response_message("user", "keep question")]
    );
}

#[tokio::test]
async fn migration_coalesces_response_first_user_message_rollback_boundary() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            rollout_response_item(input_response_message("user", "remove question")),
            user_message("remove question"),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
            user_message("replacement question"),
        ],
    );
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate response-first rollback");

    assert!(
        !read_rollout(&path)
            .iter()
            .any(|line| matches!(line.item, RolloutItem::ResponseItem(_)))
    );
}

#[tokio::test]
async fn migration_does_not_coalesce_distinct_adjacent_user_records() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            rollout_response_item(input_response_message("user", "copied parent question")),
            user_message("child question"),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
            user_message("replacement question"),
        ],
    );
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate distinct adjacent user records");

    assert_eq!(
        read_rollout(&path)
            .into_iter()
            .filter_map(|line| match line.item {
                RolloutItem::ResponseItem(response) => Some(response.into_item()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![input_response_message("user", "copied parent question")]
    );
}

#[tokio::test]
async fn migration_keeps_late_completions_for_surviving_turns_across_rollback() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            started("old"),
            user_message("old question"),
            started("remove"),
            user_message("remove question"),
            completed("old"),
            exec_completion("old", "call-old"),
            item_completed("old", "reason-old"),
            exec_completion("remove", "call-remove"),
            completed("remove"),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
            started("replacement"),
            user_message("replacement question"),
            completed("replacement"),
        ],
    );
    let legacy_turns = build_turns_from_rollout_items(
        &read_rollout(&path)
            .into_iter()
            .map(|line| line.item)
            .collect::<Vec<_>>(),
    );
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate late completion rollback");

    let lines = read_rollout(&path);
    assert!(!lines.iter().any(|line| matches!(
        &line.item,
        RolloutItem::EventMsg(EventMsg::ItemCompleted(event))
            if event.item.id() == "call-remove"
    )));
    assert!(lines.iter().any(|line| matches!(
        &line.item,
        RolloutItem::EventMsg(EventMsg::ItemCompleted(event))
            if event.turn_id == "old" && event.item.id() == "call-old"
    )));
    assert!(lines.iter().any(|line| matches!(
        &line.item,
        RolloutItem::EventMsg(EventMsg::ItemCompleted(event))
            if event.turn_id == "old" && event.item.id() == "reason-old"
    )));
    assert!(lines.iter().any(|line| matches!(
        &line.item,
        RolloutItem::EventMsg(EventMsg::TurnComplete(event)) if event.turn_id == "old"
    )));
    assert!(!lines.iter().any(|line| matches!(
        &line.item,
        RolloutItem::EventMsg(EventMsg::TurnComplete(event)) if event.turn_id == "remove"
    )));
    let turns = store
        .list_turns(ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: None,
            page_size: 10,
            sort_direction: SortDirection::Asc,
            items_view: StoredTurnItemsView::Summary,
        })
        .await
        .expect("read late-completion rollback turns");
    let turn_ids = turns
        .turns
        .iter()
        .map(|turn| turn.turn_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        turn_ids,
        legacy_turns
            .iter()
            .map(|turn| turn.id.as_str())
            .collect::<Vec<_>>()
    );
    let items = store
        .list_items(ListItemsParams {
            thread_id,
            turn_id: None,
            include_archived: false,
            cursor: None,
            page_size: 20,
            sort_direction: SortDirection::Asc,
            sort_key: ItemSortKey::CreatedAtOrdinal,
            after_updated_at_ordinal: None,
        })
        .await
        .expect("read late-completion rollback items");
    for legacy_turn in &legacy_turns {
        for legacy_item in &legacy_turn.items {
            if legacy_item.id().starts_with("item-") {
                continue;
            }
            assert_eq!(
                items
                    .items
                    .iter()
                    .find(|item| item.item_id == legacy_item.id())
                    .map(|item| item.turn_id.as_str()),
                Some(legacy_turn.id.as_str())
            );
        }
    }
}

#[tokio::test]
async fn migration_rolls_back_inter_agent_metadata_with_its_delivery() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let delivery = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::root().join("worker").expect("worker path"),
        Vec::new(),
        "remove delivery".to_string(),
        /*trigger_turn*/ true,
    );
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            RolloutItem::InterAgentCommunicationMetadata { trigger_turn: true },
            rollout_response_item(delivery.to_model_input_item()),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
            user_message("replacement question"),
        ],
    );
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate rolled-back inter-agent delivery");

    assert!(!read_rollout(&path).iter().any(|line| match &line.item {
        RolloutItem::InterAgentCommunicationMetadata { .. } => true,
        RolloutItem::ResponseItem(response_item) => {
            matches!(&response_item.item, ResponseItem::AgentMessage { .. })
        }
        _ => false,
    }));
}

#[tokio::test]
async fn migration_rolls_back_pre_compaction_turns_from_sqlite_history() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let RolloutItem::Compacted(mut checkpoint) = compacted(vec![
        input_response_message("user", "old question"),
        ResponseItem::Message {
            id: None,
            role: "assistant".to_string(),
            content: vec![ContentItem::OutputText {
                text: "old answer".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        },
    ]) else {
        unreachable!("compacted helper always creates a compaction checkpoint");
    };
    checkpoint.mcp_resource_origins = Some(McpResourceOriginCheckpoint {
        origins: vec![McpResourceOrigin {
            call_id: "widget-call".to_string(),
            turn_id: Some("keep-before-compaction".to_string()),
            tool: "_product_search".to_string(),
            connector_id: "shopping".to_string(),
            link_id: None,
            uri: "ui://shopping/widget".to_string(),
            ambiguous_account: false,
        }],
        turns: vec!["keep-before-compaction".to_string()],
        current_turn_id: Some("keep-before-compaction".to_string()),
    });
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            started("keep-before-compaction"),
            user_message("old question"),
            completed("keep-before-compaction"),
            RolloutItem::Compacted(checkpoint),
            started("remove-after-compaction"),
            user_message("new question"),
            completed("remove-after-compaction"),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 2,
            })),
            user_message("replacement question"),
        ],
    );
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate rollback through compaction");

    let turns = store
        .list_turns(ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: None,
            page_size: 10,
            sort_direction: SortDirection::Asc,
            items_view: StoredTurnItemsView::Summary,
        })
        .await
        .expect("read rollback-through-compaction turns");
    assert_eq!(turns.turns.len(), 1);
    let checkpoint = read_rollout(&path)
        .into_iter()
        .find_map(|line| match line.item {
            RolloutItem::Compacted(item) => Some(item),
            _ => None,
        })
        .expect("retained compaction");
    assert_eq!(checkpoint.replacement_history, Some(Vec::new()));
    assert_eq!(checkpoint.mcp_resource_origins, None);
}

#[tokio::test]
async fn migration_preserves_reverse_replay_anchor_after_pre_compaction_rollback() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            rollout_response_item(input_response_message("user", "remove question")),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
            compacted(vec![input_response_message("user", "old question")]),
            user_message("replacement question"),
        ],
    );
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate pre-compaction rollback");

    let replacement_history = read_rollout(&path)
        .into_iter()
        .find_map(|line| match line.item {
            RolloutItem::Compacted(item) => item.replacement_history,
            _ => None,
        })
        .expect("retained compaction");
    assert!(replacement_history.is_empty());
}

#[tokio::test]
async fn migration_keeps_empty_replay_anchor_from_rolled_back_turn() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            started("keep"),
            user_message("keep question"),
            completed("keep"),
            started("remove"),
            user_message("remove question"),
            compacted(vec![input_response_message("user", "old question")]),
            completed("remove"),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
            started("replacement"),
            user_message("replacement question"),
            completed("replacement"),
        ],
    );
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate rolled-back replay anchor");

    let replacement_history = read_rollout(&path)
        .into_iter()
        .find_map(|line| match line.item {
            RolloutItem::Compacted(item) => item.replacement_history,
            _ => None,
        })
        .expect("retained compaction");
    assert!(replacement_history.is_empty());
}

#[tokio::test]
async fn migration_uses_turn_context_to_select_reverse_replay_anchor() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            started("keep"),
            user_message("keep question"),
            compacted(vec![input_response_message("user", "keep question")]),
            completed("keep"),
            started("remove"),
            user_message("remove question"),
            serde_json::from_value(json!({
                "type": "event_msg",
                "payload": {
                    "type": "turn_aborted",
                    "turn_id": "other",
                    "reason": "interrupted"
                }
            }))
            .expect("build mismatched turn abort"),
            serde_json::from_value(json!({
                "type": "turn_context",
                "payload": {
                    "turn_id": "remove",
                    "cwd": home.path(),
                    "approval_policy": "never",
                    "sandbox_policy": {"type": "read-only"},
                    "model": "test-model",
                    "summary": "auto"
                }
            }))
            .expect("build turn context"),
            compacted(vec![input_response_message("user", "remove question")]),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
            started("replacement"),
            user_message("replacement question"),
            completed("replacement"),
        ],
    );
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate turn-context replay anchor");

    let replacement_histories = read_rollout(&path)
        .into_iter()
        .filter_map(|line| match line.item {
            RolloutItem::Compacted(item) => item.replacement_history.map(|items| {
                items
                    .into_iter()
                    .map(codex_rollout::ResponseItemEnvelope::into_item)
                    .collect::<Vec<_>>()
            }),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        replacement_histories,
        vec![vec![input_response_message("user", "keep question")]]
    );
}

#[tokio::test]
async fn migration_applies_cumulative_and_overflowing_rollbacks() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            started("first"),
            user_message("first question"),
            completed("first"),
            started("second"),
            user_message("second question"),
            completed("second"),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 1,
            })),
            started("third"),
            user_message("third question"),
            completed("third"),
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(ThreadRolledBackEvent {
                num_turns: 99,
            })),
            user_message("replacement question"),
        ],
    );
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate cumulative rollbacks");

    let turns = store
        .list_turns(ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: None,
            page_size: 10,
            sort_direction: SortDirection::Asc,
            items_view: StoredTurnItemsView::Summary,
        })
        .await
        .expect("read cumulative rollback turns");
    assert_eq!(turns.turns.len(), 1);
    assert!(!read_rollout(&path).iter().any(|line| matches!(
        line.item,
        RolloutItem::EventMsg(EventMsg::ThreadRolledBack(_))
    )));
}

#[tokio::test]
async fn migration_drops_copied_user_fork_metadata_without_creating_a_history_base() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let parent_id = ThreadId::new();
    let copied_metadata = SessionMeta {
        session_id: parent_id.into(),
        id: parent_id,
        timestamp: TIMESTAMP.to_string(),
        cwd: home.path().to_path_buf(),
        source: SessionSource::Cli,
        ..SessionMeta::default()
    };
    let copied_response =
        rollout_response_item(input_response_message("user", "copied parent history"));
    let path = write_rollout_with_fork(
        home.path(),
        thread_id,
        SessionSource::Cli,
        Some(parent_id),
        vec![
            RolloutItem::SessionMeta(SessionMetaLine {
                meta: copied_metadata,
                git: None,
            }),
            copied_response,
            user_message("child question"),
            agent_message("child answer"),
        ],
    );
    let expected_responses = read_rollout(&path)
        .into_iter()
        .filter_map(|line| match line.item {
            RolloutItem::ResponseItem(item) => {
                Some(serde_json::to_value(item.item).expect("serialize copied response"))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let store = indexed_store(home.path()).await;

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate copied user fork");

    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Migrated);
    let lines = read_rollout(&path);
    assert!(matches!(
        &lines[0].item,
        RolloutItem::SessionMeta(metadata)
            if metadata.meta.id == thread_id
                && metadata.meta.forked_from_id == Some(parent_id)
                && metadata.meta.history_mode == ThreadHistoryMode::Paginated
                && metadata.meta.history_base.is_none()
    ));
    assert_eq!(
        lines
            .iter()
            .filter(|line| matches!(line.item, RolloutItem::SessionMeta(_)))
            .count(),
        1
    );
    assert_eq!(
        lines
            .into_iter()
            .filter_map(|line| match line.item {
                RolloutItem::ResponseItem(item) => {
                    Some(serde_json::to_value(item.item).expect("serialize migrated response"))
                }
                _ => None,
            })
            .collect::<Vec<_>>(),
        expected_responses
    );
}

#[tokio::test]
async fn migration_compacts_subagent_prefix_and_does_not_project_it() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::SubAgent(SubAgentSource::Other("test".to_string())),
        bounded_subagent_items(home.path()),
    );
    writeln!(
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open legacy subagent rollout"),
        "{{not valid rollout json"
    )
    .expect("append malformed record");
    let store = indexed_store(home.path()).await;

    let dry_run = store
        .migrate_rollouts(RolloutMigrationOptions::default())
        .await
        .expect("dry-run bounded subagent migration");
    let manifest = dry_run.outcomes[0]
        .manifest
        .as_ref()
        .expect("bounded subagent manifest")
        .clone();
    assert!(!manifest.sources_retained_after_apply);
    assert_no_migration_artifacts(home.path(), path.as_path(), thread_id).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate legacy subagent");

    let lines = read_rollout(&path);
    let RolloutItem::SessionMeta(metadata) = &lines[0].item else {
        panic!("migrated rollout should start with session metadata");
    };
    assert_eq!(
        metadata.meta.subagent_history_start_ordinal,
        Some(lines.len() as u64)
    );
    assert!(
        !fs::read_to_string(&path)
            .expect("read migrated rollout")
            .contains("superseded checkpoint")
    );
    assert_manifest_target_matches_published_rollout(&manifest.targets[0], path.as_path());
    let context = store
        .load_latest_model_context(LoadThreadHistoryParams {
            thread_id,
            include_archived: false,
        })
        .await
        .expect("load migrated model context");
    assert!(context.items.iter().any(|item| {
        matches!(item, RolloutItem::Compacted(compacted) if compacted.message == "latest checkpoint")
    }));
    assert!(
        list_active_summary_turns(&store, thread_id)
            .await
            .turns
            .is_empty()
    );
}

#[tokio::test]
async fn migration_projects_small_uncompacted_subagent_replay() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::SubAgent(SubAgentSource::Other("test".to_string())),
        vec![
            turn_started("child-turn-1"),
            user_message("child question 1"),
            agent_message("child answer 1"),
            turn_complete("child-turn-1"),
            turn_started("child-turn-2"),
            user_message("child question 2"),
            agent_message("child answer 2"),
            turn_complete("child-turn-2"),
        ],
    );
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate uncompacted legacy subagent");

    let lines = read_rollout(&path);
    let RolloutItem::SessionMeta(metadata) = &lines[0].item else {
        panic!("migrated rollout should start with session metadata");
    };
    assert_eq!(metadata.meta.subagent_history_start_ordinal, None);
    assert_eq!(
        lines
            .iter()
            .filter(|line| matches!(line.item, RolloutItem::EventMsg(EventMsg::ItemCompleted(_))))
            .count(),
        4
    );
    let turns = list_active_summary_turns(&store, thread_id).await;
    assert_eq!(turns.turns.len(), 2);
    assert_eq!(turns.turns[0].turn_id, "child-turn-1");
    assert_eq!(turns.turns[0].items.len(), 2);
    assert_eq!(turns.turns[1].turn_id, "child-turn-2");
    assert_eq!(turns.turns[1].items.len(), 2);
}

#[tokio::test]
async fn migration_projects_memory_consolidation_as_ordinary_history() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::SubAgent(SubAgentSource::MemoryConsolidation),
        vec![
            user_message("memory question"),
            agent_message("memory answer"),
        ],
    );
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate memory consolidation rollout");

    let lines = read_rollout(&path);
    assert!(matches!(
        &lines[0].item,
        RolloutItem::SessionMeta(metadata)
            if metadata.meta.history_mode == ThreadHistoryMode::Paginated
                && metadata.meta.subagent_history_start_ordinal.is_none()
    ));
    let turns = list_active_summary_turns(&store, thread_id).await;
    assert_eq!(turns.turns.len(), 1);
    assert_eq!(turns.turns[0].items.len(), 2);
}

#[tokio::test]
async fn dry_run_reports_migration_order() {
    let home = TempDir::new().expect("create Codex home");
    let root_id = ThreadId::new();
    let root = write_rollout(
        home.path(),
        root_id,
        SessionSource::Cli,
        vec![user_message("root question")],
    );
    let newest_directory = home.path().join("sessions/2025/01/04");
    fs::create_dir_all(&newest_directory).expect("create newest rollout directory");
    let newest_root = newest_directory.join(format!("rollout-2025-01-04T12-00-00-{root_id}.jsonl"));
    fs::rename(&root, &newest_root).expect("move root rollout to newest date");
    let root = newest_root;
    let subagent_id = ThreadId::new();
    let subagent = write_rollout(
        home.path(),
        subagent_id,
        SessionSource::SubAgent(SubAgentSource::Other("test".to_string())),
        vec![user_message("subagent question")],
    );
    let memory_id = ThreadId::new();
    let memory = write_rollout(
        home.path(),
        memory_id,
        SessionSource::SubAgent(SubAgentSource::MemoryConsolidation),
        vec![user_message("memory question")],
    );
    let memory_directory = home.path().join("sessions/2025/01/01");
    fs::create_dir_all(&memory_directory).expect("create memory rollout directory");
    let moved_memory = memory_directory.join(memory.file_name().expect("memory filename"));
    fs::rename(memory, &moved_memory).expect("move memory rollout");
    let compressed_id = ThreadId::new();
    let oldest_directory = home.path().join("sessions/2025/01/02");
    fs::create_dir_all(&oldest_directory).expect("create oldest rollout directory");
    let source_compressed_plain = write_rollout(
        home.path(),
        compressed_id,
        SessionSource::Cli,
        vec![user_message("compressed question")],
    );
    let compressed_plain =
        oldest_directory.join(format!("rollout-2025-01-02T12-00-00-{compressed_id}.jsonl"));
    fs::rename(source_compressed_plain, &compressed_plain).expect("move compressed rollout");
    let compressed = compress_rollout(&compressed_plain);
    let archived_id = ThreadId::new();
    let archived = move_to_archived(
        home.path(),
        write_rollout(
            home.path(),
            archived_id,
            SessionSource::Cli,
            vec![user_message("archived question")],
        ),
    );
    let original = fs::read(&root).expect("read original root rollout");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let mut progress = Vec::new();
    let report = store
        .migrate_rollouts_with_progress(RolloutMigrationOptions::default(), |update| {
            progress.push(update);
        })
        .await
        .expect("inspect legacy rollouts");

    let expected = vec![
        (root.clone(), root_id, RolloutMigrationStatus::Eligible),
        (subagent, subagent_id, RolloutMigrationStatus::Eligible),
        (compressed, compressed_id, RolloutMigrationStatus::Eligible),
        (moved_memory, memory_id, RolloutMigrationStatus::Eligible),
        (archived, archived_id, RolloutMigrationStatus::Eligible),
    ];
    assert_eq!(
        report
            .outcomes
            .iter()
            .map(|outcome| (
                outcome.rollout_path.clone(),
                outcome.thread_id.expect("rollout thread ID"),
                outcome.status,
            ))
            .collect::<Vec<_>>(),
        expected,
    );
    assert_eq!(
        progress.last(),
        Some(&RolloutMigrationProgress {
            processed_paths: 5,
            total_paths: 5,
            outcome_status: Some(RolloutMigrationStatus::Eligible),
        })
    );
    assert_eq!(fs::read(&root).expect("read inspected rollout"), original);

    let selected = store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![root_id],
            ..RolloutMigrationOptions::default()
        })
        .await
        .expect("inspect selected rollout");
    assert_eq!(selected.outcomes.len(), 1);
    assert_eq!(selected.outcomes[0].thread_id, Some(root_id));
    assert_eq!(
        selected.outcomes[0].status,
        RolloutMigrationStatus::Eligible
    );
}

#[tokio::test]
async fn migration_preserves_compressed_rollouts_during_publish_and_recovery() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            user_message("compressed question"),
            agent_message("compressed answer"),
        ],
    );
    let compressed_path = compress_rollout(&path);
    let store = indexed_store(home.path()).await;

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate compressed rollout");

    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Migrated);
    assert!(!path.exists());
    assert!(compressed_path.exists());
    assert_eq!(
        codex_rollout::read_session_meta_line(&compressed_path)
            .await
            .expect("read compressed metadata")
            .meta
            .history_mode,
        ThreadHistoryMode::Paginated
    );
    let turns = list_active_summary_turns(&store, thread_id).await;
    assert_eq!(turns.turns.len(), 1);
    assert_eq!(turns.turns[0].items.len(), 2);

    thread_history::delete_thread(&store, thread_id)
        .await
        .expect("simulate missing projection");
    let journal_path = migration_journal_path(home.path(), thread_id);
    write_migration_journal(&journal_path)
        .await
        .expect("simulate pending migration journal");
    let recovered = store
        .migrate_rollouts(apply_options())
        .await
        .expect("recover compressed rollout");

    assert_eq!(
        recovered.outcomes[0].status,
        RolloutMigrationStatus::Migrated
    );
    assert!(recovered.outcomes[0].bytes_processed > 0);
    assert!(compressed_path.exists());
    assert!(!path.exists());
    assert!(!journal_path.exists());
}

#[tokio::test]
async fn migration_migrates_archived_rollouts_without_unarchiving_them() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let active_path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            user_message("archived question"),
            agent_message("archived answer"),
        ],
    );
    let archived_path = move_to_archived(home.path(), active_path.clone());
    let store = indexed_store(home.path()).await;

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate archived rollout");

    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Migrated);
    assert!(!active_path.exists());
    assert!(archived_path.exists());
    assert!(matches!(
        &read_rollout(&archived_path)[0].item,
        RolloutItem::SessionMeta(metadata)
            if metadata.meta.history_mode == ThreadHistoryMode::Paginated
    ));
    let turns = store
        .list_turns(ListTurnsParams {
            thread_id,
            include_archived: true,
            cursor: None,
            page_size: 10,
            sort_direction: SortDirection::Asc,
            items_view: StoredTurnItemsView::Summary,
        })
        .await
        .expect("read archived projected turns");
    assert_eq!(turns.turns.len(), 1);
    assert_eq!(turns.turns[0].items.len(), 2);
}

#[tokio::test]
async fn migration_retries_a_rollout_moved_after_path_discovery() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let active_path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![user_message("question"), agent_message("answer")],
    );
    let store = indexed_store(home.path()).await;
    let archived_path = move_to_archived(home.path(), active_path.clone());

    let report = store
        .migrate_rollouts_with_progress_for_trigger(
            apply_options(),
            |_| {},
            RolloutMigrationTrigger::Startup,
            RolloutMigrationPaths::Known(vec![active_path]),
        )
        .await
        .expect("migrate moved rollout");

    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Migrated);
    assert!(matches!(
        &read_rollout(&archived_path)[0].item,
        RolloutItem::SessionMeta(metadata)
            if metadata.meta.history_mode == ThreadHistoryMode::Paginated
    ));
}

#[tokio::test]
async fn migration_preserves_legacy_displayed_thread_names() {
    let home = TempDir::new().expect("create Codex home");
    let title_thread_id = ThreadId::new();
    write_rollout(
        home.path(),
        title_thread_id,
        SessionSource::Cli,
        vec![user_message("title question")],
    );
    let index_thread_id = ThreadId::new();
    write_rollout(
        home.path(),
        index_thread_id,
        SessionSource::Cli,
        vec![user_message("index question")],
    );
    let store = indexed_store(home.path()).await;
    store
        .update_thread_metadata(UpdateThreadMetadataParams {
            thread_id: title_thread_id,
            patch: ThreadMetadataPatch {
                name: Some(Some("renamed title".to_string())),
                ..Default::default()
            },
            include_archived: false,
        })
        .await
        .expect("rename legacy thread");
    codex_rollout::append_thread_name(home.path(), title_thread_id, "stale index title")
        .await
        .expect("write stale legacy index name");
    codex_rollout::append_thread_name(home.path(), index_thread_id, "indexed title")
        .await
        .expect("write legacy index name");

    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate named rollouts");

    let page = store
        .list_threads(ListThreadsParams {
            page_size: 10,
            cursor: None,
            sort_key: ThreadSortKey::CreatedAt,
            sort_direction: SortDirection::Desc,
            allowed_sources: Vec::new(),
            model_providers: None,
            cwd_filters: None,
            section: None,
            project_id: None,
            archived: false,
            search_term: None,
            relation_filter: None,
            use_state_db_only: true,
        })
        .await
        .expect("list migrated threads");
    let title_thread = page
        .items
        .iter()
        .find(|thread| thread.thread_id == title_thread_id)
        .expect("renamed title thread");
    let index_thread = page
        .items
        .iter()
        .find(|thread| thread.thread_id == index_thread_id)
        .expect("indexed title thread");

    assert_eq!(title_thread.name.as_deref(), Some("renamed title"));
    assert_eq!(index_thread.name.as_deref(), Some("indexed title"));
}

#[tokio::test]
async fn migration_repairs_a_missing_paginated_name_when_rerun() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![user_message("question")],
    );
    let store = indexed_store(home.path()).await;
    store
        .update_thread_metadata(UpdateThreadMetadataParams {
            thread_id,
            patch: ThreadMetadataPatch {
                name: Some(Some("renamed title".to_string())),
                ..Default::default()
            },
            include_archived: false,
        })
        .await
        .expect("rename legacy thread");
    store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate named rollout");
    let state_db = store.state_db().await.expect("state runtime");
    state_db
        .update_thread_title(thread_id, "question")
        .await
        .expect("restore derived title");
    state_db
        .update_thread_name(thread_id, /*name*/ None)
        .await
        .expect("clear migrated name");

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("repair migrated name");

    assert_eq!(
        report.outcomes[0].status,
        RolloutMigrationStatus::AlreadyPaginated
    );
    assert_eq!(
        state_db
            .get_thread(thread_id)
            .await
            .expect("read repaired metadata")
            .expect("repaired thread")
            .name
            .as_deref(),
        Some("renamed title")
    );
}

#[cfg(unix)]
#[tokio::test]
async fn decompression_temporaries_are_owner_only() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let compressed_path = compress_rollout(&write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![user_message("compressed question")],
    ));
    let plain_path = home.path().join("decompressed.tmp");

    decompress_rollout_to_path(&compressed_path, &plain_path)
        .await
        .expect("decompress rollout");

    assert_eq!(
        fs::metadata(&plain_path)
            .expect("read temporary metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}

#[tokio::test]
async fn migration_skips_threads_with_an_active_writer() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![user_message("active question")],
    );
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let _writer = store
        .writer_lock_coordinator
        .acquire(thread_id)
        .expect("acquire live writer lock");
    let original = fs::read(&path).expect("read active rollout");

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("inspect active writer");

    assert_eq!(
        report.outcomes[0].status,
        RolloutMigrationStatus::SkippedBusy
    );
    assert_eq!(fs::read(&path).expect("read unmodified rollout"), original);
}

#[tokio::test]
async fn migration_apply_conflicts_with_rollout_maintenance() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![user_message("maintenance question")],
    );
    let original = fs::read(&path).expect("read legacy rollout");
    let _maintenance_guard = codex_rollout::try_acquire_rollout_maintenance_lock(home.path())
        .expect("acquire rollout maintenance lock")
        .expect("claim rollout maintenance lock");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let error = store
        .migrate_rollouts(apply_options())
        .await
        .expect_err("reject concurrent rollout maintenance");

    assert!(matches!(error, ThreadStoreError::Conflict { .. }));
    assert_eq!(fs::read(&path).expect("read untouched rollout"), original);
}

#[tokio::test]
async fn migration_recovers_a_published_rollout_with_missing_projection() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            user_message("recover question"),
            agent_message("recover answer"),
        ],
    );
    let store = indexed_store(home.path()).await;
    store
        .migrate_rollouts(apply_options())
        .await
        .expect("publish canonical rollout");
    thread_history::delete_thread(&store, thread_id)
        .await
        .expect("simulate interrupted projection");
    let journal_path = migration_journal_path(home.path(), thread_id);
    write_migration_journal(&journal_path)
        .await
        .expect("simulate pending migration journal");

    let writer = store
        .writer_lock_coordinator
        .acquire(thread_id)
        .expect("acquire live writer lock");
    let busy = store
        .migrate_rollouts(apply_options())
        .await
        .expect("inspect busy published recovery");
    assert_eq!(busy.outcomes[0].status, RolloutMigrationStatus::SkippedBusy);
    assert!(journal_path.exists());
    drop(writer);

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("recover published rollout");

    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Migrated);
    assert!(!journal_path.exists());
    let projection = thread_history::projection_state(&store, thread_id)
        .await
        .expect("read repaired projection")
        .expect("projection was rebuilt");
    assert_eq!(
        projection.next_byte_offset,
        fs::metadata(&path).expect("read rollout metadata").len()
    );
}

#[tokio::test]
async fn migration_recovers_pending_rollouts_before_new_work() {
    let home = TempDir::new().expect("create Codex home");
    let pending_thread_id = ThreadId::new();
    let pending_path = write_rollout(
        home.path(),
        pending_thread_id,
        SessionSource::Cli,
        vec![user_message("pending question")],
    );
    let pending_rollout_id = ThreadId::new();
    let pending_path = pending_path.with_file_name(format!(
        "rollout-2025-01-03T12-00-00-{pending_thread_id}_{pending_rollout_id}.jsonl"
    ));
    fs::rename(
        home.path().join(format!(
            "sessions/2025/01/03/rollout-2025-01-03T12-00-00-{pending_thread_id}.jsonl"
        )),
        &pending_path,
    )
    .expect("rename pending physical rollout");
    let new_thread_id = ThreadId::new();
    let new_path = write_rollout(
        home.path(),
        new_thread_id,
        SessionSource::Cli,
        vec![user_message("new question")],
    );
    let newer_directory = home.path().join("sessions/2025/01/04");
    fs::create_dir_all(&newer_directory).expect("create newer rollout directory");
    fs::rename(
        &new_path,
        newer_directory.join(new_path.file_name().expect("rollout filename")),
    )
    .expect("move newer rollout");
    let store = indexed_store(home.path()).await;

    store
        .migrate_rollouts(RolloutMigrationOptions {
            thread_ids: vec![pending_thread_id],
            ..apply_options()
        })
        .await
        .expect("publish pending rollout");
    thread_history::delete_thread(&store, pending_rollout_id)
        .await
        .expect("simulate missing projection");
    write_migration_journal(&migration_journal_path(home.path(), pending_thread_id))
        .await
        .expect("simulate pending migration journal");

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("recover pending rollout before new work");

    assert_eq!(
        report
            .outcomes
            .iter()
            .map(|outcome| (
                outcome.thread_id.expect("rollout thread ID"),
                outcome.status
            ))
            .collect::<Vec<_>>(),
        vec![
            (pending_thread_id, RolloutMigrationStatus::Migrated),
            (new_thread_id, RolloutMigrationStatus::Migrated),
        ]
    );
}

#[tokio::test]
async fn migration_recovers_a_compressed_published_rollout() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![
            user_message("recover compressed question"),
            agent_message("recover compressed answer"),
        ],
    );
    let store = indexed_store(home.path()).await;
    store
        .migrate_rollouts(apply_options())
        .await
        .expect("publish canonical rollout");
    thread_history::delete_thread(&store, thread_id)
        .await
        .expect("simulate interrupted projection");
    let state_db = store.state_db.as_ref().expect("state db");
    let mut legacy_metadata = state_db
        .get_thread(thread_id)
        .await
        .expect("read thread metadata")
        .expect("thread metadata");
    legacy_metadata.history_mode = ThreadHistoryMode::Legacy;
    state_db
        .delete_thread(thread_id)
        .await
        .expect("remove paginated thread metadata");
    assert!(
        state_db
            .insert_thread_if_absent(&legacy_metadata)
            .await
            .expect("restore legacy thread metadata")
    );
    let journal_path = migration_journal_path(home.path(), thread_id);
    write_migration_journal(&journal_path)
        .await
        .expect("simulate pending migration journal");
    let compressed_path = compress_rollout(&path);

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("recover compressed published rollout");

    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Migrated);
    assert!(!path.exists());
    assert!(compressed_path.exists());
    assert!(!journal_path.exists());
    assert_eq!(
        store
            .state_db
            .as_ref()
            .expect("state db")
            .get_thread(thread_id)
            .await
            .expect("read repaired thread metadata")
            .expect("thread metadata")
            .history_mode,
        ThreadHistoryMode::Paginated
    );
    let projection = thread_history::projection_state(&store, thread_id)
        .await
        .expect("read repaired projection")
        .expect("projection was rebuilt");
    assert_eq!(
        projection.next_byte_offset,
        zstd::stream::decode_all(fs::File::open(&compressed_path).expect("open rollout"))
            .expect("decompress rollout")
            .len() as u64
    );
}

#[tokio::test]
async fn migration_skips_oversized_jsonl_records() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![user_message("kept question")],
    );
    let oversized_output = "x".repeat(super::MAX_ROLLOUT_LINE_BYTES);
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open legacy rollout");
    writeln!(
        file,
        "{{\"timestamp\":\"{TIMESTAMP}\",\"type\":\"response_item\",\"payload\":{{\"type\":\"function_call_output\",\"call_id\":\"call-1\",\"output\":\"{oversized_output}\"}}}}"
    )
    .expect("write oversized rollout record");
    drop(file);
    let store = indexed_store(home.path()).await;

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate oversized rollout record");

    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Migrated);
    assert!(
        fs::metadata(&path)
            .expect("read migrated rollout metadata")
            .len()
            < super::MAX_ROLLOUT_LINE_BYTES as u64
    );
    let turns = list_active_summary_turns(&store, thread_id).await;
    assert_eq!(turns.turns.len(), 1);
    assert_eq!(turns.turns[0].items.len(), 1);
}

#[tokio::test]
async fn migration_skips_empty_rollout_files() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let directory = home.path().join("sessions/2025/01/03");
    fs::create_dir_all(&directory).expect("create rollout directory");
    let path = directory.join(format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl"));
    fs::File::create(&path).expect("create empty rollout");
    let store = indexed_store(home.path()).await;

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("inspect empty rollout");

    assert_eq!(
        report.outcomes[0].status,
        RolloutMigrationStatus::SkippedEmpty
    );
    assert_eq!(fs::metadata(&path).expect("read empty rollout").len(), 0);
    assert!(
        store
            .state_db
            .as_ref()
            .expect("state db")
            .get_thread(thread_id)
            .await
            .expect("read thread metadata")
            .is_none()
    );
}

#[tokio::test]
async fn migration_reports_missing_sqlite_metadata() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![user_message("question")],
    );
    let store = indexed_store(home.path()).await;
    store
        .state_db
        .as_ref()
        .expect("state db")
        .delete_thread(thread_id)
        .await
        .expect("remove thread metadata");

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("inspect rollout with missing metadata");

    assert_failed_with_reason(
        &report.outcomes[0],
        RolloutMigrationFailureReason::MissingSqliteMetadata,
    );
}

#[tokio::test]
async fn migration_reports_invalid_session_metadata() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let directory = home.path().join("sessions/2025/01/03");
    fs::create_dir_all(&directory).expect("create rollout directory");
    let path = directory.join(format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl"));
    fs::write(path, "not a rollout record\n").expect("write malformed rollout");
    let store = indexed_store(home.path()).await;

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("inspect rollout with invalid metadata");

    assert_failed_with_reason(
        &report.outcomes[0],
        RolloutMigrationFailureReason::InvalidSessionMetadata,
    );
}

#[tokio::test]
async fn migration_skips_malformed_lines_and_trailing_partial_tail() {
    let home = TempDir::new().expect("create Codex home");
    let thread_id = ThreadId::new();
    let path = write_rollout(
        home.path(),
        thread_id,
        SessionSource::Cli,
        vec![user_message("kept question")],
    );
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open legacy rollout");
    writeln!(file, "{{not valid rollout json").expect("append malformed record");
    let later_valid_line = RolloutLine {
        timestamp: TIMESTAMP.to_string(),
        ordinal: None,
        item: agent_message("kept answer"),
    };
    writeln!(
        file,
        "{}",
        serde_json::to_string(&later_valid_line).expect("serialize later valid record")
    )
    .expect("append later valid record");
    file.write_all(br#"{"timestamp":"unterminated""#)
        .expect("append partial tail");
    drop(file);
    let store = indexed_store(home.path()).await;

    let report = store
        .migrate_rollouts(apply_options())
        .await
        .expect("migrate malformed legacy rollout");

    assert_eq!(report.outcomes[0].status, RolloutMigrationStatus::Migrated);
    assert!(!migration_journal_path(home.path(), thread_id).exists());
    let turns = list_active_summary_turns(&store, thread_id).await;
    assert_eq!(turns.turns.len(), 1);
    assert_eq!(turns.turns[0].items.len(), 2);
}
