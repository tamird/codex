use std::fs;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::ThreadId;
use codex_protocol::items::TurnItem;
use codex_protocol::items::UserMessageItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::TurnStartedEvent;
use codex_protocol::user_input::UserInput;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;

use super::super::projection_rebuild::inject_projection_rebuild_pause;
use super::super::test_support::test_config;
use super::super::thread_history;
use super::super::thread_rollout_resolver;
use super::LocalThreadStore;
use super::correct_reused_ordinals;
use super::prepare;
use crate::ThreadStoreError;

const TIMESTAMP: &str = "2025-01-03T12:00:00Z";

fn token_count(ordinal: u64) -> Vec<u8> {
    format!(
        "{{\"timestamp\":\"{TIMESTAMP}\",\"ordinal\":{ordinal},\"type\":\"event_msg\",\"payload\":{{\"type\":\"token_count\",\"info\":null,\"rate_limits\":{{\"primary\":{{\"used_percent\":12.50,\"window_minutes\":300,\"resets_at\":1786689000}}}},\"future_field\":1.2300}}}}\n"
    ).into_bytes()
}

fn fixture_bytes(home: &Path, thread_id: ThreadId) -> (SessionMetaLine, Vec<u8>) {
    let metadata = SessionMetaLine {
        meta: SessionMeta {
            session_id: thread_id.into(),
            id: thread_id,
            timestamp: TIMESTAMP.to_string(),
            cwd: home.to_path_buf(),
            originator: "ordinal-recovery-test".to_string(),
            cli_version: "0.0.0".to_string(),
            source: SessionSource::Cli,
            model_provider: Some("test-provider".to_string()),
            history_mode: ThreadHistoryMode::Paginated,
            ..SessionMeta::default()
        },
        git: None,
    };
    let items = [
        RolloutItem::SessionMeta(metadata.clone()),
        RolloutItem::EventMsg(EventMsg::TurnStarted(TurnStartedEvent {
            turn_id: "recovery-turn".to_string(),
            trace_id: None,
            started_at: None,
            model_context_window: None,
            collaboration_mode_kind: Default::default(),
        })),
        RolloutItem::EventMsg(EventMsg::ItemCompleted(ItemCompletedEvent {
            thread_id,
            turn_id: "recovery-turn".to_string(),
            item: TurnItem::UserMessage(UserMessageItem::new(&[UserInput::Text {
                text: "preserved after duplicate".to_string(),
                text_elements: Vec::new(),
            }])),
            started_at_ms: None,
            completed_at_ms: 0,
        })),
        RolloutItem::EventMsg(EventMsg::TurnComplete(TurnCompleteEvent {
            turn_id: "recovery-turn".to_string(),
            last_agent_message: None,
            error: None,
            started_at: None,
            completed_at: None,
            duration_ms: None,
            time_to_first_token_ms: None,
        })),
    ];
    let mut source = Vec::new();
    for (ordinal, item) in items.into_iter().enumerate() {
        if ordinal == 2 {
            source.extend(token_count(/*ordinal*/ 2));
        }
        serde_json::to_writer(
            &mut source,
            &RolloutLine {
                timestamp: TIMESTAMP.to_string(),
                ordinal: Some(ordinal as u64),
                item,
            },
        )
        .expect("encode fixture record");
        source.push(b'\n');
    }
    (metadata, source)
}

async fn fixture(home: &Path) -> (LocalThreadStore, ThreadId, PathBuf, Vec<u8>) {
    let thread_id = ThreadId::new();
    let (_, bytes) = fixture_bytes(home, thread_id);
    let path = home
        .join("sessions/2025/01/03")
        .join(format!("rollout-2025-01-03T12-00-00-{thread_id}.jsonl"));
    fs::create_dir_all(path.parent().expect("rollout parent")).expect("create parent");
    fs::write(&path, &bytes).expect("write corrupt fixture");
    let config = test_config(home);
    let state = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("initialize SQLite");
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        path.clone(),
        chrono::Utc::now(),
        SessionSource::Cli,
    );
    metadata.history_mode = ThreadHistoryMode::Paginated;
    metadata.cwd = home.to_path_buf();
    state
        .upsert_thread(&metadata.build(&config.default_model_provider_id))
        .await
        .expect("select fixture");
    (
        LocalThreadStore::new(config, Some(state)),
        thread_id,
        path,
        bytes,
    )
}

#[test]
fn ordinal_recovery_preserves_raw_payloads_and_rejects_unrelated_damage() {
    let thread_id = ThreadId::new();
    let (metadata, source) = fixture_bytes(Path::new("/tmp"), thread_id);
    let corrected =
        correct_reused_ordinals(&source, &metadata).expect("recognize token-count duplicate");
    let original_records = source
        .split_inclusive(|byte| *byte == b'\n')
        .collect::<Vec<_>>();
    let corrected_records = corrected
        .split_inclusive(|byte| *byte == b'\n')
        .collect::<Vec<_>>();
    for (index, (before, after)) in original_records.iter().zip(&corrected_records).enumerate() {
        let mut expected: serde_json::Value = serde_json::from_slice(before).expect("source JSON");
        expected["ordinal"] = json!(index);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(after).expect("corrected JSON"),
            expected
        );
        if index < 3 {
            assert_eq!(
                before, after,
                "records before duplicate remain byte-identical"
            );
        }
    }
    assert_eq!(correct_reused_ordinals(&corrected, &metadata), None);
    let mut with_suffix = source.clone();
    with_suffix.extend(token_count(/*ordinal*/ 4));
    let corrected_suffix = correct_reused_ordinals(&with_suffix, &metadata).expect("repair suffix");
    assert!(
        corrected_suffix.ends_with(&token_count(/*ordinal*/ 5)),
        "only the suffix ordinal token may change"
    );
    for ordinal in [0, 4, 100] {
        let mut damaged: Vec<serde_json::Value> = source
            .split(|byte| *byte == b'\n')
            .filter(|record| !record.is_empty())
            .map(|record| serde_json::from_slice(record).expect("fixture JSON"))
            .collect();
        damaged[3]["ordinal"] = json!(ordinal);
        let bytes = damaged
            .iter()
            .flat_map(|record| {
                let mut bytes = serde_json::to_vec(record).expect("damaged JSON");
                bytes.push(b'\n');
                bytes
            })
            .collect::<Vec<_>>();
        assert_eq!(correct_reused_ordinals(&bytes, &metadata), None);
    }
    assert_eq!(
        correct_reused_ordinals(&source[..source.len() - 1], &metadata),
        None
    );
    let mut wrong_boundary = metadata;
    wrong_boundary.meta.history_base = Some(HistoryPosition {
        thread_id: ThreadId::new(),
        end_ordinal_exclusive: 269,
        end_byte_offset: 1,
    });
    assert_eq!(correct_reused_ordinals(&source, &wrong_boundary), None);
}

#[tokio::test]
async fn ordinal_recovery_reuses_staged_copy_after_interruption_and_restart() {
    let home = TempDir::new().expect("temporary home");
    let (store, thread_id, source_path, source_bytes) = fixture(home.path()).await;
    let selected = thread_rollout_resolver::resolve_current(&store, thread_id)
        .await
        .expect("resolve")
        .expect("selected");
    let metadata = codex_rollout::read_session_meta_line(&source_path)
        .await
        .expect("metadata");
    let interrupted = prepare(&store, &selected, &metadata)
        .await
        .expect("prepare")
        .expect("recovery");
    let corrected_path = interrupted.rollout_path.clone();
    let corrected_bytes = fs::read(&corrected_path).expect("staged correction");
    drop(interrupted);
    assert_eq!(
        thread_rollout_resolver::resolve_current(&store, thread_id)
            .await
            .expect("resolve")
            .expect("selected")
            .path,
        source_path
    );
    assert!(
        store
            .rebuild_history_projection(thread_id)
            .await
            .expect("recover projection")
    );
    let selected = thread_rollout_resolver::resolve_current(&store, thread_id)
        .await
        .expect("resolve")
        .expect("selected");
    assert_eq!(selected.path, corrected_path);
    assert!(
        store
            .has_history_projection(thread_id)
            .await
            .expect("complete projection")
    );
    let projection = thread_history::projection_state(&store, selected.rollout_id)
        .await
        .expect("projection")
        .expect("checkpoint");
    assert_eq!(
        (
            projection.next_byte_offset,
            projection.next_ordinal,
            projection.lineage_complete
        ),
        (corrected_bytes.len() as u64, 5, true)
    );
    let restarted = LocalThreadStore::new(store.config.clone(), store.state_db.clone());
    assert!(
        restarted
            .rebuild_history_projection(thread_id)
            .await
            .expect("restart")
    );
    assert_eq!(fs::read(&source_path).expect("original"), source_bytes);
    assert_eq!(
        fs::read(&corrected_path).expect("corrected"),
        corrected_bytes
    );
}

#[tokio::test]
async fn ordinal_recovery_respects_writer_exclusion_and_source_changes() {
    let home = TempDir::new().expect("temporary home");
    let (store, thread_id, source_path, source_bytes) = fixture(home.path()).await;
    let other = LocalThreadStore::new(store.config.clone(), store.state_db.clone());
    let writer = other
        .writer_lock_coordinator
        .acquire(thread_id)
        .expect("external writer");
    assert!(matches!(
        store.rebuild_history_projection(thread_id).await,
        Err(ThreadStoreError::Conflict { .. })
    ));
    assert_eq!(
        fs::read(&source_path).expect("unchanged source"),
        source_bytes
    );
    drop(writer);

    let pause = inject_projection_rebuild_pause(thread_id);
    let worker = store.clone();
    let rebuild = tokio::spawn(async move { worker.rebuild_history_projection(thread_id).await });
    pause.entered.notified().await;
    codex_rollout::RolloutRecorder::parse_rollout_line_bytes(&token_count(/*ordinal*/ 4))
        .expect("numeric suffix must decode before projection")
        .expect("token count suffix is retained");
    fs::OpenOptions::new()
        .append(true)
        .open(&source_path)
        .expect("old writer append")
        .write_all(&token_count(/*ordinal*/ 4))
        .expect("append valid record");
    pause.release.notify_one();
    assert!(
        rebuild
            .await
            .expect("join rebuild")
            .expect("retry changed source")
    );
    assert!(
        store
            .rebuild_history_projection(thread_id)
            .await
            .expect("retry includes append")
    );
    let selected = thread_rollout_resolver::resolve_current(&store, thread_id)
        .await
        .expect("resolve")
        .expect("selected");
    assert_ne!(selected.path, source_path);
    let mut appended_source = source_bytes;
    appended_source.extend(token_count(/*ordinal*/ 4));
    assert_eq!(
        fs::read(&source_path).expect("retained source and append"),
        appended_source
    );
    let metadata = codex_rollout::read_session_meta_line(&source_path)
        .await
        .expect("source metadata");
    let expected =
        correct_reused_ordinals(&appended_source, &metadata).expect("corrected appended source");
    assert_eq!(
        fs::read(&selected.path).expect("selected correction"),
        expected
    );
    let projection = thread_history::projection_state(&store, selected.rollout_id)
        .await
        .expect("projection")
        .expect("checkpoint");
    assert_eq!(
        (
            projection.next_byte_offset,
            projection.next_ordinal,
            projection.lineage_complete
        ),
        (expected.len() as u64, 6, true)
    );
}
