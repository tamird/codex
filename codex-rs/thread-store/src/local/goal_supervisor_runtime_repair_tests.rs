use std::path::Path;

use chrono::Utc;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::models::AgentMessageInputContent;
use codex_protocol::models::ContentItem;
use codex_protocol::models::InternalChatMessageMetadataPassthrough;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::DEFAULT_ROLLOUT_REFERENCE_DEPTH;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use sha2::Digest as _;
use sha2::Sha256;
use tempfile::TempDir;

use super::LocalThreadStore;
use super::goal_supervisor_runtime_repair::reject_malformed_supplied_history;
use super::goal_supervisor_runtime_repair::repair_compatibility_history_before_access;
use super::test_support::test_config;
use crate::ListTurnsParams;
use crate::SortDirection;
use crate::StoredTurnItemsView;

fn supervisor_rollout(
    thread_id: ThreadId,
    segment_id: Option<SegmentId>,
    payload: &str,
) -> Vec<u8> {
    let mut meta = SessionMeta {
        id: thread_id,
        session_id: thread_id.into(),
        ..SessionMeta::default()
    };
    meta.cli_version = "0.148.0-alpha.6+frodex.0".to_string();
    meta.model_provider = Some("openai".to_string());
    meta.history_mode = ThreadHistoryMode::Paginated;
    meta.segment_id = segment_id;
    let lines = [
        RolloutLine {
            timestamp: "2026-08-13T00:00:00Z".to_string(),
            ordinal: Some(0),
            item: RolloutItem::SessionMeta(SessionMetaLine { meta, git: None }),
        },
        RolloutLine {
            timestamp: "2026-08-13T00:00:01Z".to_string(),
            ordinal: Some(1),
            item: RolloutItem::InterAgentCommunicationMetadata { trigger_turn: true },
        },
        RolloutLine {
            timestamp: "2026-08-13T00:00:02Z".to_string(),
            ordinal: Some(2),
            item: RolloutItem::ResponseItem(
                ResponseItem::AgentMessage {
                    id: Some(codex_protocol::ResponseItemId::from_server(
                        "amsg_01900000-0000-7000-8000-000000000002".to_string(),
                    )),
                    author: "/root/goal_supervisor".to_string(),
                    recipient: "/root".to_string(),
                    content: vec![
                        AgentMessageInputContent::InputText {
                            text: "Message Type: NEW_TASK\nTask name: /root\nSender: /root/goal_supervisor\nPayload:\n"
                                .to_string(),
                        },
                        AgentMessageInputContent::EncryptedContent {
                            encrypted_content: payload.to_string(),
                        },
                    ],
                    internal_chat_message_metadata_passthrough: Some(
                        InternalChatMessageMetadataPassthrough {
                            turn_id: Some(
                                "01900000-0000-7000-8000-000000000003".to_string(),
                            ),
                            ..Default::default()
                        },
                    ),
                }
                .into(),
            ),
        },
    ];
    let mut encoded = Vec::new();
    for line in lines {
        encoded.extend(serde_json::to_vec(&line).expect("serialize rollout line"));
        encoded.push(b'\n');
    }
    encoded
}

fn decode_rollout(bytes: &[u8]) -> Vec<RolloutLine> {
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("parse rollout line"))
        .collect()
}

fn encode_rollout(lines: &[RolloutLine]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for line in lines {
        encoded.extend(serde_json::to_vec(line).expect("serialize rollout line"));
        encoded.push(b'\n');
    }
    encoded
}

fn user_line(ordinal: u64, text: &str) -> RolloutLine {
    RolloutLine {
        timestamp: format!("2026-08-13T00:00:{ordinal:02}Z"),
        ordinal: Some(ordinal),
        item: RolloutItem::ResponseItem(
            ResponseItem::Message {
                id: None,
                role: "user".to_string(),
                content: vec![ContentItem::InputText {
                    text: text.to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            }
            .into(),
        ),
    }
}

fn set_line_ordinal(line: &mut RolloutLine, ordinal: u64) {
    line.ordinal = Some(ordinal);
    line.timestamp = format!("2026-08-13T00:00:{ordinal:02}Z");
}

fn set_agent_message_id(line: &mut RolloutLine, id: &str) {
    let RolloutItem::ResponseItem(item) = &mut line.item else {
        panic!("expected response item");
    };
    let ResponseItem::AgentMessage { id: message_id, .. } = &mut **item else {
        panic!("expected agent message");
    };
    *message_id = Some(codex_protocol::ResponseItemId::from_server(id.to_string()));
}

fn referenced_poison_rollout(
    thread_id: ThreadId,
    segment_id: SegmentId,
) -> (Vec<u8>, String, String) {
    let base = decode_rollout(
        supervisor_rollout(
            thread_id,
            Some(segment_id),
            "first synthetic poisoned supervisor instruction",
        )
        .as_slice(),
    );
    let first_id = "amsg_01900000-0000-7000-8000-000000000012".to_string();
    let second_id = "amsg_01900000-0000-7000-8000-000000000013".to_string();
    let mut first_delivery = base[1].clone();
    let mut first_message = base[2].clone();
    set_line_ordinal(&mut first_delivery, /*ordinal*/ 2);
    set_line_ordinal(&mut first_message, /*ordinal*/ 3);
    set_agent_message_id(&mut first_message, first_id.as_str());
    let mut second_delivery = base[1].clone();
    let mut second_message = base[2].clone();
    set_line_ordinal(&mut second_delivery, /*ordinal*/ 5);
    set_line_ordinal(&mut second_message, /*ordinal*/ 6);
    set_agent_message_id(&mut second_message, second_id.as_str());
    if let RolloutItem::ResponseItem(item) = &mut second_message.item
        && let ResponseItem::AgentMessage { content, .. } = &mut **item
        && let AgentMessageInputContent::EncryptedContent { encrypted_content } = &mut content[1]
    {
        *encrypted_content = "second synthetic poisoned supervisor instruction".to_string();
    }
    (
        encode_rollout(&[
            base[0].clone(),
            user_line(/*ordinal*/ 1, "first user"),
            first_delivery,
            first_message,
            user_line(/*ordinal*/ 4, "second user"),
            second_delivery,
            second_message,
        ]),
        first_id,
        second_id,
    )
}

fn write_immutable_rollout(
    home: &Path,
    thread_id: ThreadId,
    segment_id: SegmentId,
    bytes: &[u8],
) -> std::path::PathBuf {
    let directory = home
        .join("rotated_rollout_segments")
        .join(thread_id.to_string())
        .join(segment_id.to_string());
    std::fs::create_dir_all(directory.as_path()).expect("create segment directory");
    let path = directory.join(format!("rollout-2026-08-13T00-00-00-{thread_id}.jsonl"));
    std::fs::write(path.as_path(), bytes).expect("write immutable rollout");
    path
}

fn active_reference_rollout(
    thread_id: ThreadId,
    segment_id: SegmentId,
    reference: RolloutReferenceItem,
) -> Vec<u8> {
    let mut meta = decode_rollout(
        supervisor_rollout(thread_id, Some(segment_id), "unused synthetic poison").as_slice(),
    )[0]
    .clone();
    let RolloutItem::SessionMeta(session_meta) = &mut meta.item else {
        panic!("expected SessionMeta");
    };
    session_meta.meta.history_mode = ThreadHistoryMode::Legacy;
    encode_rollout(&[
        meta,
        RolloutLine {
            timestamp: "2026-08-13T00:00:01Z".to_string(),
            ordinal: Some(1),
            item: RolloutItem::RolloutReference(reference),
        },
    ])
}

fn write_rollout(home: &Path, thread_id: ThreadId, bytes: &[u8]) -> std::path::PathBuf {
    let day = home.join("sessions/2026/08/13");
    std::fs::create_dir_all(day.as_path()).expect("create session directory");
    let path = day.join(format!("rollout-2026-08-13T00-00-00-{thread_id}.jsonl"));
    std::fs::write(path.as_path(), bytes).expect("write rollout");
    path
}

fn hash_if_present(path: &Path) -> Option<[u8; 32]> {
    std::fs::read(path)
        .ok()
        .map(|bytes| Sha256::digest(bytes).into())
}

fn history_base_rollout(
    thread_id: ThreadId,
    segment_id: SegmentId,
    history_base: HistoryPosition,
) -> Vec<u8> {
    let mut lines = decode_rollout(
        supervisor_rollout(thread_id, Some(segment_id), "unused synthetic poison").as_slice(),
    );
    let RolloutItem::SessionMeta(meta) = &mut lines[0].item else {
        panic!("expected SessionMeta");
    };
    meta.meta.history_base = Some(history_base);
    encode_rollout(&lines[..1])
}

fn compress(bytes: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(bytes, 0).expect("compress rollout")
}

fn decompress(bytes: &[u8]) -> Vec<u8> {
    zstd::stream::decode_all(bytes).expect("decompress rollout")
}

#[test]
fn malformed_supplied_history_is_rejected_without_a_disk_source() {
    let thread_id = ThreadId::from_u128(/*value*/ 0x7000);
    let items = decode_rollout(
        supervisor_rollout(
            thread_id,
            Some(SegmentId::new()),
            "synthetic poisoned supervisor instruction",
        )
        .as_slice(),
    )
    .into_iter()
    .map(|line| line.item)
    .collect::<Vec<_>>();

    let error = reject_malformed_supplied_history(items.as_slice())
        .expect_err("caller-supplied poison has no durable source to repair");

    assert!(
        error.to_string().contains("goal-supervisor"),
        "unexpected supplied-history error: {error}"
    );
}

#[tokio::test]
async fn segmentless_damage_fails_closed_without_artifacts() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::from_u128(/*value*/ 0x7001);
    let source = supervisor_rollout(
        thread_id,
        /*segment_id*/ None,
        "synthetic poisoned supervisor instruction",
    );
    let path = write_rollout(home.path(), thread_id, source.as_slice());
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let error = repair_compatibility_history_before_access(&store, thread_id, path.as_path())
        .await
        .expect_err("segmentless damage must fail closed");

    assert!(error.to_string().contains("segmentless rollout"));
    assert_eq!(std::fs::read(path).expect("reread rollout"), source);
    assert!(!home.path().join("rollout-history-repair-state").exists());
    assert!(!home.path().join("rollout-history-repair-backups").exists());
    assert!(!home.path().join("rotated_rollout_segments").exists());
    assert!(!home.path().join(".tmp").exists());
}

#[tokio::test]
async fn repair_preserves_at_eof_thread_history_database_bytes() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::from_u128(/*value*/ 0x7002);
    let source = supervisor_rollout(
        thread_id,
        Some(SegmentId::new()),
        "synthetic poisoned supervisor instruction",
    );
    let path = write_rollout(home.path(), thread_id, source.as_slice());
    let config = test_config(home.path());
    let state_db = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await
    .expect("initialize state database");
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        path.clone(),
        Utc::now(),
        SessionSource::Cli,
    );
    metadata.history_mode = ThreadHistoryMode::Paginated;
    state_db
        .upsert_thread(&metadata.build(config.default_model_provider_id.as_str()))
        .await
        .expect("seed thread metadata");
    let store = LocalThreadStore::new(config, Some(state_db));
    let pool = store.thread_history_db().await.expect("history database");
    sqlx::query(
        "INSERT INTO thread_history_projection_state \
         (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, ?, ?)",
    )
    .bind(thread_id.to_string())
    .bind(i64::try_from(source.len()).expect("source length"))
    .bind(3_i64)
    .execute(pool)
    .await
    .expect("insert EOF projection");
    for (turn_id, ordinal) in [("turn-1", 1_i64), ("turn-2", 2_i64)] {
        sqlx::query(
            "INSERT INTO thread_turns (thread_id, turn_id, rollout_ordinal, status) \
             VALUES (?, ?, ?, 'completed')",
        )
        .bind(thread_id.to_string())
        .bind(turn_id)
        .bind(ordinal)
        .execute(pool)
        .await
        .expect("insert projected turn");
    }
    let first_page = store
        .list_turns(ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: None,
            page_size: 1,
            sort_direction: SortDirection::Asc,
            items_view: StoredTurnItemsView::NotLoaded,
        })
        .await
        .expect("read first projected turn page");
    assert_eq!(first_page.turns.len(), 1);
    assert_eq!(first_page.turns[0].turn_id, "turn-1");
    let cursor = first_page.next_cursor.expect("next projected turn cursor");
    let before_projection = super::thread_history::projection_state(&store, thread_id)
        .await
        .expect("read projection")
        .map(|state| (state.next_byte_offset, state.next_ordinal));
    let sqlite = home.path().join("thread_history_1.sqlite");
    let wal = home.path().join("thread_history_1.sqlite-wal");
    let shm = home.path().join("thread_history_1.sqlite-shm");
    let before = [
        hash_if_present(sqlite.as_path()),
        hash_if_present(wal.as_path()),
        hash_if_present(shm.as_path()),
    ];

    repair_compatibility_history_before_access(&store, thread_id, path.as_path())
        .await
        .expect("repair rollout");

    let after = [
        hash_if_present(sqlite.as_path()),
        hash_if_present(wal.as_path()),
        hash_if_present(shm.as_path()),
    ];
    assert_eq!(after, before);
    let second_page = store
        .list_turns(ListTurnsParams {
            thread_id,
            include_archived: false,
            cursor: Some(cursor),
            page_size: 1,
            sort_direction: SortDirection::Asc,
            items_view: StoredTurnItemsView::NotLoaded,
        })
        .await
        .expect("reuse projected turn cursor after repair");
    assert_eq!(second_page.turns.len(), 1);
    assert_eq!(second_page.turns[0].turn_id, "turn-2");
    assert!(second_page.next_cursor.is_none());
    assert_eq!(
        [
            hash_if_present(sqlite.as_path()),
            hash_if_present(wal.as_path()),
            hash_if_present(shm.as_path()),
        ],
        before
    );
    assert_eq!(
        super::thread_history::projection_state(&store, thread_id)
            .await
            .expect("reread projection")
            .map(|state| (state.next_byte_offset, state.next_ordinal)),
        before_projection
    );
    let repaired = std::fs::read(path).expect("read repaired rollout");
    assert_eq!(repaired.len(), source.len());
    assert_ne!(repaired, source);
    assert!(!home.path().join("thread_history_2.sqlite").exists());
    assert!(!home.path().join("rollout-history-repair-state").exists());
    assert!(!home.path().join("rollout-history-repair-backups").exists());
}

#[tokio::test]
async fn clean_fernet_history_creates_no_repair_artifacts() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::from_u128(/*value*/ 0x7003);
    let source = supervisor_rollout(
        thread_id,
        Some(SegmentId::new()),
        "gAAAAABqfQkApRY563QcNHss2A4AKN3hJ027UTP8TRjRMwBjGzwdQ1xZ-6mLXPG8wa8TVLFB3ggULSAKlfpl7C4YbWdR4_R28-k0urBPtKk2Amtf8DdoShe_vVF4ffJ0XoIvR1ryVWmP",
    );
    let path = write_rollout(home.path(), thread_id, source.as_slice());
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    repair_compatibility_history_before_access(&store, thread_id, path.as_path())
        .await
        .expect("clean Fernet history");

    assert_eq!(std::fs::read(path).expect("reread rollout"), source);
    assert!(
        home.path().join(".tmp/rollout-maintenance.lock").exists(),
        "clean access uses the existing maintenance lock format"
    );
    assert!(!home.path().join("rotated_rollout_segments").exists());
    assert!(!home.path().join("rollout-history-repair-state").exists());
    assert!(!home.path().join("rollout-history-repair-backups").exists());
}

#[tokio::test(start_paused = true)]
async fn clean_history_waits_for_rollout_maintenance_beyond_ten_seconds() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::from_u128(/*value*/ 0x70031);
    let source = supervisor_rollout(
        thread_id,
        Some(SegmentId::new()),
        "gAAAAABqfQkApRY563QcNHss2A4AKN3hJ027UTP8TRjRMwBjGzwdQ1xZ-6mLXPG8wa8TVLFB3ggULSAKlfpl7C4YbWdR4_R28-k0urBPtKk2Amtf8DdoShe_vVF4ffJ0XoIvR1ryVWmP",
    );
    let path = write_rollout(home.path(), thread_id, source.as_slice());
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let maintenance = codex_rollout::try_acquire_rollout_maintenance_lock(home.path())
        .expect("open rollout-maintenance lock")
        .expect("acquire rollout-maintenance lock");
    let repair_store = store.clone();
    let repair = tokio::spawn(async move {
        repair_compatibility_history_before_access(&repair_store, thread_id, path.as_path()).await
    });

    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_secs(11)).await;
    tokio::task::yield_now().await;
    assert!(
        !repair.is_finished(),
        "a healthy maintenance owner must not become a terminal thread-read error"
    );

    drop(maintenance);
    tokio::time::advance(std::time::Duration::from_millis(500)).await;
    repair
        .await
        .expect("join waiting history access")
        .expect("read history after maintenance completes");
}

#[tokio::test]
async fn indeterminate_publication_stays_quarantined_until_store_restart() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::from_u128(/*value*/ 0x7004);
    let source = supervisor_rollout(
        thread_id,
        Some(SegmentId::new()),
        "synthetic poisoned supervisor instruction",
    );
    let path = write_rollout(home.path(), thread_id, source.as_slice());
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    super::segment::history_repair_publication::inject_history_repair_postcommit_sync_failure(
        path.clone(),
    );

    let first = repair_compatibility_history_before_access(&store, thread_id, path.as_path())
        .await
        .expect_err("postcommit sync failure must fail closed");
    assert!(first.to_string().contains("restart before continuing"));
    let repaired = std::fs::read(path.as_path()).expect("read visible replacement");
    assert_ne!(repaired, source);

    let second = repair_compatibility_history_before_access(&store, thread_id, path.as_path())
        .await
        .expect_err("same process must retain quarantine");
    assert!(second.to_string().contains("restart before continuing"));

    let second_store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let second_store_error =
        repair_compatibility_history_before_access(&second_store, thread_id, path.as_path())
            .await
            .expect_err("all stores in this process retain the restart quarantine");
    assert!(
        second_store_error
            .to_string()
            .contains("restart before continuing")
    );

    super::goal_supervisor_runtime_repair::clear_quarantine_for_test(home.path());
    repair_compatibility_history_before_access(&second_store, thread_id, path.as_path())
        .await
        .expect("a simulated process restart rescans the visible clean replacement");
}

#[tokio::test]
async fn noncanonical_mutable_root_fails_closed_without_artifacts() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::from_u128(/*value*/ 0x7005);
    let source = supervisor_rollout(
        thread_id,
        Some(SegmentId::new()),
        "synthetic poisoned supervisor instruction",
    );
    let custom = home.path().join("custom");
    std::fs::create_dir_all(custom.as_path()).expect("create custom directory");
    let path = custom.join(format!("rollout-{thread_id}.jsonl"));
    std::fs::write(path.as_path(), source.as_slice()).expect("write custom rollout");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let error = repair_compatibility_history_before_access(&store, thread_id, path.as_path())
        .await
        .expect_err("custom rollout root must fail closed");

    assert!(
        error.to_string().contains("canonical rollout"),
        "unexpected noncanonical-root error: {error}"
    );
    assert_eq!(std::fs::read(path).expect("reread rollout"), source);
    assert!(!home.path().join("rotated_rollout_segments").exists());
}

#[tokio::test]
async fn dirty_immutable_root_fails_closed_without_mutation() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::from_u128(/*value*/ 0x7009);
    let segment_id = SegmentId::new();
    let source = supervisor_rollout(
        thread_id,
        Some(segment_id),
        "synthetic poisoned supervisor instruction",
    );
    let path = write_immutable_rollout(home.path(), thread_id, segment_id, source.as_slice());
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let error = repair_compatibility_history_before_access(&store, thread_id, path.as_path())
        .await
        .expect_err("an immutable root requires a mutable parent publication");

    assert!(
        error.to_string().contains("immutable"),
        "unexpected immutable-root error: {error}"
    );
    assert_eq!(std::fs::read(path).expect("reread immutable"), source);
}

#[tokio::test]
async fn clean_immutable_root_is_readable_without_mutation() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::from_u128(/*value*/ 0x7012);
    let segment_id = SegmentId::new();
    let source = supervisor_rollout(
        thread_id,
        Some(segment_id),
        "gAAAAABqfQkApRY563QcNHss2A4AKN3hJ027UTP8TRjRMwBjGzwdQ1xZ-6mLXPG8wa8TVLFB3ggULSAKlfpl7C4YbWdR4_R28-k0urBPtKk2Amtf8DdoShe_vVF4ffJ0XoIvR1ryVWmP",
    );
    let path = write_immutable_rollout(home.path(), thread_id, segment_id, source.as_slice());
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let access = repair_compatibility_history_before_access(&store, thread_id, path.as_path())
        .await
        .expect("clean immutable roots remain readable");
    drop(access);

    assert_eq!(std::fs::read(path).expect("reread immutable"), source);
}

#[tokio::test]
async fn active_only_access_does_not_scan_or_repair_a_predecessor() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::from_u128(/*value*/ 0x700a);
    let referenced_segment = SegmentId::new();
    let (referenced, _, _) = referenced_poison_rollout(thread_id, referenced_segment);
    let referenced_path = write_immutable_rollout(
        home.path(),
        thread_id,
        referenced_segment,
        referenced.as_slice(),
    );
    let root = active_reference_rollout(
        thread_id,
        SegmentId::new(),
        RolloutReferenceItem {
            rollout_id: Some(thread_id),
            rollout_path: referenced_path.clone(),
            thread_id: Some(thread_id),
            rollout_timestamp: None,
            segment_id: Some(referenced_segment),
            max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        },
    );
    let root_path = write_rollout(home.path(), thread_id, root.as_slice());
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let access = super::goal_supervisor_runtime_repair::repair_active_history_before_access(
        &store,
        thread_id,
        root_path.as_path(),
    )
    .await
    .expect("active checkpoint does not consume its predecessor");
    drop(access);

    assert_eq!(std::fs::read(root_path).expect("read root"), root);
    assert_eq!(
        std::fs::read(referenced_path).expect("read predecessor"),
        referenced
    );
}

fn agent_message_is_encrypted(bytes: &[u8], expected_id: &str) -> bool {
    decode_rollout(bytes).iter().any(|line| {
        let RolloutItem::ResponseItem(item) = &line.item else {
            return false;
        };
        let ResponseItem::AgentMessage {
            id: Some(id),
            content,
            ..
        } = &**item
        else {
            return false;
        };
        id.as_str() == expected_id
            && matches!(
                content.get(1),
                Some(AgentMessageInputContent::EncryptedContent { .. })
            )
    })
}

#[tokio::test]
async fn nth_user_reference_repairs_only_the_consumed_prefix() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::from_u128(/*value*/ 0x7006);
    let referenced_segment = SegmentId::new();
    let (referenced, first_id, second_id) =
        referenced_poison_rollout(thread_id, referenced_segment);
    let referenced_path = write_immutable_rollout(
        home.path(),
        thread_id,
        referenced_segment,
        referenced.as_slice(),
    );
    let root_segment = SegmentId::new();
    let root = active_reference_rollout(
        thread_id,
        root_segment,
        RolloutReferenceItem {
            rollout_id: Some(thread_id),
            rollout_path: referenced_path.clone(),
            thread_id: Some(thread_id),
            rollout_timestamp: None,
            segment_id: Some(referenced_segment),
            max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: Some(1),
            compacted_replacement_history_filter_texts: None,
        },
    );
    let root_path = write_rollout(home.path(), thread_id, root.as_slice());
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let access = repair_compatibility_history_before_access(&store, thread_id, root_path.as_path())
        .await
        .expect("repair bounded reference");
    drop(access);

    assert_eq!(
        std::fs::read(referenced_path.as_path()).expect("read original immutable"),
        referenced
    );
    let repaired_root = decode_rollout(
        std::fs::read(root_path.as_path())
            .expect("read repaired root")
            .as_slice(),
    );
    let RolloutItem::RolloutReference(repaired_reference) = &repaired_root[1].item else {
        panic!("expected repaired reference");
    };
    assert_ne!(repaired_reference.segment_id, Some(referenced_segment));
    let repaired_segment = std::fs::read(repaired_reference.rollout_path.as_path())
        .expect("read repaired immutable segment");
    assert!(!agent_message_is_encrypted(
        repaired_segment.as_slice(),
        first_id.as_str()
    ));
    assert!(agent_message_is_encrypted(
        repaired_segment.as_slice(),
        second_id.as_str()
    ));
}

#[tokio::test]
async fn nth_user_reference_rejects_selected_message_id_copied_across_segments() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::from_u128(/*value*/ 0x7013);
    let nested_segment = SegmentId::new();
    let outer_segment = SegmentId::new();
    let (outer_source, copied_id, _) = referenced_poison_rollout(thread_id, outer_segment);

    let mut nested_lines = decode_rollout(
        supervisor_rollout(
            thread_id,
            Some(nested_segment),
            "copied synthetic poisoned supervisor instruction",
        )
        .as_slice(),
    );
    set_agent_message_id(&mut nested_lines[2], copied_id.as_str());
    let nested_source = encode_rollout(nested_lines.as_slice());
    let nested_path = write_immutable_rollout(
        home.path(),
        thread_id,
        nested_segment,
        nested_source.as_slice(),
    );

    let mut outer_lines = decode_rollout(outer_source.as_slice());
    outer_lines.truncate(4);
    outer_lines.push(RolloutLine {
        timestamp: "2026-08-13T00:00:04Z".to_string(),
        ordinal: Some(4),
        item: RolloutItem::RolloutReference(RolloutReferenceItem {
            rollout_id: Some(thread_id),
            rollout_path: nested_path.clone(),
            thread_id: Some(thread_id),
            rollout_timestamp: None,
            segment_id: Some(nested_segment),
            max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        }),
    });
    let outer_source = encode_rollout(outer_lines.as_slice());
    let outer_path = write_immutable_rollout(
        home.path(),
        thread_id,
        outer_segment,
        outer_source.as_slice(),
    );
    let root = active_reference_rollout(
        thread_id,
        SegmentId::new(),
        RolloutReferenceItem {
            rollout_id: Some(thread_id),
            rollout_path: outer_path.clone(),
            thread_id: Some(thread_id),
            rollout_timestamp: None,
            segment_id: Some(outer_segment),
            max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: Some(1),
            compacted_replacement_history_filter_texts: None,
        },
    );
    let root_path = write_rollout(home.path(), thread_id, root.as_slice());
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let error = repair_compatibility_history_before_access(&store, thread_id, root_path.as_path())
        .await
        .expect_err("one selected message identity cannot authorize two physical occurrences");

    assert!(
        error.to_string().contains("more than one rollout segment"),
        "unexpected duplicate error: {error}"
    );
    assert_eq!(std::fs::read(root_path).expect("read root"), root);
    assert_eq!(
        std::fs::read(outer_path).expect("read outer segment"),
        outer_source
    );
    assert_eq!(
        std::fs::read(nested_path).expect("read nested segment"),
        nested_source
    );
}

#[tokio::test]
async fn recent_zero_depth_does_not_scan_an_excluded_ordinary_reference() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::from_u128(/*value*/ 0x7007);
    let referenced_segment = SegmentId::new();
    let (referenced, _, _) = referenced_poison_rollout(thread_id, referenced_segment);
    let referenced_path = write_immutable_rollout(
        home.path(),
        thread_id,
        referenced_segment,
        referenced.as_slice(),
    );
    let root_segment = SegmentId::new();
    let root = active_reference_rollout(
        thread_id,
        root_segment,
        RolloutReferenceItem {
            rollout_id: Some(thread_id),
            rollout_path: referenced_path.clone(),
            thread_id: Some(thread_id),
            rollout_timestamp: None,
            segment_id: Some(referenced_segment),
            max_depth: 0,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        },
    );
    let root_path = write_rollout(home.path(), thread_id, root.as_slice());
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let access = super::goal_supervisor_runtime_repair::repair_recent_history_before_access(
        &store,
        thread_id,
        root_path.as_path(),
    )
    .await
    .expect("recent read excludes depth-zero ordinary reference");
    drop(access);

    assert_eq!(std::fs::read(root_path).expect("read root"), root);
    assert_eq!(
        std::fs::read(referenced_path).expect("read referenced rollout"),
        referenced
    );
}

#[tokio::test]
async fn recent_zero_depth_still_repairs_a_fork_boundary_reference() {
    let home = TempDir::new().expect("temp home");
    let root_thread_id = ThreadId::from_u128(/*value*/ 0x700b);
    let referenced_thread_id = ThreadId::from_u128(/*value*/ 0x700c);
    let referenced_segment = SegmentId::new();
    let (referenced, first_id, _) =
        referenced_poison_rollout(referenced_thread_id, referenced_segment);
    let referenced_path = write_immutable_rollout(
        home.path(),
        referenced_thread_id,
        referenced_segment,
        referenced.as_slice(),
    );
    let root = active_reference_rollout(
        root_thread_id,
        SegmentId::new(),
        RolloutReferenceItem {
            rollout_id: Some(referenced_thread_id),
            rollout_path: referenced_path.clone(),
            thread_id: Some(referenced_thread_id),
            rollout_timestamp: None,
            segment_id: Some(referenced_segment),
            max_depth: 0,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        },
    );
    let root_path = write_rollout(home.path(), root_thread_id, root.as_slice());
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let access = super::goal_supervisor_runtime_repair::repair_recent_history_before_access(
        &store,
        root_thread_id,
        root_path.as_path(),
    )
    .await
    .expect("fork boundaries are not limited by ordinary reference depth");
    drop(access);

    assert_eq!(
        std::fs::read(referenced_path).expect("read original immutable"),
        referenced
    );
    let repaired_root = decode_rollout(
        std::fs::read(root_path.as_path())
            .expect("read repaired root")
            .as_slice(),
    );
    let RolloutItem::RolloutReference(reference) = &repaired_root[1].item else {
        panic!("expected repaired reference");
    };
    let repaired = std::fs::read(reference.rollout_path.as_path()).expect("read repaired segment");
    assert!(!agent_message_is_encrypted(
        repaired.as_slice(),
        first_id.as_str()
    ));
}

#[tokio::test]
async fn dirty_mutable_reference_fallback_is_repaired_in_place_with_an_immutable_backup() {
    let home = TempDir::new().expect("temp home");
    let root_thread_id = ThreadId::from_u128(/*value*/ 0x700d);
    let referenced_thread_id = ThreadId::from_u128(/*value*/ 0x700e);
    let referenced_segment = SegmentId::new();
    let referenced = supervisor_rollout(
        referenced_thread_id,
        Some(referenced_segment),
        "synthetic poisoned supervisor instruction",
    );
    let referenced_path = write_rollout(home.path(), referenced_thread_id, referenced.as_slice());
    let root = active_reference_rollout(
        root_thread_id,
        SegmentId::new(),
        RolloutReferenceItem {
            rollout_id: Some(referenced_thread_id),
            rollout_path: referenced_path.clone(),
            thread_id: Some(referenced_thread_id),
            rollout_timestamp: None,
            segment_id: Some(referenced_segment),
            max_depth: DEFAULT_ROLLOUT_REFERENCE_DEPTH,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        },
    );
    let root_path = write_rollout(home.path(), root_thread_id, root.as_slice());
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let access =
        repair_compatibility_history_before_access(&store, root_thread_id, root_path.as_path())
            .await
            .expect("mutable fallback is stable under its owner lock");
    drop(access);

    let repaired_predecessor =
        std::fs::read(referenced_path.as_path()).expect("read repaired mutable predecessor");
    assert_eq!(repaired_predecessor.len(), referenced.len());
    assert!(!agent_message_is_encrypted(
        repaired_predecessor.as_slice(),
        "amsg_01900000-0000-7000-8000-000000000002"
    ));
    let original_backup = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(referenced_thread_id.to_string())
        .join(referenced_segment.to_string())
        .join(
            referenced_path
                .file_name()
                .expect("mutable predecessor filename"),
        );
    assert_eq!(
        std::fs::read(original_backup).expect("read immutable predecessor backup"),
        referenced
    );
    let repaired_root = decode_rollout(
        std::fs::read(root_path)
            .expect("read repaired root")
            .as_slice(),
    );
    let RolloutItem::RolloutReference(reference) = &repaired_root[1].item else {
        panic!("expected repaired reference");
    };
    assert_eq!(reference.rollout_path, referenced_path);
    assert_ne!(reference.segment_id, Some(referenced_segment));
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[tokio::test]
async fn clean_retry_removes_staging_left_after_exchange_crash() {
    use super::goal_supervisor_runtime_repair::clear_quarantine_for_test;
    use super::segment::confined_publication::CRASH_TEST_LOCK;
    use super::segment::confined_publication::ConfinedCrashBoundary;
    use super::segment::confined_publication::inject_crash_boundary;

    let _crash_guard = std::sync::Arc::clone(&CRASH_TEST_LOCK).lock_owned().await;
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::from_u128(/*value*/ 0x7014);
    let source = supervisor_rollout(
        thread_id,
        Some(SegmentId::new()),
        "synthetic poisoned supervisor instruction",
    );
    let path = write_rollout(home.path(), thread_id, source.as_slice());
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    inject_crash_boundary(
        path.as_path(),
        ConfinedCrashBoundary::AfterExchangeBeforeParentSync,
    );

    let error = repair_compatibility_history_before_access(&store, thread_id, path.as_path())
        .await
        .expect_err("injected exchange crash has unknown durability");
    assert!(error.to_string().contains("restart before continuing"));
    assert!(repair_staging_entries(path.parent().expect("rollout parent")) > 0);

    // A process restart clears only in-memory quarantine. The visible replacement is already clean,
    // so the clean access path must still execute destination-scoped staging recovery under locks.
    clear_quarantine_for_test(home.path());
    let restarted = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let access = repair_compatibility_history_before_access(&restarted, thread_id, path.as_path())
        .await
        .expect("clean retry removes displaced source staging");
    drop(access);

    assert_eq!(
        repair_staging_entries(path.parent().expect("rollout parent")),
        0
    );
    assert!(!agent_message_is_encrypted(
        std::fs::read(path)
            .expect("read repaired rollout")
            .as_slice(),
        "amsg_01900000-0000-7000-8000-000000000002"
    ));
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn repair_staging_entries(directory: &Path) -> usize {
    std::fs::read_dir(directory)
        .expect("read rollout directory")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".codex-history-repair-")
        })
        .count()
}

#[tokio::test]
async fn history_repair_writer_token_is_bound_to_one_reserved_thread() {
    let home = TempDir::new().expect("temp home");
    let owned = ThreadId::from_u128(/*value*/ 0x7015);
    let unowned = ThreadId::from_u128(/*value*/ 0x7016);
    let source = supervisor_rollout(
        owned,
        Some(SegmentId::new()),
        "synthetic poisoned supervisor instruction",
    );
    let path = write_rollout(home.path(), owned, source.as_slice());
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let access = repair_compatibility_history_before_access(&store, owned, path.as_path())
        .await
        .expect("acquire complete repair ownership");

    access
        .writer_token(&store, owned)
        .await
        .expect("token for reserved owner");
    let error = match access.writer_token(&store, unowned).await {
        Ok(_) => panic!("token cannot be created for an unreserved owner"),
        Err(error) => error,
    };

    assert!(error.to_string().contains(&unowned.to_string()));
}

#[tokio::test]
async fn clean_history_remains_available_during_an_unrelated_migration_job() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::from_u128(/*value*/ 0x7017);
    let source = supervisor_rollout(
        thread_id,
        Some(SegmentId::new()),
        "gAAAAABqfQkApRY563QcNHss2A4AKN3hJ027UTP8TRjRMwBjGzwdQ1xZ-6mLXPG8wa8TVLFB3ggULSAKlfpl7C4YbWdR4_R28-k0urBPtKk2Amtf8DdoShe_vVF4ffJ0XoIvR1ryVWmP",
    );
    let path = write_rollout(home.path(), thread_id, source.as_slice());
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let job = codex_rollout::try_acquire_rollout_maintenance_job_lock(home.path())
        .expect("open migration job lock")
        .expect("claim migration job");
    let access = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        repair_compatibility_history_before_access(&store, thread_id, &path),
    )
    .await
    .expect("clean access must not wait for the unrelated job")
    .expect("read clean history");
    assert!(access.writer_token(&store, thread_id).await.is_err());
    assert_eq!(std::fs::read(path).expect("reread source"), source);
    drop(access);
    drop(job);
}

#[tokio::test]
async fn compressed_history_base_repairs_only_the_consumed_jsonl_prefix() {
    let home = TempDir::new().expect("temp home");
    let parent_thread_id = ThreadId::from_u128(/*value*/ 0x700f);
    let child_thread_id = ThreadId::from_u128(/*value*/ 0x7010);
    let parent_segment = SegmentId::new();
    let (parent, first_id, second_id) = referenced_poison_rollout(parent_thread_id, parent_segment);
    let parent_lines = decode_rollout(parent.as_slice());
    let consumed_prefix = encode_rollout(&parent_lines[..4]);
    let parent_path = write_rollout(home.path(), parent_thread_id, parent.as_slice());
    let compressed_path = parent_path.with_extension("jsonl.zst");
    std::fs::write(compressed_path.as_path(), compress(parent.as_slice()))
        .expect("write compressed parent");
    std::fs::remove_file(parent_path.as_path()).expect("remove plain parent");
    let child = history_base_rollout(
        child_thread_id,
        SegmentId::new(),
        HistoryPosition {
            thread_id: parent_thread_id,
            end_ordinal_exclusive: 4,
            end_byte_offset: consumed_prefix.len() as u64,
        },
    );
    let child_path = write_rollout(home.path(), child_thread_id, child.as_slice());
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let access =
        repair_compatibility_history_before_access(&store, child_thread_id, child_path.as_path())
            .await
            .expect("repair bounded compressed history base");
    drop(access);

    assert!(
        !parent_path.exists(),
        "repair preserves compressed representation"
    );
    let repaired = decompress(
        std::fs::read(compressed_path.as_path())
            .expect("read compressed parent")
            .as_slice(),
    );
    assert_eq!(repaired.len(), parent.len());
    assert!(!agent_message_is_encrypted(
        repaired.as_slice(),
        first_id.as_str()
    ));
    assert!(agent_message_is_encrypted(
        repaired.as_slice(),
        second_id.as_str()
    ));
    assert_eq!(std::fs::read(child_path).expect("read child"), child);
}

#[tokio::test]
async fn repaired_history_remains_writer_reserved_until_access_is_dropped() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::from_u128(/*value*/ 0x7008);
    let source = supervisor_rollout(
        thread_id,
        Some(SegmentId::new()),
        "synthetic poisoned supervisor instruction",
    );
    let path = write_rollout(home.path(), thread_id, source.as_slice());
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let competing = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let access = repair_compatibility_history_before_access(&store, thread_id, path.as_path())
        .await
        .expect("repair rollout");
    let conflict = competing
        .writer_lock_coordinator
        .acquire(thread_id)
        .err()
        .expect("repair access retains cross-process writer lock");
    assert!(conflict.to_string().contains("active writer"));
    drop(access);
    competing
        .writer_lock_coordinator
        .acquire(thread_id)
        .expect("dropping access releases writer lock");
}

#[tokio::test]
async fn clean_history_remains_writer_reserved_until_access_is_dropped() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::from_u128(/*value*/ 0x7011);
    let source = supervisor_rollout(
        thread_id,
        Some(SegmentId::new()),
        "gAAAAABqfQkApRY563QcNHss2A4AKN3hJ027UTP8TRjRMwBjGzwdQ1xZ-6mLXPG8wa8TVLFB3ggULSAKlfpl7C4YbWdR4_R28-k0urBPtKk2Amtf8DdoShe_vVF4ffJ0XoIvR1ryVWmP",
    );
    let path = write_rollout(home.path(), thread_id, source.as_slice());
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let competing = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);

    let access = repair_compatibility_history_before_access(&store, thread_id, path.as_path())
        .await
        .expect("validate clean rollout");
    assert!(access.is_reserved());
    assert!(
        competing
            .writer_lock_coordinator
            .acquire(thread_id)
            .is_err()
    );
    drop(access);
    competing
        .writer_lock_coordinator
        .acquire(thread_id)
        .expect("dropping access releases clean writer lock");
}
