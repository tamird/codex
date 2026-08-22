#![cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]

use std::io::Read as _;
use std::path::Path;
use std::time::Duration;
use std::time::SystemTime;

use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::models::AgentMessageInputContent;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_state::SqliteConfig;
use codex_state::open_thread_history_db;
use codex_utils_absolute_path::AbsolutePathBuf;
use tempfile::TempDir;

use super::super::confined_publication::CRASH_TEST_LOCK;
use super::super::confined_publication::ConfinedCrashBoundary;
use super::super::confined_publication::inject_crash_boundary;
#[cfg(unix)]
use super::CODEX_HOME_RETARGETS;
use super::HistoryRepairPublication;
use super::POSTCOMMIT_SYNC_FAILURES;
use super::PRECOMMIT_FAILURES;
use super::PUBLISHED_REPLACEMENTS;
use super::RawSessionEnvelope;
use super::SOURCE_REPLACEMENTS;
use super::TestHistoryRepairWriterToken;
use super::authorize_history_repair_writer;
use super::history_repair_segment_id;
use super::identity_cleared_preimage;
use super::install_existing_identity_history_repair_backup as install_existing_identity_history_repair_backup_impl;
use super::install_history_repair_segment as install_history_repair_segment_impl;
use super::physical_records;
use super::publish_compressed_history_repair_replacement as publish_compressed_history_repair_replacement_impl;
use super::publish_history_repair_replacement as publish_history_repair_replacement_impl;
use super::recover_history_repair_publication;
use super::recover_history_repair_publication_for_test;
use super::reserve_history_repair_lifecycle;
use super::reserve_history_repair_maintenance;
use super::validate_legacy_initial_repair_path;
use crate::local::LocalThreadStore;
use crate::local::test_support::test_config;

async fn install_history_repair_segment(
    codex_home: &Path,
    thread_id: ThreadId,
    segment_id: SegmentId,
    source_path: &Path,
    identity_cleared_bytes: &[u8],
    bytes: &[u8],
) -> crate::ThreadStoreResult<std::path::PathBuf> {
    install_history_repair_segment_impl(
        &TestHistoryRepairWriterToken { thread_id },
        codex_home,
        thread_id,
        segment_id,
        source_path,
        identity_cleared_bytes,
        bytes,
    )
    .await
}

async fn install_existing_identity_history_repair_backup(
    codex_home: &Path,
    thread_id: ThreadId,
    segment_id: SegmentId,
    source_path: &Path,
    bytes: &[u8],
) -> crate::ThreadStoreResult<std::path::PathBuf> {
    install_existing_identity_history_repair_backup_impl(
        &TestHistoryRepairWriterToken { thread_id },
        codex_home,
        thread_id,
        segment_id,
        source_path,
        bytes,
    )
    .await
}

async fn publish_history_repair_replacement(
    codex_home: &Path,
    stable_path: &Path,
    replacement: &[u8],
) -> crate::ThreadStoreResult<HistoryRepairPublication> {
    let thread_id = thread_id_from_source(replacement);
    publish_history_repair_replacement_impl(
        &TestHistoryRepairWriterToken { thread_id },
        codex_home,
        stable_path,
        replacement,
    )
    .await
}

async fn publish_compressed_history_repair_replacement(
    codex_home: &Path,
    compressed_path: &Path,
    replacement: &[u8],
) -> crate::ThreadStoreResult<HistoryRepairPublication> {
    let thread_id = thread_id_from_source(replacement);
    publish_compressed_history_repair_replacement_impl(
        &TestHistoryRepairWriterToken { thread_id },
        codex_home,
        compressed_path,
        replacement,
    )
    .await
}

fn source_with_segment(
    thread_id: ThreadId,
    segment_id: Option<SegmentId>,
    suffix: &[u8],
) -> Vec<u8> {
    let line = RolloutLine {
        timestamp: "2026-08-10T20:00:00Z".to_string(),
        ordinal: Some(0),
        item: RolloutItem::SessionMeta(SessionMetaLine {
            meta: SessionMeta {
                session_id: thread_id.into(),
                id: thread_id,
                segment_id,
                ..SessionMeta::default()
            },
            git: None,
        }),
    };
    let mut source = serde_json::to_vec(&line).expect("serialize session metadata");
    source.push(b'\n');
    source.extend_from_slice(suffix);
    source
}

fn immutable_source(thread_id: ThreadId, suffix: &[u8]) -> (SegmentId, Vec<u8>, Vec<u8>) {
    let provisional_segment_id = SegmentId::new();
    let provisional = source_with_segment(thread_id, Some(provisional_segment_id), suffix);
    let identity_cleared =
        identity_cleared_preimage(&provisional, thread_id, provisional_segment_id)
            .expect("clear provisional identity");
    let segment_id = history_repair_segment_id(identity_cleared.as_slice());
    let final_bytes = source_with_segment(thread_id, Some(segment_id), suffix);
    assert_eq!(
        identity_cleared_preimage(&final_bytes, thread_id, segment_id)
            .expect("clear final identity"),
        identity_cleared
    );
    (segment_id, identity_cleared, final_bytes)
}

fn active_source(thread_id: ThreadId, text: &str) -> Vec<u8> {
    let mut source = source_with_segment(thread_id, Some(SegmentId::new()), b"");
    let response = RolloutLine {
        timestamp: "2026-08-10T20:00:01Z".to_string(),
        ordinal: Some(1),
        item: RolloutItem::ResponseItem(
            ResponseItem::AgentMessage {
                id: None,
                author: "goal_supervisor".to_string(),
                recipient: "root".to_string(),
                content: vec![AgentMessageInputContent::InputText {
                    text: text.to_string(),
                }],
                internal_chat_message_metadata_passthrough: None,
            }
            .into(),
        ),
    };
    source.extend(serde_json::to_vec(&response).expect("serialize response item"));
    source.push(b'\n');
    source
}

fn replace_ascii(mut source: Vec<u8>, old: &[u8], new: &[u8]) -> Vec<u8> {
    assert_eq!(old.len(), new.len());
    let start = source
        .windows(old.len())
        .rposition(|window| window == old)
        .expect("replacement target");
    source[start..start + new.len()].copy_from_slice(new);
    source
}

fn repaired_active_replacement(source: &[u8], old: &[u8], new: &[u8]) -> Vec<u8> {
    let mut replacement = replace_ascii(source.to_vec(), old, new);
    let (first_record_start, first_record_len) = session_record_bounds(replacement.as_slice());
    let first_record_end = first_record_start + first_record_len;
    let mut line =
        serde_json::from_slice::<RolloutLine>(&replacement[first_record_start..first_record_end])
            .expect("parse source metadata");
    let RolloutItem::SessionMeta(meta) = &mut line.item else {
        panic!("first record is not session metadata");
    };
    let thread_id = meta.meta.id;
    let old_segment_id = meta.meta.segment_id.expect("old segment id");
    let preimage = identity_cleared_preimage(&replacement, thread_id, old_segment_id)
        .expect("derive identity preimage");
    let new_segment_id = history_repair_segment_id(preimage.as_slice());
    meta.meta.segment_id = Some(new_segment_id);
    let first = super::serialize_rollout_line_same_length(
        &line,
        &replacement[first_record_start..first_record_end],
    )
    .expect("rewrite session metadata");
    replacement[first_record_start..first_record_end].copy_from_slice(first.as_slice());
    replacement
}

fn session_record_bounds(source: &[u8]) -> (usize, usize) {
    let mut start = 0;
    for record in source.split_inclusive(|byte| *byte == b'\n') {
        if serde_json::from_slice::<RolloutLine>(record)
            .is_ok_and(|line| matches!(line.item, RolloutItem::SessionMeta(_)))
        {
            return (start, record.len());
        }
        start += record.len();
    }
    panic!("source has no session metadata record");
}

fn segment_id_from_source(source: &[u8]) -> SegmentId {
    let (start, len) = session_record_bounds(source);
    let line = serde_json::from_slice::<RolloutLine>(&source[start..start + len])
        .expect("parse session metadata");
    let RolloutItem::SessionMeta(meta) = line.item else {
        panic!("session metadata record changed type");
    };
    meta.meta.segment_id.expect("segment id")
}

fn thread_id_from_source(source: &[u8]) -> ThreadId {
    let (start, len) = session_record_bounds(source);
    let line = serde_json::from_slice::<RolloutLine>(&source[start..start + len])
        .expect("parse session metadata");
    let RolloutItem::SessionMeta(meta) = line.item else {
        panic!("session metadata record changed type");
    };
    meta.meta.id
}

async fn prepare_active_source(home: &TempDir, path: &std::path::Path, source: &[u8]) {
    tokio::fs::write(path, source)
        .await
        .expect("write active source");
    let _ = install_active_backup(home, path, source).await;
}

async fn install_active_backup(
    home: &TempDir,
    path: &std::path::Path,
    source: &[u8],
) -> std::path::PathBuf {
    let (start, len) = session_record_bounds(source);
    let first_record = &source[start..start + len];
    let line = serde_json::from_slice::<RolloutLine>(first_record).expect("parse session metadata");
    let RolloutItem::SessionMeta(meta) = line.item else {
        panic!("first record is not session metadata");
    };
    let segment_id = meta.meta.segment_id.expect("segment id");
    install_existing_identity_history_repair_backup(
        home.path(),
        meta.meta.id,
        segment_id,
        path,
        source,
    )
    .await
    .expect("install exact active backup")
}

fn compress(bytes: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(bytes, 0).expect("compress rollout")
}

#[cfg(unix)]
async fn set_private_permissions(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;

    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .await
        .expect("set private rollout permissions");
}

async fn read_if_exists(path: &Path) -> Option<Vec<u8>> {
    match tokio::fs::read(path).await {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => panic!("read {}: {error}", path.display()),
    }
}

#[test]
fn physical_segment_identity_includes_rejected_records() {
    let base = history_repair_segment_id(b"meta\nnot-json\n");
    assert_eq!(base, history_repair_segment_id(b"meta\nnot-json\n"));
    assert_ne!(base, history_repair_segment_id(b"meta\nother bad\n"));
}

#[test]
fn physical_segment_identity_preserves_session_metadata_encoding() {
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let canonical = source_with_segment(thread_id, Some(segment_id), b"");
    let session_record = physical_records(canonical.as_slice())[0];
    let envelope = serde_json::from_slice::<RawSessionEnvelope<'_>>(session_record).unwrap();
    let payload = envelope.payload.get();
    let timestamp = "2026-08-10T20:00:00Z";
    let reordered = format!(
        "{{\"ordinal\":0,\"timestamp\":\"{timestamp}\",\"type\":\"session_meta\",\"payload\":{payload}}}\n"
    );
    let reordered_with_unknown = format!(
        "{{\"ordinal\":0,\"timestamp\":\"{timestamp}\",\"type\":\"session_meta\",\"payload\":{{\"future\":\"x\",{}}}}}\n",
        payload
            .strip_prefix('{')
            .unwrap()
            .strip_suffix('}')
            .unwrap()
    );
    let reordered_with_trailing_unknown = format!(
        "{{\"ordinal\":0,\"timestamp\":\"{timestamp}\",\"type\":\"session_meta\",\"payload\":{{{},\"future\":\"x\"}}}}\n",
        payload
            .strip_prefix('{')
            .unwrap()
            .strip_suffix('}')
            .unwrap()
    );
    assert_eq!(
        reordered_with_unknown.len(),
        reordered_with_trailing_unknown.len()
    );
    for bytes in [
        reordered.as_bytes(),
        reordered_with_unknown.as_bytes(),
        reordered_with_trailing_unknown.as_bytes(),
    ] {
        let parsed = serde_json::from_slice::<RolloutLine>(bytes).expect("parse physical variant");
        assert!(matches!(parsed.item, RolloutItem::SessionMeta(_)));
    }
    let canonical_preimage = identity_cleared_preimage(&canonical, thread_id, segment_id).unwrap();
    let reordered_preimage =
        identity_cleared_preimage(reordered.as_bytes(), thread_id, segment_id).unwrap();
    let leading_unknown_preimage =
        identity_cleared_preimage(reordered_with_unknown.as_bytes(), thread_id, segment_id)
            .unwrap();
    let trailing_unknown_preimage = identity_cleared_preimage(
        reordered_with_trailing_unknown.as_bytes(),
        thread_id,
        segment_id,
    )
    .unwrap();
    assert_ne!(canonical_preimage, reordered_preimage);
    assert_ne!(leading_unknown_preimage, trailing_unknown_preimage);
    assert_ne!(
        history_repair_segment_id(leading_unknown_preimage.as_slice()),
        history_repair_segment_id(trailing_unknown_preimage.as_slice())
    );
}

#[tokio::test]
async fn raw_segment_install_is_private_idempotent_and_rejects_identity_collision() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::new();
    let source_path = home.path().join(format!("rollout-{thread_id}.jsonl"));
    let (segment_id, identity_cleared, expected) = immutable_source(thread_id, b"not-json\n\n");
    let destination = install_history_repair_segment(
        home.path(),
        thread_id,
        segment_id,
        source_path.as_path(),
        identity_cleared.as_slice(),
        expected.as_slice(),
    )
    .await
    .expect("install raw segment");
    assert_eq!(
        tokio::fs::read(destination.as_path())
            .await
            .expect("read backup"),
        expected
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(
            tokio::fs::metadata(destination.as_path())
                .await
                .expect("backup metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        tokio::fs::set_permissions(
            destination.as_path(),
            std::fs::Permissions::from_mode(0o664),
        )
        .await
        .expect("relax reused segment mode");
    }
    #[cfg(unix)]
    {
        let error = install_history_repair_segment(
            home.path(),
            thread_id,
            segment_id,
            source_path.as_path(),
            identity_cleared.as_slice(),
            expected.as_slice(),
        )
        .await
        .expect_err("reuse with mismatched metadata must fail closed");
        assert!(error.to_string().contains("private 0600 permissions"));
        use std::os::unix::fs::PermissionsExt as _;
        tokio::fs::set_permissions(
            destination.as_path(),
            std::fs::Permissions::from_mode(0o600),
        )
        .await
        .expect("restore private mode");
    }
    let same = install_history_repair_segment(
        home.path(),
        thread_id,
        segment_id,
        source_path.as_path(),
        identity_cleared.as_slice(),
        expected.as_slice(),
    )
    .await
    .expect("reuse equal segment");
    assert_eq!(same, destination);

    let different = replace_ascii(expected.clone(), b"not-json", b"bad-json");
    tokio::fs::write(destination.as_path(), different)
        .await
        .expect("corrupt existing identity");
    let error = install_history_repair_segment(
        home.path(),
        thread_id,
        segment_id,
        source_path.as_path(),
        identity_cleared.as_slice(),
        expected.as_slice(),
    )
    .await
    .expect_err("identity collision must fail");
    assert!(error.to_string().contains("different contents"));
}

#[tokio::test]
async fn raw_segment_install_reuses_equal_compressed_identity_and_rejects_a_collision() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::new();
    let source_path = home.path().join(format!("rollout-{thread_id}.jsonl"));
    let (segment_id, identity_cleared, expected) = immutable_source(thread_id, b"not-json\n");
    let destination = install_history_repair_segment(
        home.path(),
        thread_id,
        segment_id,
        source_path.as_path(),
        identity_cleared.as_slice(),
        expected.as_slice(),
    )
    .await
    .expect("install plain segment");
    tokio::fs::remove_file(destination.as_path())
        .await
        .expect("remove plain segment");
    let compressed_path = destination.with_extension("jsonl.zst");
    tokio::fs::write(compressed_path.as_path(), compress(expected.as_slice()))
        .await
        .expect("install compressed segment");
    set_private_permissions(compressed_path.as_path()).await;

    let reused = install_history_repair_segment(
        home.path(),
        thread_id,
        segment_id,
        source_path.as_path(),
        identity_cleared.as_slice(),
        expected.as_slice(),
    )
    .await
    .expect("reuse compressed segment");
    assert_eq!(reused, compressed_path);
    assert!(!tokio::fs::try_exists(destination.as_path()).await.unwrap());

    let different = replace_ascii(expected.clone(), b"not-json", b"bad-json");
    tokio::fs::write(compressed_path.as_path(), compress(different.as_slice()))
        .await
        .expect("replace compressed segment");
    let error = install_history_repair_segment(
        home.path(),
        thread_id,
        segment_id,
        source_path.as_path(),
        identity_cleared.as_slice(),
        expected.as_slice(),
    )
    .await
    .expect_err("compressed identity collision must fail");
    assert!(error.to_string().contains("different contents"));
    assert!(!tokio::fs::try_exists(destination).await.unwrap());
}

#[tokio::test]
async fn raw_segment_install_authenticates_plain_and_compressed_siblings() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::new();
    let source_path = home.path().join(format!("rollout-{thread_id}.jsonl"));
    let (segment_id, identity_cleared, expected) = immutable_source(thread_id, b"not-json\n");
    let destination = install_history_repair_segment(
        home.path(),
        thread_id,
        segment_id,
        source_path.as_path(),
        identity_cleared.as_slice(),
        expected.as_slice(),
    )
    .await
    .expect("install plain segment");
    let compressed_path = destination.with_extension("jsonl.zst");
    tokio::fs::write(compressed_path.as_path(), compress(expected.as_slice()))
        .await
        .expect("install equal compressed sibling");
    set_private_permissions(compressed_path.as_path()).await;
    install_history_repair_segment(
        home.path(),
        thread_id,
        segment_id,
        source_path.as_path(),
        identity_cleared.as_slice(),
        expected.as_slice(),
    )
    .await
    .expect("accept equal siblings");

    let different = replace_ascii(expected.clone(), b"not-json", b"bad-json");
    tokio::fs::write(compressed_path.as_path(), compress(different.as_slice()))
        .await
        .expect("replace compressed sibling");
    let error = install_history_repair_segment(
        home.path(),
        thread_id,
        segment_id,
        source_path.as_path(),
        identity_cleared.as_slice(),
        expected.as_slice(),
    )
    .await
    .expect_err("different compressed sibling must fail");
    assert!(error.to_string().contains("different contents"));
    assert_eq!(
        tokio::fs::read(destination).await.expect("plain bytes"),
        expected
    );
}

#[cfg(unix)]
#[tokio::test]
async fn raw_segment_install_rejects_a_public_compressed_only_representation() {
    use std::os::unix::fs::PermissionsExt as _;

    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::new();
    let source_path = home.path().join(format!("rollout-{thread_id}.jsonl"));
    let (segment_id, identity_cleared, expected) = immutable_source(thread_id, b"not-json\n");
    let destination = install_history_repair_segment(
        home.path(),
        thread_id,
        segment_id,
        source_path.as_path(),
        identity_cleared.as_slice(),
        expected.as_slice(),
    )
    .await
    .expect("install plain segment");
    tokio::fs::remove_file(destination.as_path())
        .await
        .expect("remove plain segment");
    let compressed_path = destination.with_extension("jsonl.zst");
    tokio::fs::write(compressed_path.as_path(), compress(expected.as_slice()))
        .await
        .expect("write compressed segment");
    tokio::fs::set_permissions(
        compressed_path.as_path(),
        std::fs::Permissions::from_mode(0o644),
    )
    .await
    .expect("make compressed segment public");

    let error = install_history_repair_segment(
        home.path(),
        thread_id,
        segment_id,
        source_path.as_path(),
        identity_cleared.as_slice(),
        expected.as_slice(),
    )
    .await
    .expect_err("public compressed segment must fail closed");
    assert!(error.to_string().contains("private 0600 permissions"));
    assert!(!tokio::fs::try_exists(destination).await.unwrap());
}

#[cfg(unix)]
#[tokio::test]
async fn raw_segment_install_rejects_a_public_equal_compressed_sibling() {
    use std::os::unix::fs::PermissionsExt as _;

    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::new();
    let source_path = home.path().join(format!("rollout-{thread_id}.jsonl"));
    let (segment_id, identity_cleared, expected) = immutable_source(thread_id, b"not-json\n");
    let destination = install_history_repair_segment(
        home.path(),
        thread_id,
        segment_id,
        source_path.as_path(),
        identity_cleared.as_slice(),
        expected.as_slice(),
    )
    .await
    .expect("install plain segment");
    let compressed_path = destination.with_extension("jsonl.zst");
    tokio::fs::write(compressed_path.as_path(), compress(expected.as_slice()))
        .await
        .expect("write equal compressed sibling");
    tokio::fs::set_permissions(
        compressed_path.as_path(),
        std::fs::Permissions::from_mode(0o664),
    )
    .await
    .expect("make compressed sibling public");

    let error = install_history_repair_segment(
        home.path(),
        thread_id,
        segment_id,
        source_path.as_path(),
        identity_cleared.as_slice(),
        expected.as_slice(),
    )
    .await
    .expect_err("public compressed sibling must fail closed");
    assert!(error.to_string().contains("private 0600 permissions"));
    assert_eq!(tokio::fs::read(destination).await.unwrap(), expected);
}

#[tokio::test]
async fn existing_identity_backup_preserves_exact_random_segment_bytes() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let source_path = home.path().join(format!("rollout-{thread_id}.jsonl"));
    let source = source_with_segment(thread_id, Some(segment_id), b"not-json\n");
    let destination = install_existing_identity_history_repair_backup(
        home.path(),
        thread_id,
        segment_id,
        source_path.as_path(),
        source.as_slice(),
    )
    .await
    .expect("install exact existing identity");
    assert_eq!(
        tokio::fs::read(destination).await.expect("backup bytes"),
        source
    );
}

#[tokio::test]
async fn repaired_segment_rejects_wrong_content_identity() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::new();
    let (segment_id, mut identity_cleared, final_bytes) =
        immutable_source(thread_id, b"not-json\n");
    identity_cleared = replace_ascii(identity_cleared, b"not-json", b"bad-json");
    let error = install_history_repair_segment(
        home.path(),
        thread_id,
        segment_id,
        home.path().join("rollout.jsonl").as_path(),
        identity_cleared.as_slice(),
        final_bytes.as_slice(),
    )
    .await
    .expect_err("wrong content identity");
    assert!(error.to_string().contains("content-derived identity"));
}

#[tokio::test]
async fn repaired_segment_rejects_unrelated_same_span_identity_preimage() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::new();
    let (_, _, final_bytes) = immutable_source(thread_id, b"not-json\n");
    let provisional_id = SegmentId::new();
    let provisional = super::serialize_rollout_line_same_length(
        &{
            let first = final_bytes
                .split_inclusive(|byte| *byte == b'\n')
                .next()
                .expect("session metadata");
            let mut line = serde_json::from_slice::<RolloutLine>(first).expect("parse metadata");
            let RolloutItem::SessionMeta(meta) = &mut line.item else {
                panic!("session metadata");
            };
            meta.meta.segment_id = Some(provisional_id);
            line
        },
        final_bytes
            .split_inclusive(|byte| *byte == b'\n')
            .next()
            .expect("session metadata"),
    )
    .expect("provisional metadata");
    let mut unrelated = provisional;
    unrelated.extend_from_slice(b"bad-json\n");
    let unrelated_preimage = identity_cleared_preimage(&unrelated, thread_id, provisional_id)
        .expect("unrelated identity preimage");
    let unrelated_id = history_repair_segment_id(unrelated_preimage.as_slice());
    let mut claimed_final = final_bytes.clone();
    let first_len = claimed_final
        .iter()
        .position(|byte| *byte == b'\n')
        .expect("first newline")
        + 1;
    let mut line = serde_json::from_slice::<RolloutLine>(&claimed_final[..first_len])
        .expect("parse final metadata");
    let RolloutItem::SessionMeta(meta) = &mut line.item else {
        panic!("session metadata");
    };
    meta.meta.segment_id = Some(unrelated_id);
    let first = super::serialize_rollout_line_same_length(&line, &claimed_final[..first_len])
        .expect("claimed metadata");
    claimed_final[..first_len].copy_from_slice(first.as_slice());

    let error = install_history_repair_segment(
        home.path(),
        thread_id,
        unrelated_id,
        home.path().join("rollout.jsonl").as_path(),
        unrelated_preimage.as_slice(),
        claimed_final.as_slice(),
    )
    .await
    .expect_err("unrelated preimage must fail");
    assert!(error.to_string().contains("content-derived identity"));
}

#[tokio::test]
async fn active_publication_rejects_immutable_segment_without_mutation() {
    let home = TempDir::new().expect("temp home");
    let thread_id = ThreadId::new();
    let segment_id = SegmentId::new();
    let source_path = home.path().join("rollout.jsonl");
    let source = source_with_segment(thread_id, Some(segment_id), b"old\n");
    let immutable = install_existing_identity_history_repair_backup(
        home.path(),
        thread_id,
        segment_id,
        source_path.as_path(),
        source.as_slice(),
    )
    .await
    .expect("install immutable source");
    let replacement = replace_ascii(source.clone(), b"old", b"new");
    let error = publish_history_repair_replacement(
        home.path(),
        immutable.as_path(),
        replacement.as_slice(),
    )
    .await
    .expect_err("immutable target must fail closed");
    assert!(error.to_string().contains("cannot be repaired in place"));
    assert_eq!(
        tokio::fs::read(immutable).await.expect("immutable bytes"),
        source
    );
}

#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
async fn active_publication_preserves_record_spans_mode_and_mtime() {
    let home = TempDir::new().expect("temp home");
    let sessions = home.path().join("sessions");
    tokio::fs::create_dir_all(sessions.as_path())
        .await
        .expect("create sessions");
    let path = sessions.join("rollout.jsonl");
    let source = active_source(ThreadId::new(), "alpha");
    let replacement = repaired_active_replacement(source.as_slice(), b"alpha", b"ALPHA");
    tokio::fs::write(path.as_path(), source.as_slice())
        .await
        .expect("write active source");
    let backup = install_active_backup(&home, path.as_path(), source.as_slice()).await;
    let old_segment_id = segment_id_from_source(source.as_slice());
    let new_segment_id = segment_id_from_source(replacement.as_slice());
    assert_ne!(old_segment_id, new_segment_id);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        tokio::fs::set_permissions(path.as_path(), std::fs::Permissions::from_mode(0o640))
            .await
            .expect("set source mode");
    }
    let old = SystemTime::now() - Duration::from_secs(300);
    std::fs::File::open(path.as_path())
        .expect("open source")
        .set_times(std::fs::FileTimes::new().set_modified(old))
        .expect("set source time");
    let before = tokio::fs::metadata(path.as_path())
        .await
        .expect("source metadata");

    let publication =
        publish_history_repair_replacement(home.path(), path.as_path(), replacement.as_slice())
            .await
            .expect("publish replacement");
    assert!(matches!(publication, HistoryRepairPublication::Durable));
    assert_eq!(
        tokio::fs::read(path.as_path()).await.expect("read result"),
        replacement
    );
    assert_eq!(
        tokio::fs::read(backup).await.expect("old segment backup"),
        source
    );
    let new_preimage = identity_cleared_preimage(
        replacement.as_slice(),
        thread_id_from_source(replacement.as_slice()),
        new_segment_id,
    )
    .expect("new identity preimage");
    assert_eq!(
        history_repair_segment_id(new_preimage.as_slice()),
        new_segment_id
    );
    let after = tokio::fs::metadata(path.as_path())
        .await
        .expect("result metadata");
    assert_eq!(
        after.modified().expect("result mtime"),
        before.modified().expect("source mtime")
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(after.permissions().mode() & 0o777, 0o640);
    }
}

#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
async fn active_publication_preserves_leading_rejected_and_blank_records() {
    let home = TempDir::new().expect("temp home");
    let path = home.path().join("rollout.jsonl");
    let mut source = b"not-json\n\n".to_vec();
    source.extend(active_source(ThreadId::new(), "alpha"));
    let replacement = repaired_active_replacement(source.as_slice(), b"alpha", b"ALPHA");
    prepare_active_source(&home, path.as_path(), source.as_slice()).await;

    let publication =
        publish_history_repair_replacement(home.path(), path.as_path(), replacement.as_slice())
            .await
            .expect("publish replacement");
    assert!(matches!(publication, HistoryRepairPublication::Durable));
    let published = tokio::fs::read(path).await.expect("published bytes");
    assert_eq!(published, replacement);
    assert!(published.starts_with(b"not-json\n\n"));
}

#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
async fn active_publication_leaves_open_thread_history_cache_bytes_and_cursor_unchanged() {
    let home = TempDir::new().expect("temp home");
    let rollout_path = home.path().join("rollout.jsonl");
    let source = active_source(ThreadId::new(), "alpha");
    let replacement = repaired_active_replacement(source.as_slice(), b"alpha", b"ALPHA");
    prepare_active_source(&home, rollout_path.as_path(), source.as_slice()).await;

    let sqlite_home = AbsolutePathBuf::try_from(home.path().to_path_buf()).expect("absolute home");
    let sqlite = SqliteConfig::new_for_testing(sqlite_home);
    let pool = open_thread_history_db(&sqlite)
        .await
        .expect("open migrated thread history cache");
    let database_path = home.path().join("thread_history_1.sqlite");
    sqlx::query("PRAGMA wal_autocheckpoint = 0")
        .execute(&pool)
        .await
        .expect("disable auto checkpoint");
    let thread_id = thread_id_from_source(source.as_slice()).to_string();
    sqlx::query(
        "INSERT INTO thread_history_projection_state(
            thread_id, next_rollout_byte_offset, next_rollout_ordinal
         ) VALUES (?, ?, ?)",
    )
    .bind(thread_id.as_str())
    .bind(i64::try_from(source.len()).expect("source length"))
    .bind(2_i64)
    .execute(&pool)
    .await
    .expect("insert at-EOF projection position");
    sqlx::query(
        "INSERT INTO thread_items(
            thread_id, turn_id, item_id, rollout_ordinal, created_at_ms,
            item_json, item_type, updated_at_ordinal
         ) VALUES (?, 'turn', 'item', 1, 1, '{\"type\":\"userMessage\"}', 'userMessage', 1)",
    )
    .bind(thread_id.as_str())
    .execute(&pool)
    .await
    .expect("insert projected item");
    let wal_path = database_path.with_extension("sqlite-wal");
    let shm_path = database_path.with_extension("sqlite-shm");
    assert!(tokio::fs::try_exists(wal_path.as_path()).await.unwrap());
    assert!(tokio::fs::try_exists(shm_path.as_path()).await.unwrap());
    let before_database = read_if_exists(database_path.as_path()).await;
    let before_wal = read_if_exists(wal_path.as_path()).await;
    let before_shm = read_if_exists(shm_path.as_path()).await;

    let publication = publish_history_repair_replacement(
        home.path(),
        rollout_path.as_path(),
        replacement.as_slice(),
    )
    .await
    .expect("publish replacement");
    assert!(matches!(publication, HistoryRepairPublication::Durable));
    assert_eq!(
        read_if_exists(database_path.as_path()).await,
        before_database
    );
    assert_eq!(read_if_exists(wal_path.as_path()).await, before_wal);
    assert_eq!(read_if_exists(shm_path.as_path()).await, before_shm);

    let projection_position = sqlx::query_as::<_, (i64, i64)>(
        "SELECT next_rollout_byte_offset, next_rollout_ordinal
         FROM thread_history_projection_state WHERE thread_id = ?",
    )
    .bind(thread_id.as_str())
    .fetch_one(&pool)
    .await
    .expect("read projection position");
    assert_eq!(
        projection_position,
        (i64::try_from(source.len()).expect("source length"), 2)
    );
    let projected_item = sqlx::query_scalar::<_, String>(
        "SELECT item_json FROM thread_items WHERE thread_id = ? AND rollout_ordinal = 1",
    )
    .bind(thread_id)
    .fetch_one(&pool)
    .await
    .expect("read projected item");
    assert_eq!(projected_item, "{\"type\":\"userMessage\"}");
}

#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
async fn publication_failure_is_old_before_commit_and_visible_unknown_after_commit() {
    let home = TempDir::new().expect("temp home");
    let path = home.path().join("rollout.jsonl");
    let source = active_source(ThreadId::new(), "old");
    let replacement = repaired_active_replacement(source.as_slice(), b"old", b"new");
    prepare_active_source(&home, path.as_path(), source.as_slice()).await;

    PRECOMMIT_FAILURES
        .lock()
        .expect("precommit mutex")
        .insert(std::fs::canonicalize(&path).expect("resolve precommit injection path"));
    let error =
        publish_history_repair_replacement(home.path(), path.as_path(), replacement.as_slice())
            .await
            .expect_err("precommit failure");
    assert!(error.to_string().contains("precommit"));
    assert_eq!(
        tokio::fs::read(path.as_path()).await.expect("old source"),
        source
    );

    POSTCOMMIT_SYNC_FAILURES
        .lock()
        .expect("postcommit mutex")
        .insert(std::fs::canonicalize(&path).expect("resolve postcommit injection path"));
    let publication =
        publish_history_repair_replacement(home.path(), path.as_path(), replacement.as_slice())
            .await
            .expect("visible publication");
    let HistoryRepairPublication::DurabilityUnknown { error } = publication else {
        panic!("expected unknown durability");
    };
    assert!(error.to_string().contains("postcommit"));
    assert_eq!(
        tokio::fs::read(path).await.expect("new source"),
        replacement
    );
}

#[tokio::test]
async fn recovery_cleans_a_crash_left_stage_before_a_clean_runtime_return() {
    let _crash_guard = std::sync::Arc::clone(&CRASH_TEST_LOCK).lock_owned().await;
    let home = TempDir::new().expect("temp home");
    let path = home.path().join("rollout.jsonl");
    let source = active_source(ThreadId::new(), "old");
    let thread_id = thread_id_from_source(source.as_slice());
    let replacement = repaired_active_replacement(source.as_slice(), b"old", b"new");
    prepare_active_source(&home, path.as_path(), source.as_slice()).await;
    inject_crash_boundary(
        path.as_path(),
        ConfinedCrashBoundary::AfterExchangeBeforeParentSync,
    );
    let publication =
        publish_history_repair_replacement(home.path(), path.as_path(), replacement.as_slice())
            .await
            .expect("visible publication");
    assert!(matches!(
        publication,
        HistoryRepairPublication::DurabilityUnknown { .. }
    ));
    assert!(repair_stages(home.path()).await > 0);

    recover_history_repair_publication_for_test(
        &TestHistoryRepairWriterToken { thread_id },
        home.path(),
        thread_id,
        path.as_path(),
    )
    .await
    .expect("recover publication before clean return");
    assert_eq!(repair_stages(home.path()).await, 0);
    assert_eq!(tokio::fs::read(path).await.unwrap(), replacement);
}

#[tokio::test]
async fn runtime_recovery_token_borrows_every_required_lock() {
    let home = TempDir::new().expect("temp home");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let path = home.path().join("rollout.jsonl");
    let source = active_source(ThreadId::new(), "clean");
    let thread_id = thread_id_from_source(source.as_slice());
    tokio::fs::write(path.as_path(), source)
        .await
        .expect("write clean source");
    let maintenance = reserve_history_repair_maintenance(&store)
        .await
        .expect("maintenance lock")
        .expect("maintenance lock available");
    let lifecycle = reserve_history_repair_lifecycle(&store, thread_id).await;
    let writers = store
        .reserve_rollout_writers(&[thread_id])
        .await
        .expect("writer reservation");
    let token =
        authorize_history_repair_writer(&store, thread_id, &maintenance, &lifecycle, &writers)
            .await
            .expect("authorize repair writer");

    recover_history_repair_publication(&token, home.path(), thread_id, path.as_path())
        .await
        .expect("recover clean publication under every lock");
}

#[tokio::test]
async fn runtime_recovery_accepts_clean_segmentless_rollout() {
    let home = TempDir::new().expect("temp home");
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let path = home.path().join("rollout.jsonl");
    let thread_id = ThreadId::new();
    let source = source_with_segment(thread_id, /*segment_id*/ None, b"");
    tokio::fs::write(path.as_path(), source.as_slice())
        .await
        .expect("write clean segmentless source");
    let maintenance = reserve_history_repair_maintenance(&store)
        .await
        .expect("maintenance lock")
        .expect("maintenance lock available");
    let lifecycle = reserve_history_repair_lifecycle(&store, thread_id).await;
    let writers = store
        .reserve_rollout_writers(&[thread_id])
        .await
        .expect("writer reservation");
    let token =
        authorize_history_repair_writer(&store, thread_id, &maintenance, &lifecycle, &writers)
            .await
            .expect("authorize repair writer");

    recover_history_repair_publication(&token, home.path(), thread_id, path.as_path())
        .await
        .expect("clean segmentless rollout needs no backup");
    assert_eq!(tokio::fs::read(path).await.expect("read source"), source);
}

async fn repair_stages(directory: &Path) -> usize {
    let mut entries = tokio::fs::read_dir(directory).await.unwrap();
    let mut count = 0;
    while let Some(entry) = entries.next_entry().await.unwrap() {
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(".codex-history-repair-")
        {
            count += 1;
        }
    }
    count
}

#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
async fn publication_rechecks_source_identity_immediately_before_commit() {
    let home = TempDir::new().expect("temp home");
    let path = home.path().join("rollout.jsonl");
    let source = active_source(ThreadId::new(), "old");
    let replacement = repaired_active_replacement(source.as_slice(), b"old", b"new");
    let concurrent = replace_ascii(source.clone(), b"old", b"alt");
    prepare_active_source(&home, path.as_path(), source.as_slice()).await;
    SOURCE_REPLACEMENTS
        .lock()
        .expect("source replacement mutex")
        .insert(
            std::fs::canonicalize(&path).expect("resolve source replacement path"),
            concurrent.clone(),
        );

    let error =
        publish_history_repair_replacement(home.path(), path.as_path(), replacement.as_slice())
            .await
            .expect_err("concurrent source replacement");
    assert!(error.to_string().contains("changed before publication"));
    assert_eq!(
        tokio::fs::read(path).await.expect("concurrent source"),
        concurrent
    );
}

#[tokio::test]
async fn publication_authenticates_every_existing_backup_representation() {
    let home = TempDir::new().expect("temp home");
    let path = home.path().join("rollout.jsonl");
    let source = active_source(ThreadId::new(), "old");
    let replacement = repaired_active_replacement(source.as_slice(), b"old", b"new");
    tokio::fs::write(path.as_path(), source.as_slice())
        .await
        .expect("write active source");
    let backup = install_active_backup(&home, path.as_path(), source.as_slice()).await;
    let compressed_backup = backup.with_extension("jsonl.zst");
    let different = replace_ascii(source.clone(), b"old", b"alt");
    tokio::fs::write(compressed_backup.as_path(), compress(different.as_slice()))
        .await
        .expect("write conflicting compressed backup");
    set_private_permissions(compressed_backup.as_path()).await;

    let error =
        publish_history_repair_replacement(home.path(), path.as_path(), replacement.as_slice())
            .await
            .expect_err("conflicting backup representation");
    assert!(error.to_string().contains("different contents"));
    assert_eq!(tokio::fs::read(path).await.expect("active source"), source);
}

#[tokio::test]
async fn publication_rejects_changed_record_boundaries() {
    let home = TempDir::new().expect("temp home");
    let path = home.path().join("rollout.jsonl");
    let (_, _, source) = immutable_source(ThreadId::new(), b"aa\nbb\n");
    let mut replacement = source.clone();
    replacement.truncate(replacement.len() - b"aa\nbb\n".len());
    replacement.extend_from_slice(b"a\nbbb\n");
    prepare_active_source(&home, path.as_path(), source.as_slice()).await;
    let error =
        publish_history_repair_replacement(home.path(), path.as_path(), replacement.as_slice())
            .await
            .expect_err("changed boundary");
    assert!(error.to_string().contains("record boundary"));
    assert_eq!(tokio::fs::read(path).await.expect("unchanged"), source);
}

#[tokio::test]
async fn publication_rejects_changed_rollout_semantics() {
    let home = TempDir::new().expect("temp home");
    let path = home.path().join("rollout.jsonl");
    let source = active_source(ThreadId::new(), "suffix");
    let replacement = repaired_active_replacement(
        source.as_slice(),
        b"2026-08-10T20:00:00Z",
        b"2026-08-11T20:00:00Z",
    );
    prepare_active_source(&home, path.as_path(), source.as_slice()).await;
    let error =
        publish_history_repair_replacement(home.path(), path.as_path(), replacement.as_slice())
            .await
            .expect_err("timestamp change");
    assert!(
        error
            .to_string()
            .contains("changed persisted rollout semantics")
    );
    assert_eq!(tokio::fs::read(path).await.expect("source bytes"), source);
}

#[tokio::test]
async fn publication_preserves_each_rejected_and_blank_physical_record() {
    let home = TempDir::new().expect("temp home");
    let path = home.path().join("rollout.jsonl");
    let (_, _, source) = immutable_source(ThreadId::new(), b"bad-one\n\n");
    let replacement = repaired_active_replacement(source.as_slice(), b"bad-one", b"bad-two");
    prepare_active_source(&home, path.as_path(), source.as_slice()).await;
    let error =
        publish_history_repair_replacement(home.path(), path.as_path(), replacement.as_slice())
            .await
            .expect_err("rejected record mutation");
    assert!(
        error
            .to_string()
            .contains("changed persisted rollout semantics")
    );
    assert_eq!(tokio::fs::read(path).await.expect("source bytes"), source);
}

#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
async fn compressed_publication_atomically_replaces_the_compressed_source() {
    let home = TempDir::new().expect("temp home");
    let compressed_path = home.path().join("rollout.jsonl.zst");
    let source = active_source(ThreadId::new(), "alpha");
    let replacement = repaired_active_replacement(source.as_slice(), b"alpha", b"ALPHA");
    let _ = install_active_backup(&home, compressed_path.as_path(), source.as_slice()).await;
    let compressed = compress(source.as_slice());
    tokio::fs::write(compressed_path.as_path(), compressed)
        .await
        .expect("write compressed source");

    let publication = publish_compressed_history_repair_replacement(
        home.path(),
        compressed_path.as_path(),
        replacement.as_slice(),
    )
    .await
    .expect("publish materialized repair");
    assert!(matches!(publication, HistoryRepairPublication::Durable));
    let compressed_result = tokio::fs::read(compressed_path.as_path())
        .await
        .expect("read compressed repair");
    assert_eq!(
        zstd::stream::decode_all(compressed_result.as_slice()).expect("decode repair"),
        replacement
    );
    assert!(
        !tokio::fs::try_exists(home.path().join("rollout.jsonl"))
            .await
            .unwrap()
    );
}

#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
async fn compressed_publication_reports_unknown_after_visible_replacement() {
    let home = TempDir::new().expect("temp home");
    let compressed_path = home.path().join("rollout.jsonl.zst");
    let source = active_source(ThreadId::new(), "alpha");
    let replacement = repaired_active_replacement(source.as_slice(), b"alpha", b"ALPHA");
    let _ = install_active_backup(&home, compressed_path.as_path(), source.as_slice()).await;
    let compressed = compress(source.as_slice());
    tokio::fs::write(compressed_path.as_path(), compressed)
        .await
        .expect("write compressed source");
    POSTCOMMIT_SYNC_FAILURES
        .lock()
        .expect("postcommit mutex")
        .insert(
            std::fs::canonicalize(&compressed_path)
                .expect("resolve compressed postcommit injection path"),
        );

    let publication = publish_compressed_history_repair_replacement(
        home.path(),
        compressed_path.as_path(),
        replacement.as_slice(),
    )
    .await
    .expect("visible compressed publication");
    assert!(matches!(
        publication,
        HistoryRepairPublication::DurabilityUnknown { .. }
    ));
    let visible = tokio::fs::read(compressed_path.as_path())
        .await
        .expect("compressed repair");
    assert_eq!(
        zstd::stream::decode_all(visible.as_slice()).expect("decode repair"),
        replacement
    );
    assert!(
        !tokio::fs::try_exists(home.path().join("rollout.jsonl"))
            .await
            .unwrap()
    );
}

#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
async fn compressed_publication_retry_recognizes_the_visible_repair() {
    let home = TempDir::new().expect("temp home");
    let compressed_path = home.path().join("rollout.jsonl.zst");
    let source = active_source(ThreadId::new(), "alpha");
    let replacement = repaired_active_replacement(source.as_slice(), b"alpha", b"ALPHA");
    let _ = install_active_backup(&home, compressed_path.as_path(), source.as_slice()).await;
    tokio::fs::write(compressed_path.as_path(), compress(source.as_slice()))
        .await
        .expect("write compressed source");
    POSTCOMMIT_SYNC_FAILURES
        .lock()
        .expect("postcommit mutex")
        .insert(
            std::fs::canonicalize(&compressed_path)
                .expect("resolve compressed postcommit injection path"),
        );

    let first = publish_compressed_history_repair_replacement(
        home.path(),
        compressed_path.as_path(),
        replacement.as_slice(),
    )
    .await
    .expect("first publication");
    assert!(matches!(
        first,
        HistoryRepairPublication::DurabilityUnknown { .. }
    ));
    let visible = tokio::fs::read(compressed_path.as_path())
        .await
        .expect("visible compressed repair");
    assert_eq!(
        zstd::stream::decode_all(visible.as_slice()).expect("decode repair"),
        replacement
    );

    let second = publish_compressed_history_repair_replacement(
        home.path(),
        compressed_path.as_path(),
        replacement.as_slice(),
    )
    .await
    .expect("retry publication");
    assert!(matches!(second, HistoryRepairPublication::Durable));
    let visible = tokio::fs::read(compressed_path.as_path())
        .await
        .expect("visible compressed repair");
    assert_eq!(
        zstd::stream::decode_all(visible.as_slice()).expect("decode repair"),
        replacement
    );
    assert!(
        !tokio::fs::try_exists(home.path().join("rollout.jsonl"))
            .await
            .unwrap()
    );
}

#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
async fn compressed_publication_preserves_a_source_replaced_before_exchange() {
    let home = TempDir::new().expect("temp home");
    let compressed_path = home.path().join("rollout.jsonl.zst");
    let source = active_source(ThreadId::new(), "alpha");
    let replacement = repaired_active_replacement(source.as_slice(), b"alpha", b"ALPHA");
    let _ = install_active_backup(&home, compressed_path.as_path(), source.as_slice()).await;
    let compressed = zstd::stream::encode_all(source.as_slice(), 0).expect("compress source");
    tokio::fs::write(compressed_path.as_path(), compressed)
        .await
        .expect("write compressed source");
    let concurrent_source = active_source(ThreadId::new(), "other");
    let concurrent_compressed = compress(concurrent_source.as_slice());
    SOURCE_REPLACEMENTS
        .lock()
        .expect("compressed replacement mutex")
        .insert(
            std::fs::canonicalize(&compressed_path)
                .expect("resolve compressed source replacement path"),
            concurrent_compressed.clone(),
        );

    let error = publish_compressed_history_repair_replacement(
        home.path(),
        compressed_path.as_path(),
        replacement.as_slice(),
    )
    .await
    .expect_err("changed compressed source");
    assert!(error.to_string().contains("changed before publication"));
    assert_eq!(
        tokio::fs::read(compressed_path)
            .await
            .expect("concurrent compressed source"),
        concurrent_compressed
    );
    assert!(
        !tokio::fs::try_exists(home.path().join("rollout.jsonl"))
            .await
            .unwrap()
    );
    let mut decoder = zstd::stream::read::Decoder::new(concurrent_compressed.as_slice())
        .expect("decode concurrent source");
    let mut visible = Vec::new();
    decoder
        .read_to_end(&mut visible)
        .expect("read concurrent source");
    assert_eq!(visible, concurrent_source);
}

#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
async fn active_publication_reports_unknown_if_the_published_name_is_replaced() {
    let home = TempDir::new().expect("temp home");
    let path = home.path().join("rollout.jsonl");
    let source = active_source(ThreadId::new(), "old");
    let replacement = repaired_active_replacement(source.as_slice(), b"old", b"new");
    prepare_active_source(&home, path.as_path(), source.as_slice()).await;
    let concurrent = b"concurrent replacement\n".to_vec();
    PUBLISHED_REPLACEMENTS
        .lock()
        .expect("published replacement mutex")
        .insert(
            std::fs::canonicalize(&path).expect("resolve published replacement path"),
            concurrent.clone(),
        );

    let publication =
        publish_history_repair_replacement(home.path(), path.as_path(), replacement.as_slice())
            .await
            .expect("publication outcome");
    assert!(matches!(
        publication,
        HistoryRepairPublication::DurabilityUnknown { .. }
    ));
    assert_eq!(
        tokio::fs::read(path).await.expect("current name"),
        concurrent
    );
}

#[tokio::test]
async fn compressed_publication_never_overwrites_existing_plain_rollout() {
    let home = TempDir::new().expect("temp home");
    let compressed_path = home.path().join("rollout.jsonl.zst");
    let plain_path = home.path().join("rollout.jsonl");
    let source = active_source(ThreadId::new(), "old");
    let replacement = repaired_active_replacement(source.as_slice(), b"old", b"new");
    let _ = install_active_backup(&home, compressed_path.as_path(), source.as_slice()).await;
    let compressed = compress(source.as_slice());
    tokio::fs::write(compressed_path.as_path(), compressed)
        .await
        .expect("write compressed source");
    tokio::fs::write(plain_path.as_path(), b"concurrent writer\n")
        .await
        .expect("write concurrent plain source");

    let error = publish_compressed_history_repair_replacement(
        home.path(),
        compressed_path.as_path(),
        replacement.as_slice(),
    )
    .await
    .expect_err("existing plain source");
    assert!(error.to_string().contains("already exists"));
    assert_eq!(
        tokio::fs::read(plain_path).await.expect("plain bytes"),
        b"concurrent writer\n"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn compressed_publication_rejects_a_dangling_plain_sibling() {
    use std::os::unix::fs::symlink;

    let home = TempDir::new().expect("temp home");
    let compressed_path = home.path().join("rollout.jsonl.zst");
    let plain_path = home.path().join("rollout.jsonl");
    let source = active_source(ThreadId::new(), "alpha");
    let replacement = repaired_active_replacement(source.as_slice(), b"alpha", b"ALPHA");
    let _ = install_active_backup(&home, compressed_path.as_path(), source.as_slice()).await;
    tokio::fs::write(compressed_path.as_path(), compress(source.as_slice()))
        .await
        .expect("write compressed source");
    symlink(
        home.path().join("missing-rollout.jsonl"),
        plain_path.as_path(),
    )
    .expect("create dangling plain sibling");

    let error = publish_compressed_history_repair_replacement(
        home.path(),
        compressed_path.as_path(),
        replacement.as_slice(),
    )
    .await
    .expect_err("dangling plain sibling must fail closed");
    assert!(error.to_string().contains("plain rollout"));
    let visible = tokio::fs::read(compressed_path).await.unwrap();
    assert_eq!(
        zstd::stream::decode_all(visible.as_slice()).unwrap(),
        source
    );
    assert!(tokio::fs::symlink_metadata(plain_path).await.is_ok());
}

#[tokio::test]
async fn segmentless_source_fails_closed_without_touching_history_cache_or_creating_artifacts() {
    let home = TempDir::new().expect("temp home");
    let path = home.path().join("rollout.jsonl");
    let thread_id = ThreadId::new();
    let source = source_with_segment(thread_id, /*segment_id*/ None, b"poison\n");
    let replacement = replace_ascii(source.clone(), b"poison", b"repair");
    tokio::fs::write(path.as_path(), source.as_slice())
        .await
        .expect("write segmentless source");
    let cache = home.path().join("thread_history_1.sqlite");
    tokio::fs::write(cache.as_path(), b"existing cache bytes")
        .await
        .expect("write cache sentinel");

    let error =
        publish_history_repair_replacement(home.path(), path.as_path(), replacement.as_slice())
            .await
            .expect_err("segmentless history must fail closed");
    assert!(error.to_string().contains("segment_id=None"));
    assert_eq!(tokio::fs::read(path).await.expect("source bytes"), source);
    assert_eq!(
        tokio::fs::read(cache).await.expect("cache bytes"),
        b"existing cache bytes"
    );
    assert!(
        !tokio::fs::try_exists(
            home.path()
                .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        )
        .await
        .expect("artifact existence")
    );
}

#[cfg(unix)]
#[tokio::test]
async fn legacy_initial_confinement_accepts_only_home_aliases() {
    use std::os::unix::fs::symlink;

    let home = TempDir::new().expect("temp home");
    let outside = TempDir::new().expect("outside");
    let thread_id = ThreadId::new();
    let external = outside.path().join("rollout.jsonl");
    tokio::fs::write(external.as_path(), b"unchanged\n")
        .await
        .expect("write outside source");
    let error = validate_legacy_initial_repair_path(home.path(), thread_id, external.as_path())
        .await
        .expect_err("external path");
    assert!(error.to_string().contains("outside CODEX_HOME"));

    let thread_root = home
        .path()
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string());
    tokio::fs::create_dir_all(thread_root.as_path())
        .await
        .expect("create thread root");
    symlink(outside.path(), thread_root.join("initial")).expect("symlink initial");
    let linked = thread_root.join("initial").join("rollout.jsonl");
    let error = validate_legacy_initial_repair_path(home.path(), thread_id, linked.as_path())
        .await
        .expect_err("symlinked initial");
    assert!(error.to_string().contains("not a real directory"));

    std::fs::remove_file(thread_root.join("initial")).expect("remove symlinked initial");
    tokio::fs::create_dir(thread_root.join("initial"))
        .await
        .expect("create real initial directory");
    tokio::fs::write(linked.as_path(), b"unchanged\n")
        .await
        .expect("write initial source");
    let alias = outside.path().join("codex-home");
    symlink(home.path(), alias.as_path()).expect("symlink CODEX_HOME");
    let canonical = tokio::fs::canonicalize(linked.as_path())
        .await
        .expect("canonical initial source");
    let aliased = alias.join(linked.strip_prefix(home.path()).expect("relative initial"));
    for path in [aliased, canonical.clone()] {
        assert_eq!(
            validate_legacy_initial_repair_path(alias.as_path(), thread_id, path.as_path())
                .await
                .expect("configured and resolved home aliases are accepted"),
            canonical,
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn active_publication_rejects_external_and_symlinked_sources() {
    use std::os::unix::fs::symlink;

    let home = TempDir::new().expect("temp home");
    let outside = TempDir::new().expect("outside");
    let (_, _, source) = immutable_source(ThreadId::new(), b"old\n");
    let replacement = replace_ascii(source.clone(), b"old", b"new");
    let external = outside.path().join("rollout.jsonl");
    tokio::fs::write(external.as_path(), source.as_slice())
        .await
        .expect("write external source");
    let error =
        publish_history_repair_replacement(home.path(), external.as_path(), replacement.as_slice())
            .await
            .expect_err("external source");
    assert!(error.to_string().contains("outside CODEX_HOME"));
    assert_eq!(
        tokio::fs::read(external.as_path())
            .await
            .expect("external bytes"),
        source
    );

    let linked = home.path().join("linked.jsonl");
    symlink(external.as_path(), linked.as_path()).expect("link source");
    let error =
        publish_history_repair_replacement(home.path(), linked.as_path(), replacement.as_slice())
            .await
            .expect_err("symlinked source");
    assert!(error.to_string().contains("not a real file"));
    assert_eq!(
        tokio::fs::read(external).await.expect("external target"),
        source
    );
}

#[cfg(unix)]
#[tokio::test]
async fn active_publication_supports_a_symlinked_codex_home() {
    use std::os::unix::fs::symlink;

    let physical = TempDir::new().expect("physical home");
    let parent = TempDir::new().expect("link parent");
    let linked_home = parent.path().join(".codex");
    symlink(physical.path(), linked_home.as_path()).expect("symlink CODEX_HOME");
    let path = linked_home.join("rollout.jsonl");
    let source = active_source(ThreadId::new(), "alpha");
    let replacement = repaired_active_replacement(source.as_slice(), b"alpha", b"ALPHA");
    tokio::fs::write(path.as_path(), source.as_slice())
        .await
        .expect("write source");
    install_existing_identity_history_repair_backup(
        linked_home.as_path(),
        thread_id_from_source(source.as_slice()),
        segment_id_from_source(source.as_slice()),
        path.as_path(),
        source.as_slice(),
    )
    .await
    .expect("install source backup");

    let publication = publish_history_repair_replacement(
        linked_home.as_path(),
        path.as_path(),
        replacement.as_slice(),
    )
    .await
    .expect("publish through symlinked home");
    assert!(matches!(publication, HistoryRepairPublication::Durable));
    assert_eq!(tokio::fs::read(path).await.unwrap(), replacement);

    let compressed_path = linked_home.join("compressed-rollout.jsonl.zst");
    let compressed_source = active_source(ThreadId::new(), "bravo");
    let compressed_replacement =
        repaired_active_replacement(compressed_source.as_slice(), b"bravo", b"BRAVO");
    install_existing_identity_history_repair_backup(
        linked_home.as_path(),
        thread_id_from_source(compressed_source.as_slice()),
        segment_id_from_source(compressed_source.as_slice()),
        compressed_path.as_path(),
        compressed_source.as_slice(),
    )
    .await
    .expect("install compressed source backup");
    tokio::fs::write(
        compressed_path.as_path(),
        compress(compressed_source.as_slice()),
    )
    .await
    .expect("write compressed source");

    let publication = publish_compressed_history_repair_replacement(
        linked_home.as_path(),
        compressed_path.as_path(),
        compressed_replacement.as_slice(),
    )
    .await
    .expect("publish compressed through symlinked home");
    assert!(matches!(publication, HistoryRepairPublication::Durable));
    let visible = tokio::fs::read(compressed_path).await.unwrap();
    assert_eq!(
        zstd::stream::decode_all(visible.as_slice()).unwrap(),
        compressed_replacement
    );
}

#[cfg(unix)]
#[tokio::test]
async fn canonical_sources_publish_and_recover_under_a_home_alias() {
    use std::os::unix::fs::symlink;

    for compressed in [false, true] {
        let physical = TempDir::new().expect("physical home");
        let parent = TempDir::new().expect("alias parent");
        let alias = parent.path().join("codex-home");
        symlink(physical.path(), &alias).expect("alias CODEX_HOME");
        let thread_id = ThreadId::new();
        let source = active_source(thread_id, "alpha");
        let replacement = repaired_active_replacement(&source, b"alpha", b"ALPHA");
        let source_path = alias.join(if compressed {
            "rollout.jsonl.zst"
        } else {
            "rollout.jsonl"
        });
        let stored = if compressed {
            compress(&source)
        } else {
            source.clone()
        };
        tokio::fs::write(&source_path, stored)
            .await
            .expect("write source");
        install_existing_identity_history_repair_backup(
            &alias,
            thread_id,
            segment_id_from_source(&source),
            &source_path,
            &source,
        )
        .await
        .expect("retain exact preimage");
        let canonical_source = tokio::fs::canonicalize(&source_path)
            .await
            .expect("canonical source returned by resolver");
        let store = LocalThreadStore::new(test_config(&alias), /*state_db*/ None);
        let maintenance = reserve_history_repair_maintenance(&store)
            .await
            .expect("reserve maintenance")
            .expect("maintenance lease");
        let lifecycle = reserve_history_repair_lifecycle(&store, thread_id).await;
        let writers = store
            .reserve_rollout_writers(&[thread_id])
            .await
            .expect("reserve writer");
        let writer =
            authorize_history_repair_writer(&store, thread_id, &maintenance, &lifecycle, &writers)
                .await
                .expect("authorize bound home");

        let publication = if compressed {
            publish_compressed_history_repair_replacement_impl(
                &writer,
                &alias,
                &canonical_source,
                &replacement,
            )
            .await
        } else {
            publish_history_repair_replacement_impl(
                &writer,
                &alias,
                &canonical_source,
                &replacement,
            )
            .await
        }
        .expect("publish canonical source under aliased home");
        assert!(matches!(publication, HistoryRepairPublication::Durable));
        recover_history_repair_publication(&writer, &alias, thread_id, &canonical_source)
            .await
            .expect("recover canonical source under aliased home");
        let stored = tokio::fs::read(&source_path)
            .await
            .expect("read caller-visible source");
        let restored = if compressed {
            zstd::stream::decode_all(stored.as_slice()).expect("decode repaired source")
        } else {
            stored
        };
        assert_eq!(restored, replacement);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn active_publication_binds_one_symlinked_codex_home_target() {
    use std::os::unix::fs::symlink;

    let first = TempDir::new().expect("first physical home");
    let second = TempDir::new().expect("second physical home");
    let parent = TempDir::new().expect("link parent");
    let linked_home = parent.path().join(".codex");
    symlink(first.path(), linked_home.as_path()).expect("symlink first CODEX_HOME");
    let linked_path = linked_home.join("rollout.jsonl");
    let first_path = first.path().join("rollout.jsonl");
    let second_path = second.path().join("rollout.jsonl");
    let source = active_source(ThreadId::new(), "alpha");
    let replacement = repaired_active_replacement(source.as_slice(), b"alpha", b"ALPHA");
    tokio::fs::write(first_path.as_path(), source.as_slice())
        .await
        .unwrap();
    tokio::fs::write(second_path.as_path(), source.as_slice())
        .await
        .unwrap();
    install_existing_identity_history_repair_backup(
        linked_home.as_path(),
        thread_id_from_source(source.as_slice()),
        segment_id_from_source(source.as_slice()),
        linked_path.as_path(),
        source.as_slice(),
    )
    .await
    .expect("install source backup");
    CODEX_HOME_RETARGETS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(linked_home.clone(), second.path().to_path_buf());

    let publication = publish_history_repair_replacement(
        linked_home.as_path(),
        linked_path.as_path(),
        replacement.as_slice(),
    )
    .await
    .expect("publish through bound first home");
    assert!(matches!(publication, HistoryRepairPublication::Durable));
    assert_eq!(tokio::fs::read(first_path).await.unwrap(), replacement);
    assert_eq!(tokio::fs::read(second_path).await.unwrap(), source);
}
