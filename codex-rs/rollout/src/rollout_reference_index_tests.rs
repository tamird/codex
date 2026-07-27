use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutReferenceItem;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use uuid::Uuid;

use super::RolloutReferenceIndex;
use crate::RolloutItem;
use crate::RolloutLine;

#[tokio::test]
async fn scans_active_archived_and_compressed_history_bases() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let source_id = thread_id(Uuid::from_u128(1))?;
    let active_base = history_position(source_id);
    let archived_base = history_position(source_id);
    let compressed_base = history_position(source_id);
    let active_child_id = thread_id(Uuid::from_u128(2))?;
    let archived_child_id = thread_id(Uuid::from_u128(3))?;
    let compressed_child_id = thread_id(Uuid::from_u128(4))?;

    write_rollout(
        active_rollout_path(home.path(), Uuid::from_u128(2)),
        active_child_id,
        Some(active_base),
    )?;
    write_rollout(
        archived_rollout_path(home.path(), Uuid::from_u128(3)),
        archived_child_id,
        Some(archived_base),
    )?;
    let compressed_path = archived_rollout_path(home.path(), Uuid::from_u128(4));
    write_rollout(
        compressed_path.clone(),
        compressed_child_id,
        Some(compressed_base),
    )?;
    compress_now(compressed_path.as_path())?;

    let index = RolloutReferenceIndex::scan(home.path()).await?;

    assert_eq!(index.reference_count(source_id), 3);
    assert_eq!(index.history_base(active_child_id), Some(&active_base));
    assert_eq!(index.history_base(archived_child_id), Some(&archived_base));
    assert_eq!(
        index.history_base(compressed_child_id),
        Some(&compressed_base)
    );
    Ok(())
}

#[tokio::test]
async fn active_duplicate_wins_history_base_lookup_while_direct_edges_are_unioned()
-> anyhow::Result<()> {
    let home = TempDir::new()?;
    let active_source_id = thread_id(Uuid::from_u128(8))?;
    let archived_source_id = thread_id(Uuid::from_u128(9))?;
    let child_uuid = Uuid::from_u128(10);
    let child_id = thread_id(Uuid::from_u128(10))?;
    let active_base = history_position(active_source_id);
    write_rollout(
        active_rollout_path(home.path(), child_uuid),
        child_id,
        Some(active_base),
    )?;
    write_rollout(
        archived_rollout_path(home.path(), child_uuid),
        child_id,
        Some(history_position(archived_source_id)),
    )?;

    let index = RolloutReferenceIndex::scan(home.path()).await?;

    assert_eq!(index.history_base(child_id), Some(&active_base));
    assert_eq!(index.reference_count(active_source_id), 1);
    assert_eq!(index.reference_count(archived_source_id), 1);
    Ok(())
}

#[tokio::test]
async fn indexes_multiple_rollouts_for_the_same_thread() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let source_rollout_id = thread_id(Uuid::from_u128(21))?;
    let replacement_rollout_id = thread_id(Uuid::from_u128(22))?;
    let thread_id = thread_id(Uuid::from_u128(20))?;
    let history_base = history_position(source_rollout_id);
    write_rollout(
        active_rollout_path(home.path(), Uuid::from_u128(21)),
        thread_id,
        Some(history_base),
    )?;
    write_rollout(
        active_rollout_path(home.path(), Uuid::from_u128(22)),
        thread_id,
        Some(history_base),
    )?;

    let index = RolloutReferenceIndex::scan(home.path()).await?;
    assert_eq!(index.history_base(source_rollout_id), Some(&history_base));
    assert_eq!(
        index.history_base(replacement_rollout_id),
        Some(&history_base)
    );
    assert_eq!(index.reference_count(source_rollout_id), 1);
    Ok(())
}

#[tokio::test]
async fn self_history_base_does_not_count_as_reference() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let thread_id = thread_id(Uuid::from_u128(11))?;
    let history_base = history_position(thread_id);
    write_rollout(
        active_rollout_path(home.path(), Uuid::from_u128(11)),
        thread_id,
        Some(history_base),
    )?;

    let index = RolloutReferenceIndex::scan(home.path()).await?;

    assert_eq!(index.history_base(thread_id), Some(&history_base));
    assert_eq!(index.reference_count(thread_id), 0);
    Ok(())
}

#[tokio::test]
async fn leading_rollout_reference_counts_physical_target() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let stable_source_id = thread_id(Uuid::from_u128(31))?;
    let physical_source_id = thread_id(Uuid::from_u128(32))?;
    let child_id = thread_id(Uuid::from_u128(33))?;
    let child_path = active_rollout_path(home.path(), Uuid::from_u128(33));
    write_rollout(child_path.clone(), child_id, /*history_base*/ None)?;
    let reference = RolloutLine {
        timestamp: "2025-01-03T12:00:00Z".to_string(),
        ordinal: Some(1),
        item: RolloutItem::RolloutReference(RolloutReferenceItem {
            rollout_path: active_rollout_path(home.path(), Uuid::from_u128(32)),
            thread_id: Some(stable_source_id),
            rollout_id: Some(physical_source_id),
            rollout_timestamp: None,
            segment_id: None,
            max_depth: 2,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        }),
    };
    let mut file = fs::OpenOptions::new().append(true).open(child_path)?;
    use std::io::Write;
    writeln!(file, "{}", serde_json::to_string(&reference)?)?;

    let index = RolloutReferenceIndex::scan(home.path()).await?;
    assert_eq!(index.reference_count(physical_source_id), 1);
    assert_eq!(index.reference_count(stable_source_id), 0);
    assert_eq!(
        index.direct_references(child_id),
        Some(&std::collections::HashSet::from([physical_source_id]))
    );
    Ok(())
}

#[tokio::test]
async fn duplicate_physical_rollouts_union_direct_references_once() -> anyhow::Result<()> {
    let home = TempDir::new()?;
    let child_uuid = Uuid::from_u128(41);
    let child_id = thread_id(child_uuid)?;
    let first_target = thread_id(Uuid::from_u128(42))?;
    let second_target = thread_id(Uuid::from_u128(43))?;
    let active = active_rollout_path(home.path(), child_uuid);
    let archived = archived_rollout_path(home.path(), child_uuid);
    write_rollout(active.clone(), child_id, None)?;
    write_rollout(archived.clone(), child_id, None)?;
    append_reference(&active, Some(first_target), Some(first_target))?;
    append_reference(&archived, Some(second_target), Some(second_target))?;

    let index = RolloutReferenceIndex::scan(home.path()).await?;
    assert_eq!(index.reference_count(first_target), 1);
    assert_eq!(index.reference_count(second_target), 1);
    assert_eq!(
        index.direct_references(child_id),
        Some(&std::collections::HashSet::from([
            first_target,
            second_target
        ]))
    );
    Ok(())
}

#[tokio::test]
async fn duplicate_physical_rollouts_union_history_bases_and_exact_sibling_references()
-> anyhow::Result<()> {
    let home = TempDir::new()?;
    let child_uuid = Uuid::from_u128(61);
    let child_id = thread_id(child_uuid)?;
    let first_target = thread_id(Uuid::from_u128(62))?;
    let second_target = thread_id(Uuid::from_u128(63))?;
    let third_target = thread_id(Uuid::from_u128(64))?;
    let active = active_rollout_path(home.path(), child_uuid);
    let archived = archived_rollout_path(home.path(), child_uuid);
    write_rollout(
        active.clone(),
        child_id,
        Some(history_position(first_target)),
    )?;
    write_rollout(
        archived.clone(),
        child_id,
        Some(history_position(second_target)),
    )?;
    let archived_compressed = archived.with_extension("jsonl.zst");
    append_reference(&archived, Some(third_target), Some(third_target))?;
    compress_copy(&archived, &archived_compressed)?;
    write_rollout(
        archived.clone(),
        child_id,
        Some(history_position(second_target)),
    )?;
    append_reference(&archived, Some(second_target), Some(second_target))?;

    let index = RolloutReferenceIndex::scan(home.path()).await?;
    assert_eq!(index.reference_count(first_target), 1);
    assert_eq!(index.reference_count(second_target), 1);
    assert_eq!(index.reference_count(third_target), 1);
    Ok(())
}

#[tokio::test]
async fn leading_legacy_reference_falls_back_to_thread_id_and_ignores_self_count()
-> anyhow::Result<()> {
    let home = TempDir::new()?;
    let target_id = thread_id(Uuid::from_u128(51))?;
    let child_id = thread_id(Uuid::from_u128(52))?;
    let child = active_rollout_path(home.path(), Uuid::from_u128(52));
    write_rollout(child.clone(), child_id, None)?;
    append_reference(&child, Some(target_id), None)?;
    let self_child = active_rollout_path(home.path(), Uuid::from_u128(53));
    let self_id = thread_id(Uuid::from_u128(53))?;
    write_rollout(self_child.clone(), self_id, None)?;
    append_reference(&self_child, Some(self_id), None)?;

    let index = RolloutReferenceIndex::scan(home.path()).await?;
    assert_eq!(index.reference_count(target_id), 1);
    assert_eq!(index.reference_count(self_id), 0);
    Ok(())
}

#[tokio::test]
async fn expired_deadline_returns_none() -> anyhow::Result<()> {
    let home = TempDir::new()?;

    let index =
        RolloutReferenceIndex::scan_until(home.path(), Instant::now(), Duration::ZERO).await?;

    assert!(index.is_none());
    Ok(())
}

fn active_rollout_path(home: &Path, uuid: Uuid) -> PathBuf {
    home.join("sessions/2025/01/03")
        .join(format!("rollout-2025-01-03T12-00-00-{uuid}.jsonl"))
}

fn archived_rollout_path(home: &Path, uuid: Uuid) -> PathBuf {
    home.join("archived_sessions")
        .join(format!("rollout-2025-01-03T12-00-00-{uuid}.jsonl"))
}

fn write_rollout(
    path: PathBuf,
    thread_id: ThreadId,
    history_base: Option<HistoryPosition>,
) -> anyhow::Result<()> {
    fs::create_dir_all(path.parent().expect("rollout parent"))?;
    let session_meta = serde_json::json!({
        "timestamp": "2025-01-03T12:00:00Z",
        "type": "session_meta",
        "payload": {
            "id": thread_id,
            "timestamp": "2025-01-03T12:00:00Z",
            "cwd": path.parent().expect("rollout parent"),
            "originator": "test",
            "cli_version": "test",
            "source": "cli",
            "model_provider": "test-provider",
            "history_mode": "paginated",
            "history_base": history_base,
        },
    });
    fs::write(path, format!("{session_meta}\n"))?;
    Ok(())
}

fn history_position(rollout_id: ThreadId) -> HistoryPosition {
    HistoryPosition {
        thread_id: rollout_id,
        end_ordinal_exclusive: 2,
        end_byte_offset: 100,
    }
}

fn append_reference(
    path: &Path,
    thread_id: Option<ThreadId>,
    rollout_id: Option<ThreadId>,
) -> anyhow::Result<()> {
    use std::io::Write;
    let reference = RolloutLine {
        timestamp: "2025-01-03T12:00:00Z".to_string(),
        ordinal: Some(1),
        item: RolloutItem::RolloutReference(RolloutReferenceItem {
            rollout_path: path.to_path_buf(),
            thread_id,
            rollout_id,
            rollout_timestamp: None,
            segment_id: None,
            max_depth: 2,
            nth_user_message: None,
            compacted_replacement_history_filter_texts: None,
        }),
    };
    writeln!(
        fs::OpenOptions::new().append(true).open(path)?,
        "{}",
        serde_json::to_string(&reference)?
    )?;
    Ok(())
}

fn thread_id(uuid: Uuid) -> anyhow::Result<ThreadId> {
    Ok(ThreadId::from_string(&uuid.to_string())?)
}

fn compress_now(path: &Path) -> anyhow::Result<()> {
    let compressed_path = path.with_extension("jsonl.zst");
    let input = fs::File::open(path)?;
    let output = fs::File::create(compressed_path)?;
    let mut encoder = zstd::stream::write::Encoder::new(output, 3)?;
    std::io::copy(&mut std::io::BufReader::new(input), &mut encoder)?;
    encoder.finish()?;
    fs::remove_file(path)?;
    Ok(())
}

fn compress_copy(path: &Path, compressed_path: &Path) -> anyhow::Result<()> {
    let input = fs::File::open(path)?;
    let output = fs::File::create(compressed_path)?;
    let mut encoder = zstd::stream::write::Encoder::new(output, 3)?;
    std::io::copy(&mut std::io::BufReader::new(input), &mut encoder)?;
    encoder.finish()?;
    Ok(())
}
