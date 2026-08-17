//! Rewrites generated IDs in unpublished canonical files without replaying Legacy sources.
//!
//! The first replay records exact JSON scalar ranges. Explicit lookalike IDs are not recorded.
//! Files are rewritten oldest first because a changed file length changes its successor's
//! `history_base.end_byte_offset`. Only the final measurements enter the durable journal.

use std::collections::HashMap;
use std::io::SeekFrom;

use codex_protocol::RolloutId;
use serde::Deserialize;
use serde_json::value::RawValue;
use sha2::Digest;
use sha2::Sha256;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::BufWriter;

use super::lineage_stage::MeasuringWriter;
use super::lineage_stage::StagedLineageTarget;
use super::migration_error;
use super::publish::sync_parent_directory;
use crate::ThreadStoreResult;

/// An exact JSON string range allocated by `LegacyRolloutCanonicalizer::next_item_id`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GeneratedItemEdit {
    start: u64,
    end: u64,
    /// Distinguishes generated IDs from explicit lookalike IDs inherited by another turn.
    pub(super) turn_id: String,
    pub(super) item_id: String,
}

/// Borrows the generated completion's item without materializing its other fields.
#[derive(Deserialize)]
struct CompletedRecord<'a> {
    #[serde(borrow)]
    payload: CompletedPayload<'a>,
}

/// The `item_completed` event fields needed to locate its item.
#[derive(Deserialize)]
struct CompletedPayload<'a> {
    turn_id: String,
    #[serde(borrow)]
    item: CompletedItem<'a>,
}

/// Preserves the exact serialized ID token, including JSON escaping.
#[derive(Deserialize)]
struct CompletedItem<'a> {
    #[serde(borrow)]
    id: &'a RawValue,
}

impl GeneratedItemEdit {
    pub(super) fn from_record(
        bytes: &[u8],
        record_offset: u64,
        item_id: String,
    ) -> ThreadStoreResult<Self> {
        let record: CompletedRecord<'_> = serde_json::from_slice(bytes).map_err(migration_error)?;
        let raw = record.payload.item.id.get();
        if serde_json::from_str::<String>(raw).map_err(migration_error)? != item_id {
            return Err(migration_error(
                "generated item ID does not match canonical record",
            ));
        }
        let start = record_offset
            .checked_add((raw.as_ptr() as usize - bytes.as_ptr() as usize) as u64)
            .ok_or_else(|| migration_error("generated item ID offset overflow"))?;
        let end = start
            .checked_add(raw.len() as u64)
            .ok_or_else(|| migration_error("generated item ID range overflow"))?;
        Ok(Self {
            start,
            end,
            turn_id: record.payload.turn_id,
            item_id,
        })
    }
}

/// Borrows only the first record's predecessor coordinates.
#[derive(Deserialize)]
struct HeadRecord<'a> {
    #[serde(borrow)]
    payload: HeadPayload<'a>,
}

/// Session metadata may omit `history_base` at an external or filtered boundary.
#[derive(Deserialize)]
struct HeadPayload<'a> {
    #[serde(borrow)]
    history_base: Option<HistoryBase<'a>>,
}

/// Identifies the predecessor and the exact byte-count token that can change.
#[derive(Deserialize)]
struct HistoryBase<'a> {
    thread_id: RolloutId,
    #[serde(borrow)]
    end_byte_offset: &'a RawValue,
}

/// A verified replacement for one JSON scalar in the original staged bytes.
struct ScalarEdit {
    start: u64,
    expected: Vec<u8>,
    replacement: Vec<u8>,
}

pub(super) async fn rewrite_generated_item_ids(
    staged: &mut [StagedLineageTarget],
    remap: &HashMap<String, String>,
) -> ThreadStoreResult<()> {
    let mut previous: Option<(RolloutId, u64)> = None;
    for target in staged {
        let mut reader = BufReader::with_capacity(
            256 * 1024,
            tokio::fs::File::open(&target.staged_path)
                .await
                .map_err(migration_error)?,
        );
        let mut head = Vec::new();
        reader
            .read_until(b'\n', &mut head)
            .await
            .map_err(migration_error)?;
        let header: HeadRecord<'_> = serde_json::from_slice(&head).map_err(migration_error)?;
        let mut edits = Vec::new();
        if let (Some((previous_id, previous_bytes)), Some(base)) =
            (previous, header.payload.history_base)
        {
            if base.thread_id != previous_id {
                return Err(migration_error(
                    "staged history_base does not identify the previous target",
                ));
            }
            let old_bytes: u64 =
                serde_json::from_str(base.end_byte_offset.get()).map_err(migration_error)?;
            if old_bytes != previous_bytes {
                let raw = base.end_byte_offset.get();
                edits.push(ScalarEdit {
                    start: (raw.as_ptr() as usize - head.as_ptr() as usize) as u64,
                    expected: raw.as_bytes().to_vec(),
                    replacement: previous_bytes.to_string().into_bytes(),
                });
            }
        }
        for edit in &target.generated_item_edits {
            if let Some(replacement) = remap.get(&edit.item_id)
                && replacement != &edit.item_id
            {
                let expected = serde_json::to_vec(&edit.item_id).map_err(migration_error)?;
                if edit.end.checked_sub(edit.start) != Some(expected.len() as u64) {
                    return Err(migration_error("generated item ID range has changed"));
                }
                edits.push(ScalarEdit {
                    start: edit.start,
                    expected,
                    replacement: serde_json::to_vec(replacement).map_err(migration_error)?,
                });
            }
        }
        if !edits.is_empty() {
            reader
                .seek(SeekFrom::Start(0))
                .await
                .map_err(migration_error)?;
            let rewritten = target.staged_path.with_extension("jsonl.rewrite");
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&rewritten)
                .await
                .map_err(migration_error)?;
            file.set_permissions(
                reader
                    .get_ref()
                    .metadata()
                    .await
                    .map_err(migration_error)?
                    .permissions(),
            )
            .await
            .map_err(migration_error)?;
            let mut writer = MeasuringWriter::new(BufWriter::with_capacity(256 * 1024, file));
            let mut hasher = Sha256::new();
            let mut buffer = vec![0_u8; 256 * 1024];
            let mut position = 0_u64;
            for edit in edits {
                let count = edit
                    .start
                    .checked_sub(position)
                    .ok_or_else(|| migration_error("overlapping generated item ID ranges"))?;
                copy_bytes(&mut reader, &mut writer, count, &mut hasher, &mut buffer).await?;
                let mut original = vec![0_u8; edit.expected.len()];
                reader
                    .read_exact(&mut original)
                    .await
                    .map_err(migration_error)?;
                if original != edit.expected {
                    return Err(migration_error("staged JSON scalar changed before rewrite"));
                }
                hasher.update(&original);
                writer
                    .write_all(&edit.replacement)
                    .await
                    .map_err(migration_error)?;
                position = edit.start + original.len() as u64;
            }
            let remaining = target
                .byte_count
                .checked_sub(position)
                .ok_or_else(|| migration_error("generated item ID range exceeds staged file"))?;
            copy_bytes(
                &mut reader,
                &mut writer,
                remaining,
                &mut hasher,
                &mut buffer,
            )
            .await?;
            if reader
                .read(&mut buffer[..1])
                .await
                .map_err(migration_error)?
                != 0
                || format!("{:x}", hasher.finalize()) != target.sha256
            {
                return Err(migration_error(
                    "staged file changed before generated item ID rewrite",
                ));
            }
            writer.flush().await.map_err(migration_error)?;
            writer
                .inner
                .get_ref()
                .sync_all()
                .await
                .map_err(migration_error)?;
            let (byte_count, record_count, sha256) = writer.finish();
            if record_count != target.record_count {
                return Err(migration_error(
                    "generated item ID rewrite changed record count",
                ));
            }
            drop(reader);
            tokio::fs::rename(&rewritten, &target.staged_path)
                .await
                .map_err(migration_error)?;
            sync_parent_directory(&target.staged_path).await?;
            target.byte_count = byte_count;
            target.sha256 = sha256;
        }
        target.generated_item_edits.clear();
        previous = Some((target.rollout_id, target.byte_count));
    }
    Ok(())
}

async fn copy_bytes(
    reader: &mut BufReader<tokio::fs::File>,
    writer: &mut MeasuringWriter<BufWriter<tokio::fs::File>>,
    mut remaining: u64,
    hasher: &mut Sha256,
    buffer: &mut [u8],
) -> ThreadStoreResult<()> {
    while remaining != 0 {
        let length = remaining.min(buffer.len() as u64) as usize;
        let read = reader
            .read(&mut buffer[..length])
            .await
            .map_err(migration_error)?;
        if read == 0 {
            return Err(migration_error(
                "staged file ended before generated item ID range",
            ));
        }
        hasher.update(&buffer[..read]);
        writer
            .write_all(&buffer[..read])
            .await
            .map_err(migration_error)?;
        remaining -= read as u64;
    }
    Ok(())
}
