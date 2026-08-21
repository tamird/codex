//! Validation of immutable fork snapshots after the receiver acquires its own source lease.

use super::LocalThreadStore;
use super::model_context::MAX_INTERACTIVE_MODEL_CONTEXT_RECORD_BYTES;
use super::model_context::MAX_INTERACTIVE_MODEL_CONTEXT_SCAN_BYTES;
use crate::FrozenRolloutSegment;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use codex_protocol::ThreadId;
use codex_rollout::ReverseJsonlScanner;
use codex_rollout::ScanOutcome;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::sync::Arc;

impl LocalThreadStore {
    /// Freezes a legacy source while carrying one lifecycle lease through its repair and snapshot.
    pub async fn prepare_legacy_fork_handoff(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<crate::PreparedFork> {
        let source =
            super::thread_rollout_resolver::resolve_current_including_archived(self, thread_id)
                .await?
                .ok_or(crate::ThreadStoreError::ThreadNotFound { thread_id })?;
        let mut access =
            super::goal_supervisor_runtime_repair::repair_recent_history_before_access(
                self,
                thread_id,
                &source.path,
            )
            .await?;
        let lifecycle = access
            .take_lifecycle(thread_id)
            .ok_or_else(|| invalid("source lifecycle reservation is missing"))?;
        let writer = access
            .writer_reservation()
            .ok_or_else(|| invalid("source writer reservation is missing"))?;
        let frozen = super::segment::freeze_thread_segment_reserved(
            self,
            thread_id,
            crate::FreezeRolloutSegmentParams::snapshot(),
            Some(source.rollout_id),
            writer,
        )
        .await?;
        drop(access);
        let context = Arc::new(
            codex_rollout::materialize_model_context_rollout_items_from(
                &self.config.codex_home,
                vec![
                    codex_rollout::RolloutLine {
                        timestamp: String::new(),
                        ordinal: None,
                        item: codex_rollout::RolloutItem::SessionMeta(
                            frozen.source_session_meta.clone(),
                        ),
                    },
                    codex_rollout::RolloutLine {
                        timestamp: String::new(),
                        ordinal: None,
                        item: codex_rollout::RolloutItem::RolloutReference(
                            frozen.reference.clone(),
                        ),
                    },
                ],
            )
            .await
            .map_err(invalid)?,
        );
        Ok(crate::PreparedFork::new(
            thread_id,
            /*history_base*/ None,
            Some(frozen),
            Arc::clone(&context),
            Arc::clone(&context),
            context,
            /*interrupt_if_open*/ true,
            crate::ThreadLifecycleReservation::new(lifecycle),
        ))
    }

    /// Validates the captured physical prefix without repairing or reserving a mutable writer.
    /// The caller holds `reserve_imported_fork` until the child's reference becomes durable.
    pub async fn validate_imported_fork(
        &self,
        source_thread_id: ThreadId,
        frozen: &FrozenRolloutSegment,
    ) -> ThreadStoreResult<()> {
        let source = super::thread_rollout_resolver::resolve_current_including_archived(
            self,
            source_thread_id,
        )
        .await?
        .ok_or_else(|| invalid("source was deleted before the handoff was claimed"))?;
        if !tokio::fs::try_exists(&source.path).await.map_err(invalid)?
            || frozen.source_session_meta.meta.id != source_thread_id
        {
            return Err(invalid("source identity is missing or mismatched"));
        }
        let Some(boundary) = frozen.history_base else {
            codex_rollout::resolve_rollout_reference_path(
                &self.config.codex_home,
                &frozen.reference,
            )
            .await
            .map_err(invalid)?;
            return Ok(());
        };
        let path = super::helpers::scoped_rollout_path(
            self.config
                .codex_home
                .join(codex_rollout::SESSIONS_SUBDIR)
                .join(codex_rollout::ROLLOUT_SEGMENTS_SUBDIR),
            frozen.reference.rollout_path.as_path(),
            "immutable rollout segments",
        )?;
        let metadata = codex_rollout::read_session_meta_line(&path)
            .await
            .map_err(invalid)?;
        if codex_rollout::rollout_id_from_path(&path) != Some(boundary.thread_id)
            || metadata.meta.segment_id.is_some()
            || metadata.meta.history_mode != codex_protocol::protocol::ThreadHistoryMode::Paginated
            || frozen.reference.rollout_id != Some(boundary.thread_id)
            || frozen.reference.thread_id != Some(metadata.meta.id)
            || frozen.next_rollout_ordinal != Some(boundary.end_ordinal_exclusive)
            || tokio::fs::metadata(&path).await.map_err(invalid)?.len() != boundary.end_byte_offset
        {
            return Err(invalid("frozen identity or byte boundary does not match"));
        }
        // Native snapshots end at the captured boundary. A reverse scan verifies that boundary
        // without reading the full prefix or walking any predecessor, including foreign ancestors.
        let ordinal = tokio::task::spawn_blocking(move || {
            let mut file = std::fs::File::open(path)?;
            file.seek(io::SeekFrom::End(-1))?;
            let mut ending = [0];
            file.read_exact(&mut ending)?;
            if ending != *b"\n" {
                return Err(io::Error::other(
                    "frozen boundary is not a complete rollout record",
                ));
            }
            let mut scanner = ReverseJsonlScanner::new(file)?
                .with_max_record_bytes(MAX_INTERACTIVE_MODEL_CONTEXT_RECORD_BYTES);
            while let Some(outcome) = scanner.scan_next::<serde_json::Value>()? {
                if scanner.bytes_scanned() > MAX_INTERACTIVE_MODEL_CONTEXT_SCAN_BYTES
                    || scanner.oversized_records_skipped() != 0
                {
                    return Err(io::Error::other(
                        "frozen boundary exceeds the interactive scan limit",
                    ));
                }
                let value = match outcome {
                    ScanOutcome::Parsed(value) => value,
                    ScanOutcome::Rejected(error) => return Err(io::Error::other(error)),
                };
                let Some(line) = codex_rollout::RolloutRecorder::parse_rollout_line_value(value)
                    .map_err(io::Error::other)?
                else {
                    continue;
                };
                if let Some(ordinal) = line.ordinal {
                    return Ok(ordinal.checked_add(1));
                }
            }
            Ok(None)
        })
        .await
        .map_err(invalid)?
        .map_err(invalid)?;
        if ordinal != Some(boundary.end_ordinal_exclusive) {
            return Err(invalid("frozen ordinal boundary does not match"));
        }
        Ok(())
    }
}

fn invalid(error: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: format!("invalid fork handoff: {error}"),
    }
}
