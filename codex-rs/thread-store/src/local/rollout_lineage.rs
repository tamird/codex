use std::collections::HashSet;
use std::io;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

use codex_app_server_protocol::ThreadItem;
use codex_protocol::RolloutId;
use codex_protocol::ThreadId;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::ModelContextScan;
use codex_rollout::ModelContextScanProgress;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_rollout::RolloutRecorder;

use super::LocalThreadStore;
use super::RolloutWriterReservation;
use super::goal_supervisor_history_repair::GoalSupervisorLineageProvenance;
use super::goal_supervisor_history_repair::repair_legacy_goal_supervisor_lines_with_provenance;
use super::thread_rollout_resolver;
use crate::StoredThreadItem;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

/// One immutable physical range contributing to a logical paginated history.
///
/// A thread may contribute multiple ranges after rollout rotation. Their ordinal ranges do not
/// overlap, so the existing `(physical_thread_id, rollout_ordinal)` cursor remains unambiguous.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RolloutLineageSegment {
    /// Stable thread identity from the segment's `SessionMeta`.
    pub(super) thread_id: ThreadId,
    /// Concrete rollout filename/projection identity.
    pub(super) rollout_id: ThreadId,
    pub(super) rollout_path: PathBuf,
    pub(super) start_ordinal: u64,
    pub(super) end_ordinal_exclusive: Option<u64>,
    /// End of the consumed decoded JSONL prefix.
    ///
    /// Repair publication uses this authoritative `HistoryPosition` after decoding the rollout.
    /// The reverse scanner's `end_byte_offset` also addresses decoded JSONL bytes, including for
    /// compressed files opened through the seekable rollout reader.
    pub(super) jsonl_end_byte_offset: Option<u64>,
    pub(super) end_byte_offset: Option<u64>,
    pub(super) filter_texts: Vec<String>,
    /// Provenance inherited before this physical segment's `SessionMeta` is applied.
    pub(super) goal_supervisor_provenance: GoalSupervisorLineageProvenance,
    /// Whether this segment was reached through upstream `SessionMeta.history_base`.
    pub(super) uses_history_base: bool,
    /// Whether this segment crosses a fork or user-message boundary.
    pub(super) uses_fork_boundary: bool,
}

/// Ordered physical rollout ranges contributing to one logical history.
///
/// `SessionMeta.history_base` is the canonical paginated persistence format. A same-thread edge is
/// a physical segment boundary; a cross-thread edge is a fork boundary. `RolloutReference`
/// remains a compatibility input for Legacy and older Frodex rollouts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RolloutLineage {
    /// Physical rollout selected for the logical thread when this lineage was resolved.
    ///
    /// Pagination cursors bind to this identity so replacing a thread's selected rollout cannot
    /// reinterpret an ordinal from the previous rollout as a position in the replacement.
    pub(super) root_rollout_id: RolloutId,
    pub(super) segments: Vec<RolloutLineageSegment>,
}

/// A clean same-thread lineage prepared in one bounded-buffer pass per physical segment.
pub(super) struct PreparedForkLineage {
    pub(super) lineage: RolloutLineage,
    pub(super) model_context: Vec<RolloutItem>,
    pub(super) full_history: Option<Vec<RolloutItem>>,
    pub(super) session_meta: SessionMetaLine,
    pub(super) source_projection_was_missing: bool,
}

/// One boundary-authenticated same-thread prefix for an indexed explicit fork.
///
/// The prefix records are parsed while verifying the SQLite byte and ordinal boundary. Fork
/// context reconstruction and immutable publication consume these records directly so neither
/// operation reopens the selected physical segment.
pub(super) struct BoundedSameThreadPrefix {
    pub(super) segment: RolloutLineageSegment,
    pub(super) lines: Vec<RolloutLine>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum LineageOffsetMode {
    Resolve,
    Deferred,
}

impl LocalThreadStore {
    pub(super) async fn resolve_rollout_lineage(
        &self,
        requested_thread_id: ThreadId,
    ) -> ThreadStoreResult<RolloutLineage> {
        let resolved =
            thread_rollout_resolver::resolve_current_including_archived(self, requested_thread_id)
                .await?
                .ok_or_else(|| malformed_lineage(requested_thread_id, "missing source rollout"))?;
        let mut active_paths = HashSet::new();
        let segments = resolve_path(
            self,
            requested_thread_id,
            resolved.rollout_id,
            resolved.path,
            /*end*/ None,
            /*inherited_filter_texts*/ None,
            /*graph_depth*/ 0,
            &mut active_paths,
            LineageOffsetMode::Resolve,
        )
        .await?;
        Ok(RolloutLineage {
            root_rollout_id: resolved.rollout_id,
            segments,
        })
    }

    /// Resolves the lineage rooted at one explicit physical rollout rather than the thread's
    /// currently selected rollout.
    pub(super) async fn resolve_rollout_lineage_from_path(
        &self,
        requested_thread_id: ThreadId,
        rollout_path: &Path,
    ) -> ThreadStoreResult<RolloutLineage> {
        let resolved_path = codex_rollout::existing_rollout_path(rollout_path)
            .await
            .ok_or_else(|| malformed_lineage(requested_thread_id, "missing source rollout"))?;
        let session_meta = codex_rollout::read_session_meta_line(resolved_path.as_path())
            .await
            .map_err(lineage_io_error)?;
        if session_meta.meta.id != requested_thread_id {
            return Err(malformed_lineage(
                requested_thread_id,
                "source rollout belongs to another thread",
            ));
        }
        let plain_path = codex_rollout::plain_rollout_path(resolved_path.as_path());
        let rollout_id = codex_rollout::rollout_id_from_path(plain_path.as_path())
            .unwrap_or(requested_thread_id);
        let mut active_paths = HashSet::new();
        let segments = resolve_path(
            self,
            requested_thread_id,
            rollout_id,
            resolved_path,
            /*end*/ None,
            /*inherited_filter_texts*/ None,
            /*graph_depth*/ 0,
            &mut active_paths,
            LineageOffsetMode::Resolve,
        )
        .await?;
        Ok(RolloutLineage {
            root_rollout_id: rollout_id,
            segments,
        })
    }

    pub(super) async fn resolve_rollout_lineage_for_reference(
        &self,
        requested_thread_id: ThreadId,
        expected_rollout_id: Option<codex_protocol::RolloutId>,
    ) -> ThreadStoreResult<(RolloutLineage, RolloutWriterReservation, bool)> {
        let mut thread_ids = vec![requested_thread_id];
        let mut source_projection_state = None;
        loop {
            let reservation = self.reserve_rollout_writers(thread_ids.as_slice()).await?;
            let mut source = thread_rollout_resolver::resolve_current_including_archived(
                self,
                requested_thread_id,
            )
            .await?;
            if source.is_none() {
                // A deferred empty thread has no rollout path until persistence runs. Materialize
                // only that missing source while its writer is reserved; persisting an existing
                // source here would repair stale projection state before fork fallback inspects it.
                match super::live_writer::persist_thread_reserved(self, requested_thread_id).await {
                    Ok(()) | Err(ThreadStoreError::ThreadNotFound { .. }) => {}
                    Err(error) => return Err(error),
                }
                source = thread_rollout_resolver::resolve_current_including_archived(
                    self,
                    requested_thread_id,
                )
                .await?;
            }
            let source = source
                .ok_or_else(|| malformed_lineage(requested_thread_id, "missing source rollout"))?;
            if expected_rollout_id.is_some_and(|expected| expected != source.rollout_id) {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!(
                        "rollout path does not select the current rollout for thread {requested_thread_id}"
                    ),
                });
            }
            super::helpers::scoped_rollout_path(
                self.config.codex_home.clone(),
                source.path.as_path(),
                "Codex home",
            )?;
            if source_projection_state
                .as_ref()
                .is_none_or(|(rollout_id, _)| *rollout_id != source.rollout_id)
            {
                source_projection_state = Some((
                    source.rollout_id,
                    super::thread_history::projection_state(self, source.rollout_id)
                        .await?
                        .is_none(),
                ));
            }
            let lineage = self.resolve_rollout_lineage(requested_thread_id).await?;
            let mut discovered_ids = lineage
                .segments
                .iter()
                .map(|segment| segment.thread_id)
                .collect::<Vec<_>>();
            discovered_ids.push(requested_thread_id);
            discovered_ids.sort_unstable_by_key(ThreadId::to_string);
            discovered_ids.dedup();
            if discovered_ids
                .iter()
                .all(|thread_id| reservation.contains(*thread_id))
            {
                let lineage = self
                    .materialize_rollout_lineage_for_reference(
                        requested_thread_id,
                        lineage,
                        &reservation,
                    )
                    .await?;
                return Ok((
                    lineage,
                    reservation,
                    source_projection_state
                        .map(|(_, was_missing)| was_missing)
                        .unwrap_or(true),
                ));
            }
            thread_ids = discovered_ids;
        }
    }

    /// Resolves and materializes a fork lineage while the caller retains every writer owner.
    pub(super) async fn resolve_rollout_lineage_for_reference_reserved(
        &self,
        requested_thread_id: ThreadId,
        expected_rollout_id: Option<codex_protocol::RolloutId>,
        reservation: &RolloutWriterReservation,
    ) -> ThreadStoreResult<(RolloutLineage, bool)> {
        let source =
            thread_rollout_resolver::resolve_current_including_archived(self, requested_thread_id)
                .await?
                .ok_or_else(|| malformed_lineage(requested_thread_id, "missing source rollout"))?;
        if expected_rollout_id.is_some_and(|expected| expected != source.rollout_id) {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!(
                    "rollout path does not select the current rollout for thread {requested_thread_id}"
                ),
            });
        }
        super::helpers::scoped_rollout_path(
            self.config.codex_home.clone(),
            source.path.as_path(),
            "Codex home",
        )?;
        let source_projection_was_missing =
            super::thread_history::projection_state(self, source.rollout_id)
                .await?
                .is_none();
        let lineage = self.resolve_rollout_lineage(requested_thread_id).await?;
        let mut discovered_ids = lineage
            .segments
            .iter()
            .map(|segment| segment.thread_id)
            .collect::<Vec<_>>();
        discovered_ids.push(requested_thread_id);
        discovered_ids.sort_unstable_by_key(ThreadId::to_string);
        discovered_ids.dedup();
        if let Some(unreserved) = discovered_ids
            .iter()
            .find(|thread_id| !reservation.contains(**thread_id))
        {
            return Err(ThreadStoreError::Conflict {
                message: format!("fork lineage discovered unreserved writer owner {unreserved}"),
            });
        }
        let lineage = self
            .materialize_rollout_lineage_for_reference(requested_thread_id, lineage, reservation)
            .await?;
        Ok((lineage, source_projection_was_missing))
    }

    /// Prepares a clean same-thread rotation lineage without repeating compatibility scans.
    ///
    /// The head pass authenticates the reference graph. The second pass keeps at most one physical
    /// segment in memory while validating the closed goal-supervisor compatibility invariant,
    /// calculating every byte boundary, and reconstructing model context. Histories that need
    /// repair or cross a fork or filter boundary return `None` and retain the existing
    /// compatibility-repair implementation. Same-thread `history_base` boundaries are exact
    /// ordinal and byte positions, so native segmented Paginated history uses this path.
    pub(super) async fn try_prepare_same_thread_fork_lineage_reserved(
        &self,
        requested_thread_id: ThreadId,
        expected_rollout_id: Option<RolloutId>,
        reservation: &RolloutWriterReservation,
        include_full_history: bool,
    ) -> ThreadStoreResult<Option<PreparedForkLineage>> {
        let source =
            thread_rollout_resolver::resolve_current_including_archived(self, requested_thread_id)
                .await?
                .ok_or_else(|| malformed_lineage(requested_thread_id, "missing source rollout"))?;
        if expected_rollout_id.is_some_and(|expected| expected != source.rollout_id) {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!(
                    "rollout path does not select the current rollout for thread {requested_thread_id}"
                ),
            });
        }
        super::helpers::scoped_rollout_path(
            self.config.codex_home.clone(),
            source.path.as_path(),
            "Codex home",
        )?;
        let source_projection_was_missing =
            super::thread_history::projection_state(self, source.rollout_id)
                .await?
                .is_none();
        let mut active_paths = HashSet::new();
        let segments = resolve_path(
            self,
            requested_thread_id,
            source.rollout_id,
            source.path,
            /*end*/ None,
            /*inherited_filter_texts*/ None,
            /*graph_depth*/ 0,
            &mut active_paths,
            LineageOffsetMode::Deferred,
        )
        .await?;
        if segments.iter().any(|segment| {
            segment.thread_id != requested_thread_id
                || segment.uses_fork_boundary
                || !segment.filter_texts.is_empty()
                || !reservation.contains(segment.thread_id)
        }) {
            return Ok(None);
        }
        let lineage = RolloutLineage {
            root_rollout_id: source.rollout_id,
            segments,
        };
        let Some((lineage, model_context, full_history, session_meta)) = self
            .materialize_clean_fork_lineage(
                requested_thread_id,
                lineage,
                reservation,
                include_full_history,
            )
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(PreparedForkLineage {
            lineage,
            model_context,
            full_history,
            session_meta,
            source_projection_was_missing,
        }))
    }

    async fn materialize_clean_fork_lineage(
        &self,
        requested_thread_id: ThreadId,
        mut lineage: RolloutLineage,
        reservation: &RolloutWriterReservation,
        include_full_history: bool,
    ) -> ThreadStoreResult<
        Option<(
            RolloutLineage,
            Vec<RolloutItem>,
            Option<Vec<RolloutItem>>,
            SessionMetaLine,
        )>,
    > {
        let source =
            thread_rollout_resolver::resolve_current_including_archived(self, requested_thread_id)
                .await?
                .ok_or_else(|| malformed_lineage(requested_thread_id, "missing source rollout"))?;
        let mut model_context_scan = ModelContextScan::default();
        let mut model_context_complete = false;
        let mut canonical_session_meta = None;
        let mut full_history_segments = include_full_history.then(Vec::new);

        for segment in lineage.segments.iter_mut().rev() {
            debug_assert!(reservation.contains(segment.thread_id));
            let rollout_path = codex_rollout::existing_rollout_path(segment.rollout_path.as_path())
                .await
                .unwrap_or_else(|| segment.rollout_path.clone());
            let rollout_path = super::helpers::scoped_rollout_path(
                self.config.codex_home.clone(),
                rollout_path.as_path(),
                "Codex home",
            )?;
            let materialized_path = if segment.rollout_id == source.rollout_id
                && rollout_is_standalone(rollout_path.as_path(), segment.thread_id).await?
            {
                codex_rollout::materialize_rollout_for_reference(rollout_path.as_path())
                    .await
                    .map_err(|err| ThreadStoreError::Internal {
                        message: format!(
                            "failed to materialize referenced rollout {}: {err}",
                            rollout_path.display()
                        ),
                    })?
            } else {
                rollout_path
            };
            let mut bytes = read_decoded_rollout(materialized_path.as_path()).await?;
            if let Some(end_ordinal_exclusive) = segment.end_ordinal_exclusive {
                if let Some(end_byte_offset) =
                    segment.jsonl_end_byte_offset.or(segment.end_byte_offset)
                {
                    // Validate the authenticated prefix before any repair or context projection.
                    let end = HistoryPosition {
                        thread_id: segment.rollout_id,
                        end_ordinal_exclusive,
                        end_byte_offset,
                    };
                    trim_segment_to_history_position_in_bytes(segment, end, bytes.as_slice())?;
                } else {
                    segment.end_byte_offset =
                        byte_offset_for_ordinal_in_bytes(bytes.as_slice(), end_ordinal_exclusive)?;
                    segment.jsonl_end_byte_offset = segment.end_byte_offset;
                }
            }
            if let Some(end_byte_offset) = segment.jsonl_end_byte_offset.or(segment.end_byte_offset)
            {
                let end_byte_offset = usize::try_from(end_byte_offset).map_err(|_| {
                    malformed_lineage(
                        segment.thread_id,
                        "rollout byte offset exceeds addressable memory",
                    )
                })?;
                if end_byte_offset > bytes.len() {
                    return Err(malformed_lineage(
                        segment.thread_id,
                        "cutoff byte offset is past the source rollout",
                    ));
                }
                bytes.truncate(end_byte_offset);
            }
            let parsed = parse_rollout_bytes(bytes.as_slice(), segment.thread_id)?;
            let mut lines = parsed
                .iter()
                .map(|(_, line)| line.clone())
                .collect::<Vec<_>>();
            let repair = repair_legacy_goal_supervisor_lines_with_provenance(
                lines.as_mut_slice(),
                segment.goal_supervisor_provenance,
            )
            .map_err(|error| ThreadStoreError::Internal {
                message: format!(
                    "failed to validate goal-supervisor history {}: {error}",
                    materialized_path.display()
                ),
            })?;
            if repair.total() != 0 {
                return Ok(None);
            }

            if let Some(full_history_segments) = full_history_segments.as_mut() {
                let mut segment_items = Vec::new();
                for line in &lines {
                    let Some(ordinal) = line.ordinal else {
                        continue;
                    };
                    if ordinal < segment.start_ordinal
                        || segment
                            .end_ordinal_exclusive
                            .is_some_and(|end| ordinal >= end)
                    {
                        continue;
                    }
                    let mut item = line.item.clone();
                    if matches!(
                        item,
                        RolloutItem::SessionMeta(_) | RolloutItem::RolloutReference(_)
                    ) || !segment.filter_rollout_item(&mut item)
                    {
                        continue;
                    }
                    segment_items.push(item);
                }
                full_history_segments.push(segment_items);
            }

            if canonical_session_meta.is_none() {
                canonical_session_meta = lines.iter().find_map(|line| match &line.item {
                    RolloutItem::SessionMeta(meta) if meta.meta.id == requested_thread_id => {
                        Some(meta.clone())
                    }
                    _ => None,
                });
            }
            if !model_context_complete {
                for line in lines.into_iter().rev() {
                    if let Some(ordinal) = line.ordinal
                        && (ordinal < segment.start_ordinal
                            || segment
                                .end_ordinal_exclusive
                                .is_some_and(|end| ordinal >= end))
                    {
                        continue;
                    }
                    let mut item = line.item;
                    if matches!(item, RolloutItem::SessionMeta(_)) {
                        break;
                    }
                    if matches!(item, RolloutItem::RolloutReference(_))
                        || !segment.filter_rollout_item(&mut item)
                    {
                        continue;
                    }
                    if matches!(
                        model_context_scan.push(item),
                        ModelContextScanProgress::Complete
                    ) {
                        model_context_complete = true;
                        break;
                    }
                }
            }

            if segment.end_ordinal_exclusive.is_none() {
                segment.end_ordinal_exclusive = parsed
                    .iter()
                    .filter_map(|(_, line)| line.ordinal)
                    .max()
                    .and_then(|ordinal| ordinal.checked_add(1));
            }

            let end_byte_offset = match segment.end_ordinal_exclusive {
                Some(end_ordinal_exclusive) => parsed
                    .iter()
                    .find_map(|(start, line)| {
                        line.ordinal
                            .is_some_and(|ordinal| ordinal >= end_ordinal_exclusive)
                            .then_some(*start)
                    })
                    .unwrap_or_else(|| u64::try_from(bytes.len()).unwrap_or(u64::MAX)),
                None => u64::try_from(bytes.len()).map_err(|_| {
                    malformed_lineage(segment.thread_id, "rollout byte offset overflow")
                })?,
            };
            segment.end_byte_offset = Some(end_byte_offset);
            segment.jsonl_end_byte_offset = Some(end_byte_offset);
            segment.rollout_path = materialized_path;
        }

        let canonical_session_meta = canonical_session_meta.ok_or_else(|| {
            malformed_lineage(
                requested_thread_id,
                "source rollout has no session metadata",
            )
        })?;
        let canonical_meta = canonical_session_meta.clone();
        let mut model_context = model_context_scan.finish(canonical_session_meta);
        if !matches!(model_context.first(), Some(RolloutItem::SessionMeta(_))) {
            model_context.insert(0, RolloutItem::SessionMeta(canonical_meta.clone()));
        }
        let full_history = full_history_segments.map(|segments| {
            let mut items = vec![RolloutItem::SessionMeta(canonical_meta.clone())];
            for segment in segments.into_iter().rev() {
                items.extend(segment);
            }
            items
        });
        Ok(Some((lineage, model_context, full_history, canonical_meta)))
    }

    async fn materialize_rollout_lineage_for_reference(
        &self,
        requested_thread_id: ThreadId,
        mut lineage: RolloutLineage,
        reservation: &RolloutWriterReservation,
    ) -> ThreadStoreResult<RolloutLineage> {
        let source =
            thread_rollout_resolver::resolve_current_including_archived(self, requested_thread_id)
                .await?
                .ok_or_else(|| malformed_lineage(requested_thread_id, "missing source rollout"))?;
        super::helpers::scoped_rollout_path(
            self.config.codex_home.clone(),
            source.path.as_path(),
            "Codex home",
        )?;
        if lineage.root_rollout_id != source.rollout_id {
            return Err(malformed_lineage(
                requested_thread_id,
                "selected rollout changed while reserving its lineage",
            ));
        }
        for segment in lineage.segments.iter_mut().rev() {
            debug_assert!(reservation.contains(segment.thread_id));
            let rollout_path = codex_rollout::existing_rollout_path(segment.rollout_path.as_path())
                .await
                .unwrap_or_else(|| segment.rollout_path.clone());
            let rollout_path = super::helpers::scoped_rollout_path(
                self.config.codex_home.clone(),
                rollout_path.as_path(),
                "Codex home",
            )?;
            let standalone =
                rollout_is_standalone(rollout_path.as_path(), segment.thread_id).await?;
            let materialized_path = if segment.rollout_id == source.rollout_id && standalone {
                // Newly shared standalone sources must remain readable by older binaries.
                codex_rollout::materialize_rollout_for_reference(rollout_path.as_path())
                    .await
                    .map_err(|err| ThreadStoreError::Internal {
                        message: format!(
                            "failed to materialize referenced rollout {}: {err}",
                            rollout_path.display()
                        ),
                    })?
            } else {
                // Already shared compressed ancestors remain immutable and read-only.
                rollout_path
            };
            segment.rollout_path = materialized_path;
            match segment.end_ordinal_exclusive {
                Some(end_ordinal_exclusive) => {
                    if let Some(end_byte_offset) =
                        segment.jsonl_end_byte_offset.or(segment.end_byte_offset)
                    {
                        // The authenticated prefix may end before later unordinaled records.
                        // Revalidate that exact boundary even for cross-thread ancestors whose
                        // canonical file is still under sessions/ or has since been compressed.
                        let end = HistoryPosition {
                            thread_id: segment.rollout_id,
                            end_ordinal_exclusive,
                            end_byte_offset,
                        };
                        trim_segment_to_history_position(segment, end).await?;
                    } else {
                        segment.end_byte_offset = byte_offset_for_ordinal(
                            segment.rollout_path.as_path(),
                            end_ordinal_exclusive,
                        )
                        .await?;
                        segment.jsonl_end_byte_offset = segment.end_byte_offset;
                    }
                }
                None => {
                    segment.end_byte_offset =
                        Some(decoded_rollout_len(segment.rollout_path.as_path()).await?);
                    segment.jsonl_end_byte_offset = segment.end_byte_offset;
                }
            }
        }
        Ok(lineage)
    }

    pub(super) async fn resolve_rollout_lineage_at(
        &self,
        end: HistoryPosition,
    ) -> ThreadStoreResult<RolloutLineage> {
        let rollout_path = resolve_rollout_path_by_id(self, end.thread_id)
            .await?
            .ok_or_else(|| malformed_lineage(end.thread_id, "missing source rollout"))?;
        let meta = codex_rollout::read_session_meta_line(rollout_path.as_path())
            .await
            .map_err(lineage_io_error)?;
        let mut active_paths = HashSet::new();
        let segments = resolve_path(
            self,
            meta.meta.id,
            end.thread_id,
            rollout_path,
            Some(end),
            /*inherited_filter_texts*/ None,
            /*graph_depth*/ 0,
            &mut active_paths,
            LineageOffsetMode::Resolve,
        )
        .await?;
        Ok(RolloutLineage {
            root_rollout_id: end.thread_id,
            segments,
        })
    }

    /// Resolves only the same-thread segment containing `end`.
    ///
    /// A complete SQLite projection already authenticates the logical turn boundary. Walking the
    /// leading immutable references from newest to oldest finds the containing physical segment
    /// without replaying unrelated predecessors. Histories that cross a fork, filter, or upstream
    /// `history_base` boundary return `None` and retain the compatibility implementation.
    pub(super) async fn resolve_bounded_same_thread_prefix_at(
        &self,
        requested_thread_id: ThreadId,
        expected_rollout_id: RolloutId,
        active_rollout_path: &Path,
        active_head: RolloutHead,
        end: HistoryPosition,
    ) -> ThreadStoreResult<Option<BoundedSameThreadPrefix>> {
        if end.thread_id != expected_rollout_id {
            return Ok(None);
        }
        let mut path = active_rollout_path.to_path_buf();
        let active_path = codex_rollout::existing_rollout_path(active_rollout_path).await;
        let mut active_head = Some(active_head);
        let mut visited = HashSet::new();
        let mut remaining_segments = codex_rollout::FRODEX_RECENT_ROLLOUT_SEGMENTS;
        loop {
            if remaining_segments == 0 {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!(
                        "the requested fork boundary is older than the recent {}-segment window; \
                         page or migrate the history before forking at that boundary",
                        codex_rollout::FRODEX_RECENT_ROLLOUT_SEGMENTS
                    ),
                });
            }
            remaining_segments -= 1;
            let Some(resolved_path) = codex_rollout::existing_rollout_path(path.as_path()).await
            else {
                return Ok(None);
            };
            if !visited.insert(resolved_path.clone()) {
                return Err(malformed_lineage(requested_thread_id, "cycle detected"));
            }
            super::helpers::scoped_rollout_path(
                self.config.codex_home.clone(),
                resolved_path.as_path(),
                "Codex home",
            )?;
            let head = if active_path.as_deref() == Some(resolved_path.as_path()) {
                active_head
                    .take()
                    .ok_or_else(|| malformed_lineage(requested_thread_id, "active rollout cycle"))?
            } else {
                read_rollout_head(resolved_path.as_path()).await?
            };
            if head.session_meta.meta.id != requested_thread_id
                || head.session_meta.meta.history_mode != ThreadHistoryMode::Paginated
                || head.session_meta.meta.forked_from_id.is_some()
                || head
                    .session_meta
                    .meta
                    .subagent_history_start_ordinal
                    .is_some()
            {
                return Ok(None);
            }
            if let Some(history_base) = head.session_meta.meta.history_base
                && end.end_ordinal_exclusive <= history_base.end_ordinal_exclusive
            {
                let Some(predecessor_path) =
                    resolve_rollout_path_by_id(self, history_base.thread_id).await?
                else {
                    return Ok(None);
                };
                let predecessor_meta =
                    codex_rollout::read_session_meta_line(predecessor_path.as_path())
                        .await
                        .map_err(lineage_io_error)?;
                if predecessor_meta.meta.id != requested_thread_id
                    || predecessor_meta.meta.history_mode != ThreadHistoryMode::Paginated
                {
                    return Ok(None);
                }
                path = predecessor_path;
                continue;
            }
            if let Some((reference_ordinal, reference)) = head.leading_reference.as_ref() {
                let predecessor_end = reference_ordinal.checked_sub(1).ok_or_else(|| {
                    malformed_lineage(
                        requested_thread_id,
                        "rollout reference precedes its session metadata",
                    )
                })?;
                if end.end_ordinal_exclusive <= predecessor_end {
                    if !canonical_same_thread_reference(
                        self.config.codex_home.as_path(),
                        active_rollout_path,
                        requested_thread_id,
                        expected_rollout_id,
                        reference,
                    ) {
                        return Ok(None);
                    }
                    path = codex_rollout::resolve_rollout_reference_path(
                        self.config.codex_home.as_path(),
                        reference,
                    )
                    .await
                    .map_err(lineage_io_error)?;
                    continue;
                }
            }

            if end.end_ordinal_exclusive < head.first_local_ordinal {
                return Ok(None);
            }
            let materialized_path = if active_path.as_deref() == Some(resolved_path.as_path())
                && rollout_is_standalone(resolved_path.as_path(), requested_thread_id).await?
            {
                codex_rollout::materialize_rollout_for_reference(resolved_path.as_path())
                    .await
                    .map_err(lineage_io_error)?
            } else {
                resolved_path
            };
            let bytes = read_decoded_rollout(materialized_path.as_path()).await?;
            let Some(expected_end_byte_offset) =
                byte_offset_for_ordinal_in_bytes(bytes.as_slice(), end.end_ordinal_exclusive)?
            else {
                return Ok(None);
            };
            if expected_end_byte_offset != end.end_byte_offset {
                return Ok(None);
            }
            let end_byte_offset = usize::try_from(end.end_byte_offset).map_err(|_| {
                malformed_lineage(
                    requested_thread_id,
                    "rollout byte offset exceeds addressable memory",
                )
            })?;
            let Some(prefix) = bytes.get(..end_byte_offset) else {
                return Ok(None);
            };
            if !prefix.ends_with(b"\n") {
                return Ok(None);
            }
            let lines = prefix
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
                .filter_map(
                    |line| match RolloutRecorder::parse_rollout_line_bytes(line) {
                        Ok(Some(line)) => Some(Ok(line)),
                        Ok(None) => None,
                        Err(error) => Some(Err(ThreadStoreError::Internal {
                            message: format!(
                                "malformed rollout lineage for {requested_thread_id}: selected \
                             rollout prefix contains an invalid record: {error}"
                            ),
                        })),
                    },
                )
                .collect::<ThreadStoreResult<Vec<_>>>()?;
            if lines
                .last()
                .and_then(|line| line.ordinal)
                .and_then(|ordinal| ordinal.checked_add(1))
                != Some(end.end_ordinal_exclusive)
            {
                return Ok(None);
            }
            return Ok(Some(BoundedSameThreadPrefix {
                segment: RolloutLineageSegment {
                    thread_id: requested_thread_id,
                    rollout_id: expected_rollout_id,
                    rollout_path: materialized_path,
                    start_ordinal: head.first_local_ordinal,
                    end_ordinal_exclusive: Some(end.end_ordinal_exclusive),
                    jsonl_end_byte_offset: Some(end.end_byte_offset),
                    end_byte_offset: Some(end.end_byte_offset),
                    filter_texts: Vec::new(),
                    goal_supervisor_provenance: GoalSupervisorLineageProvenance::Untrusted,
                    uses_history_base: false,
                    uses_fork_boundary: false,
                },
                lines,
            }));
        }
    }
}

/// Returns whether `reference` names the immutable predecessor created by same-thread rotation.
///
/// Logical fork references and upstream `history_base` entries use different lineage semantics.
/// Keeping the physical-layout check here ensures every bounded lineage consumer accepts the same
/// path, identity, and filter invariants.
pub(super) fn canonical_same_thread_reference(
    codex_home: &Path,
    active_rollout_path: &Path,
    thread_id: ThreadId,
    active_rollout_id: RolloutId,
    reference: &RolloutReferenceItem,
) -> bool {
    let Some(segment_id) = reference.segment_id else {
        return false;
    };
    let Some(file_name) = codex_rollout::plain_rollout_path(active_rollout_path)
        .file_name()
        .map(std::ffi::OsStr::to_owned)
    else {
        return false;
    };
    let expected_path = codex_home
        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
        .join(thread_id.to_string())
        .join(segment_id.to_string())
        .join(file_name);
    reference.thread_id == Some(thread_id)
        && reference.rollout_id.or(reference.thread_id) == Some(active_rollout_id)
        && reference.rollout_path == expected_path
        && reference.nth_user_message.is_none()
        && reference
            .compacted_replacement_history_filter_texts
            .is_none()
}

pub(super) async fn has_same_thread_history_base(
    store: &LocalThreadStore,
    head: &RolloutHead,
    thread_id: ThreadId,
) -> ThreadStoreResult<bool> {
    let Some(history_base) = head.session_meta.meta.history_base else {
        return Ok(false);
    };
    let Some(predecessor_path) = resolve_rollout_path_by_id(store, history_base.thread_id).await?
    else {
        return Ok(false);
    };
    let predecessor_meta = codex_rollout::read_session_meta_line(predecessor_path.as_path())
        .await
        .map_err(lineage_io_error)?;
    Ok(predecessor_meta.meta.id == thread_id)
}

async fn resolve_rollout_path_by_id(
    store: &LocalThreadStore,
    rollout_id: ThreadId,
) -> ThreadStoreResult<Option<PathBuf>> {
    let path = codex_rollout::find_rollout_path_by_rollout_id(
        store.config.codex_home.as_path(),
        rollout_id,
    )
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to locate rollout {rollout_id}: {err}"),
    })?;
    if path.is_some() {
        return Ok(path);
    }
    Ok(
        thread_rollout_resolver::resolve_current_including_archived(store, rollout_id)
            .await?
            .filter(|resolved| resolved.rollout_id == rollout_id)
            .map(|resolved| resolved.path),
    )
}

/// Reuses one filesystem index while following a native `history_base` chain.
///
/// Resolving every edge with an independent recursive filename scan makes startup projection
/// quadratic in the number of physical segments.
async fn resolve_indexed_rollout_path_by_id(
    store: &LocalThreadStore,
    rollout_id: ThreadId,
    index: &mut Option<std::collections::HashMap<ThreadId, PathBuf>>,
) -> ThreadStoreResult<Option<PathBuf>> {
    if index.is_none() {
        *index = Some(
            codex_rollout::index_rollout_paths_by_rollout_id(store.config.codex_home.as_path())
                .await
                .map_err(lineage_io_error)?,
        );
    }
    if let Some(path) = index.as_ref().and_then(|index| index.get(&rollout_id)) {
        return Ok(Some(path.to_path_buf()));
    }
    resolve_rollout_path_by_id(store, rollout_id).await
}

impl RolloutLineage {
    /// Applies one outer compatibility reference to an already-resolved historical parent.
    pub(super) async fn apply_reference_constraints(
        mut self,
        reference: &RolloutReferenceItem,
    ) -> ThreadStoreResult<Self> {
        for segment in &mut self.segments {
            segment.filter_texts =
                codex_rollout::compose_compacted_replacement_history_filter_texts(
                    reference
                        .compacted_replacement_history_filter_texts
                        .as_deref(),
                    Some(&segment.filter_texts),
                )
                .unwrap_or_default();
        }
        if let Some(nth) = reference.nth_user_message {
            trim_before_nth_user_message(&mut self.segments, nth, LineageOffsetMode::Resolve)
                .await?;
        }
        Ok(self)
    }
    pub(super) fn root_rollout_id(&self) -> RolloutId {
        self.root_rollout_id
    }

    pub(super) fn segments(&self) -> &[RolloutLineageSegment] {
        self.segments.as_slice()
    }

    /// Returns whether inherited filtering prevents an exact `HistoryPosition` boundary.
    pub(super) fn requires_copied_history(&self) -> bool {
        self.segments
            .iter()
            .any(RolloutLineageSegment::filters_items)
    }

    pub(super) fn segment_index_for_ordinal(&self, ordinal: u64) -> Option<usize> {
        self.segments
            .iter()
            .position(|segment| segment.contains_ordinal(ordinal))
    }

    pub(super) async fn truncate_at(
        mut self,
        end: HistoryPosition,
    ) -> ThreadStoreResult<RolloutLineage> {
        trim_to_history_position(&mut self.segments, end, LineageOffsetMode::Resolve).await?;
        Ok(self)
    }

    pub(super) fn segment_for_position(
        &self,
        rollout_id: ThreadId,
        rollout_ordinal: u64,
    ) -> ThreadStoreResult<&RolloutLineageSegment> {
        let matching_segments = self
            .segments
            .iter()
            .filter(|segment| {
                segment.rollout_id == rollout_id && segment.contains_ordinal(rollout_ordinal)
            })
            .collect::<Vec<_>>();
        match matching_segments.as_slice() {
            [segment] => Ok(*segment),
            [] => Err(malformed_lineage(rollout_id, "unknown physical segment")),
            [_, _, ..] => Err(malformed_lineage(rollout_id, "ambiguous physical segment")),
        }
    }
}

impl RolloutLineageSegment {
    pub(super) fn thread_id(&self) -> ThreadId {
        self.thread_id
    }

    pub(super) fn rollout_id(&self) -> ThreadId {
        self.rollout_id
    }

    pub(super) fn rollout_path(&self) -> &Path {
        self.rollout_path.as_path()
    }

    pub(super) fn end_byte_offset(&self) -> Option<u64> {
        self.end_byte_offset
    }

    /// Returns the consumed byte boundary in decoded JSONL coordinates.
    ///
    /// `None` means the complete decoded rollout is consumed.
    pub(super) fn jsonl_end_byte_offset(&self) -> Option<u64> {
        self.jsonl_end_byte_offset
    }

    pub(super) fn start_ordinal(&self) -> u64 {
        self.start_ordinal
    }

    pub(super) fn end_ordinal(&self) -> Option<u64> {
        self.end_ordinal_exclusive
    }

    pub(super) fn contains_ordinal(&self, ordinal: u64) -> bool {
        ordinal >= self.start_ordinal && self.end_ordinal_exclusive.is_none_or(|end| ordinal < end)
    }

    pub(super) fn allows_stored_item(&self, item: &StoredThreadItem) -> ThreadStoreResult<bool> {
        if self.filter_texts.is_empty() {
            return Ok(true);
        }
        let item = serde_json::from_slice::<ThreadItem>(&item.item_json).map_err(|err| {
            ThreadStoreError::Internal {
                message: format!("failed to read projected thread item: {err}"),
            }
        })?;
        Ok(self.allows_thread_item(&item))
    }

    pub(super) fn allows_thread_item(&self, item: &ThreadItem) -> bool {
        if self.filter_texts.is_empty() {
            return true;
        }
        let ThreadItem::RawResponseItem { item, .. } = item else {
            return true;
        };
        !matches_filtered_developer_message(item, self.filter_texts.as_slice())
    }

    pub(super) fn filters_items(&self) -> bool {
        !self.filter_texts.is_empty()
    }

    pub(super) fn filter_rollout_item(&self, item: &mut RolloutItem) -> bool {
        filter_rollout_item(item, self.filter_texts.as_slice())
    }
}

#[derive(Clone)]
pub(super) struct RolloutHead {
    pub(super) session_meta: SessionMetaLine,
    pub(super) session_meta_ordinal: Option<u64>,
    pub(super) leading_reference: Option<(u64, RolloutReferenceItem)>,
    pub(super) first_local_ordinal: u64,
    /// Whether the physical rollout contains a local record after its optional leading reference.
    pub(super) has_local_history: bool,
}

/// Defers a referenced source's ordinal and fork cutoffs until older segments are resolved.
struct PendingLineageReference {
    rollout_id: ThreadId,
    end_ordinal: u64,
    nth_user_message: Option<usize>,
}

/// Retains one physical segment until iterative traversal can reconstruct oldest-first order.
struct PendingLineageSegment {
    thread_id: ThreadId,
    rollout_id: ThreadId,
    rollout_path: PathBuf,
    first_local_ordinal: u64,
    filter_texts: Vec<String>,
    end: Option<HistoryPosition>,
    reference: Option<PendingLineageReference>,
    goal_supervisor_provenance: GoalSupervisorLineageProvenance,
    uses_history_base: bool,
    uses_fork_boundary: bool,
}

#[expect(
    clippy::too_many_arguments,
    reason = "the iterative lineage traversal carries each bounded resolution constraint explicitly"
)]
async fn resolve_path(
    store: &LocalThreadStore,
    expected_thread_id: ThreadId,
    expected_rollout_id: ThreadId,
    rollout_path: PathBuf,
    end: Option<HistoryPosition>,
    inherited_filter_texts: Option<Vec<String>>,
    graph_depth: usize,
    active_paths: &mut HashSet<PathBuf>,
    offset_mode: LineageOffsetMode,
) -> ThreadStoreResult<Vec<RolloutLineageSegment>> {
    let mut inserted_paths = Vec::new();
    let result = resolve_path_iteratively(
        store,
        expected_thread_id,
        expected_rollout_id,
        rollout_path,
        end,
        inherited_filter_texts,
        graph_depth,
        active_paths,
        &mut inserted_paths,
        offset_mode,
    )
    .await;
    for path in inserted_paths {
        active_paths.remove(&path);
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn resolve_path_iteratively(
    store: &LocalThreadStore,
    mut expected_thread_id: ThreadId,
    mut expected_rollout_id: ThreadId,
    mut rollout_path: PathBuf,
    mut end: Option<HistoryPosition>,
    mut inherited_filter_texts: Option<Vec<String>>,
    mut graph_depth: usize,
    active_paths: &mut HashSet<PathBuf>,
    inserted_paths: &mut Vec<PathBuf>,
    offset_mode: LineageOffsetMode,
) -> ThreadStoreResult<Vec<RolloutLineageSegment>> {
    let mut pending_segments = Vec::new();
    let mut goal_supervisor_provenance = GoalSupervisorLineageProvenance::Untrusted;
    let mut rollout_reference_index = None;
    let mut prefetched_head = None;

    loop {
        let resolved_path = codex_rollout::existing_rollout_path(rollout_path.as_path())
            .await
            .ok_or_else(|| malformed_lineage(expected_thread_id, "missing source rollout"))?;
        if codex_rollout::rollout_id_from_path(
            codex_rollout::plain_rollout_path(resolved_path.as_path()).as_path(),
        ) != Some(expected_rollout_id)
        {
            return Err(malformed_lineage(
                expected_rollout_id,
                "source rollout has another physical rollout id",
            ));
        }
        if !active_paths.insert(resolved_path.clone()) {
            return Err(malformed_lineage(expected_thread_id, "cycle detected"));
        }
        inserted_paths.push(resolved_path.clone());

        let head = match prefetched_head.take() {
            Some((prefetched_path, head)) if prefetched_path == resolved_path => head,
            _ => read_rollout_head(resolved_path.as_path()).await?,
        };
        if head.session_meta.meta.id != expected_thread_id {
            return Err(malformed_lineage(
                expected_thread_id,
                "source rollout belongs to another thread",
            ));
        }
        if head.session_meta.meta.history_mode != ThreadHistoryMode::Paginated {
            return Err(malformed_lineage(
                expected_thread_id,
                "source rollout is not paginated",
            ));
        }
        let segment_goal_supervisor_provenance = goal_supervisor_provenance;
        let current_goal_supervisor_provenance =
            goal_supervisor_provenance.continued_through_session_meta(&head.session_meta);

        if let Some((reference_ordinal, reference)) = head.leading_reference {
            let referenced_thread_id = reference.thread_id.ok_or_else(|| {
                malformed_lineage(expected_thread_id, "rollout reference is missing thread_id")
            })?;
            let fork_boundary =
                referenced_thread_id != expected_thread_id || reference.nth_user_message.is_some();
            if fork_boundary && graph_depth >= codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH {
                let detail = format!(
                    "rollout reference graph exceeds maximum depth of {}",
                    codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH
                );
                return Err(malformed_lineage(expected_thread_id, &detail));
            }
            let referenced_path = codex_rollout::resolve_rollout_reference_path(
                store.config.codex_home.as_path(),
                &reference,
            )
            .await
            .map_err(lineage_io_error)?;
            let referenced_end_ordinal = reference_ordinal.checked_sub(1).ok_or_else(|| {
                malformed_lineage(
                    expected_thread_id,
                    "rollout reference precedes its session metadata",
                )
            })?;
            let next_filter_texts =
                codex_rollout::compose_compacted_replacement_history_filter_texts(
                    inherited_filter_texts.as_deref(),
                    reference
                        .compacted_replacement_history_filter_texts
                        .as_deref(),
                );
            pending_segments.push(PendingLineageSegment {
                thread_id: expected_thread_id,
                rollout_id: expected_rollout_id,
                rollout_path: resolved_path,
                first_local_ordinal: head.first_local_ordinal,
                filter_texts: inherited_filter_texts.unwrap_or_default(),
                end,
                reference: Some(PendingLineageReference {
                    rollout_id: reference.rollout_id.unwrap_or(referenced_thread_id),
                    end_ordinal: referenced_end_ordinal,
                    nth_user_message: reference.nth_user_message,
                }),
                goal_supervisor_provenance: segment_goal_supervisor_provenance,
                uses_history_base: false,
                uses_fork_boundary: fork_boundary,
            });
            expected_thread_id = referenced_thread_id;
            expected_rollout_id = reference.rollout_id.unwrap_or(referenced_thread_id);
            rollout_path = referenced_path;
            end = None;
            inherited_filter_texts = next_filter_texts;
            goal_supervisor_provenance =
                current_goal_supervisor_provenance.continued_through_reference(&reference);
            graph_depth += usize::from(fork_boundary);
            continue;
        }

        if let Some(history_base) = head.session_meta.meta.history_base {
            let source_path = resolve_indexed_rollout_path_by_id(
                store,
                history_base.thread_id,
                &mut rollout_reference_index,
            )
            .await?
            .ok_or_else(|| malformed_lineage(history_base.thread_id, "missing source rollout"))?;
            let source_head = read_rollout_head(source_path.as_path()).await?;
            let fork_boundary = source_head.session_meta.meta.id != expected_thread_id;
            if fork_boundary && graph_depth >= codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH {
                let detail = format!(
                    "rollout reference graph exceeds maximum depth of {}",
                    codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH
                );
                return Err(malformed_lineage(expected_thread_id, &detail));
            }
            let next_filter_texts = inherited_filter_texts.clone();
            pending_segments.push(PendingLineageSegment {
                thread_id: expected_thread_id,
                rollout_id: expected_rollout_id,
                rollout_path: resolved_path,
                first_local_ordinal: head.first_local_ordinal,
                filter_texts: inherited_filter_texts.unwrap_or_default(),
                end,
                reference: None,
                goal_supervisor_provenance: segment_goal_supervisor_provenance,
                uses_history_base: true,
                uses_fork_boundary: fork_boundary,
            });
            expected_thread_id = source_head.session_meta.meta.id;
            expected_rollout_id = history_base.thread_id;
            rollout_path = source_path.clone();
            prefetched_head = Some((source_path, source_head));
            end = Some(history_base);
            inherited_filter_texts = next_filter_texts;
            goal_supervisor_provenance = GoalSupervisorLineageProvenance::Untrusted;
            graph_depth += usize::from(fork_boundary);
            continue;
        }

        pending_segments.push(PendingLineageSegment {
            thread_id: expected_thread_id,
            rollout_id: expected_rollout_id,
            rollout_path: resolved_path,
            first_local_ordinal: head.first_local_ordinal,
            filter_texts: inherited_filter_texts.unwrap_or_default(),
            end,
            reference: None,
            goal_supervisor_provenance: segment_goal_supervisor_provenance,
            uses_history_base: false,
            uses_fork_boundary: false,
        });
        break;
    }

    let mut segments = Vec::with_capacity(pending_segments.len());
    while let Some(pending) = pending_segments.pop() {
        if let Some(reference) = pending.reference {
            trim_to_ordinal(
                &mut segments,
                reference.rollout_id,
                reference.end_ordinal,
                offset_mode,
            )
            .await?;
            if let Some(nth_user_message) = reference.nth_user_message {
                trim_before_nth_user_message(&mut segments, nth_user_message, offset_mode).await?;
            }
        }

        let jsonl_end_byte_offset = match offset_mode {
            LineageOffsetMode::Resolve => {
                Some(decoded_rollout_len(pending.rollout_path.as_path()).await?)
            }
            LineageOffsetMode::Deferred => None,
        };
        segments.push(RolloutLineageSegment {
            thread_id: pending.thread_id,
            rollout_id: pending.rollout_id,
            rollout_path: pending.rollout_path,
            start_ordinal: pending.first_local_ordinal,
            end_ordinal_exclusive: None,
            jsonl_end_byte_offset,
            end_byte_offset: jsonl_end_byte_offset,
            filter_texts: pending.filter_texts,
            goal_supervisor_provenance: pending.goal_supervisor_provenance,
            uses_history_base: pending.uses_history_base,
            uses_fork_boundary: pending.uses_fork_boundary,
        });
        if let Some(end) = pending.end {
            trim_to_history_position(&mut segments, end, offset_mode).await?;
        }
    }

    Ok(segments)
}

pub(super) async fn read_rollout_head(path: &Path) -> ThreadStoreResult<RolloutHead> {
    let mut reader = codex_rollout::open_rollout_line_reader(path)
        .await
        .map_err(lineage_io_error)?;
    let first = next_rollout_line(&mut reader)
        .await?
        .ok_or_else(|| malformed_lineage(ThreadId::default(), "source rollout is empty"))?;
    let RolloutItem::SessionMeta(session_meta) = first.item else {
        return Err(malformed_lineage(
            ThreadId::default(),
            "source rollout does not start with session metadata",
        ));
    };
    let empty_local_start = session_meta
        .meta
        .history_base
        .map_or(first.ordinal.unwrap_or(0), |base| {
            base.end_ordinal_exclusive
        })
        .checked_add(1)
        .ok_or_else(|| malformed_lineage(session_meta.meta.id, "source ordinal overflow"))?;
    let next = next_rollout_line(&mut reader).await?;
    let (leading_reference, first_local_ordinal, has_local_history) = match next {
        Some(RolloutLine {
            ordinal: Some(ordinal),
            item: RolloutItem::RolloutReference(reference),
            ..
        }) => {
            let has_local_history = match next_rollout_line(&mut reader).await? {
                Some(RolloutLine {
                    item: RolloutItem::SessionMeta(_) | RolloutItem::RolloutReference(_),
                    ..
                }) => {
                    return Err(malformed_lineage(
                        session_meta.meta.id,
                        "source rollout contains a non-leading metadata or reference record",
                    ));
                }
                Some(_) => true,
                None => false,
            };
            (Some((ordinal, reference)), ordinal, has_local_history)
        }
        Some(line) => (None, line.ordinal.unwrap_or(1), true),
        None => (None, empty_local_start, false),
    };
    Ok(RolloutHead {
        session_meta,
        session_meta_ordinal: first.ordinal,
        leading_reference,
        first_local_ordinal,
        has_local_history,
    })
}

pub(super) async fn rollout_is_standalone(
    path: &Path,
    expected_thread_id: ThreadId,
) -> ThreadStoreResult<bool> {
    let head = read_rollout_head(path).await?;
    if head.session_meta.meta.id != expected_thread_id {
        return Err(malformed_lineage(
            expected_thread_id,
            "source rollout belongs to another thread",
        ));
    }
    Ok(head.session_meta.meta.history_base.is_none() && head.leading_reference.is_none())
}

/// Returns the ordinal of an active segment's leading same-thread reference.
///
/// A projection created from only the active segment can otherwise look current by byte offset
/// even though it omitted every visible turn from the referenced immutable segments.
pub(super) async fn leading_same_thread_reference_ordinal(
    path: &Path,
    thread_id: ThreadId,
) -> ThreadStoreResult<Option<u64>> {
    let head = read_rollout_head(path).await?;
    Ok(head
        .leading_reference
        .filter(|(_, reference)| {
            reference.thread_id == Some(thread_id) && reference.nth_user_message.is_none()
        })
        .map(|(ordinal, _)| ordinal))
}

async fn next_rollout_line(
    reader: &mut codex_rollout::RolloutLineReader,
) -> ThreadStoreResult<Option<RolloutLine>> {
    while let Some(line) = reader.next_line().await.map_err(lineage_io_error)? {
        if line.trim().is_empty() {
            continue;
        }
        let parsed = RolloutRecorder::parse_rollout_line_bytes(line.as_bytes()).map_err(|err| {
            ThreadStoreError::Internal {
                message: format!("failed to read paginated rollout line: {err}"),
            }
        })?;
        if parsed.is_some() {
            return Ok(parsed);
        }
    }
    Ok(None)
}

fn parse_rollout_bytes(
    bytes: &[u8],
    thread_id: ThreadId,
) -> ThreadStoreResult<Vec<(u64, RolloutLine)>> {
    let mut parsed = Vec::new();
    let mut offset = 0_u64;
    for physical_line in bytes.split_inclusive(|byte| *byte == b'\n') {
        let start = offset;
        offset = offset
            .checked_add(
                u64::try_from(physical_line.len())
                    .map_err(|_| malformed_lineage(thread_id, "rollout byte offset overflow"))?,
            )
            .ok_or_else(|| malformed_lineage(thread_id, "rollout byte offset overflow"))?;
        if physical_line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let value =
            serde_json::from_slice::<serde_json::Value>(physical_line).map_err(|error| {
                malformed_lineage(
                    thread_id,
                    format!("source rollout contains invalid JSON: {error}").as_str(),
                )
            })?;
        let Some(line) = RolloutRecorder::parse_rollout_line_value(value).map_err(|error| {
            malformed_lineage(
                thread_id,
                format!("source rollout contains an invalid record: {error}").as_str(),
            )
        })?
        else {
            continue;
        };
        parsed.push((start, line));
    }
    Ok(parsed)
}

async fn trim_to_history_position(
    segments: &mut Vec<RolloutLineageSegment>,
    end: HistoryPosition,
    offset_mode: LineageOffsetMode,
) -> ThreadStoreResult<()> {
    // This function validates the recorded byte boundary below, so asking `trim_to_ordinal` to
    // resolve the same file first would read every native predecessor twice.
    trim_to_ordinal(
        segments,
        end.thread_id,
        end.end_ordinal_exclusive,
        LineageOffsetMode::Deferred,
    )
    .await?;
    let Some(segment) = segments.iter_mut().rev().find(|segment| {
        segment.rollout_id == end.thread_id
            && end.end_ordinal_exclusive >= segment.start_ordinal
            && segment.end_ordinal_exclusive == Some(end.end_ordinal_exclusive)
    }) else {
        return Err(malformed_lineage(
            end.thread_id,
            "cutoff is outside resolved source rollout",
        ));
    };
    if offset_mode == LineageOffsetMode::Deferred {
        segment.end_byte_offset = Some(end.end_byte_offset);
        segment.jsonl_end_byte_offset = segment.end_byte_offset;
        return Ok(());
    }
    trim_segment_to_history_position(segment, end).await
}

async fn trim_segment_to_history_position(
    segment: &mut RolloutLineageSegment,
    end: HistoryPosition,
) -> ThreadStoreResult<()> {
    segment.rollout_path = codex_rollout::existing_rollout_path(segment.rollout_path.as_path())
        .await
        .ok_or_else(|| malformed_lineage(end.thread_id, "missing source rollout"))?;
    let bytes = read_decoded_rollout(segment.rollout_path.as_path()).await?;
    trim_segment_to_history_position_in_bytes(segment, end, bytes.as_slice())
}

fn trim_segment_to_history_position_in_bytes(
    segment: &mut RolloutLineageSegment,
    end: HistoryPosition,
    bytes: &[u8],
) -> ThreadStoreResult<()> {
    let end_byte_offset = usize::try_from(end.end_byte_offset).map_err(|_| {
        malformed_lineage(
            end.thread_id,
            "cutoff byte offset is past the source rollout",
        )
    })?;
    if end_byte_offset > bytes.len() {
        return Err(malformed_lineage(
            end.thread_id,
            "cutoff byte offset is past the source rollout",
        ));
    }
    if end_byte_offset != 0 && bytes.get(end_byte_offset.saturating_sub(1)) != Some(&b'\n') {
        // Snapshot stabilization can change physical line lengths while preserving ordinals.
        // Recover a stale offset only when it no longer lands between complete JSONL records.
        segment.end_byte_offset =
            byte_offset_for_ordinal_in_bytes(bytes, end.end_ordinal_exclusive)?;
        segment.jsonl_end_byte_offset = segment.end_byte_offset;
        return Ok(());
    }
    let ordinal_end_byte_offset =
        byte_offset_for_ordinal_in_bytes(bytes, end.end_ordinal_exclusive)?.ok_or_else(|| {
            malformed_lineage(end.thread_id, "plain rollout is missing its byte boundary")
        })?;
    if end.end_byte_offset != ordinal_end_byte_offset {
        let ordinal_end_byte_offset = usize::try_from(ordinal_end_byte_offset).map_err(|_| {
            malformed_lineage(
                end.thread_id,
                "ordinal byte boundary is past the source rollout",
            )
        })?;
        let valid_unordinaled_suffix = end_byte_offset < ordinal_end_byte_offset
            && bytes[end_byte_offset..ordinal_end_byte_offset]
                .split_inclusive(|byte| *byte == b'\n')
                .all(|line| {
                    codex_rollout::rollout_ordinal_from_slice(line)
                        .is_ok_and(|ordinal| ordinal.is_none())
                });
        if !valid_unordinaled_suffix {
            return Err(malformed_lineage(
                end.thread_id,
                "cutoff byte offset does not match its ordinal boundary",
            ));
        }
    }
    // The recorded offset remains authoritative when unordinaled records were appended after the
    // selected boundary; ordinal-only reconstruction cannot recover that earlier cutoff.
    segment.end_byte_offset = Some(end.end_byte_offset);
    segment.jsonl_end_byte_offset = segment.end_byte_offset;
    Ok(())
}

async fn trim_to_ordinal(
    segments: &mut Vec<RolloutLineageSegment>,
    rollout_id: ThreadId,
    end_ordinal_exclusive: u64,
    offset_mode: LineageOffsetMode,
) -> ThreadStoreResult<()> {
    if end_ordinal_exclusive == 0 {
        return Err(malformed_lineage(
            rollout_id,
            "cutoff cannot include source session metadata",
        ));
    }
    let contains_previous_ordinal = |segment: &RolloutLineageSegment| {
        segment.rollout_id == rollout_id
            && end_ordinal_exclusive > segment.start_ordinal
            && segment
                .end_ordinal_exclusive
                .is_none_or(|end| end_ordinal_exclusive <= end)
    };
    let empty_segment_at_cutoff = |segment: &RolloutLineageSegment| {
        segment.rollout_id == rollout_id
            && end_ordinal_exclusive == segment.start_ordinal
            && segment
                .end_ordinal_exclusive
                .is_none_or(|end| end_ordinal_exclusive <= end)
    };
    let Some(index) = segments
        .iter()
        .rposition(contains_previous_ordinal)
        .or_else(|| segments.iter().rposition(empty_segment_at_cutoff))
    else {
        return Err(malformed_lineage(
            rollout_id,
            "cutoff is outside resolved source rollout",
        ));
    };
    segments.truncate(index + 1);
    let segment = &mut segments[index];
    if segment.end_ordinal_exclusive == Some(end_ordinal_exclusive)
        && segment.end_byte_offset.is_some()
    {
        return Ok(());
    }
    segment.end_ordinal_exclusive = Some(end_ordinal_exclusive);
    segment.end_byte_offset = if offset_mode == LineageOffsetMode::Resolve {
        byte_offset_for_ordinal(segment.rollout_path.as_path(), end_ordinal_exclusive).await?
    } else {
        None
    };
    segment.jsonl_end_byte_offset = segment.end_byte_offset;
    Ok(())
}

async fn decoded_rollout_len(path: &Path) -> ThreadStoreResult<u64> {
    let path = path.to_path_buf();
    let length = tokio::task::spawn_blocking(move || {
        let reader = codex_rollout::open_rollout_seekable_reader(&path)?;
        let metadata = reader.metadata()?;
        Ok::<_, io::Error>(metadata.len())
    })
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to join rollout length read: {err}"),
    })?;
    length.map_err(lineage_io_error)
}

async fn read_decoded_rollout(path: &Path) -> ThreadStoreResult<Vec<u8>> {
    let path = path.to_path_buf();
    let bytes = tokio::task::spawn_blocking(move || {
        let mut reader = codex_rollout::open_rollout_seekable_reader(&path)?;
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        Ok::<_, io::Error>(bytes)
    })
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to join rollout read: {err}"),
    })?;
    bytes.map_err(lineage_io_error)
}

pub(super) async fn byte_offset_for_ordinal(
    path: &Path,
    end_ordinal_exclusive: u64,
) -> ThreadStoreResult<Option<u64>> {
    let bytes = read_decoded_rollout(path).await?;
    byte_offset_for_ordinal_in_bytes(bytes.as_slice(), end_ordinal_exclusive)
}

fn byte_offset_for_ordinal_in_bytes(
    bytes: &[u8],
    end_ordinal_exclusive: u64,
) -> ThreadStoreResult<Option<u64>> {
    let mut offset = 0u64;
    for physical_line in bytes.split_inclusive(|byte| *byte == b'\n') {
        let line_len = u64::try_from(physical_line.len())
            .map_err(|_| malformed_lineage(ThreadId::default(), "rollout byte offset overflow"))?;
        let next_offset = offset.checked_add(line_len).ok_or_else(|| {
            malformed_lineage(ThreadId::default(), "rollout byte offset overflow")
        })?;
        if let Ok(Some(ordinal)) = codex_rollout::rollout_ordinal_from_slice(physical_line)
            && ordinal >= end_ordinal_exclusive
        {
            return Ok(Some(offset));
        }
        offset = next_offset;
    }
    Ok(Some(offset))
}

#[derive(Clone, Copy)]
struct UserBoundary {
    segment_index: usize,
    rollout_ordinal: u64,
}

async fn trim_before_nth_user_message(
    segments: &mut Vec<RolloutLineageSegment>,
    nth_user_message: usize,
    offset_mode: LineageOffsetMode,
) -> ThreadStoreResult<()> {
    if nth_user_message == usize::MAX {
        return Ok(());
    }
    let mut event_boundaries = Vec::new();
    let mut response_boundaries = Vec::new();
    let mut canonical_user_items = HashSet::new();
    let mut active_turn_start = None;
    for (segment_index, segment) in segments.iter().enumerate() {
        let (lines, _, parse_errors) =
            codex_rollout::RolloutRecorder::load_rollout_lines(segment.rollout_path.as_path())
                .await
                .map_err(lineage_io_error)?;
        if parse_errors != 0 {
            return Err(malformed_lineage(
                segment.thread_id,
                "source rollout contains invalid records",
            ));
        }
        if let Some(end) = segment.end_ordinal_exclusive {
            // Authenticate the original reference cutoff before a user-message boundary replaces
            // it. Otherwise the shorter prefix can conceal an ordinal beyond the source's end.
            let actual_end = lines
                .iter()
                .filter_map(|line| line.ordinal)
                .take_while(|ordinal| *ordinal < end)
                .last()
                .and_then(|ordinal| ordinal.checked_add(1));
            let empty_compatibility_prefix = end == segment.start_ordinal
                && matches!(
                    lines.as_slice(),
                    [
                        RolloutLine {
                            ordinal: Some(0),
                            item: RolloutItem::SessionMeta(_),
                            ..
                        },
                        RolloutLine {
                            ordinal: Some(reference_ordinal),
                            item: RolloutItem::RolloutReference(_),
                            ..
                        },
                        ..
                    ] if *reference_ordinal == end
                );
            if actual_end != Some(end) && !empty_compatibility_prefix {
                return Err(malformed_lineage(
                    segment.rollout_id,
                    format!("cutoff ordinal {end} is not a source boundary").as_str(),
                ));
            }
        }
        for line in lines {
            let Some(ordinal) = line.ordinal else {
                continue;
            };
            if ordinal < segment.start_ordinal
                || segment
                    .end_ordinal_exclusive
                    .is_some_and(|end| ordinal >= end)
            {
                continue;
            }
            let boundary = active_turn_start.unwrap_or(UserBoundary {
                segment_index,
                rollout_ordinal: ordinal,
            });
            match line.item {
                RolloutItem::EventMsg(EventMsg::TurnStarted(_)) => {
                    active_turn_start = Some(UserBoundary {
                        segment_index,
                        rollout_ordinal: ordinal,
                    });
                }
                RolloutItem::EventMsg(EventMsg::UserMessage(_)) => {
                    event_boundaries.push(boundary);
                }
                RolloutItem::EventMsg(EventMsg::ItemCompleted(event))
                    if matches!(event.item, codex_protocol::items::TurnItem::UserMessage(_))
                        && canonical_user_items
                            .insert((event.turn_id.clone(), event.item.id())) =>
                {
                    event_boundaries.push(boundary);
                }
                RolloutItem::ResponseItem(item) if item.is_user_message() => {
                    response_boundaries.push(boundary);
                }
                RolloutItem::EventMsg(EventMsg::TurnComplete(_) | EventMsg::TurnAborted(_)) => {
                    active_turn_start = None;
                }
                RolloutItem::EventMsg(EventMsg::ThreadRolledBack(rollback)) => {
                    let count = usize::try_from(rollback.num_turns).unwrap_or(usize::MAX);
                    event_boundaries.truncate(event_boundaries.len().saturating_sub(count));
                    response_boundaries.truncate(response_boundaries.len().saturating_sub(count));
                }
                _ => {}
            }
        }
    }
    let boundaries = if event_boundaries.is_empty() {
        response_boundaries
    } else {
        event_boundaries
    };
    let Some(boundary) = boundaries.get(nth_user_message).copied() else {
        return Ok(());
    };
    segments.truncate(boundary.segment_index + 1);
    let segment = &mut segments[boundary.segment_index];
    segment.end_ordinal_exclusive = Some(boundary.rollout_ordinal);
    segment.end_byte_offset = if offset_mode == LineageOffsetMode::Resolve {
        byte_offset_for_ordinal(segment.rollout_path.as_path(), boundary.rollout_ordinal).await?
    } else {
        None
    };
    segment.jsonl_end_byte_offset = segment.end_byte_offset;
    Ok(())
}

pub(super) fn filter_rollout_item(item: &mut RolloutItem, filter_texts: &[String]) -> bool {
    if filter_texts.is_empty() {
        return true;
    }
    match item {
        RolloutItem::Compacted(compacted) => {
            if let Some(replacement_history) = compacted.replacement_history.as_mut() {
                replacement_history
                    .retain(|item| !matches_filtered_developer_message(&item.item, filter_texts));
            }
            true
        }
        RolloutItem::ResponseItem(item) => {
            !matches_filtered_developer_message(&item.item, filter_texts)
        }
        _ => true,
    }
}

fn matches_filtered_developer_message(item: &ResponseItem, filter_texts: &[String]) -> bool {
    let ResponseItem::Message { role, content, .. } = item else {
        return false;
    };
    let [ContentItem::InputText { text }] = content.as_slice() else {
        return false;
    };
    role == "developer" && filter_texts.iter().any(|filter_text| filter_text == text)
}

fn malformed_lineage(thread_id: ThreadId, detail: &str) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: format!("invalid paginated history lineage for {thread_id}: {detail}"),
    }
}

fn lineage_io_error(err: io::Error) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("failed to read paginated history lineage: {err}"),
    }
}

#[cfg(test)]
#[path = "rollout_lineage_tests.rs"]
mod tests;
