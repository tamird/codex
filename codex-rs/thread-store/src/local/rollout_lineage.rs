use std::collections::HashSet;
use std::io;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

use codex_app_server_protocol::ThreadItem;
use codex_protocol::RolloutId;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;

use super::LocalThreadStore;
use super::RolloutWriterReservation;
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
    pub(super) end_byte_offset: Option<u64>,
    pub(super) filter_texts: Vec<String>,
}

/// Ordered physical rollout ranges contributing to one logical history.
///
/// `RolloutReference` is the canonical Frodex persistence format. `SessionMeta.history_base` is a
/// compatibility input from upstream. Both are normalized here so pagination and model-context
/// reconstruction have one lineage implementation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RolloutLineage {
    /// Physical rollout selected for the logical thread when this lineage was resolved.
    ///
    /// Pagination cursors bind to this identity so replacing a thread's selected rollout cannot
    /// reinterpret an ordinal from the previous rollout as a position in the replacement.
    pub(super) root_rollout_id: RolloutId,
    pub(super) segments: Vec<RolloutLineageSegment>,
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
        )
        .await?;
        Ok(RolloutLineage {
            root_rollout_id: resolved.rollout_id,
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
            let head = read_rollout_head(rollout_path.as_path()).await?;
            if head.session_meta.meta.id != segment.thread_id {
                return Err(malformed_lineage(
                    segment.thread_id,
                    "source rollout belongs to another thread",
                ));
            }
            let materialized_path = if segment.rollout_id == source.rollout_id
                && head.session_meta.meta.history_base.is_none()
                && head.leading_reference.is_none()
            {
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
            // A previously computed boundary is reusable only when it still names the same
            // immutable segment under the combined writer reservation.
            let reusable_end_byte_offset = if materialized_path == segment.rollout_path
                && segment.end_ordinal_exclusive.is_some()
                && let Some(end_byte_offset) = segment.end_byte_offset
            {
                if segment.thread_id == requested_thread_id {
                    Some(end_byte_offset)
                } else {
                    let immutable_thread_root = self
                        .config
                        .codex_home
                        .join(codex_rollout::ROTATED_ROLLOUT_SEGMENTS_SUBDIR)
                        .join(segment.thread_id.to_string());
                    let immutable_segment_id = materialized_path
                        .strip_prefix(immutable_thread_root.as_path())
                        .ok()
                        .and_then(|relative_path| {
                            let mut components = relative_path.components();
                            let segment_id = components
                                .next()
                                .and_then(|component| component.as_os_str().to_str())
                                .and_then(|component| SegmentId::from_string(component).ok())?;
                            let file_name = components.next()?.as_os_str();
                            let is_expected_rollout = components.next().is_none()
                                && codex_rollout::rollout_id_from_path(Path::new(file_name))
                                    == Some(segment.rollout_id);
                            is_expected_rollout.then_some(segment_id)
                        });
                    if let Some(segment_id) = immutable_segment_id {
                        let session_meta =
                            codex_rollout::read_session_meta_line(materialized_path.as_path())
                                .await
                                .map_err(lineage_io_error)?;
                        (session_meta.meta.id == segment.thread_id
                            && session_meta.meta.segment_id == Some(segment_id))
                        .then_some(end_byte_offset)
                    } else {
                        None
                    }
                }
            } else {
                None
            };
            segment.end_byte_offset = match segment.end_ordinal_exclusive {
                Some(end_ordinal_exclusive) => match reusable_end_byte_offset {
                    Some(end_byte_offset) => Some(end_byte_offset),
                    None => {
                        byte_offset_for_ordinal(materialized_path.as_path(), end_ordinal_exclusive)
                            .await?
                    }
                },
                None => Some(decoded_rollout_len(materialized_path.as_path()).await?),
            };
            segment.rollout_path = materialized_path;
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
        )
        .await?;
        Ok(RolloutLineage {
            root_rollout_id: end.thread_id,
            segments,
        })
    }
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

impl RolloutLineage {
    pub(super) fn root_rollout_id(&self) -> RolloutId {
        self.root_rollout_id
    }

    pub(super) fn segments(&self) -> &[RolloutLineageSegment] {
        self.segments.as_slice()
    }

    /// Returns whether any segment depends on upstream `SessionMeta.history_base` ancestry.
    pub(super) async fn requires_copied_history(&self) -> ThreadStoreResult<bool> {
        for segment in &self.segments {
            let meta = codex_rollout::read_session_meta_line(segment.rollout_path.as_path())
                .await
                .map_err(lineage_io_error)?;
            if meta.meta.history_base.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
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
        trim_to_history_position(&mut self.segments, end).await?;
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

struct RolloutHead {
    session_meta: SessionMetaLine,
    leading_reference: Option<(u64, RolloutReferenceItem)>,
    first_local_ordinal: u64,
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
) -> ThreadStoreResult<Vec<RolloutLineageSegment>> {
    let mut pending_segments = Vec::new();

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

        let head = read_rollout_head(resolved_path.as_path()).await?;
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
            });
            expected_thread_id = referenced_thread_id;
            expected_rollout_id = reference.rollout_id.unwrap_or(referenced_thread_id);
            rollout_path = referenced_path;
            end = None;
            inherited_filter_texts = next_filter_texts;
            graph_depth += usize::from(fork_boundary);
            continue;
        }

        if let Some(history_base) = head.session_meta.meta.history_base {
            if graph_depth >= codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH {
                let detail = format!(
                    "rollout reference graph exceeds maximum depth of {}",
                    codex_rollout::MAX_ROLLOUT_REFERENCE_DEPTH
                );
                return Err(malformed_lineage(expected_thread_id, &detail));
            }
            let source_path = resolve_rollout_path_by_id(store, history_base.thread_id)
                .await?
                .ok_or_else(|| {
                    malformed_lineage(history_base.thread_id, "missing source rollout")
                })?;
            let source_meta = codex_rollout::read_session_meta_line(source_path.as_path())
                .await
                .map_err(lineage_io_error)?;
            let next_filter_texts = inherited_filter_texts.clone();
            pending_segments.push(PendingLineageSegment {
                thread_id: expected_thread_id,
                rollout_id: expected_rollout_id,
                rollout_path: resolved_path,
                first_local_ordinal: head.first_local_ordinal,
                filter_texts: inherited_filter_texts.unwrap_or_default(),
                end,
                reference: None,
            });
            expected_thread_id = source_meta.meta.id;
            expected_rollout_id = history_base.thread_id;
            rollout_path = source_path;
            end = Some(history_base);
            inherited_filter_texts = next_filter_texts;
            graph_depth += 1;
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
        });
        break;
    }

    let mut segments = Vec::with_capacity(pending_segments.len());
    while let Some(pending) = pending_segments.pop() {
        if let Some(reference) = pending.reference {
            trim_to_ordinal(&mut segments, reference.rollout_id, reference.end_ordinal).await?;
            if let Some(nth_user_message) = reference.nth_user_message {
                trim_before_nth_user_message(&mut segments, nth_user_message).await?;
            }
        }

        let file_len = decoded_rollout_len(pending.rollout_path.as_path()).await?;
        segments.push(RolloutLineageSegment {
            thread_id: pending.thread_id,
            rollout_id: pending.rollout_id,
            rollout_path: pending.rollout_path,
            start_ordinal: pending.first_local_ordinal,
            end_ordinal_exclusive: None,
            end_byte_offset: Some(file_len),
            filter_texts: pending.filter_texts,
        });
        if let Some(end) = pending.end {
            trim_to_history_position(&mut segments, end).await?;
        }
    }

    Ok(segments)
}

async fn read_rollout_head(path: &Path) -> ThreadStoreResult<RolloutHead> {
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
    let empty_local_start = match session_meta.meta.history_base {
        Some(base) => base
            .end_ordinal_exclusive
            .checked_add(1)
            .ok_or_else(|| malformed_lineage(session_meta.meta.id, "source ordinal overflow"))?,
        None => 1,
    };
    let next = next_rollout_line(&mut reader).await?;
    let (leading_reference, first_local_ordinal) = match next {
        Some(RolloutLine {
            ordinal: Some(ordinal),
            item: RolloutItem::RolloutReference(reference),
            ..
        }) => (Some((ordinal, reference)), ordinal),
        Some(line) => (None, line.ordinal.unwrap_or(1)),
        None => (None, empty_local_start),
    };
    Ok(RolloutHead {
        session_meta,
        leading_reference,
        first_local_ordinal,
    })
}

async fn next_rollout_line(
    reader: &mut codex_rollout::RolloutLineReader,
) -> ThreadStoreResult<Option<RolloutLine>> {
    while let Some(line) = reader.next_line().await.map_err(lineage_io_error)? {
        if line.trim().is_empty() {
            continue;
        }
        return serde_json::from_str(line.as_str())
            .map(Some)
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to read paginated rollout line: {err}"),
            });
    }
    Ok(None)
}

async fn trim_to_history_position(
    segments: &mut Vec<RolloutLineageSegment>,
    end: HistoryPosition,
) -> ThreadStoreResult<()> {
    trim_to_ordinal(segments, end.thread_id, end.end_ordinal_exclusive).await?;
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
    segment.rollout_path = codex_rollout::existing_rollout_path(segment.rollout_path.as_path())
        .await
        .ok_or_else(|| malformed_lineage(end.thread_id, "missing source rollout"))?;
    let bytes = read_decoded_rollout(segment.rollout_path.as_path()).await?;
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
            byte_offset_for_ordinal(segment.rollout_path.as_path(), end.end_ordinal_exclusive)
                .await?;
        return Ok(());
    }
    let ordinal_end_byte_offset =
        byte_offset_for_ordinal(segment.rollout_path.as_path(), end.end_ordinal_exclusive)
            .await?
            .ok_or_else(|| {
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
                    serde_json::from_slice::<RolloutLine>(line)
                        .is_ok_and(|line| line.ordinal.is_none())
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
    Ok(())
}

async fn trim_to_ordinal(
    segments: &mut Vec<RolloutLineageSegment>,
    rollout_id: ThreadId,
    end_ordinal_exclusive: u64,
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
    segment.end_ordinal_exclusive = Some(end_ordinal_exclusive);
    segment.end_byte_offset =
        byte_offset_for_ordinal(segment.rollout_path.as_path(), end_ordinal_exclusive).await?;
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

async fn byte_offset_for_ordinal(
    path: &Path,
    end_ordinal_exclusive: u64,
) -> ThreadStoreResult<Option<u64>> {
    let bytes = read_decoded_rollout(path).await?;
    let mut offset = 0u64;
    for physical_line in bytes.split_inclusive(|byte| *byte == b'\n') {
        let line_len = u64::try_from(physical_line.len())
            .map_err(|_| malformed_lineage(ThreadId::default(), "rollout byte offset overflow"))?;
        let next_offset = offset.checked_add(line_len).ok_or_else(|| {
            malformed_lineage(ThreadId::default(), "rollout byte offset overflow")
        })?;
        if let Ok(line) = serde_json::from_slice::<RolloutLine>(physical_line)
            && line
                .ordinal
                .is_some_and(|ordinal| ordinal >= end_ordinal_exclusive)
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
) -> ThreadStoreResult<()> {
    if nth_user_message == usize::MAX {
        return Ok(());
    }
    let mut event_boundaries = Vec::new();
    let mut response_boundaries = Vec::new();
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
            let boundary = UserBoundary {
                segment_index,
                rollout_ordinal: active_turn_start
                    .map_or(ordinal, |boundary: UserBoundary| boundary.rollout_ordinal),
            };
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
    segment.end_byte_offset =
        byte_offset_for_ordinal(segment.rollout_path.as_path(), boundary.rollout_ordinal).await?;
    Ok(())
}

fn filter_rollout_item(item: &mut RolloutItem, filter_texts: &[String]) -> bool {
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
