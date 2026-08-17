//! Builds a read-only migration plan for a reference-backed Legacy rollout.
//!
//! The ordinary migrator can replace one unreferenced file in place. A segmented rollout is a
//! graph: the selected active file names immutable predecessors, and a fork can name another
//! thread's prefix. This module authenticates that graph and records its sources before the
//! migration transaction writes any target file.

use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

use chrono::DateTime;
use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_protocol::RolloutId;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use sha2::Digest;
use sha2::Sha256;
use tokio::io::AsyncReadExt;

use super::migration_error;
use crate::ThreadStoreResult;

/// One authenticated physical source in oldest-to-newest migration order.
#[derive(Clone, Debug)]
pub(super) struct LegacyLineageSource {
    /// Stable logical thread identity stored in `SessionMeta`.
    pub(super) thread_id: ThreadId,
    /// Physical rollout identity derived from the JSONL filename.
    pub(super) rollout_id: RolloutId,
    /// Immutable segment identity, when the source is a rotated segment.
    pub(super) segment_id: Option<SegmentId>,
    pub(super) path: PathBuf,
    pub(super) history_mode: ThreadHistoryMode,
    pub(super) byte_count: u64,
    pub(super) record_count: u64,
    pub(super) sha256: String,
    /// RFC 3339 timestamp recorded in the source `SessionMeta`.
    pub(super) timestamp: String,
    /// Legacy reducer position after this source's materialized predecessor boundary.
    pub(super) initial_source_line_index: u64,
    /// Legacy synthetic item counter after this source's materialized predecessor boundary.
    pub(super) initial_next_item_index: u64,
    /// Reference stored in this source. The referenced source precedes this source in the plan.
    pub(super) predecessor: Option<LegacyLineagePredecessor>,
    /// Ordinal occupied by a leading reference in a Paginated source.
    ///
    /// Native `history_base` replaces this record, so Paginated replay uses the ordinal to
    /// translate `subagent_history_start_ordinal` without changing the child-history boundary.
    pub(super) reference_ordinal: Option<u64>,
}

/// The persisted edge from one physical source to its predecessor.
#[derive(Clone, Debug)]
pub(super) enum LegacyLineagePredecessor {
    RolloutReference(RolloutReferenceItem),
    HistoryBase(HistoryPosition),
}

pub(super) fn reference_is_history_base_compatible(reference: &RolloutReferenceItem) -> bool {
    reference.nth_user_message.is_none()
        && reference
            .compacted_replacement_history_filter_texts
            .is_none()
}

/// Complete authenticated source graph for one selected rollout.
#[derive(Clone, Debug)]
pub(super) struct LegacyLineageMigrationPlan {
    pub(super) selected_thread_id: ThreadId,
    pub(super) selected_rollout_id: RolloutId,
    pub(super) sources: Vec<LegacyLineageSource>,
    /// Paginated source prefixes retained by `SessionMeta.history_base` rather than rewritten.
    pub(super) history_bases: Vec<LegacyHistoryBaseDependency>,
    /// Immutable Paginated segments retained by a Legacy successor's `RolloutReference`.
    pub(super) reference_dependencies: Vec<LegacyReferenceDependency>,
    /// Deterministic unpublished targets in the same oldest-to-newest order as `sources`.
    pub(super) targets: Vec<LegacyLineageTarget>,
}

/// One already-Paginated source whose authenticated prefix remains external to the migration.
#[derive(Clone, Debug)]
pub(super) struct LegacyHistoryBaseDependency {
    pub(super) position: HistoryPosition,
    pub(super) thread_id: ThreadId,
    pub(super) rollout_id: RolloutId,
    pub(super) path: PathBuf,
    pub(super) byte_count: u64,
    pub(super) record_count: u64,
    pub(super) sha256: String,
}

/// One immutable Paginated source retained by a staged successor reference.
#[derive(Clone, Debug)]
pub(super) struct LegacyReferenceDependency {
    pub(super) successor_rollout_id: RolloutId,
    pub(super) thread_id: ThreadId,
    pub(super) rollout_id: RolloutId,
    pub(super) segment_id: SegmentId,
    pub(super) path: PathBuf,
    pub(super) end_ordinal_exclusive: u64,
    pub(super) byte_count: u64,
    pub(super) record_count: u64,
    pub(super) sha256: String,
}

/// One deterministic target in an unpublished lineage migration transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LegacyLineageTarget {
    pub(super) thread_id: ThreadId,
    pub(super) rollout_id: RolloutId,
    pub(super) segment_id: Option<SegmentId>,
    pub(super) path: PathBuf,
    pub(super) selected: bool,
    pub(super) predecessor_segment_id: Option<SegmentId>,
}

/// Resolve and hash a selected Legacy rollout without creating migration artifacts.
pub(super) async fn plan_legacy_lineage(
    codex_home: &Path,
    selected_path: &Path,
) -> ThreadStoreResult<LegacyLineageMigrationPlan> {
    let selected_path = codex_rollout::existing_rollout_path(selected_path)
        .await
        .ok_or_else(|| migration_error("selected rollout does not exist"))?;
    let selected = inspect_source(selected_path.as_path()).await?;
    let selected_thread_id = selected.session_meta.meta.id;
    let selected_rollout_id = selected.rollout_id;
    let mut active = HashSet::new();
    let mut planned = HashSet::new();
    let mut sources = Vec::new();
    let mut history_bases = Vec::new();
    let mut reference_dependencies = Vec::new();
    plan_source(
        codex_home,
        selected,
        &mut active,
        &mut planned,
        &mut sources,
        &mut history_bases,
        &mut reference_dependencies,
    )
    .await?;
    let source_bytes = sources.iter().try_fold(0_u64, |total, source| {
        total
            .checked_add(source.byte_count)
            .ok_or_else(|| migration_error("lineage migration source byte count overflowed"))
    })?;
    if source_bytes <= super::MAX_BOUNDED_DESKTOP_COMPATIBILITY_BYTES {
        for source in &mut sources {
            let (initial_source_line_index, initial_next_item_index) =
                initial_legacy_replay_positions(codex_home, source).await?;
            source.initial_source_line_index = initial_source_line_index;
            source.initial_next_item_index = initial_next_item_index;
        }
    }
    let targets = plan_targets(codex_home, selected_path.as_path(), &sources)?;
    Ok(LegacyLineageMigrationPlan {
        selected_thread_id,
        selected_rollout_id,
        sources,
        history_bases,
        reference_dependencies,
        targets,
    })
}

async fn initial_legacy_replay_positions(
    codex_home: &Path,
    source: &LegacyLineageSource,
) -> ThreadStoreResult<(u64, u64)> {
    if source.history_mode == ThreadHistoryMode::Paginated {
        return Ok((1, 1));
    }
    let Some(LegacyLineagePredecessor::RolloutReference(reference)) = &source.predecessor else {
        return Ok((1, 1));
    };

    let session_meta = codex_rollout::read_session_meta_line(source.path.as_path())
        .await
        .map_err(migration_error)?;
    let timestamp = session_meta.meta.timestamp.clone();
    let prefix = codex_rollout::materialize_rollout_lines_from(
        codex_home,
        vec![
            RolloutLine {
                timestamp: timestamp.clone(),
                ordinal: None,
                item: RolloutItem::SessionMeta(session_meta),
            },
            RolloutLine {
                timestamp,
                ordinal: None,
                item: RolloutItem::RolloutReference(reference.clone()),
            },
        ],
    )
    .await
    .map_err(migration_error)?;
    let mut builder = ThreadHistoryBuilder::new();
    for line in prefix {
        if codex_rollout::is_persisted_rollout_item(&line.item, ThreadHistoryMode::Legacy) {
            builder.handle_rollout_item(&line.item);
        }
    }
    let source_line_index = u64::try_from(builder.next_legacy_rollout_index())
        .map_err(|_| migration_error("materialized reference prefix is too large"))?;
    let next_item_index = u64::try_from(builder.next_synthetic_item_index())
        .map_err(|_| migration_error("Legacy synthetic item index is negative"))?;
    Ok((source_line_index, next_item_index))
}

/// Reject boundaries whose migration would require truncating or filtering a predecessor.
pub(super) fn validate_segment_migration(
    plan: &LegacyLineageMigrationPlan,
) -> ThreadStoreResult<()> {
    for (index, source) in plan.sources.iter().enumerate() {
        let selected = index + 1 == plan.sources.len();
        if !selected && source.thread_id != plan.selected_thread_id && source.segment_id.is_none() {
            return Err(migration_error(
                "reference-backed legacy rollout migration requires an immutable authenticated cross-thread source",
            ));
        }
        match source.predecessor.as_ref() {
            Some(LegacyLineagePredecessor::HistoryBase(position))
                if index != 0
                    || plan.history_bases.len() != 1
                    || plan.history_bases[0].position != *position =>
            {
                return Err(migration_error(
                    "lineage migration history_base must be the authenticated oldest boundary",
                ));
            }
            Some(LegacyLineagePredecessor::RolloutReference(reference)) => {
                let dependency = plan
                    .reference_dependencies
                    .iter()
                    .find(|dependency| dependency.successor_rollout_id == source.rollout_id);
                if let Some(dependency) = dependency {
                    if index != 0
                        || reference.thread_id != Some(dependency.thread_id)
                        || reference.rollout_id != Some(dependency.rollout_id)
                        || reference.segment_id != Some(dependency.segment_id)
                    {
                        return Err(migration_error(
                            "external Paginated reference dependency does not match its oldest Legacy successor",
                        ));
                    }
                } else if index == 0 {
                    return Err(migration_error(
                        "oldest Legacy source has no migrated or external reference predecessor",
                    ));
                }
            }
            _ => {}
        }
    }
    if plan.reference_dependencies.len()
        != plan
            .sources
            .iter()
            .filter(|source| {
                plan.reference_dependencies
                    .iter()
                    .any(|dependency| dependency.successor_rollout_id == source.rollout_id)
            })
            .count()
    {
        return Err(migration_error(
            "lineage migration contains an unbound Paginated reference dependency",
        ));
    }
    Ok(())
}

fn plan_targets(
    codex_home: &Path,
    selected_path: &Path,
    sources: &[LegacyLineageSource],
) -> ThreadStoreResult<Vec<LegacyLineageTarget>> {
    let selected_source = sources
        .last()
        .ok_or_else(|| migration_error("lineage migration has no selected source"))?;
    // Canonicalization can update an item first recorded in a shared predecessor using records
    // that appear later in the selected lineage. Include the selected source in every target
    // identity so two divergent lineages never publish different bytes to the same target path.
    let mut lineage_scope_hasher = Sha256::new();
    lineage_scope_hasher.update(b"frodex-paginated-lineage-scope-v1\0");
    lineage_scope_hasher.update(selected_source.thread_id.to_string().as_bytes());
    lineage_scope_hasher.update(b"\0");
    lineage_scope_hasher.update(selected_source.rollout_id.to_string().as_bytes());
    lineage_scope_hasher.update(b"\0");
    lineage_scope_hasher.update(selected_source.sha256.as_bytes());
    let lineage_scope = lineage_scope_hasher.finalize();
    let selected_plain = codex_rollout::plain_rollout_path(selected_path);
    let selected_is_archived =
        selected_plain.starts_with(codex_home.join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR));
    let selected_directory = selected_plain
        .parent()
        .ok_or_else(|| migration_error("selected rollout has no parent directory"))?;
    let selected_compressed = selected_path.extension().is_some_and(|ext| ext == "zst");
    let mut predecessor_segment_id: Option<SegmentId> = None;
    let mut targets = Vec::with_capacity(sources.len());
    for (index, source) in sources.iter().enumerate() {
        let selected = index + 1 == sources.len();
        let mut segment_hasher = Sha256::new();
        segment_hasher.update(b"frodex-paginated-segment-v3-history-base\0");
        segment_hasher.update(lineage_scope.as_slice());
        segment_hasher.update(b"\0");
        segment_hasher.update(source.sha256.as_bytes());
        segment_hasher.update(b"\0");
        if let Some(segment_id) = predecessor_segment_id {
            segment_hasher.update(segment_id.to_string().as_bytes());
        }
        let mut bytes: [u8; 16] = segment_hasher.finalize()[..16]
            .try_into()
            .map_err(|_| migration_error("invalid segment identity digest"))?;
        bytes[6] = (bytes[6] & 0x0f) | 0x50;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        let segment_id = SegmentId::from_bytes(bytes);
        let mut rollout_hasher = Sha256::new();
        rollout_hasher.update(b"frodex-paginated-rollout-v3-history-base\0");
        rollout_hasher.update(lineage_scope.as_slice());
        rollout_hasher.update(b"\0");
        rollout_hasher.update(source.thread_id.to_string().as_bytes());
        rollout_hasher.update(b"\0");
        rollout_hasher.update(source.sha256.as_bytes());
        rollout_hasher.update(b"\0");
        if let Some(predecessor_segment_id) = predecessor_segment_id {
            rollout_hasher.update(predecessor_segment_id.to_string().as_bytes());
        }
        let rollout_id = ThreadId::from_u128(u128::from_be_bytes(
            rollout_hasher.finalize()[..16]
                .try_into()
                .map_err(|_| migration_error("invalid rollout identity digest"))?,
        ));
        let compressed = source.path.extension().is_some_and(|ext| ext == "zst");
        let filename = target_file_name(
            source.timestamp.as_str(),
            source.thread_id,
            rollout_id,
            if selected {
                selected_compressed
            } else {
                compressed
            },
            !selected,
        )?;
        let path = if selected {
            selected_directory.join(filename)
        } else {
            let (year, month, day) =
                codex_rollout::rollout_date_parts(std::ffi::OsStr::new(filename.as_str()))
                    .ok_or_else(|| {
                        migration_error("lineage target has no canonical rollout date")
                    })?;
            codex_home
                .join(codex_rollout::SESSIONS_SUBDIR)
                .join(codex_rollout::ROLLOUT_SEGMENTS_SUBDIR)
                .join(year)
                .join(month)
                .join(day)
                .join(filename)
        };
        if path == source.path || sources.iter().any(|candidate| candidate.path == path) {
            return Err(migration_error(format!(
                "lineage migration target collides with source {}",
                path.display()
            )));
        }
        targets.push(LegacyLineageTarget {
            thread_id: source.thread_id,
            rollout_id,
            segment_id: Some(segment_id),
            path,
            selected,
            predecessor_segment_id,
        });
        predecessor_segment_id = Some(segment_id);
    }
    if selected_is_archived
        != targets.last().is_some_and(|target| {
            target
                .path
                .starts_with(codex_home.join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR))
        })
    {
        return Err(migration_error(
            "lineage migration target changed archive placement",
        ));
    }
    Ok(targets)
}

fn target_file_name(
    timestamp: &str,
    thread_id: ThreadId,
    rollout_id: RolloutId,
    compressed: bool,
    physical_history: bool,
) -> ThreadStoreResult<String> {
    let timestamp = DateTime::parse_from_rfc3339(timestamp).map_err(migration_error)?;
    let timestamp = if physical_history {
        timestamp - chrono::Duration::seconds(1)
    } else {
        timestamp
    }
    .format("%Y-%m-%dT%H-%M-%S")
    .to_string();
    let identity = if rollout_id == thread_id {
        thread_id.to_string()
    } else {
        format!("{thread_id}_{rollout_id}")
    };
    let suffix = if compressed { ".jsonl.zst" } else { ".jsonl" };
    Ok(format!("rollout-{timestamp}-{identity}{suffix}"))
}

async fn plan_source(
    codex_home: &Path,
    source: InspectedSource,
    active: &mut HashSet<PathBuf>,
    planned: &mut HashSet<PathBuf>,
    sources: &mut Vec<LegacyLineageSource>,
    history_bases: &mut Vec<LegacyHistoryBaseDependency>,
    reference_dependencies: &mut Vec<LegacyReferenceDependency>,
) -> ThreadStoreResult<()> {
    let canonical_path = tokio::fs::canonicalize(source.path.as_path())
        .await
        .map_err(migration_error)?;
    if planned.contains(&canonical_path) {
        return Ok(());
    }
    if !active.insert(canonical_path.clone()) {
        return Err(migration_error(format!(
            "rollout migration lineage contains a cycle at {}",
            source.path.display()
        )));
    }

    match source.predecessor.clone() {
        Some(LegacyLineagePredecessor::RolloutReference(reference)) => {
            let predecessor_path =
                codex_rollout::resolve_rollout_reference_path(codex_home, &reference)
                    .await
                    .map_err(migration_error)?;
            let predecessor = inspect_source(predecessor_path.as_path()).await?;
            if !reference_is_history_base_compatible(&reference)
                && predecessor.session_meta.meta.history_mode == ThreadHistoryMode::Paginated
            {
                let segment_id = predecessor.session_meta.meta.segment_id.ok_or_else(|| {
                    migration_error(
                        "filtered Paginated reference dependency is missing an immutable segment id",
                    )
                })?;
                let end_ordinal_exclusive =
                    paginated_end_ordinal(predecessor.path.as_path()).await?;
                reference_dependencies.push(LegacyReferenceDependency {
                    successor_rollout_id: source.rollout_id,
                    thread_id: predecessor.session_meta.meta.id,
                    rollout_id: predecessor.rollout_id,
                    segment_id,
                    path: predecessor.path,
                    end_ordinal_exclusive,
                    byte_count: predecessor.byte_count,
                    record_count: predecessor.record_count,
                    sha256: predecessor.sha256,
                });
            } else {
                Box::pin(plan_source(
                    codex_home,
                    predecessor,
                    active,
                    planned,
                    sources,
                    history_bases,
                    reference_dependencies,
                ))
                .await?;
            }
        }
        Some(LegacyLineagePredecessor::HistoryBase(position)) => {
            if !history_bases.is_empty() {
                return Err(migration_error(
                    "lineage migration contains more than one history_base boundary",
                ));
            }
            let path =
                codex_rollout::find_rollout_path_by_rollout_id(codex_home, position.thread_id)
                    .await
                    .map_err(migration_error)?
                    .ok_or_else(|| {
                        migration_error(format!(
                            "rollout migration history_base source {} does not exist",
                            position.thread_id
                        ))
                    })?;
            let dependency = inspect_source(path.as_path()).await?;
            if dependency.rollout_id != position.thread_id {
                return Err(migration_error(
                    "rollout migration history_base resolved another physical rollout",
                ));
            }
            if dependency.session_meta.meta.history_mode != ThreadHistoryMode::Paginated {
                return Err(migration_error(
                    "rollout migration history_base source is not Paginated",
                ));
            }
            history_bases.push(LegacyHistoryBaseDependency {
                position,
                thread_id: dependency.session_meta.meta.id,
                rollout_id: dependency.rollout_id,
                path: dependency.path,
                byte_count: dependency.byte_count,
                record_count: dependency.record_count,
                sha256: dependency.sha256,
            });
        }
        None => {}
    }

    active.remove(&canonical_path);
    planned.insert(canonical_path);
    sources.push(source.into_plan_source());
    Ok(())
}

async fn paginated_end_ordinal(path: &Path) -> ThreadStoreResult<u64> {
    let mut reader = codex_rollout::open_rollout_line_reader(path)
        .await
        .map_err(migration_error)?;
    let mut expected = None;
    while let Some(raw) = reader.next_line().await.map_err(migration_error)? {
        if raw.trim().is_empty() {
            continue;
        }
        let line = serde_json::from_str::<RolloutLine>(raw.as_str()).map_err(|error| {
            migration_error(format!(
                "Paginated reference dependency {} contains an invalid record: {error}",
                path.display()
            ))
        })?;
        let ordinal = line.ordinal.ok_or_else(|| {
            migration_error(format!(
                "Paginated reference dependency {} contains an unordinaled record",
                path.display()
            ))
        })?;
        if let Some(expected) = expected
            && ordinal != expected
        {
            return Err(migration_error(format!(
                "Paginated reference dependency {} has a non-contiguous ordinal",
                path.display()
            )));
        }
        expected = Some(
            ordinal
                .checked_add(1)
                .ok_or_else(|| migration_error("Paginated rollout ordinal overflow"))?,
        );
    }
    expected.ok_or_else(|| migration_error("Paginated reference dependency is empty"))
}

struct InspectedSource {
    session_meta: SessionMetaLine,
    rollout_id: RolloutId,
    path: PathBuf,
    byte_count: u64,
    record_count: u64,
    sha256: String,
    predecessor: Option<LegacyLineagePredecessor>,
    reference_ordinal: Option<u64>,
}

impl InspectedSource {
    fn into_plan_source(self) -> LegacyLineageSource {
        LegacyLineageSource {
            thread_id: self.session_meta.meta.id,
            rollout_id: self.rollout_id,
            segment_id: self.session_meta.meta.segment_id,
            path: self.path,
            history_mode: self.session_meta.meta.history_mode,
            byte_count: self.byte_count,
            record_count: self.record_count,
            sha256: self.sha256,
            timestamp: self.session_meta.meta.timestamp,
            initial_source_line_index: 1,
            initial_next_item_index: 1,
            predecessor: self.predecessor,
            reference_ordinal: self.reference_ordinal,
        }
    }
}

async fn inspect_source(path: &Path) -> ThreadStoreResult<InspectedSource> {
    let session_meta = codex_rollout::read_session_meta_line(path)
        .await
        .map_err(migration_error)?;
    let rollout_id =
        codex_rollout::rollout_id_from_path(codex_rollout::plain_rollout_path(path).as_path())
            .unwrap_or(session_meta.meta.id);
    let history_base = session_meta.meta.history_base;
    let mut reader = codex_rollout::open_rollout_line_reader(path)
        .await
        .map_err(migration_error)?;
    let mut saw_session_meta = false;
    let mut saw_local_record = false;
    let mut leading_reference = None;
    let mut reference_ordinal = None;
    let mut record_count = 0_u64;
    while let Some(raw) = reader.next_line().await.map_err(migration_error)? {
        if raw.trim().is_empty() {
            continue;
        }
        let line = match serde_json::from_str::<RolloutLine>(raw.as_str()) {
            Ok(line) => line,
            Err(error) => {
                let reference_record = serde_json::from_str::<serde_json::Value>(raw.as_str())
                    .ok()
                    .and_then(|value| {
                        value
                            .get("type")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned)
                    })
                    .is_some_and(|kind| {
                        matches!(kind.as_str(), "rollout_reference" | "fork_reference")
                    });
                if reference_record {
                    return Err(migration_error(format!(
                        "rollout migration source {} contains a malformed reference: {error}",
                        path.display()
                    )));
                }
                continue;
            }
        };
        record_count = record_count
            .checked_add(1)
            .ok_or_else(|| migration_error("rollout migration record count overflow"))?;
        match line.item {
            RolloutItem::SessionMeta(_) if !saw_session_meta => saw_session_meta = true,
            RolloutItem::SessionMeta(_) => {}
            RolloutItem::RolloutReference(reference)
                if saw_session_meta && !saw_local_record && leading_reference.is_none() =>
            {
                reference_ordinal = line.ordinal;
                leading_reference = Some(reference);
            }
            RolloutItem::RolloutReference(_) => {
                return Err(migration_error(format!(
                    "rollout migration source {} contains a non-leading reference",
                    path.display()
                )));
            }
            _ if saw_session_meta => saw_local_record = true,
            _ => {}
        }
    }
    if history_base.is_some() && leading_reference.is_some() {
        return Err(migration_error(format!(
            "rollout migration source {} contains both history_base and RolloutReference",
            path.display()
        )));
    }
    let predecessor = leading_reference
        .map(LegacyLineagePredecessor::RolloutReference)
        .or(history_base.map(LegacyLineagePredecessor::HistoryBase));
    let (byte_count, sha256) = hash_file(path).await?;
    Ok(InspectedSource {
        session_meta,
        rollout_id,
        path: path.to_path_buf(),
        byte_count,
        record_count,
        sha256,
        predecessor,
        reference_ordinal,
    })
}

pub(super) async fn hash_file(path: &Path) -> ThreadStoreResult<(u64, String)> {
    let mut file = tokio::fs::File::open(path).await.map_err(migration_error)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 256 * 1024];
    let mut byte_count = 0_u64;
    loop {
        let read = file.read(&mut buffer).await.map_err(migration_error)?;
        if read == 0 {
            break;
        }
        byte_count = byte_count
            .checked_add(
                u64::try_from(read)
                    .map_err(|_| migration_error("rollout migration byte count overflow"))?,
            )
            .ok_or_else(|| migration_error("rollout migration byte count overflow"))?;
        hasher.update(&buffer[..read]);
    }
    Ok((byte_count, format!("{:x}", hasher.finalize())))
}
