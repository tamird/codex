//! Builds a read-only migration plan for a reference-backed Legacy rollout.
//!
//! The ordinary migrator can replace one unreferenced file in place. A segmented rollout is a
//! graph: the selected active file names immutable predecessors, and a fork can name another
//! thread's prefix. This module authenticates that graph and records its sources before the
//! migration transaction writes any target file.

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::RolloutId;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use sha2::Digest;
use sha2::Sha256;
use tokio::io::AsyncReadExt;

use super::line_parser;
use super::migration_error;
use super::parse_rollout_timestamp;
use super::turn_context_cache::DecodeMode;
use super::turn_context_cache::TurnContextCache;
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
    /// Whether replay must resolve rollback ownership before writing any records.
    pub(super) has_rollback: bool,
    /// Every ordinary Paginated record already has the exact canonical serialized bytes.
    pub(super) canonical_paginated_suffix: bool,
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
    /// Exact decoded prefix selected by a descendant's `history_base`.
    pub(super) replay_end: Option<HistoryPosition>,
    /// Effective native records after consuming a filtered predecessor edge.
    pub(super) native_replay: Option<NativeReplayRange>,
    /// The predecessor's filtering and truncation have already been applied to private sources.
    pub(super) materialized_predecessor: bool,
}

/// Authenticated logical bounds and filters, separate from a source's full physical-file hash.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub(super) struct NativeReplayRange {
    pub(super) start_ordinal: u64,
    pub(super) end_ordinal_exclusive: Option<u64>,
    pub(super) jsonl_end_byte_offset: Option<u64>,
    pub(super) filter_texts: Vec<String>,
}

impl LegacyLineageSource {
    pub(super) fn filter_native_line(
        &self,
        mut line: RolloutLine,
    ) -> ThreadStoreResult<Option<RolloutLine>> {
        let Some(range) = &self.native_replay else {
            return Ok(Some(line));
        };
        if matches!(
            line.item,
            RolloutItem::SessionMeta(_) | RolloutItem::RolloutReference(_)
        ) {
            return Ok(Some(line));
        }
        if line.ordinal.is_none_or(|ordinal| {
            ordinal < range.start_ordinal
                || range
                    .end_ordinal_exclusive
                    .is_some_and(|end| ordinal >= end)
        }) || !super::super::rollout_lineage::filter_rollout_item(
            &mut line.item,
            &range.filter_texts,
        ) {
            return Ok(None);
        }
        Ok(Some(line))
    }
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
    /// Files whose headers determine effective ancestry but contribute no copied records.
    pub(super) authentication_sources: Vec<LegacyLineageSource>,
    /// Paginated source prefixes retained by `SessionMeta.history_base` rather than rewritten.
    pub(super) history_bases: Vec<LegacyHistoryBaseDependency>,
    /// Immutable Paginated segments retained by a Legacy successor's `RolloutReference`.
    pub(super) reference_dependencies: Vec<LegacyReferenceDependency>,
    /// Deterministic unpublished targets in the same oldest-to-newest order as `sources`.
    pub(super) targets: Vec<LegacyLineageTarget>,
    /// Synthetic IDs reassigned so the initial bounded Legacy response remains unchanged.
    pub(super) synthetic_item_id_remap: HashMap<String, String>,
    /// New plans retain already-native referenced prefixes; old durable journals retain their plan.
    pub(super) reuse_native_prefixes: bool,
    /// Replay inherited native records when a Legacy rollback can remove them.
    pub(super) replay_native_rollbacks: bool,
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
    /// Byte boundary in decoded JSONL, not the compressed file's authenticated size.
    pub(super) end_byte_offset: u64,
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
    plan_lineage(
        codex_home,
        selected_path,
        NativePrefixPolicy::RecordedJournal,
    )
    .await
}

/// Used only after authenticating an old, still-unpublished Planned journal.
pub(super) async fn plan_legacy_lineage_with_native_prefix_reuse(
    codex_home: &Path,
    selected_path: &Path,
) -> ThreadStoreResult<LegacyLineageMigrationPlan> {
    plan_lineage(codex_home, selected_path, NativePrefixPolicy::Current).await
}

/// Durable journals retain the source graph that authenticated their recorded target bytes.
enum NativePrefixPolicy {
    RecordedJournal,
    Current,
    #[cfg(test)]
    PreviousRollback,
}

/// Recorded rewrite choices determine both the authenticated graph and target bytes.
#[derive(Clone, Copy)]
struct NativeReplayPolicy {
    reuse_native_prefixes: bool,
    replay_native_rollbacks: bool,
}

#[cfg(test)]
pub(super) async fn plan_without_native_rollback_replay(
    codex_home: &Path,
    selected_path: &Path,
) -> ThreadStoreResult<LegacyLineageMigrationPlan> {
    plan_lineage(
        codex_home,
        selected_path,
        NativePrefixPolicy::PreviousRollback,
    )
    .await
}

async fn plan_lineage(
    codex_home: &Path,
    selected_path: &Path,
    policy: NativePrefixPolicy,
) -> ThreadStoreResult<LegacyLineageMigrationPlan> {
    let selected_path = codex_rollout::existing_rollout_path(selected_path)
        .await
        .ok_or_else(|| migration_error("selected rollout does not exist"))?;
    let mut context_cache = TurnContextCache::default();
    let selected = inspect_source(selected_path.as_path(), &mut context_cache).await?;
    let selected_thread_id = selected.session_meta.meta.id;
    let selected_rollout_id = selected.rollout_id;
    let journal_path = super::publish::migration_journal_path(codex_home, selected_thread_id);
    let (reuse_native_prefixes, replay_native_rollbacks) =
        match (policy, tokio::fs::metadata(&journal_path).await) {
            #[cfg(test)]
            (NativePrefixPolicy::PreviousRollback, _) => (true, false),
            (NativePrefixPolicy::RecordedJournal, Ok(metadata)) if metadata.len() > 0 => {
                let journal =
                    super::lineage_journal::read_lineage_migration_journal(&journal_path).await?;
                (
                    journal.reuse_native_prefixes,
                    journal.replay_native_rollbacks,
                )
            }
            (_, Ok(_)) => (true, true),
            (_, Err(error)) if error.kind() == std::io::ErrorKind::NotFound => (true, true),
            (_, Err(error)) => return Err(migration_error(error)),
        };
    let mut sources = Vec::new();
    let mut history_bases = Vec::new();
    let mut reference_dependencies = Vec::new();
    plan_source(
        codex_home,
        selected,
        &mut sources,
        &mut history_bases,
        &mut reference_dependencies,
        NativeReplayPolicy {
            reuse_native_prefixes,
            replay_native_rollbacks,
        },
        &mut context_cache,
    )
    .await?;
    // Same-thread sources carry one reducer checkpoint through staging. Only the first source and
    // sources after a fork, filtered reference, or Paginated boundary start a new reducer and need
    // their materialized predecessor position. Computing it for every same-thread source would
    // recursively replay the same predecessors and make planning quadratic.
    for index in 0..sources.len() {
        let starts_reducer =
            index == 0 || !ordinary_same_thread_successor(&sources[index - 1], &sources[index]);
        if starts_reducer {
            let (initial_source_line_index, initial_next_item_index) =
                initial_legacy_replay_positions(codex_home, &sources[index]).await?;
            sources[index].initial_source_line_index = initial_source_line_index;
            sources[index].initial_next_item_index = initial_next_item_index;
        }
    }
    let replay_native_rollbacks = replay_native_rollbacks
        && sources
            .iter()
            .any(|source| source.history_mode == ThreadHistoryMode::Legacy && source.has_rollback)
        && (sources
            .iter()
            .any(|source| source.history_mode == ThreadHistoryMode::Paginated)
            || !reference_dependencies.is_empty()
            || !history_bases.is_empty());
    let targets = plan_targets(
        codex_home,
        selected_path.as_path(),
        &sources,
        replay_native_rollbacks,
    )?;
    Ok(LegacyLineageMigrationPlan {
        selected_thread_id,
        selected_rollout_id,
        sources,
        authentication_sources: Vec::new(),
        history_bases,
        reference_dependencies,
        targets,
        synthetic_item_id_remap: HashMap::new(),
        reuse_native_prefixes,
        replay_native_rollbacks,
    })
}

fn ordinary_same_thread_successor(
    source: &LegacyLineageSource,
    successor: &LegacyLineageSource,
) -> bool {
    if source.thread_id != successor.thread_id {
        return false;
    }
    matches!(
        successor.predecessor.as_ref(),
        Some(LegacyLineagePredecessor::RolloutReference(reference))
            if reference.thread_id == Some(source.thread_id)
                && reference.nth_user_message.is_none()
                && reference.compacted_replacement_history_filter_texts.is_none()
    )
}

/// Materializes only the native prefix whose outer filter would otherwise hide it from rollback.
pub(super) async fn expand_filtered_native_rollbacks(
    store: &super::LocalThreadStore,
    plan: &mut LegacyLineageMigrationPlan,
) -> ThreadStoreResult<()> {
    if !plan.replay_native_rollbacks || plan.reference_dependencies.is_empty() {
        return Ok(());
    }
    let Some(first) = plan.sources.first() else {
        return Ok(());
    };
    let Some(LegacyLineagePredecessor::RolloutReference(reference)) = &first.predecessor else {
        return Ok(());
    };
    if reference_is_history_base_compatible(reference) {
        return Ok(());
    }
    let reference = reference.clone();
    let dependency = plan
        .reference_dependencies
        .iter()
        .find(|dependency| dependency.successor_rollout_id == first.rollout_id)
        .ok_or_else(|| {
            migration_error("filtered rollback has no authenticated native predecessor")
        })?;
    let lineage = store
        .resolve_rollout_lineage_from_path(dependency.thread_id, &dependency.path)
        .await?;
    let physical_sources = lineage
        .segments
        .iter()
        .map(|segment| segment.rollout_path.clone())
        .collect::<Vec<_>>();
    let lineage = lineage.apply_reference_constraints(&reference).await?;
    let mut context_cache = TurnContextCache::default();
    let mut sources = Vec::with_capacity(lineage.segments.len() + plan.sources.len());
    for segment in lineage.segments {
        if segment
            .end_ordinal_exclusive
            .is_some_and(|end| end <= segment.start_ordinal)
        {
            continue;
        }
        let mut source = inspect_source(&segment.rollout_path, &mut context_cache)
            .await?
            .into_plan_source();
        if source.history_mode != ThreadHistoryMode::Paginated
            || source.rollout_id != segment.rollout_id
        {
            return Err(migration_error(
                "filtered rollback predecessor is not the authenticated native source",
            ));
        }
        if let (Some(end_ordinal_exclusive), Some(end_byte_offset)) =
            (segment.end_ordinal_exclusive, segment.jsonl_end_byte_offset)
        {
            let end = HistoryPosition {
                thread_id: source.rollout_id,
                end_ordinal_exclusive,
                end_byte_offset,
            };
            validate_native_replay_end(&source.path, end).await?;
            source.replay_end = Some(end);
        }
        source.native_replay = Some(NativeReplayRange {
            start_ordinal: segment.start_ordinal,
            end_ordinal_exclusive: segment.end_ordinal_exclusive,
            jsonl_end_byte_offset: segment.jsonl_end_byte_offset,
            filter_texts: segment.filter_texts,
        });
        source.materialized_predecessor = true;
        sources.push(source);
    }
    plan.sources[0].materialized_predecessor = true;
    sources.append(&mut plan.sources);
    plan.sources = sources;
    for path in physical_sources {
        if !plan.sources.iter().any(|source| source.path == path) {
            plan.authentication_sources.push(
                inspect_source(&path, &mut context_cache)
                    .await?
                    .into_plan_source(),
            );
        }
    }
    plan.reference_dependencies.clear();
    let selected_path = plan
        .sources
        .last()
        .ok_or_else(|| migration_error("filtered rollback has no selected source"))?
        .path
        .clone();
    plan.targets = plan_targets(
        &store.config.codex_home,
        &selected_path,
        &plan.sources,
        /*replay_native_rollbacks*/ true,
    )?;
    Ok(())
}

/// Reports whether a selected Paginated lineage still contains a `RolloutReference` that native
/// `history_base` can represent without filtering or truncation.
pub(super) async fn contains_convertible_rollout_reference(
    codex_home: &Path,
    selected_path: &Path,
) -> ThreadStoreResult<bool> {
    let mut source_path = codex_rollout::existing_rollout_path(selected_path)
        .await
        .ok_or_else(|| migration_error("selected rollout does not exist"))?;
    let mut active = HashSet::new();
    let mut rollout_paths_by_id = None;
    // A native history can contain thousands of same-thread segments. Keep this walk iterative so
    // polling the migration inspection does not recurse once per `history_base` edge.
    loop {
        let canonical_path = tokio::fs::canonicalize(source_path.as_path())
            .await
            .map_err(migration_error)?;
        if !active.insert(canonical_path) {
            return Err(migration_error(format!(
                "rollout migration lineage contains a cycle at {}",
                source_path.display()
            )));
        }
        match inspect_predecessor(source_path.as_path()).await? {
            Some(LegacyLineagePredecessor::RolloutReference(reference)) => {
                return Ok(reference_is_history_base_compatible(&reference));
            }
            Some(LegacyLineagePredecessor::HistoryBase(position)) => {
                if rollout_paths_by_id.is_none() {
                    rollout_paths_by_id = Some(
                        codex_rollout::index_rollout_paths_by_rollout_id(codex_home)
                            .await
                            .map_err(migration_error)?,
                    );
                }
                let path = rollout_paths_by_id
                    .as_ref()
                    .and_then(|paths| paths.get(&position.thread_id))
                    .cloned()
                    .ok_or_else(|| {
                        migration_error(format!(
                            "rollout migration history_base source {} does not exist",
                            position.thread_id
                        ))
                    })?;
                let session_meta = codex_rollout::read_session_meta_line(path.as_path())
                    .await
                    .map_err(migration_error)?;
                let rollout_id = codex_rollout::rollout_id_from_path(
                    codex_rollout::plain_rollout_path(path.as_path()).as_path(),
                )
                .unwrap_or(session_meta.meta.id);
                if rollout_id != position.thread_id {
                    return Err(migration_error(
                        "rollout migration history_base resolved another physical rollout",
                    ));
                }
                if session_meta.meta.history_mode != ThreadHistoryMode::Paginated {
                    return Err(migration_error(
                        "rollout migration history_base source is not Paginated",
                    ));
                }
                source_path = path;
            }
            None => return Ok(false),
        }
    }
}

pub(super) async fn has_leading_filtered_rollout_reference(path: &Path) -> ThreadStoreResult<bool> {
    Ok(matches!(
        inspect_predecessor(path).await?,
        Some(LegacyLineagePredecessor::RolloutReference(reference))
            if !reference_is_history_base_compatible(&reference)
    ))
}

async fn inspect_predecessor(path: &Path) -> ThreadStoreResult<Option<LegacyLineagePredecessor>> {
    let session_meta = codex_rollout::read_session_meta_line(path)
        .await
        .map_err(migration_error)?;
    if let Some(position) = session_meta.meta.history_base {
        return Ok(Some(LegacyLineagePredecessor::HistoryBase(position)));
    }
    let mut reader = codex_rollout::open_rollout_line_reader(path)
        .await
        .map_err(migration_error)?;
    let mut saw_session_meta = false;
    while let Some(raw) = reader.next_line().await.map_err(migration_error)? {
        let Ok(line) = line_parser::parse_paginated_rollout_line(raw.as_bytes()) else {
            continue;
        };
        match line.item {
            RolloutItem::SessionMeta(_) if !saw_session_meta => saw_session_meta = true,
            RolloutItem::SessionMeta(_) => {}
            RolloutItem::RolloutReference(reference) if saw_session_meta => {
                return Ok(Some(LegacyLineagePredecessor::RolloutReference(reference)));
            }
            _ if saw_session_meta => return Ok(None),
            _ => {}
        }
    }
    Ok(None)
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
    let prefix = codex_rollout::BoundedRolloutMaterializer::new(codex_home, &source.path)
        .retaining_source_metadata()
        .materialize_from(
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
            /*ordinary_reference_limit*/ usize::MAX,
        )
        .await
        .map_err(migration_error)?;
    let builder = super::lineage_compatibility::replay_materialized_history(
        prefix.lines.iter().map(|line| &line.item),
        ThreadHistoryMode::Legacy,
    );
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
        if source.materialized_predecessor {
            continue;
        }
        match source.predecessor.as_ref() {
            Some(LegacyLineagePredecessor::HistoryBase(position)) => match index {
                0 if plan.history_bases.len() == 1
                    && plan.history_bases[0].position == *position => {}
                index
                    if index > 0
                        && plan.sources.get(index - 1).is_some_and(|predecessor| {
                            predecessor.rollout_id == position.thread_id
                        }) => {}
                _ => {
                    return Err(migration_error(
                        "lineage migration history_base does not match its authenticated predecessor",
                    ));
                }
            },
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
    replay_native_rollbacks: bool,
) -> ThreadStoreResult<Vec<LegacyLineageTarget>> {
    let selected_source = sources
        .last()
        .ok_or_else(|| migration_error("lineage migration has no selected source"))?;
    // Canonicalization can update an item first recorded in a shared predecessor using records
    // that appear later in the selected lineage. Include the selected source in every target
    // identity so two divergent lineages never publish different bytes to the same target path.
    let mut lineage_scope_hasher = Sha256::new();
    lineage_scope_hasher.update(b"frodex-paginated-lineage-scope-v1\0");
    if replay_native_rollbacks {
        lineage_scope_hasher.update(b"native-rollback-replay-v1\0");
    }
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
        if let Some(range) = &source.native_replay {
            segment_hasher.update(serde_json::to_vec(range).map_err(migration_error)?);
        }
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
        if let Some(range) = &source.native_replay {
            rollout_hasher.update(serde_json::to_vec(range).map_err(migration_error)?);
        }
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

pub(super) fn target_file_name(
    timestamp: &str,
    thread_id: ThreadId,
    rollout_id: RolloutId,
    compressed: bool,
    physical_history: bool,
) -> ThreadStoreResult<String> {
    let timestamp = parse_rollout_timestamp(timestamp)?;
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
    sources: &mut Vec<LegacyLineageSource>,
    history_bases: &mut Vec<LegacyHistoryBaseDependency>,
    reference_dependencies: &mut Vec<LegacyReferenceDependency>,
    policy: NativeReplayPolicy,
    context_cache: &mut TurnContextCache,
) -> ThreadStoreResult<()> {
    let mut source = source;
    let mut active = HashSet::new();
    let mut pending = Vec::new();
    // A lineage has at most one predecessor per source. Collect that chain explicitly so a deep
    // migration does not create one nested `Future::poll` frame per rollout segment.
    loop {
        let canonical_path = tokio::fs::canonicalize(source.path.as_path())
            .await
            .map_err(migration_error)?;
        if !active.insert(canonical_path.clone()) {
            return Err(migration_error(format!(
                "rollout migration lineage contains a cycle at {}",
                source.path.display()
            )));
        }

        let predecessor = source.predecessor.clone();
        let successor_rollout_id = source.rollout_id;
        let successor_history_mode = source.session_meta.meta.history_mode;
        pending.push((canonical_path, source));
        match predecessor {
            Some(LegacyLineagePredecessor::RolloutReference(reference)) => {
                let predecessor_path =
                    codex_rollout::resolve_rollout_reference_path(codex_home, &reference)
                        .await
                        .map_err(migration_error)?;
                let predecessor = inspect_source(predecessor_path.as_path(), context_cache).await?;
                if predecessor.session_meta.meta.history_mode == ThreadHistoryMode::Paginated
                    && (!reference_is_history_base_compatible(&reference)
                        || (policy.reuse_native_prefixes
                            && successor_history_mode == ThreadHistoryMode::Legacy
                            && !pending.iter().any(|(_, source)| source.has_rollback)
                            && reference.segment_id == predecessor.session_meta.meta.segment_id
                            && reference.segment_id.is_some()
                            && reference.rollout_id == Some(predecessor.rollout_id)
                            && native_prefix_is_addressable(
                                codex_home,
                                &predecessor.path,
                                predecessor.rollout_id,
                            )
                            .await?
                            && !contains_convertible_rollout_reference(
                                codex_home,
                                &predecessor.path,
                            )
                            .await?))
                {
                    let segment_id = predecessor.session_meta.meta.segment_id.ok_or_else(|| {
                        migration_error(
                            "filtered Paginated reference dependency is missing an immutable segment id",
                        )
                    })?;
                    let end_ordinal_exclusive =
                        paginated_end_ordinal(predecessor.path.as_path()).await?;
                    let end_byte_offset = logical_rollout_byte_count(&predecessor.path).await?;
                    reference_dependencies.push(LegacyReferenceDependency {
                        successor_rollout_id,
                        thread_id: predecessor.session_meta.meta.id,
                        rollout_id: predecessor.rollout_id,
                        segment_id,
                        path: predecessor.path,
                        end_ordinal_exclusive,
                        end_byte_offset,
                        byte_count: predecessor.byte_count,
                        record_count: predecessor.record_count,
                        sha256: predecessor.sha256,
                    });
                    break;
                }
                source = predecessor;
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
                let mut dependency = inspect_source(path.as_path(), context_cache).await?;
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
                if policy.replay_native_rollbacks
                    && pending.iter().any(|(_, source)| {
                        source.session_meta.meta.history_mode == ThreadHistoryMode::Legacy
                            && source.has_rollback
                    })
                {
                    validate_native_replay_end(&dependency.path, position).await?;
                    dependency.replay_end = Some(position);
                    source = dependency;
                } else if contains_convertible_rollout_reference(
                    codex_home,
                    dependency.path.as_path(),
                )
                .await?
                {
                    source = dependency;
                } else {
                    history_bases.push(LegacyHistoryBaseDependency {
                        position,
                        thread_id: dependency.session_meta.meta.id,
                        rollout_id: dependency.rollout_id,
                        path: dependency.path,
                        byte_count: dependency.byte_count,
                        record_count: dependency.record_count,
                        sha256: dependency.sha256,
                    });
                    break;
                }
            }
            None => break,
        }
    }

    while let Some((_, source)) = pending.pop() {
        sources.push(source.into_plan_source());
    }
    Ok(())
}

async fn native_prefix_is_addressable(
    codex_home: &Path,
    path: &Path,
    rollout_id: RolloutId,
) -> ThreadStoreResult<bool> {
    let Some(resolved) = codex_rollout::find_rollout_path_by_rollout_id(codex_home, rollout_id)
        .await
        .map_err(migration_error)?
    else {
        return Ok(false);
    };
    Ok(tokio::fs::canonicalize(resolved)
        .await
        .map_err(migration_error)?
        == tokio::fs::canonicalize(path)
            .await
            .map_err(migration_error)?)
}

/// Authenticate both coordinates without materializing a compressed predecessor in memory.
async fn validate_native_replay_end(path: &Path, end: HistoryPosition) -> ThreadStoreResult<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::io::BufRead;
        use std::io::Read;
        #[derive(serde::Deserialize)]
        struct Ordinal {
            ordinal: u64,
        }
        let file = std::fs::File::open(&path).map_err(migration_error)?;
        let decoded: Box<dyn Read> = if path.extension().is_some_and(|extension| extension == "zst")
        {
            Box::new(zstd::stream::read::Decoder::new(file).map_err(migration_error)?)
        } else {
            Box::new(file)
        };
        let mut reader =
            std::io::BufReader::with_capacity(256 * 1024, decoded.take(end.end_byte_offset));
        let mut bytes = Vec::new();
        let mut offset = 0_u64;
        let mut expected = None;
        loop {
            bytes.clear();
            let count = reader
                .read_until(b'\n', &mut bytes)
                .map_err(migration_error)?;
            if count == 0 {
                break;
            }
            offset += count as u64;
            if bytes.last() != Some(&b'\n') || bytes.len() > super::MAX_ROLLOUT_LINE_BYTES {
                return Err(migration_error(
                    "native rollback predecessor has an incomplete or oversized record",
                ));
            }
            let ordinal = serde_json::from_slice::<Ordinal>(&bytes)
                .map_err(migration_error)?
                .ordinal;
            if expected.is_some_and(|expected| ordinal != expected)
                || ordinal >= end.end_ordinal_exclusive
            {
                return Err(migration_error(
                    "native rollback predecessor has an invalid history_base ordinal",
                ));
            }
            expected = ordinal.checked_add(1);
        }
        if offset != end.end_byte_offset || expected != Some(end.end_ordinal_exclusive) {
            return Err(migration_error(
                "native rollback predecessor has an invalid history_base boundary",
            ));
        }
        Ok(())
    })
    .await
    .map_err(migration_error)?
}

async fn logical_rollout_byte_count(path: &Path) -> ThreadStoreResult<u64> {
    if path.extension().is_none_or(|extension| extension != "zst") {
        return tokio::fs::metadata(path)
            .await
            .map(|metadata| metadata.len())
            .map_err(migration_error);
    }
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let input = std::fs::File::open(path)?;
        let mut decoder = zstd::stream::read::Decoder::new(input)?;
        std::io::copy(&mut decoder, &mut std::io::sink())
    })
    .await
    .map_err(migration_error)?
    .map_err(migration_error)
}

async fn paginated_end_ordinal(path: &Path) -> ThreadStoreResult<u64> {
    let mut reader = super::open_migration_line_reader(path)
        .await
        .map_err(migration_error)?;
    let mut expected = None;
    while let Some(raw) = reader.next_line().await.map_err(migration_error)? {
        if raw.trim().is_empty() {
            continue;
        }
        let line = line_parser::parse_paginated_rollout_line(raw.as_bytes()).map_err(|error| {
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
    has_rollback: bool,
    canonical_paginated_suffix: bool,
    sha256: String,
    predecessor: Option<LegacyLineagePredecessor>,
    reference_ordinal: Option<u64>,
    replay_end: Option<HistoryPosition>,
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
            has_rollback: self.has_rollback,
            canonical_paginated_suffix: self.canonical_paginated_suffix,
            sha256: self.sha256,
            timestamp: self.session_meta.meta.timestamp,
            initial_source_line_index: 1,
            initial_next_item_index: 1,
            predecessor: self.predecessor,
            reference_ordinal: self.reference_ordinal,
            replay_end: self.replay_end,
            native_replay: None,
            materialized_predecessor: false,
        }
    }
}

async fn inspect_source(
    path: &Path,
    context_cache: &mut TurnContextCache,
) -> ThreadStoreResult<InspectedSource> {
    let session_meta = codex_rollout::read_session_meta_line(path)
        .await
        .map_err(migration_error)?;
    let rollout_id =
        codex_rollout::rollout_id_from_path(codex_rollout::plain_rollout_path(path).as_path())
            .unwrap_or(session_meta.meta.id);
    let history_base = session_meta.meta.history_base;
    let mut reader = super::open_migration_line_reader(path)
        .await
        .map_err(migration_error)?;
    let mut saw_session_meta = false;
    let mut saw_local_record = false;
    let mut leading_reference = None;
    let mut reference_ordinal = None;
    let mut record_count = 0_u64;
    let mut has_rollback = false;
    let mut canonical_paginated_suffix =
        session_meta.meta.history_mode == ThreadHistoryMode::Paginated;
    while let Some(raw) = reader.next_line().await.map_err(migration_error)? {
        if raw.trim().is_empty() {
            continue;
        }
        let prepared_context = context_cache.parse(raw.as_bytes(), DecodeMode::Paginated);
        let decoded = match &prepared_context {
            Some(context) => Ok(context.rollout_line()),
            None => line_parser::parse_paginated_rollout_line(raw.as_bytes()),
        };
        let line = match decoded {
            Ok(line) => line,
            Err(error) => {
                canonical_paginated_suffix = false;
                let value = serde_json::from_str::<serde_json::Value>(raw.as_str()).ok();
                let kind = value
                    .as_ref()
                    .and_then(|value| value.get("type"))
                    .and_then(serde_json::Value::as_str);
                has_rollback |= kind == Some("event_msg")
                    && value
                        .as_ref()
                        .and_then(|value| value.pointer("/payload/type"))
                        .and_then(serde_json::Value::as_str)
                        == Some("thread_rolled_back");
                let reference_record = matches!(kind, Some("rollout_reference" | "fork_reference"));
                if reference_record {
                    return Err(migration_error(format!(
                        "rollout migration source {} contains a malformed reference: {error}",
                        path.display()
                    )));
                }
                continue;
            }
        };
        // Native replay preserves payloads instead of canonicalizing legacy presentation events.
        // Reject such inputs before staging: a later projection failure must not follow selection.
        if session_meta.meta.history_mode == ThreadHistoryMode::Paginated
            && session_meta
                .meta
                .subagent_history_start_ordinal
                .is_none_or(|start| line.ordinal.is_none_or(|ordinal| ordinal >= start))
            && codex_rollout::is_persisted_rollout_item(&line.item, ThreadHistoryMode::Legacy)
            && !codex_rollout::is_persisted_rollout_item(&line.item, ThreadHistoryMode::Paginated)
        {
            return Err(migration_error(format!(
                "Paginated source {} contains legacy-only presentation events; retain its supported reader",
                path.display()
            )));
        }
        if canonical_paginated_suffix
            && !matches!(
                &line.item,
                RolloutItem::SessionMeta(_) | RolloutItem::RolloutReference(_)
            )
        {
            let canonical = match (&prepared_context, line.ordinal) {
                (Some(context), Some(ordinal)) => context.canonical_record(ordinal)?,
                _ => serde_json::to_vec(&line).map_err(migration_error)?,
            };
            canonical_paginated_suffix = canonical == raw.as_bytes();
        }
        record_count = record_count
            .checked_add(1)
            .ok_or_else(|| migration_error("rollout migration record count overflow"))?;
        has_rollback |= matches!(
            &line.item,
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(_))
        );
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
        has_rollback,
        canonical_paginated_suffix,
        sha256,
        predecessor,
        reference_ordinal,
        replay_end: None,
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
