//! Machine-readable dry-run manifest for one lineage migration.
//!
//! The manifest is derived by the same canonical replayer used by apply. It therefore reports
//! exact uncompressed target payload bytes, record counts, ordinals, and hashes without creating a
//! target file. SQLite page allocation, directory entries, and temporary compressed bytes remain
//! filesystem-dependent and are reported explicitly rather than hidden inside an inaccurate total.

use std::path::PathBuf;

use codex_protocol::RolloutId;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ThreadHistoryMode;
use serde::Serialize;

use super::RolloutMigrationKind;
use super::lineage::LegacyLineageMigrationPlan;
use super::lineage::LegacyLineagePredecessor;
use super::lineage_stage::measure_legacy_lineage;
use super::single_manifest::measure_single_rollout;
use crate::ThreadStoreResult;

const LINEAGE_MANIFEST_VERSION: u32 = 2;

/// Exact dry-run description of one authenticated migration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RolloutMigrationManifest {
    pub version: u32,
    pub input_kind: RolloutMigrationManifestKind,
    pub selected_thread_id: ThreadId,
    pub selected_source_rollout_id: RolloutId,
    pub sources: Vec<RolloutMigrationLineageSource>,
    pub history_base_dependencies: Vec<RolloutMigrationHistoryBaseDependency>,
    pub reference_dependencies: Vec<RolloutMigrationReferenceDependency>,
    pub targets: Vec<RolloutMigrationLineageTarget>,
    pub source_bytes: u64,
    pub dependency_bytes: u64,
    /// Exact bytes written into private uncompressed target files before publication.
    pub target_payload_bytes: u64,
    /// Exact minimum payload reservation. SQLite and filesystem metadata are additional.
    pub minimum_free_bytes: u64,
    pub additional_free_space: RolloutMigrationAdditionalFreeSpace,
    pub publication_phases: Vec<RolloutMigrationPublicationPhase>,
    pub sources_retained_after_apply: bool,
}

/// Physical input selected for migration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RolloutMigrationManifestKind {
    SingleRollout,
    SegmentedLineage,
}

/// One physical Legacy source authenticated by dry-run.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RolloutMigrationLineageSource {
    pub thread_id: ThreadId,
    pub rollout_id: RolloutId,
    pub segment_id: Option<SegmentId>,
    pub path: PathBuf,
    pub history_mode: ThreadHistoryMode,
    pub byte_count: u64,
    pub record_count: u64,
    pub sha256: String,
    pub predecessor: Option<RolloutMigrationLineagePredecessor>,
}

/// Boundary that precedes one migrated physical source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum RolloutMigrationLineagePredecessor {
    RolloutReference(RolloutMigrationReferenceBoundary),
    HistoryBase(HistoryPosition),
}

/// Persisted reference fields that determine the inherited boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RolloutMigrationReferenceBoundary {
    pub rollout_id: Option<RolloutId>,
    pub rollout_path: PathBuf,
    pub thread_id: Option<ThreadId>,
    pub rollout_timestamp: Option<String>,
    pub segment_id: Option<SegmentId>,
    pub max_depth: usize,
    pub nth_user_message: Option<usize>,
    pub compacted_replacement_history_filter_texts: Option<Vec<String>>,
}

/// Existing Paginated prefix retained through `SessionMeta.history_base`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RolloutMigrationHistoryBaseDependency {
    pub position: HistoryPosition,
    pub thread_id: ThreadId,
    pub rollout_id: RolloutId,
    pub path: PathBuf,
    pub byte_count: u64,
    pub record_count: u64,
    pub sha256: String,
}

/// Existing immutable Paginated segment retained through `RolloutReferenceItem`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RolloutMigrationReferenceDependency {
    pub successor_rollout_id: RolloutId,
    pub thread_id: ThreadId,
    pub rollout_id: RolloutId,
    pub segment_id: SegmentId,
    pub path: PathBuf,
    pub end_ordinal_exclusive: u64,
    pub byte_count: u64,
    pub record_count: u64,
    pub sha256: String,
}

/// Exact canonical output expected for one unpublished target.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RolloutMigrationLineageTarget {
    pub thread_id: ThreadId,
    pub rollout_id: RolloutId,
    pub segment_id: Option<SegmentId>,
    pub path: PathBuf,
    /// Exact predecessor boundary written into the target `SessionMeta`.
    pub history_base: Option<HistoryPosition>,
    pub start_ordinal: u64,
    pub end_ordinal_exclusive: u64,
    pub byte_count: u64,
    pub record_count: u64,
    pub sha256: String,
    pub selected: bool,
    pub compressed_on_publication: bool,
}

/// Storage not expressible as a portable exact byte count before publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RolloutMigrationAdditionalFreeSpace {
    pub sqlite_projection: &'static str,
    pub filesystem_metadata: &'static str,
    pub dry_run_decompression_temporary: &'static str,
    pub compressed_publication_temporary: &'static str,
}

/// Ordered durable phases used by apply and restart recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RolloutMigrationPublicationPhase {
    Planned,
    TargetsDurable,
    ProjectionDurable,
    Selected,
    Verified,
    Complete,
}

pub(super) async fn build_lineage_manifest(
    plan: &LegacyLineageMigrationPlan,
) -> ThreadStoreResult<RolloutMigrationManifest> {
    let measured = measure_legacy_lineage(plan).await?;
    let sources = plan
        .sources
        .iter()
        .map(|source| RolloutMigrationLineageSource {
            thread_id: source.thread_id,
            rollout_id: source.rollout_id,
            segment_id: source.segment_id,
            path: source.path.clone(),
            history_mode: source.history_mode,
            byte_count: source.byte_count,
            record_count: source.record_count,
            sha256: source.sha256.clone(),
            predecessor: source
                .predecessor
                .as_ref()
                .map(|predecessor| match predecessor {
                    LegacyLineagePredecessor::RolloutReference(reference) => {
                        RolloutMigrationLineagePredecessor::RolloutReference(
                            RolloutMigrationReferenceBoundary {
                                rollout_id: reference.rollout_id,
                                rollout_path: reference.rollout_path.clone(),
                                thread_id: reference.thread_id,
                                rollout_timestamp: reference.rollout_timestamp.clone(),
                                segment_id: reference.segment_id,
                                max_depth: reference.max_depth,
                                nth_user_message: reference.nth_user_message,
                                compacted_replacement_history_filter_texts: reference
                                    .compacted_replacement_history_filter_texts
                                    .clone(),
                            },
                        )
                    }
                    LegacyLineagePredecessor::HistoryBase(position) => {
                        RolloutMigrationLineagePredecessor::HistoryBase(*position)
                    }
                }),
        })
        .collect::<Vec<_>>();
    let history_base_dependencies = plan
        .history_bases
        .iter()
        .map(|dependency| RolloutMigrationHistoryBaseDependency {
            position: dependency.position,
            thread_id: dependency.thread_id,
            rollout_id: dependency.rollout_id,
            path: dependency.path.clone(),
            byte_count: dependency.byte_count,
            record_count: dependency.record_count,
            sha256: dependency.sha256.clone(),
        })
        .collect::<Vec<_>>();
    let reference_dependencies = plan
        .reference_dependencies
        .iter()
        .map(|dependency| RolloutMigrationReferenceDependency {
            successor_rollout_id: dependency.successor_rollout_id,
            thread_id: dependency.thread_id,
            rollout_id: dependency.rollout_id,
            segment_id: dependency.segment_id,
            path: dependency.path.clone(),
            end_ordinal_exclusive: dependency.end_ordinal_exclusive,
            byte_count: dependency.byte_count,
            record_count: dependency.record_count,
            sha256: dependency.sha256.clone(),
        })
        .collect::<Vec<_>>();
    let targets = measured
        .into_iter()
        .map(|target| RolloutMigrationLineageTarget {
            thread_id: target.thread_id,
            rollout_id: target.rollout_id,
            segment_id: target.segment_id,
            compressed_on_publication: target
                .final_path
                .extension()
                .is_some_and(|extension| extension == "zst"),
            path: target.final_path,
            history_base: target.history_base,
            start_ordinal: target.start_ordinal,
            end_ordinal_exclusive: target.end_ordinal_exclusive,
            byte_count: target.byte_count,
            record_count: target.record_count,
            sha256: target.sha256,
            selected: target.selected,
        })
        .collect::<Vec<_>>();
    let source_bytes = sources.iter().map(|source| source.byte_count).sum();
    let dependency_bytes = history_base_dependencies
        .iter()
        .map(|source| source.byte_count)
        .chain(
            reference_dependencies
                .iter()
                .map(|source| source.byte_count),
        )
        .sum();
    let target_payload_bytes = targets.iter().map(|target| target.byte_count).sum();
    Ok(RolloutMigrationManifest {
        version: LINEAGE_MANIFEST_VERSION,
        input_kind: RolloutMigrationManifestKind::SegmentedLineage,
        selected_thread_id: plan.selected_thread_id,
        selected_source_rollout_id: plan.selected_rollout_id,
        sources,
        history_base_dependencies,
        reference_dependencies,
        targets,
        source_bytes,
        dependency_bytes,
        target_payload_bytes,
        minimum_free_bytes: target_payload_bytes,
        additional_free_space: RolloutMigrationAdditionalFreeSpace {
            sqlite_projection: "filesystem-dependent SQLite page allocation",
            filesystem_metadata: "filesystem-dependent directory and allocation metadata",
            dry_run_decompression_temporary: "none for segmented lineage inputs",
            compressed_publication_temporary: "exact only after streaming compression",
        },
        publication_phases: vec![
            RolloutMigrationPublicationPhase::Planned,
            RolloutMigrationPublicationPhase::TargetsDurable,
            RolloutMigrationPublicationPhase::ProjectionDurable,
            RolloutMigrationPublicationPhase::Selected,
            RolloutMigrationPublicationPhase::Verified,
            RolloutMigrationPublicationPhase::Complete,
        ],
        sources_retained_after_apply: true,
    })
}

pub(super) async fn build_single_manifest(
    path: &std::path::Path,
    kind: RolloutMigrationKind,
) -> ThreadStoreResult<RolloutMigrationManifest> {
    let measured = measure_single_rollout(path, kind).await?;
    let source = RolloutMigrationLineageSource {
        thread_id: measured.thread_id,
        rollout_id: measured.rollout_id,
        segment_id: measured.segment_id,
        path: path.to_path_buf(),
        history_mode: ThreadHistoryMode::Legacy,
        byte_count: measured.source_byte_count,
        record_count: measured.source_record_count,
        sha256: measured.source_sha256,
        predecessor: None,
    };
    let target = RolloutMigrationLineageTarget {
        thread_id: measured.thread_id,
        rollout_id: measured.rollout_id,
        segment_id: measured.segment_id,
        path: path.to_path_buf(),
        history_base: None,
        start_ordinal: 0,
        end_ordinal_exclusive: measured.target_end_ordinal_exclusive,
        byte_count: measured.target_byte_count,
        record_count: measured.target_record_count,
        sha256: measured.target_sha256,
        selected: true,
        compressed_on_publication: path.extension().is_some_and(|extension| extension == "zst"),
    };
    Ok(RolloutMigrationManifest {
        version: LINEAGE_MANIFEST_VERSION,
        input_kind: RolloutMigrationManifestKind::SingleRollout,
        selected_thread_id: measured.thread_id,
        selected_source_rollout_id: measured.rollout_id,
        source_bytes: source.byte_count,
        dependency_bytes: 0,
        target_payload_bytes: target.byte_count,
        minimum_free_bytes: target.byte_count,
        sources: vec![source],
        history_base_dependencies: Vec::new(),
        reference_dependencies: Vec::new(),
        targets: vec![target],
        additional_free_space: RolloutMigrationAdditionalFreeSpace {
            sqlite_projection: "filesystem-dependent SQLite page allocation",
            filesystem_metadata: "filesystem-dependent directory and allocation metadata",
            dry_run_decompression_temporary: if path
                .extension()
                .is_some_and(|extension| extension == "zst")
                && kind == RolloutMigrationKind::Subagent
            {
                "one decompressed source copy for bounded-context reverse scan"
            } else {
                "none"
            },
            compressed_publication_temporary: "exact only after streaming compression",
        },
        publication_phases: vec![
            RolloutMigrationPublicationPhase::Planned,
            RolloutMigrationPublicationPhase::TargetsDurable,
            RolloutMigrationPublicationPhase::ProjectionDurable,
            RolloutMigrationPublicationPhase::Selected,
            RolloutMigrationPublicationPhase::Verified,
            RolloutMigrationPublicationPhase::Complete,
        ],
        sources_retained_after_apply: false,
    })
}
