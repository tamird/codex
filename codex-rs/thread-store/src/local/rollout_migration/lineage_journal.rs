//! Durable transaction journal for a lineage-wide rollout migration.
//!
//! The ordinary one-file migration journal is an empty marker because the selected path names the
//! only replacement. A lineage migration publishes several immutable files before it replaces the
//! selected active rollout, so recovery needs an authenticated manifest and an explicit phase.

use std::path::Path;
use std::path::PathBuf;

use codex_protocol::RolloutId;
use codex_protocol::SegmentId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use super::lineage::LegacyLineageMigrationPlan;
use super::lineage::hash_file;
use super::lineage_stage::StagedLineageTarget;
use super::migration_error;
use super::publish::sync_parent_directory;
use crate::ThreadStoreResult;

const PREVIOUS_LINEAGE_MIGRATION_JOURNAL_VERSIONS: &[u32] = &[2, 3];
const LINEAGE_MIGRATION_JOURNAL_VERSION: u32 = 4;

/// Durable publication boundary reached by a lineage migration transaction.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum LineageMigrationPhase {
    Planned,
    TargetsDurable,
    ProjectionDurable,
    Selected,
    Verified,
    Complete,
}

/// Versioned source and target manifest used for restart recovery.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct LineageMigrationJournal {
    version: u32,
    /// Absent in older v4 journals, whose authenticated source graph must not be replanned.
    #[serde(default)]
    pub(super) reuse_native_prefixes: bool,
    /// Absent in journals written before mixed-format rollback replay.
    #[serde(default)]
    pub(super) replay_native_rollbacks: bool,
    pub(super) selected_thread_id: ThreadId,
    pub(super) selected_source_rollout_id: RolloutId,
    pub(super) phase: LineageMigrationPhase,
    pub(super) sources: Vec<LineageMigrationJournalSource>,
    /// Authenticated ancestry files omitted by a selected prefix or filter.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) authentication_sources: Vec<LineageMigrationJournalSource>,
    pub(super) history_bases: Vec<LineageMigrationJournalHistoryBase>,
    pub(super) reference_dependencies: Vec<LineageMigrationJournalReference>,
    pub(super) targets: Vec<LineageMigrationJournalTarget>,
}

/// Source identity rechecked before publication or recovery.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct LineageMigrationJournalSource {
    pub(super) thread_id: ThreadId,
    pub(super) rollout_id: RolloutId,
    pub(super) segment_id: Option<SegmentId>,
    pub(super) path: PathBuf,
    pub(super) byte_count: u64,
    pub(super) record_count: u64,
    pub(super) sha256: String,
    /// Decoded source prefix used by mixed-format rollback replay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) replay_end: Option<HistoryPosition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) native_replay: Option<super::lineage::NativeReplayRange>,
    #[serde(default)]
    pub(super) materialized_predecessor: bool,
}

/// An external Paginated prefix that must remain unchanged through selection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct LineageMigrationJournalHistoryBase {
    pub(super) position: HistoryPosition,
    pub(super) thread_id: ThreadId,
    pub(super) rollout_id: RolloutId,
    pub(super) path: PathBuf,
    pub(super) byte_count: u64,
    pub(super) record_count: u64,
    pub(super) sha256: String,
}

/// An immutable Paginated segment retained by one successor reference.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct LineageMigrationJournalReference {
    pub(super) successor_rollout_id: RolloutId,
    pub(super) thread_id: ThreadId,
    pub(super) rollout_id: RolloutId,
    pub(super) segment_id: SegmentId,
    pub(super) path: PathBuf,
    pub(super) end_ordinal_exclusive: u64,
    /// Decoded JSONL boundary; older journals only retained filtered references.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) end_byte_offset: Option<u64>,
    pub(super) byte_count: u64,
    pub(super) record_count: u64,
    pub(super) sha256: String,
}

/// Unpublished or published target identity. Paths are never inferred during recovery.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct LineageMigrationJournalTarget {
    pub(super) thread_id: ThreadId,
    pub(super) rollout_id: RolloutId,
    pub(super) segment_id: Option<SegmentId>,
    pub(super) path: PathBuf,
    pub(super) selected: bool,
    pub(super) predecessor_segment_id: Option<SegmentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) staged_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) start_ordinal: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) end_ordinal_exclusive: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) byte_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) record_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) published_sha256: Option<String>,
}

impl LineageMigrationJournal {
    pub(super) fn from_plan(plan: &LegacyLineageMigrationPlan) -> Self {
        Self {
            version: LINEAGE_MIGRATION_JOURNAL_VERSION,
            reuse_native_prefixes: plan.reuse_native_prefixes,
            replay_native_rollbacks: plan.replay_native_rollbacks,
            selected_thread_id: plan.selected_thread_id,
            selected_source_rollout_id: plan.selected_rollout_id,
            phase: LineageMigrationPhase::Planned,
            sources: plan.sources.iter().map(journal_source).collect(),
            authentication_sources: plan
                .authentication_sources
                .iter()
                .map(journal_source)
                .collect(),
            history_bases: plan
                .history_bases
                .iter()
                .map(|source| LineageMigrationJournalHistoryBase {
                    position: source.position,
                    thread_id: source.thread_id,
                    rollout_id: source.rollout_id,
                    path: source.path.clone(),
                    byte_count: source.byte_count,
                    record_count: source.record_count,
                    sha256: source.sha256.clone(),
                })
                .collect(),
            reference_dependencies: plan
                .reference_dependencies
                .iter()
                .map(|source| LineageMigrationJournalReference {
                    successor_rollout_id: source.successor_rollout_id,
                    thread_id: source.thread_id,
                    rollout_id: source.rollout_id,
                    segment_id: source.segment_id,
                    path: source.path.clone(),
                    end_ordinal_exclusive: source.end_ordinal_exclusive,
                    end_byte_offset: Some(source.end_byte_offset),
                    byte_count: source.byte_count,
                    record_count: source.record_count,
                    sha256: source.sha256.clone(),
                })
                .collect(),
            targets: plan
                .targets
                .iter()
                .map(|target| LineageMigrationJournalTarget {
                    thread_id: target.thread_id,
                    rollout_id: target.rollout_id,
                    segment_id: target.segment_id,
                    path: target.path.clone(),
                    selected: target.selected,
                    predecessor_segment_id: target.predecessor_segment_id,
                    staged_path: None,
                    start_ordinal: None,
                    end_ordinal_exclusive: None,
                    byte_count: None,
                    record_count: None,
                    sha256: None,
                    published_sha256: None,
                })
                .collect(),
        }
    }

    pub(super) fn verify_plan(&self, plan: &LegacyLineageMigrationPlan) -> ThreadStoreResult<()> {
        let targets_match = self.targets.len() == plan.targets.len()
            && self
                .targets
                .iter()
                .zip(&plan.targets)
                .all(|(journal, target)| {
                    journal.thread_id == target.thread_id
                        && journal.rollout_id == target.rollout_id
                        && journal.segment_id == target.segment_id
                        && journal.path == target.path
                        && journal.selected == target.selected
                        && journal.predecessor_segment_id == target.predecessor_segment_id
                });
        let previous_selected_journal = PREVIOUS_LINEAGE_MIGRATION_JOURNAL_VERSIONS
            .contains(&self.version)
            && phase_rank(self.phase) >= phase_rank(LineageMigrationPhase::Selected);
        if !self.authenticated_inputs_match(plan) || (!previous_selected_journal && !targets_match)
        {
            return Err(migration_error(
                "pending lineage migration manifest does not match the current authenticated plan",
            ));
        }
        Ok(())
    }

    /// Accepts only a complete-record append to the selected source of an unpublished plan.
    /// Every immutable dependency and every previously authenticated source byte must still match.
    pub(super) async fn permits_selected_source_append(
        &self,
        plan: &LegacyLineageMigrationPlan,
    ) -> ThreadStoreResult<bool> {
        if self.version != LINEAGE_MIGRATION_JOURNAL_VERSION
            || !matches!(
                self.phase,
                LineageMigrationPhase::Planned
                    | LineageMigrationPhase::TargetsDurable
                    | LineageMigrationPhase::ProjectionDurable
            )
        {
            return Ok(false);
        }
        let Some(previous) = self.sources.last() else {
            return Ok(false);
        };
        let Some(current) = plan.sources.last() else {
            return Ok(false);
        };
        if current
            .path
            .extension()
            .is_none_or(|extension| extension != "jsonl")
            || current.byte_count <= previous.byte_count
            || current.record_count <= previous.record_count
        {
            return Ok(false);
        }
        let mut previous_plan = plan.clone();
        let Some(source) = previous_plan.sources.last_mut() else {
            return Ok(false);
        };
        source.byte_count = previous.byte_count;
        source.record_count = previous.record_count;
        source.sha256.clone_from(&previous.sha256);
        if !self.authenticated_inputs_match(&previous_plan)
            || self.targets.iter().any(|old| {
                plan.targets
                    .iter()
                    .any(|new| old.rollout_id == new.rollout_id)
            })
        {
            return Ok(false);
        }
        let mut file = tokio::fs::File::open(&current.path)
            .await
            .map_err(migration_error)?
            .take(previous.byte_count);
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 256 * 1024];
        let mut bytes = 0_u64;
        let mut last_byte = None;
        loop {
            let read = file.read(&mut buffer).await.map_err(migration_error)?;
            if read == 0 {
                break;
            }
            bytes += read as u64;
            last_byte = Some(buffer[read - 1]);
            hasher.update(&buffer[..read]);
        }
        Ok(bytes == previous.byte_count
            && last_byte == Some(b'\n')
            && format!("{:x}", hasher.finalize()) == previous.sha256)
    }

    /// Returns true when a pre-selection journal uses the previous unscoped target identity.
    pub(super) fn requires_target_identity_upgrade(
        &self,
        plan: &LegacyLineageMigrationPlan,
    ) -> ThreadStoreResult<bool> {
        if self.version == LINEAGE_MIGRATION_JOURNAL_VERSION {
            return Ok(false);
        }
        if !PREVIOUS_LINEAGE_MIGRATION_JOURNAL_VERSIONS.contains(&self.version)
            || !self.authenticated_inputs_match(plan)
        {
            return Err(migration_error(
                "pending lineage migration manifest does not match the current authenticated plan",
            ));
        }
        Ok(phase_rank(self.phase) <= phase_rank(LineageMigrationPhase::ProjectionDurable))
    }

    fn authenticated_inputs_match(&self, plan: &LegacyLineageMigrationPlan) -> bool {
        let sources_match = self.sources.len() == plan.sources.len()
            && self
                .sources
                .iter()
                .zip(&plan.sources)
                .all(|(journal, source)| {
                    journal.thread_id == source.thread_id
                        && journal.rollout_id == source.rollout_id
                        && journal.segment_id == source.segment_id
                        && journal.path == source.path
                        && journal.byte_count == source.byte_count
                        && journal.record_count == source.record_count
                        && journal.sha256 == source.sha256
                        && journal.replay_end == source.replay_end
                        && journal.native_replay == source.native_replay
                        && journal.materialized_predecessor == source.materialized_predecessor
                });
        let history_bases_match = self.history_bases.len() == plan.history_bases.len()
            && self
                .history_bases
                .iter()
                .zip(&plan.history_bases)
                .all(|(journal, source)| {
                    journal.position == source.position
                        && journal.thread_id == source.thread_id
                        && journal.rollout_id == source.rollout_id
                        && journal.path == source.path
                        && journal.byte_count == source.byte_count
                        && journal.record_count == source.record_count
                        && journal.sha256 == source.sha256
                });
        let references_match = self.reference_dependencies.len()
            == plan.reference_dependencies.len()
            && self
                .reference_dependencies
                .iter()
                .zip(&plan.reference_dependencies)
                .all(|(journal, source)| {
                    journal.successor_rollout_id == source.successor_rollout_id
                        && journal.thread_id == source.thread_id
                        && journal.rollout_id == source.rollout_id
                        && journal.segment_id == source.segment_id
                        && journal.path == source.path
                        && journal.end_ordinal_exclusive == source.end_ordinal_exclusive
                        && journal
                            .end_byte_offset
                            .is_none_or(|offset| offset == source.end_byte_offset)
                        && journal.byte_count == source.byte_count
                        && journal.record_count == source.record_count
                        && journal.sha256 == source.sha256
                });
        self.selected_thread_id == plan.selected_thread_id
            && self.authentication_sources
                == plan
                    .authentication_sources
                    .iter()
                    .map(journal_source)
                    .collect::<Vec<_>>()
            && self.selected_source_rollout_id == plan.selected_rollout_id
            && sources_match
            && history_bases_match
            && references_match
    }

    pub(super) fn advance(&mut self, phase: LineageMigrationPhase) -> ThreadStoreResult<()> {
        if phase_rank(phase) < phase_rank(self.phase) {
            return Err(migration_error(format!(
                "lineage migration journal cannot move backward from {:?} to {phase:?}",
                self.phase
            )));
        }
        self.phase = phase;
        Ok(())
    }

    pub(super) fn record_staged_targets(
        &mut self,
        staged: &[StagedLineageTarget],
    ) -> ThreadStoreResult<()> {
        if staged.len() != self.targets.len() {
            return Err(migration_error(
                "staged lineage target count does not match journal",
            ));
        }
        for (journal, staged) in self.targets.iter_mut().zip(staged) {
            if journal.thread_id != staged.thread_id
                || journal.rollout_id != staged.rollout_id
                || journal.segment_id != staged.segment_id
                || journal.path != staged.final_path
                || journal.selected != staged.selected
            {
                return Err(migration_error(
                    "staged lineage target identity does not match journal",
                ));
            }
            journal.staged_path = Some(staged.staged_path.clone());
            journal.start_ordinal = Some(staged.start_ordinal);
            journal.end_ordinal_exclusive = Some(staged.end_ordinal_exclusive);
            journal.byte_count = Some(staged.byte_count);
            journal.record_count = Some(staged.record_count);
            journal.sha256 = Some(staged.sha256.clone());
        }
        self.advance(LineageMigrationPhase::TargetsDurable)
    }

    pub(super) async fn verify_staged_targets(&self) -> ThreadStoreResult<()> {
        for target in &self.targets {
            let path = target.staged_path.as_ref().ok_or_else(|| {
                migration_error("lineage migration journal target has no staged path")
            })?;
            let expected_bytes = target.byte_count.ok_or_else(|| {
                migration_error("lineage migration journal target has no byte count")
            })?;
            let expected_sha = target.sha256.as_deref().ok_or_else(|| {
                migration_error("lineage migration journal target has no SHA-256")
            })?;
            let (byte_count, sha256) = hash_file(path.as_path()).await?;
            if byte_count != expected_bytes || sha256 != expected_sha {
                return Err(migration_error(format!(
                    "staged lineage migration target changed: {}",
                    path.display()
                )));
            }
        }
        Ok(())
    }

    pub(super) async fn verify_sources(&self) -> ThreadStoreResult<()> {
        for source in self.sources.iter().chain(&self.authentication_sources) {
            let (byte_count, sha256) = hash_file(source.path.as_path()).await?;
            if byte_count != source.byte_count || sha256 != source.sha256 {
                return Err(migration_error(format!(
                    "lineage migration source changed after planning: {}",
                    source.path.display()
                )));
            }
        }
        for source in &self.history_bases {
            let (byte_count, sha256) = hash_file(source.path.as_path()).await?;
            if byte_count != source.byte_count || sha256 != source.sha256 {
                return Err(migration_error(format!(
                    "lineage migration history_base source changed after planning: {}",
                    source.path.display()
                )));
            }
        }
        for source in &self.reference_dependencies {
            let (byte_count, sha256) = hash_file(source.path.as_path()).await?;
            if byte_count != source.byte_count || sha256 != source.sha256 {
                return Err(migration_error(format!(
                    "lineage migration reference source changed after planning: {}",
                    source.path.display()
                )));
            }
        }
        Ok(())
    }
}

fn journal_source(source: &super::lineage::LegacyLineageSource) -> LineageMigrationJournalSource {
    LineageMigrationJournalSource {
        thread_id: source.thread_id,
        rollout_id: source.rollout_id,
        segment_id: source.segment_id,
        path: source.path.clone(),
        byte_count: source.byte_count,
        record_count: source.record_count,
        sha256: source.sha256.clone(),
        replay_end: source.replay_end,
        native_replay: source.native_replay.clone(),
        materialized_predecessor: source.materialized_predecessor,
    }
}

fn phase_rank(phase: LineageMigrationPhase) -> u8 {
    match phase {
        LineageMigrationPhase::Planned => 0,
        LineageMigrationPhase::TargetsDurable => 1,
        LineageMigrationPhase::ProjectionDurable => 2,
        LineageMigrationPhase::Selected => 3,
        LineageMigrationPhase::Verified => 4,
        LineageMigrationPhase::Complete => 5,
    }
}

pub(super) async fn write_lineage_migration_journal(
    path: &Path,
    journal: &LineageMigrationJournal,
) -> ThreadStoreResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| migration_error("lineage migration journal has no parent directory"))?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(migration_error)?;
    let filename = path
        .file_name()
        .and_then(|filename| filename.to_str())
        .ok_or_else(|| migration_error("lineage migration journal has no valid filename"))?;
    let staged = path.with_file_name(format!(".{filename}.next"));
    let mut bytes = serde_json::to_vec(journal).map_err(migration_error)?;
    bytes.push(b'\n');
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(staged.as_path())
        .await
        .map_err(migration_error)?;
    file.write_all(bytes.as_slice())
        .await
        .map_err(migration_error)?;
    file.sync_all().await.map_err(migration_error)?;
    drop(file);
    tokio::fs::rename(staged.as_path(), path)
        .await
        .map_err(migration_error)?;
    sync_parent_directory(path).await
}

pub(super) async fn read_lineage_migration_journal(
    path: &Path,
) -> ThreadStoreResult<LineageMigrationJournal> {
    let bytes = tokio::fs::read(path).await.map_err(migration_error)?;
    let journal = serde_json::from_slice::<LineageMigrationJournal>(bytes.as_slice())
        .map_err(migration_error)?;
    if journal.version != LINEAGE_MIGRATION_JOURNAL_VERSION
        && !PREVIOUS_LINEAGE_MIGRATION_JOURNAL_VERSIONS.contains(&journal.version)
    {
        return Err(migration_error(format!(
            "unsupported lineage migration journal version {}",
            journal.version
        )));
    }
    Ok(journal)
}
