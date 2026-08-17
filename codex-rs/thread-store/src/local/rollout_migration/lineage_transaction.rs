//! Executes and recovers a same-thread segmented Legacy migration transaction.

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::ThreadId;

use super::ClassifiedMigrationResult;
use super::LocalThreadStore;
use super::RolloutMigrationFailure;
use super::RolloutMigrationFailureReason::InterruptedMigrationRecoveryFailed;
use super::RolloutMigrationFailureReason::LegacyRolloutConversionFailed;
use super::RolloutMigrationFailureReason::MissingSqliteMetadata;
use super::RolloutMigrationFailureReason::RolloutPublishFailed;
use super::RolloutMigrationFailureReason::RolloutReadFailed;
use super::RolloutMigrationFailureReason::SqliteMaterializationFailed;
use super::RolloutMigrationRateLimiter;
use super::distinct_thread_metadata_title;
use super::lineage::LegacyLineageMigrationPlan;
use super::lineage::LegacyLineagePredecessor;
use super::lineage::plan_legacy_lineage;
use super::lineage::validate_segment_migration;
use super::lineage_journal::LineageMigrationJournal;
use super::lineage_journal::LineageMigrationPhase;
use super::lineage_journal::read_lineage_migration_journal;
use super::lineage_journal::write_lineage_migration_journal;
use super::lineage_publish::publish_lineage_targets;
use super::lineage_publish::verify_published_lineage_targets;
use super::lineage_stage::stage_legacy_lineage;
use super::migration_error;
use super::publish::decompress_rollout_to_path;
use super::publish::sync_parent_directory;
use super::thread_history;
use super::with_failure_reason;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

impl LocalThreadStore {
    pub(super) async fn validate_legacy_lineage_plan(
        &self,
        plan: &LegacyLineageMigrationPlan,
    ) -> ThreadStoreResult<()> {
        validate_segment_migration(plan)?;
        for dependency in &plan.history_bases {
            let lineage = self.resolve_rollout_lineage_at(dependency.position).await?;
            if lineage.root_rollout_id != dependency.rollout_id {
                return Err(migration_error(
                    "history_base validation resolved another physical rollout",
                ));
            }
        }
        for dependency in &plan.reference_dependencies {
            let reference = plan
                .sources
                .iter()
                .find(|source| source.rollout_id == dependency.successor_rollout_id)
                .and_then(|source| match source.predecessor.as_ref() {
                    Some(LegacyLineagePredecessor::RolloutReference(reference)) => Some(reference),
                    _ => None,
                })
                .ok_or_else(|| {
                    migration_error("Paginated reference dependency has no successor edge")
                })?;
            let resolved = codex_rollout::resolve_rollout_reference_path(
                self.config.codex_home.as_path(),
                reference,
            )
            .await
            .map_err(migration_error)?;
            let resolved = tokio::fs::canonicalize(resolved)
                .await
                .map_err(migration_error)?;
            let expected = tokio::fs::canonicalize(dependency.path.as_path())
                .await
                .map_err(migration_error)?;
            if resolved != expected {
                return Err(migration_error(
                    "Paginated reference dependency resolved another physical rollout",
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn validate_legacy_lineage_desktop_compatibility(
        &self,
        plan: &LegacyLineageMigrationPlan,
    ) -> ThreadStoreResult<()> {
        // Paginated sources already persist stable turn and item identities. The bounded-view
        // comparison below protects Legacy synthetic IDs, which can depend on how many
        // predecessor segments a reader materializes. Applying that comparison to a Paginated
        // reference chain would reject a lossless migration whenever an ordinary bounded view
        // intentionally omits older item bodies.
        if plan.sources.iter().all(|source| {
            source.history_mode == codex_protocol::protocol::ThreadHistoryMode::Paginated
        }) {
            return Ok(());
        }
        super::lineage_compatibility::validate_bounded_desktop_history(
            self.config.codex_home.as_path(),
            plan,
        )
        .await
    }

    pub(super) async fn cleanup_unpublished_lineage_migration(
        &self,
        journal_path: &Path,
    ) -> ThreadStoreResult<()> {
        if !tokio::fs::try_exists(journal_path)
            .await
            .map_err(migration_error)?
        {
            return Ok(());
        }
        let journal = read_lineage_migration_journal(journal_path).await?;
        if !matches!(
            journal.phase,
            LineageMigrationPhase::Planned | LineageMigrationPhase::TargetsDurable
        ) {
            return Ok(());
        }
        for target in &journal.targets {
            thread_history::delete_thread(self, target.rollout_id).await?;
        }
        let stage_root = journal_path.with_extension("staging");
        if tokio::fs::try_exists(stage_root.as_path())
            .await
            .map_err(migration_error)?
        {
            tokio::fs::remove_dir_all(stage_root.as_path())
                .await
                .map_err(migration_error)?;
        }
        tokio::fs::remove_file(journal_path)
            .await
            .map_err(migration_error)?;
        sync_parent_directory(journal_path).await
    }

    pub(super) async fn migrate_legacy_lineage(
        &self,
        selected_source_path: &Path,
        journal_path: &Path,
        plan: LegacyLineageMigrationPlan,
        legacy_names: &HashMap<ThreadId, String>,
        limiter: &mut RolloutMigrationRateLimiter,
    ) -> ClassifiedMigrationResult<PathBuf> {
        self.migrate_legacy_lineage_inner(
            selected_source_path,
            journal_path,
            plan,
            legacy_names,
            limiter,
            /*stop_after*/ None,
        )
        .await
    }

    #[cfg(test)]
    pub(super) async fn migrate_legacy_lineage_until_phase_for_test(
        &self,
        selected_source_path: &Path,
        journal_path: &Path,
        plan: LegacyLineageMigrationPlan,
        limiter: &mut RolloutMigrationRateLimiter,
        stop_after: LineageMigrationPhase,
    ) -> ThreadStoreResult<PathBuf> {
        self.migrate_legacy_lineage_inner(
            selected_source_path,
            journal_path,
            plan,
            &HashMap::new(),
            limiter,
            Some(stop_after),
        )
        .await
        .map_err(|failure| failure.error)
    }

    pub(super) async fn recover_legacy_lineage(
        &self,
        journal_path: &Path,
        legacy_names: &HashMap<ThreadId, String>,
        limiter: &mut RolloutMigrationRateLimiter,
    ) -> ThreadStoreResult<PathBuf> {
        let journal = read_lineage_migration_journal(journal_path).await?;
        let selected_source_path = journal
            .sources
            .last()
            .map(|source| source.path.clone())
            .ok_or_else(|| migration_error("lineage journal has no selected source"))?;
        let plan = plan_legacy_lineage(
            self.config.codex_home.as_path(),
            selected_source_path.as_path(),
        )
        .await?;
        // The caller classifies the complete recovery attempt, including journal planning.
        self.migrate_legacy_lineage(
            selected_source_path.as_path(),
            journal_path,
            plan,
            legacy_names,
            limiter,
        )
        .await
        .map_err(|failure| failure.error)
    }

    async fn migrate_legacy_lineage_inner(
        &self,
        selected_source_path: &Path,
        journal_path: &Path,
        plan: LegacyLineageMigrationPlan,
        legacy_names: &HashMap<ThreadId, String>,
        limiter: &mut RolloutMigrationRateLimiter,
        stop_after: Option<LineageMigrationPhase>,
    ) -> ClassifiedMigrationResult<PathBuf> {
        with_failure_reason(
            self.validate_legacy_lineage_plan(&plan).await,
            LegacyRolloutConversionFailed,
        )?;
        with_failure_reason(
            self.validate_legacy_lineage_desktop_compatibility(&plan)
                .await,
            LegacyRolloutConversionFailed,
        )?;
        let stage_root = journal_path.with_extension("staging");
        let journal_exists = with_failure_reason(
            tokio::fs::try_exists(journal_path)
                .await
                .map_err(migration_error),
            InterruptedMigrationRecoveryFailed,
        )?;
        let mut journal = if journal_exists {
            with_failure_reason(
                read_lineage_migration_journal(journal_path).await,
                InterruptedMigrationRecoveryFailed,
            )?
        } else {
            let journal = LineageMigrationJournal::from_plan(&plan);
            with_failure_reason(
                write_lineage_migration_journal(journal_path, &journal).await,
                RolloutPublishFailed,
            )?;
            journal
        };
        let requires_target_identity_upgrade = with_failure_reason(
            journal.requires_target_identity_upgrade(&plan),
            InterruptedMigrationRecoveryFailed,
        )?;
        if requires_target_identity_upgrade {
            with_failure_reason(
                self.restart_preselection_lineage_after_target_upgrade(
                    journal_path,
                    &journal,
                    limiter,
                )
                .await,
                InterruptedMigrationRecoveryFailed,
            )?;
            journal = LineageMigrationJournal::from_plan(&plan);
            with_failure_reason(
                write_lineage_migration_journal(journal_path, &journal).await,
                RolloutPublishFailed,
            )?;
        }
        with_failure_reason(
            journal.verify_plan(&plan),
            InterruptedMigrationRecoveryFailed,
        )?;

        if journal.phase == LineageMigrationPhase::Planned {
            with_failure_reason(
                stop_after_phase(stop_after, journal.phase),
                InterruptedMigrationRecoveryFailed,
            )?;
            with_failure_reason(journal.verify_sources().await, RolloutReadFailed)?;
            // Staging failures describe conversion, while recording its durable checkpoint
            // below is publication. Keep the original cause across that boundary.
            let conversion_result = async {
                if tokio::fs::try_exists(stage_root.as_path())
                    .await
                    .map_err(migration_error)?
                {
                    tokio::fs::remove_dir_all(stage_root.as_path())
                        .await
                        .map_err(migration_error)?;
                }
                let staged = stage_legacy_lineage(&plan, stage_root.as_path()).await?;
                journal.record_staged_targets(staged.as_slice())
            }
            .await;
            with_failure_reason(conversion_result, LegacyRolloutConversionFailed)?;
            with_failure_reason(
                write_lineage_migration_journal(journal_path, &journal).await,
                RolloutPublishFailed,
            )?;
            with_failure_reason(
                stop_after_phase(stop_after, journal.phase),
                InterruptedMigrationRecoveryFailed,
            )?;
        }

        if journal.phase == LineageMigrationPhase::TargetsDurable {
            with_failure_reason(journal.verify_sources().await, RolloutReadFailed)?;
            with_failure_reason(
                journal.verify_staged_targets().await,
                InterruptedMigrationRecoveryFailed,
            )?;
            for target in &journal.targets {
                let staged_path = with_failure_reason(
                    target.staged_path.as_ref().ok_or_else(|| {
                        migration_error("lineage target is missing its staged path")
                    }),
                    InterruptedMigrationRecoveryFailed,
                )?;
                with_failure_reason(
                    thread_history::delete_thread(self, target.rollout_id).await,
                    SqliteMaterializationFailed,
                )?;
                let start_ordinal = with_failure_reason(
                    target.start_ordinal.ok_or_else(|| {
                        migration_error("lineage target is missing its start ordinal")
                    }),
                    InterruptedMigrationRecoveryFailed,
                )?;
                with_failure_reason(
                    thread_history::reset_projection_for_replacement(
                        self,
                        target.rollout_id,
                        start_ordinal,
                    )
                    .await,
                    SqliteMaterializationFailed,
                )?;
                with_failure_reason(
                    self.project_rollout_in_batches(target.rollout_id, staged_path, limiter)
                        .await,
                    SqliteMaterializationFailed,
                )?;
                let projection = with_failure_reason(
                    thread_history::projection_state(self, target.rollout_id).await,
                    SqliteMaterializationFailed,
                )?;
                let projection = with_failure_reason(
                    projection.ok_or_else(|| {
                        migration_error("staged lineage target has no SQLite projection")
                    }),
                    SqliteMaterializationFailed,
                )?;
                let byte_count = with_failure_reason(
                    target
                        .byte_count
                        .ok_or_else(|| migration_error("lineage target is missing its byte count")),
                    InterruptedMigrationRecoveryFailed,
                )?;
                let projection_complete = if projection.next_byte_offset == byte_count {
                    let end_ordinal_exclusive = with_failure_reason(
                        target.end_ordinal_exclusive.ok_or_else(|| {
                            migration_error("lineage target is missing its ordinal boundary")
                        }),
                        InterruptedMigrationRecoveryFailed,
                    )?;
                    projection.next_ordinal == end_ordinal_exclusive
                } else {
                    false
                };
                if !projection_complete {
                    return Err(RolloutMigrationFailure::new(
                        SqliteMaterializationFailed,
                        migration_error(
                            "SQLite projection does not cover a complete staged lineage target",
                        ),
                    ));
                }
            }
            with_failure_reason(
                journal.advance(LineageMigrationPhase::ProjectionDurable),
                InterruptedMigrationRecoveryFailed,
            )?;
            with_failure_reason(
                write_lineage_migration_journal(journal_path, &journal).await,
                RolloutPublishFailed,
            )?;
            with_failure_reason(
                stop_after_phase(stop_after, journal.phase),
                InterruptedMigrationRecoveryFailed,
            )?;
        }

        if journal.phase == LineageMigrationPhase::ProjectionDurable {
            with_failure_reason(journal.verify_sources().await, RolloutReadFailed)?;
            with_failure_reason(
                publish_lineage_targets(journal_path, &mut journal).await,
                RolloutPublishFailed,
            )?;
            let selected_target = with_failure_reason(
                journal
                    .targets
                    .iter()
                    .find(|target| target.selected)
                    .ok_or_else(|| migration_error("lineage journal has no selected target")),
                InterruptedMigrationRecoveryFailed,
            )?;
            let state_db = self.state_db.as_ref().ok_or_else(|| {
                RolloutMigrationFailure::new(
                    MissingSqliteMetadata,
                    migration_error("lineage migration requires SQLite thread metadata"),
                )
            })?;
            let current = with_failure_reason(
                state_db
                    .get_thread(journal.selected_thread_id)
                    .await
                    .map_err(migration_error),
                RolloutPublishFailed,
            )?;
            let current = current.ok_or_else(|| {
                RolloutMigrationFailure::new(
                    MissingSqliteMetadata,
                    migration_error("selected lineage thread is missing"),
                )
            })?;
            if current.rollout_path != selected_target.path {
                let replaced = with_failure_reason(
                    state_db
                        .replace_rollout_path_if_current(
                            journal.selected_thread_id,
                            selected_source_path,
                            selected_target.path.as_path(),
                        )
                        .await
                        .map_err(migration_error),
                    RolloutPublishFailed,
                )?;
                if !replaced {
                    return Err(RolloutMigrationFailure::new(
                        RolloutPublishFailed,
                        ThreadStoreError::Conflict {
                            message: "selected rollout changed during lineage migration"
                                .to_string(),
                        },
                    ));
                }
            }
            let legacy_name = distinct_thread_metadata_title(&current)
                .or_else(|| legacy_names.get(&journal.selected_thread_id).cloned())
                .filter(|name| !name.trim().is_empty());
            let thread_marked_paginated = with_failure_reason(
                state_db
                    .mark_thread_paginated(journal.selected_thread_id, legacy_name.as_deref())
                    .await
                    .map_err(migration_error),
                RolloutPublishFailed,
            )?;
            if !thread_marked_paginated {
                return Err(RolloutMigrationFailure::new(
                    MissingSqliteMetadata,
                    migration_error("selected lineage thread is missing"),
                ));
            }
            with_failure_reason(
                journal.advance(LineageMigrationPhase::Selected),
                InterruptedMigrationRecoveryFailed,
            )?;
            with_failure_reason(
                write_lineage_migration_journal(journal_path, &journal).await,
                RolloutPublishFailed,
            )?;
            with_failure_reason(
                stop_after_phase(stop_after, journal.phase),
                InterruptedMigrationRecoveryFailed,
            )?;
        }

        if journal.phase == LineageMigrationPhase::Selected {
            with_failure_reason(
                verify_published_lineage_targets(&journal).await,
                RolloutPublishFailed,
            )?;
            let selected_target = with_failure_reason(
                journal
                    .targets
                    .iter()
                    .find(|target| target.selected)
                    .ok_or_else(|| migration_error("lineage journal has no selected target")),
                InterruptedMigrationRecoveryFailed,
            )?;
            let state_db = self.state_db.as_ref().ok_or_else(|| {
                RolloutMigrationFailure::new(
                    MissingSqliteMetadata,
                    migration_error("lineage migration requires SQLite metadata"),
                )
            })?;
            let selected = with_failure_reason(
                state_db
                    .get_thread(journal.selected_thread_id)
                    .await
                    .map_err(migration_error),
                RolloutPublishFailed,
            )?;
            let selected = selected.ok_or_else(|| {
                RolloutMigrationFailure::new(
                    MissingSqliteMetadata,
                    migration_error("selected lineage thread is missing"),
                )
            })?;
            if selected.rollout_path != selected_target.path {
                return Err(RolloutMigrationFailure::new(
                    RolloutPublishFailed,
                    migration_error("selected lineage target does not match SQLite metadata"),
                ));
            }
            for target in &journal.targets {
                let projection = with_failure_reason(
                    thread_history::projection_state(self, target.rollout_id).await,
                    SqliteMaterializationFailed,
                )?;
                let projection = with_failure_reason(
                    projection.ok_or_else(|| {
                        migration_error("published lineage target has no SQLite projection")
                    }),
                    SqliteMaterializationFailed,
                )?;
                let byte_count = with_failure_reason(
                    target.byte_count.ok_or_else(|| {
                        migration_error("published lineage target is missing its byte count")
                    }),
                    InterruptedMigrationRecoveryFailed,
                )?;
                let projection_complete = if projection.next_byte_offset == byte_count {
                    let end_ordinal_exclusive = with_failure_reason(
                        target.end_ordinal_exclusive.ok_or_else(|| {
                            migration_error(
                                "published lineage target is missing its ordinal boundary",
                            )
                        }),
                        InterruptedMigrationRecoveryFailed,
                    )?;
                    projection.next_ordinal == end_ordinal_exclusive
                } else {
                    false
                };
                if !projection_complete {
                    return Err(RolloutMigrationFailure::new(
                        SqliteMaterializationFailed,
                        migration_error("published lineage projection failed verification"),
                    ));
                }
            }
            with_failure_reason(
                journal.advance(LineageMigrationPhase::Verified),
                InterruptedMigrationRecoveryFailed,
            )?;
            with_failure_reason(
                write_lineage_migration_journal(journal_path, &journal).await,
                RolloutPublishFailed,
            )?;
            with_failure_reason(
                stop_after_phase(stop_after, journal.phase),
                InterruptedMigrationRecoveryFailed,
            )?;
        }

        if journal.phase == LineageMigrationPhase::Verified {
            with_failure_reason(
                journal.advance(LineageMigrationPhase::Complete),
                InterruptedMigrationRecoveryFailed,
            )?;
            with_failure_reason(
                write_lineage_migration_journal(journal_path, &journal).await,
                RolloutPublishFailed,
            )?;
            with_failure_reason(
                stop_after_phase(stop_after, journal.phase),
                InterruptedMigrationRecoveryFailed,
            )?;
        }
        if journal.phase != LineageMigrationPhase::Complete {
            return Err(RolloutMigrationFailure::new(
                InterruptedMigrationRecoveryFailed,
                migration_error("lineage migration stopped before completion"),
            ));
        }
        let selected_path = with_failure_reason(
            journal
                .targets
                .iter()
                .find(|target| target.selected)
                .map(|target| target.path.clone())
                .ok_or_else(|| migration_error("lineage journal has no selected target")),
            InterruptedMigrationRecoveryFailed,
        )?;
        let cleanup_result = async {
            if tokio::fs::try_exists(stage_root.as_path())
                .await
                .map_err(migration_error)?
            {
                tokio::fs::remove_dir_all(stage_root.as_path())
                    .await
                    .map_err(migration_error)?;
            }
            tokio::fs::remove_file(journal_path)
                .await
                .map_err(migration_error)?;
            sync_parent_directory(journal_path).await
        }
        .await;
        with_failure_reason(cleanup_result, RolloutPublishFailed)?;
        Ok(selected_path)
    }

    async fn restart_preselection_lineage_after_target_upgrade(
        &self,
        journal_path: &Path,
        journal: &LineageMigrationJournal,
        limiter: &mut RolloutMigrationRateLimiter,
    ) -> ThreadStoreResult<()> {
        journal.verify_sources().await?;
        let stage_root = journal_path.with_extension("staging");
        for (index, target) in journal.targets.iter().enumerate() {
            thread_history::delete_thread(self, target.rollout_id).await?;
            if tokio::fs::try_exists(target.path.as_path())
                .await
                .map_err(migration_error)?
            {
                let start_ordinal = target.start_ordinal.ok_or_else(|| {
                    migration_error(
                        "outdated lineage target is missing its projection start ordinal",
                    )
                })?;
                thread_history::reset_projection_for_replacement(
                    self,
                    target.rollout_id,
                    start_ordinal,
                )
                .await?;
                let projection_path = if target
                    .path
                    .extension()
                    .is_some_and(|extension| extension == "zst")
                {
                    let path = stage_root.join(format!("restore-{index:08}.jsonl"));
                    decompress_rollout_to_path(target.path.as_path(), path.as_path()).await?;
                    path
                } else {
                    target.path.clone()
                };
                self.project_rollout_in_batches(
                    target.rollout_id,
                    projection_path.as_path(),
                    limiter,
                )
                .await?;
            }
        }
        if tokio::fs::try_exists(stage_root.as_path())
            .await
            .map_err(migration_error)?
        {
            tokio::fs::remove_dir_all(stage_root.as_path())
                .await
                .map_err(migration_error)?;
        }
        tokio::fs::remove_file(journal_path)
            .await
            .map_err(migration_error)?;
        sync_parent_directory(journal_path).await
    }
}

fn stop_after_phase(
    stop_after: Option<LineageMigrationPhase>,
    phase: LineageMigrationPhase,
) -> ThreadStoreResult<()> {
    if stop_after == Some(phase) {
        return Err(migration_error(format!(
            "injected lineage migration stop after {phase:?}"
        )));
    }
    Ok(())
}
