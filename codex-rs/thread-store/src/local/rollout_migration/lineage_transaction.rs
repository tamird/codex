//! Executes and recovers a same-thread segmented Legacy migration transaction.

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::time::Instant;

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
use super::lineage_compatibility::stage_compatible_lineage;
use super::lineage_journal::LineageMigrationJournal;
use super::lineage_journal::LineageMigrationPhase;
use super::lineage_journal::read_lineage_migration_journal;
use super::lineage_journal::write_lineage_migration_journal;
use super::lineage_publish::publish_lineage_targets;
use super::lineage_publish::resolve_published_selection;
use super::lineage_publish::verify_published_lineage_targets;
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
        for position in plan
            .sources
            .iter()
            .filter(|source| !source.materialized_predecessor)
            .filter_map(|source| match source.predecessor.as_ref() {
                Some(LegacyLineagePredecessor::HistoryBase(position)) => Some(*position),
                _ => None,
            })
        {
            let lineage = self.resolve_rollout_lineage_at(position).await?;
            if lineage.root_rollout_id != position.thread_id {
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
        plan: &mut LegacyLineageMigrationPlan,
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
        mut plan: LegacyLineageMigrationPlan,
        legacy_names: &HashMap<ThreadId, String>,
        limiter: &mut RolloutMigrationRateLimiter,
        stop_after: Option<LineageMigrationPhase>,
    ) -> ClassifiedMigrationResult<PathBuf> {
        with_failure_reason(
            super::lineage::expand_filtered_native_rollbacks(self, &mut plan).await,
            LegacyRolloutConversionFailed,
        )?;
        let started = Instant::now();
        with_failure_reason(
            self.validate_legacy_lineage_plan(&plan).await,
            LegacyRolloutConversionFailed,
        )?;
        tracing::info!(thread_id = %plan.selected_thread_id, phase = "validate_plan", elapsed_ms = started.elapsed().as_millis() as u64, "rollout migration phase complete");
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
        let permits_source_append = if journal.verify_plan(&plan).is_err() {
            with_failure_reason(
                journal.permits_selected_source_append(&plan).await,
                InterruptedMigrationRecoveryFailed,
            )?
        } else {
            false
        };
        if permits_source_append {
            let state_db = self.state_db.as_ref().ok_or_else(|| {
                RolloutMigrationFailure::new(
                    MissingSqliteMetadata,
                    migration_error("lineage migration requires SQLite metadata"),
                )
            })?;
            let current = with_failure_reason(
                state_db
                    .get_thread(plan.selected_thread_id)
                    .await
                    .map_err(migration_error),
                InterruptedMigrationRecoveryFailed,
            )?;
            if current.is_some_and(|metadata| metadata.rollout_path == selected_source_path) {
                // ProjectionDurable can include published targets, or even a selected target if the
                // process died before recording Selected. Only restart while the original source is
                // still selected. Retain old files and projections because another history may use them.
                let cleanup_result = async {
                    if tokio::fs::try_exists(&stage_root)
                        .await
                        .map_err(migration_error)?
                    {
                        tokio::fs::remove_dir_all(&stage_root)
                            .await
                            .map_err(migration_error)?;
                    }
                    Ok(())
                }
                .await;
                with_failure_reason(cleanup_result, InterruptedMigrationRecoveryFailed)?;
                journal = LineageMigrationJournal::from_plan(&plan);
                with_failure_reason(
                    write_lineage_migration_journal(journal_path, &journal).await,
                    RolloutPublishFailed,
                )?;
            }
        }
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

        if !journal.replay_native_rollbacks
            && matches!(
                journal.phase,
                LineageMigrationPhase::Planned
                    | LineageMigrationPhase::TargetsDurable
                    | LineageMigrationPhase::ProjectionDurable
            )
            && plan.sources.iter().any(|source| {
                source.history_mode == codex_protocol::protocol::ThreadHistoryMode::Legacy
                    && source.has_rollback
            })
        {
            with_failure_reason(journal.verify_sources().await, RolloutReadFailed)?;
            let state_db = self.state_db.as_ref().ok_or_else(|| {
                RolloutMigrationFailure::new(
                    MissingSqliteMetadata,
                    migration_error("lineage migration requires SQLite metadata"),
                )
            })?;
            let selected = with_failure_reason(
                state_db
                    .get_thread(plan.selected_thread_id)
                    .await
                    .map_err(migration_error),
                InterruptedMigrationRecoveryFailed,
            )?;
            if selected.is_some_and(|metadata| metadata.rollout_path == selected_source_path) {
                let mut current_plan = with_failure_reason(
                    super::lineage::plan_legacy_lineage_with_native_prefix_reuse(
                        self.config.codex_home.as_path(),
                        selected_source_path,
                    )
                    .await,
                    LegacyRolloutConversionFailed,
                )?;
                with_failure_reason(
                    super::lineage::expand_filtered_native_rollbacks(self, &mut current_plan).await,
                    LegacyRolloutConversionFailed,
                )?;
                if current_plan.replay_native_rollbacks {
                    with_failure_reason(
                        self.validate_legacy_lineage_plan(&current_plan).await,
                        LegacyRolloutConversionFailed,
                    )?;
                    if journal.targets.iter().any(|old| {
                        current_plan
                            .targets
                            .iter()
                            .any(|new| new.rollout_id == old.rollout_id)
                    }) {
                        return Err(RolloutMigrationFailure::new(
                            InterruptedMigrationRecoveryFailed,
                            migration_error("native rollback replan reuses an old target identity"),
                        ));
                    }
                    // Published predecessors may already belong to another fork. Restart only the
                    // child journal, retaining every old target and its projection.
                    plan = current_plan;
                    journal = LineageMigrationJournal::from_plan(&plan);
                    with_failure_reason(
                        write_lineage_migration_journal(journal_path, &journal).await,
                        RolloutPublishFailed,
                    )?;
                }
            }
        }

        if journal.phase == LineageMigrationPhase::Planned && !journal.reuse_native_prefixes {
            with_failure_reason(journal.verify_sources().await, RolloutReadFailed)?;
            let state_db = self.state_db.as_ref().ok_or_else(|| {
                RolloutMigrationFailure::new(
                    MissingSqliteMetadata,
                    migration_error("lineage migration requires SQLite metadata"),
                )
            })?;
            let current = with_failure_reason(
                state_db
                    .get_thread(plan.selected_thread_id)
                    .await
                    .map_err(migration_error),
                InterruptedMigrationRecoveryFailed,
            )?;
            if current.is_some_and(|metadata| metadata.rollout_path == selected_source_path) {
                let mut current_plan = with_failure_reason(
                    super::lineage::plan_legacy_lineage_with_native_prefix_reuse(
                        self.config.codex_home.as_path(),
                        selected_source_path,
                    )
                    .await,
                    LegacyRolloutConversionFailed,
                )?;
                with_failure_reason(
                    super::lineage::expand_filtered_native_rollbacks(self, &mut current_plan).await,
                    LegacyRolloutConversionFailed,
                )?;
                with_failure_reason(
                    self.validate_legacy_lineage_plan(&current_plan).await,
                    LegacyRolloutConversionFailed,
                )?;
                if current_plan.targets != plan.targets
                    && journal.targets.iter().any(|old| {
                        current_plan
                            .targets
                            .iter()
                            .any(|new| new.rollout_id == old.rollout_id)
                    })
                {
                    return Err(RolloutMigrationFailure::new(
                        InterruptedMigrationRecoveryFailed,
                        migration_error("native-prefix replan reuses an old target identity"),
                    ));
                }
                plan = current_plan;
                journal = LineageMigrationJournal::from_plan(&plan);
                with_failure_reason(
                    write_lineage_migration_journal(journal_path, &journal).await,
                    RolloutPublishFailed,
                )?;
            }
        }

        let ancestor_ids = if plan.replay_native_rollbacks {
            plan.sources
                .iter()
                .chain(&plan.authentication_sources)
                .map(|source| source.thread_id)
                .chain(plan.history_bases.iter().map(|source| source.thread_id))
                .chain(
                    plan.reference_dependencies
                        .iter()
                        .map(|source| source.thread_id),
                )
                .filter(|thread_id| *thread_id != plan.selected_thread_id)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let _ancestor_writers = with_failure_reason(
            self.try_reserve_rollout_writers(&ancestor_ids).await,
            super::RolloutMigrationFailureReason::Unknown,
        )?;

        if journal.phase == LineageMigrationPhase::Planned {
            let started = Instant::now();
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
                let staged = stage_compatible_lineage(
                    self.config.codex_home.as_path(),
                    &mut plan,
                    stage_root.as_path(),
                )
                .await?;
                journal.record_staged_targets(staged.as_slice())
            }
            .await;
            with_failure_reason(conversion_result, LegacyRolloutConversionFailed)?;
            with_failure_reason(
                write_lineage_migration_journal(journal_path, &journal).await,
                RolloutPublishFailed,
            )?;
            tracing::info!(thread_id = %plan.selected_thread_id, phase = "stage_targets", elapsed_ms = started.elapsed().as_millis() as u64, "rollout migration phase complete");
            with_failure_reason(
                stop_after_phase(stop_after, journal.phase),
                InterruptedMigrationRecoveryFailed,
            )?;
        }

        if journal.phase == LineageMigrationPhase::TargetsDurable {
            let started = Instant::now();
            with_failure_reason(journal.verify_sources().await, RolloutReadFailed)?;
            with_failure_reason(
                journal.verify_staged_targets().await,
                InterruptedMigrationRecoveryFailed,
            )?;
            let complete_root = with_failure_reason(
                super::lineage_projection::complete_staged_root(&plan, &journal).await,
                SqliteMaterializationFailed,
            )?;
            let projected = with_failure_reason(
                super::lineage_projection::try_project_staged_targets(
                    self,
                    &journal,
                    complete_root,
                    limiter,
                    super::lineage_projection::BULK_PROJECTION_BUDGET,
                )
                .await,
                SqliteMaterializationFailed,
            )?;
            if !projected {
                if let Some(root) = complete_root {
                    with_failure_reason(
                        thread_history::delete_thread(self, root).await,
                        SqliteMaterializationFailed,
                    )?;
                    with_failure_reason(
                        thread_history::reset_projection_for_replacement(
                            self, root, /*next_rollout_ordinal*/ 0,
                        )
                        .await,
                        SqliteMaterializationFailed,
                    )?;
                }
                for target in &journal.targets {
                    let staged_path = with_failure_reason(
                        target.staged_path.as_ref().ok_or_else(|| {
                            migration_error("lineage target is missing its staged path")
                        }),
                        InterruptedMigrationRecoveryFailed,
                    )?;
                    let start_ordinal = with_failure_reason(
                        target.start_ordinal.ok_or_else(|| {
                            migration_error("lineage target is missing its start ordinal")
                        }),
                        InterruptedMigrationRecoveryFailed,
                    )?;
                    if let Some(root) = complete_root {
                        with_failure_reason(
                            thread_history::reset_projection_for_replacement(
                                self,
                                root,
                                start_ordinal,
                            )
                            .await,
                            SqliteMaterializationFailed,
                        )?;
                    }
                    if complete_root != Some(target.rollout_id) {
                        with_failure_reason(
                            thread_history::delete_thread(self, target.rollout_id).await,
                            SqliteMaterializationFailed,
                        )?;
                        with_failure_reason(
                            prepare_lineage_target_projection(
                                self,
                                target.rollout_id,
                                staged_path,
                                start_ordinal,
                            )
                            .await,
                            SqliteMaterializationFailed,
                        )?;
                    }
                    with_failure_reason(
                        self.project_rollout_in_batches(
                            target.rollout_id,
                            staged_path,
                            complete_root.filter(|root| *root != target.rollout_id),
                            limiter,
                        )
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
                        target.byte_count.ok_or_else(|| {
                            migration_error("lineage target is missing its byte count")
                        }),
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
                            && (complete_root != Some(target.rollout_id)
                                || projection.lineage_complete)
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
            }
            with_failure_reason(
                journal.advance(LineageMigrationPhase::ProjectionDurable),
                InterruptedMigrationRecoveryFailed,
            )?;
            with_failure_reason(
                write_lineage_migration_journal(journal_path, &journal).await,
                RolloutPublishFailed,
            )?;
            tracing::info!(thread_id = %plan.selected_thread_id, phase = "project_targets", elapsed_ms = started.elapsed().as_millis() as u64, "rollout migration phase complete");
            with_failure_reason(
                stop_after_phase(stop_after, journal.phase),
                InterruptedMigrationRecoveryFailed,
            )?;
        }

        let selected_target_path = with_failure_reason(
            resolve_published_selection(self, &journal, selected_source_path).await,
            InterruptedMigrationRecoveryFailed,
        )?;
        if journal.phase == LineageMigrationPhase::ProjectionDurable {
            let started = Instant::now();
            with_failure_reason(journal.verify_sources().await, RolloutReadFailed)?;
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
            with_failure_reason(
                publish_lineage_targets(journal_path, &mut journal, &selected_target_path).await,
                RolloutPublishFailed,
            )?;
            if !codex_rollout::rollout_paths_match(&current.rollout_path, &selected_target_path)
                .await
            {
                let replaced = with_failure_reason(
                    state_db
                        .replace_rollout_path_if_current(
                            journal.selected_thread_id,
                            selected_source_path,
                            selected_target_path.as_path(),
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
            tracing::info!(thread_id = %plan.selected_thread_id, phase = "publish_targets", elapsed_ms = started.elapsed().as_millis() as u64, "rollout migration phase complete");
            with_failure_reason(
                stop_after_phase(stop_after, journal.phase),
                InterruptedMigrationRecoveryFailed,
            )?;
        }

        if journal.phase == LineageMigrationPhase::Selected {
            let started = Instant::now();
            with_failure_reason(
                verify_published_lineage_targets(&journal, &selected_target_path).await,
                RolloutPublishFailed,
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
            if !codex_rollout::rollout_paths_match(&selected.rollout_path, &selected_target_path)
                .await
            {
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
                let expected_bytes = with_failure_reason(
                    target.byte_count.ok_or_else(|| {
                        migration_error("published lineage target is missing its byte count")
                    }),
                    InterruptedMigrationRecoveryFailed,
                )?;
                let expected_ordinal = with_failure_reason(
                    target.end_ordinal_exclusive.ok_or_else(|| {
                        migration_error("published lineage target is missing its ordinal boundary")
                    }),
                    InterruptedMigrationRecoveryFailed,
                )?;
                let complete = projection.next_byte_offset == expected_bytes
                    && projection.next_ordinal == expected_ordinal;
                if !complete {
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
            tracing::info!(thread_id = %plan.selected_thread_id, phase = "verify_targets", elapsed_ms = started.elapsed().as_millis() as u64, "rollout migration phase complete");
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
        Ok(selected_target_path)
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
                prepare_lineage_target_projection(
                    self,
                    target.rollout_id,
                    projection_path.as_path(),
                    start_ordinal,
                )
                .await?;
                self.project_rollout_in_batches(
                    target.rollout_id,
                    projection_path.as_path(),
                    /*complete_root*/ None,
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

async fn prepare_lineage_target_projection(
    store: &LocalThreadStore,
    rollout_id: codex_protocol::RolloutId,
    rollout_path: &Path,
    start_ordinal: u64,
) -> ThreadStoreResult<()> {
    let session_meta = codex_rollout::read_session_meta_line(rollout_path)
        .await
        .map_err(migration_error)?;
    if session_meta.meta.history_base.is_some() {
        thread_history::begin_incomplete_paginated_projection(store, rollout_id, start_ordinal)
            .await
    } else {
        thread_history::reset_projection_for_replacement(store, rollout_id, start_ordinal).await
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
