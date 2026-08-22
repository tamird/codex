//! Publishes and verifies staged lineage targets.

use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::protocol::ThreadHistoryMode;

use sha2::Digest;
use sha2::Sha256;

use super::LocalThreadStore;
use super::lineage::hash_file;
use super::lineage_journal::LineageMigrationJournal;
use super::lineage_journal::LineageMigrationJournalTarget;
use super::lineage_journal::write_lineage_migration_journal;
use super::migration_error;
use super::publish::compress_rollout_to_path;
use super::publish::sync_parent_directory;
use super::selection;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

/// Resolve an already-selected target without changing the authenticated journal paths.
/// The transaction holds writer ownership and verifies the journal against its source plan first.
pub(super) async fn resolve_published_selection(
    store: &LocalThreadStore,
    journal: &LineageMigrationJournal,
    selected_source_path: &Path,
) -> ThreadStoreResult<PathBuf> {
    let target = journal
        .targets
        .iter()
        .find(|target| target.selected)
        .ok_or_else(|| migration_error("lineage journal has no selected target"))?;
    let Some(state_db) = &store.state_db else {
        return Ok(target.path.clone());
    };
    let Some(selected) = state_db
        .get_thread(journal.selected_thread_id)
        .await
        .map_err(migration_error)?
    else {
        return Ok(target.path.clone());
    };
    if codex_rollout::rollout_paths_match(&selected.rollout_path, selected_source_path).await {
        return Ok(target.path.clone());
    }
    let mut selected_path = selected.rollout_path;
    let metadata =
        selection::read_locked_metadata(&store.config.codex_home, &mut selected_path).await?;
    let selection = selection::classify_selected_path(&target.path, &selected_path).await?;
    if matches!(selection, selection::RolloutSelection::Other) {
        return Err(ThreadStoreError::Conflict {
            message: "selected rollout changed during lineage migration recovery".to_string(),
        });
    }
    if metadata.meta.id != journal.selected_thread_id
        || metadata.meta.history_mode != ThreadHistoryMode::Paginated
        || codex_rollout::rollout_id_from_path(&selected_path) != Some(target.rollout_id)
    {
        return Err(migration_error(
            "relocated lineage target metadata does not match its journal",
        ));
    }
    verify_published_target(target, &selected_path).await?;
    Ok(selected_path)
}

pub(super) async fn publish_lineage_targets(
    journal_path: &Path,
    journal: &mut LineageMigrationJournal,
    selected_target_path: &Path,
) -> ThreadStoreResult<()> {
    for index in 0..journal.targets.len() {
        let target = &mut journal.targets[index];
        if target.selected && target.path != selected_target_path {
            // Selection already happened before the crash. Verify its current representation in
            // place; republishing the journal path would recreate a superseded active file.
            verify_published_target(target, selected_target_path).await?;
        } else {
            publish_one_target(target).await?;
        }
        write_lineage_migration_journal(journal_path, journal).await?;
    }
    Ok(())
}

pub(super) async fn verify_published_lineage_targets(
    journal: &LineageMigrationJournal,
    selected_target_path: &Path,
) -> ThreadStoreResult<()> {
    for target in &journal.targets {
        let path = if target.selected {
            selected_target_path
        } else {
            &target.path
        };
        verify_published_target(target, path).await?;
    }
    Ok(())
}

async fn publish_one_target(target: &mut LineageMigrationJournalTarget) -> ThreadStoreResult<()> {
    if tokio::fs::try_exists(target.path.as_path())
        .await
        .map_err(migration_error)?
    {
        target.published_sha256 = Some(verify_published_target(target, &target.path).await?);
        return Ok(());
    }
    let staged_path = target
        .staged_path
        .as_ref()
        .ok_or_else(|| migration_error("lineage migration target has no staged path"))?;
    let parent = target
        .path
        .parent()
        .ok_or_else(|| migration_error("lineage migration target has no parent"))?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(migration_error)?;
    let compressed = target
        .path
        .extension()
        .is_some_and(|extension| extension == "zst");
    if compressed {
        let temporary = staged_path.with_extension("publish.zst.tmp");
        let permissions = tokio::fs::metadata(staged_path)
            .await
            .map_err(migration_error)?
            .permissions();
        compress_rollout_to_path(
            staged_path,
            temporary.as_path(),
            permissions,
            /*modified_at*/ None,
        )
        .await?;
        tokio::fs::rename(temporary.as_path(), target.path.as_path())
            .await
            .map_err(migration_error)?;
    } else {
        tokio::fs::rename(staged_path, target.path.as_path())
            .await
            .map_err(migration_error)?;
    }
    sync_parent_directory(target.path.as_path()).await?;
    target.published_sha256 = Some(verify_published_target(target, &target.path).await?);
    Ok(())
}

async fn verify_published_target(
    target: &LineageMigrationJournalTarget,
    path: &Path,
) -> ThreadStoreResult<String> {
    let expected_plain_sha = target
        .sha256
        .as_deref()
        .ok_or_else(|| migration_error("lineage migration target has no staged SHA-256"))?;
    let expected_bytes = target
        .byte_count
        .ok_or_else(|| migration_error("lineage migration target has no staged byte count"))?;
    let compressed = path.extension().is_some_and(|extension| extension == "zst");
    let plain_sha = if compressed {
        hash_decompressed(path, expected_bytes).await?
    } else {
        let (byte_count, sha256) = hash_file(path).await?;
        if byte_count != expected_bytes {
            return Err(migration_error("published lineage target length changed"));
        }
        sha256
    };
    if plain_sha != expected_plain_sha {
        return Err(migration_error(format!(
            "published lineage migration target failed verification: {}",
            path.display()
        )));
    }
    let published_sha = hash_file(path).await?.1;
    // Compression changes physical bytes, but the exact decoded preimage must still match above.
    // Keep the original journal digest unchanged so later recovery checks the same contract.
    if path.extension() == target.path.extension()
        && target
            .published_sha256
            .as_deref()
            .is_some_and(|expected| expected != published_sha)
    {
        return Err(migration_error(format!(
            "published lineage migration target changed: {}",
            path.display()
        )));
    }
    Ok(published_sha)
}

async fn hash_decompressed(path: &Path, expected_bytes: u64) -> ThreadStoreResult<String> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> ThreadStoreResult<String> {
        let input = std::fs::File::open(path).map_err(migration_error)?;
        let mut decoder = zstd::stream::read::Decoder::new(input).map_err(migration_error)?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 256 * 1024];
        let mut byte_count = 0_u64;
        loop {
            let read = decoder
                .read(buffer.as_mut_slice())
                .map_err(migration_error)?;
            if read == 0 {
                break;
            }
            byte_count = byte_count.saturating_add(read as u64);
            if byte_count > expected_bytes {
                return Err(migration_error("published lineage target length changed"));
            }
            hasher.update(&buffer[..read]);
        }
        if byte_count != expected_bytes {
            return Err(migration_error("published lineage target length changed"));
        }
        Ok(format!("{:x}", hasher.finalize()))
    })
    .await
    .map_err(migration_error)?
}
