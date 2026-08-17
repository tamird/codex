//! Publishes and verifies staged lineage targets.

use std::io::Read;
use std::path::Path;

use sha2::Digest;
use sha2::Sha256;

use super::lineage::hash_file;
use super::lineage_journal::LineageMigrationJournal;
use super::lineage_journal::LineageMigrationJournalTarget;
use super::lineage_journal::write_lineage_migration_journal;
use super::migration_error;
use super::publish::compress_rollout_to_path;
use super::publish::sync_parent_directory;
use crate::ThreadStoreResult;

pub(super) async fn publish_lineage_targets(
    journal_path: &Path,
    journal: &mut LineageMigrationJournal,
) -> ThreadStoreResult<()> {
    for index in 0..journal.targets.len() {
        publish_one_target(&mut journal.targets[index]).await?;
        write_lineage_migration_journal(journal_path, journal).await?;
    }
    Ok(())
}

pub(super) async fn verify_published_lineage_targets(
    journal: &LineageMigrationJournal,
) -> ThreadStoreResult<()> {
    for target in &journal.targets {
        verify_published_target(target).await?;
    }
    Ok(())
}

async fn publish_one_target(target: &mut LineageMigrationJournalTarget) -> ThreadStoreResult<()> {
    if tokio::fs::try_exists(target.path.as_path())
        .await
        .map_err(migration_error)?
    {
        target.published_sha256 = Some(verify_published_target(target).await?);
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
    target.published_sha256 = Some(verify_published_target(target).await?);
    Ok(())
}

async fn verify_published_target(
    target: &LineageMigrationJournalTarget,
) -> ThreadStoreResult<String> {
    let expected_plain_sha = target
        .sha256
        .as_deref()
        .ok_or_else(|| migration_error("lineage migration target has no staged SHA-256"))?;
    let compressed = target
        .path
        .extension()
        .is_some_and(|extension| extension == "zst");
    let plain_sha = if compressed {
        hash_decompressed(target.path.as_path()).await?
    } else {
        hash_file(target.path.as_path()).await?.1
    };
    if plain_sha != expected_plain_sha {
        return Err(migration_error(format!(
            "published lineage migration target failed verification: {}",
            target.path.display()
        )));
    }
    let published_sha = hash_file(target.path.as_path()).await?.1;
    if target
        .published_sha256
        .as_deref()
        .is_some_and(|expected| expected != published_sha)
    {
        return Err(migration_error(format!(
            "published lineage migration target changed: {}",
            target.path.display()
        )));
    }
    Ok(published_sha)
}

async fn hash_decompressed(path: &Path) -> ThreadStoreResult<String> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> ThreadStoreResult<String> {
        let input = std::fs::File::open(path).map_err(migration_error)?;
        let mut decoder = zstd::stream::read::Decoder::new(input).map_err(migration_error)?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 256 * 1024];
        loop {
            let read = decoder
                .read(buffer.as_mut_slice())
                .map_err(migration_error)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        Ok(format!("{:x}", hasher.finalize()))
    })
    .await
    .map_err(migration_error)?
}
