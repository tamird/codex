//! Bounded migration admission reads, separate from full lineage authentication.
//!
//! Missing or unusual headers retain exclusive maintenance ownership. A complete explicit
//! reference chain can reserve its logical and physical identities without inventorying home.

use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_rollout::RolloutItem;
use sha2::Digest;
use sha2::Sha256;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;

use super::lineage::LegacyLineageMigrationPlan;

const MAX_HEADER_BYTES: u64 = 64 * 1024;
const MAX_DISCOVERY_BYTES: u64 = 4 * 1024 * 1024;
const MAX_DEPENDENCIES: usize = 512;

/// A failed coverage check must retry exclusively, not be cached as supported-reader success.
#[derive(Clone)]
pub(super) enum MigrationAdmission {
    /// Explicit sweeps retain their existing wait for an in-process writer.
    Manual,
    Exclusive,
    Shared(std::sync::Arc<MigrationDependencies>),
    RequiresExclusive,
}

/// Header identities reserved by one migration attempt, never a cached decoded history.
#[derive(Debug)]
pub(super) struct MigrationDependencies {
    pub(super) thread_ids: Vec<ThreadId>,
    headers: Vec<HeaderSnapshot>,
    /// Includes reader read-ahead, so classification cost cannot hide buffered payload reads.
    pub(super) bytes_read: u64,
}

/// Exact bytes that established an edge; appending an unrelated tail does not change the edge.
#[derive(Debug)]
struct HeaderSnapshot {
    path: PathBuf,
    logical_id: ThreadId,
    physical_id: ThreadId,
    byte_count: u64,
    sha256: [u8; 32],
}

impl MigrationDependencies {
    /// The full planner remains authoritative. Native replay can add ancestors not present in
    /// its direct dependency collections, so it requires exclusive admission for now.
    pub(super) async fn covers_plan(&self, plan: &LegacyLineageMigrationPlan) -> bool {
        if !plan.history_bases.is_empty()
            || !plan.reference_dependencies.is_empty()
            || !plan.authentication_sources.is_empty()
            || !plan.reuse_native_prefixes
            || plan.replay_native_rollbacks
        {
            return false;
        }
        for source in &plan.sources {
            if source.history_mode != ThreadHistoryMode::Legacy
                || source.has_rollback
                || source.materialized_predecessor
                || source.native_replay.is_some()
            {
                return false;
            }
            let Ok(path) = tokio::fs::canonicalize(&source.path).await else {
                return false;
            };
            if !self.headers.iter().any(|header| {
                header.path == path
                    && header.logical_id == source.thread_id
                    && header.physical_id == source.rollout_id
            }) {
                return false;
            }
        }
        true
    }

    pub(super) fn selected_thread_id(&self) -> ThreadId {
        self.headers[0].logical_id
    }

    /// Recheck after reservation, before ordinary migration can write a journal or projection.
    pub(super) async fn unchanged(&self) -> std::io::Result<bool> {
        for header in &self.headers {
            let file = tokio::fs::File::open(&header.path).await?;
            let mut bytes = Vec::with_capacity(header.byte_count as usize);
            file.take(header.byte_count).read_to_end(&mut bytes).await?;
            if bytes.len() as u64 != header.byte_count
                || <[u8; 32]>::from(Sha256::digest(&bytes)) != header.sha256
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// `None` is conservative exclusion, not evidence that the rollout has no dependencies.
pub(super) async fn discover_dependencies(
    codex_home: &Path,
    selected_path: &Path,
) -> std::io::Result<Option<MigrationDependencies>> {
    let mut path = selected_path.to_path_buf();
    let mut seen = HashSet::new();
    let mut identities = HashSet::new();
    let mut headers = Vec::new();
    let mut bytes_read = 0;
    loop {
        if headers.len() == MAX_DEPENDENCIES || bytes_read == MAX_DISCOVERY_BYTES {
            return Ok(None);
        }
        // Compressed or relocated references still migrate, but this classifier must not start
        // an unbounded decompressor or filesystem search merely to establish independence.
        if path.extension().is_some_and(|extension| extension == "zst") {
            return Ok(None);
        }
        let canonical = match tokio::fs::canonicalize(&path).await {
            Ok(path) => path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        if !seen.insert(canonical.clone()) {
            return Ok(None);
        }
        let file = tokio::fs::File::open(&canonical).await?;
        let limit = MAX_HEADER_BYTES.min(MAX_DISCOVERY_BYTES - bytes_read);
        let mut reader = BufReader::with_capacity(4096, file.take(limit));
        let mut bytes = Vec::new();
        reader.read_until(b'\n', &mut bytes).await?;
        let metadata_end = bytes.len();
        let Ok(value) = serde_json::from_slice(&bytes) else {
            return Ok(None);
        };
        let Ok(line) = codex_rollout::decode_rollout_line(value) else {
            return Ok(None);
        };
        let RolloutItem::SessionMeta(metadata) = line.item else {
            return Ok(None);
        };
        if metadata.meta.history_mode != ThreadHistoryMode::Legacy
            || metadata.meta.history_base.is_some()
        {
            return Ok(None);
        }
        let Some(physical_id) = codex_rollout::rollout_id_from_path(&canonical) else {
            return Ok(None);
        };
        // Reserve physical IDs too: rotated files have distinct rollout IDs, and projections
        // are keyed by those IDs rather than the logical SessionMeta.id.
        identities.insert(metadata.meta.id);
        identities.insert(physical_id);
        reader.read_until(b'\n', &mut bytes).await?;
        bytes_read += limit - reader.get_ref().limit();
        if bytes.last() != Some(&b'\n') || bytes.len() as u64 == limit {
            return Ok(None);
        }
        let predecessor = if bytes.len() == metadata_end {
            None
        } else {
            let Ok(value) = serde_json::from_slice(&bytes[metadata_end..]) else {
                return Ok(None);
            };
            let Ok(line) = codex_rollout::decode_rollout_line(value) else {
                return Ok(None);
            };
            match line.item {
                RolloutItem::RolloutReference(reference)
                    if super::lineage::reference_is_history_base_compatible(&reference) =>
                {
                    Some(reference)
                }
                RolloutItem::RolloutReference(_) | RolloutItem::SessionMeta(_) => return Ok(None),
                _ => None,
            }
        };
        headers.push(HeaderSnapshot {
            path: canonical,
            logical_id: metadata.meta.id,
            physical_id,
            byte_count: bytes.len() as u64,
            sha256: Sha256::digest(&bytes).into(),
        });
        if let Some(reference) = predecessor {
            path = codex_home.join(reference.rollout_path);
        } else {
            let mut thread_ids = identities.into_iter().collect::<Vec<_>>();
            thread_ids.sort_unstable_by_key(ThreadId::to_string);
            return Ok(Some(MigrationDependencies {
                thread_ids,
                headers,
                bytes_read,
            }));
        }
    }
}
