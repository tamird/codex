//! Indexes direct fork references found in local rollout files.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use codex_protocol::RolloutId;
use codex_protocol::ThreadId;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMetaLine;

use crate::ARCHIVED_SESSIONS_SUBDIR;
use crate::RolloutItem;
use crate::RolloutLine;
use crate::SESSIONS_SUBDIR;

/// Direct history-base edges discovered from local rollout metadata.
///
/// This indexes immutable rollout IDs, not a thread's selected lineage. Callers use it to answer
/// cheap inverse-reference questions without each reimplementing rollout discovery.
#[derive(Debug, Default)]
pub struct RolloutReferenceIndex {
    rollouts_by_id: HashMap<RolloutId, IndexedRollout>,
    direct_references_by_rollout: HashMap<RolloutId, HashSet<RolloutId>>,
    reference_counts_by_rollout: HashMap<RolloutId, usize>,
}

#[derive(Debug)]
struct IndexedRollout {
    thread_id: ThreadId,
    path: PathBuf,
    history_base: Option<HistoryPosition>,
    has_shared_history: bool,
}

impl RolloutReferenceIndex {
    /// Scans active and archived local rollout metadata without a deadline.
    pub async fn scan(codex_home: &Path) -> io::Result<Self> {
        let Some(index) = Self::scan_with_deadline(codex_home, ScanDeadline::Unlimited).await?
        else {
            return Err(io::Error::other(
                "unlimited rollout reference scan exceeded a deadline",
            ));
        };
        Ok(index)
    }

    /// Scans active and archived local rollout metadata until the worker deadline expires.
    ///
    /// Returns None instead of a partial index when the deadline expires.
    pub(crate) async fn scan_until(
        codex_home: &Path,
        started_at: Instant,
        max_runtime: Duration,
    ) -> io::Result<Option<Self>> {
        Self::scan_with_deadline(
            codex_home,
            ScanDeadline::Until {
                started_at,
                max_runtime,
            },
        )
        .await
    }

    /// Returns how many other discovered rollouts directly reference `rollout_id`.
    pub fn reference_count(&self, rollout_id: RolloutId) -> usize {
        self.reference_counts_by_rollout
            .get(&rollout_id)
            .copied()
            .unwrap_or_default()
    }

    /// Returns the direct history-base edge for `rollout_id`, if one was discovered.
    pub fn history_base(&self, rollout_id: RolloutId) -> Option<&HistoryPosition> {
        self.rollouts_by_id
            .get(&rollout_id)
            .and_then(|rollout| rollout.history_base.as_ref())
    }

    /// Includes pointer leaves whose detached references do not pin their mutable source.
    pub(crate) fn has_shared_history(&self, rollout_id: RolloutId) -> Option<bool> {
        self.rollouts_by_id
            .get(&rollout_id)
            .map(|rollout| rollout.has_shared_history)
    }

    /// Returns rollout IDs and paths whose session metadata belongs to `thread_id`.
    pub fn rollouts_for_thread(
        &self,
        thread_id: ThreadId,
    ) -> impl Iterator<Item = (RolloutId, &Path)> {
        self.rollouts_by_id
            .iter()
            .filter(move |(_, rollout)| rollout.thread_id == thread_id)
            .map(|(rollout_id, rollout)| (*rollout_id, rollout.path.as_path()))
    }

    /// Returns every direct physical rollout referenced by `rollout_id`.
    ///
    /// This includes both `SessionMeta.history_base` and a leading `RolloutReference` record.
    pub fn direct_references(&self, rollout_id: RolloutId) -> Option<&HashSet<RolloutId>> {
        self.direct_references_by_rollout.get(&rollout_id)
    }

    async fn scan_with_deadline(
        codex_home: &Path,
        deadline: ScanDeadline,
    ) -> io::Result<Option<Self>> {
        let mut rollouts_by_id = HashMap::new();
        let mut direct_references_by_rollout = HashMap::new();
        let mut stack = vec![
            codex_home.join(ARCHIVED_SESSIONS_SUBDIR),
            codex_home.join(SESSIONS_SUBDIR),
        ];
        while let Some(directory) = stack.pop() {
            if deadline.expired() {
                return Ok(None);
            }
            let mut entries = match tokio::fs::read_dir(directory.as_path()).await {
                Ok(entries) => entries,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            };
            loop {
                if deadline.expired() {
                    return Ok(None);
                }
                let Some(entry) = entries.next_entry().await? else {
                    break;
                };
                let path = entry.path();
                let file_type = entry.file_type().await?;
                if file_type.is_dir() {
                    stack.push(path);
                    continue;
                }
                if !file_type.is_file() {
                    continue;
                }
                let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                    continue;
                };
                if crate::compression::parse_rollout_file_name(file_name).is_none() {
                    continue;
                }
                let Ok((meta, leading_reference)) =
                    read_direct_reference_metadata(path.as_path()).await
                else {
                    continue;
                };
                let rollout_id =
                    crate::rollout_id_from_path(path.as_path()).unwrap_or(meta.meta.id);
                let history_base = meta.meta.history_base;
                let has_shared_history = history_base.is_some() || leading_reference.is_some();
                if let Some(history_base) = history_base {
                    direct_references_by_rollout
                        .entry(rollout_id)
                        .or_insert_with(HashSet::new)
                        .insert(history_base.thread_id);
                }
                if let Some(reference) = leading_reference
                    && let Some(referenced_rollout_id) =
                        reference.rollout_id.or(reference.thread_id)
                {
                    direct_references_by_rollout
                        .entry(rollout_id)
                        .or_insert_with(HashSet::new)
                        .insert(referenced_rollout_id);
                }
                match rollouts_by_id.entry(rollout_id) {
                    Entry::Vacant(entry) => {
                        entry.insert(IndexedRollout {
                            thread_id: meta.meta.id,
                            path,
                            history_base,
                            has_shared_history,
                        });
                    }
                    Entry::Occupied(mut entry) => {
                        entry.get_mut().has_shared_history |= has_shared_history;
                    }
                }
            }
        }

        let mut reference_counts_by_rollout = HashMap::new();
        for (rollout_id, direct_references) in &direct_references_by_rollout {
            for referenced_rollout_id in direct_references {
                if referenced_rollout_id == rollout_id {
                    continue;
                }
                *reference_counts_by_rollout
                    .entry(*referenced_rollout_id)
                    .or_default() += 1;
            }
        }
        Ok(Some(Self {
            rollouts_by_id,
            direct_references_by_rollout,
            reference_counts_by_rollout,
        }))
    }
}

async fn read_direct_reference_metadata(
    path: &Path,
) -> io::Result<(SessionMetaLine, Option<RolloutReferenceItem>)> {
    let mut reader = crate::compression::open_rollout_line_reader_exact(path).await?;
    let mut session_meta = None;
    let mut leading_reference = None;
    while let Some(line) = reader.next_line().await? {
        let Ok(line) = serde_json::from_str::<RolloutLine>(line.trim()) else {
            continue;
        };
        match line.item {
            RolloutItem::SessionMeta(meta) if session_meta.is_none() => {
                session_meta = Some(meta);
            }
            RolloutItem::RolloutReference(reference) if session_meta.is_some() => {
                leading_reference = Some(reference);
                break;
            }
            _ if session_meta.is_some() => break,
            _ => {}
        }
    }
    session_meta
        .map(|meta| (meta, leading_reference))
        .ok_or_else(|| {
            io::Error::other(format!(
                "rollout {} has no session metadata",
                path.display()
            ))
        })
}

#[derive(Clone, Copy)]
enum ScanDeadline {
    Unlimited,
    Until {
        started_at: Instant,
        max_runtime: Duration,
    },
}

impl ScanDeadline {
    fn expired(self) -> bool {
        match self {
            Self::Unlimited => false,
            Self::Until {
                started_at,
                max_runtime,
            } => started_at.elapsed() >= max_runtime,
        }
    }
}

#[cfg(test)]
#[path = "rollout_reference_index_tests.rs"]
mod tests;
