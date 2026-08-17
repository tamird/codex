//! Recovers ordinal reuse by older recorders without changing an issued rollout identity.
//!
//! Only a repeated ordinal immediately following `token_count` is recognized. Recovery keeps the
//! original bytes available for existing history boundaries, projects a corrected sibling, and
//! selects that sibling only after its complete projection is durable.

use std::borrow::Cow;
use std::path::PathBuf;

use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionMetaLine;
use serde::Deserialize;
use serde_json::value::RawValue;
use sha2::Digest;
use sha2::Sha256;
use tokio::sync::OwnedRwLockReadGuard;

use super::LocalThreadStore;
use super::RolloutWriterReservation;
use super::segment::confined_publication;
use super::segment::confined_publication::ConfinedFileSnapshot;
use super::segment::confined_publication::ConfinedRootIdentity;
use super::thread_rollout_resolver;
use super::thread_rollout_resolver::ResolvedThreadRollout;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

/// An installed correction whose source remains selected until projection publication succeeds.
pub(super) struct PreparedOrdinalRecovery {
    pub(super) rollout_path: PathBuf,
    source_path: PathBuf,
    canonical_home: PathBuf,
    canonical_source: PathBuf,
    source_snapshot: ConfinedFileSnapshot,
    root_identity: ConfinedRootIdentity,
    _maintenance: codex_rollout::RolloutMaintenanceReadGuard,
    _lifecycle: OwnedRwLockReadGuard<()>,
    _writers: RolloutWriterReservation,
}

pub(super) async fn prepare(
    store: &LocalThreadStore,
    selected: &ResolvedThreadRollout,
    metadata: &SessionMetaLine,
) -> ThreadStoreResult<Option<PreparedOrdinalRecovery>> {
    if selected
        .path
        .extension()
        .and_then(|extension| extension.to_str())
        != Some("jsonl")
        || metadata.meta.subagent_history_start_ordinal.is_some()
    {
        return Ok(None);
    }
    let Some(relative) = selected.path.strip_prefix(&store.config.codex_home).ok() else {
        return Ok(None);
    };
    let maintenance =
        codex_rollout::acquire_rollout_maintenance_read_lock(&store.config.codex_home)
            .await
            .map_err(recovery_error)?;
    let canonical_home = tokio::fs::canonicalize(&store.config.codex_home)
        .await
        .map_err(recovery_error)?;
    let canonical_source = canonical_home.join(relative);
    let root_identity = confined_publication::confined_root_identity(&canonical_home)
        .await
        .map_err(recovery_error)?;
    let (source, source_snapshot) = confined_publication::read_confined_file_under_root(
        &canonical_home,
        &canonical_source,
        &root_identity,
    )
    .await
    .map_err(recovery_error)?;
    let Some(corrected) = correct_reused_ordinals(&source, metadata) else {
        return Ok(None);
    };

    let lifecycle = store
        .live_writer_locks
        .reserve_lifecycle(selected.thread_id)
        .await;
    let writers = store.reserve_rollout_writers(&[selected.thread_id]).await?;
    // Switching a resident recorder's physical identity would invalidate its in-memory ordinal
    // state. Cold load repairs before recorder admission; a running task must be reopened first.
    if store
        .live_recorders
        .lock()
        .await
        .contains_key(&selected.thread_id)
    {
        return Err(ThreadStoreError::Conflict {
            message: format!(
                "ordinal recovery for thread {} requires its live writer to close",
                selected.thread_id
            ),
        });
    }
    let current =
        thread_rollout_resolver::resolve_current_including_archived(store, selected.thread_id)
            .await?
            .ok_or(ThreadStoreError::ThreadNotFound {
                thread_id: selected.thread_id,
            })?;
    if current.path != selected.path || current.rollout_id != selected.rollout_id {
        return Err(ThreadStoreError::Conflict {
            message: "selected rollout changed before ordinal recovery".to_string(),
        });
    }
    let mut digest = Sha256::new();
    digest.update(b"codex-token-count-ordinal-recovery-v1\0");
    digest.update(selected.rollout_id.to_string().as_bytes());
    digest.update(&source);
    let digest = digest.finalize();
    let mut identity = [0; 16];
    identity.copy_from_slice(&digest[..16]);
    let rollout_id = ThreadId::from_u128(u128::from_be_bytes(identity));
    let rollout_path = codex_rollout::rollout_path_with_rollout_id(&selected.path, rollout_id)
        .ok_or_else(|| recovery_error("ordinal recovery requires a canonical rollout filename"))?;
    let recovery = PreparedOrdinalRecovery {
        rollout_path,
        source_path: selected.path.clone(),
        canonical_home,
        canonical_source,
        source_snapshot,
        root_identity,
        _maintenance: maintenance,
        _lifecycle: lifecycle,
        _writers: writers,
    };
    recovery.verify_source().await?;
    let destination = recovery.canonical_home.join(
        recovery
            .rollout_path
            .strip_prefix(&store.config.codex_home)
            .map_err(recovery_error)?,
    );
    confined_publication::install_confined_file_under_root(
        &recovery.canonical_home,
        &destination,
        &corrected,
        recovery.source_snapshot.permissions.clone(),
        recovery.source_snapshot.modified,
        &recovery.root_identity,
    )
    .await
    .map_err(recovery_error)?;
    Ok(Some(recovery))
}

impl PreparedOrdinalRecovery {
    async fn verify_source(&self) -> ThreadStoreResult<()> {
        let (_, current) = confined_publication::read_confined_file_under_root(
            &self.canonical_home,
            &self.canonical_source,
            &self.root_identity,
        )
        .await
        .map_err(recovery_error)?;
        if current != self.source_snapshot {
            return Err(ThreadStoreError::Conflict {
                message: "source rollout changed during ordinal recovery".to_string(),
            });
        }
        Ok(())
    }

    /// The caller has already published a complete projection for `rollout_path`.
    pub(super) async fn select(
        &self,
        store: &LocalThreadStore,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<()> {
        self.verify_source().await?;
        let state = store
            .state_db
            .as_ref()
            .ok_or_else(|| recovery_error("ordinal recovery requires SQLite selection"))?;
        if !state
            .replace_rollout_path_if_current(thread_id, &self.source_path, &self.rollout_path)
            .await
            .map_err(recovery_error)?
        {
            return Err(ThreadStoreError::Conflict {
                message: "selected rollout changed during ordinal recovery".to_string(),
            });
        }
        tracing::info!(%thread_id, source = %self.source_path.display(), replacement = %self.rollout_path.display(), "recovered token_count ordinal reuse");
        Ok(())
    }
}

/// Raw fields keep numeric payloads and unknown fields byte-for-byte unchanged.
#[derive(Deserialize)]
struct OrdinalEnvelope<'a> {
    #[serde(borrow)]
    ordinal: &'a RawValue,
    #[serde(rename = "type", borrow)]
    kind: Cow<'a, str>,
    #[serde(borrow)]
    payload: &'a RawValue,
}

/// Only event discrimination is needed to recognize the defective restart boundary.
#[derive(Deserialize)]
struct EventKind<'a> {
    #[serde(rename = "type", borrow)]
    kind: Cow<'a, str>,
}

fn correct_reused_ordinals(source: &[u8], metadata: &SessionMetaLine) -> Option<Vec<u8>> {
    let mut output = Vec::with_capacity(source.len());
    let mut expected = metadata
        .meta
        .history_base
        .map_or(0, |base| base.end_ordinal_exclusive);
    let mut previous = None;
    let mut previous_token_count = false;
    let mut repaired = false;
    for (index, record) in source.split_inclusive(|byte| *byte == b'\n').enumerate() {
        if !record.ends_with(b"\n") {
            return None;
        }
        let envelope: OrdinalEnvelope<'_> = serde_json::from_slice(record).ok()?;
        let ordinal: u64 = serde_json::from_str(envelope.ordinal.get()).ok()?;
        if index == 0 {
            if envelope.kind != "session_meta" || ordinal != expected {
                return None;
            }
        } else {
            if envelope.kind == "session_meta" || envelope.kind == "rollout_reference" {
                return None;
            }
            let previous: u64 = previous?;
            if ordinal == previous && previous_token_count {
                repaired = true;
            } else if ordinal != previous.checked_add(1)? {
                // A gap, another kind of duplicate, or a backward jump is not this recorder bug.
                return None;
            }
        }
        let token = envelope.ordinal.get().as_bytes();
        let start = (token.as_ptr() as usize).checked_sub(record.as_ptr() as usize)?;
        let end = start.checked_add(token.len())?;
        if record.get(start..end)? != token {
            return None;
        }
        output.extend_from_slice(&record[..start]);
        if ordinal == expected {
            output.extend_from_slice(token);
        } else {
            output.extend_from_slice(expected.to_string().as_bytes());
        }
        output.extend_from_slice(&record[end..]);
        previous = Some(ordinal);
        previous_token_count = envelope.kind == "event_msg"
            && serde_json::from_str::<EventKind<'_>>(envelope.payload.get())
                .is_ok_and(|event| event.kind == "token_count");
        expected = expected.checked_add(1)?;
    }
    repaired.then_some(output)
}

fn recovery_error(error: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("failed to recover rollout ordinals: {error}"),
    }
}

#[cfg(test)]
#[path = "ordinal_recovery_tests.rs"]
mod tests;
