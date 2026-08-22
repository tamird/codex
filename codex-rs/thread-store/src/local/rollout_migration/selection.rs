//! Reconciles a selected rollout's archive or compression move without selecting older history.

use std::io;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionMetaLine;
use codex_rollout::StateDbHandle;

use super::migration_error;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

pub(super) enum RolloutSelection {
    Current,
    Relocated,
    Other,
}

/// Requires writer ownership; the caller validates the returned owner and history mode.
pub(super) async fn read_locked_metadata(
    codex_home: &Path,
    path: &mut PathBuf,
) -> ThreadStoreResult<SessionMetaLine> {
    // Archiving can win between the initial SessionMeta read and writer acquisition. Follow the
    // same full basename once while holding the writer, never a different selected rollout.
    if !tokio::fs::try_exists(&path)
        .await
        .map_err(migration_error)?
        && let Some(current_path) = super::find_current_rollout_path(codex_home, path).await?
    {
        *path = current_path;
    }
    codex_rollout::read_session_meta_line(path)
        .await
        .map_err(migration_error)
}

/// The caller authenticates the candidate's SessionMeta before interpreting its selection.
pub(super) async fn classify_selected_path(
    selected_path: &Path,
    candidate: &Path,
) -> ThreadStoreResult<RolloutSelection> {
    if codex_rollout::rollout_paths_match(selected_path, candidate).await {
        return Ok(RolloutSelection::Current);
    }
    let selected_plain = codex_rollout::plain_rollout_path(selected_path);
    let candidate_plain = codex_rollout::plain_rollout_path(candidate);
    if selected_plain.file_name().is_none()
        || selected_plain.file_name() != candidate_plain.file_name()
    {
        return Ok(RolloutSelection::Other);
    }
    // A matching basename preserves the physical identity, including imported names. An existing
    // selection or an unreadable directory must not be mistaken for an archive move.
    let selected_compressed = selected_plain.with_extension("jsonl.zst");
    for path in [selected_plain, selected_compressed] {
        match tokio::fs::symlink_metadata(path).await {
            Ok(_) => return Ok(RolloutSelection::Other),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(migration_error(error)),
        }
    }
    Ok(RolloutSelection::Relocated)
}

/// Requires writer ownership and freshly authenticated candidate metadata.
pub(super) async fn reconcile_selected_path(
    state_db: &StateDbHandle,
    thread_id: ThreadId,
    selected_path: &Path,
    candidate: &Path,
) -> ThreadStoreResult<()> {
    let selection = classify_selected_path(selected_path, candidate).await?;
    match selection {
        RolloutSelection::Current => return Ok(()),
        RolloutSelection::Relocated => {
            let replaced = state_db
                .replace_rollout_path_if_current(thread_id, selected_path, candidate)
                .await
                .map_err(migration_error)?;
            if replaced {
                return Ok(());
            }
        }
        RolloutSelection::Other => {}
    }
    Err(ThreadStoreError::Conflict {
        message: "selected rollout changed while waiting for migration writer ownership"
            .to_string(),
    })
}
