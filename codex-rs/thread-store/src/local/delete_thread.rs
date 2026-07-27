//! Local hard-delete support for persisted threads.
//!
//! Existing rollout files are deleted before this operation reports success. A rollout file that
//! vanishes after discovery counts as already deleted. The app-server deletes main state DB rows
//! after every associated rollout is removed; this module deletes local history projection rows.

use std::collections::HashMap;
use std::collections::HashSet;
use std::io::ErrorKind;
use std::path::Path;

use codex_protocol::RolloutId;
use codex_protocol::ThreadId;
use codex_rollout::ARCHIVED_SESSIONS_SUBDIR;
use codex_rollout::RolloutReferenceIndex;
use codex_rollout::SESSIONS_SUBDIR;
use codex_rollout::find_all_rollout_paths_by_thread_id;
use codex_rollout::remove_thread_name_entries;

use super::LocalThreadStore;
use super::helpers::matching_rollout_file_name;
use super::helpers::scoped_rollout_path;
use crate::DeleteThreadParams;
use crate::DeleteThreadsParams;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

#[derive(Clone)]
struct OwnedRollout {
    path: std::path::PathBuf,
    rollout_id: RolloutId,
    /// Noncanonical files are authenticated individually and must not authorize sibling deletion.
    authenticated_noncanonical: bool,
}

pub(super) async fn delete_thread(
    store: &LocalThreadStore,
    params: DeleteThreadParams,
) -> ThreadStoreResult<()> {
    let thread_id = params.thread_id;
    let _lifecycle_guard = store.live_writer_locks.lock_lifecycle(thread_id).await;
    let _live_writer_guard = store.live_writer_locks.lock(thread_id).await;
    let mut writer_guards = store.acquire_writer_locks(&[thread_id]).await?;
    let owned_rollouts = owned_rollouts_for_thread(store, thread_id).await?;
    let reference_index = scan_reference_index(store).await?;
    if owned_rollouts
        .iter()
        .map(|rollout| rollout.rollout_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .any(|rollout_id| reference_index.reference_count(rollout_id) > 0)
    {
        return Err(referenced_thread_error(thread_id));
    }
    delete_thread_after_reference_check(store, thread_id, owned_rollouts, &mut writer_guards).await
}

pub(super) async fn delete_threads(
    store: &LocalThreadStore,
    params: DeleteThreadsParams,
) -> ThreadStoreResult<()> {
    let thread_ids = params.thread_ids;
    if thread_ids.is_empty() {
        return Ok(());
    }

    let deletion_set: HashSet<_> = thread_ids.iter().copied().collect();
    let mut lock_thread_ids: Vec<_> = deletion_set.iter().copied().collect();
    lock_thread_ids.sort_unstable_by_key(ToString::to_string);
    let mut _lifecycle_guards = Vec::with_capacity(lock_thread_ids.len());
    for thread_id in &lock_thread_ids {
        _lifecycle_guards.push(store.live_writer_locks.lock_lifecycle(*thread_id).await);
    }
    let mut _live_writer_guards = Vec::with_capacity(lock_thread_ids.len());
    for &thread_id in &lock_thread_ids {
        _live_writer_guards.push(store.live_writer_locks.lock(thread_id).await);
    }
    let mut writer_guards = store.acquire_writer_locks(&lock_thread_ids).await?;

    let mut owned_rollouts_by_thread = HashMap::new();
    let mut targeted_rollout_ids = HashSet::new();
    for thread_id in &deletion_set {
        let owned_rollouts = owned_rollouts_for_thread(store, *thread_id).await?;
        targeted_rollout_ids.extend(owned_rollouts.iter().map(|rollout| rollout.rollout_id));
        owned_rollouts_by_thread.insert(*thread_id, owned_rollouts);
    }
    let reference_index = scan_reference_index(store).await?;
    // References from children in this delete set are removed by the same request, so only
    // references from children outside the set should block it.
    let mut internal_reference_counts = HashMap::new();
    for child_rollouts in owned_rollouts_by_thread.values() {
        for child_rollout_id in child_rollouts
            .iter()
            .map(|rollout| rollout.rollout_id)
            .collect::<HashSet<_>>()
        {
            if let Some(direct_references) = reference_index.direct_references(child_rollout_id) {
                for referenced_rollout_id in direct_references {
                    if *referenced_rollout_id != child_rollout_id
                        && targeted_rollout_ids.contains(referenced_rollout_id)
                    {
                        *internal_reference_counts
                            .entry(*referenced_rollout_id)
                            .or_default() += 1;
                    }
                }
            }
        }
    }
    for thread_id in &thread_ids {
        for rollout_id in owned_rollouts_by_thread
            .get(thread_id)
            .into_iter()
            .flatten()
            .map(|rollout| rollout.rollout_id)
            .collect::<HashSet<_>>()
        {
            let internal_reference_count = internal_reference_counts
                .get(&rollout_id)
                .copied()
                .unwrap_or_default();
            if reference_index.reference_count(rollout_id) > internal_reference_count {
                return Err(referenced_thread_error(*thread_id));
            }
        }
    }

    for thread_id in thread_ids {
        let owned_rollouts = owned_rollouts_by_thread
            .remove(&thread_id)
            .unwrap_or_default();
        match delete_thread_after_reference_check(
            store,
            thread_id,
            owned_rollouts,
            &mut writer_guards,
        )
        .await
        {
            Ok(()) | Err(ThreadStoreError::ThreadNotFound { .. }) => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

async fn scan_reference_index(
    store: &LocalThreadStore,
) -> ThreadStoreResult<RolloutReferenceIndex> {
    RolloutReferenceIndex::scan(store.config.codex_home.as_path())
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to scan fork history references: {err}"),
        })
}

fn referenced_thread_error(thread_id: codex_protocol::ThreadId) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: format!("cannot delete thread {thread_id}: forked history still references it"),
    }
}

async fn owned_rollouts_for_thread(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<Vec<OwnedRollout>> {
    let mut owned_rollouts =
        find_all_rollout_paths_by_thread_id(store.config.codex_home.as_path(), thread_id)
            .await
            .map_err(|err| ThreadStoreError::InvalidRequest {
                message: format!("failed to enumerate rollout files for thread {thread_id}: {err}"),
            })?
            .into_iter()
            .filter_map(|path| {
                codex_rollout::rollout_id_from_path(path.as_path()).map(|rollout_id| OwnedRollout {
                    path,
                    rollout_id,
                    authenticated_noncanonical: false,
                })
            })
            .collect::<Vec<_>>();

    if let Some(state_db) = store.state_db().await
        && let Some(selected_path) = state_db
            .find_rollout_path_by_id(thread_id, /*archived_only*/ None)
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to read selected rollout for {thread_id}: {err}"),
            })?
    {
        let existing_path = codex_rollout::existing_rollout_path(selected_path.as_path())
            .await
            .ok_or_else(|| ThreadStoreError::InvalidRequest {
                message: format!(
                    "selected rollout `{}` for thread {thread_id} does not exist",
                    selected_path.display()
                ),
            })?;
        let canonical_path = scoped_rollout_path(
            store.config.codex_home.join(SESSIONS_SUBDIR),
            existing_path.as_path(),
            "sessions",
        )
        .or_else(|_| {
            scoped_rollout_path(
                store.config.codex_home.join(ARCHIVED_SESSIONS_SUBDIR),
                existing_path.as_path(),
                "archived sessions",
            )
        })?;
        if let Some(encoded_thread_id) = codex_rollout::thread_id_from_path(&canonical_path) {
            if encoded_thread_id != thread_id {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!(
                        "selected rollout `{}` encodes thread {encoded_thread_id}, not {thread_id}",
                        existing_path.display()
                    ),
                });
            }
        } else {
            let mut sibling_paths = vec![codex_rollout::plain_rollout_path(&canonical_path)];
            sibling_paths.push(sibling_paths[0].with_extension("jsonl.zst"));
            for sibling_path in sibling_paths {
                if !sibling_path.exists() {
                    continue;
                }
                let sibling_path = scoped_rollout_path(
                    store.config.codex_home.join(SESSIONS_SUBDIR),
                    sibling_path.as_path(),
                    "sessions",
                )
                .or_else(|_| {
                    scoped_rollout_path(
                        store.config.codex_home.join(ARCHIVED_SESSIONS_SUBDIR),
                        sibling_path.as_path(),
                        "archived sessions",
                    )
                })?;
                let metadata = codex_rollout::read_session_meta_line_exact(sibling_path.as_path())
                    .await
                    .map_err(|err| ThreadStoreError::InvalidRequest {
                        message: format!(
                            "selected noncanonical rollout `{}` could not be authenticated: {err}",
                            sibling_path.display()
                        ),
                    })?;
                if metadata.meta.id != thread_id {
                    return Err(ThreadStoreError::InvalidRequest {
                        message: format!(
                            "selected noncanonical rollout `{}` belongs to thread {}, not {thread_id}",
                            sibling_path.display(),
                            metadata.meta.id
                        ),
                    });
                }
                if !owned_rollouts
                    .iter()
                    .any(|rollout| rollout.path == sibling_path)
                {
                    owned_rollouts.push(OwnedRollout {
                        path: sibling_path,
                        rollout_id: thread_id,
                        authenticated_noncanonical: true,
                    });
                }
            }
        }
        if codex_rollout::thread_id_from_path(&canonical_path).is_none()
            && !owned_rollouts
                .iter()
                .any(|rollout| rollout.authenticated_noncanonical)
        {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!(
                    "selected noncanonical rollout `{}` has no readable representation",
                    existing_path.display()
                ),
            });
        }
    }

    owned_rollouts.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(owned_rollouts)
}

async fn delete_thread_after_reference_check(
    store: &LocalThreadStore,
    thread_id: codex_protocol::ThreadId,
    owned_rollouts: Vec<OwnedRollout>,
    writer_guards: &mut Vec<super::writer_lock::WriterLockGuard>,
) -> ThreadStoreResult<()> {
    let mut rollout_ids = owned_rollouts
        .iter()
        .map(|rollout| rollout.rollout_id)
        .collect::<HashSet<_>>();
    // Remove rows created before rollout replacement used the physical rollout ID as its key.
    rollout_ids.insert(thread_id);
    super::thread_history::delete_threads(store, &rollout_ids.into_iter().collect::<Vec<_>>())
        .await?;

    // Drop the recorder before removing files, but retain its writer lock until cleanup finishes.
    if let Some(entry) = store.live_recorders.lock().await.remove(&thread_id) {
        writer_guards.push(entry.writer_lock);
    }
    let found_rollout_path = !owned_rollouts.is_empty();
    for rollout in owned_rollouts {
        delete_rollout_file(store, &rollout, thread_id)?;
    }
    remove_thread_name_entries(store.config.codex_home.as_path(), thread_id)
        .await
        .map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to delete thread name index entries for {thread_id}: {err}"),
        })?;

    if !found_rollout_path {
        return Err(ThreadStoreError::ThreadNotFound { thread_id });
    }

    Ok(())
}

fn delete_rollout_file(
    store: &LocalThreadStore,
    rollout: &OwnedRollout,
    thread_id: codex_protocol::ThreadId,
) -> ThreadStoreResult<bool> {
    if rollout.authenticated_noncanonical {
        return delete_rollout_path(
            store,
            rollout.path.as_path(),
            thread_id,
            /*authenticated_noncanonical*/ true,
        );
    }
    let plain_path = codex_rollout::plain_rollout_path(rollout.path.as_path());
    let compressed_path = plain_path.with_extension("jsonl.zst");
    let deleted_plain = delete_rollout_path(
        store,
        plain_path.as_path(),
        thread_id,
        rollout.authenticated_noncanonical,
    )?;
    let deleted_compressed = delete_rollout_path(
        store,
        compressed_path.as_path(),
        thread_id,
        rollout.authenticated_noncanonical,
    )?;
    Ok(deleted_plain || deleted_compressed)
}

fn delete_rollout_path(
    store: &LocalThreadStore,
    rollout_path: &Path,
    thread_id: codex_protocol::ThreadId,
    authenticated_noncanonical: bool,
) -> ThreadStoreResult<bool> {
    let canonical_rollout_path = scoped_rollout_path(
        store.config.codex_home.join(SESSIONS_SUBDIR),
        rollout_path,
        "sessions",
    )
    .or_else(|_| {
        scoped_rollout_path(
            store.config.codex_home.join(ARCHIVED_SESSIONS_SUBDIR),
            rollout_path,
            "archived sessions",
        )
    })
    .or_else(|err| match rollout_path.try_exists() {
        Ok(false) => Ok(rollout_path.to_path_buf()),
        Ok(true) | Err(_) => Err(err),
    })?;
    if !authenticated_noncanonical {
        matching_rollout_file_name(&canonical_rollout_path, thread_id, rollout_path)?;
    }
    match std::fs::remove_file(&canonical_rollout_path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
        Err(err) => Err(ThreadStoreError::Internal {
            message: format!(
                "failed to delete rollout file `{}`: {err}",
                canonical_rollout_path.display()
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use codex_protocol::ThreadId;
    use codex_protocol::protocol::HistoryPosition;
    use codex_protocol::protocol::RolloutReferenceItem;
    use codex_protocol::protocol::ThreadHistoryMode;
    use codex_protocol::protocol::ThreadMemoryMode;
    use codex_rollout::RolloutItem;
    use codex_rollout::RolloutLine;
    use codex_utils_absolute_path::test_support::PathExt;
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::ResumeThreadParams;
    use crate::ThreadPersistenceMetadata;
    use crate::ThreadStore;
    use crate::local::LocalThreadStore;
    use crate::local::test_support::test_config;
    use crate::local::test_support::write_archived_session_file;
    use crate::local::test_support::write_session_file;
    use crate::local::test_support::write_session_file_with;
    use crate::local::test_support::write_session_file_with_history_mode;

    #[tokio::test]
    async fn delete_thread_removes_active_and_archived_rollouts() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let active_path =
            write_session_file(home.path(), "2025-01-03T12-00-00", Uuid::from_u128(301))
                .expect("session file");
        let compressed_path = active_path.with_extension("jsonl.zst");
        std::fs::write(&compressed_path, b"compressed sibling").expect("compressed sibling");
        let cases = [
            (Uuid::from_u128(301), active_path),
            (
                Uuid::from_u128(302),
                write_archived_session_file(
                    home.path(),
                    "2025-01-03T12-00-00",
                    Uuid::from_u128(302),
                )
                .expect("archived session file"),
            ),
        ];

        for (uuid, path) in cases {
            let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
            store
                .delete_thread(DeleteThreadParams { thread_id })
                .await
                .expect("delete thread");

            assert!(!path.exists());
        }
        assert!(!compressed_path.exists());
    }

    #[tokio::test]
    async fn delete_thread_rejects_referenced_paginated_history() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let source_uuid = Uuid::from_u128(303);
        let source_thread_id =
            ThreadId::from_string(&source_uuid.to_string()).expect("valid source thread id");
        let source_path = write_session_file_with_history_mode(
            home.path(),
            "2025-01-03T12-00-00",
            source_uuid,
            ThreadHistoryMode::Paginated,
        )
        .expect("source session file");
        let child_path = write_session_file_with(
            home.path(),
            home.path().join(ARCHIVED_SESSIONS_SUBDIR),
            "2025-01-03T12-00-01",
            Uuid::from_u128(304),
            "Archived user message",
            Some("test-provider"),
            ThreadHistoryMode::Paginated,
        )
        .expect("child session file");
        set_history_base(
            child_path.as_path(),
            HistoryPosition {
                thread_id: source_thread_id,
                end_ordinal_exclusive: 1,
                end_byte_offset: std::fs::metadata(source_path.as_path())
                    .expect("source rollout metadata")
                    .len(),
            },
        );

        let err = store
            .delete_thread(DeleteThreadParams {
                thread_id: source_thread_id,
            })
            .await
            .expect_err("referenced source should not be deleted");

        assert_eq!(
            err.to_string(),
            format!(
                "invalid thread-store request: cannot delete thread {source_thread_id}: forked history still references it"
            )
        );
        assert!(source_path.exists());
    }

    #[tokio::test]
    async fn delete_thread_rejects_reference_to_reverted_rollout() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let thread_uuid = Uuid::from_u128(320);
        let thread_id = ThreadId::from_string(&thread_uuid.to_string()).expect("thread id");
        let source_path = write_session_file_with_history_mode(
            home.path(),
            "2025-01-03T12-00-00",
            thread_uuid,
            ThreadHistoryMode::Paginated,
        )
        .expect("source session file");
        let replacement_uuid = Uuid::from_u128(321);
        let replacement_rollout_id =
            ThreadId::from_string(&replacement_uuid.to_string()).expect("replacement rollout id");
        let replacement_path = write_session_file_with_history_mode(
            home.path(),
            "2025-01-03T12-00-01",
            replacement_uuid,
            ThreadHistoryMode::Paginated,
        )
        .expect("replacement session file");
        set_thread_id(replacement_path.as_path(), thread_id);
        set_history_base(
            replacement_path.as_path(),
            HistoryPosition {
                thread_id,
                end_ordinal_exclusive: 1,
                end_byte_offset: std::fs::metadata(source_path.as_path())
                    .expect("source rollout metadata")
                    .len(),
            },
        );
        let child_path = write_session_file_with_history_mode(
            home.path(),
            "2025-01-03T12-00-02",
            Uuid::from_u128(322),
            ThreadHistoryMode::Paginated,
        )
        .expect("child session file");
        set_history_base(
            child_path.as_path(),
            HistoryPosition {
                thread_id: replacement_rollout_id,
                end_ordinal_exclusive: 1,
                end_byte_offset: std::fs::metadata(replacement_path.as_path())
                    .expect("replacement rollout metadata")
                    .len(),
            },
        );

        let err = store
            .delete_thread(DeleteThreadParams { thread_id })
            .await
            .expect_err("referenced replacement should not be deleted");

        assert_eq!(
            err.to_string(),
            format!(
                "invalid thread-store request: cannot delete thread {thread_id}: forked history still references it"
            )
        );
        assert!(source_path.exists());
        assert!(replacement_path.exists());
    }

    #[tokio::test]
    async fn delete_thread_ignores_unreadable_reference_metadata() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let source_uuid = Uuid::from_u128(305);
        let source_thread_id =
            ThreadId::from_string(&source_uuid.to_string()).expect("valid source thread id");
        let source_path = write_session_file(home.path(), "2025-01-03T12-00-00", source_uuid)
            .expect("source session file");
        let unreadable_path = source_path.with_file_name(format!(
            "rollout-2025-01-03T12-00-01-{}.jsonl",
            Uuid::from_u128(306)
        ));
        std::fs::write(unreadable_path, "{not json}\n").expect("unreadable rollout metadata");

        store
            .delete_thread(DeleteThreadParams {
                thread_id: source_thread_id,
            })
            .await
            .expect("unreadable metadata should not block delete");

        assert!(!source_path.exists());
    }

    #[tokio::test]
    async fn delete_threads_allows_internal_history_references() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let source_uuid = Uuid::from_u128(307);
        let source_thread_id =
            ThreadId::from_string(&source_uuid.to_string()).expect("valid source thread id");
        let source_path = write_session_file_with_history_mode(
            home.path(),
            "2025-01-03T12-00-00",
            source_uuid,
            ThreadHistoryMode::Paginated,
        )
        .expect("source session file");
        let child_uuid = Uuid::from_u128(308);
        let child_thread_id =
            ThreadId::from_string(&child_uuid.to_string()).expect("valid child thread id");
        let child_path = write_session_file_with_history_mode(
            home.path(),
            "2025-01-03T12-00-01",
            child_uuid,
            ThreadHistoryMode::Paginated,
        )
        .expect("child session file");
        set_history_base(
            child_path.as_path(),
            HistoryPosition {
                thread_id: source_thread_id,
                end_ordinal_exclusive: 1,
                end_byte_offset: std::fs::metadata(source_path.as_path())
                    .expect("source rollout metadata")
                    .len(),
            },
        );

        store
            .delete_threads(DeleteThreadsParams {
                thread_ids: vec![child_thread_id, source_thread_id],
            })
            .await
            .expect("internal references should not block batch delete");

        assert!(!source_path.exists());
        assert!(!child_path.exists());
    }

    #[tokio::test]
    async fn delete_thread_checks_every_physical_rollout_reference_before_deleting() {
        let home = TempDir::new().expect("temp dir");
        let thread_uuid = Uuid::from_u128(320);
        let thread_id = ThreadId::from_string(&thread_uuid.to_string()).expect("thread id");
        let first_rollout_id =
            ThreadId::from_string(&Uuid::from_u128(321).to_string()).expect("first rollout id");
        let second_rollout_id =
            ThreadId::from_string(&Uuid::from_u128(322).to_string()).expect("second rollout id");
        let source = write_session_file_with_history_mode(
            home.path(),
            "2025-01-03T12-00-00",
            thread_uuid,
            ThreadHistoryMode::Paginated,
        )
        .expect("source session file");
        let first_path = source.with_file_name(format!(
            "rollout-2025-01-03T12-00-00-{thread_id}_{first_rollout_id}.jsonl"
        ));
        std::fs::rename(&source, &first_path).expect("rename first physical rollout");
        let archived_root = home.path().join(ARCHIVED_SESSIONS_SUBDIR);
        std::fs::create_dir_all(&archived_root).expect("archived root");
        let second_path = archived_root.join(format!(
            "rollout-2025-01-03T12-00-01-{thread_id}_{second_rollout_id}.jsonl"
        ));
        std::fs::copy(&first_path, &second_path).expect("copy second physical rollout");

        let child_uuid = Uuid::from_u128(323);
        let child_id = ThreadId::from_string(&child_uuid.to_string()).expect("child id");
        let child_path = write_session_file_with_history_mode(
            home.path(),
            "2025-01-03T12-00-02",
            child_uuid,
            ThreadHistoryMode::Paginated,
        )
        .expect("child session file");
        let reference = RolloutLine {
            timestamp: "2025-01-03T12:00:02Z".to_string(),
            ordinal: Some(1),
            item: RolloutItem::RolloutReference(RolloutReferenceItem {
                rollout_path: first_path.clone(),
                thread_id: Some(thread_id),
                rollout_id: Some(first_rollout_id),
                rollout_timestamp: None,
                segment_id: None,
                max_depth: 2,
                nth_user_message: None,
                compacted_replacement_history_filter_texts: None,
            }),
        };
        let contents = std::fs::read_to_string(&child_path).expect("read child rollout");
        let (head, tail) = contents
            .split_once('\n')
            .expect("child rollout has session metadata");
        std::fs::write(
            &child_path,
            format!(
                "{head}\n{}\n{tail}",
                serde_json::to_string(&reference).expect("serialize reference")
            ),
        )
        .expect("insert child reference");
        let archived_child_path =
            archived_root.join(child_path.file_name().expect("child rollout filename"));
        std::fs::copy(&child_path, &archived_child_path)
            .expect("copy duplicate child physical rollout");
        let external_uuid = Uuid::from_u128(325);
        let external_id = ThreadId::from_string(&external_uuid.to_string()).expect("external id");
        let external_path = write_session_file_with_history_mode(
            home.path(),
            "2025-01-03T12-00-03",
            external_uuid,
            ThreadHistoryMode::Paginated,
        )
        .expect("external child session file");
        let contents = std::fs::read_to_string(&external_path).expect("read external rollout");
        let (head, tail) = contents
            .split_once('\n')
            .expect("external rollout has session metadata");
        std::fs::write(
            &external_path,
            format!(
                "{head}\n{}\n{tail}",
                serde_json::to_string(&reference).expect("serialize external reference")
            ),
        )
        .expect("insert external reference");
        let config = test_config(home.path());
        let rollout_config = codex_rollout::RolloutConfig {
            codex_home: config.codex_home.clone(),
            sqlite: config.sqlite.clone(),
            cwd: home.path().to_path_buf(),
            model_provider_id: config.default_model_provider_id.clone(),
            generate_memories: false,
        };
        let state_db = codex_rollout::state_db::try_init(&rollout_config)
            .await
            .expect("backfill physical rollouts");
        let store = LocalThreadStore::new(config, Some(state_db));
        for rollout_id in [thread_id, first_rollout_id, second_rollout_id] {
            super::super::thread_history::apply_projection(
                &store,
                rollout_id,
                /*start_offset*/ 0,
                /*next_offset*/ 0,
                /*initial_ordinal*/ 0,
                Vec::new(),
            )
            .await
            .expect("seed physical projection");
        }

        let error = store
            .delete_thread(DeleteThreadParams { thread_id })
            .await
            .expect_err("old physical rollout reference must block deletion");
        assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));
        assert!(first_path.exists());
        assert!(second_path.exists());
        assert!(child_path.exists());

        let error = store
            .delete_threads(DeleteThreadsParams {
                thread_ids: vec![thread_id, child_id],
            })
            .await
            .expect_err("external reference must survive duplicate internal child subtraction");
        assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));
        assert!(first_path.exists());
        assert!(second_path.exists());
        assert!(child_path.exists());
        assert!(archived_child_path.exists());

        store
            .delete_threads(DeleteThreadsParams {
                thread_ids: vec![thread_id, child_id, external_id],
            })
            .await
            .expect("batch deletion should discount each direct reference once");
        assert!(!first_path.exists());
        assert!(!second_path.exists());
        assert!(!child_path.exists());
        assert!(!archived_child_path.exists());
        assert!(!external_path.exists());
        for rollout_id in [thread_id, first_rollout_id, second_rollout_id] {
            assert!(
                super::super::thread_history::projection_state(&store, rollout_id)
                    .await
                    .expect("read deleted projection")
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn delete_thread_accepts_authenticated_selected_noncanonical_rollout() {
        let home = TempDir::new().expect("temp dir");
        let uuid = Uuid::from_u128(324);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("thread id");
        let canonical_path =
            write_session_file(home.path(), "2025-01-03T12-00-00", uuid).expect("session file");
        let imported_path = canonical_path.with_file_name("rollout-imported.jsonl");
        std::fs::rename(&canonical_path, &imported_path).expect("rename imported rollout");
        let config = test_config(home.path());
        let rollout_config = codex_rollout::RolloutConfig {
            codex_home: config.codex_home.clone(),
            sqlite: config.sqlite.clone(),
            cwd: home.path().to_path_buf(),
            model_provider_id: config.default_model_provider_id.clone(),
            generate_memories: false,
        };
        let state_db = codex_rollout::state_db::try_init(&rollout_config)
            .await
            .expect("backfill imported rollout");
        let store = LocalThreadStore::new(config, Some(state_db));

        store
            .delete_thread(DeleteThreadParams { thread_id })
            .await
            .expect("delete authenticated imported rollout");

        assert!(!imported_path.exists());
    }

    #[tokio::test]
    async fn delete_thread_rejects_mismatched_noncanonical_sibling_before_mutation() {
        let home = TempDir::new().expect("temp dir");
        let uuid = Uuid::from_u128(326);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("thread id");
        let canonical_path =
            write_session_file(home.path(), "2025-01-03T12-00-00", uuid).expect("session file");
        let imported_path = canonical_path.with_file_name("rollout-imported.jsonl");
        std::fs::rename(&canonical_path, &imported_path).expect("rename imported rollout");
        let config = test_config(home.path());
        let rollout_config = codex_rollout::RolloutConfig {
            codex_home: config.codex_home.clone(),
            sqlite: config.sqlite.clone(),
            cwd: home.path().to_path_buf(),
            model_provider_id: config.default_model_provider_id.clone(),
            generate_memories: false,
        };
        let state_db = codex_rollout::state_db::try_init(&rollout_config)
            .await
            .expect("backfill imported rollout");

        let sibling_uuid = Uuid::from_u128(327);
        let sibling_source = write_session_file(home.path(), "2025-01-03T12-00-01", sibling_uuid)
            .expect("sibling session file");
        let compressed_sibling = imported_path.with_extension("jsonl.zst");
        let sibling_bytes = std::fs::read(&sibling_source).expect("read sibling rollout");
        std::fs::write(
            &compressed_sibling,
            zstd::stream::encode_all(sibling_bytes.as_slice(), 3).expect("compress sibling"),
        )
        .expect("write mismatched compressed sibling");
        let store = LocalThreadStore::new(config, Some(state_db));

        let error = store
            .delete_thread(DeleteThreadParams { thread_id })
            .await
            .expect_err("mismatched sibling must fail before deletion");
        assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));
        assert!(imported_path.exists());
        assert!(compressed_sibling.exists());
    }

    #[tokio::test]
    async fn delete_thread_rejects_missing_selected_rollout_before_mutation() {
        let home = TempDir::new().expect("temp dir");
        let uuid = Uuid::from_u128(328);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("thread id");
        let rollout_path =
            write_session_file(home.path(), "2025-01-03T12-00-00", uuid).expect("session file");
        let config = test_config(home.path());
        let rollout_config = codex_rollout::RolloutConfig {
            codex_home: config.codex_home.clone(),
            sqlite: config.sqlite.clone(),
            cwd: home.path().to_path_buf(),
            model_provider_id: config.default_model_provider_id.clone(),
            generate_memories: false,
        };
        let state_db = codex_rollout::state_db::try_init(&rollout_config)
            .await
            .expect("backfill rollout");
        let mut metadata = state_db
            .get_thread(thread_id)
            .await
            .expect("read selected rollout")
            .expect("thread metadata");
        metadata.rollout_path = rollout_path.with_file_name("rollout-missing.jsonl");
        state_db
            .upsert_thread(&metadata)
            .await
            .expect("select missing rollout");
        let store = LocalThreadStore::new(config, Some(state_db));

        let error = store
            .delete_thread(DeleteThreadParams { thread_id })
            .await
            .expect_err("missing selected rollout must fail before deletion");
        assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));
        assert!(rollout_path.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delete_thread_rejects_out_of_scope_noncanonical_sibling_before_mutation() {
        use std::os::unix::fs::symlink;

        let home = TempDir::new().expect("temp dir");
        let uuid = Uuid::from_u128(329);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("thread id");
        let canonical_path =
            write_session_file(home.path(), "2025-01-03T12-00-00", uuid).expect("session file");
        let imported_path = canonical_path.with_file_name("rollout-imported.jsonl");
        std::fs::rename(&canonical_path, &imported_path).expect("rename imported rollout");
        let config = test_config(home.path());
        let rollout_config = codex_rollout::RolloutConfig {
            codex_home: config.codex_home.clone(),
            sqlite: config.sqlite.clone(),
            cwd: home.path().to_path_buf(),
            model_provider_id: config.default_model_provider_id.clone(),
            generate_memories: false,
        };
        let state_db = codex_rollout::state_db::try_init(&rollout_config)
            .await
            .expect("backfill imported rollout");
        let outside_sibling = home.path().join("outside-rollout.jsonl.zst");
        std::fs::create_dir_all(home.path().join(ARCHIVED_SESSIONS_SUBDIR))
            .expect("create archived sessions root");
        std::fs::write(
            &outside_sibling,
            zstd::stream::encode_all(
                std::fs::read(&imported_path)
                    .expect("read imported rollout")
                    .as_slice(),
                3,
            )
            .expect("compress outside sibling"),
        )
        .expect("write outside sibling");
        let sibling_link = imported_path.with_extension("jsonl.zst");
        symlink(&outside_sibling, &sibling_link).expect("link outside sibling");
        let store = LocalThreadStore::new(config, Some(state_db));
        super::super::thread_history::apply_projection(&store, thread_id, 0, 0, 0, Vec::new())
            .await
            .expect("seed projection");

        let error = store
            .delete_thread(DeleteThreadParams { thread_id })
            .await
            .expect_err("out-of-scope sibling must fail before deletion");
        assert!(matches!(error, ThreadStoreError::InvalidRequest { .. }));
        assert!(imported_path.exists());
        assert!(
            super::super::thread_history::projection_state(&store, thread_id)
                .await
                .expect("read projection")
                .is_some()
        );
    }

    #[tokio::test]
    async fn delete_threads_rejects_owned_descendants_before_deleting_anything() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let owner = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        for (parent_uuid, child_uuid, history_mode) in [
            (
                Uuid::from_u128(309),
                Uuid::from_u128(310),
                ThreadHistoryMode::Legacy,
            ),
            (
                Uuid::from_u128(312),
                Uuid::from_u128(313),
                ThreadHistoryMode::Paginated,
            ),
        ] {
            let parent_thread_id =
                ThreadId::from_string(&parent_uuid.to_string()).expect("valid parent thread id");
            let parent_path = write_session_file_with_history_mode(
                home.path(),
                "2025-01-03T12-00-00",
                parent_uuid,
                history_mode,
            )
            .expect("parent session file");
            let child_thread_id =
                ThreadId::from_string(&child_uuid.to_string()).expect("valid child thread id");
            let child_path = write_session_file_with_history_mode(
                home.path(),
                "2025-01-03T12-00-01",
                child_uuid,
                history_mode,
            )
            .expect("child session file");
            let _owner_guard = owner
                .writer_lock_coordinator
                .acquire(child_thread_id)
                .expect("acquire child writer lock");

            let error = store
                .delete_threads(DeleteThreadsParams {
                    thread_ids: vec![parent_thread_id, child_thread_id],
                })
                .await
                .expect_err("owned descendant should block deletion");

            assert!(matches!(error, ThreadStoreError::Conflict { .. }));
            assert!(parent_path.exists());
            assert!(child_path.exists());
        }
    }

    #[tokio::test]
    async fn delete_threads_rejects_owned_thread_before_rollout_materializes() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let owner = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let thread_id = ThreadId::default();
        let _owner_guard = owner
            .writer_lock_coordinator
            .acquire(thread_id)
            .expect("acquire writer lock before rollout exists");

        let error = store
            .delete_threads(DeleteThreadsParams {
                thread_ids: vec![thread_id],
            })
            .await
            .expect_err("owned thread should block deletion before rollout exists");

        assert!(matches!(error, ThreadStoreError::Conflict { .. }));
    }

    #[tokio::test]
    async fn delete_threads_removes_rollout_with_unreadable_metadata() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let uuid = Uuid::from_u128(311);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path =
            write_session_file(home.path(), "2025-01-03T12-00-00", uuid).expect("session file");
        std::fs::write(&rollout_path, "{not json}\n").expect("damage rollout metadata");

        store
            .delete_threads(DeleteThreadsParams {
                thread_ids: vec![thread_id],
            })
            .await
            .expect("delete rollout with unreadable metadata");

        assert!(!rollout_path.exists());
    }

    #[tokio::test]
    async fn delete_rollout_file_treats_vanished_path_as_already_deleted() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let uuid = Uuid::from_u128(305);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let path =
            write_session_file(home.path(), "2025-01-03T12-00-00", uuid).expect("session file");
        std::fs::remove_file(&path).expect("remove session file");

        assert!(
            !delete_rollout_file(
                &store,
                &OwnedRollout {
                    path,
                    rollout_id: thread_id,
                    authenticated_noncanonical: false,
                },
                thread_id,
            )
            .expect("delete rollout")
        );
    }

    #[tokio::test]
    async fn delete_thread_without_state_db_preserves_materialized_thread_history() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let store = LocalThreadStore::new(config.clone(), /*state_db*/ None);
        let uuid = Uuid::from_u128(312);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path = write_session_file_with_history_mode(
            home.path(),
            "2025-01-03T12-00-00",
            uuid,
            ThreadHistoryMode::Paginated,
        )
        .expect("session file");
        let pool = codex_state::open_thread_history_db(&config.sqlite)
            .await
            .expect("open existing thread history database");
        let thread_id_string = thread_id.to_string();
        sqlx::query(
            "INSERT INTO thread_turns (thread_id, turn_id, rollout_ordinal, status) VALUES (?, 'turn-1', 1, 'completed')",
        )
        .bind(thread_id_string.as_str())
        .execute(&pool)
        .await
        .expect("insert turn");
        sqlx::query(
            "INSERT INTO thread_items (thread_id, turn_id, item_id, rollout_ordinal, created_at_ms, item_json) VALUES (?, 'turn-1', 'item-1', 2, 1, '{}')",
        )
        .bind(thread_id_string.as_str())
        .execute(&pool)
        .await
        .expect("insert item");
        sqlx::query(
            "INSERT INTO thread_realtime_items (thread_id, item_id, rollout_ordinal, created_at_ms, item_type, item_json) VALUES (?, 'realtime-1', 3, 1, 'realtime_session_started', '{}')",
        )
        .bind(thread_id_string.as_str())
        .execute(&pool)
        .await
        .expect("insert realtime item");
        sqlx::query(
            "INSERT INTO thread_history_projection_state (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, 3, 3)",
        )
        .bind(thread_id_string.as_str())
        .execute(&pool)
        .await
        .expect("insert projection state");

        let error = store
            .delete_thread(DeleteThreadParams { thread_id })
            .await
            .expect_err("projected history without a state database should prevent deletion");

        assert!(matches!(
            error,
            ThreadStoreError::Unsupported {
                operation: "paginated_history"
            }
        ));
        assert!(rollout_path.exists());
        let counts = sqlx::query_as::<_, (i64, i64, i64, i64)>(
            r#"
SELECT
    (SELECT COUNT(*) FROM thread_turns WHERE thread_id = ?),
    (SELECT COUNT(*) FROM thread_items WHERE thread_id = ?),
    (SELECT COUNT(*) FROM thread_realtime_items WHERE thread_id = ?),
    (SELECT COUNT(*) FROM thread_history_projection_state WHERE thread_id = ?)
            "#,
        )
        .bind(thread_id_string.as_str())
        .bind(thread_id_string.as_str())
        .bind(thread_id_string.as_str())
        .bind(thread_id_string.as_str())
        .fetch_one(&pool)
        .await
        .expect("read preserved history rows");
        assert_eq!(counts, (1, 1, 1, 1));
    }

    #[tokio::test]
    async fn delete_thread_removes_materialized_thread_history() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let state_db = codex_state::StateRuntime::init(
            config.sqlite.clone(),
            config.default_model_provider_id.clone(),
        )
        .await
        .expect("initialize state database for materialized history");
        let store = LocalThreadStore::new(config, Some(state_db));
        let uuid = Uuid::from_u128(306);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path = write_session_file_with_history_mode(
            home.path(),
            "2025-01-03T12-00-00",
            uuid,
            ThreadHistoryMode::Paginated,
        )
        .expect("session file");
        let pool = codex_state::open_thread_history_db(
            &codex_state::SqliteConfig::new_for_testing(home.path().abs()),
        )
        .await
        .expect("open thread history db");
        let thread_id_string = thread_id.to_string();
        sqlx::query(
            "INSERT INTO thread_turns (thread_id, turn_id, rollout_ordinal, status) VALUES (?, 'turn-1', 1, 'completed')",
        )
        .bind(thread_id_string.as_str())
        .execute(&pool)
        .await
        .expect("insert turn");
        sqlx::query(
            "INSERT INTO thread_items (thread_id, turn_id, item_id, rollout_ordinal, created_at_ms, item_json) VALUES (?, 'turn-1', 'item-1', 2, 1, '{}')",
        )
        .bind(thread_id_string.as_str())
        .execute(&pool)
        .await
        .expect("insert item");
        sqlx::query(
            "INSERT INTO thread_realtime_items (thread_id, item_id, rollout_ordinal, created_at_ms, item_type, item_json) VALUES (?, 'realtime-1', 3, 1, 'realtime_session_started', '{}')",
        )
        .bind(thread_id_string.as_str())
        .execute(&pool)
        .await
        .expect("insert realtime item");
        sqlx::query(
            "INSERT INTO thread_history_projection_state (thread_id, next_rollout_byte_offset, next_rollout_ordinal) VALUES (?, 3, 3)",
        )
        .bind(thread_id_string.as_str())
        .execute(&pool)
        .await
        .expect("insert projection state");

        store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(rollout_path),
                history: None,
                include_archived: false,
                metadata: ThreadPersistenceMetadata {
                    cwd: Some(home.path().to_path_buf()),
                    model_provider: "test-provider".to_string(),
                    memory_mode: ThreadMemoryMode::Enabled,
                },
            })
            .await
            .expect("resume paginated writer before deletion");
        let lock_path = home
            .path()
            .join("thread-writer-locks")
            .join(format!("{thread_id}.lock"));
        assert!(lock_path.exists());

        store
            .delete_thread(DeleteThreadParams { thread_id })
            .await
            .expect("delete thread");
        assert!(!lock_path.exists());

        let counts = sqlx::query_as::<_, (i64, i64, i64, i64)>(
            r#"
SELECT
    (SELECT COUNT(*) FROM thread_turns WHERE thread_id = ?),
    (SELECT COUNT(*) FROM thread_items WHERE thread_id = ?),
    (SELECT COUNT(*) FROM thread_realtime_items WHERE thread_id = ?),
    (SELECT COUNT(*) FROM thread_history_projection_state WHERE thread_id = ?)
            "#,
        )
        .bind(thread_id_string.as_str())
        .bind(thread_id_string.as_str())
        .bind(thread_id_string.as_str())
        .bind(thread_id_string.as_str())
        .fetch_one(&pool)
        .await
        .expect("read remaining history rows");
        assert_eq!(counts, (0, 0, 0, 0));
    }

    #[tokio::test]
    async fn delete_thread_reports_missing_thread() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
        let thread_id =
            ThreadId::from_string("00000000-0000-0000-0000-000000000304").expect("valid thread id");

        let err = store
            .delete_thread(DeleteThreadParams { thread_id })
            .await
            .expect_err("missing thread should fail");
        assert_eq!(
            err.to_string(),
            "thread 00000000-0000-0000-0000-000000000304 not found"
        );
    }

    fn set_history_base(path: &Path, history_base: HistoryPosition) {
        let mut session_meta: serde_json::Value = serde_json::from_str(
            std::fs::read_to_string(path)
                .expect("read session file")
                .lines()
                .next()
                .expect("session metadata"),
        )
        .expect("parse session metadata");
        session_meta["payload"]["history_base"] =
            serde_json::to_value(history_base).expect("serialize history base");
        std::fs::write(path, format!("{session_meta}\n")).expect("write session file");
    }

    fn set_thread_id(path: &Path, thread_id: ThreadId) {
        let mut session_meta: serde_json::Value = serde_json::from_str(
            std::fs::read_to_string(path)
                .expect("read session file")
                .lines()
                .next()
                .expect("session metadata"),
        )
        .expect("parse session metadata");
        session_meta["payload"]["id"] = serde_json::to_value(thread_id).expect("thread id");
        std::fs::write(path, format!("{session_meta}\n")).expect("write session file");
    }
}
