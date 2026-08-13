use super::*;
use codex_app_server_protocol::ThreadArchiveParams;
use codex_app_server_protocol::ThreadArchiveResponse;
use codex_app_server_protocol::ThreadDeleteParams;
use codex_thread_store::ReadThreadParams as StoreReadThreadParams;
use codex_thread_store::ThreadStoreError;

fn remaining_ids(members: &BTreeMap<String, ThreadId>) -> HashSet<ThreadId> {
    members.values().copied().collect()
}

#[tokio::test]
async fn partial_archive_retires_current_only_descendants_of_successful_ancestors() -> Result<()> {
    let mut fixture = build_current_tree().await?;
    let branches = branch_ids(&fixture)?;
    let failure = branches[0];
    let success = branches[1];
    fixture.store.fail_archive_thread(failure.0).await;

    let _: ThreadArchiveResponse = request(
        &fixture.app,
        ClientRequest::ThreadArchive {
            request_id: next_request_id(),
            params: ThreadArchiveParams {
                thread_id: fixture.root_thread_id.to_string(),
            },
        },
    )
    .await?;
    let archived = read_archived_notifications(&mut fixture.app, /*count*/ 2).await?;
    pretty_assertions::assert_eq!(archived, HashSet::from([fixture.root_thread_id, success.0]));
    let mut expected_remaining = fixture.ids_by_path.clone();
    expected_remaining.retain(|path, _| path.starts_with("/root/reconciliation_a"));
    pretty_assertions::assert_eq!(
        list_current_members(&fixture.app, fixture.root_thread_id).await?,
        expected_remaining
    );
    Ok(())
}

#[tokio::test]
async fn partial_delete_notifies_and_retires_only_the_successful_current_branch() -> Result<()> {
    let mut fixture = build_current_tree().await?;
    let branches = branch_ids(&fixture)?;
    let failure = branches[0];
    let success = branches[1];
    fixture.store.fail_delete_thread(failure.0).await;

    let error = fixture
        .app
        .request(ClientRequest::ThreadDelete {
            request_id: next_request_id(),
            params: ThreadDeleteParams {
                thread_id: fixture.root_thread_id.to_string(),
            },
        })
        .await?
        .expect_err("injected member failure should fail thread/delete");
    let deleted = read_deleted_notifications(&mut fixture.app, /*count*/ 2).await?;
    assert!(error.message.contains("injected delete failure"));
    pretty_assertions::assert_eq!(deleted, HashSet::from([success.0, success.1]));
    pretty_assertions::assert_eq!(
        remaining_ids(&list_current_members(&fixture.app, fixture.root_thread_id,).await?),
        HashSet::from([failure.0, failure.1])
    );
    assert!(matches!(
        ThreadStore::read_thread(
            fixture.store.as_ref(),
            StoreReadThreadParams {
                thread_id: success.0,
                include_archived: true,
                include_history: false,
            },
        )
        .await,
        Err(ThreadStoreError::ThreadNotFound { .. })
    ));
    for preserved_id in [failure.0, fixture.root_thread_id] {
        ThreadStore::read_thread(
            fixture.store.as_ref(),
            StoreReadThreadParams {
                thread_id: preserved_id,
                include_archived: true,
                include_history: false,
            },
        )
        .await?;
    }
    pretty_assertions::assert_eq!(
        fixture
            .state_db
            .list_thread_spawn_descendants(fixture.root_thread_id)
            .await?,
        vec![failure.0]
    );
    Ok(())
}
