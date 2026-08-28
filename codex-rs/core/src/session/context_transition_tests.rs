use super::*;
use crate::responses_metadata::CodexResponsesRequestKind;
use crate::session::step_activation::tests::ActivationFixture;
use crate::session::step_activation::tests::activation_fixture;
use crate::session::step_activation::tests::activation_models;
use crate::session::tests::HeldStepTask;
use crate::state::TaskKind;
use crate::turn_metadata::McpTurnMetadataContext;
use codex_protocol::config_types::ReasoningSummary;
use codex_protocol::openai_models::ModelInfo;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::protocol::TurnSettingsUpdate;
use codex_protocol::protocol::TurnSettingsUpdateOutcome;
use pretty_assertions::assert_eq;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;

async fn active_task_identity(session: &Session) -> (Arc<Notify>, CancellationToken) {
    let active = session.active_turn.lock().await;
    let task = active
        .as_ref()
        .and_then(|turn| turn.task.as_ref())
        .expect("active task");
    (Arc::clone(&task.done), task.cancellation_token.clone())
}

#[tokio::test]
async fn workspace_publication_retries_explicit_settings_and_shares_late_metadata() {
    let ActivationFixture {
        session,
        turn,
        finish: _,
        lookup: _,
    } = activation_fixture(activation_models()).await;
    turn.turn_metadata_state
        .set_parent_turn_id("parent-turn".to_string());
    turn.turn_metadata_state
        .set_root_turn_id("root-turn".to_string());
    turn.turn_metadata_state
        .set_turn_trigger("user".to_string());
    turn.turn_metadata_state
        .set_turn_started_at_unix_ms(/*turn_started_at_unix_ms*/ 123_456);
    let (done, cancellation_token) = active_task_identity(&session).await;
    let mut target = session
        .capture_context_transition(&turn, &done, &cancellation_token)
        .await
        .expect("active task");
    let prepared = session
        .prepare_workspace_turn_context(&turn, Arc::clone(&target.settings))
        .await;
    let before_model = session.services.thread_extension_data.get::<ModelInfo>();
    assert_eq!(
        session
            .apply_turn_settings(
                &turn.sub_id,
                TurnSettingsUpdate {
                    summary: Some(ReasoningSummary::Detailed),
                    ..Default::default()
                }
            )
            .await,
        TurnSettingsUpdateOutcome::Applied,
    );
    let winning = turn.current_settings.load_full();
    target.settings = match session
        .publish_context_transition(&target, prepared)
        .await
    {
        Err(ContextTransitionError::SettingsChanged(settings)) => settings,
        other => panic!("stale preparation must retry: {other:?}"),
    };
    assert!(Arc::ptr_eq(&target.settings, &winning));
    assert!(Arc::ptr_eq(
        &session.active_task_context(&target.done).await.unwrap(),
        &turn
    ));
    assert_eq!(
        session.services.thread_extension_data.get::<ModelInfo>(),
        before_model
    );

    let prepared = session
        .prepare_workspace_turn_context(&turn, Arc::clone(&target.settings))
        .await;
    // Steering may still address the registered original context while preparation is pending.
    turn.turn_metadata_state
        .set_responsesapi_client_metadata(HashMap::from([(
            "late-steering".to_string(),
            "retained".to_string(),
        )]));
    turn.turn_metadata_state.mark_root_turn_ambiguous();
    turn.turn_metadata_state
        .mark_user_input_requested_during_turn();
    let published = session
        .publish_context_transition(&target, prepared)
        .await
        .expect("publish with winning settings");
    let registered = session
        .active_task_context(&target.done)
        .await
        .expect("same task");
    assert!(Arc::ptr_eq(&registered, &published));
    assert!(!Arc::ptr_eq(
        &published.turn_metadata_state,
        &turn.turn_metadata_state
    ));
    assert!(Arc::ptr_eq(
        &published.current_settings.load_full(),
        &winning
    ));
    assert_eq!(
        published
            .turn_metadata_state
            .to_responses_metadata(
                "installation".to_string(),
                "window".to_string(),
                CodexResponsesRequestKind::Turn,
            )
            .turn_metadata_value(),
        turn.turn_metadata_state
            .to_responses_metadata(
                "installation".to_string(),
                "window".to_string(),
                CodexResponsesRequestKind::Turn,
            )
            .turn_metadata_value(),
    );
    let [original_mcp, refreshed_mcp] = [&turn, &published].map(|context| {
        context
            .turn_metadata_state
            .current_meta_value_for_mcp_request(McpTurnMetadataContext {
                model: &winning.model_info.slug,
                reasoning_effort: winning.reasoning_effort().cloned(),
                node_repl_disabled: winning.model_info.node_repl_disabled,
            })
    });
    assert_eq!(refreshed_mcp, original_mcp);
    let step = session
        .capture_step_context(published, &CancellationToken::new())
        .await
        .expect("capture published context");
    assert!(Arc::ptr_eq(&step.settings, &winning));
    session.abort_all_tasks(TurnAbortReason::Replaced).await;
}

#[tokio::test]
async fn prepared_context_and_old_completion_cannot_retarget_reused_context() {
    let ActivationFixture {
        session,
        turn,
        finish,
        lookup: _,
    } = activation_fixture(activation_models()).await;
    let (done, cancellation_token) = active_task_identity(&session).await;
    let target = session
        .capture_context_transition(&turn, &done, &cancellation_token)
        .await
        .expect("active task");
    let prepared = session
        .prepare_workspace_turn_context(&turn, Arc::clone(&target.settings))
        .await;
    let completed = target.done.notified();
    finish.notify_one();
    timeout(Duration::from_secs(/*secs*/ 10), completed)
        .await
        .expect("old task completed");
    session
        .spawn_task(
            Arc::clone(&turn),
            Vec::new(),
            HeldStepTask {
                kind: TaskKind::Compact,
                finish: Arc::new(Notify::new()),
            },
        )
        .await;
    let (replacement_done, replacement_cancel) = active_task_identity(&session).await;
    let replacement = session
        .capture_context_transition(&turn, &replacement_done, &replacement_cancel)
        .await
        .expect("replacement task");
    assert!(!Arc::ptr_eq(&replacement.done, &target.done));
    let before_model = session.services.thread_extension_data.get::<ModelInfo>();
    // Even a first capture after replacement must reject the old invocation.
    assert!(
        session
            .capture_context_transition(&turn, &target.done, &replacement_cancel)
            .await
            .is_none()
    );
    assert!(matches!(
        session.publish_context_transition(&target, prepared).await,
        Err(ContextTransitionError::Unavailable),
    ));
    session.on_task_finished(&target.done, Ok(None)).await;
    assert!(session.active_task_context(&target.done).await.is_none());
    assert!(Arc::ptr_eq(
        &session
            .active_task_context(&replacement.done)
            .await
            .expect("replacement remains active"),
        &turn,
    ));
    assert_eq!(
        session.services.thread_extension_data.get::<ModelInfo>(),
        before_model
    );
    session.abort_all_tasks(TurnAbortReason::Replaced).await;
}
