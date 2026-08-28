use super::*;
use crate::responses_metadata::CodexResponsesRequestKind;
use crate::session::step_activation::tests::ActivationFixture;
use crate::session::step_activation::tests::activation_fixture;
use crate::session::step_activation::tests::activation_models;
use crate::turn_metadata::McpTurnMetadataContext;
use codex_protocol::config_types::ReasoningSummary;
use codex_protocol::openai_models::ModelInfo;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::protocol::TurnSettingsUpdate;
use codex_protocol::protocol::TurnSettingsUpdateOutcome;
use pretty_assertions::assert_eq;
use std::collections::HashMap;

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
    let (done, cancellation_token) = {
        let active = session.active_turn.lock().await;
        let task = active
            .as_ref()
            .and_then(|turn| turn.task.as_ref())
            .expect("active task");
        (Arc::clone(&task.done), task.cancellation_token.clone())
    };
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
    target.settings = match session.publish_context_transition(&target, prepared).await {
        Err(ContextTransitionError::SettingsChanged(settings)) => settings,
        other => panic!("stale preparation must retry: {other:?}"),
    };
    assert!(Arc::ptr_eq(&target.settings, &winning));
    {
        let active = session.active_turn.lock().await;
        let registered = &active
            .as_ref()
            .and_then(|turn| turn.task.as_ref())
            .expect("active task")
            .turn_context;
        assert!(Arc::ptr_eq(registered, &turn));
    }
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
    let registered = {
        let active = session.active_turn.lock().await;
        Arc::clone(
            &active
                .as_ref()
                .and_then(|turn| turn.task.as_ref())
                .expect("active task")
                .turn_context,
        )
    };
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
