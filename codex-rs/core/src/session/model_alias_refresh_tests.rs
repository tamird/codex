use super::*;
use codex_models_manager::ModelRoutingProfile;
use codex_protocol::openai_models::ReasoningEffort;
use pretty_assertions::assert_eq;

fn routed_alias(candidates: Vec<ModelRoutingCandidate>) -> CustomModelConfig {
    CustomModelConfig {
        model: candidates[0].model.clone(),
        routing_profile: Some(ModelRoutingProfile { candidates }),
        model_context_window: None,
        model_auto_compact_token_limit: None,
    }
}

fn candidate(model: &str, effort: ReasoningEffort) -> ModelRoutingCandidate {
    ModelRoutingCandidate {
        model: model.to_string(),
        reasoning_effort: Some(effort),
        service_tier: None,
    }
}

#[test]
fn unique_identical_replacement_is_a_rename() {
    let profile = routed_alias(vec![candidate("test-primary", ReasoningEffort::High)]);
    let previous = HashMap::from([("old-name".to_string(), profile.clone())]);
    let next = HashMap::from([("new-name".to_string(), profile)]);

    assert_eq!(
        selected_alias_update("old-name", &previous, &next, /*last_success*/ None),
        SelectedAliasUpdate::Renamed {
            alias: "new-name".to_string()
        }
    );
}

#[test]
fn ambiguous_replacements_detach_to_last_success() {
    let primary = candidate("test-primary", ReasoningEffort::High);
    let fallback = candidate("test-fallback", ReasoningEffort::Medium);
    let profile = routed_alias(vec![primary, fallback.clone()]);
    let previous = HashMap::from([("old-name".to_string(), profile.clone())]);
    let next = HashMap::from([
        ("new-name-a".to_string(), profile.clone()),
        ("new-name-b".to_string(), profile),
    ]);

    assert_eq!(
        selected_alias_update("old-name", &previous, &next, Some(&fallback)),
        SelectedAliasUpdate::DetachedProfile {
            candidate: fallback
        }
    );
}

#[test]
fn removal_without_success_detaches_to_first_candidate() {
    let primary = candidate("test-primary", ReasoningEffort::High);
    let profile = routed_alias(vec![
        primary.clone(),
        candidate("test-fallback", ReasoningEffort::Medium),
    ]);
    let previous = HashMap::from([("old-name".to_string(), profile)]);

    assert_eq!(
        selected_alias_update(
            "old-name",
            &previous,
            &HashMap::new(),
            /*last_success*/ None
        ),
        SelectedAliasUpdate::DetachedProfile { candidate: primary }
    );
}

#[test]
fn direct_alias_removal_detaches_only_the_model() {
    let previous = HashMap::from([(
        "old-name".to_string(),
        CustomModelConfig {
            model: "test-model".to_string(),
            routing_profile: None,
            model_context_window: None,
            model_auto_compact_token_limit: None,
        },
    )]);

    assert_eq!(
        selected_alias_update(
            "old-name",
            &previous,
            &HashMap::new(),
            /*last_success*/ None
        ),
        SelectedAliasUpdate::DetachedAlias {
            model: "test-model".to_string()
        }
    );
}

#[test]
fn materialized_addition_survives_a_layer_reload() {
    let profile = routed_alias(vec![candidate("test-primary", ReasoningEffort::High)]);
    let previous_materialized = HashMap::from([("runtime".to_string(), profile.clone())]);
    let mut next_layer = HashMap::from([("user".to_string(), profile.clone())]);

    preserve_materialized_custom_model_overrides(
        &HashMap::new(),
        &previous_materialized,
        &mut next_layer,
    );

    assert_eq!(
        next_layer,
        HashMap::from([
            ("runtime".to_string(), profile.clone()),
            ("user".to_string(), profile),
        ])
    );
}

#[test]
fn unchanged_layer_alias_accepts_the_reloaded_value() {
    let previous = routed_alias(vec![candidate("old-primary", ReasoningEffort::High)]);
    let next = routed_alias(vec![candidate("new-primary", ReasoningEffort::Medium)]);
    let previous_layer = HashMap::from([("user".to_string(), previous.clone())]);
    let previous_materialized = HashMap::from([("user".to_string(), previous)]);
    let mut next_layer = HashMap::from([("user".to_string(), next.clone())]);

    preserve_materialized_custom_model_overrides(
        &previous_layer,
        &previous_materialized,
        &mut next_layer,
    );

    assert_eq!(next_layer, HashMap::from([("user".to_string(), next)]));
}

#[test]
fn materialized_removal_survives_a_layer_reload() {
    let previous = routed_alias(vec![candidate("old-primary", ReasoningEffort::High)]);
    let previous_layer = HashMap::from([("user".to_string(), previous)]);
    let mut next_layer = HashMap::from([(
        "user".to_string(),
        routed_alias(vec![candidate("new-primary", ReasoningEffort::Medium)]),
    )]);

    preserve_materialized_custom_model_overrides(&previous_layer, &HashMap::new(), &mut next_layer);

    assert!(next_layer.is_empty());
}
