use codex_history::CompactedItem;
use codex_history::ResponseItemEnvelope;
use codex_history::RolloutItem;
use codex_protocol::config_types::ApprovalsReviewer;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ModeKind;
use codex_protocol::config_types::Settings;
use codex_protocol::config_types::WindowsSandboxLevel;
use codex_protocol::models::ContentItem;
use codex_protocol::models::PermissionProfile;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SegmentPreviousTurnSettings;
use codex_protocol::protocol::SegmentStateCheckpointDisposition;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::ThreadSettingsSnapshot;
use codex_protocol::protocol::TokenCountEvent;
use codex_protocol::protocol::TurnEnvironmentSelections;
use codex_protocol::protocol::WorldStateItem;
use codex_utils_absolute_path::AbsolutePathBuf;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::CertifiedSegmentStateCheckpoint;
use super::validate_certified_segment_state_checkpoint;
use super::validated_segment_state_checkpoint;

fn compacted() -> CompactedItem {
    CompactedItem {
        message: "checkpoint".to_string(),
        replacement_history: Some(vec![ResponseItemEnvelope::new(ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![ContentItem::InputText {
                text: "replacement".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        })]),
        mcp_resource_origins: None,
        window_number: Some(3),
        first_window_id: Some("019b3f6e-0000-7000-8000-000000000001".to_string()),
        previous_window_id: Some("019b3f6e-0000-7000-8000-000000000002".to_string()),
        window_id: Some("019b3f6e-0000-7000-8000-000000000003".to_string()),
        segment_state_checkpoint: None,
    }
}

fn thread_settings() -> ThreadSettingsAppliedEvent {
    let cwd: AbsolutePathBuf = serde_json::from_value(json!("/tmp")).expect("absolute test cwd");
    ThreadSettingsAppliedEvent {
        thread_settings: ThreadSettingsSnapshot {
            model: "gpt-test".to_string(),
            model_provider_id: "test-provider".to_string(),
            service_tier: None,
            approval_policy: AskForApproval::Never,
            approvals_reviewer: ApprovalsReviewer::User,
            permission_profile: PermissionProfile::workspace_write(),
            active_permission_profile: None,
            cwd: cwd.clone(),
            environments: Some(TurnEnvironmentSelections::new(cwd, Vec::new())),
            workspace_roots: Some(Vec::new()),
            profile_workspace_roots: Some(Vec::new()),
            windows_sandbox_level: Some(WindowsSandboxLevel::Disabled),
            reasoning_effort: None,
            reasoning_summary: None,
            personality: None,
            collaboration_mode: CollaborationMode {
                mode: ModeKind::Default,
                settings: Settings {
                    model: "gpt-test".to_string(),
                    reasoning_effort: None,
                    developer_instructions: None,
                },
            },
        },
    }
}

fn token_count() -> TokenCountEvent {
    TokenCountEvent {
        info: None,
        rate_limits: None,
    }
}

#[test]
fn cleared_checkpoint_has_canonical_order_and_descriptor() {
    let checkpoint = CertifiedSegmentStateCheckpoint::new(
        compacted(),
        Some(SegmentPreviousTurnSettings {
            model: "gpt-test".to_string(),
            comp_hash: Some("hash".to_string()),
            realtime_active: Some(false),
        }),
        /*world_state*/ None,
        /*reference_context*/ None,
        thread_settings(),
        token_count(),
    )
    .expect("valid checkpoint");

    let [
        RolloutItem::Compacted(compacted),
        RolloutItem::EventMsg(_),
        RolloutItem::EventMsg(_),
    ] = checkpoint.items()
    else {
        panic!("cleared checkpoint should contain compaction and current-state records");
    };
    let descriptor = compacted
        .segment_state_checkpoint
        .as_ref()
        .expect("checkpoint descriptor");
    assert_eq!(
        (
            descriptor.world_state,
            descriptor.reference_context,
            descriptor.previous_turn_settings.clone(),
        ),
        (
            SegmentStateCheckpointDisposition::Cleared,
            SegmentStateCheckpointDisposition::Cleared,
            Some(SegmentPreviousTurnSettings {
                model: "gpt-test".to_string(),
                comp_hash: Some("hash".to_string()),
                realtime_active: Some(false),
            }),
        )
    );
    checkpoint.validate().expect("checkpoint remains valid");
}

#[test]
fn full_world_state_must_be_adjacent_to_checkpoint() {
    let checkpoint = CertifiedSegmentStateCheckpoint::new(
        compacted(),
        /*previous_turn_settings*/ None,
        Some(WorldStateItem::full(
            serde_json::from_value(json!({"environment": {"cwd": "/tmp"}}))
                .expect("world-state object"),
        )),
        /*reference_context*/ None,
        thread_settings(),
        token_count(),
    )
    .expect("valid checkpoint");
    let mut items = checkpoint.into_items();
    let RolloutItem::Compacted(compacted) = &items[0] else {
        panic!("checkpoint compaction");
    };
    assert!(validated_segment_state_checkpoint(compacted, &items[1..]).is_some());

    items.remove(1);
    let RolloutItem::Compacted(compacted) = &items[0] else {
        panic!("checkpoint compaction");
    };
    assert!(validated_segment_state_checkpoint(compacted, &items[1..]).is_none());
    assert!(validate_certified_segment_state_checkpoint(&items).is_err());
}

#[test]
fn checkpoint_requires_complete_thread_settings_and_token_count() {
    let checkpoint = CertifiedSegmentStateCheckpoint::new(
        compacted(),
        /*previous_turn_settings*/ None,
        /*world_state*/ None,
        /*reference_context*/ None,
        thread_settings(),
        token_count(),
    )
    .expect("valid checkpoint")
    .into_items();

    for missing_index in [1, 2] {
        let mut incomplete = checkpoint.clone();
        incomplete.remove(missing_index);
        assert!(validate_certified_segment_state_checkpoint(&incomplete).is_err());
    }

    for clear_field in [
        |settings: &mut ThreadSettingsSnapshot| settings.environments = None,
        |settings: &mut ThreadSettingsSnapshot| settings.workspace_roots = None,
        |settings: &mut ThreadSettingsSnapshot| settings.profile_workspace_roots = None,
        |settings: &mut ThreadSettingsSnapshot| settings.windows_sandbox_level = None,
    ] {
        let mut settings = thread_settings();
        clear_field(&mut settings.thread_settings);
        assert!(
            CertifiedSegmentStateCheckpoint::new(
                compacted(),
                /*previous_turn_settings*/ None,
                /*world_state*/ None,
                /*reference_context*/ None,
                settings,
                token_count(),
            )
            .is_err()
        );
    }
}

#[test]
fn malformed_or_unsupported_checkpoint_is_rejected() {
    let checkpoint = CertifiedSegmentStateCheckpoint::new(
        compacted(),
        /*previous_turn_settings*/ None,
        /*world_state*/ None,
        /*reference_context*/ None,
        thread_settings(),
        token_count(),
    )
    .expect("valid checkpoint");

    let mut unsupported = checkpoint.clone().into_items();
    let RolloutItem::Compacted(compacted) = &mut unsupported[0] else {
        panic!("checkpoint compaction");
    };
    compacted
        .segment_state_checkpoint
        .as_mut()
        .expect("checkpoint descriptor")
        .version = 999;
    assert!(validate_certified_segment_state_checkpoint(&unsupported).is_err());

    let mut invalid_window = checkpoint.into_items();
    let RolloutItem::Compacted(compacted) = &mut invalid_window[0] else {
        panic!("checkpoint compaction");
    };
    compacted.window_id = Some("550e8400-e29b-41d4-a716-446655440000".to_string());
    assert!(validate_certified_segment_state_checkpoint(&invalid_window).is_err());
}

#[test]
fn constructor_rejects_an_existing_checkpoint_descriptor() {
    let checkpoint = CertifiedSegmentStateCheckpoint::new(
        compacted(),
        /*previous_turn_settings*/ None,
        /*world_state*/ None,
        /*reference_context*/ None,
        thread_settings(),
        token_count(),
    )
    .expect("valid checkpoint");
    let RolloutItem::Compacted(compacted) = checkpoint.items()[0].clone() else {
        panic!("checkpoint compaction");
    };

    let result = CertifiedSegmentStateCheckpoint::new(
        compacted,
        /*previous_turn_settings*/ None,
        /*world_state*/ None,
        /*reference_context*/ None,
        thread_settings(),
        token_count(),
    );
    assert!(result.is_err());
}

#[test]
fn unrelated_records_are_not_part_of_the_checkpoint_unit() {
    let checkpoint = CertifiedSegmentStateCheckpoint::new(
        compacted(),
        /*previous_turn_settings*/ None,
        /*world_state*/ None,
        /*reference_context*/ None,
        thread_settings(),
        token_count(),
    )
    .expect("valid checkpoint");
    let mut items = checkpoint.into_items();
    items.push(RolloutItem::EventMsg(EventMsg::ContextCompacted(
        codex_protocol::protocol::ContextCompactedEvent,
    )));

    assert!(validate_certified_segment_state_checkpoint(&items).is_err());
}
