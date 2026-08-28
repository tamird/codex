use super::GoalSupervisorAction;
use super::GoalSupervisorContinuity;
use super::MAX_RENDERED_BYTES;
use super::SupervisorActionKind;
use crate::context::ContextualUserFragment;
use chrono::DateTime;
use chrono::Utc;
use codex_context_fragments::AnnotatedContent;
use codex_context_fragments::RenderedFragment;
use codex_protocol::AgentPath;
use codex_protocol::models::ContentItemKind;
use codex_protocol::protocol::InterAgentCommunication;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;

fn communication(content: &str) -> InterAgentCommunication {
    let mut message = InterAgentCommunication::new(
        AgentPath::try_from("/root/goal_supervisor").expect("supervisor path"),
        AgentPath::root(),
        vec![AgentPath::try_from("/root/private_recipient").expect("other recipient path")],
        content.to_string(),
        /*trigger_turn*/ true,
    );
    message.id = Some(codex_protocol::ResponseItemId::new("private-message-id"));
    message.set_turn_id_if_missing("private-message-metadata");
    message
}

fn continuity(message: &InterAgentCommunication) -> GoalSupervisorContinuity<'_> {
    GoalSupervisorContinuity {
        previous_supervisor_action: Some(GoalSupervisorAction {
            kind: &SupervisorActionKind::FollowupTask,
            sent_at: DateTime::from_timestamp(/*secs*/ 115, /*nsecs*/ 0).expect("action time"),
            delivered_parent_message: Some(message),
            snoozed_seconds: None,
        }),
        goal_created_at: 100,
        now: DateTime::from_timestamp(/*secs*/ 120, /*nsecs*/ 0).expect("current time"),
        snooze_count_since_goal_created: 2,
        snoozed_seconds_since_goal_created: 30,
        last_parent_message_at: Some(110),
        snooze_count_since_last_parent_message: 1,
        snoozed_seconds_since_last_parent_message: 10,
    }
}

fn expected_continuity(message: Value) -> Value {
    json!({
        "supervisor_identity": "/root/goal_supervisor",
        "activation_reason": "thread_idle",
        "previous_supervisor_action": {
            "kind": "followup_task",
            "sent_at_utc": "1970-01-01T00:01:55+00:00",
            "delivered_parent_message": message,
            "snoozed_seconds": null,
        },
        "goal_timing": {
            "goal_created_at_utc": "1970-01-01T00:01:40+00:00",
            "seconds_since_goal_created": 20,
            "snooze_count_since_goal_created": 2,
            "snoozed_seconds_since_goal_created": 30,
        },
        "parent_timing": {
            "last_parent_message_at_utc": "1970-01-01T00:01:50+00:00",
            "snooze_count_since_last_parent_message": 1,
            "snoozed_seconds_since_last_parent_message": 10,
        },
    })
}

#[test]
fn plaintext_preview_preserves_facts_without_private_metadata() {
    let message = communication("continue \"carefully\"\n🦀");
    let expected = expected_continuity(json!({
        "author": "/root/goal_supervisor",
        "author_truncated": false,
        "recipient": "/root",
        "recipient_truncated": false,
        "trigger_turn": true,
        "encrypted": false,
        "content_preview": "continue \"carefully\"\n🦀",
        "content_truncated": false,
    }));
    let expected_body = serde_json::to_string_pretty(&expected).expect("expected continuity JSON");
    assert_eq!(
        continuity(&message).render_fragment(),
        RenderedFragment::new(
            "developer",
            AnnotatedContent::input_text(
                format!("# Goal Supervisor Continuity\n\n{expected_body}"),
                ContentItemKind("goal_supervisor.continuity".to_string()),
            ),
        )
    );
}

#[test]
fn encrypted_preview_excludes_ciphertext_and_accompanying_plaintext() {
    let mut message = communication("private-plaintext-sentinel");
    message.encrypted_content = Some("private-ciphertext-sentinel".to_string());
    let expected = expected_continuity(json!({
        "author": "/root/goal_supervisor",
        "author_truncated": false,
        "recipient": "/root",
        "recipient_truncated": false,
        "trigger_turn": true,
        "encrypted": true,
        "content_preview": null,
        "content_truncated": false,
    }));
    let rendered = continuity(&message).render();
    let body = rendered
        .strip_prefix("# Goal Supervisor Continuity\n\n")
        .expect("continuity header");
    assert_eq!(
        serde_json::from_str::<Value>(body).expect("continuity JSON"),
        expected
    );
}

#[test]
fn rendered_budget_counts_escaping_utf8_and_identity_previews() {
    for pattern in ["a", "\"\\\n\u{0001}", "é🦀", "é\"\n🦀\u{0001}"] {
        let mut message = communication(&pattern.repeat(/*n*/ 8192));
        message.author = AgentPath::root()
            .join(&"a".repeat(/*n*/ 8192))
            .expect("long author");
        message.recipient = AgentPath::root()
            .join(&"b".repeat(/*n*/ 8192))
            .expect("long recipient");
        let mut fragment = continuity(&message);
        fragment.goal_created_at = DateTime::<Utc>::MIN_UTC.timestamp();
        fragment.now = DateTime::<Utc>::MAX_UTC;
        fragment.last_parent_message_at = Some(DateTime::<Utc>::MAX_UTC.timestamp());
        fragment.snooze_count_since_goal_created = u64::MAX;
        fragment.snoozed_seconds_since_goal_created = u64::MAX;
        fragment.snooze_count_since_last_parent_message = u64::MAX;
        fragment.snoozed_seconds_since_last_parent_message = u64::MAX;
        let action = fragment
            .previous_supervisor_action
            .as_mut()
            .expect("previous action");
        action.sent_at = DateTime::<Utc>::MAX_UTC;
        action.snoozed_seconds = Some(u64::MAX);

        let rendered = fragment.render();
        assert!(rendered.len() <= MAX_RENDERED_BYTES, "{pattern:?}");
        let (header, _) = GoalSupervisorContinuity::type_markers();
        let mut parsed: Value =
            serde_json::from_str(rendered.strip_prefix(header).expect("continuity header"))
                .expect("bounded continuity must remain valid JSON");
        let delivered = &parsed["previous_supervisor_action"]["delivered_parent_message"];
        let preview = delivered["content_preview"]
            .as_str()
            .expect("plaintext preview");
        assert!(!preview.is_empty(), "{pattern:?}");
        assert!(message.content.starts_with(preview), "{pattern:?}");
        assert_eq!(
            delivered,
            &json!({
                "author": format!("/root/{}", "a".repeat(/*n*/ 122)),
                "author_truncated": true,
                "recipient": format!("/root/{}", "b".repeat(/*n*/ 122)),
                "recipient_truncated": true,
                "trigger_turn": true,
                "encrypted": false,
                "content_preview": preview,
                "content_truncated": true,
            })
        );
        assert_eq!(
            parsed["goal_timing"],
            json!({
                "goal_created_at_utc": "-262143-01-01T00:00:00+00:00",
                "seconds_since_goal_created": DateTime::<Utc>::MAX_UTC.timestamp() - DateTime::<Utc>::MIN_UTC.timestamp(),
                "snooze_count_since_goal_created": u64::MAX,
                "snoozed_seconds_since_goal_created": u64::MAX,
            })
        );
        assert_eq!(
            parsed["parent_timing"],
            json!({
                "last_parent_message_at_utc": "+262142-12-31T23:59:59+00:00",
                "snooze_count_since_last_parent_message": u64::MAX,
                "snoozed_seconds_since_last_parent_message": u64::MAX,
            })
        );
        // The next full character must exceed the cap: JSON escaping and the
        // final truncation flag are part of the budget, not post-processing.
        let next = message.content[preview.len()..]
            .chars()
            .next()
            .expect("omitted character");
        let larger_preview = format!("{preview}{next}");
        parsed["previous_supervisor_action"]["delivered_parent_message"]["content_preview"] =
            json!(larger_preview);
        let larger = serde_json::to_string_pretty(&parsed).expect("larger continuity JSON");
        assert!(
            header.len() + larger.len() > MAX_RENDERED_BYTES,
            "{pattern:?}"
        );
    }
}
