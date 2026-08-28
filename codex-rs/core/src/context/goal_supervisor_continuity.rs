use chrono::DateTime;
use chrono::Utc;
use codex_protocol::models::ContentItemKind;
use codex_protocol::protocol::InterAgentCommunication;
use codex_utils_string::take_bytes_at_char_boundary;
use serde::Serialize;

use super::ContextualUserFragment;

const MAX_RENDERED_BYTES: usize = 4096;
const MAX_IDENTITY_BYTES: usize = 128;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SupervisorActionKind {
    CompactParentContext,
    FollowupTask,
    Snooze,
}

pub(crate) struct GoalSupervisorAction<'a> {
    pub(crate) kind: &'a SupervisorActionKind,
    pub(crate) sent_at: DateTime<Utc>,
    pub(crate) delivered_parent_message: Option<&'a InterAgentCommunication>,
    pub(crate) snoozed_seconds: Option<u64>,
}

/// A bounded preview of the previous supervisor action for the next helper.
///
/// The delivered communication remains unchanged in runtime state and history.
/// The byte cap includes the header and JSON escaping; it is not a token count.
pub(crate) struct GoalSupervisorContinuity<'a> {
    pub(crate) previous_supervisor_action: Option<GoalSupervisorAction<'a>>,
    pub(crate) goal_created_at: i64,
    pub(crate) now: DateTime<Utc>,
    pub(crate) snooze_count_since_goal_created: u64,
    pub(crate) snoozed_seconds_since_goal_created: u64,
    pub(crate) last_parent_message_at: Option<i64>,
    pub(crate) snooze_count_since_last_parent_message: u64,
    pub(crate) snoozed_seconds_since_last_parent_message: u64,
}

impl ContextualUserFragment for GoalSupervisorContinuity<'_> {
    fn content_kind(&self) -> ContentItemKind {
        ContentItemKind("goal_supervisor.continuity".to_string())
    }

    fn role(&self) -> &'static str {
        "developer"
    }

    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        ("# Goal Supervisor Continuity\n\n", "")
    }

    fn body(&self) -> String {
        let Self {
            previous_supervisor_action,
            goal_created_at,
            now,
            snooze_count_since_goal_created,
            snoozed_seconds_since_goal_created,
            last_parent_message_at,
            snooze_count_since_last_parent_message,
            snoozed_seconds_since_last_parent_message,
        } = self;
        let previous_action = previous_supervisor_action.as_ref().map(
            |GoalSupervisorAction {
                 kind,
                 sent_at,
                 delivered_parent_message,
                 snoozed_seconds,
             }| {
                let message = delivered_parent_message.map(
                    |InterAgentCommunication {
                         id: _,
                         author,
                         recipient,
                         other_recipients: _,
                         content: _,
                         encrypted_content,
                         internal_chat_message_metadata_passthrough: _,
                         trigger_turn,
                     }| {
                        let author_preview =
                            take_bytes_at_char_boundary(author.as_str(), MAX_IDENTITY_BYTES);
                        let recipient_preview =
                            take_bytes_at_char_boundary(recipient.as_str(), MAX_IDENTITY_BYTES);
                        serde_json::json!({
                            "author": author_preview,
                            "author_truncated": author_preview.len() < author.len(),
                            "recipient": recipient_preview,
                            "recipient_truncated": recipient_preview.len() < recipient.len(),
                            "trigger_turn": trigger_turn,
                            "encrypted": encrypted_content.is_some(),
                            "content_preview": encrypted_content.is_none().then_some(""),
                            "content_truncated": false,
                        })
                    },
                );
                serde_json::json!({
                    "kind": kind,
                    "sent_at_utc": sent_at.to_rfc3339(),
                    "delivered_parent_message": message,
                    "snoozed_seconds": snoozed_seconds,
                })
            },
        );
        let mut continuity = serde_json::json!({
            "supervisor_identity": "/root/goal_supervisor",
            "activation_reason": "thread_idle",
            "previous_supervisor_action": previous_action,
            "goal_timing": {
                "goal_created_at_utc": DateTime::<Utc>::from_timestamp(*goal_created_at, /*nsecs*/ 0).map(|created_at| created_at.to_rfc3339()),
                "seconds_since_goal_created": now.timestamp().saturating_sub(*goal_created_at),
                "snooze_count_since_goal_created": snooze_count_since_goal_created,
                "snoozed_seconds_since_goal_created": snoozed_seconds_since_goal_created,
            },
            "parent_timing": {
                "last_parent_message_at_utc": last_parent_message_at.and_then(|completed_at| DateTime::<Utc>::from_timestamp(completed_at, /*nsecs*/ 0)).map(|completed_at| completed_at.to_rfc3339()),
                "snooze_count_since_last_parent_message": snooze_count_since_last_parent_message,
                "snoozed_seconds_since_last_parent_message": snoozed_seconds_since_last_parent_message,
            },
        });
        let (start, end) = Self::type_markers();
        let max_body_bytes = MAX_RENDERED_BYTES - start.len() - end.len();
        let body = if let Some(message) = previous_supervisor_action
            .as_ref()
            .and_then(|action| action.delivered_parent_message)
            .filter(|message| message.encrypted_content.is_none())
        {
            let mut render_preview = |preview: &str| {
                let delivered =
                    &mut continuity["previous_supervisor_action"]["delivered_parent_message"];
                delivered["content_preview"] = serde_json::json!(preview);
                delivered["content_truncated"] =
                    serde_json::json!(preview.len() < message.content.len());
                format!("{continuity:#}")
            };
            // Search only a bounded prefix, measuring the complete escaped JSON
            // with its final flags instead of treating source bytes as tokens.
            let mut lower = 0;
            let mut upper = message.content.len().min(max_body_bytes);
            while lower < upper {
                let middle = lower + (upper - lower).div_ceil(/*rhs*/ 2);
                let preview = take_bytes_at_char_boundary(&message.content, middle);
                if render_preview(preview).len() <= max_body_bytes {
                    lower = middle;
                } else {
                    upper = middle - 1;
                }
            }
            render_preview(take_bytes_at_char_boundary(&message.content, lower))
        } else {
            format!("{continuity:#}")
        };
        // Fixed facts and bounded identities fit today. Keep the final boundary
        // fail-closed if a future field increases the fixed representation.
        if body.len() > max_body_bytes {
            return r#"{"continuity_omitted":"rendered_size_limit"}"#.to_string();
        }
        body
    }
}

#[cfg(test)]
#[path = "goal_supervisor_continuity_tests.rs"]
mod tests;
