use std::collections::HashSet;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE;
use codex_protocol::ThreadId;
use codex_protocol::models::AgentMessageInputContent;
use codex_protocol::models::InternalChatMessageMetadataPassthrough;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use pretty_assertions::assert_eq;

use super::AFFECTED_CLI_VERSION;
use super::GoalSupervisorLineageProvenance;
use super::SELECTED_MISSING_MESSAGE_ID;
use super::looks_like_fernet_v0;
use super::looks_like_printable_natural_language;
use super::reject_malformed_goal_supervisor_supplied_history;
use super::repair_legacy_goal_supervisor_jsonl_lines_selected_with_provenance;
use super::repair_legacy_goal_supervisor_jsonl_lines_with_provenance;
use super::repair_legacy_goal_supervisor_lines;
use super::repair_legacy_goal_supervisor_lines_selected_with_provenance;
use super::repair_legacy_goal_supervisor_lines_with_provenance;
use super::rewrite_legacy_goal_supervisor_jsonl_same_length;
use super::rewrite_rollout_jsonl_same_length;

const THREAD_ID: &str = "01900000-0000-7000-8000-000000000001";

fn session_meta(version: &str) -> RolloutLine {
    let thread_id = ThreadId::from_string(THREAD_ID).expect("synthetic thread ID");
    let mut meta = SessionMeta {
        id: thread_id,
        session_id: thread_id.into(),
        ..SessionMeta::default()
    };
    meta.cli_version = version.to_string();
    meta.model_provider = Some("openai".to_string());
    RolloutLine {
        timestamp: "2026-01-01T00:00:00Z".to_string(),
        ordinal: Some(0),
        item: RolloutItem::SessionMeta(SessionMetaLine { meta, git: None }),
    }
}

fn affected_provenance() -> GoalSupervisorLineageProvenance {
    GoalSupervisorLineageProvenance::AffectedAlpha6 {
        thread_id: ThreadId::from_string(THREAD_ID).expect("synthetic thread ID"),
    }
}

fn delivery(ordinal: u64, trigger_turn: bool) -> RolloutLine {
    RolloutLine {
        timestamp: format!("2026-01-01T00:00:{ordinal:02}Z"),
        ordinal: Some(ordinal),
        item: RolloutItem::InterAgentCommunicationMetadata { trigger_turn },
    }
}

fn poisoned_response_item_with_id(payload: &str, message_id: &str) -> ResponseItem {
    ResponseItem::AgentMessage {
        id: Some(codex_protocol::ResponseItemId::from_server(
            message_id.to_string(),
        )),
        author: "/root/goal_supervisor".to_string(),
        recipient: "/root".to_string(),
        content: vec![
            AgentMessageInputContent::InputText {
                text: "Message Type: NEW_TASK\nTask name: /root\nSender: /root/goal_supervisor\nPayload:\n"
                    .to_string(),
            },
            AgentMessageInputContent::EncryptedContent {
                encrypted_content: payload.to_string(),
            },
        ],
        internal_chat_message_metadata_passthrough: Some(
            InternalChatMessageMetadataPassthrough {
                turn_id: Some("01900000-0000-7000-8000-000000000003".to_string()),
                ..Default::default()
            },
        ),
    }
}

fn poisoned_response_item_without_id(payload: &str) -> ResponseItem {
    let mut item = poisoned_response_item_with_id(payload, "unused");
    let ResponseItem::AgentMessage { id, .. } = &mut item else {
        unreachable!();
    };
    *id = None;
    item
}

fn poisoned_message(ordinal: u64, payload: &str) -> RolloutLine {
    poisoned_message_with_id(
        ordinal,
        payload,
        "amsg_01900000-0000-7000-8000-000000000002",
    )
}

fn poisoned_message_with_id(ordinal: u64, payload: &str, message_id: &str) -> RolloutLine {
    RolloutLine {
        timestamp: format!("2026-01-01T00:00:{ordinal:02}Z"),
        ordinal: Some(ordinal),
        item: RolloutItem::ResponseItem(poisoned_response_item_with_id(payload, message_id).into()),
    }
}

fn compacted_message(ordinal: u64, payload: &str) -> RolloutLine {
    compacted_message_with_id(
        ordinal,
        payload,
        "amsg_01900000-0000-7000-8000-000000000002",
    )
}

fn compacted_message_with_id(ordinal: u64, payload: &str, message_id: &str) -> RolloutLine {
    serde_json::from_value(serde_json::json!({
        "timestamp": format!("2026-01-01T00:00:{ordinal:02}Z"),
        "ordinal": ordinal,
        "type": "compacted",
        "payload": {
            "message": "synthetic summary",
            "replacement_history": [
                serde_json::to_value(poisoned_response_item_with_id(payload, message_id))
                    .expect("serialize item")
            ]
        }
    }))
    .expect("synthetic compaction")
}

fn selected(message_ids: &[&str]) -> HashSet<String> {
    message_ids.iter().map(|id| (*id).to_string()).collect()
}

fn encode_jsonl(lines: &[RolloutLine]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for line in lines {
        bytes.extend(serde_json::to_vec(line).expect("serialize synthetic rollout"));
        bytes.push(b'\n');
    }
    bytes
}

fn response_item(line: &RolloutLine) -> &ResponseItem {
    let RolloutItem::ResponseItem(item) = &line.item else {
        panic!("expected response item");
    };
    item
}

fn genuine_synthetic_fernet_token() -> &'static str {
    "gAAAAABqfQkApRY563QcNHss2A4AKN3hJ027UTP8TRjRMwBjGzwdQ1xZ-6mLXPG8wa8TVLFB3ggULSAKlfpl7C4YbWdR4_R28-k0urBPtKk2Amtf8DdoShe_vVF4ffJ0XoIvR1ryVWmP"
}

fn forged_invalid_hmac_fernet_token() -> String {
    let mut decoded = URL_SAFE
        .decode(genuine_synthetic_fernet_token())
        .expect("decode synthetic Fernet token");
    *decoded.last_mut().expect("HMAC byte") ^= 1;
    URL_SAFE.encode(decoded)
}

#[test]
fn repairs_exact_top_level_delivery() {
    let mut lines = vec![
        session_meta(AFFECTED_CLI_VERSION),
        delivery(/*ordinal*/ 1, /*trigger_turn*/ true),
        poisoned_message(/*ordinal*/ 2, "synthetic supervisor instruction"),
    ];

    let count = repair_legacy_goal_supervisor_lines(&mut lines).expect("repair lines");

    assert_eq!(count.top_level, 1);
    let ResponseItem::AgentMessage { content, .. } = response_item(&lines[2]) else {
        panic!("expected agent message");
    };
    assert!(matches!(
        content.as_slice(),
        [AgentMessageInputContent::InputText { text }]
            if text.ends_with("synthetic supervisor instruction")
    ));
}

#[test]
fn same_rollout_repairs_plaintext_and_preserves_fernet() {
    let lines = vec![
        session_meta(AFFECTED_CLI_VERSION),
        delivery(/*ordinal*/ 1, /*trigger_turn*/ true),
        poisoned_message(/*ordinal*/ 2, genuine_synthetic_fernet_token()),
        delivery(/*ordinal*/ 3, /*trigger_turn*/ true),
        poisoned_message(
            /*ordinal*/ 4,
            "synthetic poisoned supervisor instruction",
        ),
    ];
    let source = encode_jsonl(&lines);
    let before = source
        .split_inclusive(|byte| *byte == b'\n')
        .collect::<Vec<_>>();

    let rewritten = rewrite_legacy_goal_supervisor_jsonl_same_length(&source)
        .expect("classify mixed history")
        .expect("repair plaintext poison");
    let after = rewritten
        .split_inclusive(|byte| *byte == b'\n')
        .collect::<Vec<_>>();

    assert_eq!(after[2], before[2]);
    assert_ne!(after[4], before[4]);
    assert_eq!(rewritten.len(), source.len());
    assert!(
        rewrite_legacy_goal_supervisor_jsonl_same_length(&rewritten)
            .expect("idempotent scan")
            .is_none()
    );
}

#[test]
fn forged_invalid_hmac_fernet_is_protected() {
    let forged = forged_invalid_hmac_fernet_token();
    assert!(looks_like_fernet_v0(&forged));
    let source = encode_jsonl(&[
        session_meta(AFFECTED_CLI_VERSION),
        delivery(/*ordinal*/ 1, /*trigger_turn*/ true),
        poisoned_message(/*ordinal*/ 2, &forged),
    ]);

    assert!(
        rewrite_legacy_goal_supervisor_jsonl_same_length(&source)
            .expect("scan forged Fernet frame")
            .is_none()
    );
}

#[test]
fn natural_language_rule_is_bounded() {
    assert!(looks_like_printable_natural_language(
        "synthetic instruction"
    ));
    assert!(looks_like_printable_natural_language(
        "synthetic\nUnicode instruction \u{2603}"
    ));
    assert!(!looks_like_printable_natural_language("one-word"));
    assert!(!looks_like_printable_natural_language("   \n\t"));
    assert!(!looks_like_printable_natural_language(
        "synthetic \0instruction"
    ));
}

#[test]
fn unknown_non_fernet_payload_fails_closed() {
    let lines = vec![
        session_meta(AFFECTED_CLI_VERSION),
        delivery(/*ordinal*/ 1, /*trigger_turn*/ true),
        poisoned_message(
            /*ordinal*/ 2,
            "synthetic-opaque-format-v1:0123456789abcdef",
        ),
    ];
    let source = encode_jsonl(&lines);

    let error = rewrite_legacy_goal_supervisor_jsonl_same_length(&source)
        .expect_err("unknown payload must fail closed");

    assert!(error.to_string().contains("ambiguous goal-supervisor"));
    assert!(!error.to_string().contains("synthetic-opaque-format"));
    assert_eq!(source, encode_jsonl(&lines));
}

#[test]
fn repairs_nested_compaction_without_new_provenance_field() {
    let compacted = compacted_message(/*ordinal*/ 1, "synthetic compacted instruction");
    let serialized = serde_json::to_value(&compacted).expect("serialize compaction");
    assert!(
        serialized["payload"]
            .get("opaque_encrypted_agent_message_ids")
            .is_none()
    );
    let mut lines = vec![session_meta(AFFECTED_CLI_VERSION), compacted];

    let count = repair_legacy_goal_supervisor_lines(&mut lines).expect("repair compaction");

    assert_eq!(count.compacted, 1);
    let RolloutItem::Compacted(compacted) = &lines[1].item else {
        panic!("expected compaction");
    };
    assert!(matches!(
        compacted.replacement_history.as_deref(),
        Some([item]) if matches!(
            &**item,
            ResponseItem::AgentMessage { content, .. }
                if matches!(content.as_slice(), [AgentMessageInputContent::InputText { text }]
                    if text.ends_with("synthetic compacted instruction"))
        )
    ));
}

#[test]
fn selection_repairs_only_consumed_top_level_message() {
    let selected_id = "amsg_01900000-0000-7000-8000-000000000011";
    let excluded_id = "amsg_01900000-0000-7000-8000-000000000012";
    let mut lines = vec![
        session_meta(AFFECTED_CLI_VERSION),
        delivery(/*ordinal*/ 1, /*trigger_turn*/ true),
        poisoned_message_with_id(
            /*ordinal*/ 2,
            "selected supervisor instruction",
            selected_id,
        ),
        delivery(/*ordinal*/ 3, /*trigger_turn*/ true),
        poisoned_message_with_id(/*ordinal*/ 4, "unknown-opaque-format", excluded_id),
    ];
    let excluded_before = serde_json::to_value(&lines[4]).expect("serialize excluded line");

    let count = repair_legacy_goal_supervisor_lines_selected_with_provenance(
        lines.as_mut_slice(),
        GoalSupervisorLineageProvenance::Untrusted,
        &selected(&[selected_id]),
    )
    .expect("repair selected message");

    assert_eq!(count.top_level, 1);
    assert_eq!(
        serde_json::to_value(&lines[4]).expect("serialize excluded line"),
        excluded_before
    );
}

#[test]
fn selection_applies_inside_compacted_replacement_history() {
    let selected_id = "amsg_01900000-0000-7000-8000-000000000021";
    let excluded_id = "amsg_01900000-0000-7000-8000-000000000022";
    let compacted = serde_json::from_value(serde_json::json!({
        "timestamp": "2026-01-01T00:00:01Z",
        "ordinal": 1,
        "type": "compacted",
        "payload": {
            "message": "synthetic summary",
            "replacement_history": [
                serde_json::to_value(poisoned_response_item_with_id(
                    "selected compacted instruction",
                    selected_id,
                )).expect("serialize selected item"),
                serde_json::to_value(poisoned_response_item_with_id(
                    "unknown-opaque-format",
                    excluded_id,
                )).expect("serialize excluded item"),
            ]
        }
    }))
    .expect("synthetic compaction");
    let source = encode_jsonl(&[session_meta(AFFECTED_CLI_VERSION), compacted]);

    let (lines, count) = repair_legacy_goal_supervisor_jsonl_lines_selected_with_provenance(
        source.as_slice(),
        GoalSupervisorLineageProvenance::Untrusted,
        &selected(&[selected_id]),
    )
    .expect("repair selected compacted item");

    assert_eq!(count.compacted, 1);
    let RolloutItem::Compacted(compacted) = &lines[1].item else {
        panic!("expected compaction");
    };
    let history = compacted
        .replacement_history
        .as_deref()
        .expect("replacement history");
    assert!(matches!(
        &*history[0],
        ResponseItem::AgentMessage { content, .. }
            if matches!(content.as_slice(), [AgentMessageInputContent::InputText { .. }])
    ));
    assert!(matches!(
        &*history[1],
        ResponseItem::AgentMessage { content, .. }
            if matches!(content.as_slice(), [_, AgentMessageInputContent::EncryptedContent { .. }])
    ));
}

#[test]
fn selected_missing_message_id_fails_closed() {
    let mut lines = vec![
        session_meta(AFFECTED_CLI_VERSION),
        delivery(/*ordinal*/ 1, /*trigger_turn*/ true),
        RolloutLine {
            timestamp: "2026-01-01T00:00:02Z".to_string(),
            ordinal: Some(2),
            item: RolloutItem::ResponseItem(
                poisoned_response_item_without_id("synthetic supervisor instruction").into(),
            ),
        },
    ];
    let before = encode_jsonl(lines.as_slice());

    let error = repair_legacy_goal_supervisor_lines_selected_with_provenance(
        &mut lines,
        affected_provenance(),
        &selected(&[SELECTED_MISSING_MESSAGE_ID]),
    )
    .expect_err("a selected candidate without a durable message ID is ambiguous");

    assert!(error.to_string().contains("ambiguous goal-supervisor"));
    assert_eq!(encode_jsonl(lines.as_slice()), before);
}

#[test]
fn empty_selection_ignores_exact_and_ambiguous_candidates() {
    let mut lines = vec![
        session_meta(AFFECTED_CLI_VERSION),
        delivery(/*ordinal*/ 1, /*trigger_turn*/ true),
        poisoned_message(/*ordinal*/ 2, "synthetic supervisor instruction"),
        delivery(/*ordinal*/ 3, /*trigger_turn*/ true),
        poisoned_message_with_id(
            /*ordinal*/ 4,
            "unknown-opaque-format",
            "amsg_01900000-0000-7000-8000-000000000032",
        ),
    ];
    let before = serde_json::to_value(&lines).expect("serialize lines");

    let count = repair_legacy_goal_supervisor_lines_selected_with_provenance(
        lines.as_mut_slice(),
        affected_provenance(),
        &HashSet::new(),
    )
    .expect("ignore excluded candidates");

    assert_eq!(count.total(), 0);
    assert_eq!(
        serde_json::to_value(&lines).expect("serialize lines"),
        before
    );
}

#[test]
fn duplicate_selected_message_id_fails_before_mutation() {
    let duplicate_id = "amsg_01900000-0000-7000-8000-000000000041";
    let mut lines = vec![
        session_meta(AFFECTED_CLI_VERSION),
        delivery(/*ordinal*/ 1, /*trigger_turn*/ true),
        poisoned_message_with_id(
            /*ordinal*/ 2,
            "first supervisor instruction",
            duplicate_id,
        ),
        delivery(/*ordinal*/ 3, /*trigger_turn*/ true),
        poisoned_message_with_id(
            /*ordinal*/ 4,
            "second supervisor instruction",
            duplicate_id,
        ),
    ];
    let before = serde_json::to_value(&lines).expect("serialize lines");

    let error = repair_legacy_goal_supervisor_lines_selected_with_provenance(
        lines.as_mut_slice(),
        GoalSupervisorLineageProvenance::Untrusted,
        &selected(&[duplicate_id]),
    )
    .expect_err("duplicate selected identity must fail closed");

    assert!(error.to_string().contains("occurs more than once"));
    assert_eq!(
        serde_json::to_value(&lines).expect("serialize lines"),
        before
    );
}

#[test]
fn duplicate_selected_fernet_message_id_remains_protected() {
    let duplicate_id = "amsg_01900000-0000-7000-8000-000000000051";
    let mut lines = vec![
        session_meta(AFFECTED_CLI_VERSION),
        delivery(/*ordinal*/ 1, /*trigger_turn*/ true),
        poisoned_message_with_id(
            /*ordinal*/ 2,
            genuine_synthetic_fernet_token(),
            duplicate_id,
        ),
        compacted_message_with_id(
            /*ordinal*/ 3,
            genuine_synthetic_fernet_token(),
            duplicate_id,
        ),
    ];
    let before = serde_json::to_value(&lines).expect("serialize lines");

    let count = repair_legacy_goal_supervisor_lines_selected_with_provenance(
        lines.as_mut_slice(),
        GoalSupervisorLineageProvenance::Untrusted,
        &selected(&[duplicate_id]),
    )
    .expect("duplicate opaque message is not repairable");

    assert_eq!(count.total(), 0);
    assert_eq!(
        serde_json::to_value(&lines).expect("serialize lines"),
        before
    );
}

#[test]
fn delivery_serialization_has_no_new_content_encoding_field() {
    let serialized = serde_json::to_value(delivery(/*ordinal*/ 1, /*trigger_turn*/ true))
        .expect("serialize delivery");
    assert!(serialized["payload"].get("content_encoding").is_none());
}

#[test]
fn inherited_provenance_is_same_thread_openai_only() {
    let mut lines = vec![
        session_meta("0.148.0-alpha.5+frodex.0"),
        delivery(/*ordinal*/ 1, /*trigger_turn*/ true),
        poisoned_message(/*ordinal*/ 2, "synthetic inherited instruction"),
    ];
    let count =
        repair_legacy_goal_supervisor_lines_with_provenance(&mut lines, affected_provenance())
            .expect("same-thread repair");
    assert_eq!(count.total(), 1);

    let mut different_thread = session_meta("0.148.0-alpha.5+frodex.0");
    let RolloutItem::SessionMeta(meta) = &mut different_thread.item else {
        panic!("session metadata");
    };
    meta.meta.id = ThreadId::from_u128(/*value*/ 0x22);
    assert_eq!(
        affected_provenance().continued_through(&[different_thread]),
        GoalSupervisorLineageProvenance::Untrusted
    );
}

#[test]
fn affected_provenance_crosses_only_unfiltered_same_thread_reference() {
    let provenance = affected_provenance();
    let GoalSupervisorLineageProvenance::AffectedAlpha6 { thread_id } = provenance else {
        unreachable!();
    };
    let mut reference = RolloutReferenceItem {
        rollout_path: "synthetic-rollout.jsonl".into(),
        thread_id: Some(thread_id),
        rollout_id: None,
        rollout_timestamp: None,
        segment_id: None,
        max_depth: 0,
        nth_user_message: None,
        compacted_replacement_history_filter_texts: None,
    };
    assert_eq!(
        provenance.continued_through_reference(&reference),
        provenance
    );
    reference.nth_user_message = Some(0);
    assert_eq!(
        provenance.continued_through_reference(&reference),
        GoalSupervisorLineageProvenance::Untrusted
    );
}

#[test]
fn invalid_jsonl_is_preserved_and_breaks_delivery_adjacency() {
    let lines = [
        session_meta(AFFECTED_CLI_VERSION),
        delivery(/*ordinal*/ 1, /*trigger_turn*/ true),
        poisoned_message(/*ordinal*/ 2, "synthetic nonadjacent instruction"),
    ];
    let mut source = encode_jsonl(&lines[..1]);
    source.extend_from_slice(b"{synthetic rejected history\n");
    source.extend_from_slice(&encode_jsonl(&lines[1..]));
    let invalid_start = encode_jsonl(&lines[..1]).len();
    let invalid_end = invalid_start + b"{synthetic rejected history\n".len();

    let (repaired, count) = repair_legacy_goal_supervisor_jsonl_lines_with_provenance(
        &source,
        GoalSupervisorLineageProvenance::Untrusted,
    )
    .expect("repair after rejected boundary");
    let replacement = rewrite_rollout_jsonl_same_length(&source, &repaired)
        .expect("encode repair")
        .expect("damaged record changes");

    assert_eq!(count.total(), 1);
    assert_eq!(
        &replacement[invalid_start..invalid_end],
        &source[invalid_start..invalid_end]
    );

    let mut blocked = encode_jsonl(&lines[..2]);
    blocked.extend_from_slice(b"{synthetic rejected history\n");
    blocked.extend_from_slice(&encode_jsonl(&lines[2..]));
    let error = match repair_legacy_goal_supervisor_jsonl_lines_with_provenance(
        &blocked,
        GoalSupervisorLineageProvenance::Untrusted,
    ) {
        Ok(_) => panic!("rejected line breaks adjacency"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("ambiguous goal-supervisor"));
}

#[test]
fn rewrite_preserves_unterminated_tail_and_projection() {
    let lines = vec![
        session_meta(AFFECTED_CLI_VERSION),
        delivery(/*ordinal*/ 1, /*trigger_turn*/ true),
        poisoned_message(/*ordinal*/ 2, "synthetic unterminated instruction"),
    ];
    let mut source = encode_jsonl(&lines);
    assert_eq!(source.pop(), Some(b'\n'));

    let rewritten = rewrite_legacy_goal_supervisor_jsonl_same_length(&source)
        .expect("rewrite unterminated record")
        .expect("repair target");

    assert_eq!(rewritten.len(), source.len());
    assert!(!rewritten.ends_with(b"\n"));
}

#[test]
fn generic_rewrite_rejects_identity_and_projector_changes() {
    let lines = vec![session_meta(AFFECTED_CLI_VERSION)];
    let source = encode_jsonl(&lines);
    let mut changed = lines;
    changed[0].timestamp = "2026-01-02T00:00:00Z".to_string();
    let error = rewrite_rollout_jsonl_same_length(&source, &changed)
        .expect_err("timestamp change must fail");
    assert!(error.to_string().contains("ordinal or timestamp"));
}

#[test]
fn non_openai_and_nonaffected_candidates_fail_closed() {
    let mut non_openai = session_meta(AFFECTED_CLI_VERSION);
    let RolloutItem::SessionMeta(meta) = &mut non_openai.item else {
        panic!("session metadata");
    };
    meta.meta.model_provider = Some("synthetic-provider".to_string());
    for meta in [non_openai, session_meta("0.148.0-alpha.7+frodex.0")] {
        let source = encode_jsonl(&[
            meta,
            delivery(/*ordinal*/ 1, /*trigger_turn*/ true),
            poisoned_message(/*ordinal*/ 2, "synthetic natural language instruction"),
        ]);
        assert!(rewrite_legacy_goal_supervisor_jsonl_same_length(&source).is_err());
    }
}

#[test]
fn supplied_history_rejects_natural_language_and_preserves_fernet() {
    let natural_language = [
        delivery(/*ordinal*/ 0, /*trigger_turn*/ true).item,
        poisoned_message(/*ordinal*/ 1, "synthetic supplied instruction").item,
    ];
    let fernet = [
        delivery(/*ordinal*/ 0, /*trigger_turn*/ true).item,
        poisoned_message(/*ordinal*/ 1, genuine_synthetic_fernet_token()).item,
    ];

    let error = reject_malformed_goal_supervisor_supplied_history(&natural_language)
        .expect_err("natural-language supplied content must fail closed");
    assert!(matches!(
        error,
        crate::ThreadStoreError::InvalidRequest { .. }
    ));
    reject_malformed_goal_supervisor_supplied_history(&fernet)
        .expect("Fernet-shaped supplied content is protected");
}
