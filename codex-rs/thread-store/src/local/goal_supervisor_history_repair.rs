use std::collections::HashSet;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::models::AgentMessageInputContent;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::RolloutReferenceItem;
use codex_protocol::protocol::SessionMetaLine;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;

use crate::ThreadStoreError;
use crate::ThreadStoreResult;

pub(super) const SELECTED_MISSING_MESSAGE_ID: &str = "\0missing-goal-supervisor-message-id";

const AFFECTED_CLI_VERSION: &str = "0.148.0-alpha.6+frodex.0";

/// Evidence that an alpha6 Frodex rollout using the `openai` model provider consumes same-thread
/// rotation history.
///
/// The thread ID prevents alpha6 evidence from crossing a fork, `history_base`, or filtered
/// reference. A physical rollout must also persist the `openai` provider before inherited evidence
/// can authorize its legacy representation.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub(super) enum GoalSupervisorLineageProvenance {
    #[default]
    Untrusted,
    AffectedAlpha6 {
        thread_id: ThreadId,
    },
}

impl GoalSupervisorLineageProvenance {
    /// Applies one physical rollout's persisted identity to inherited lineage provenance.
    pub(super) fn continued_through_session_meta(self, meta: &SessionMetaLine) -> Self {
        if meta.meta.model_provider.as_deref() != Some("openai") {
            return Self::Untrusted;
        }
        let inherited_same_thread = matches!(
            self,
            Self::AffectedAlpha6 { thread_id } if thread_id == meta.meta.id
        );
        if meta.meta.cli_version == AFFECTED_CLI_VERSION || inherited_same_thread {
            Self::AffectedAlpha6 {
                thread_id: meta.meta.id,
            }
        } else {
            Self::Untrusted
        }
    }

    pub(super) fn continued_through(self, lines: &[RolloutLine]) -> Self {
        let Some(meta) = lines.iter().find_map(|line| match &line.item {
            RolloutItem::SessionMeta(meta) => Some(meta),
            _ => None,
        }) else {
            // Callers may split one already-classified physical file at rejected JSONL records.
            return self;
        };
        self.continued_through_session_meta(meta)
    }

    /// Carries affected provenance only across a canonical, unfiltered same-thread rotation.
    pub(super) fn continued_through_reference(self, reference: &RolloutReferenceItem) -> Self {
        match self {
            Self::AffectedAlpha6 { thread_id }
                if reference.thread_id == Some(thread_id)
                    && reference.nth_user_message.is_none()
                    && reference
                        .compacted_replacement_history_filter_texts
                        .is_none() =>
            {
                self
            }
            _ => Self::Untrusted,
        }
    }

    pub(super) fn is_affected(self) -> bool {
        matches!(self, Self::AffectedAlpha6 { .. })
    }
}

/// Classification of the exact legacy goal-supervisor envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CandidateClassification {
    NotCandidate,
    ExactLegacyDamage,
    ProtectedOpaque,
    Ambiguous,
}

#[derive(Clone, Copy)]
enum CandidateContext {
    TopLevel(Option<bool>),
    Compacted,
}

/// Counts the physical and compacted messages repaired in one rollout.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct GoalSupervisorRepairCount {
    pub(super) top_level: usize,
    pub(super) compacted: usize,
}

impl GoalSupervisorRepairCount {
    pub(super) fn total(self) -> usize {
        self.top_level.saturating_add(self.compacted)
    }
}

/// Repairs only the exact goal-supervisor representation emitted by the affected Frodex build.
///
/// `supervisor.followup_parent` accepted a plaintext `message`, but the affected producer stored
/// that message as opaque encrypted content. The affected build version, persisted `openai` model
/// provider, delivery metadata, agent paths, message identity, turn identity, exact two-part
/// envelope, and payload classification jointly bound the compatibility repair. Canonical
/// Fernet-v0 framing is preserved without authenticating its HMAC. A rejected Fernet payload is
/// repairable only when it contains both Unicode whitespace and a printable non-whitespace scalar,
/// with no non-whitespace control scalar.
///
/// This accepts a bounded false positive for legacy non-Fernet opaque data that looks like natural
/// language. Fernet-shaped data and one-word plaintext are safe false negatives and fail closed.
#[cfg(test)]
pub(super) fn repair_legacy_goal_supervisor_lines(
    lines: &mut [RolloutLine],
) -> ThreadStoreResult<GoalSupervisorRepairCount> {
    repair_legacy_goal_supervisor_lines_with_provenance(
        lines,
        GoalSupervisorLineageProvenance::Untrusted,
    )
}

/// Repairs exact legacy damage when this rollout or same-thread rotation ancestry proves that the
/// affected Frodex build wrote the lineage.
pub(super) fn repair_legacy_goal_supervisor_lines_with_provenance(
    lines: &mut [RolloutLine],
    inherited_provenance: GoalSupervisorLineageProvenance,
) -> ThreadStoreResult<GoalSupervisorRepairCount> {
    repair_selected_legacy_goal_supervisor_lines_with_provenance(
        lines,
        inherited_provenance,
        /*selected_message_ids*/ None,
    )
}

/// Repairs only candidates whose message IDs occur in `selected_message_ids`.
///
/// A bounded history reader can use this API after materialization identifies the records it will
/// consume. Target-shaped records outside that set are not classified, so excluded encrypted
/// content cannot be changed or reject the bounded read.
#[cfg(test)]
pub(super) fn repair_legacy_goal_supervisor_lines_selected_with_provenance(
    lines: &mut [RolloutLine],
    inherited_provenance: GoalSupervisorLineageProvenance,
    selected_message_ids: &HashSet<String>,
) -> ThreadStoreResult<GoalSupervisorRepairCount> {
    reject_duplicate_selected_candidates(lines, selected_message_ids)?;
    repair_selected_legacy_goal_supervisor_lines_with_provenance(
        lines,
        inherited_provenance,
        Some(selected_message_ids),
    )
}

fn repair_selected_legacy_goal_supervisor_lines_with_provenance(
    lines: &mut [RolloutLine],
    inherited_provenance: GoalSupervisorLineageProvenance,
    selected_message_ids: Option<&HashSet<String>>,
) -> ThreadStoreResult<GoalSupervisorRepairCount> {
    let provenance = inherited_provenance.continued_through(lines);
    validate_candidates(lines, provenance.is_affected(), selected_message_ids)?;
    if !provenance.is_affected() {
        return Ok(GoalSupervisorRepairCount::default());
    }

    let mut count = GoalSupervisorRepairCount::default();
    let mut preceding_delivery = None;
    for line in lines {
        match &mut line.item {
            RolloutItem::InterAgentCommunicationMetadata { trigger_turn } => {
                preceding_delivery = Some(*trigger_turn);
                continue;
            }
            RolloutItem::ResponseItem(item)
                if candidate_is_selected(item, selected_message_ids)
                    && classify_agent_message(
                        item,
                        CandidateContext::TopLevel(preceding_delivery),
                    ) == CandidateClassification::ExactLegacyDamage =>
            {
                repair_agent_message(item)?;
                count.top_level = count.top_level.saturating_add(1);
            }
            RolloutItem::Compacted(compacted) => {
                if let Some(history) = compacted.replacement_history.as_mut() {
                    for item in history {
                        if candidate_is_selected(item, selected_message_ids)
                            && classify_agent_message(item, CandidateContext::Compacted)
                                == CandidateClassification::ExactLegacyDamage
                        {
                            repair_agent_message(item)?;
                            count.compacted = count.compacted.saturating_add(1);
                        }
                    }
                }
            }
            _ => {}
        }
        preceding_delivery = None;
    }
    Ok(count)
}

/// Returns a same-length JSONL replacement, or `None` when the rollout needs no repair.
///
/// Unchanged physical lines are copied byte-for-byte. Changed records are serialized from the
/// structured protocol type and padded with trailing JSON whitespace before their newline. The
/// fixed physical spans preserve paginated history cutoffs and SQLite byte offsets.
#[cfg(test)]
pub(super) fn rewrite_legacy_goal_supervisor_jsonl_same_length(
    source: &[u8],
) -> ThreadStoreResult<Option<Vec<u8>>> {
    let (lines, count) = repair_legacy_goal_supervisor_jsonl_lines_with_provenance(
        source,
        GoalSupervisorLineageProvenance::Untrusted,
    )?;
    if count.total() == 0 {
        return Ok(None);
    }
    rewrite_rollout_jsonl_same_length(source, lines.as_slice())
}

/// Rejects target-shaped encrypted envelopes supplied directly by a caller.
///
/// Supplied bounded history may omit `SessionMeta`. Fernet-shaped opaque content remains
/// protected, while natural-language candidates and unsupported encodings fail closed rather than
/// being persisted under an ambiguous legacy representation.
pub(super) fn reject_malformed_goal_supervisor_supplied_history(
    items: &[RolloutItem],
) -> ThreadStoreResult<()> {
    let lines = items
        .iter()
        .cloned()
        .enumerate()
        .map(|(ordinal, item)| RolloutLine {
            timestamp: String::new(),
            ordinal: u64::try_from(ordinal).ok(),
            item,
        })
        .collect::<Vec<_>>();
    validate_candidates(
        lines.as_slice(),
        /*affected_ancestry*/ false,
        /*selected_message_ids*/ None,
    )
    .map_err(|_| ThreadStoreError::InvalidRequest {
        message: "supplied history contains an untrusted goal-supervisor encrypted envelope"
            .to_string(),
    })
}

/// Encodes caller-mutated rollout records without moving any physical JSONL boundary.
///
/// This helper also proves that the mutation does not change thread-history projection. It is
/// shared by message repair and reference repair so one active-root publication can contain both.
pub(super) fn rewrite_rollout_jsonl_same_length(
    source: &[u8],
    desired_lines: &[RolloutLine],
) -> ThreadStoreResult<Option<Vec<u8>>> {
    let parsed = parse_complete_jsonl(source)?;
    let source_line_count = parsed
        .iter()
        .filter(|line| line.rollout_line.is_some())
        .count();
    if desired_lines.len() != source_line_count {
        return Err(repair_error(format!(
            "caller supplied {} rollout records for {source_line_count} physical records",
            desired_lines.len()
        )));
    }

    let mut rewritten = Vec::with_capacity(source.len());
    let mut desired_iter = desired_lines.iter();
    let mut changed = false;
    for parsed in &parsed {
        let Some(before) = parsed.rollout_line.as_ref() else {
            rewritten.extend_from_slice(parsed.raw_line);
            continue;
        };
        let Some(line) = desired_iter.next() else {
            return Err(repair_error(
                "desired rollout length changed during same-length rewrite",
            ));
        };
        if serde_json::to_value(before).map_err(repair_error)?
            == serde_json::to_value(line).map_err(repair_error)?
        {
            rewritten.extend_from_slice(parsed.raw_line);
            continue;
        }
        changed = true;
        if before.ordinal != line.ordinal || before.timestamp != line.timestamp {
            return Err(repair_error(
                "same-length rewrite changed rollout ordinal or timestamp",
            ));
        }
        if codex_app_server_protocol::project_rollout_line(before)
            != codex_app_server_protocol::project_rollout_line(line)
        {
            return Err(repair_error(
                "same-length rewrite changed the thread-history projection",
            ));
        }

        let encoded = serde_json::to_vec(line).map_err(repair_error)?;
        let content_len = parsed.raw_line.len() - usize::from(parsed.newline_terminated);
        if encoded.len() > content_len {
            return Err(repair_error(format!(
                "same-length rewrite grew rollout ordinal {} from {content_len} to {} bytes",
                line.ordinal
                    .map_or_else(|| "unknown".to_string(), |ordinal| ordinal.to_string()),
                encoded.len()
            )));
        }
        rewritten.extend_from_slice(encoded.as_slice());
        rewritten.resize(rewritten.len() + content_len - encoded.len(), b' ');
        if parsed.newline_terminated {
            rewritten.push(b'\n');
        }
    }

    if !changed {
        return Ok(None);
    }

    if rewritten.len() != source.len() {
        return Err(repair_error(
            "goal-supervisor repair changed rollout length",
        ));
    }
    let reparsed = parse_complete_jsonl(rewritten.as_slice())?;
    if reparsed.len() != parsed.len() {
        return Err(repair_error(
            "goal-supervisor repair changed the physical rollout record count",
        ));
    }
    let before_lines = parsed.iter().filter_map(|line| line.rollout_line.as_ref());
    let after_lines = reparsed
        .iter()
        .filter_map(|line| line.rollout_line.as_ref());
    for ((before, after), expected) in before_lines.zip(after_lines).zip(desired_lines) {
        if before.ordinal != after.ordinal
            || before.timestamp != after.timestamp
            || serde_json::to_value(after).map_err(repair_error)?
                != serde_json::to_value(expected).map_err(repair_error)?
        {
            return Err(repair_error(
                "goal-supervisor repair did not preserve rollout record identity",
            ));
        }
    }
    Ok(Some(rewritten))
}

/// Repairs valid records in physical JSONL order while retaining rejected records byte-for-byte.
///
/// Each rejected or blank physical line is an adjacency boundary: delivery metadata on one side
/// cannot authorize an agent message on the other side.
pub(super) fn repair_legacy_goal_supervisor_jsonl_lines_with_provenance(
    source: &[u8],
    inherited_provenance: GoalSupervisorLineageProvenance,
) -> ThreadStoreResult<(Vec<RolloutLine>, GoalSupervisorRepairCount)> {
    repair_selected_legacy_goal_supervisor_jsonl_lines_with_provenance(
        source,
        inherited_provenance,
        /*selected_message_ids*/ None,
    )
}

/// Repairs selected message IDs while retaining rejected and unselected records byte-for-byte.
pub(super) fn repair_legacy_goal_supervisor_jsonl_lines_selected_with_provenance(
    source: &[u8],
    inherited_provenance: GoalSupervisorLineageProvenance,
    selected_message_ids: &HashSet<String>,
) -> ThreadStoreResult<(Vec<RolloutLine>, GoalSupervisorRepairCount)> {
    repair_selected_legacy_goal_supervisor_jsonl_lines_with_provenance(
        source,
        inherited_provenance,
        Some(selected_message_ids),
    )
}

fn repair_selected_legacy_goal_supervisor_jsonl_lines_with_provenance(
    source: &[u8],
    inherited_provenance: GoalSupervisorLineageProvenance,
    selected_message_ids: Option<&HashSet<String>>,
) -> ThreadStoreResult<(Vec<RolloutLine>, GoalSupervisorRepairCount)> {
    let parsed = parse_complete_jsonl(source)?;
    let all_lines = parsed
        .iter()
        .filter_map(|line| line.rollout_line.clone())
        .collect::<Vec<_>>();
    if let Some(selected_message_ids) = selected_message_ids {
        reject_duplicate_selected_candidates(all_lines.as_slice(), selected_message_ids)?;
    }
    let provenance = inherited_provenance.continued_through(all_lines.as_slice());
    let mut repaired_lines = Vec::with_capacity(all_lines.len());
    let mut total = GoalSupervisorRepairCount::default();
    let mut contiguous = Vec::new();

    let flush = |contiguous: &mut Vec<RolloutLine>,
                 repaired_lines: &mut Vec<RolloutLine>,
                 total: &mut GoalSupervisorRepairCount|
     -> ThreadStoreResult<()> {
        if contiguous.is_empty() {
            return Ok(());
        }
        let count = repair_selected_legacy_goal_supervisor_lines_with_provenance(
            contiguous.as_mut_slice(),
            provenance,
            selected_message_ids,
        )?;
        total.top_level = total.top_level.saturating_add(count.top_level);
        total.compacted = total.compacted.saturating_add(count.compacted);
        repaired_lines.append(contiguous);
        Ok(())
    };

    for parsed_line in parsed {
        if let Some(line) = parsed_line.rollout_line {
            contiguous.push(line);
        } else {
            flush(&mut contiguous, &mut repaired_lines, &mut total)?;
        }
    }
    flush(&mut contiguous, &mut repaired_lines, &mut total)?;
    Ok((repaired_lines, total))
}

fn validate_candidates(
    lines: &[RolloutLine],
    affected_ancestry: bool,
    selected_message_ids: Option<&HashSet<String>>,
) -> ThreadStoreResult<()> {
    let mut preceding_delivery = None;
    for line in lines {
        match &line.item {
            RolloutItem::InterAgentCommunicationMetadata { trigger_turn } => {
                preceding_delivery = Some(*trigger_turn);
                continue;
            }
            RolloutItem::ResponseItem(item) => {
                if !candidate_is_selected(item, selected_message_ids) {
                    preceding_delivery = None;
                    continue;
                }
                let classification =
                    classify_agent_message(item, CandidateContext::TopLevel(preceding_delivery));
                if classification == CandidateClassification::Ambiguous
                    || (!affected_ancestry
                        && classification == CandidateClassification::ExactLegacyDamage)
                {
                    return Err(ambiguous_candidate_error(
                        line.ordinal,
                        /*compacted_index*/ None,
                    ));
                }
            }
            RolloutItem::Compacted(compacted) => {
                for (index, item) in compacted
                    .replacement_history
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .enumerate()
                {
                    if !candidate_is_selected(item, selected_message_ids) {
                        continue;
                    }
                    let classification = classify_agent_message(item, CandidateContext::Compacted);
                    if classification == CandidateClassification::Ambiguous
                        || (!affected_ancestry
                            && classification == CandidateClassification::ExactLegacyDamage)
                    {
                        return Err(ambiguous_candidate_error(line.ordinal, Some(index)));
                    }
                }
            }
            _ => {}
        }
        preceding_delivery = None;
    }
    Ok(())
}

fn candidate_is_selected(
    item: &ResponseItem,
    selected_message_ids: Option<&HashSet<String>>,
) -> bool {
    let Some(selected_message_ids) = selected_message_ids else {
        return true;
    };
    let ResponseItem::AgentMessage { id, .. } = item else {
        return false;
    };
    match id {
        Some(id) => selected_message_ids.contains(id.as_str()),
        None => selected_message_ids.contains(SELECTED_MISSING_MESSAGE_ID),
    }
}

/// Returns selected exact-or-ambiguous candidate IDs for cross-segment duplicate rejection.
pub(super) fn selected_goal_supervisor_candidate_ids(
    lines: &[RolloutLine],
    selected_message_ids: &HashSet<String>,
) -> HashSet<String> {
    let mut candidates = HashSet::new();
    let mut inspect = |item: &ResponseItem, context: CandidateContext| {
        if !candidate_is_selected(item, Some(selected_message_ids))
            || matches!(
                classify_agent_message(item, context),
                CandidateClassification::NotCandidate | CandidateClassification::ProtectedOpaque
            )
        {
            return;
        }
        let ResponseItem::AgentMessage { id, .. } = item else {
            return;
        };
        candidates.insert(
            id.as_ref()
                .map(|id| id.as_str().to_string())
                .unwrap_or_else(|| SELECTED_MISSING_MESSAGE_ID.to_string()),
        );
    };
    let mut preceding_delivery = None;
    for line in lines {
        match &line.item {
            RolloutItem::InterAgentCommunicationMetadata { trigger_turn } => {
                preceding_delivery = Some(*trigger_turn);
                continue;
            }
            RolloutItem::ResponseItem(item) => {
                inspect(item, CandidateContext::TopLevel(preceding_delivery));
            }
            RolloutItem::Compacted(compacted) => {
                for item in compacted.replacement_history.as_deref().unwrap_or_default() {
                    inspect(item, CandidateContext::Compacted);
                }
            }
            _ => {}
        }
        preceding_delivery = None;
    }
    candidates
}

/// Returns selected candidate IDs from the original physical JSONL before any repair is applied.
pub(super) fn selected_goal_supervisor_candidate_ids_from_jsonl(
    source: &[u8],
    selected_message_ids: &HashSet<String>,
) -> ThreadStoreResult<HashSet<String>> {
    let lines = parse_complete_jsonl(source)?
        .into_iter()
        .filter_map(|line| line.rollout_line)
        .collect::<Vec<_>>();
    Ok(selected_goal_supervisor_candidate_ids(
        lines.as_slice(),
        selected_message_ids,
    ))
}

fn reject_duplicate_selected_candidates(
    lines: &[RolloutLine],
    selected_message_ids: &HashSet<String>,
) -> ThreadStoreResult<()> {
    let mut seen = HashSet::new();
    for line in lines {
        let mut inspect = |item: &ResponseItem| -> ThreadStoreResult<()> {
            if !candidate_is_selected(item, Some(selected_message_ids)) {
                return Ok(());
            }
            let classification = classify_agent_message(item, CandidateContext::Compacted);
            if matches!(
                classification,
                CandidateClassification::NotCandidate | CandidateClassification::ProtectedOpaque
            ) {
                return Ok(());
            }
            let ResponseItem::AgentMessage { id: Some(id), .. } = item else {
                return Ok(());
            };
            if !seen.insert(id.as_str().to_string()) {
                return Err(ThreadStoreError::Conflict {
                    message: format!(
                        "selected goal-supervisor message ID {} occurs more than once",
                        id.as_str()
                    ),
                });
            }
            Ok(())
        };
        match &line.item {
            RolloutItem::ResponseItem(item) => inspect(item)?,
            RolloutItem::Compacted(compacted) => {
                for item in compacted.replacement_history.as_deref().unwrap_or_default() {
                    inspect(item)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn classify_agent_message(
    item: &ResponseItem,
    context: CandidateContext,
) -> CandidateClassification {
    let ResponseItem::AgentMessage {
        id,
        author,
        recipient,
        content,
        internal_chat_message_metadata_passthrough,
    } = item
    else {
        return CandidateClassification::NotCandidate;
    };
    let Ok(recipient_path) = AgentPath::from_string(recipient.clone()) else {
        return CandidateClassification::NotCandidate;
    };
    let Ok(expected_author) = recipient_path.join("goal_supervisor") else {
        return CandidateClassification::NotCandidate;
    };
    if expected_author.as_str() != author {
        return CandidateClassification::NotCandidate;
    }
    let expected_header =
        format!("Message Type: NEW_TASK\nTask name: {recipient}\nSender: {author}\nPayload:\n");
    let [
        AgentMessageInputContent::InputText { text: header },
        AgentMessageInputContent::EncryptedContent { encrypted_content },
    ] = content.as_slice()
    else {
        return CandidateClassification::NotCandidate;
    };
    if header != &expected_header {
        return CandidateClassification::NotCandidate;
    }

    if looks_like_fernet_v0(encrypted_content) {
        return CandidateClassification::ProtectedOpaque;
    }
    match context {
        CandidateContext::TopLevel(Some(true)) | CandidateContext::Compacted => {}
        CandidateContext::TopLevel(_) => return CandidateClassification::Ambiguous,
    }
    if id.as_ref().is_none_or(|id| id.as_str().is_empty())
        || internal_chat_message_metadata_passthrough
            .as_ref()
            .and_then(|metadata| metadata.turn_id.as_deref())
            .is_none_or(str::is_empty)
    {
        return CandidateClassification::Ambiguous;
    }
    if looks_like_printable_natural_language(encrypted_content) {
        CandidateClassification::ExactLegacyDamage
    } else {
        CandidateClassification::Ambiguous
    }
}

/// Recognizes canonical URL-safe padded Fernet-v0 framing without authenticating or decrypting it.
fn looks_like_fernet_v0(payload: &str) -> bool {
    const VERSION_TIMESTAMP_IV_LEN: usize = 1 + 8 + 16;
    const HMAC_LEN: usize = 32;
    const CIPHERTEXT_BLOCK_LEN: usize = 16;
    const MIN_TOKEN_LEN: usize = VERSION_TIMESTAMP_IV_LEN + CIPHERTEXT_BLOCK_LEN + HMAC_LEN;

    let Ok(decoded) = URL_SAFE.decode(payload) else {
        return false;
    };
    if URL_SAFE.encode(decoded.as_slice()) != payload
        || decoded.len() < MIN_TOKEN_LEN
        || decoded.first() != Some(&0x80)
    {
        return false;
    }
    let ciphertext_len = decoded.len() - VERSION_TIMESTAMP_IV_LEN - HMAC_LEN;
    ciphertext_len >= CIPHERTEXT_BLOCK_LEN && ciphertext_len.is_multiple_of(CIPHERTEXT_BLOCK_LEN)
}

/// Applies the exact structural proxy used for repairable natural-language payloads.
fn looks_like_printable_natural_language(payload: &str) -> bool {
    let mut has_whitespace = false;
    let mut has_non_whitespace = false;
    for character in payload.chars() {
        if character.is_whitespace() {
            has_whitespace = true;
        } else if character.is_control() {
            return false;
        } else {
            has_non_whitespace = true;
        }
    }
    has_whitespace && has_non_whitespace
}

fn repair_agent_message(item: &mut ResponseItem) -> ThreadStoreResult<()> {
    let ResponseItem::AgentMessage { content, .. } = item else {
        return Err(repair_error("classified item changed before repair"));
    };
    let [
        AgentMessageInputContent::InputText { text: header },
        AgentMessageInputContent::EncryptedContent { encrypted_content },
    ] = content.as_slice()
    else {
        return Err(repair_error(
            "classified goal-supervisor envelope changed before repair",
        ));
    };
    let plaintext = format!("{header}{encrypted_content}");
    *content = vec![AgentMessageInputContent::InputText { text: plaintext }];
    Ok(())
}

fn ambiguous_candidate_error(
    ordinal: Option<u64>,
    compacted_index: Option<usize>,
) -> ThreadStoreError {
    let mut message = format!(
        "ambiguous goal-supervisor encrypted envelope at rollout ordinal {}",
        ordinal.map_or_else(|| "unknown".to_string(), |ordinal| ordinal.to_string())
    );
    if let Some(index) = compacted_index {
        message.push_str(format!(", compacted replacement index {index}").as_str());
    }
    repair_error(message)
}

struct ParsedLine<'a> {
    raw_line: &'a [u8],
    newline_terminated: bool,
    rollout_line: Option<RolloutLine>,
}

fn parse_complete_jsonl(source: &[u8]) -> ThreadStoreResult<Vec<ParsedLine<'_>>> {
    let mut parsed = Vec::new();
    for raw_line in source.split_inclusive(|byte| *byte == b'\n') {
        let newline_terminated = raw_line.ends_with(b"\n");
        let content = &raw_line[..raw_line.len() - usize::from(newline_terminated)];
        if content.iter().all(u8::is_ascii_whitespace) {
            parsed.push(ParsedLine {
                raw_line,
                newline_terminated,
                rollout_line: None,
            });
            continue;
        }
        // Legacy rollout readers skip records that the current protocol cannot decode. Keep that
        // compatibility contract here and retain every rejected byte in the same physical span.
        let rollout_line = serde_json::from_slice::<RolloutLine>(content).ok();
        parsed.push(ParsedLine {
            raw_line,
            newline_terminated,
            rollout_line,
        });
    }
    Ok(parsed)
}

fn repair_error(error: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("failed to repair legacy goal-supervisor history: {error}"),
    }
}

#[cfg(test)]
#[path = "goal_supervisor_history_repair_tests.rs"]
mod tests;
