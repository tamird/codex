use crate::ResponseItemEnvelope;
use crate::RolloutItem;
use codex_protocol::items::TurnItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::SessionMetaLine;

use crate::segment_checkpoint::SegmentStateCheckpointMatch;
use crate::segment_checkpoint::match_segment_state_checkpoint;

/// Whether a reverse model-context scan needs more rollout items.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelContextScanProgress {
    /// The reader should provide the next older rollout item.
    Continue,
    /// The scan has collected a safe bounded suffix.
    Complete,
}

impl ModelContextScanProgress {
    pub fn is_complete(self) -> bool {
        matches!(self, Self::Complete)
    }
}

/// Accumulates newest-to-oldest rollout items until they are sufficient to reconstruct the latest
/// model context.
///
/// Storage implementations own how they fetch older items. Local JSONL readers and future
/// reverse-paged cloud readers can both feed their items through this scan to share the cutoff
/// rules and chronological replay assembly.
///
/// A versioned segment-state checkpoint is an immediate cutoff. Its replacement history,
/// previous-turn settings, reference-context disposition, and world-state disposition certify
/// that older segments are not needed for current-state reconstruction.
///
/// An unmarked compaction remains a compatibility boundary for model-visible history, but sticky
/// settings and token state may still exist in an older segment. The scan therefore continues to
/// the beginning while retaining only the newest required sticky-state records. A rollback,
/// unusable compaction, or invalid checkpoint forces complete replay.
#[derive(Debug, Default)]
pub struct ModelContextScan {
    items_newest_first: Vec<RolloutItem>,
    saw_segment_checkpoint: bool,
    segment_checkpoint_blocked: bool,
    must_scan_to_start: bool,
    saw_unmarked_compaction: bool,
    saw_completed_turn_context: bool,
    compatibility_boundary: bool,
    active_segment: ActiveTurnSegment,
    saw_thread_settings: bool,
    saw_token_count: bool,
    token_info_resolved: bool,
    rate_limits_resolved: bool,
}

impl ModelContextScan {
    /// Adds the next newest-to-oldest rollout item and reports whether the reader can stop.
    pub fn push(&mut self, item: RolloutItem) -> ModelContextScanProgress {
        let retain = self.should_retain(&item);
        let progress = self.observe(&item);
        if retain {
            self.items_newest_first.push(item);
        }
        progress
    }

    /// Reports whether the completed cutoff is a certified active-segment checkpoint.
    pub fn completed_at_segment_checkpoint(&self) -> bool {
        self.saw_segment_checkpoint && !self.must_scan_to_start
    }

    /// Reports whether [`Self::finish`] will return a bounded replay.
    pub fn has_bounded_context(&self) -> bool {
        self.has_bounded_cutoff()
    }

    /// Returns the collected items in chronological order with canonical head metadata.
    ///
    /// Call this after the reader reaches the beginning of its source or after [`Self::push`]
    /// returns [`ModelContextScanProgress::Complete`].
    pub fn finish(mut self, session_meta: SessionMetaLine) -> Vec<RolloutItem> {
        self.items_newest_first.reverse();
        if self.has_bounded_cutoff() {
            // A bounded scan stops before reaching the head. Prepend the separately loaded head
            // SessionMeta, which remains canonical when copied fork history contains later
            // metadata.
            self.items_newest_first
                .insert(0, RolloutItem::SessionMeta(session_meta));
        }
        self.items_newest_first
    }

    fn observe(&mut self, item: &RolloutItem) -> ModelContextScanProgress {
        self.observe_sticky_state(item);

        if self.must_scan_to_start || self.compatibility_boundary {
            return ModelContextScanProgress::Continue;
        }

        match item {
            RolloutItem::Compacted(compacted) => {
                match match_segment_state_checkpoint(compacted, self.items_newest_first.as_slice())
                {
                    SegmentStateCheckpointMatch::NotPresent => {
                        self.segment_checkpoint_blocked = true;
                        if compacted.replacement_history.is_some()
                            && compacted.window_number.is_some()
                        {
                            self.saw_unmarked_compaction = true;
                        } else {
                            self.must_scan_to_start = true;
                        }
                    }
                    SegmentStateCheckpointMatch::Valid if !self.segment_checkpoint_blocked => {
                        self.saw_segment_checkpoint = true;
                    }
                    SegmentStateCheckpointMatch::Valid => {}
                    SegmentStateCheckpointMatch::Invalid => {
                        self.must_scan_to_start = true;
                    }
                }
            }
            RolloutItem::EventMsg(EventMsg::ThreadRolledBack(_)) => {
                // Paginated threads reject rollback. Keep old rollouts correct rather than
                // duplicating rollback survival semantics in this bounded selector.
                self.must_scan_to_start = true;
            }
            RolloutItem::EventMsg(EventMsg::ItemCompleted(event)) => {
                if self.active_segment.turn_id.is_none() {
                    self.active_segment.turn_id = Some(event.turn_id.clone());
                }
                if turn_ids_are_compatible(
                    self.active_segment.turn_id.as_deref(),
                    Some(event.turn_id.as_str()),
                ) {
                    self.active_segment.has_user_turn |=
                        matches!(&event.item, TurnItem::UserMessage(_));
                }
            }
            RolloutItem::EventMsg(EventMsg::TurnComplete(event)) => {
                self.active_segment
                    .turn_id
                    .get_or_insert_with(|| event.turn_id.clone());
            }
            RolloutItem::EventMsg(EventMsg::TurnAborted(event)) => {
                if let Some(turn_id) = &event.turn_id {
                    self.active_segment
                        .turn_id
                        .get_or_insert_with(|| turn_id.clone());
                }
            }
            RolloutItem::EventMsg(EventMsg::TurnStarted(event)) => {
                if turn_ids_are_compatible(
                    self.active_segment.turn_id.as_deref(),
                    Some(event.turn_id.as_str()),
                ) {
                    self.finalize_active_segment();
                }
            }
            RolloutItem::TurnContext(context) => {
                if self.active_segment.turn_id.is_none() {
                    self.active_segment.turn_id = context.turn_id.clone();
                }
                if turn_ids_are_compatible(
                    self.active_segment.turn_id.as_deref(),
                    context.turn_id.as_deref(),
                ) {
                    self.active_segment.has_turn_context = true;
                }
            }
            RolloutItem::ResponseItem(response_item) => {
                self.active_segment.has_user_turn |=
                    response_item_counts_as_user_turn(response_item);
            }
            RolloutItem::InterAgentCommunication(_) => {
                self.active_segment.has_user_turn = true;
            }
            RolloutItem::EventMsg(EventMsg::UserMessage(_)) => {
                self.active_segment.has_user_turn = true;
            }
            RolloutItem::EventMsg(_)
            | RolloutItem::RolloutReference(_)
            | RolloutItem::SessionMeta(_)
            | RolloutItem::InterAgentCommunicationMetadata { .. }
            | RolloutItem::RealtimeItem(_)
            | RolloutItem::SecurityRiskScore(_)
            | RolloutItem::WorldState(_) => {}
        }

        self.compatibility_boundary = !self.must_scan_to_start
            && self.saw_unmarked_compaction
            && self.saw_completed_turn_context;

        if self.saw_segment_checkpoint && !self.must_scan_to_start {
            ModelContextScanProgress::Complete
        } else {
            ModelContextScanProgress::Continue
        }
    }

    fn should_retain(&self, item: &RolloutItem) -> bool {
        if self.must_scan_to_start || !self.compatibility_boundary {
            return true;
        }

        match item {
            RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(_)) => !self.saw_thread_settings,
            RolloutItem::EventMsg(EventMsg::TokenCount(event)) => {
                !self.saw_token_count
                    || (!self.token_info_resolved && event.info.is_some())
                    || (!self.rate_limits_resolved && event.rate_limits.is_some())
            }
            _ => false,
        }
    }

    fn observe_sticky_state(&mut self, item: &RolloutItem) {
        match item {
            RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(_)) => {
                self.saw_thread_settings = true;
            }
            RolloutItem::EventMsg(EventMsg::TokenCount(event)) => {
                self.saw_token_count = true;
                self.token_info_resolved |= event.info.is_some();
                self.rate_limits_resolved |= event.rate_limits.is_some();
            }
            _ => {}
        }
    }

    fn finalize_active_segment(&mut self) {
        if self.active_segment.has_user_turn && self.active_segment.has_turn_context {
            self.saw_completed_turn_context = true;
        }
        self.active_segment = ActiveTurnSegment::default();
    }

    fn has_bounded_cutoff(&self) -> bool {
        !self.must_scan_to_start && (self.saw_segment_checkpoint || self.compatibility_boundary)
    }
}

#[derive(Debug, Default)]
struct ActiveTurnSegment {
    turn_id: Option<String>,
    has_user_turn: bool,
    has_turn_context: bool,
}

fn turn_ids_are_compatible(active_turn_id: Option<&str>, item_turn_id: Option<&str>) -> bool {
    active_turn_id
        .is_none_or(|turn_id| item_turn_id.is_none_or(|item_turn_id| item_turn_id == turn_id))
}

fn response_item_counts_as_user_turn(response_item: &ResponseItemEnvelope) -> bool {
    match &response_item.item {
        ResponseItem::AgentMessage { .. } => true,
        ResponseItem::Message { role, content, .. } => {
            role == "assistant" && InterAgentCommunication::is_message_content(content)
        }
        _ => false,
    }
}
