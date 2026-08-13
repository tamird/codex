use std::error::Error;
use std::fmt;

use codex_history::CompactedItem;
use codex_history::RolloutItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SegmentPreviousTurnSettings;
use codex_protocol::protocol::SegmentStateCheckpoint;
use codex_protocol::protocol::SegmentStateCheckpointDisposition;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::TokenCountEvent;
use codex_protocol::protocol::TurnContextItem;
use codex_protocol::protocol::WorldStateItem;
use uuid::Uuid;
use uuid::Version;

/// Checkpoint grammar understood by this version of the rollout reader.
pub const SEGMENT_STATE_CHECKPOINT_VERSION: u32 = 1;

/// A validated sequence that makes current model state independent of older rollout segments.
///
/// Construction certifies the replacement-history record, optional comparison-state baselines,
/// effective thread settings, and token/rate-limit snapshot as one unit. Storage APIs accept this
/// type instead of an arbitrary item vector so rotation callers cannot persist an incomplete unit.
#[derive(Clone, Debug)]
pub struct CertifiedSegmentStateCheckpoint {
    items: Vec<RolloutItem>,
}

impl CertifiedSegmentStateCheckpoint {
    /// Builds a version-1 checkpoint from state already installed in the session.
    pub fn new(
        mut compacted: CompactedItem,
        previous_turn_settings: Option<SegmentPreviousTurnSettings>,
        world_state: Option<WorldStateItem>,
        reference_context: Option<TurnContextItem>,
        thread_settings: ThreadSettingsAppliedEvent,
        token_count: TokenCountEvent,
    ) -> Result<Self, SegmentStateCheckpointError> {
        if compacted.segment_state_checkpoint.is_some() {
            return Err(SegmentStateCheckpointError::new(
                "compaction already contains a segment-state checkpoint descriptor",
            ));
        }
        compacted.segment_state_checkpoint = Some(SegmentStateCheckpoint {
            version: SEGMENT_STATE_CHECKPOINT_VERSION,
            previous_turn_settings,
            world_state: disposition(world_state.is_some()),
            reference_context: disposition(reference_context.is_some()),
        });

        let mut items = Vec::with_capacity(5);
        items.push(RolloutItem::Compacted(compacted));
        if let Some(world_state) = world_state {
            items.push(RolloutItem::WorldState(world_state));
        }
        if let Some(reference_context) = reference_context {
            items.push(RolloutItem::TurnContext(reference_context));
        }
        items.push(RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(
            thread_settings,
        )));
        items.push(RolloutItem::EventMsg(EventMsg::TokenCount(token_count)));
        validate_checkpoint_items(items.as_slice())?;
        Ok(Self { items })
    }

    /// Returns the canonical ordered rollout records for persistence.
    pub fn items(&self) -> &[RolloutItem] {
        self.items.as_slice()
    }

    /// Revalidates the complete ordered unit at a storage boundary.
    pub fn validate(&self) -> Result<(), SegmentStateCheckpointError> {
        validate_checkpoint_items(self.items.as_slice())
    }

    /// Consumes the checkpoint and returns its canonical ordered rollout records.
    pub fn into_items(self) -> Vec<RolloutItem> {
        self.items
    }
}

/// Result of checking a marked compaction while scanning records from newest to oldest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SegmentStateCheckpointMatch {
    NotPresent,
    Valid,
    Invalid,
}

pub(crate) fn match_segment_state_checkpoint(
    compacted: &CompactedItem,
    newer_items_newest_first: &[RolloutItem],
) -> SegmentStateCheckpointMatch {
    let Some(descriptor) = compacted.segment_state_checkpoint.as_ref() else {
        return SegmentStateCheckpointMatch::NotPresent;
    };
    if validate_compacted(compacted).is_err()
        || descriptor.version != SEGMENT_STATE_CHECKPOINT_VERSION
    {
        return SegmentStateCheckpointMatch::Invalid;
    }

    let mut newer = newer_items_newest_first.iter().rev();
    if matches!(
        descriptor.world_state,
        SegmentStateCheckpointDisposition::Established
    ) && !matches!(newer.next(), Some(RolloutItem::WorldState(item)) if item.full)
    {
        return SegmentStateCheckpointMatch::Invalid;
    }
    if matches!(
        descriptor.reference_context,
        SegmentStateCheckpointDisposition::Established
    ) && !matches!(newer.next(), Some(RolloutItem::TurnContext(_)))
    {
        return SegmentStateCheckpointMatch::Invalid;
    }
    let Some(RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(thread_settings))) =
        newer.next()
    else {
        return SegmentStateCheckpointMatch::Invalid;
    };
    if !complete_thread_settings(&thread_settings.thread_settings)
        || !matches!(
            newer.next(),
            Some(RolloutItem::EventMsg(EventMsg::TokenCount(_)))
        )
    {
        return SegmentStateCheckpointMatch::Invalid;
    }
    SegmentStateCheckpointMatch::Valid
}

/// Returns the descriptor and footer length when the following records satisfy its grammar.
/// The length counts only records after the compaction, excluding any newer rollout tail.
pub fn validated_segment_state_checkpoint<'a>(
    compacted: &'a CompactedItem,
    newer_items_chronological: &[RolloutItem],
) -> Option<(&'a SegmentStateCheckpoint, usize)> {
    let descriptor = compacted.segment_state_checkpoint.as_ref()?;
    if validate_compacted(compacted).is_err() {
        return None;
    }
    let mut index = 0;
    if matches!(
        descriptor.world_state,
        SegmentStateCheckpointDisposition::Established
    ) {
        if !matches!(newer_items_chronological.get(index), Some(RolloutItem::WorldState(item)) if item.full)
        {
            return None;
        }
        index += 1;
    }
    if matches!(
        descriptor.reference_context,
        SegmentStateCheckpointDisposition::Established
    ) {
        if !matches!(
            newer_items_chronological.get(index),
            Some(RolloutItem::TurnContext(_))
        ) {
            return None;
        }
        index += 1;
    }
    let Some(RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(thread_settings))) =
        newer_items_chronological.get(index)
    else {
        return None;
    };
    if !complete_thread_settings(&thread_settings.thread_settings)
        || !matches!(
            newer_items_chronological.get(index + 1),
            Some(RolloutItem::EventMsg(EventMsg::TokenCount(_)))
        )
    {
        return None;
    }
    Some((descriptor, index + 2))
}

fn complete_thread_settings(settings: &codex_protocol::protocol::ThreadSettingsSnapshot) -> bool {
    settings.environments.is_some()
        && settings.workspace_roots.is_some()
        && settings.profile_workspace_roots.is_some()
        && settings.windows_sandbox_level.is_some()
}

fn validate_checkpoint_items(items: &[RolloutItem]) -> Result<(), SegmentStateCheckpointError> {
    let Some(RolloutItem::Compacted(compacted)) = items.first() else {
        return Err(SegmentStateCheckpointError::new(
            "checkpoint must start with a compacted item",
        ));
    };
    validate_compacted(compacted)?;
    let newer_items_newest_first = items[1..].iter().rev().cloned().collect::<Vec<_>>();
    if !matches!(
        match_segment_state_checkpoint(compacted, newer_items_newest_first.as_slice()),
        SegmentStateCheckpointMatch::Valid
    ) {
        return Err(SegmentStateCheckpointError::new(
            "checkpoint state records do not match its descriptor",
        ));
    }

    let descriptor = compacted
        .segment_state_checkpoint
        .as_ref()
        .ok_or_else(|| SegmentStateCheckpointError::new("checkpoint descriptor is missing"))?;
    let expected_len =
        1 + usize::from(matches!(
            descriptor.world_state,
            SegmentStateCheckpointDisposition::Established
        )) + usize::from(matches!(
            descriptor.reference_context,
            SegmentStateCheckpointDisposition::Established
        )) + 2;
    if items.len() != expected_len {
        return Err(SegmentStateCheckpointError::new(
            "checkpoint contains records not declared by its descriptor",
        ));
    }
    Ok(())
}

/// Validates complete ordered records accepted by a segment-rotation storage boundary.
pub fn validate_certified_segment_state_checkpoint(
    items: &[RolloutItem],
) -> Result<(), SegmentStateCheckpointError> {
    validate_checkpoint_items(items)
}

fn validate_compacted(compacted: &CompactedItem) -> Result<(), SegmentStateCheckpointError> {
    let descriptor = compacted
        .segment_state_checkpoint
        .as_ref()
        .ok_or_else(|| SegmentStateCheckpointError::new("checkpoint descriptor is missing"))?;
    if descriptor.version != SEGMENT_STATE_CHECKPOINT_VERSION {
        return Err(SegmentStateCheckpointError::new(
            "checkpoint version is unsupported",
        ));
    }
    if compacted.replacement_history.is_none()
        || compacted.window_number.is_none()
        || compacted.first_window_id.is_none()
        || compacted.window_id.is_none()
    {
        return Err(SegmentStateCheckpointError::new(
            "checkpoint compaction is missing replacement history or window metadata",
        ));
    }
    if !compacted.first_window_id.as_deref().is_some_and(is_uuid_v7)
        || !compacted.window_id.as_deref().is_some_and(is_uuid_v7)
        || compacted
            .previous_window_id
            .as_deref()
            .is_some_and(|id| !is_uuid_v7(id))
    {
        return Err(SegmentStateCheckpointError::new(
            "checkpoint compaction contains invalid window identifiers",
        ));
    }
    Ok(())
}

fn is_uuid_v7(value: &str) -> bool {
    Uuid::parse_str(value)
        .ok()
        .is_some_and(|id| id.get_version() == Some(Version::SortRand))
}

fn disposition(established: bool) -> SegmentStateCheckpointDisposition {
    if established {
        SegmentStateCheckpointDisposition::Established
    } else {
        SegmentStateCheckpointDisposition::Cleared
    }
}

/// Invalid segment-state checkpoint construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentStateCheckpointError {
    message: &'static str,
}

impl SegmentStateCheckpointError {
    fn new(message: &'static str) -> Self {
        Self { message }
    }

    pub fn uncertified() -> Self {
        Self::new("segment rotation requires a certified current-state checkpoint")
    }
}

impl fmt::Display for SegmentStateCheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl Error for SegmentStateCheckpointError {}

#[cfg(test)]
#[path = "segment_checkpoint_tests.rs"]
mod tests;
