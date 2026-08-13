use super::*;
use crate::context::world_state::WorldStateSnapshot;
use crate::context_manager::is_user_turn_boundary;
use codex_history::ResponseItemEnvelope;
use codex_protocol::protocol::SessionContextWindow;
use codex_rollout::validated_segment_state_checkpoint;
use uuid::Uuid;

// Return value of `Session::reconstruct_history_from_rollout`, bundling the rebuilt history with
// the resume/fork hydration metadata derived from the same replay.
#[derive(Debug)]
pub(super) struct RolloutReconstruction {
    pub(super) history: Vec<ResponseItemEnvelope>,
    pub(super) previous_turn_settings: Option<PreviousTurnSettings>,
    pub(super) reference_context_item: Option<TurnContextItem>,
    pub(super) world_state_baseline: Option<WorldStateSnapshot>,
    pub(super) window_number: u64,
    pub(super) first_window_id: Option<Uuid>,
    pub(super) previous_window_id: Option<Uuid>,
    pub(super) window_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy)]
struct ReconstructedWindow {
    number: u64,
    first_id: Option<Uuid>,
    previous_id: Option<Uuid>,
    id: Option<Uuid>,
}

#[derive(Debug, Default)]
enum TurnReferenceContextItem {
    /// No `TurnContextItem` has been seen for this replay span yet.
    ///
    /// This differs from `Cleared`: `NeverSet` means there is no evidence this turn ever
    /// established a baseline, while `Cleared` means a baseline existed and a later compaction
    /// invalidated it. Only the latter must emit an explicit clearing segment for resume/fork
    /// hydration.
    #[default]
    NeverSet,
    /// A previously established baseline was invalidated by later compaction.
    Cleared,
    /// The latest baseline established by this replay span.
    Latest(Box<TurnContextItem>),
}

#[derive(Debug, Default)]
struct ActiveReplaySegment<'a> {
    newest_item_index: usize,
    turn_id: Option<String>,
    counts_as_user_turn: bool,
    previous_turn_settings: Option<PreviousTurnSettings>,
    /// Checkpoint value, including an explicit `None` that clears older settings.
    checkpoint_previous_turn_settings: Option<Option<PreviousTurnSettings>>,
    has_segment_state_checkpoint: bool,
    has_context_baseline_after_checkpoint: bool,
    reference_context_item: TurnReferenceContextItem,
    world_state_replay: Vec<&'a RolloutItem>,
    base_replacement_history: Option<&'a [ResponseItemEnvelope]>,
    window: Option<ReconstructedWindow>,
}

impl ActiveReplaySegment<'_> {
    fn new(newest_item_index: usize) -> Self {
        Self {
            newest_item_index,
            ..Default::default()
        }
    }
}

fn turn_ids_are_compatible(active_turn_id: Option<&str>, item_turn_id: Option<&str>) -> bool {
    active_turn_id
        .is_none_or(|turn_id| item_turn_id.is_none_or(|item_turn_id| item_turn_id == turn_id))
}

#[expect(
    clippy::too_many_arguments,
    reason = "segment replay updates distinct reconstruction accumulators from one finalized segment"
)]
fn finalize_active_segment<'a>(
    active_segment: ActiveReplaySegment<'a>,
    base_replacement_history: &mut Option<&'a [ResponseItemEnvelope]>,
    previous_turn_settings: &mut Option<PreviousTurnSettings>,
    previous_turn_settings_resolved: &mut bool,
    reference_context_item: &mut TurnReferenceContextItem,
    world_state_replay: &mut Vec<&'a RolloutItem>,
    window: &mut Option<ReconstructedWindow>,
    pending_rollback_turns: &mut usize,
) {
    // Thread rollback drops the newest surviving real user-message boundaries. In replay, that
    // means skipping the next finalized segments that contain a non-contextual
    // `EventMsg::UserMessage`.
    if *pending_rollback_turns > 0 {
        if active_segment.counts_as_user_turn {
            *pending_rollback_turns -= 1;
        }
        return;
    }

    // Full world-state snapshots are persisted after installing initial context. They still
    // establish a baseline when a child fork removes the parent turn's agent message. Do not
    // count these context-only segments as user turns for rollback, or use a snapshot from
    // before the segment's latest compaction.
    let has_context_baseline = active_segment.counts_as_user_turn
        || active_segment
            .world_state_replay
            .iter()
            .take_while(|item| !matches!(item, RolloutItem::Compacted(_)))
            .any(|item| matches!(item, RolloutItem::WorldState(state) if state.full));
    world_state_replay.extend(active_segment.world_state_replay);

    // A surviving replacement-history checkpoint is a complete history base. Once we
    // know the newest surviving one, older rollout items do not affect rebuilt history.
    if base_replacement_history.is_none()
        && let Some(segment_base_replacement_history) = active_segment.base_replacement_history
    {
        *base_replacement_history = Some(segment_base_replacement_history);
    }

    if window.is_none() {
        *window = active_segment.window;
    }

    // A certified footer owns its saved settings, including an explicit reset. Only a
    // surviving user turn or a full context baseline outside that footer can replace them.
    if !*previous_turn_settings_resolved {
        if has_context_baseline
            && (!active_segment.has_segment_state_checkpoint
                || active_segment.counts_as_user_turn
                || active_segment.has_context_baseline_after_checkpoint)
            && let Some(settings) = active_segment.previous_turn_settings
        {
            *previous_turn_settings = Some(settings);
            *previous_turn_settings_resolved = true;
        } else if let Some(settings) = active_segment.checkpoint_previous_turn_settings {
            *previous_turn_settings = settings;
            *previous_turn_settings_resolved = true;
        }
    }

    // `reference_context_item` comes from the newest surviving context baseline, or
    // from a surviving compaction that explicitly cleared that baseline.
    if matches!(reference_context_item, TurnReferenceContextItem::NeverSet)
        && (has_context_baseline
            || active_segment.has_segment_state_checkpoint
            || matches!(
                active_segment.reference_context_item,
                TurnReferenceContextItem::Cleared
            ))
    {
        *reference_context_item = active_segment.reference_context_item;
    }
}

impl Session {
    pub(super) async fn reconstruct_history_from_rollout(
        &self,
        turn_context: &TurnContext,
        rollout_items: &[RolloutItem],
    ) -> RolloutReconstruction {
        // Replay metadata should already match the shape of the future lazy reverse loader, even
        // while history materialization still uses an eager bridge. Scan newest-to-oldest,
        // stopping once a surviving replacement-history checkpoint and the required resume metadata
        // are both known; then replay only the buffered surviving tail forward to preserve exact
        // history semantics.
        let has_legacy_compaction_without_window_number =
            rollout_items.iter().any(|item| {
                matches!(item, RolloutItem::Compacted(compacted) if compacted.window_number.is_none())
            });
        let initial_window = if has_legacy_compaction_without_window_number {
            None
        } else {
            rollout_items.iter().find_map(|item| match item {
                RolloutItem::SessionMeta(session_meta) => session_meta
                    .meta
                    .context_window
                    .as_ref()
                    .and_then(reconstructed_window_from_session_context_window),
                _ => None,
            })
        };
        let mut base_replacement_history: Option<&[ResponseItemEnvelope]> = None;
        let mut previous_turn_settings = None;
        let mut previous_turn_settings_resolved = false;
        let mut reference_context_item = TurnReferenceContextItem::NeverSet;
        let mut world_state_replay = Vec::new();
        let mut window = None;
        // Rollback is "drop the newest N user turns". While scanning in reverse, that becomes
        // "skip the next N user-turn segments we finalize".
        let mut pending_rollback_turns = 0usize;
        // Borrowed suffix of rollout items newer than the newest surviving replacement-history
        // checkpoint. If no such checkpoint exists, this remains the full rollout.
        let mut rollout_suffix = rollout_items;
        // Reverse replay accumulates rollout items into the newest in-progress turn segment until
        // we hit its matching `TurnStarted`, at which point the segment can be finalized.
        let mut active_segment: Option<ActiveReplaySegment<'_>> = None;

        for (index, item) in rollout_items.iter().enumerate().rev() {
            let mut reached_segment_state_checkpoint = false;
            match item {
                RolloutItem::Compacted(compacted) => {
                    let active_segment =
                        active_segment.get_or_insert_with(|| ActiveReplaySegment::new(index));
                    if let Some((checkpoint, footer_len)) =
                        validated_segment_state_checkpoint(compacted, &rollout_items[index + 1..])
                    {
                        active_segment.has_segment_state_checkpoint = true;
                        // A checkpoint's own full snapshot and context are comparison-state
                        // sidecars, not a newer turn. Limit the candidate baseline to this
                        // surviving segment's tail after the validator's complete footer.
                        let footer_end = index + 1 + footer_len;
                        let segment_end = active_segment.newest_item_index + 1;
                        if footer_end < segment_end {
                            let newer_tail = &rollout_items[footer_end..segment_end];
                            let has_full_snapshot = newer_tail
                                .iter()
                                .rev()
                                .take_while(|item| !matches!(item, RolloutItem::Compacted(_)))
                                .any(|item| matches!(item, RolloutItem::WorldState(state) if state.full));
                            if (active_segment.counts_as_user_turn || has_full_snapshot)
                                && let Some(context) = newer_tail.iter().find_map(|item| {
                                    let RolloutItem::TurnContext(context) = item else {
                                        return None;
                                    };
                                    turn_ids_are_compatible(
                                        active_segment.turn_id.as_deref(),
                                        context.turn_id.as_deref(),
                                    )
                                    .then_some(context)
                                })
                            {
                                active_segment.previous_turn_settings =
                                    Some(PreviousTurnSettings {
                                        model: context.model.clone(),
                                        comp_hash: context.comp_hash.clone(),
                                        realtime_active: context.realtime_active,
                                    });
                                active_segment.has_context_baseline_after_checkpoint = true;
                            }
                        }
                        active_segment.checkpoint_previous_turn_settings =
                            Some(checkpoint.previous_turn_settings.as_ref().map(|settings| {
                                PreviousTurnSettings {
                                    model: settings.model.clone(),
                                    comp_hash: settings.comp_hash.clone(),
                                    realtime_active: settings.realtime_active,
                                }
                            }));
                        reached_segment_state_checkpoint = pending_rollback_turns == 0;
                    }
                    active_segment.world_state_replay.push(item);
                    if active_segment.window.is_none()
                        && let Some(window_number) = compacted.window_number
                    {
                        active_segment.window = Some(ReconstructedWindow {
                            number: window_number,
                            first_id: compacted.first_window_id.as_deref().and_then(parse_uuid_v7),
                            previous_id: compacted
                                .previous_window_id
                                .as_deref()
                                .and_then(parse_uuid_v7),
                            id: compacted.window_id.as_deref().and_then(parse_uuid_v7),
                        });
                    }
                    // Looking backward, compaction clears any older baseline unless a newer
                    // `TurnContextItem` in this same segment has already re-established it.
                    if matches!(
                        active_segment.reference_context_item,
                        TurnReferenceContextItem::NeverSet
                    ) {
                        active_segment.reference_context_item = TurnReferenceContextItem::Cleared;
                    }
                    if active_segment.base_replacement_history.is_none()
                        && let Some(replacement_history) = &compacted.replacement_history
                    {
                        active_segment.base_replacement_history = Some(replacement_history);
                        rollout_suffix = &rollout_items[index + 1..];
                        // A newer rollback still applies to this complete history base. Continue
                        // metadata replay, but retain the checkpoint history while doing so.
                        if pending_rollback_turns > 0 && base_replacement_history.is_none() {
                            base_replacement_history = Some(replacement_history);
                        }
                    }
                }
                RolloutItem::EventMsg(EventMsg::ThreadRolledBack(rollback)) => {
                    pending_rollback_turns = pending_rollback_turns
                        .saturating_add(usize::try_from(rollback.num_turns).unwrap_or(usize::MAX));
                }
                RolloutItem::EventMsg(EventMsg::TurnComplete(event)) => {
                    let active_segment =
                        active_segment.get_or_insert_with(|| ActiveReplaySegment::new(index));
                    // Reverse replay often sees `TurnComplete` before any turn-scoped metadata.
                    // Capture the turn id early so later `TurnContext` / abort items can match it.
                    if active_segment.turn_id.is_none() {
                        active_segment.turn_id = Some(event.turn_id.clone());
                    }
                }
                RolloutItem::EventMsg(EventMsg::TurnAborted(event)) => {
                    if let Some(active_segment) = active_segment.as_mut() {
                        if active_segment.turn_id.is_none()
                            && let Some(turn_id) = &event.turn_id
                        {
                            active_segment.turn_id = Some(turn_id.clone());
                        }
                    } else if let Some(turn_id) = &event.turn_id {
                        active_segment = Some(ActiveReplaySegment {
                            turn_id: Some(turn_id.clone()),
                            ..ActiveReplaySegment::new(index)
                        });
                    }
                }
                RolloutItem::EventMsg(EventMsg::UserMessage(_)) => {
                    let active_segment =
                        active_segment.get_or_insert_with(|| ActiveReplaySegment::new(index));
                    active_segment.counts_as_user_turn = true;
                }
                RolloutItem::TurnContext(ctx) => {
                    let active_segment =
                        active_segment.get_or_insert_with(|| ActiveReplaySegment::new(index));
                    // `TurnContextItem` can attach metadata to an existing segment, but only a
                    // real `UserMessage` event should make the segment count as a user turn.
                    if active_segment.turn_id.is_none() {
                        active_segment.turn_id = ctx.turn_id.clone();
                    }
                    if turn_ids_are_compatible(
                        active_segment.turn_id.as_deref(),
                        ctx.turn_id.as_deref(),
                    ) {
                        active_segment.previous_turn_settings = Some(PreviousTurnSettings {
                            model: ctx.model.clone(),
                            comp_hash: ctx.comp_hash.clone(),
                            realtime_active: ctx.realtime_active,
                        });
                        if matches!(
                            active_segment.reference_context_item,
                            TurnReferenceContextItem::NeverSet
                        ) {
                            active_segment.reference_context_item =
                                TurnReferenceContextItem::Latest(Box::new(ctx.clone()));
                        }
                    }
                }
                RolloutItem::WorldState(_) => {
                    let active_segment =
                        active_segment.get_or_insert_with(|| ActiveReplaySegment::new(index));
                    active_segment.world_state_replay.push(item);
                }
                RolloutItem::EventMsg(EventMsg::TurnStarted(event)) => {
                    // `TurnStarted` is the oldest boundary of the active reverse segment.
                    if active_segment.as_ref().is_some_and(|active_segment| {
                        turn_ids_are_compatible(
                            active_segment.turn_id.as_deref(),
                            Some(event.turn_id.as_str()),
                        )
                    }) && let Some(active_segment) = active_segment.take()
                    {
                        finalize_active_segment(
                            active_segment,
                            &mut base_replacement_history,
                            &mut previous_turn_settings,
                            &mut previous_turn_settings_resolved,
                            &mut reference_context_item,
                            &mut world_state_replay,
                            &mut window,
                            &mut pending_rollback_turns,
                        );
                    }
                }
                RolloutItem::ResponseItem(response_item) => {
                    let active_segment =
                        active_segment.get_or_insert_with(|| ActiveReplaySegment::new(index));
                    active_segment.counts_as_user_turn |=
                        is_user_turn_boundary(&response_item.item);
                }
                RolloutItem::InterAgentCommunication(_) => {
                    let active_segment =
                        active_segment.get_or_insert_with(|| ActiveReplaySegment::new(index));
                    active_segment.counts_as_user_turn = true;
                }
                RolloutItem::EventMsg(_)
                | RolloutItem::RolloutReference(_)
                | RolloutItem::SessionMeta(_)
                | RolloutItem::RealtimeItem(_)
                | RolloutItem::SecurityRiskScore(_)
                | RolloutItem::InterAgentCommunicationMetadata { .. } => {}
            }

            if reached_segment_state_checkpoint {
                if let Some(active_segment) = active_segment.take() {
                    finalize_active_segment(
                        active_segment,
                        &mut base_replacement_history,
                        &mut previous_turn_settings,
                        &mut previous_turn_settings_resolved,
                        &mut reference_context_item,
                        &mut world_state_replay,
                        &mut window,
                        &mut pending_rollback_turns,
                    );
                }
                break;
            }

            if base_replacement_history.is_some()
                && previous_turn_settings_resolved
                && !matches!(reference_context_item, TurnReferenceContextItem::NeverSet)
            {
                // At this point we have both eager resume metadata values and the replacement-
                // history base for the surviving tail, so older rollout items cannot affect this
                // result.
                break;
            }
        }

        if let Some(active_segment) = active_segment.take() {
            finalize_active_segment(
                active_segment,
                &mut base_replacement_history,
                &mut previous_turn_settings,
                &mut previous_turn_settings_resolved,
                &mut reference_context_item,
                &mut world_state_replay,
                &mut window,
                &mut pending_rollback_turns,
            );
        }

        let fallback_window_number = u64::try_from(
            rollout_items
                .iter()
                .filter(|item| matches!(item, RolloutItem::Compacted(_)))
                .count(),
        )
        .unwrap_or(u64::MAX);

        let mut history = ContextManager::new();
        let mut saw_legacy_compaction_without_replacement_history = false;
        if let Some(base_replacement_history) = base_replacement_history {
            history.replace_annotated(base_replacement_history.to_vec());
        }
        // Materialize exact history semantics from the replay-derived suffix. The eventual lazy
        // design should keep this same replay shape, but drive it from a resumable reverse source
        // instead of an eagerly loaded `&[RolloutItem]`.
        for item in rollout_suffix {
            match item {
                RolloutItem::ResponseItem(response_item) => {
                    history.record_annotated_items(
                        std::slice::from_ref(response_item),
                        turn_context.model_info().truncation_policy.into(),
                    );
                }
                RolloutItem::InterAgentCommunication(communication) => {
                    let response_item = communication.to_model_input_item();
                    history.record_items(
                        std::iter::once(&response_item),
                        turn_context.model_info().truncation_policy.into(),
                    );
                }
                RolloutItem::InterAgentCommunicationMetadata { .. } => {}
                RolloutItem::Compacted(compacted) => {
                    if let Some(replacement_history) = &compacted.replacement_history {
                        // This should actually never happen, because the reverse loop above (to build rollout_suffix)
                        // should stop before any compaction that has Some replacement_history
                        history.replace_annotated(replacement_history.clone());
                    } else {
                        saw_legacy_compaction_without_replacement_history = true;
                        // Legacy rollouts without `replacement_history` should rebuild the
                        // historical TurnContext at the correct insertion point from persisted
                        // `TurnContextItem`s. These are rare enough that we currently just clear
                        // `reference_context_item`, reinject canonical context at the end of the
                        // resumed conversation, and accept the temporary out-of-distribution
                        // prompt shape.
                        // TODO(ccunningham): if we drop support for None replacement_history compaction items,
                        // we can get rid of this second loop entirely and just build `history` directly in the first loop.
                        let user_messages =
                            compact::collect_annotated_user_messages(history.annotated_items());
                        let rebuilt = compact::build_compacted_history(
                            Vec::new(),
                            &user_messages,
                            &compacted.message,
                        );
                        history.replace_annotated(rebuilt);
                    }
                }
                RolloutItem::EventMsg(EventMsg::ThreadRolledBack(rollback)) => {
                    history.drop_last_n_user_turns(rollback.num_turns);
                }
                RolloutItem::EventMsg(_)
                | RolloutItem::RolloutReference(_)
                | RolloutItem::TurnContext(_)
                | RolloutItem::RealtimeItem(_)
                | RolloutItem::WorldState(_)
                | RolloutItem::SecurityRiskScore(_)
                | RolloutItem::SessionMeta(_) => {}
            }
        }

        let reference_context_item = match reference_context_item {
            TurnReferenceContextItem::NeverSet | TurnReferenceContextItem::Cleared => None,
            TurnReferenceContextItem::Latest(turn_reference_context_item) => {
                Some(*turn_reference_context_item)
            }
        };
        let reference_context_item = if saw_legacy_compaction_without_replacement_history {
            None
        } else {
            reference_context_item
        };

        // Segments and their contents were collected newest-first; replay the surviving records
        // chronologically so compaction resets and merge patches have their original meaning.
        world_state_replay.reverse();
        let mut world_state_baseline: Option<WorldStateSnapshot> = None;
        for item in world_state_replay {
            match item {
                RolloutItem::Compacted(_) => world_state_baseline = None,
                RolloutItem::WorldState(world_state) if world_state.full => {
                    world_state_baseline = Some(WorldStateSnapshot::from(&world_state.state));
                }
                RolloutItem::WorldState(world_state) => {
                    let Some(baseline) = world_state_baseline.as_mut() else {
                        tracing::warn!("ignored world-state patch without a full snapshot");
                        continue;
                    };
                    baseline.apply_merge_patch(&world_state.state);
                }
                RolloutItem::SessionMeta(_)
                | RolloutItem::RolloutReference(_)
                | RolloutItem::ResponseItem(_)
                | RolloutItem::InterAgentCommunication(_)
                | RolloutItem::InterAgentCommunicationMetadata { .. }
                | RolloutItem::TurnContext(_)
                | RolloutItem::RealtimeItem(_)
                | RolloutItem::SecurityRiskScore(_)
                | RolloutItem::EventMsg(_) => {
                    unreachable!("only world-state replay items are collected")
                }
            }
        }

        let window = window.or(initial_window).unwrap_or(ReconstructedWindow {
            number: fallback_window_number,
            first_id: None,
            previous_id: None,
            id: None,
        });
        RolloutReconstruction {
            history: history.into_annotated_items(),
            previous_turn_settings,
            reference_context_item,
            world_state_baseline,
            window_number: window.number,
            first_window_id: window.first_id,
            previous_window_id: window.previous_id,
            window_id: window.id,
        }
    }
}

fn parse_uuid_v7(value: &str) -> Option<Uuid> {
    Uuid::parse_str(value)
        .ok()
        .filter(|uuid| uuid.get_version_num() == 7)
}

fn reconstructed_window_from_session_context_window(
    context_window: &SessionContextWindow,
) -> Option<ReconstructedWindow> {
    let id = parse_uuid_v7(&context_window.window_id)?;
    Some(ReconstructedWindow {
        number: 0,
        first_id: Some(id),
        previous_id: None,
        id: Some(id),
    })
}
