//! Projects authenticated canonical output without allocating model-context payloads.
//!
//! Source validation and canonicalization must run before this reader. It validates JSON syntax
//! and record coordinates, but only deserializes payloads used by `project_rollout_line`.

use std::borrow::Cow;
use std::sync::LazyLock;

use codex_app_server_protocol::ThreadHistoryChangeSet;
use codex_app_server_protocol::project_rollout_line;
use codex_protocol::RolloutId;
use codex_protocol::realtime::RealtimeItem;
use codex_rollout::RolloutItem;
use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::io::AsyncBufRead;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;

use super::jsonl_spans::JsonlSpan;
use super::jsonl_spans::JsonlSpanScanner;
use super::line_parser::parse_paginated_rollout_line;
use super::migration_error;
use crate::ThreadStoreResult;
use crate::local::LocalThreadStore;
use crate::local::thread_history;
use crate::local::thread_history::RolloutProjectionStep;

/// Only these canonical events can change the desktop projection. Escaped JSON names take the
/// existing parser; matches in ordinary payload text are harmless extra candidates.
static PROJECTION_CANDIDATES: LazyLock<Result<JsonlSpanScanner, regex::Error>> = LazyLock::new(
    || {
        JsonlSpanScanner::new(
            r#""type"[ \t\r]*:[ \t\r]*"(?:task_started|turn_started|task_complete|turn_complete|turn_aborted|item_completed|realtime_item)"|\\u"#,
        )
    },
);

pub(super) fn candidate_spans(
    bytes: &[u8],
) -> ThreadStoreResult<impl Iterator<Item = JsonlSpan> + '_> {
    Ok(PROJECTION_CANDIDATES
        .as_ref()
        .map_err(migration_error)?
        .scan(bytes))
}

/// Keeps borrowed spans within a bounded buffer ending at a complete physical record.
pub(super) async fn read_canonical_chunk<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    bytes: &mut Vec<u8>,
) -> ThreadStoreResult<bool> {
    bytes.clear();
    (&mut *reader)
        .take(super::PROJECTION_BATCH_BYTES)
        .read_to_end(bytes)
        .await
        .map_err(migration_error)?;
    if bytes.is_empty() {
        return Ok(false);
    }
    if !bytes.ends_with(b"\n") {
        let partial_length = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(bytes.len(), |last| bytes.len() - last - 1);
        let remaining = (super::MAX_ROLLOUT_LINE_BYTES + 1).saturating_sub(partial_length);
        let appended = reader
            .take(remaining as u64)
            .read_until(b'\n', bytes)
            .await
            .map_err(migration_error)?;
        if !bytes.ends_with(b"\n")
            || partial_length.saturating_add(appended) > super::MAX_ROLLOUT_LINE_BYTES
        {
            return Err(migration_error(
                "canonical rollout has an oversized or incomplete record",
            ));
        }
    }
    Ok(true)
}

/// Applies the same ordered records to a physical segment and, when eligible, its unpublished
/// complete root. Replaying final SQLite rows instead would lose cross-segment turn transitions.
pub(super) async fn apply_projection_batch(
    store: &LocalThreadStore,
    rollout_id: RolloutId,
    complete_root: Option<RolloutId>,
    start_offset: u64,
    end_offset: u64,
    batch: Vec<RolloutProjectionStep>,
) -> ThreadStoreResult<()> {
    if let Some(root) = complete_root {
        thread_history::apply_projection(
            store,
            root,
            start_offset,
            end_offset,
            /*initial_ordinal*/ 0,
            batch.clone(),
        )
        .await?;
    }
    thread_history::apply_projection(
        store,
        rollout_id,
        start_offset,
        end_offset,
        /*initial_ordinal*/ 0,
        batch,
    )
    .await
}

/// Coordinates and visible changes from one previously validated canonical record.
pub(super) struct CanonicalProjectionLine<'a> {
    pub(super) timestamp: Cow<'a, str>,
    pub(super) ordinal: u64,
    pub(super) changes: ThreadHistoryChangeSet,
    pub(super) realtime_item: Option<RealtimeItem>,
}

/// Borrows the payload so ignored model-context records never become a JSON value tree.
#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    timestamp: Cow<'a, str>,
    ordinal: u64,
    #[serde(rename = "type", borrow)]
    kind: Cow<'a, str>,
    #[serde(borrow)]
    payload: &'a RawValue,
}

#[derive(Deserialize)]
struct EventKind<'a> {
    #[serde(rename = "type", borrow)]
    kind: Cow<'a, str>,
}

pub(super) fn project_canonical_record(
    bytes: &[u8],
) -> Result<CanonicalProjectionLine<'_>, String> {
    let envelope: Envelope<'_> =
        serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    let needs_projection = match envelope.kind.as_ref() {
        "event_msg" => {
            let event: EventKind<'_> =
                serde_json::from_str(envelope.payload.get()).map_err(|error| error.to_string())?;
            matches!(
                event.kind.as_ref(),
                "task_started"
                    | "turn_started"
                    | "task_complete"
                    | "turn_complete"
                    | "turn_aborted"
                    | "item_completed"
            )
        }
        "realtime_item" => true,
        "session_meta"
        | "rollout_reference"
        | "fork_reference"
        | "response_item"
        | "inter_agent_communication"
        | "inter_agent_communication_metadata"
        | "compacted"
        | "turn_context"
        | "world_state"
        | "security_risk_score" => false,
        _ => true,
    };
    let (changes, realtime_item) = if needs_projection {
        let line = parse_paginated_rollout_line(bytes)?;
        let changes = project_rollout_line(&line);
        let realtime_item = match line.item {
            RolloutItem::RealtimeItem(item) => Some(item),
            _ => None,
        };
        (changes, realtime_item)
    } else {
        (ThreadHistoryChangeSet::default(), None)
    };
    Ok(CanonicalProjectionLine {
        timestamp: envelope.timestamp,
        ordinal: envelope.ordinal,
        changes,
        realtime_item,
    })
}

#[cfg(test)]
#[path = "canonical_projection_tests.rs"]
mod tests;
