//! Reuses exactly repeated turn configuration while retaining per-record coordinates and IDs.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use codex_protocol::protocol::TurnContextItem;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use serde::Deserialize;
use serde::Serialize;
use serde_json::value::RawValue;

use super::line_parser;
use super::migration_error;
use crate::ThreadStoreResult;

const CACHE_BUDGET: usize = 16 * 1024 * 1024;

/// Strict source inspection and normalized Legacy replay must not share acceptance results.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub(super) enum DecodeMode {
    Legacy,
    Paginated,
}

/// Unknown outer fields fall back to the existing decoder, including ignored oversized numbers.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope<'a> {
    #[serde(borrow)]
    timestamp: Cow<'a, str>,
    #[serde(default)]
    ordinal: Option<u64>,
    #[serde(rename = "type")]
    _kind: ContextKind,
    #[serde(borrow)]
    payload: &'a RawValue,
}

/// Reject other record kinds before inspecting their payload, accepting only a JSON string.
enum ContextKind {
    TurnContext,
}

impl<'de> Deserialize<'de> for ContextKind {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match Cow::<'de, str>::deserialize(deserializer)?.as_ref() {
            "turn_context" => Ok(Self::TurnContext),
            _ => Err(serde::de::Error::custom("not a turn_context record")),
        }
    }
}

#[derive(Deserialize)]
struct ContextHeader<'a> {
    #[serde(borrow)]
    turn_id: &'a RawValue,
}

/// The exact payload bytes, with only the validated turn_id value replaced by null.
#[derive(Eq, Hash, PartialEq)]
struct ConfigurationKey {
    mode: DecodeMode,
    bytes: Vec<u8>,
}

/// Both representations have passed the existing decoder with turn_id set to None.
struct Configuration {
    context: TurnContextItem,
    canonical_payload: Vec<u8>,
}

/// A disposable cache with a conservative allocation charge, not a process-RSS limit.
pub(super) struct TurnContextCache {
    entries: HashMap<ConfigurationKey, Arc<Configuration>>,
    charged_bytes: usize,
    budget: usize,
}

impl Default for TurnContextCache {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            charged_bytes: 0,
            budget: CACHE_BUDGET,
        }
    }
}

/// A verified configuration plus the fields that vary between physical rollout records.
/// Consume this immediately; retaining records across eviction would retain uncharged Arcs.
pub(super) struct PreparedTurnContext {
    timestamp: String,
    ordinal: Option<u64>,
    turn_id: String,
    configuration: Arc<Configuration>,
}

impl TurnContextCache {
    #[cfg(test)]
    pub(super) fn disabled() -> Self {
        Self {
            budget: 0,
            ..Self::default()
        }
    }

    pub(super) fn parse(&mut self, bytes: &[u8], mode: DecodeMode) -> Option<PreparedTurnContext> {
        if self.budget == 0 {
            return None;
        }
        let envelope: Envelope<'_> = serde_json::from_slice(bytes).ok()?;
        let payload = envelope.payload.get();
        let header: ContextHeader<'_> = serde_json::from_str(payload).ok()?;
        let turn_id = serde_json::from_str::<String>(header.turn_id.get()).ok()?;
        let token = header.turn_id.get();
        let start = token.as_ptr() as usize - payload.as_ptr() as usize;
        let mut configuration_bytes = Vec::with_capacity(payload.len() - token.len() + 4);
        configuration_bytes.extend_from_slice(&payload.as_bytes()[..start]);
        configuration_bytes.extend_from_slice(b"null");
        configuration_bytes.extend_from_slice(&payload.as_bytes()[start + token.len()..]);
        let key = ConfigurationKey {
            mode,
            bytes: configuration_bytes,
        };
        let configuration = if let Some(configuration) = self.entries.get(&key) {
            Arc::clone(configuration)
        } else {
            let payload_start = payload.as_ptr() as usize - bytes.as_ptr() as usize;
            let mut reduced = Vec::with_capacity(bytes.len() - payload.len() + key.bytes.len());
            reduced.extend_from_slice(&bytes[..payload_start]);
            reduced.extend_from_slice(&key.bytes);
            reduced.extend_from_slice(&bytes[payload_start + payload.len()..]);
            let line = match mode {
                DecodeMode::Legacy => line_parser::parse_legacy_rollout_line(&reduced).ok()??,
                DecodeMode::Paginated => {
                    line_parser::parse_paginated_rollout_line(&reduced).ok()?
                }
            };
            let RolloutItem::TurnContext(context) = line.item else {
                return None;
            };
            if context.turn_id.is_some() {
                return None;
            }
            let canonical_payload = serde_json::to_vec(&context).ok()?;
            let charge = key
                .bytes
                .len()
                .saturating_mul(3)
                .saturating_add(canonical_payload.len());
            if charge > self.budget {
                return None;
            }
            if self.charged_bytes.saturating_add(charge) > self.budget {
                self.entries.clear();
                self.charged_bytes = 0;
            }
            let configuration = Arc::new(Configuration {
                context,
                canonical_payload,
            });
            self.entries.insert(key, Arc::clone(&configuration));
            self.charged_bytes += charge;
            configuration
        };
        Some(PreparedTurnContext {
            timestamp: envelope.timestamp.into_owned(),
            ordinal: envelope.ordinal,
            turn_id,
            configuration,
        })
    }
}

impl PreparedTurnContext {
    pub(super) fn rollout_line(&self) -> RolloutLine {
        let mut context = self.configuration.context.clone();
        context.turn_id = Some(self.turn_id.clone());
        RolloutLine {
            timestamp: self.timestamp.clone(),
            ordinal: self.ordinal,
            item: RolloutItem::TurnContext(context),
        }
    }

    pub(super) fn canonical_record(&self, ordinal: u64) -> ThreadStoreResult<Vec<u8>> {
        #[derive(Serialize)]
        struct Head<'a> {
            timestamp: &'a str,
            ordinal: u64,
        }
        let mut result = serde_json::to_vec(&Head {
            timestamp: &self.timestamp,
            ordinal,
        })
        .map_err(migration_error)?;
        if result.pop() != Some(b'}') || self.configuration.canonical_payload.first() != Some(&b'{')
        {
            return Err(migration_error(
                "cached turn configuration is not a JSON object",
            ));
        }
        result.extend_from_slice(b",\"type\":\"turn_context\",\"payload\":{\"turn_id\":");
        serde_json::to_writer(&mut result, &self.turn_id).map_err(migration_error)?;
        result.push(b',');
        result.extend_from_slice(&self.configuration.canonical_payload[1..]);
        result.push(b'}');
        Ok(result)
    }
}

#[cfg(test)]
#[path = "turn_context_cache_tests.rs"]
mod tests;
