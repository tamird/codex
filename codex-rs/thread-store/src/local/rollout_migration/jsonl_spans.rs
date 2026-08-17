//! Locates candidate JSONL records without deserializing the intervening bytes.

use std::ops::Range;

use regex::bytes::Regex;

/// Whether a caller can retain a range or must inspect its records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum JsonlSpanKind {
    /// No record in this range matched the caller's conservative candidate expression.
    Copy,
    /// At least one match touches these complete record boundaries.
    Candidate,
}

/// A borrowed input range. An unterminated final record remains included in its range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct JsonlSpan {
    pub(super) kind: JsonlSpanKind,
    /// Offsets in the exact input slice passed to `JsonlSpanScanner::scan`.
    pub(super) range: Range<usize>,
    /// Number of LF bytes, excluding an unterminated final record.
    pub(super) newline_count: usize,
}

/// Compiles a conservative candidate expression once and scans borrowed byte buffers.
///
/// The caller defines which records require inspection. Matches inside payload strings are
/// harmless false positives. A Copy range is not a claim that its JSON is valid or that its
/// ordinals need no adjustment; those decisions belong to the caller's rewrite plan.
pub(super) struct JsonlSpanScanner {
    candidates: Regex,
}

impl JsonlSpanScanner {
    pub(super) fn new(candidate_pattern: &str) -> Result<Self, regex::Error> {
        Ok(Self {
            candidates: Regex::new(candidate_pattern)?,
        })
    }

    /// Partitions `bytes` without allocations or parsing. Buffer boundaries must coincide with
    /// physical record boundaries, except that the last buffer may end in a partial record.
    pub(super) fn scan<'a>(&'a self, bytes: &'a [u8]) -> impl Iterator<Item = JsonlSpan> + 'a {
        JsonlSpans {
            scanner: self,
            bytes,
            position: 0,
            pending: None,
        }
    }
}

/// Retains only the next candidate range, never the payloads or all matches in a large record.
struct JsonlSpans<'a> {
    scanner: &'a JsonlSpanScanner,
    bytes: &'a [u8],
    position: usize,
    pending: Option<Range<usize>>,
}

impl JsonlSpans<'_> {
    fn emit(&mut self, kind: JsonlSpanKind, range: Range<usize>) -> JsonlSpan {
        self.position = range.end;
        JsonlSpan {
            kind,
            newline_count: self.bytes[range.clone()]
                .iter()
                .filter(|byte| **byte == b'\n')
                .count(),
            range,
        }
    }
}

impl Iterator for JsonlSpans<'_> {
    type Item = JsonlSpan;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(range) = self.pending.take() {
            return Some(self.emit(JsonlSpanKind::Candidate, range));
        }
        if self.position == self.bytes.len() {
            return None;
        }
        let Some(found) = self.scanner.candidates.find_at(self.bytes, self.position) else {
            return Some(self.emit(JsonlSpanKind::Copy, self.position..self.bytes.len()));
        };
        let start = self.bytes[self.position..found.start()]
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(self.position, |offset| self.position + offset + 1);
        let last = found.end().saturating_sub(1).max(found.start());
        let end = self.bytes[last..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(self.bytes.len(), |offset| last + offset + 1);
        if start == end {
            return Some(self.emit(JsonlSpanKind::Copy, self.position..self.bytes.len()));
        }
        if start > self.position {
            self.pending = Some(start..end);
            return Some(self.emit(JsonlSpanKind::Copy, self.position..start));
        }
        Some(self.emit(JsonlSpanKind::Candidate, start..end))
    }
}

#[cfg(test)]
#[path = "jsonl_spans_tests.rs"]
mod tests;
