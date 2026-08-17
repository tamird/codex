use pretty_assertions::assert_eq;

use super::JsonlSpan;
use super::JsonlSpanKind;
use super::JsonlSpanScanner;

#[test]
fn candidate_matches_expand_to_records_and_preserve_every_byte() {
    let bytes = b"ordinary\r\ninteresting one\nordinary\ninteresting two\npartial";
    let scanner = JsonlSpanScanner::new("interesting").expect("regex");
    let spans = scanner.scan(bytes).collect::<Vec<_>>();
    let expected = [
        (JsonlSpanKind::Copy, &b"ordinary\r\n"[..], 1),
        (JsonlSpanKind::Candidate, &b"interesting one\n"[..], 1),
        (JsonlSpanKind::Copy, &b"ordinary\n"[..], 1),
        (JsonlSpanKind::Candidate, &b"interesting two\n"[..], 1),
        (JsonlSpanKind::Copy, &b"partial"[..], 0),
    ];
    assert_eq!(
        spans
            .iter()
            .map(|span| (span.kind, &bytes[span.range.clone()], span.newline_count))
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(
        spans
            .iter()
            .flat_map(|span| bytes[span.range.clone()].iter().copied())
            .collect::<Vec<_>>(),
        bytes
    );
}

#[test]
fn many_matches_in_one_record_do_not_duplicate_it() {
    let scanner = JsonlSpanScanner::new("hit").expect("regex");
    let bytes = b"hit hit hit\nhit\n";
    assert_eq!(
        scanner.scan(bytes).collect::<Vec<_>>(),
        vec![
            JsonlSpan {
                kind: JsonlSpanKind::Candidate,
                range: 0..12,
                newline_count: 1
            },
            JsonlSpan {
                kind: JsonlSpanKind::Candidate,
                range: 12..16,
                newline_count: 1
            },
        ]
    );
}

#[test]
fn multiline_matches_and_partial_eof_keep_their_boundaries() {
    let scanner = JsonlSpanScanner::new("one\\ntwo").expect("regex");
    let bytes = b"before\none\ntwo rest\nafter";
    let spans = scanner.scan(bytes).collect::<Vec<_>>();
    assert_eq!(
        spans[1],
        JsonlSpan {
            kind: JsonlSpanKind::Candidate,
            range: 7..20,
            newline_count: 2
        }
    );
    assert_eq!(spans.last().expect("last").range.end, bytes.len());
    assert_eq!(
        JsonlSpanScanner::new("$")
            .expect("regex")
            .scan(b"partial")
            .collect::<Vec<_>>(),
        vec![JsonlSpan {
            kind: JsonlSpanKind::Candidate,
            range: 0..7,
            newline_count: 0
        }]
    );
    assert!(scanner.scan(b"").next().is_none());
}
