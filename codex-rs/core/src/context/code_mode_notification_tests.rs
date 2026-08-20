use super::*;
use codex_protocol::ResponseItemId;
use pretty_assertions::assert_eq;

#[test]
fn notification_keeps_the_longest_prefix_within_its_serialized_budget() {
    let origin = CodeModeNotificationOrigin {
        source_item_id: ResponseItemId::from_server("ctco_source".to_string()),
        call_id: "call-1".to_string(),
        cell_id: "1".to_string(),
    };
    let escaped = [1_u8; 2048];
    let mut unicode = [0_u8; MAX_FRAGMENT_BYTES];
    for chunk in unicode.chunks_exact_mut(4) {
        chunk.copy_from_slice("🦀".as_bytes());
    }
    for bytes in [escaped.as_slice(), unicode.as_slice()] {
        let output = std::str::from_utf8(bytes).unwrap();
        let rendered = CodeModeNotification {
            origin: &origin,
            output,
        }
        .render();
        assert!(rendered.len() <= MAX_FRAGMENT_BYTES);
        let (start, end) = CodeModeNotification::type_markers();
        let json = rendered
            .strip_prefix(start)
            .unwrap()
            .strip_prefix(EXPLANATION)
            .unwrap()
            .strip_suffix(end)
            .unwrap()
            .trim_end();
        let mut payload: serde_json::Value = serde_json::from_str(json).unwrap();
        let retained = payload["output"].as_str().unwrap();
        assert!(!retained.is_empty());
        assert!(output.starts_with(retained));
        assert_eq!(payload["truncated"], true);
        let next = output
            .get(retained.len()..)
            .unwrap()
            .chars()
            .next()
            .unwrap();
        payload["output"] = format!("{retained}{next}").into();
        assert!(format!("{start}{EXPLANATION}{payload}\n{end}").len() > MAX_FRAGMENT_BYTES);
    }
}
