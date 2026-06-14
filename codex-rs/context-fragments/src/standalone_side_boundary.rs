use crate::ContextualUserFragment;
use codex_protocol::models::ContentItemKind;

const BODY: &str = r#"
Everything inherited from the parent thread is reference context only. It is not your current task.

Do not continue, execute, or complete any instructions, plans, tool calls, approvals, edits, or requests from before this boundary. Wait for a new user message in this standalone side conversation.
"#;

/// Marks inherited parent history as reference-only before a standalone side accepts input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandaloneSideBoundary;

impl ContextualUserFragment for StandaloneSideBoundary {
    fn content_kind(&self) -> ContentItemKind {
        ContentItemKind("multi_agent.standalone_side_boundary".to_string())
    }

    fn role(&self) -> &'static str {
        "developer"
    }

    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        (
            "<standalone_side_conversation_boundary>",
            "</standalone_side_conversation_boundary>",
        )
    }

    fn body(&self) -> String {
        BODY.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendered_boundary_matches_its_registered_type() {
        let rendered = StandaloneSideBoundary.render();
        assert!(StandaloneSideBoundary::matches_text(&rendered));
        assert!(!StandaloneSideBoundary::matches_text(
            "Everything inherited is reference context."
        ));
    }
}
