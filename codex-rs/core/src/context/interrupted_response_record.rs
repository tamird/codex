use super::ContextualUserFragment;
use codex_protocol::models::ContentItemKind;

/// Bounded factual record appended after a provider response is interrupted and made recoverable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InterruptedResponseRecord;

impl ContextualUserFragment for InterruptedResponseRecord {
    fn content_kind(&self) -> ContentItemKind {
        ContentItemKind("generic.interrupted_response".to_string())
    }

    fn role(&self) -> &'static str {
        "developer"
    }

    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        ("<interrupted_response>", "</interrupted_response>")
    }

    fn body(&self) -> String {
        "\nThe preceding provider response ended before it completed. Any incomplete tool call recorded with a failure output was not executed. No unfinished provider-managed operation should be assumed successful.\n".to_string()
    }
}
