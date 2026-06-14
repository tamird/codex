use codex_protocol::ThreadId;

use crate::client::ResponseContinuation;
use crate::state::McpToolSnapshot;

#[derive(Clone, Default)]
pub(crate) struct InheritedThreadState {
    prompt_cache_key: Option<ThreadId>,
    response_continuation: Option<ResponseContinuation>,
    mcp_tool_snapshot: Option<McpToolSnapshot>,
}

impl InheritedThreadState {
    pub(crate) fn builder() -> InheritedThreadStateBuilder {
        InheritedThreadStateBuilder::default()
    }

    pub(crate) fn prompt_cache_key(&self) -> Option<ThreadId> {
        self.prompt_cache_key
    }

    pub(crate) fn response_continuation(&self) -> Option<ResponseContinuation> {
        self.response_continuation.clone()
    }

    pub(crate) fn mcp_tool_snapshot(&self) -> Option<McpToolSnapshot> {
        self.mcp_tool_snapshot.clone()
    }
}

#[derive(Default)]
pub(crate) struct InheritedThreadStateBuilder {
    prompt_cache_key: Option<ThreadId>,
    response_continuation: Option<ResponseContinuation>,
    mcp_tool_snapshot: Option<McpToolSnapshot>,
}

impl InheritedThreadStateBuilder {
    pub(crate) fn prompt_cache_key(mut self, prompt_cache_key: Option<ThreadId>) -> Self {
        self.prompt_cache_key = prompt_cache_key;
        self
    }

    pub(crate) fn response_continuation(
        mut self,
        response_continuation: Option<ResponseContinuation>,
    ) -> Self {
        self.response_continuation = response_continuation;
        self
    }

    pub(crate) fn mcp_tool_snapshot(mut self, mcp_tool_snapshot: Option<McpToolSnapshot>) -> Self {
        self.mcp_tool_snapshot = mcp_tool_snapshot;
        self
    }

    pub(crate) fn build(self) -> InheritedThreadState {
        InheritedThreadState {
            prompt_cache_key: self.prompt_cache_key,
            response_continuation: self.response_continuation,
            mcp_tool_snapshot: self.mcp_tool_snapshot,
        }
    }
}
