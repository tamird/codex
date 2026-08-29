use super::ContextualUserFragment;
use codex_protocol::ThreadId;
use codex_protocol::models::ContentItemKind;
use codex_protocol::protocol::ThreadGoal;

/// Conservative UTF-8 byte ceiling for a complete dynamic goal projection, including wrappers.
pub const MAX_GOAL_CONTEXT_BYTES: usize = 8 * 1024;

/// Explains an outbound omission without changing the stored goal or implying completion.
pub fn goal_objective_omission_notice(goal: &ThreadGoal) -> String {
    format!(
        "The stored goal objective is unchanged but was omitted from this model item because it exceeds the context limit. Do not treat the omission as completion or narrow the goal. Obtain the complete objective from the goal tool if available, or ask the user for a file reference.\nGoal thread: {}\nGoal status: {:?}",
        goal.thread_id, goal.status
    )
}

/// The complete initial user message for a goal supervisor, with no extra assignment wrapper.
pub(crate) struct GoalSupervisorAssignment<'a> {
    pub(crate) parent_thread_id: ThreadId,
    pub(crate) goal: &'a ThreadGoal,
}

impl ContextualUserFragment for GoalSupervisorAssignment<'_> {
    fn content_kind(&self) -> ContentItemKind {
        ContentItemKind("goal_supervisor.assignment".to_string())
    }

    fn role(&self) -> &'static str {
        "user"
    }

    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        ("", "")
    }

    fn body(&self) -> String {
        let parent_thread_id = self.parent_thread_id;
        let render = |objective: &str| {
            format!(
                "# Goal Supervisor Assignment\n\nParent agent id: {parent_thread_id}\n\nActive goal objective:\n\n{objective}\n\nEvaluate whether the parent should continue now, snooze, compact, or mark the goal complete."
            )
        };
        if self.goal.objective.len() <= MAX_GOAL_CONTEXT_BYTES {
            let prompt = render(&self.goal.objective);
            if prompt.len() <= MAX_GOAL_CONTEXT_BYTES {
                return prompt;
            }
        }
        render(&goal_objective_omission_notice(self.goal))
    }
}

#[cfg(test)]
#[path = "goal_context_tests.rs"]
mod tests;
