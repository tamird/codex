use super::*;
use codex_protocol::protocol::ThreadGoalStatus;
use pretty_assertions::assert_eq;

#[test]
fn supervisor_assignment_bounds_the_complete_message_and_preserves_the_goal() {
    let parent_thread_id = ThreadId::new();
    let mut goal = ThreadGoal {
        thread_id: parent_thread_id,
        objective: "complete objective".to_string(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: 1,
        updated_at: 1,
    };
    let normal = GoalSupervisorAssignment {
        parent_thread_id,
        goal: &goal,
    }
    .render();
    assert_eq!(
        normal,
        format!(
            "# Goal Supervisor Assignment\n\nParent agent id: {parent_thread_id}\n\nActive goal objective:\n\ncomplete objective\n\nEvaluate whether the parent should continue now, snooze, compact, or mark the goal complete."
        )
    );
    let wrapper_bytes = normal.len() - goal.objective.len();
    goal.objective = "x".repeat(MAX_GOAL_CONTEXT_BYTES - wrapper_bytes);
    let boundary = GoalSupervisorAssignment {
        parent_thread_id,
        goal: &goal,
    }
    .render();
    assert_eq!(boundary.len(), MAX_GOAL_CONTEXT_BYTES);
    assert!(boundary.contains(&goal.objective));

    goal.objective.push('x');
    let stored = goal.clone();
    let bounded = GoalSupervisorAssignment {
        parent_thread_id,
        goal: &goal,
    }
    .render();
    assert!(bounded.len() <= MAX_GOAL_CONTEXT_BYTES);
    assert!(bounded.contains(&goal_objective_omission_notice(&goal)));
    assert!(!bounded.contains(&goal.objective));
    assert_eq!(goal, stored);
}
