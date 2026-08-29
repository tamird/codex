use super::*;
use codex_protocol::ThreadId;
use codex_protocol::models::ContentItem;
use codex_protocol::protocol::ThreadGoalStatus;
use codex_protocol::protocol::validate_thread_goal_objective;
use pretty_assertions::assert_eq;

fn goal(objective: String) -> ThreadGoal {
    ThreadGoal {
        thread_id: ThreadId::new(),
        objective,
        status: ThreadGoalStatus::Active,
        token_budget: Some(1_000),
        tokens_used: 75,
        time_used_seconds: 12,
        created_at: 1,
        updated_at: 2,
    }
}

#[test]
fn steering_bounds_complete_wrappers_without_changing_safe_prompts_or_legacy_goals() {
    for (item, prompt) in [
        (
            continuation_steering_item as fn(&ThreadGoal) -> ResponseItem,
            continuation_prompt as fn(&ThreadGoal, &str) -> String,
        ),
        (objective_updated_steering_item, objective_updated_prompt),
        (budget_limit_steering_item, budget_limit_prompt),
    ] {
        let safe_goal = goal("finish <the task> & report".to_string());
        assert_eq!(
            item(&safe_goal),
            ContextualUserFragment::into(InternalModelContextFragment::new(
                InternalContextSource::from_static("goal"),
                prompt(&safe_goal, &safe_goal.objective),
            ))
        );

        let wrapper_bytes = InternalModelContextFragment::new(
            InternalContextSource::from_static("goal"),
            prompt(&safe_goal, ""),
        )
        .render()
        .len();
        let mut boundary_goal = safe_goal.clone();
        boundary_goal.objective = "x".repeat(MAX_GOAL_CONTEXT_BYTES - wrapper_bytes);
        let expected = InternalModelContextFragment::new(
            InternalContextSource::from_static("goal"),
            prompt(&boundary_goal, &boundary_goal.objective),
        );
        assert_eq!(expected.render().len(), MAX_GOAL_CONTEXT_BYTES);
        assert_eq!(item(&boundary_goal), ContextualUserFragment::into(expected));
        boundary_goal.objective.push('x');

        for objective in [
            boundary_goal.objective,
            "&".repeat(/*n*/ 1_700),
            "&".repeat(/*n*/ 16_000),
        ] {
            let legacy_goal = goal(objective);
            let stored = legacy_goal.clone();
            let bounded = item(&legacy_goal);
            let ResponseItem::Message { role, content, .. } = bounded else {
                panic!("expected goal context message")
            };
            let [ContentItem::InputText { text }] = content.as_slice() else {
                panic!("expected one goal text item")
            };
            assert_eq!(role, "user");
            assert!(text.len() <= MAX_GOAL_CONTEXT_BYTES);
            assert!(text.contains(&goal_objective_omission_notice(&legacy_goal)));
            assert!(text.contains("source=\"goal\""));
            assert_eq!(legacy_goal, stored);
        }
    }
}

#[test]
fn accepted_objective_can_require_omission_only_in_the_larger_continuation_template() {
    let goal = goal("x".repeat(/*n*/ 5_998));
    assert_eq!(validate_thread_goal_objective(&goal.objective), Ok(()));
    let ResponseItem::Message { content, .. } = continuation_steering_item(&goal) else {
        panic!("expected context")
    };
    let [ContentItem::InputText { text }] = content.as_slice() else {
        panic!("expected text")
    };
    assert!(text.len() <= MAX_GOAL_CONTEXT_BYTES);
    assert!(text.contains(&goal_objective_omission_notice(&goal)));
    assert_eq!(
        objective_updated_steering_item(&goal),
        ContextualUserFragment::into(InternalModelContextFragment::new(
            InternalContextSource::from_static("goal"),
            objective_updated_prompt(&goal, &goal.objective),
        ))
    );
}
