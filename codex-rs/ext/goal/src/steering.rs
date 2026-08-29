use codex_core::context::ContextualUserFragment;
use codex_core::context::InternalContextSource;
use codex_core::context::InternalModelContextFragment;
use codex_core::context::MAX_GOAL_CONTEXT_BYTES;
use codex_core::context::goal_objective_omission_notice;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::ThreadGoal;
use codex_utils_template::Template;
use std::sync::LazyLock;

static CONTINUATION_PROMPT_TEMPLATE: LazyLock<Template> = LazyLock::new(|| {
    parse_embedded_template(
        include_str!("../templates/goals/continuation.md"),
        "goals/continuation.md",
    )
});

static BUDGET_LIMIT_PROMPT_TEMPLATE: LazyLock<Template> = LazyLock::new(|| {
    parse_embedded_template(
        include_str!("../templates/goals/budget_limit.md"),
        "goals/budget_limit.md",
    )
});

static OBJECTIVE_UPDATED_PROMPT_TEMPLATE: LazyLock<Template> = LazyLock::new(|| {
    parse_embedded_template(
        include_str!("../templates/goals/objective_updated.md"),
        "goals/objective_updated.md",
    )
});

fn parse_embedded_template(source: &'static str, template_name: &str) -> Template {
    match Template::parse(source) {
        Ok(template) => template,
        Err(err) => panic!("embedded template {template_name} is invalid: {err}"),
    }
}

pub(crate) fn budget_limit_steering_item(goal: &ThreadGoal) -> ResponseItem {
    goal_context_input_item(goal, budget_limit_prompt)
}

pub(crate) fn objective_updated_steering_item(goal: &ThreadGoal) -> ResponseItem {
    goal_context_input_item(goal, objective_updated_prompt)
}

pub(crate) fn continuation_steering_item(goal: &ThreadGoal) -> ResponseItem {
    goal_context_input_item(goal, continuation_prompt)
}

fn goal_context_input_item(
    goal: &ThreadGoal,
    prompt: fn(&ThreadGoal, &str) -> String,
) -> ResponseItem {
    let omission = goal_objective_omission_notice(goal);
    let objective = if goal.objective.len() <= MAX_GOAL_CONTEXT_BYTES {
        &goal.objective
    } else {
        &omission
    };
    let mut fragment = InternalModelContextFragment::new(
        InternalContextSource::from_static("goal"),
        prompt(goal, objective),
    );
    if fragment.render().len() > MAX_GOAL_CONTEXT_BYTES {
        fragment = InternalModelContextFragment::new(
            InternalContextSource::from_static("goal"),
            prompt(goal, &omission),
        );
        if fragment.render().len() > MAX_GOAL_CONTEXT_BYTES {
            // A future template may exhaust the budget even without the objective. Keep the
            // delivery and goal metadata, explicitly omitting that whole context projection.
            fragment = InternalModelContextFragment::new(
                InternalContextSource::from_static("goal"),
                format!(
                    "Goal context omitted because its complete template exceeds the context limit.\n{omission}"
                ),
            );
        }
    }
    ContextualUserFragment::into(fragment)
}

fn continuation_prompt(goal: &ThreadGoal, objective: &str) -> String {
    let objective = escape_xml_text(objective);
    let tokens_used = goal.tokens_used.to_string();
    let token_budget = goal
        .token_budget
        .map(|budget| budget.to_string())
        .unwrap_or_else(|| "none".to_string());
    let remaining_tokens = goal
        .token_budget
        .map(|budget| (budget - goal.tokens_used).max(0).to_string())
        .unwrap_or_else(|| "unbounded".to_string());

    CONTINUATION_PROMPT_TEMPLATE
        .render([
            ("objective", objective.as_str()),
            ("tokens_used", tokens_used.as_str()),
            ("token_budget", token_budget.as_str()),
            ("remaining_tokens", remaining_tokens.as_str()),
        ])
        .unwrap_or_else(|err| {
            panic!("embedded goals/continuation.md template failed to render: {err}")
        })
}

fn budget_limit_prompt(goal: &ThreadGoal, objective: &str) -> String {
    let objective = escape_xml_text(objective);
    let time_used_seconds = goal.time_used_seconds.to_string();
    let tokens_used = goal.tokens_used.to_string();
    let token_budget = goal
        .token_budget
        .map(|budget| budget.to_string())
        .unwrap_or_else(|| "none".to_string());

    BUDGET_LIMIT_PROMPT_TEMPLATE
        .render([
            ("objective", objective.as_str()),
            ("time_used_seconds", time_used_seconds.as_str()),
            ("tokens_used", tokens_used.as_str()),
            ("token_budget", token_budget.as_str()),
        ])
        .unwrap_or_else(|err| {
            panic!("embedded goals/budget_limit.md template failed to render: {err}")
        })
}

fn objective_updated_prompt(goal: &ThreadGoal, objective: &str) -> String {
    let objective = escape_xml_text(objective);
    let tokens_used = goal.tokens_used.to_string();
    let (token_budget, remaining_tokens) = match goal.token_budget {
        Some(token_budget) => (
            token_budget.to_string(),
            (token_budget - goal.tokens_used).max(0).to_string(),
        ),
        None => ("none".to_string(), "unknown".to_string()),
    };

    OBJECTIVE_UPDATED_PROMPT_TEMPLATE
        .render([
            ("objective", objective.as_str()),
            ("tokens_used", tokens_used.as_str()),
            ("token_budget", token_budget.as_str()),
            ("remaining_tokens", remaining_tokens.as_str()),
        ])
        .unwrap_or_else(|err| {
            panic!("embedded goals/objective_updated.md template failed to render: {err}")
        })
}

fn escape_xml_text(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
#[path = "steering_tests.rs"]
mod tests;
