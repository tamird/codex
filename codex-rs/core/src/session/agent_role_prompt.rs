use crate::config::Config;
use codex_features::Feature;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use std::path::Path;
use tokio::io::AsyncReadExt;
use tracing::warn;

// Bytes, not a token estimate: dynamic role text remains below the context-item limit.
// Bundled prompts are separately reviewed static inputs, including the larger supervisor prompt.
const MAX_AGENT_ROLE_OVERRIDE_BYTES: usize = 8 * 1024;
const ROOT_AGENT_PROMPT_FALLBACK: &str = include_str!("../../assets/root_agent_prompt.md");
const ROOT_AGENT_SUPERVISOR_PROMPT_FALLBACK: &str =
    include_str!("../../assets/root_agent_supervisor_prompt.md");
const SUBAGENT_PROMPT_FALLBACK: &str = include_str!("../../assets/subagent_prompt.md");
const SUPERVISOR_AGENT_PROMPT_FALLBACK: &str =
    include_str!("../../assets/supervisor_agent_prompt.md");

async fn load_agent_prompt_fallback(
    codex_home: &Path,
    fallback: &str,
    override_filename: &str,
) -> String {
    let override_path = codex_home.join(override_filename);
    let Ok(file) = tokio::fs::File::open(&override_path).await else {
        return fallback.to_string();
    };
    let mut contents = Vec::new();
    if file
        .take((MAX_AGENT_ROLE_OVERRIDE_BYTES + 1) as u64)
        .read_to_end(&mut contents)
        .await
        .is_err()
    {
        return fallback.to_string();
    }
    // Check the bounded bytes before UTF-8 decoding: the extra byte may split a code point.
    if contents.len() > MAX_AGENT_ROLE_OVERRIDE_BYTES {
        warn!(
            override_filename,
            max_bytes = MAX_AGENT_ROLE_OVERRIDE_BYTES,
            "agent role override exceeds the byte limit; using the complete bundled prompt; shorten the override to load it"
        );
        return fallback.to_string();
    }
    if let Ok(contents) = String::from_utf8(contents)
        && !contents.trim().is_empty()
    {
        return contents;
    }
    fallback.to_string()
}

pub(super) async fn load_root_agent_prompt(codex_home: &Path) -> String {
    load_agent_prompt_fallback(codex_home, ROOT_AGENT_PROMPT_FALLBACK, "AGENTS.root.md").await
}

pub(super) async fn load_root_agent_supervisor_prompt(codex_home: &Path) -> String {
    load_agent_prompt_fallback(
        codex_home,
        ROOT_AGENT_SUPERVISOR_PROMPT_FALLBACK,
        "AGENTS.root-supervisor.md",
    )
    .await
}

pub(super) async fn load_subagent_prompt(codex_home: &Path) -> String {
    load_agent_prompt_fallback(codex_home, SUBAGENT_PROMPT_FALLBACK, "AGENTS.subagent.md").await
}

pub(crate) async fn load_supervisor_agent_prompt(codex_home: &Path) -> String {
    load_agent_prompt_fallback(
        codex_home,
        SUPERVISOR_AGENT_PROMPT_FALLBACK,
        "AGENTS.supervisor.md",
    )
    .await
}

pub(crate) async fn load_agent_role_prompt(
    config: &Config,
    session_source: &SessionSource,
) -> Option<String> {
    if !config.features.enabled(Feature::AgentPromptInjection) {
        return None;
    }

    let role_prompt = match session_source {
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn { agent_role, .. })
            if agent_role.as_deref() == Some(crate::goal_supervisor::GOAL_SUPERVISOR_ROLE_NAME) =>
        {
            load_supervisor_agent_prompt(&config.codex_home).await
        }
        SessionSource::SubAgent(_) => load_subagent_prompt(&config.codex_home).await,
        SessionSource::Cli
        | SessionSource::VSCode
        | SessionSource::Exec
        | SessionSource::Mcp
        | SessionSource::Custom(_)
        | SessionSource::Internal(_)
        | SessionSource::Unknown => {
            let mut prompt = load_root_agent_prompt(&config.codex_home).await;
            if config.features.enabled(Feature::Goals)
                && config.features.enabled(Feature::GoalSupervisor)
            {
                let supervisor_prompt = load_root_agent_supervisor_prompt(&config.codex_home).await;
                if !supervisor_prompt.trim().is_empty() {
                    prompt.push_str("\n\n");
                    prompt.push_str(&supervisor_prompt);
                }
                if prompt.len() > MAX_AGENT_ROLE_OVERRIDE_BYTES {
                    warn!(
                        max_bytes = MAX_AGENT_ROLE_OVERRIDE_BYTES,
                        "combined AGENTS.root.md and AGENTS.root-supervisor.md exceed the byte limit; using the complete bundled root prompts; shorten the overrides to load them"
                    );
                    prompt = format!(
                        "{ROOT_AGENT_PROMPT_FALLBACK}\n\n{ROOT_AGENT_SUPERVISOR_PROMPT_FALLBACK}"
                    );
                }
            }
            prompt
        }
    };

    if role_prompt.trim().is_empty() {
        None
    } else {
        Some(role_prompt)
    }
}

#[cfg(test)]
#[path = "agent_role_prompt_tests.rs"]
mod tests;
