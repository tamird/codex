use super::*;
use crate::config::ConfigBuilder;
use pretty_assertions::assert_eq;

#[tokio::test]
#[tracing_test::traced_test]
async fn overrides_accept_the_byte_boundary_and_reject_whole_oversized_files() {
    let home = tempfile::tempdir().expect("temporary home");
    let path = home.path().join("AGENTS.supervisor.md");
    let accepted = format!("{}🚀", "a".repeat(MAX_AGENT_ROLE_OVERRIDE_BYTES - 4));
    tokio::fs::write(&path, &accepted).await.unwrap();
    assert_eq!(load_supervisor_agent_prompt(home.path()).await, accepted);

    // First split valid UTF-8 at the extra byte so another case cannot supply its warning.
    for oversized in [
        format!("{}🚀", "a".repeat(MAX_AGENT_ROLE_OVERRIDE_BYTES - 1)),
        "a".repeat(MAX_AGENT_ROLE_OVERRIDE_BYTES + 1),
    ] {
        tokio::fs::write(&path, oversized).await.unwrap();
        assert_eq!(
            load_supervisor_agent_prompt(home.path()).await,
            SUPERVISOR_AGENT_PROMPT_FALLBACK
        );
        assert!(logs_contain("agent role override exceeds the byte limit"));
    }
    assert!(logs_contain("using the complete bundled prompt"));
    assert!(logs_contain("AGENTS.supervisor.md"));
}

#[tokio::test]
#[tracing_test::traced_test]
async fn root_composition_includes_separator_in_the_byte_budget() {
    let home = tempfile::tempdir().expect("temporary home");
    let mut config = ConfigBuilder::without_managed_config_for_tests()
        .codex_home(home.path().to_path_buf())
        .build()
        .await
        .unwrap();
    for feature in [
        Feature::AgentPromptInjection,
        Feature::Goals,
        Feature::GoalSupervisor,
    ] {
        config.features.enable(feature).unwrap();
    }
    let root = "r".repeat(MAX_AGENT_ROLE_OVERRIDE_BYTES / 2);
    let supervisor = "s".repeat(MAX_AGENT_ROLE_OVERRIDE_BYTES / 2 - 2);
    tokio::fs::write(home.path().join("AGENTS.root.md"), &root)
        .await
        .unwrap();
    let supervisor_path = home.path().join("AGENTS.root-supervisor.md");
    tokio::fs::write(&supervisor_path, &supervisor)
        .await
        .unwrap();
    assert_eq!(
        load_agent_role_prompt(&config, &SessionSource::Cli).await,
        Some(format!("{root}\n\n{supervisor}"))
    );

    tokio::fs::write(supervisor_path, format!("{supervisor}s"))
        .await
        .unwrap();
    assert_eq!(
        load_agent_role_prompt(&config, &SessionSource::Cli).await,
        Some(format!(
            "{ROOT_AGENT_PROMPT_FALLBACK}\n\n{ROOT_AGENT_SUPERVISOR_PROMPT_FALLBACK}"
        ))
    );
    assert!(logs_contain(
        "combined AGENTS.root.md and AGENTS.root-supervisor.md exceed the byte limit"
    ));
}
