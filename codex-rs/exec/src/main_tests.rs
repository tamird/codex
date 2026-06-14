use super::*;
use pretty_assertions::assert_eq;

#[test]
fn top_cli_parses_resume_prompt_after_config_flag() {
    const PROMPT: &str = "echo resume-with-global-flags-after-subcommand";
    let cli = TopCli::parse_from([
        "codex-exec",
        "resume",
        "--strict-config",
        "--last",
        "--json",
        "--model",
        "gpt-5.2-codex",
        "--config",
        "reasoning_level=xhigh",
        "--dangerously-bypass-approvals-and-sandbox",
        "--skip-git-repo-check",
        PROMPT,
    ]);
    let mut inner = cli.inner;
    inner
        .config_overrides
        .prepend_root_overrides(cli.config_overrides);

    let Some(codex_exec::Command::Resume(args)) = inner.command.as_ref() else {
        panic!("expected resume command");
    };
    let effective_prompt = args.prompt.clone().or_else(|| {
        if args.last {
            args.session_id.clone()
        } else {
            None
        }
    });
    assert_eq!(effective_prompt.as_deref(), Some(PROMPT));
    assert_eq!(inner.config_overrides.raw_overrides.len(), 1);
    assert_eq!(
        inner.config_overrides.raw_overrides[0],
        "reasoning_level=xhigh"
    );
    assert!(inner.strict_config);
}

#[test]
fn top_cli_parses_fork_option_with_root_config() {
    let cli = TopCli::parse_from([
        "codex-exec",
        "--config",
        "reasoning_level=xhigh",
        "--fork",
        "session-123",
        "echo fork",
    ]);

    assert_eq!(cli.inner.fork_session_id.as_deref(), Some("session-123"));
    assert!(cli.inner.command.is_none());
    assert_eq!(cli.inner.prompt.as_deref(), Some("echo fork"));
    assert_eq!(cli.config_overrides.raw_overrides.len(), 1);
    assert_eq!(
        cli.config_overrides.raw_overrides[0],
        "reasoning_level=xhigh"
    );
}
