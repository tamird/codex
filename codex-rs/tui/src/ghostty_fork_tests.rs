use super::*;
use crate::legacy_core::config::ConfigBuilder;
use codex_protocol::openai_models::ReasoningEffort;
use codex_terminal_detection::Multiplexer;
use codex_utils_absolute_path::AbsolutePathBuf;
use insta::assert_snapshot;
use pretty_assertions::assert_eq;
use tempfile::tempdir;

async fn test_config(cwd: &str) -> Config {
    let codex_home = tempdir().expect("temp codex home");
    let mut config = ConfigBuilder::default()
        .codex_home(codex_home.path().to_path_buf())
        .build()
        .await
        .expect("config");
    config.cwd =
        AbsolutePathBuf::from_absolute_path(PathBuf::from(cwd)).expect("absolute repo cwd");
    config
}

fn terminal_info(name: TerminalName, multiplexer: Option<Multiplexer>) -> TerminalInfo {
    TerminalInfo {
        name,
        term_program: None,
        version: None,
        term: None,
        multiplexer,
    }
}

#[test]
fn ghostty_placement_requires_explicit_placement_without_a_multiplexer() {
    let ghostty = terminal_info(TerminalName::Ghostty, /*multiplexer*/ None);
    assert_eq!(
        ghostty_placement_for_platform(
            &ghostty,
            Some(ForkPanePlacement::Right),
            /*platform_supported*/ true,
        ),
        Some(ForkPanePlacement::Right)
    );
    assert_eq!(
        ghostty_placement_for_platform(
            &ghostty, /*placement*/ None, /*platform_supported*/ true,
        ),
        None
    );
    assert_eq!(
        ghostty_placement_for_platform(
            &terminal_info(
                TerminalName::Ghostty,
                Some(Multiplexer::Tmux { version: None })
            ),
            Some(ForkPanePlacement::Right),
            /*platform_supported*/ true,
        ),
        None
    );
    assert_eq!(
        ghostty_placement_for_platform(
            &terminal_info(TerminalName::Iterm2, /*multiplexer*/ None),
            Some(ForkPanePlacement::Right),
            /*platform_supported*/ true,
        ),
        None
    );
    assert_eq!(
        ghostty_placement_for_platform(
            &ghostty,
            Some(ForkPanePlacement::Right),
            /*platform_supported*/ false,
        ),
        None
    );
}

#[test]
fn ghostty_fork_usage_is_contextual() {
    assert_snapshot!(
        "ghostty_fork_command_usage",
        ghostty_fork_usage_for_platform(
            &terminal_info(TerminalName::Ghostty, /*multiplexer*/ None),
            /*platform_supported*/ true,
        )
        .expect("Ghostty usage")
    );
    assert_eq!(
        ghostty_fork_usage_for_platform(
            &terminal_info(
                TerminalName::Ghostty,
                Some(Multiplexer::Tmux { version: None })
            ),
            /*platform_supported*/ true,
        ),
        None
    );
    assert_eq!(
        ghostty_fork_usage_for_platform(
            &terminal_info(TerminalName::Ghostty, /*multiplexer*/ None),
            /*platform_supported*/ false,
        ),
        None
    );
}

#[test]
fn ghostty_applescript_maps_all_cardinal_directions() {
    for (placement, expected) in [
        (ForkPanePlacement::Right, "direction right"),
        (ForkPanePlacement::Left, "direction left"),
        (ForkPanePlacement::Down, "direction down"),
        (ForkPanePlacement::Up, "direction up"),
    ] {
        let direction = SplitDirection::from_placement(placement).expect("cardinal direction");
        let args = applescript_args(direction);
        assert!(args.iter().any(|arg| arg.contains(expected)));
    }
}

#[tokio::test]
async fn ghostty_spawn_config_passes_command_and_cwd_as_argv() {
    let config = test_config("/repo with spaces").await;
    let thread_id =
        ThreadId::from_string("67e55044-10b1-426f-9247-bb680e5fe0c8").expect("thread id");
    let spawn_config = build_ghostty_spawn_config(
        Path::new("/Applications/Frodex Bin/codex"),
        &thread_id,
        &config,
        &[PathBuf::from("/extra dir;$(not-run)")],
        ForkPanePlacement::Right,
    )
    .expect("Ghostty spawn config");

    assert_eq!(spawn_config.program, PathBuf::from("/usr/bin/osascript"));
    let command = spawn_config
        .args
        .get(spawn_config.args.len() - 2)
        .expect("fork command");
    let cwd = spawn_config.args.last().expect("fork cwd");
    let parsed = shlex::split(command).expect("valid shell command");
    assert_eq!(
        &parsed[..4],
        [
            "env".to_string(),
            format!("CODEX_HOME={}", config.codex_home.display()),
            "/Applications/Frodex Bin/codex".to_string(),
            "fork".to_string(),
        ]
    );
    assert!(parsed.contains(&"/extra dir;$(not-run)".to_string()));
    assert_eq!(parsed.last(), Some(&thread_id.to_string()));
    assert_eq!(cwd, "/repo with spaces");
    assert!(
        spawn_config.args[..spawn_config.args.len() - 2]
            .iter()
            .all(|arg| !arg.contains(&thread_id.to_string()) && !arg.contains(cwd))
    );
}

#[tokio::test]
async fn ghostty_standalone_side_spawn_config_uses_hidden_child_startup() {
    let mut config = test_config("/repo with spaces").await;
    config.model = Some("gpt-5".to_string());
    config.model_reasoning_effort = Some(ReasoningEffort::High);
    config.service_tier = Some("priority".to_string());
    let thread_id =
        ThreadId::from_string("67e55044-10b1-426f-9247-bb680e5fe0c8").expect("thread id");

    let spawn_config = build_standalone_side_ghostty_spawn_config(
        Path::new("/Applications/Frodex Bin/codex"),
        &thread_id,
        &config,
        &[],
        ForkPanePlacement::Right,
    )
    .expect("Ghostty standalone side spawn config");

    let command = spawn_config
        .args
        .get(spawn_config.args.len() - 2)
        .expect("side command");
    let parsed = shlex::split(command).expect("valid shell command");
    assert_eq!(
        &parsed[..3],
        [
            "env".to_string(),
            format!("CODEX_HOME={}", config.codex_home.display()),
            "/Applications/Frodex Bin/codex".to_string(),
        ]
    );
    assert!(!parsed.contains(&"fork".to_string()));
    assert!(parsed.contains(&"model_reasoning_effort=\"high\"".to_string()));
    assert!(parsed.contains(&"service_tier=\"priority\"".to_string()));
    let thread_id = thread_id.to_string();
    assert_eq!(
        &parsed[parsed.len() - 2..],
        ["--internal-side-session-id", thread_id.as_str()]
    );
    assert_eq!(
        spawn_config.args.last().map(String::as_str),
        Some("/repo with spaces")
    );
}

#[tokio::test]
async fn ghostty_spawn_config_rejects_float_and_unquotable_arguments() {
    let config = test_config("/repo").await;
    let thread_id = ThreadId::new();

    assert_eq!(
        build_ghostty_spawn_config(
            Path::new("/bin/codex"),
            &thread_id,
            &config,
            &[],
            ForkPanePlacement::Float,
        )
        .expect_err("float must fail closed"),
        GHOSTTY_FLOAT_UNSUPPORTED_MESSAGE
    );

    let err = build_ghostty_spawn_config(
        Path::new("/bin/codex"),
        &thread_id,
        &config,
        &[PathBuf::from("bad\0root")],
        ForkPanePlacement::Right,
    )
    .expect_err("NUL must fail shell quoting");
    assert!(err.contains("failed to quote fork command for Ghostty"));
}

#[cfg(unix)]
#[tokio::test]
async fn ghostty_runner_reports_failure_and_times_out() {
    let failed = run_ghostty_spawn_config(
        GhosttySpawnConfig {
            program: PathBuf::from("/bin/sh"),
            args: vec![
                "-c".to_string(),
                "printf 'Automation denied\\n' >&2; exit 7".to_string(),
            ],
        },
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        failed,
        ForkPaneSpawnResult::Failed(
            "/bin/sh exited with status exit status: 7: Automation denied".to_string()
        )
    );

    let timed_out = run_ghostty_spawn_config(
        GhosttySpawnConfig {
            program: PathBuf::from("/bin/sh"),
            args: vec!["-c".to_string(), "sleep 60".to_string()],
        },
        Duration::from_millis(10),
    )
    .await;
    assert_eq!(
        timed_out,
        ForkPaneSpawnResult::Failed("/bin/sh timed out after 10ms".to_string())
    );
}

#[cfg(unix)]
#[tokio::test]
async fn ghostty_runner_bounds_inherited_stderr_after_child_exit() {
    let started = Instant::now();
    let result = run_ghostty_spawn_config(
        GhosttySpawnConfig {
            program: PathBuf::from("/bin/sh"),
            args: vec!["-c".to_string(), "(sleep 2) >&2 & exit 7".to_string()],
        },
        Duration::from_millis(50),
    )
    .await;

    assert_eq!(
        result,
        ForkPaneSpawnResult::Failed("/bin/sh exited with status exit status: 7".to_string())
    );
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn ghostty_diagnostic_is_sanitized_and_marks_truncation() {
    assert_eq!(
        format_bounded_diagnostic(b"permission\x1b[31m denied\n", /*truncated*/ false,),
        "permission[31m denied"
    );
    let diagnostic =
        format_bounded_diagnostic(&vec![b'x'; GHOSTTY_ERROR_LIMIT], /*truncated*/ true);
    assert!(diagnostic.ends_with('…'));
    assert_eq!(diagnostic.chars().count(), GHOSTTY_ERROR_LIMIT + 1);
}
