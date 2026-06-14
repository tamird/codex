use crate::app_event::ForkPanePlacement;
use crate::legacy_core::config::Config;
use codex_config::ConfigLayerSource;
use codex_protocol::ThreadId;
use codex_protocol::config_types::WebSearchMode;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::SandboxPolicy;
use codex_terminal_detection::Multiplexer;
use shlex::try_join;
use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;
use tokio::time::timeout;

#[derive(Debug)]
pub(crate) struct MultiplexerSpawnConfig {
    pub(crate) program: PathBuf,
    pub(crate) args: Vec<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ForkPaneSpawnResult {
    Spawned,
    InvalidPlacement(String),
    Failed(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PaneChildKind {
    Fork,
    StandaloneSide,
}

impl PaneChildKind {
    fn pane_name(self, thread_id: &ThreadId) -> String {
        match self {
            Self::Fork => format!("Fork of {thread_id}"),
            Self::StandaloneSide => format!("Side of {thread_id}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ForkPaneOption {
    pub(crate) placement: ForkPanePlacement,
    pub(crate) name: &'static str,
    pub(crate) description: &'static str,
}

const TMUX_FORK_PANE_OPTIONS: &[ForkPaneOption] = &[
    ForkPaneOption {
        placement: ForkPanePlacement::Right,
        name: "right",
        description: "Open the fork in a pane to the right.",
    },
    ForkPaneOption {
        placement: ForkPanePlacement::Left,
        name: "left",
        description: "Open the fork in a pane to the left.",
    },
    ForkPaneOption {
        placement: ForkPanePlacement::Up,
        name: "up",
        description: "Open the fork in a pane above.",
    },
    ForkPaneOption {
        placement: ForkPanePlacement::Down,
        name: "down",
        description: "Open the fork in a pane below.",
    },
];

const ZELLIJ_FORK_PANE_OPTIONS: &[ForkPaneOption] = &[
    ForkPaneOption {
        placement: ForkPanePlacement::Float,
        name: "float",
        description: "Open the fork in a floating pane.",
    },
    ForkPaneOption {
        placement: ForkPanePlacement::Right,
        name: "right",
        description: "Open the fork in a pane to the right.",
    },
    ForkPaneOption {
        placement: ForkPanePlacement::Down,
        name: "down",
        description: "Open the fork in a pane below.",
    },
];

pub(crate) fn fork_pane_options(multiplexer: &Multiplexer) -> &'static [ForkPaneOption] {
    match multiplexer {
        Multiplexer::Zellij { .. } => ZELLIJ_FORK_PANE_OPTIONS,
        Multiplexer::Tmux { .. } => TMUX_FORK_PANE_OPTIONS,
    }
}

pub(crate) fn parse_fork_pane_placement(arg: &str) -> Option<ForkPanePlacement> {
    match arg.to_ascii_lowercase().as_str() {
        "left" => Some(ForkPanePlacement::Left),
        "right" => Some(ForkPanePlacement::Right),
        "up" => Some(ForkPanePlacement::Up),
        "down" => Some(ForkPanePlacement::Down),
        "float" => Some(ForkPanePlacement::Float),
        _ => None,
    }
}

pub(crate) fn codex_executable() -> PathBuf {
    std::env::current_exe()
        .map(|path| resolve_codex_executable(&path))
        .unwrap_or_else(|_| PathBuf::from("codex"))
}

fn resolve_codex_executable(current_exe: &Path) -> PathBuf {
    let Some(file_name) = current_exe.file_name().and_then(|name| name.to_str()) else {
        return PathBuf::from("codex");
    };
    let Some(base_name) = file_name
        .strip_suffix(".exe")
        .unwrap_or(file_name)
        .strip_prefix("codex-tui")
    else {
        return current_exe.to_path_buf();
    };
    if !base_name.is_empty() {
        return current_exe.to_path_buf();
    }

    let sibling = if file_name.ends_with(".exe") {
        current_exe.with_file_name("codex.exe")
    } else {
        current_exe.with_file_name("codex")
    };

    if sibling.is_file() {
        sibling
    } else {
        PathBuf::from("codex")
    }
}

pub(crate) fn fork_command_parts(
    exe: &Path,
    thread_id: &ThreadId,
    config: &Config,
    additional_writable_roots: &[PathBuf],
) -> Vec<String> {
    pane_child_command_parts(
        PaneChildKind::Fork,
        exe,
        thread_id,
        config,
        additional_writable_roots,
    )
}

pub(crate) fn standalone_side_command_parts(
    exe: &Path,
    thread_id: &ThreadId,
    config: &Config,
    additional_writable_roots: &[PathBuf],
) -> Vec<String> {
    pane_child_command_parts(
        PaneChildKind::StandaloneSide,
        exe,
        thread_id,
        config,
        additional_writable_roots,
    )
}

fn pane_child_command_parts(
    kind: PaneChildKind,
    exe: &Path,
    thread_id: &ThreadId,
    config: &Config,
    additional_writable_roots: &[PathBuf],
) -> Vec<String> {
    let mut args = vec![
        "env".to_string(),
        format!("CODEX_HOME={}", config.codex_home.display()),
        exe.display().to_string(),
    ];
    if kind == PaneChildKind::Fork {
        args.push("fork".to_string());
    }
    args.push("-C".to_string());
    args.push(config.cwd.display().to_string());

    match config.permissions.approval_policy.value() {
        AskForApproval::UnlessTrusted => {
            args.push("-a".to_string());
            args.push("untrusted".to_string());
        }
        AskForApproval::OnRequest => {
            args.push("-a".to_string());
            args.push("on-request".to_string());
        }
        AskForApproval::Never => {
            args.push("-a".to_string());
            args.push("never".to_string());
        }
        AskForApproval::Granular(granular_config) => {
            let sandbox_approval = granular_config.sandbox_approval;
            let rules = granular_config.rules;
            let skill_approval = granular_config.skill_approval;
            let request_permissions = granular_config.request_permissions;
            let mcp_elicitations = granular_config.mcp_elicitations;
            args.push("-c".to_string());
            args.push(format!(
                "approval_policy={{ granular = {{ sandbox_approval = {sandbox_approval}, rules = {rules}, skill_approval = {skill_approval}, request_permissions = {request_permissions}, mcp_elicitations = {mcp_elicitations} }} }}"
            ));
        }
    }

    if let Some(profile) = active_user_profile_from_config(config) {
        args.push("-p".to_string());
        args.push(profile.to_string());
    }
    if let Some(model) = config.model.as_deref() {
        args.push("-m".to_string());
        args.push(model.to_string());
    }
    if let Some(effort) = config.model_reasoning_effort.as_ref() {
        args.push("-c".to_string());
        args.push(format!(
            "model_reasoning_effort={}",
            toml::Value::String(effort.to_string())
        ));
    }
    if let Some(service_tier) = config.service_tier.as_deref() {
        args.push("-c".to_string());
        args.push(format!(
            "service_tier={}",
            toml::Value::String(service_tier.to_string())
        ));
    }
    if let Some(sandbox_mode) = sandbox_mode_arg(&config.legacy_sandbox_policy()) {
        args.push("-s".to_string());
        args.push(sandbox_mode.to_string());
    }
    if config.web_search_mode.value() == WebSearchMode::Live {
        args.push("--search".to_string());
    }
    for root in additional_writable_roots {
        args.push("--add-dir".to_string());
        args.push(root.display().to_string());
    }
    match kind {
        PaneChildKind::Fork => args.push(thread_id.to_string()),
        PaneChildKind::StandaloneSide => {
            args.push("--internal-side-session-id".to_string());
            args.push(thread_id.to_string());
        }
    }

    args
}

fn active_user_profile_from_config(config: &Config) -> Option<&str> {
    let user_layer = config.config_layer_stack.get_active_user_layer()?;
    let ConfigLayerSource::User { profile, .. } = &user_layer.name else {
        return None;
    };
    profile.as_deref()
}

fn sandbox_mode_arg(policy: &SandboxPolicy) -> Option<&'static str> {
    match policy {
        SandboxPolicy::DangerFullAccess => Some("danger-full-access"),
        SandboxPolicy::ReadOnly { .. } => Some("read-only"),
        SandboxPolicy::WorkspaceWrite { .. } => Some("workspace-write"),
        SandboxPolicy::ExternalSandbox { .. } => None,
    }
}

fn zellij_direction(placement: ForkPanePlacement) -> Option<&'static str> {
    match placement {
        ForkPanePlacement::Right => Some("right"),
        ForkPanePlacement::Down => Some("down"),
        _ => None,
    }
}

fn build_zellij_new_pane_args(
    multiplexer: &Multiplexer,
    command: &[String],
    pane_name: String,
    placement: Option<ForkPanePlacement>,
) -> Vec<String> {
    let mut args = vec![
        "action".to_string(),
        "new-pane".to_string(),
        "--close-on-exit".to_string(),
    ];
    if zellij_supports_near_current_pane(multiplexer) {
        args.push("--near-current-pane".to_string());
    }
    args.push("--name".to_string());
    args.push(pane_name);
    if let Some(placement) = placement {
        if placement == ForkPanePlacement::Float {
            args.push("--floating".to_string());
        } else if let Some(direction) = zellij_direction(placement) {
            args.push("--direction".to_string());
            args.push(direction.to_string());
        } else {
            unreachable!("invalid zellij placement");
        }
    }
    args.push("--".to_string());
    args.extend(command.iter().cloned());
    args
}

fn zellij_supports_near_current_pane(multiplexer: &Multiplexer) -> bool {
    let Multiplexer::Zellij {
        version: Some(version),
    } = multiplexer
    else {
        return false;
    };
    let mut parts = version.split('.');
    let Some(major) = parts.next().and_then(|part| part.parse::<u64>().ok()) else {
        return false;
    };
    let Some(minor) = parts.next().and_then(|part| part.parse::<u64>().ok()) else {
        return false;
    };

    (major, minor) >= (0, 44)
}

fn tmux_split_flags(placement: Option<ForkPanePlacement>) -> [&'static str; 2] {
    match placement {
        None | Some(ForkPanePlacement::Right) => ["-h", ""],
        Some(ForkPanePlacement::Left) => ["-h", "-b"],
        Some(ForkPanePlacement::Down) => ["-v", ""],
        Some(ForkPanePlacement::Up) => ["-v", "-b"],
        _ => unreachable!("invalid tmux placement"),
    }
}

fn build_tmux_new_pane_args(
    command: &[String],
    placement: Option<ForkPanePlacement>,
) -> Result<Vec<String>, String> {
    let command = try_join(command.iter().map(String::as_str))
        .map_err(|err| format!("failed to quote fork command for tmux: {err}"))?;
    let flags = tmux_split_flags(placement);
    let mut args = vec!["split-window".to_string(), flags[0].to_string()];
    if !flags[1].is_empty() {
        args.push(flags[1].to_string());
    }
    args.push(command);
    Ok(args)
}

#[allow(clippy::too_many_arguments)]
fn pane_spawn_config(
    multiplexer: &Multiplexer,
    kind: PaneChildKind,
    exe: &Path,
    thread_id: &ThreadId,
    config: &Config,
    additional_writable_roots: &[PathBuf],
    placement: Option<ForkPanePlacement>,
    tmux_pane: Option<&str>,
) -> Result<MultiplexerSpawnConfig, String> {
    #[cfg(windows)]
    {
        let _ = (
            multiplexer,
            kind,
            exe,
            thread_id,
            config,
            additional_writable_roots,
            placement,
            tmux_pane,
        );
        return Err(WINDOWS_FORK_PANE_UNSUPPORTED_MESSAGE.to_string());
    }

    #[cfg(not(windows))]
    {
        let command =
            pane_child_command_parts(kind, exe, thread_id, config, additional_writable_roots);
        match multiplexer {
            Multiplexer::Zellij { .. } => Ok(MultiplexerSpawnConfig {
                program: PathBuf::from("zellij"),
                args: build_zellij_new_pane_args(
                    multiplexer,
                    &command,
                    kind.pane_name(thread_id),
                    placement,
                ),
            }),
            Multiplexer::Tmux { .. } => {
                let mut args = build_tmux_new_pane_args(&command, placement)?;
                let tmux_pane = tmux_pane
                    .filter(|pane| valid_tmux_pane_id(pane))
                    .ok_or_else(|| {
                        "TMUX_PANE must be a canonical tmux pane ID like `%1`".to_string()
                    })?;
                args.splice(1..1, ["-t".to_string(), tmux_pane.to_string()]);
                Ok(MultiplexerSpawnConfig {
                    program: PathBuf::from("tmux"),
                    args,
                })
            }
        }
    }
}

fn valid_tmux_pane_id(value: &str) -> bool {
    value.strip_prefix('%').is_some_and(|digits| {
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
    })
}

const TMUX_FLOAT_UNSUPPORTED_MESSAGE: &str = "tmux does not support /fork float.";
const ZELLIJ_UNSUPPORTED_MESSAGE: &str = "Zellij only supports /fork [float|right|down].";
const ZELLIJ_SIDE_UNSUPPORTED_MESSAGE: &str = "Zellij only supports /side [--right|--down].";
#[cfg(windows)]
const WINDOWS_FORK_PANE_UNSUPPORTED_MESSAGE: &str =
    "Fork pane placement is not supported on Windows.";
pub(crate) const FORK_PLACEMENT_REQUIRES_PANE_HOST_MESSAGE: &str =
    "Fork pane placement requires tmux, Zellij, or macOS Ghostty.";
const MULTIPLEXER_COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) fn fork_command_usage(multiplexer: Option<&Multiplexer>) -> String {
    let Some(multiplexer) = multiplexer else {
        return "Usage: /fork".to_string();
    };
    let options = fork_pane_options(multiplexer);
    if options.is_empty() {
        return "Usage: /fork".to_string();
    }

    let options = options
        .iter()
        .map(|option| option.name)
        .collect::<Vec<_>>()
        .join("|");
    format!("Usage: /fork [{options}]")
}

fn validate_fork_placement_for_multiplexer(
    multiplexer: &Multiplexer,
    placement: Option<ForkPanePlacement>,
    kind: PaneChildKind,
) -> Result<(), String> {
    match multiplexer {
        Multiplexer::Zellij { .. } => {
            if placement.is_none_or(|placement| {
                ZELLIJ_FORK_PANE_OPTIONS.iter().any(|option| {
                    option.placement == placement
                        && (kind == PaneChildKind::Fork || placement != ForkPanePlacement::Float)
                })
            }) {
                Ok(())
            } else if kind == PaneChildKind::StandaloneSide {
                Err(ZELLIJ_SIDE_UNSUPPORTED_MESSAGE.to_string())
            } else {
                Err(ZELLIJ_UNSUPPORTED_MESSAGE.to_string())
            }
        }
        Multiplexer::Tmux { .. } => {
            if placement.is_none_or(|placement| {
                TMUX_FORK_PANE_OPTIONS
                    .iter()
                    .any(|option| option.placement == placement)
            }) {
                Ok(())
            } else {
                Err(TMUX_FLOAT_UNSUPPORTED_MESSAGE.to_string())
            }
        }
    }
}

pub(crate) async fn spawn_fork_in_new_pane(
    multiplexer: &Multiplexer,
    thread_id: &ThreadId,
    config: &Config,
    additional_writable_roots: &[PathBuf],
    placement: Option<ForkPanePlacement>,
) -> ForkPaneSpawnResult {
    spawn_pane_child_in_new_pane(
        multiplexer,
        PaneChildKind::Fork,
        thread_id,
        config,
        additional_writable_roots,
        placement,
    )
    .await
}

pub(crate) async fn spawn_standalone_side_in_new_pane(
    multiplexer: &Multiplexer,
    thread_id: &ThreadId,
    config: &Config,
    additional_writable_roots: &[PathBuf],
    placement: ForkPanePlacement,
) -> ForkPaneSpawnResult {
    spawn_pane_child_in_new_pane(
        multiplexer,
        PaneChildKind::StandaloneSide,
        thread_id,
        config,
        additional_writable_roots,
        Some(placement),
    )
    .await
}

async fn spawn_pane_child_in_new_pane(
    multiplexer: &Multiplexer,
    kind: PaneChildKind,
    thread_id: &ThreadId,
    config: &Config,
    additional_writable_roots: &[PathBuf],
    placement: Option<ForkPanePlacement>,
) -> ForkPaneSpawnResult {
    if let Err(err) = validate_fork_placement_for_multiplexer(multiplexer, placement, kind) {
        return ForkPaneSpawnResult::InvalidPlacement(err);
    }

    let exe = codex_executable();
    let tmux_pane = std::env::var("TMUX_PANE").ok();
    let spawn_config = match pane_spawn_config(
        multiplexer,
        kind,
        &exe,
        thread_id,
        config,
        additional_writable_roots,
        placement,
        tmux_pane.as_deref(),
    ) {
        Ok(spawn_config) => spawn_config,
        Err(err) => return ForkPaneSpawnResult::Failed(err),
    };
    run_multiplexer_spawn_config(spawn_config, MULTIPLEXER_COMMAND_TIMEOUT).await
}

async fn run_multiplexer_spawn_config(
    spawn_config: MultiplexerSpawnConfig,
    command_timeout: Duration,
) -> ForkPaneSpawnResult {
    let MultiplexerSpawnConfig { program, args } = spawn_config;
    let program_display = program.display().to_string();
    let mut command = Command::new(&program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    match timeout(command_timeout, command.status()).await {
        Ok(Ok(status)) if status.success() => ForkPaneSpawnResult::Spawned,
        Ok(Ok(status)) => {
            ForkPaneSpawnResult::Failed(format!("{program_display} exited with status {status}"))
        }
        Ok(Err(err)) => {
            ForkPaneSpawnResult::Failed(format!("failed to run {program_display}: {err}"))
        }
        Err(_) => ForkPaneSpawnResult::Failed(format!(
            "{program_display} timed out after {command_timeout:?}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::legacy_core::config::ConfigBuilder;
    use codex_protocol::openai_models::ReasoningEffort;
    use codex_protocol::protocol::GranularApprovalConfig;
    use codex_utils_absolute_path::AbsolutePathBuf;
    use insta::assert_snapshot;
    use pretty_assertions::assert_eq;
    use tempfile::tempdir;

    #[test]
    fn resolve_codex_executable_prefers_sibling_codex_for_codex_tui() {
        let tempdir = tempdir().expect("tempdir");
        let current_exe = tempdir.path().join("codex-tui");
        let sibling = tempdir.path().join("codex");
        std::fs::write(&sibling, b"").expect("create sibling codex");

        assert_eq!(resolve_codex_executable(&current_exe), sibling);
    }

    #[test]
    fn resolve_codex_executable_keeps_non_tui_binary() {
        let current_exe = PathBuf::from("/tmp/codex");

        assert_eq!(resolve_codex_executable(&current_exe), current_exe);
    }

    #[test]
    fn validate_zellij_fork_placement_rejects_left() {
        assert_eq!(
            validate_fork_placement_for_multiplexer(
                &Multiplexer::Zellij { version: None },
                Some(ForkPanePlacement::Left),
                PaneChildKind::Fork,
            ),
            Err(ZELLIJ_UNSUPPORTED_MESSAGE.to_string())
        );
    }

    #[test]
    fn validate_tmux_fork_placement_rejects_float() {
        assert_eq!(
            validate_fork_placement_for_multiplexer(
                &Multiplexer::Tmux { version: None },
                Some(ForkPanePlacement::Float),
                PaneChildKind::Fork,
            ),
            Err(TMUX_FLOAT_UNSUPPORTED_MESSAGE.to_string())
        );
    }

    #[test]
    fn validate_zellij_side_placement_rejects_float_and_left() {
        for placement in [
            ForkPanePlacement::Float,
            ForkPanePlacement::Left,
            ForkPanePlacement::Up,
        ] {
            assert_eq!(
                validate_fork_placement_for_multiplexer(
                    &Multiplexer::Zellij { version: None },
                    Some(placement),
                    PaneChildKind::StandaloneSide,
                ),
                Err(ZELLIJ_SIDE_UNSUPPORTED_MESSAGE.to_string())
            );
        }
        assert_snapshot!(
            "zellij_side_unsupported_message",
            ZELLIJ_SIDE_UNSUPPORTED_MESSAGE
        );
    }

    #[test]
    fn fork_command_usage_is_contextual() {
        assert_snapshot!(
            "fork_command_usage_default",
            fork_command_usage(/*multiplexer*/ None)
        );
        assert_snapshot!(
            "fork_command_usage_tmux",
            fork_command_usage(Some(&Multiplexer::Tmux { version: None }))
        );
        assert_snapshot!(
            "fork_command_usage_zellij",
            fork_command_usage(Some(&Multiplexer::Zellij { version: None }))
        );
    }

    #[tokio::test]
    #[cfg(not(windows))]
    async fn tmux_spawn_config_targets_origin_pane() {
        let codex_home = tempdir().expect("temp codex home");
        let config = ConfigBuilder::default()
            .codex_home(codex_home.path().to_path_buf())
            .build()
            .await
            .expect("config");
        let thread_id = ThreadId::new();

        let spawn_config = pane_spawn_config(
            &Multiplexer::Tmux { version: None },
            PaneChildKind::Fork,
            Path::new("/bin/codex"),
            &thread_id,
            &config,
            &[],
            Some(ForkPanePlacement::Right),
            Some("%42"),
        )
        .expect("tmux spawn config");

        assert_eq!(spawn_config.program, PathBuf::from("tmux"));
        assert_eq!(&spawn_config.args[..4], ["split-window", "-t", "%42", "-h"]);
        assert!(
            spawn_config
                .args
                .last()
                .is_some_and(|command| command.contains(&thread_id.to_string()))
        );
    }

    #[tokio::test]
    #[cfg(not(windows))]
    async fn tmux_spawn_config_rejects_missing_or_malformed_origin_pane() {
        let codex_home = tempdir().expect("temp codex home");
        let config = ConfigBuilder::default()
            .codex_home(codex_home.path().to_path_buf())
            .build()
            .await
            .expect("config");
        let multiplexer = Multiplexer::Tmux { version: None };
        let thread_id = ThreadId::new();

        for tmux_pane in [
            None,
            Some(""),
            Some("1"),
            Some("%abc"),
            Some("%1;split-window"),
        ] {
            let err = pane_spawn_config(
                &multiplexer,
                PaneChildKind::Fork,
                Path::new("/bin/codex"),
                &thread_id,
                &config,
                &[],
                Some(ForkPanePlacement::Right),
                tmux_pane,
            )
            .expect_err("invalid tmux pane must fail closed");

            assert_eq!(err, "TMUX_PANE must be a canonical tmux pane ID like `%1`");
        }
    }

    #[tokio::test]
    #[cfg(not(windows))]
    async fn zellij_0_44_spawn_config_stays_near_invoking_pane() {
        let codex_home = tempdir().expect("temp codex home");
        let config = ConfigBuilder::default()
            .codex_home(codex_home.path().to_path_buf())
            .build()
            .await
            .expect("config");

        let thread_id = ThreadId::new();
        let spawn_config = pane_spawn_config(
            &Multiplexer::Zellij {
                version: Some("0.44.0".to_string()),
            },
            PaneChildKind::Fork,
            Path::new("/bin/codex"),
            &thread_id,
            &config,
            &[],
            Some(ForkPanePlacement::Right),
            /*tmux_pane*/ None,
        )
        .expect("zellij spawn config");

        assert_eq!(spawn_config.program, PathBuf::from("zellij"));
        assert!(
            spawn_config
                .args
                .windows(2)
                .any(|args| args == ["--near-current-pane", "--name"])
        );
    }

    #[tokio::test]
    #[cfg(not(windows))]
    async fn zellij_standalone_side_spawn_config_names_and_launches_hidden_child() {
        let codex_home = tempdir().expect("temp codex home");
        let config = ConfigBuilder::default()
            .codex_home(codex_home.path().to_path_buf())
            .build()
            .await
            .expect("config");
        let thread_id = ThreadId::new();

        let spawn_config = pane_spawn_config(
            &Multiplexer::Zellij {
                version: Some("0.44.0".to_string()),
            },
            PaneChildKind::StandaloneSide,
            Path::new("/bin/codex"),
            &thread_id,
            &config,
            &[],
            Some(ForkPanePlacement::Right),
            /*tmux_pane*/ None,
        )
        .expect("zellij standalone side spawn config");

        assert_eq!(spawn_config.program, PathBuf::from("zellij"));
        let pane_name = format!("Side of {thread_id}");
        assert!(spawn_config.args.contains(&pane_name));
        assert!(
            spawn_config
                .args
                .windows(2)
                .any(|args| args == ["--direction", "right"])
        );
        let command_start = spawn_config
            .args
            .iter()
            .position(|arg| arg == "--")
            .expect("command separator")
            + 1;
        let command = &spawn_config.args[command_start..];
        assert!(!command.iter().any(|arg| arg == "fork"));
        let thread_id = thread_id.to_string();
        assert_eq!(
            &command[command.len() - 2..],
            ["--internal-side-session-id", thread_id.as_str()]
        );
    }

    #[tokio::test]
    #[cfg(not(windows))]
    async fn older_or_unknown_zellij_uses_compatible_pane_arguments() {
        let codex_home = tempdir().expect("temp codex home");
        let config = ConfigBuilder::default()
            .codex_home(codex_home.path().to_path_buf())
            .build()
            .await
            .expect("config");
        let thread_id = ThreadId::new();

        for version in [
            None,
            Some("0.43.1".to_string()),
            Some("invalid".to_string()),
        ] {
            let spawn_config = pane_spawn_config(
                &Multiplexer::Zellij { version },
                PaneChildKind::Fork,
                Path::new("/bin/codex"),
                &thread_id,
                &config,
                &[],
                Some(ForkPanePlacement::Right),
                /*tmux_pane*/ None,
            )
            .expect("zellij spawn config");

            assert!(
                !spawn_config
                    .args
                    .contains(&"--near-current-pane".to_string())
            );
        }
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_fork_pane_launch_fails_before_building_a_posix_env_command() {
        let codex_home = tempdir().expect("temp codex home");
        let config = ConfigBuilder::default()
            .codex_home(codex_home.path().to_path_buf())
            .build()
            .await
            .expect("config");

        let thread_id = ThreadId::new();
        let err = pane_spawn_config(
            &Multiplexer::Zellij {
                version: Some("0.44.0".to_string()),
            },
            PaneChildKind::Fork,
            Path::new("C:\\codex.exe"),
            &thread_id,
            &config,
            &[],
            Some(ForkPanePlacement::Right),
            /*tmux_pane*/ None,
        )
        .expect_err("native Windows launch must fail closed");

        assert_eq!(err, WINDOWS_FORK_PANE_UNSUPPORTED_MESSAGE);
    }

    #[test]
    fn tmux_command_quoting_failure_is_not_downgraded() {
        let err = build_tmux_new_pane_args(
            &["codex".to_string(), "bad\0argument".to_string()],
            Some(ForkPanePlacement::Right),
        )
        .expect_err("NUL must fail shell quoting");

        assert!(err.contains("failed to quote fork command for tmux"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn multiplexer_command_timeout_fails_closed() {
        let result = run_multiplexer_spawn_config(
            MultiplexerSpawnConfig {
                program: PathBuf::from("/bin/sh"),
                args: vec!["-c".to_string(), "sleep 60".to_string()],
            },
            Duration::from_millis(10),
        )
        .await;

        assert_eq!(
            result,
            ForkPaneSpawnResult::Failed("/bin/sh timed out after 10ms".to_string())
        );
    }

    #[tokio::test]
    async fn fork_command_parts_include_current_session_overrides() {
        let codex_home = tempdir().expect("temp codex home");
        let mut config = ConfigBuilder::default()
            .codex_home(codex_home.path().to_path_buf())
            .build()
            .await
            .expect("config");
        let user_config_file = config
            .config_layer_stack
            .get_user_config_file()
            .expect("user config file")
            .clone();
        let profile = "work".parse().expect("profile-v2 name");
        config.config_layer_stack = config
            .config_layer_stack
            .with_user_config_profile(
                &user_config_file,
                Some(&profile),
                toml::Value::Table(toml::map::Map::new()),
            )
            .expect("config layer stack");
        config.model = Some("gpt-5".to_string());
        config.model_reasoning_effort = Some(ReasoningEffort::High);
        config.service_tier = Some("priority".to_string());
        config.cwd =
            AbsolutePathBuf::from_absolute_path(PathBuf::from("/repo")).expect("absolute repo cwd");
        config
            .permissions
            .approval_policy
            .set(AskForApproval::OnRequest)
            .expect("approval policy");
        config
            .set_legacy_sandbox_policy(SandboxPolicy::new_workspace_write_policy())
            .expect("sandbox policy");
        config.web_search_mode = codex_config::Constrained::allow_any(WebSearchMode::Live);
        let expected_cwd = config.cwd.to_string_lossy().to_string();

        let command = fork_command_parts(
            Path::new("/bin/codex"),
            &ThreadId::new(),
            &config,
            &[PathBuf::from("/extra")],
        );
        let thread_id = command.last().expect("thread id").clone();

        assert_eq!(
            command,
            vec![
                "env".to_string(),
                format!("CODEX_HOME={}", codex_home.path().display()),
                "/bin/codex".to_string(),
                "fork".to_string(),
                "-C".to_string(),
                expected_cwd,
                "-a".to_string(),
                "on-request".to_string(),
                "-p".to_string(),
                "work".to_string(),
                "-m".to_string(),
                "gpt-5".to_string(),
                "-c".to_string(),
                "model_reasoning_effort=\"high\"".to_string(),
                "-c".to_string(),
                "service_tier=\"priority\"".to_string(),
                "-s".to_string(),
                "workspace-write".to_string(),
                "--search".to_string(),
                "--add-dir".to_string(),
                "/extra".to_string(),
                thread_id,
            ]
        );
    }

    #[tokio::test]
    async fn standalone_side_command_parts_include_live_settings_without_fork_subcommand() {
        let codex_home = tempdir().expect("temp codex home");
        let mut config = ConfigBuilder::default()
            .codex_home(codex_home.path().to_path_buf())
            .build()
            .await
            .expect("config");
        config.model = Some("gpt-5".to_string());
        config.model_reasoning_effort = Some(ReasoningEffort::High);
        config.service_tier = Some("priority".to_string());
        config.cwd =
            AbsolutePathBuf::from_absolute_path(PathBuf::from("/repo")).expect("absolute repo cwd");
        config
            .permissions
            .approval_policy
            .set(AskForApproval::OnRequest)
            .expect("approval policy");
        config
            .set_legacy_sandbox_policy(SandboxPolicy::new_workspace_write_policy())
            .expect("sandbox policy");
        config.web_search_mode = codex_config::Constrained::allow_any(WebSearchMode::Live);
        let thread_id = ThreadId::new();

        let command = standalone_side_command_parts(
            Path::new("/bin/codex"),
            &thread_id,
            &config,
            &[PathBuf::from("/extra")],
        );

        assert_eq!(
            command,
            vec![
                "env".to_string(),
                format!("CODEX_HOME={}", codex_home.path().display()),
                "/bin/codex".to_string(),
                "-C".to_string(),
                "/repo".to_string(),
                "-a".to_string(),
                "on-request".to_string(),
                "-m".to_string(),
                "gpt-5".to_string(),
                "-c".to_string(),
                "model_reasoning_effort=\"high\"".to_string(),
                "-c".to_string(),
                "service_tier=\"priority\"".to_string(),
                "-s".to_string(),
                "workspace-write".to_string(),
                "--search".to_string(),
                "--add-dir".to_string(),
                "/extra".to_string(),
                "--internal-side-session-id".to_string(),
                thread_id.to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn fork_command_parts_preserve_granular_approval_policy() {
        let codex_home = tempdir().expect("temp codex home");
        let mut config = ConfigBuilder::default()
            .codex_home(codex_home.path().to_path_buf())
            .build()
            .await
            .expect("config");
        config.cwd =
            AbsolutePathBuf::from_absolute_path(PathBuf::from("/repo")).expect("absolute repo cwd");
        let expected_cwd = config.cwd.to_string_lossy().to_string();
        config
            .permissions
            .approval_policy
            .set(AskForApproval::Granular(GranularApprovalConfig {
                sandbox_approval: true,
                rules: false,
                skill_approval: true,
                request_permissions: false,
                mcp_elicitations: true,
            }))
            .expect("approval policy");
        config
            .set_legacy_sandbox_policy(SandboxPolicy::new_read_only_policy())
            .expect("sandbox policy");

        let command = fork_command_parts(Path::new("/bin/codex"), &ThreadId::new(), &config, &[]);
        let thread_id = command.last().expect("thread id").clone();

        assert_eq!(
            command,
            vec![
                "env".to_string(),
                format!("CODEX_HOME={}", codex_home.path().display()),
                "/bin/codex".to_string(),
                "fork".to_string(),
                "-C".to_string(),
                expected_cwd,
                "-c".to_string(),
                "approval_policy={ granular = { sandbox_approval = true, rules = false, skill_approval = true, request_permissions = false, mcp_elicitations = true } }".to_string(),
                "-s".to_string(),
                "read-only".to_string(),
                thread_id,
            ]
        );
    }
}
