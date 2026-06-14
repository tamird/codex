use crate::app_event::ForkPanePlacement;
use crate::legacy_core::config::Config;
use crate::terminal_multiplexer::ForkPaneSpawnResult;
use crate::terminal_multiplexer::codex_executable;
use crate::terminal_multiplexer::fork_command_parts;
use crate::terminal_multiplexer::standalone_side_command_parts;
use codex_protocol::ThreadId;
use codex_terminal_detection::TerminalInfo;
use codex_terminal_detection::TerminalName;
use shlex::try_join;
use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::Instant;
use tokio::time::timeout_at;

const GHOSTTY_COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
const GHOSTTY_ERROR_LIMIT: usize = 4_096;
const GHOSTTY_FORK_USAGE: &str = "Usage: /fork [left|right|up|down]";
const GHOSTTY_FLOAT_UNSUPPORTED_MESSAGE: &str = "Ghostty does not support /fork float.";
const GHOSTTY_PLATFORM_UNSUPPORTED_MESSAGE: &str =
    "Ghostty split launch is only available on macOS.";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SplitDirection {
    Right,
    Left,
    Down,
    Up,
}

impl SplitDirection {
    fn from_placement(placement: ForkPanePlacement) -> Option<Self> {
        match placement {
            ForkPanePlacement::Right => Some(Self::Right),
            ForkPanePlacement::Left => Some(Self::Left),
            ForkPanePlacement::Down => Some(Self::Down),
            ForkPanePlacement::Up => Some(Self::Up),
            ForkPanePlacement::Float => None,
        }
    }

    fn applescript_statement(self) -> &'static str {
        match self {
            Self::Right => {
                "set forkTerminal to split targetTerminal direction right with configuration splitConfig"
            }
            Self::Left => {
                "set forkTerminal to split targetTerminal direction left with configuration splitConfig"
            }
            Self::Down => {
                "set forkTerminal to split targetTerminal direction down with configuration splitConfig"
            }
            Self::Up => {
                "set forkTerminal to split targetTerminal direction up with configuration splitConfig"
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct GhosttySpawnConfig {
    program: PathBuf,
    args: Vec<String>,
}

pub(crate) fn ghostty_placement(
    terminal_info: &TerminalInfo,
    placement: Option<ForkPanePlacement>,
) -> Option<ForkPanePlacement> {
    ghostty_placement_for_platform(terminal_info, placement, cfg!(target_os = "macos"))
}

fn ghostty_placement_for_platform(
    terminal_info: &TerminalInfo,
    placement: Option<ForkPanePlacement>,
    platform_supported: bool,
) -> Option<ForkPanePlacement> {
    if platform_supported
        && terminal_info.multiplexer.is_none()
        && terminal_info.name == TerminalName::Ghostty
    {
        placement
    } else {
        None
    }
}

pub(crate) fn ghostty_fork_usage(terminal_info: &TerminalInfo) -> Option<&'static str> {
    ghostty_fork_usage_for_platform(terminal_info, cfg!(target_os = "macos"))
}

fn ghostty_fork_usage_for_platform(
    terminal_info: &TerminalInfo,
    platform_supported: bool,
) -> Option<&'static str> {
    (platform_supported
        && terminal_info.multiplexer.is_none()
        && terminal_info.name == TerminalName::Ghostty)
        .then_some(GHOSTTY_FORK_USAGE)
}

pub(crate) async fn spawn_fork_in_ghostty_split(
    thread_id: &ThreadId,
    config: &Config,
    additional_writable_roots: &[PathBuf],
    placement: ForkPanePlacement,
) -> ForkPaneSpawnResult {
    if SplitDirection::from_placement(placement).is_none() {
        return ForkPaneSpawnResult::InvalidPlacement(
            GHOSTTY_FLOAT_UNSUPPORTED_MESSAGE.to_string(),
        );
    }
    if !cfg!(target_os = "macos") {
        return ForkPaneSpawnResult::Failed(GHOSTTY_PLATFORM_UNSUPPORTED_MESSAGE.to_string());
    }

    let spawn_config = match build_ghostty_spawn_config(
        &codex_executable(),
        thread_id,
        config,
        additional_writable_roots,
        placement,
    ) {
        Ok(spawn_config) => spawn_config,
        Err(err) => return ForkPaneSpawnResult::Failed(err),
    };
    run_ghostty_spawn_config(spawn_config, GHOSTTY_COMMAND_TIMEOUT).await
}

pub(crate) async fn spawn_standalone_side_in_ghostty_split(
    thread_id: &ThreadId,
    config: &Config,
    additional_writable_roots: &[PathBuf],
    placement: ForkPanePlacement,
) -> ForkPaneSpawnResult {
    if SplitDirection::from_placement(placement).is_none() {
        return ForkPaneSpawnResult::InvalidPlacement(
            GHOSTTY_FLOAT_UNSUPPORTED_MESSAGE.to_string(),
        );
    }
    if !cfg!(target_os = "macos") {
        return ForkPaneSpawnResult::Failed(GHOSTTY_PLATFORM_UNSUPPORTED_MESSAGE.to_string());
    }

    let spawn_config = match build_standalone_side_ghostty_spawn_config(
        &codex_executable(),
        thread_id,
        config,
        additional_writable_roots,
        placement,
    ) {
        Ok(spawn_config) => spawn_config,
        Err(err) => return ForkPaneSpawnResult::Failed(err),
    };
    run_ghostty_spawn_config(spawn_config, GHOSTTY_COMMAND_TIMEOUT).await
}

fn build_ghostty_spawn_config(
    exe: &Path,
    thread_id: &ThreadId,
    config: &Config,
    additional_writable_roots: &[PathBuf],
    placement: ForkPanePlacement,
) -> Result<GhosttySpawnConfig, String> {
    let command = fork_command_parts(exe, thread_id, config, additional_writable_roots);
    build_ghostty_spawn_config_for_command(command, config, placement)
}

fn build_standalone_side_ghostty_spawn_config(
    exe: &Path,
    thread_id: &ThreadId,
    config: &Config,
    additional_writable_roots: &[PathBuf],
    placement: ForkPanePlacement,
) -> Result<GhosttySpawnConfig, String> {
    let command = standalone_side_command_parts(exe, thread_id, config, additional_writable_roots);
    build_ghostty_spawn_config_for_command(command, config, placement)
}

fn build_ghostty_spawn_config_for_command(
    command: Vec<String>,
    config: &Config,
    placement: ForkPanePlacement,
) -> Result<GhosttySpawnConfig, String> {
    let direction = SplitDirection::from_placement(placement)
        .ok_or_else(|| GHOSTTY_FLOAT_UNSUPPORTED_MESSAGE.to_string())?;
    let command = try_join(command.iter().map(String::as_str))
        .map_err(|err| format!("failed to quote fork command for Ghostty: {err}"))?;
    let mut args = applescript_args(direction);
    args.push(command);
    args.push(config.cwd.display().to_string());
    Ok(GhosttySpawnConfig {
        program: PathBuf::from("/usr/bin/osascript"),
        args,
    })
}

fn applescript_args(direction: SplitDirection) -> Vec<String> {
    [
        "-e",
        "on run argv",
        "-e",
        "set forkCommand to item 1 of argv",
        "-e",
        "set forkCwd to item 2 of argv",
        "-e",
        "tell application \"Ghostty\"",
        "-e",
        "set targetTerminal to focused terminal of selected tab of front window",
        "-e",
        "set splitConfig to new surface configuration from {initial working directory:forkCwd, initial input:(forkCommand & linefeed)}",
        "-e",
        direction.applescript_statement(),
        "-e",
        "focus forkTerminal",
        "-e",
        "end tell",
        "-e",
        "end run",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

async fn run_ghostty_spawn_config(
    spawn_config: GhosttySpawnConfig,
    command_timeout: Duration,
) -> ForkPaneSpawnResult {
    let GhosttySpawnConfig { program, args } = spawn_config;
    let program_display = program.display().to_string();
    let mut command = Command::new(&program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            return ForkPaneSpawnResult::Failed(format!("failed to run {program_display}: {err}"));
        }
    };
    let stderr = child.stderr.take();
    let mut stderr_task = tokio::spawn(async move {
        match stderr {
            Some(stderr) => read_bounded(stderr, GHOSTTY_ERROR_LIMIT).await,
            None => (Vec::new(), false),
        }
    });
    let deadline = Instant::now() + command_timeout;
    match timeout_at(deadline, child.wait()).await {
        Ok(Ok(status)) if status.success() => {
            if timeout_at(deadline, &mut stderr_task).await.is_err() {
                stderr_task.abort();
            }
            ForkPaneSpawnResult::Spawned
        }
        Ok(Ok(status)) => {
            let (stderr, truncated) = match timeout_at(deadline, &mut stderr_task).await {
                Ok(result) => result.unwrap_or_default(),
                Err(_) => {
                    stderr_task.abort();
                    (Vec::new(), false)
                }
            };
            let detail = format_bounded_diagnostic(&stderr, truncated);
            let suffix = if !detail.is_empty() {
                format!(": {detail}")
            } else {
                Default::default()
            };
            ForkPaneSpawnResult::Failed(format!(
                "{program_display} exited with status {status}{suffix}"
            ))
        }
        Ok(Err(err)) => {
            let _ = child.start_kill();
            stderr_task.abort();
            ForkPaneSpawnResult::Failed(format!("failed to run {program_display}: {err}"))
        }
        Err(_) => {
            let _ = child.start_kill();
            stderr_task.abort();
            ForkPaneSpawnResult::Failed(format!(
                "{program_display} timed out after {command_timeout:?}"
            ))
        }
    }
}

async fn read_bounded(mut reader: impl AsyncRead + Unpin, limit: usize) -> (Vec<u8>, bool) {
    let mut retained = Vec::new();
    let mut buffer = [0_u8; 1_024];
    let mut truncated = false;
    loop {
        let count = match reader.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(count) => count,
        };
        let remaining = limit.saturating_sub(retained.len());
        retained.extend_from_slice(&buffer[..count.min(remaining)]);
        truncated |= count > remaining;
    }
    (retained, truncated)
}

fn format_bounded_diagnostic(stderr: &[u8], truncated: bool) -> String {
    let sanitized = String::from_utf8_lossy(stderr)
        .chars()
        .filter(|character| !character.is_control() || character.is_whitespace())
        .collect::<String>();
    let mut detail = sanitized.split_whitespace().collect::<Vec<_>>().join(" ");
    if truncated {
        detail.push('…');
    }
    detail
}

#[cfg(test)]
#[path = "ghostty_fork_tests.rs"]
mod tests;
