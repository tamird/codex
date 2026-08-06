use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use codex_install_context::InstallContext;
use codex_install_context::InstallMethod;
use serde::Deserialize;
use serde::Serialize;

use crate::Daemon;
use crate::backend;
use crate::ensure_supported_platform;
use crate::managed_install::managed_codex_bin;
use crate::settings::DaemonSettings;

/// Installation ownership, not binary freshness. External updates at the same
/// path take effect on restart; standalone updates follow the managed current link.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "path", rename_all = "camelCase")]
pub(crate) enum Executable {
    Standalone(PathBuf),
    External(PathBuf),
}

impl Daemon {
    pub(super) async fn lock_connection(&self) -> Result<DaemonConnectionGuard> {
        let operation_lock = self.acquire_operation_lock().await?;
        let settings = self.load_settings().await?;
        self.ensure_socket_ownership(&settings).await?;
        Ok(DaemonConnectionGuard {
            _operation_lock: operation_lock,
        })
    }

    pub(super) async fn ensure_ownership(&self, settings: &DaemonSettings) -> Result<bool> {
        let backend = backend::pid_backend(self.backend_paths(settings));
        if !self.executable.is_standalone() {
            let updater = backend::pid_update_loop_backend(self.backend_paths(settings));
            let ownership = updater.ownership().await?;
            match ownership {
                backend::ProcessOwnership::NotRunning => {}
                backend::ProcessOwnership::Unrecorded | backend::ProcessOwnership::Recorded(_) => {
                    bail!(
                        "a standalone Codex updater is active for this CODEX_HOME; use a separate CODEX_HOME for the externally managed installation (daemon stop does not stop the updater)"
                    )
                }
            }
        }
        let ownership = backend.ownership().await?;
        match ownership {
            backend::ProcessOwnership::NotRunning => Ok(false),
            backend::ProcessOwnership::Unrecorded => {
                self.executable.ensure_matches(/*recorded*/ None)?;
                Ok(true)
            }
            backend::ProcessOwnership::Recorded(executable) => {
                self.executable.ensure_matches(Some(&executable))?;
                Ok(true)
            }
        }
    }

    pub(super) async fn ensure_socket_ownership(&self, settings: &DaemonSettings) -> Result<()> {
        let running = self.ensure_ownership(settings).await?;
        if !self.executable.is_standalone() && !running {
            bail!(
                "app-server socket has no daemon executable ownership record; use --remote to connect explicitly, or use a separate CODEX_HOME"
            );
        }
        Ok(())
    }

    #[cfg(unix)]
    pub(super) async fn ensure_standalone_updater(&self) -> Result<()> {
        if !self.executable.is_standalone() {
            bail!("externally managed Codex installations do not use the standalone updater");
        }
        let settings = self.load_settings().await?;
        self.ensure_ownership(&settings).await?;
        Ok(())
    }
}

impl Executable {
    pub(crate) fn current(codex_home: &Path) -> Result<Self> {
        let InstallContext {
            method,
            package_layout: _,
        } = InstallContext::current();
        let executable = match method {
            InstallMethod::Standalone {
                release_dir: _,
                resources_dir: _,
                platform: _,
            } => Self::Standalone(managed_codex_bin(codex_home)),
            InstallMethod::Npm
            | InstallMethod::Bun
            | InstallMethod::Pnpm
            | InstallMethod::Brew
            | InstallMethod::Other => {
                let executable = std::env::current_exe()
                    .context("failed to resolve the externally managed Codex executable")?;
                Self::External(executable)
            }
        };
        Ok(executable)
    }

    pub(crate) fn ensure_recordable(&self) -> Result<()> {
        if self.path().to_str().is_none() {
            bail!(
                "daemon executable path {} cannot be recorded as JSON; use executable and CODEX_HOME paths containing valid UTF-8",
                self.path().display(),
            );
        }
        Ok(())
    }

    pub(crate) fn path(&self) -> &Path {
        match self {
            Self::Standalone(path) | Self::External(path) => path,
        }
    }

    pub(crate) fn is_standalone(&self) -> bool {
        matches!(self, Self::Standalone(_))
    }

    pub(crate) fn ensure_available(&self) -> Result<()> {
        self.ensure_recordable()?;
        if self.path().is_file() {
            return Ok(());
        }
        let path = self.path().display();
        match self {
            Self::Standalone(_) => bail!(
                "managed standalone Codex install not found at {path}; reinstall it with the Codex installer"
            ),
            Self::External(_) => bail!(
                "externally managed Codex executable not found at {path}; reinstall it with its original installation tool"
            ),
        }
    }

    pub(crate) fn ensure_matches(&self, recorded: Option<&Self>) -> Result<()> {
        match recorded {
            Some(recorded) => {
                if self == recorded || (self.is_standalone() && recorded.is_standalone()) {
                    return Ok(());
                }
                bail!(
                    "app-server daemon uses a different Codex installation ({}) than this CLI ({}); run `codex app-server daemon stop` before switching installations, or use a separate CODEX_HOME",
                    recorded.path().display(),
                    self.path().display(),
                );
            }
            None => {
                // Older daemon versions could only launch the standalone install.
                if self.is_standalone() {
                    return Ok(());
                }
                bail!(
                    "app-server daemon executable ownership is unknown; run `codex app-server daemon stop` before starting this installation, or use a separate CODEX_HOME"
                );
            }
        }
    }
}

/// Keeps lifecycle replacement serialized until an implicit local connection is established.
pub struct DaemonConnectionGuard {
    _operation_lock: tokio::fs::File,
}

/// Validates ownership under the lifecycle lock. Hold the guard through connection
/// initialization, then release it; explicit remote connections do not need it.
pub async fn lock_default_daemon_connection(codex_home: &Path) -> Result<DaemonConnectionGuard> {
    ensure_supported_platform()?;
    let daemon = Daemon::for_codex_home(codex_home)?;
    daemon.lock_connection().await
}

#[cfg(test)]
#[path = "executable_tests.rs"]
mod tests;
