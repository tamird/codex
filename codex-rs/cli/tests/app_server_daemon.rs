#![cfg(unix)]

use std::path::PathBuf;
use std::process::Output;

use anyhow::Result;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;

struct DaemonTest {
    home: TempDir,
    binary: PathBuf,
}

impl DaemonTest {
    fn command(&self, args: &[&str]) -> Result<Output> {
        let output = std::process::Command::new(&self.binary)
            .env("CODEX_HOME", self.home.path())
            .args(args)
            .output()?;
        Ok(output)
    }

    fn daemon(&self, command: &str) -> Result<Value> {
        let output = self.command(&["app-server", "daemon", command])?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let response = serde_json::from_slice(&output.stdout)?;
        Ok(response)
    }

    fn fails(&self, args: &[&str], expected: &str) -> Result<()> {
        let output = self.command(args)?;
        let stderr = String::from_utf8(output.stderr)?;
        assert!(!output.status.success(), "{args:?}: {stderr}");
        assert!(
            stderr.contains(expected),
            "expected {expected:?} for {args:?}, got {stderr}"
        );
        Ok(())
    }
}

impl Drop for DaemonTest {
    fn drop(&mut self) {
        // Detached children must be stopped even when an assertion panics.
        let _ = self.command(&["app-server", "daemon", "stop"]);
    }
}

#[test]
fn external_daemon_lifecycle_and_implicit_attachment() -> Result<()> {
    let test = DaemonTest {
        home: tempfile::tempdir_in("/tmp")?,
        binary: codex_utils_cargo_bin::cargo_bin("codex")?,
    };
    let managed_dir = test.home.path().join("packages/standalone/current");
    std::fs::create_dir_all(&managed_dir)?;
    std::fs::write(managed_dir.join("codex"), "not the invoking executable")?;

    let started = test.daemon("start")?;
    let pid = started.get("pid").expect("started pid").clone();
    let version = env!("CARGO_PKG_VERSION");
    let binary = test.binary.canonicalize()?;
    let canonical_home = test.home.path().canonicalize()?;
    let socket = codex_app_server::app_server_control_socket_path(&canonical_home)?;
    let mut expected = json!({
        "status": "started", "backend": "pid", "pid": pid,
        "managedCodexPath": binary, "managedCodexVersion": version,
        "socketPath": socket, "cliVersion": version, "appServerVersion": version,
    });
    assert_eq!(started, expected);
    expected.as_object_mut().expect("object").remove("pid");
    expected
        .as_object_mut()
        .expect("object")
        .insert("status".into(), json!("alreadyRunning"));
    let reused = test.daemon("start")?;
    assert_eq!(reused, expected);
    expected
        .as_object_mut()
        .expect("object")
        .insert("status".into(), json!("running"));
    let running = test.daemon("version")?;
    assert_eq!(running, expected);
    let restarted = test.daemon("restart")?;
    assert_ne!(restarted.get("pid"), Some(&pid));
    let object = expected.as_object_mut().expect("object");
    object.insert("status".into(), json!("restarted"));
    object.insert(
        "pid".into(),
        restarted.get("pid").expect("restarted pid").clone(),
    );
    assert_eq!(restarted, expected);

    let bootstrapped = test.daemon("bootstrap")?;
    assert_eq!(
        bootstrapped,
        json!({
            "status": "bootstrapped", "backend": "pid", "autoUpdateEnabled": false,
            "remoteControlEnabled": false, "managedCodexPath": binary,
            "managedCodexVersion": version, "socketPath": socket,
            "cliVersion": version, "appServerVersion": version,
        })
    );
    let state = test.home.path().join("app-server-daemon");
    assert!(!state.join("app-server-updater.pid").exists());
    test.fails(
        &["app-server", "daemon", "pid-update-loop"],
        "do not use the standalone updater",
    )?;

    // Queue overrides must not bypass a running daemon through an embedded server.
    test.fails(
        &[
            "queue",
            "-c",
            "model=\"test-model\"",
            "--thread",
            "123e4567-e89b-12d3-a456-426614174000",
            "--message",
            "test",
        ],
        "embedded app server while a local app-server daemon is running",
    )?;

    let pid_path = state.join("app-server.pid");
    let original = std::fs::read(&pid_path)?;
    let mut record: Value = serde_json::from_slice(&original)?;
    for (ownership, message) in [
        (
            json!({"kind": "external", "path": "/another/codex"}),
            "different Codex installation",
        ),
        (Value::Null, "executable ownership is unknown"),
    ] {
        record
            .as_object_mut()
            .expect("record")
            .insert("executable".into(), ownership);
        let conflicting = serde_json::to_vec(&record)?;
        std::fs::write(&pid_path, &conflicting)?;
        for command in ["start", "restart", "bootstrap", "enable-remote-control"] {
            test.fails(&["app-server", "daemon", command], message)?;
            let current_record = std::fs::read(&pid_path)?;
            assert_eq!(current_record, conflicting);
        }
    }
    std::fs::write(&pid_path, &original)?;
    // A live updater must block even when there is no daemon PID record.
    std::fs::write(state.join("app-server-updater.pid"), &original)?;
    std::fs::remove_file(&pid_path)?;
    let output = test.command(&["app-server", "daemon", "start"]);
    std::fs::write(&pid_path, original)?;
    std::fs::remove_file(state.join("app-server-updater.pid"))?;
    let output = output?;
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("standalone Codex updater is active"));
    let stopped = test.daemon("stop")?;
    assert_eq!(stopped.get("status"), Some(&json!("stopped")));
    Ok(())
}
