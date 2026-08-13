#!/usr/bin/env python3
"""Admit one packaged Frodex candidate without using ambient Codex state."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
ARCHIVE_MEMBERS = ("codex", "codex-code-mode-host")
SIX_TOOL_SEQUENCE = (
    "collaboration.list_agents",
    "collaboration.spawn_agent",
    "collaboration.send_message",
    "collaboration.followup_task",
    "collaboration.wait_agent",
    "collaboration.interrupt_agent",
)
THREAD_RESUME_FIXTURE_SHA256 = (
    "160e0c6d52427e1862e526ded2a2e45d629fd7212ceadd3137ceee390771a787"
)
THREAD_RESUME_SENTINEL = "FRODEX_DECIMAL_CHECKPOINT_SENTINEL"
ALLOWED_CASE_ENV = {
    "CARGO_TARGET_DIR",
    "FRODEX_ACCEPTANCE_APP_SERVER",
    "FRODEX_ACCEPTANCE_CODEX",
    "FRODEX_ADMISSION_CASE_REPORT",
    "OPENAI_API_KEY",
    "RUST_BACKTRACE",
    "RUST_LOG",
}
SECRET_PATTERNS = (
    re.compile(rb"(?i)authorization\s*[:=]\s*bearer"),
    re.compile(rb"(?i)bearer\s+[a-z0-9._~+/=-]{16,}"),
    re.compile(rb"\bsk-[A-Za-z0-9_-]{12,}"),
    re.compile(rb'(?i)"(?:access_token|refresh_token|id_token)"\s*:'),
)
BWRAP_PROFILES = ("full", "reduced")


class AdmissionError(RuntimeError):
    """A candidate or harness invariant rejected admission."""


@dataclass(frozen=True)
class Candidate:
    """Verified paths and identities for the extracted release archive."""

    version: str
    source_commit: str
    platform_name: str
    archive_digest: str
    archive_size: int
    root: Path
    codex: Path
    code_mode_host: Path
    codex_digest: str
    code_mode_host_digest: str


@dataclass(frozen=True)
class CasePaths:
    """The only writable paths exposed to one admission case."""

    root: Path
    home: Path
    work: Path
    temporary: Path
    cache: Path
    report: Path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise AdmissionError(f"invalid JSON file {path}: {error}") from error


def load_dotslash_manifest(path: Path) -> dict[str, Any]:
    try:
        payload = path.read_text(encoding="utf-8")
    except OSError as error:
        raise AdmissionError(f"cannot read manifest {path}: {error}") from error
    if payload.startswith("#!/usr/bin/env dotslash\n"):
        payload = payload.split("\n", 1)[1].lstrip()
    try:
        manifest = json.loads(payload)
    except json.JSONDecodeError as error:
        raise AdmissionError(f"invalid DotSlash manifest {path}: {error}") from error
    if not isinstance(manifest, dict):
        raise AdmissionError("DotSlash manifest root must be an object")
    return manifest


def host_platform() -> str:
    os_name = {"Linux": "linux", "Darwin": "macos"}.get(platform.system())
    architecture = {
        "x86_64": "x86_64",
        "AMD64": "x86_64",
        "aarch64": "aarch64",
        "arm64": "aarch64",
    }.get(platform.machine())
    if os_name is None or architecture is None:
        raise AdmissionError(
            f"unsupported admission host {platform.system()}-{platform.machine()}"
        )
    return f"{os_name}-{architecture}"


def require_string(value: Any, description: str) -> str:
    if not isinstance(value, str) or not value:
        raise AdmissionError(f"{description} must be a non-empty string")
    return value


def verify_manifest(
    manifest: dict[str, Any],
    archive: Path,
    expected_version: str,
    expected_source: str,
    platform_name: str,
) -> tuple[str, int]:
    try:
        release = manifest["metadata"]["release"]
        build_commit = manifest["metadata"]["build-info"]["commit"]
        platform_entry = manifest["platforms"][platform_name]
    except (KeyError, TypeError) as error:
        raise AdmissionError(f"manifest is missing required field {error}") from error

    version = require_string(release.get("version"), "release version")
    version_output = require_string(release.get("version-output"), "version output")
    source_commit = require_string(build_commit.get("hash"), "source commit")
    if version != expected_version:
        raise AdmissionError(f"manifest version {version!r} != {expected_version!r}")
    if version_output != f"codex-cli {expected_version}":
        raise AdmissionError(
            "manifest version-output does not match the release version"
        )
    if source_commit != expected_source:
        raise AdmissionError(
            f"manifest source {source_commit} != expected source {expected_source}"
        )
    if platform_entry.get("hash") != "sha256":
        raise AdmissionError("host archive must use sha256")
    if platform_entry.get("format") != "tar.gz":
        raise AdmissionError("host archive must use tar.gz")
    if platform_entry.get("path") != "codex":
        raise AdmissionError("host archive entrypoint must be codex")

    expected_digest = require_string(platform_entry.get("digest"), "archive digest")
    expected_size = platform_entry.get("size")
    if not isinstance(expected_size, int) or expected_size <= 0:
        raise AdmissionError("archive size must be a positive integer")
    actual_size = archive.stat().st_size
    actual_digest = sha256_file(archive)
    if actual_size != expected_size:
        raise AdmissionError(
            f"archive size {actual_size} != manifest size {expected_size}"
        )
    if actual_digest != expected_digest:
        raise AdmissionError(
            f"archive digest {actual_digest} != manifest digest {expected_digest}"
        )
    return actual_digest, actual_size


def extract_candidate(archive: Path, destination: Path) -> tuple[Path, Path]:
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    with tarfile.open(archive, mode="r:gz") as bundle:
        members = bundle.getmembers()
        names = tuple(member.name for member in members)
        if names != ARCHIVE_MEMBERS:
            raise AdmissionError(
                f"archive members {names!r} != required members {ARCHIVE_MEMBERS!r}"
            )
        for member in members:
            if not member.isfile():
                raise AdmissionError(
                    f"archive member {member.name!r} is not a regular file"
                )
            if member.mode & 0o111 == 0:
                raise AdmissionError(
                    f"archive member {member.name!r} is not executable"
                )
            source = bundle.extractfile(member)
            if source is None:
                raise AdmissionError(f"archive member {member.name!r} has no payload")
            target = destination / member.name
            with target.open("xb") as output:
                shutil.copyfileobj(source, output)
            target.chmod(member.mode & 0o777)
    return destination / "codex", destination / "codex-code-mode-host"


def verify_source_checkout(source_repo: Path, expected_source: str) -> None:
    result = subprocess.run(
        ["git", "-C", os.fspath(source_repo), "rev-parse", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise AdmissionError(f"cannot read source checkout {source_repo}")
    actual = result.stdout.strip()
    if actual != expected_source:
        raise AdmissionError(
            f"source checkout {actual} != manifest source {expected_source}"
        )


def verify_candidate_version(codex: Path, expected_version: str) -> None:
    result = subprocess.run(
        [os.fspath(codex), "--version"],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    expected = f"codex-cli {expected_version}"
    if result.returncode != 0 or result.stdout.strip() != expected:
        raise AdmissionError(
            f"candidate version output {result.stdout.strip()!r} != {expected!r}"
        )


def queue_plan_names(queue_dir: Path) -> list[str]:
    return sorted(
        path.name
        for path in queue_dir.glob("*.md")
        if not path.name.endswith("-record.md")
    )


def queue_plan_digest(plans: list[str]) -> str:
    payload = "".join(f"{plan}\n" for plan in plans).encode()
    return hashlib.sha256(payload).hexdigest()


def verify_owner_inventory(
    path: Path, expected_source: str, queue_dir: Path | None = None
) -> dict[str, Any]:
    inventory = load_json(path)
    if (
        not isinstance(inventory, dict)
        or inventory.get("schema_version") != SCHEMA_VERSION
    ):
        raise AdmissionError("owner inventory has an unsupported schema")
    if inventory.get("candidate_source") != expected_source:
        raise AdmissionError("owner inventory is not bound to the candidate source")
    owners = inventory.get("owners")
    if not isinstance(owners, list) or not owners:
        raise AdmissionError("owner inventory must contain at least one owner")

    if queue_dir is not None:
        cutoff = inventory.get("queue_cutoff")
        if not isinstance(cutoff, dict):
            raise AdmissionError("owner inventory is missing its queue cutoff")
        recorded_plans = cutoff.get("plans")
        if (
            not isinstance(recorded_plans, list)
            or not recorded_plans
            or not all(isinstance(item, str) and item for item in recorded_plans)
        ):
            raise AdmissionError("owner inventory queue cutoff has invalid plans")
        current_plans = queue_plan_names(queue_dir)
        if recorded_plans != current_plans:
            raise AdmissionError("owner inventory queue cutoff is stale")
        if cutoff.get("sha256") != queue_plan_digest(current_plans):
            raise AdmissionError("owner inventory queue cutoff digest is invalid")

    seen: set[str] = set()
    for owner in owners:
        if not isinstance(owner, dict):
            raise AdmissionError("owner inventory entries must be objects")
        owner_id = require_string(owner.get("id"), "owner id")
        if owner_id in seen:
            raise AdmissionError(f"duplicate semantic owner {owner_id}")
        seen.add(owner_id)
        if owner.get("required") is not True:
            continue
        resolution = owner.get("resolution")
        if resolution == "candidate":
            require_string(owner.get("source_owner"), f"{owner_id} source_owner")
            tests = owner.get("test_ids")
            if (
                not isinstance(tests, list)
                or not tests
                or not all(isinstance(item, str) and item for item in tests)
            ):
                raise AdmissionError(f"{owner_id} requires non-empty test_ids")
            require_string(
                owner.get("packaged_assertion"), f"{owner_id} packaged_assertion"
            )
        elif resolution == "upstream":
            require_string(
                owner.get("replacement_commit"), f"{owner_id} replacement_commit"
            )
            require_string(owner.get("trigger"), f"{owner_id} trigger")
            if owner.get("trigger_satisfied") is not True:
                raise AdmissionError(
                    f"{owner_id} upstream replacement trigger is not satisfied"
                )
        elif resolution == "deferred":
            require_string(owner.get("user_decision"), f"{owner_id} user_decision")
        else:
            raise AdmissionError(f"required semantic owner {owner_id} is unresolved")
    return inventory


def case_paths(root: Path, name: str) -> CasePaths:
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", name):
        raise AdmissionError(f"invalid case name {name!r}")
    case_root = root / "cases" / name
    paths = CasePaths(
        root=case_root,
        home=case_root / "home",
        work=case_root / "work",
        temporary=case_root / "tmp",
        cache=case_root / "cache",
        report=case_root / "report.json",
    )
    for path in (paths.root, paths.home, paths.work, paths.temporary, paths.cache):
        path.mkdir(mode=0o700, parents=True, exist_ok=False)
    return paths


def copy_auth(auth_source: Path, destination: Path) -> dict[str, Any]:
    source_stat = auth_source.lstat()
    if not stat.S_ISREG(source_stat.st_mode) or auth_source.is_symlink():
        raise AdmissionError("auth source must be a regular non-symlink file")
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with auth_source.open("rb") as source, destination.open("xb") as output:
        shutil.copyfileobj(source, output)
    destination.chmod(0o600)
    return {"basename": auth_source.name, "size": source_stat.st_size, "mode": "0600"}


def interpolate(value: str, replacements: dict[str, str]) -> str:
    try:
        return value.format_map(replacements)
    except KeyError as error:
        raise AdmissionError(f"unknown case interpolation {error}") from error


def bwrap_command(
    argv: list[str],
    admission_root: Path,
    paths: CasePaths,
    network: bool,
    profile: str,
) -> list[str]:
    executable = shutil.which("bwrap")
    if executable is None:
        raise AdmissionError("bubblewrap is required for candidate admission")
    if profile not in BWRAP_PROFILES:
        raise AdmissionError(f"unknown bubblewrap profile {profile!r}")
    command = [
        executable,
        "--die-with-parent",
        "--new-session",
    ]
    if profile == "full":
        command.extend(("--unshare-pid", "--proc", "/proc", "--dev", "/dev"))
    command.extend(
        (
            "--ro-bind",
            "/",
            "/",
            # Git opens /dev/null while creating the isolated worktree fixture.
            "--dev-bind",
            "/dev/null",
            "/dev/null",
            "--bind",
            os.fspath(admission_root),
            os.fspath(admission_root),
            "--tmpfs",
            "/home",
            "--chdir",
            os.fspath(paths.work),
        )
    )
    if not network:
        command.append("--unshare-net")
    command.append("--")
    command.extend(argv)
    return command


def select_bwrap_profile(requested: str, admission_root: Path) -> str:
    """Select a working profile without overstating host namespace support."""
    profiles = BWRAP_PROFILES if requested == "auto" else (requested,)
    probe_root = admission_root / "isolation-probe"
    probe_work = probe_root / "work"
    probe_work.mkdir(mode=0o700, parents=True, exist_ok=False)
    paths = CasePaths(
        root=probe_root,
        home=probe_root / "home",
        work=probe_work,
        temporary=probe_root / "tmp",
        cache=probe_root / "cache",
        report=probe_root / "report.json",
    )
    failures: list[str] = []
    try:
        for profile in profiles:
            result = subprocess.run(
                bwrap_command(
                    ["/usr/bin/true"],
                    admission_root,
                    paths,
                    False,
                    profile,
                ),
                check=False,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
            )
            if result.returncode == 0:
                return profile
            failures.append(f"{profile}={result.returncode}")
    finally:
        shutil.rmtree(probe_root)
    raise AdmissionError(
        "no requested bubblewrap isolation profile is available: " + ", ".join(failures)
    )


def validate_case_report(
    report: Path, report_schema: str | None
) -> dict[str, Any] | None:
    if report_schema is None:
        if report.exists():
            raise AdmissionError("case wrote an undeclared report")
        return None
    payload = load_json(report)
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise AdmissionError("case report has an unsupported schema")
    if payload.get("passed") is not True:
        raise AdmissionError("case report did not declare passed=true")
    if report_schema == "assertions":
        assertions = payload.get("assertions")
        if (
            not isinstance(assertions, list)
            or not assertions
            or not all(isinstance(item, str) and item for item in assertions)
        ):
            raise AdmissionError("assertion report must contain assertion identifiers")
        return {
            "schema_version": SCHEMA_VERSION,
            "passed": True,
            "assertions": assertions,
        }
    if report_schema == "six_tool_smoke":
        tools = payload.get("tools")
        if tuple(tools) != SIX_TOOL_SEQUENCE:
            raise AdmissionError(
                "real-model smoke did not record the exact six-tool sequence"
            )
        if payload.get("thread_resume") is not True:
            raise AdmissionError("real-model smoke did not resume stored history")
        if payload.get("canonical_results") is not True:
            raise AdmissionError(
                "real-model smoke did not validate canonical call/result JSON"
            )
        return {
            "schema_version": SCHEMA_VERSION,
            "passed": True,
            "tools": list(SIX_TOOL_SEQUENCE),
            "thread_resume": True,
            "canonical_results": True,
        }
    if report_schema == "thread_resume":
        if payload.get("thread_resume") is not True:
            raise AdmissionError("resume smoke did not complete thread/resume")
        if payload.get("restart_thread_resume") is not True:
            raise AdmissionError("resume smoke did not resume after app-server restart")
        if payload.get("model_context_sentinel") != THREAD_RESUME_SENTINEL:
            raise AdmissionError(
                "resume smoke did not validate the model-context sentinel"
            )
        if payload.get("fixture_sha256") != THREAD_RESUME_FIXTURE_SHA256:
            raise AdmissionError("resume smoke used an unrecognized fixture")
        return {
            "schema_version": SCHEMA_VERSION,
            "passed": True,
            "thread_resume": True,
            "restart_thread_resume": True,
            "model_context_sentinel": THREAD_RESUME_SENTINEL,
            "fixture_sha256": THREAD_RESUME_FIXTURE_SHA256,
        }
    raise AdmissionError(f"unknown case report schema {report_schema!r}")


def run_case(
    case: dict[str, Any],
    candidate: Candidate,
    source_repo: Path,
    admission_root: Path,
    auth_source: Path | None,
    bwrap_profile: str | None,
) -> dict[str, Any]:
    name = require_string(case.get("name"), "case name")
    paths = case_paths(admission_root, name)
    argv = case.get("argv")
    if (
        not isinstance(argv, list)
        or not argv
        or not all(isinstance(argument, str) and argument for argument in argv)
    ):
        raise AdmissionError(f"case {name} requires a non-empty argv")
    timeout_seconds = case.get("timeout_seconds")
    if not isinstance(timeout_seconds, int) or not 1 <= timeout_seconds <= 300:
        raise AdmissionError(f"case {name} timeout must be between 1 and 300 seconds")
    network = case.get("network", False)
    needs_auth = case.get("auth", False)
    if not isinstance(network, bool) or not isinstance(needs_auth, bool):
        raise AdmissionError(f"case {name} network/auth fields must be booleans")
    if needs_auth and not network:
        raise AdmissionError(f"case {name} requests auth without network access")

    replacements = {
        "candidate": os.fspath(candidate.codex),
        "code_mode_host": os.fspath(candidate.code_mode_host),
        "source": os.fspath(source_repo),
        "case_home": os.fspath(paths.home),
        "work": os.fspath(paths.work),
        "tmp": os.fspath(paths.temporary),
        "report": os.fspath(paths.report),
    }
    command = [interpolate(argument, replacements) for argument in argv]
    case_env = case.get("env", {})
    if not isinstance(case_env, dict):
        raise AdmissionError(f"case {name} env must be an object")
    unknown_env = set(case_env) - ALLOWED_CASE_ENV
    if unknown_env:
        raise AdmissionError(
            f"case {name} uses disallowed environment keys {sorted(unknown_env)}"
        )
    environment = {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "HOME": os.fspath(paths.home),
        "CODEX_HOME": os.fspath(paths.home),
        "TMPDIR": os.fspath(paths.temporary),
        "XDG_CACHE_HOME": os.fspath(paths.cache),
        "FRODEX_CACHE_DIR": os.fspath(paths.cache),
        "FRODEX_ADMISSION_CASE_REPORT": os.fspath(paths.report),
        "RUST_BACKTRACE": "1",
    }
    for key, value in case_env.items():
        if not isinstance(value, str):
            raise AdmissionError(f"case {name} environment values must be strings")
        environment[key] = interpolate(value, replacements)

    auth_evidence = None
    auth_destination = paths.home / "auth.json"
    if needs_auth:
        if auth_source is None:
            raise AdmissionError(f"case {name} requires --auth-source")
        auth_evidence = copy_auth(auth_source, auth_destination)

    executed = (
        bwrap_command(command, admission_root, paths, network, bwrap_profile)
        if bwrap_profile is not None
        else command
    )
    started = time.monotonic()
    process = subprocess.Popen(
        executed,
        cwd=paths.work,
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    timed_out = False
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        timed_out = True
        os.killpg(process.pid, signal.SIGTERM)
        try:
            stdout, stderr = process.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            stdout, stderr = process.communicate()
    elapsed_ms = round((time.monotonic() - started) * 1000)
    if auth_destination.exists():
        auth_destination.unlink()
    if timed_out:
        raise AdmissionError(
            f"case {name} exceeded its {timeout_seconds}-second timeout"
        )
    if process.returncode != 0:
        raise AdmissionError(
            f"case {name} exited {process.returncode}; stdout_sha256={hashlib.sha256(stdout).hexdigest()} stderr_sha256={hashlib.sha256(stderr).hexdigest()}"
        )

    report = validate_case_report(paths.report, case.get("report_schema"))
    return {
        "name": name,
        "passed": True,
        "elapsed_ms": elapsed_ms,
        "timeout_seconds": timeout_seconds,
        "exit_status": process.returncode,
        "stdout_sha256": hashlib.sha256(stdout).hexdigest(),
        "stderr_sha256": hashlib.sha256(stderr).hexdigest(),
        "stdout_bytes": len(stdout),
        "stderr_bytes": len(stderr),
        "network": network,
        "isolation_profile": (
            f"bubblewrap-{bwrap_profile}"
            if bwrap_profile is not None
            else "host-process-group"
        ),
        "auth": auth_evidence,
        "report": report,
    }


def secret_scan(root: Path) -> None:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        payload = path.read_bytes()
        for pattern in SECRET_PATTERNS:
            if pattern.search(payload):
                raise AdmissionError(
                    f"secret pattern detected in evidence file {path.name}"
                )


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    path.chmod(0o600)


def ensure_admission_root(root: Path) -> Path:
    resolved = root.resolve()
    allowed = Path("/build/frodex-admission").resolve()
    if resolved == allowed or allowed not in resolved.parents:
        raise AdmissionError(f"admission root must be below {allowed}")
    resolved.mkdir(mode=0o700, parents=True, exist_ok=False)
    if stat.S_IMODE(resolved.stat().st_mode) != 0o700:
        raise AdmissionError("admission root must have mode 0700")
    return resolved


def cleanup_runtime(admission_root: Path, evidence: Path) -> dict[str, Any]:
    removed: list[str] = []
    for name in ("cases", "candidate"):
        target = admission_root / name
        if target.exists():
            shutil.rmtree(target)
            removed.append(name)
    remaining = sorted(
        path.relative_to(admission_root).as_posix()
        for path in admission_root.rglob("*")
        if path.is_file()
    )
    result = {
        "schema_version": SCHEMA_VERSION,
        "removed": removed,
        "remaining_files": remaining,
        "owned_processes_remaining": False,
        "auth_remaining": False,
    }
    write_json(evidence / "cleanup.json", result)
    return result


def admit(args: argparse.Namespace) -> int:
    os.umask(0o077)
    manifest_path = Path(args.manifest).resolve(strict=True)
    archive = Path(args.archive).resolve(strict=True)
    source_repo = Path(args.source_repo).resolve(strict=True)
    owner_inventory_path = Path(args.owner_inventory).resolve(strict=True)
    cases_path = Path(args.cases).resolve(strict=True)
    auth_source = (
        Path(args.auth_source).resolve(strict=True) if args.auth_source else None
    )
    manifest = load_dotslash_manifest(manifest_path)
    platform_name = args.platform or host_platform()
    archive_digest, archive_size = verify_manifest(
        manifest,
        archive,
        args.expected_version,
        args.expected_source,
        platform_name,
    )
    verify_source_checkout(source_repo, args.expected_source)
    queue_dir = Path(args.queue_dir).resolve(strict=True)
    owners = verify_owner_inventory(
        owner_inventory_path, args.expected_source, queue_dir
    )
    root = ensure_admission_root(Path(args.output_root) / archive_digest)
    evidence = root / "evidence"
    evidence.mkdir(mode=0o700)
    bwrap_profile = (
        None if args.no_bwrap else select_bwrap_profile(args.bwrap_profile, root)
    )
    candidate_root = root / "candidate"
    codex, code_mode_host = extract_candidate(archive, candidate_root)
    verify_candidate_version(codex, args.expected_version)
    candidate = Candidate(
        version=args.expected_version,
        source_commit=args.expected_source,
        platform_name=platform_name,
        archive_digest=archive_digest,
        archive_size=archive_size,
        root=candidate_root,
        codex=codex,
        code_mode_host=code_mode_host,
        codex_digest=sha256_file(codex),
        code_mode_host_digest=sha256_file(code_mode_host),
    )
    case_document = load_json(cases_path)
    if (
        not isinstance(case_document, dict)
        or case_document.get("schema_version") != SCHEMA_VERSION
    ):
        raise AdmissionError("case manifest has an unsupported schema")
    cases = case_document.get("cases")
    if not isinstance(cases, list) or not cases:
        raise AdmissionError("case manifest must contain at least one case")

    write_json(
        evidence / "manifest.json",
        {
            "schema_version": SCHEMA_VERSION,
            "version": candidate.version,
            "source_commit": candidate.source_commit,
            "platform": candidate.platform_name,
            "manifest_sha256": sha256_file(manifest_path),
            "archive_sha256": candidate.archive_digest,
            "archive_size": candidate.archive_size,
            "codex_sha256": candidate.codex_digest,
            "code_mode_host_sha256": candidate.code_mode_host_digest,
            "owner_inventory_sha256": sha256_file(owner_inventory_path),
            "case_manifest_sha256": sha256_file(cases_path),
            "isolation_profile": (
                f"bubblewrap-{bwrap_profile}"
                if bwrap_profile is not None
                else "host-process-group"
            ),
        },
    )
    write_json(evidence / "owners.json", owners)

    results: list[dict[str, Any]] = []
    try:
        for case in cases:
            if not isinstance(case, dict):
                raise AdmissionError("case entries must be objects")
            results.append(
                run_case(
                    case,
                    candidate,
                    source_repo,
                    root,
                    auth_source,
                    bwrap_profile,
                )
            )
        write_json(
            evidence / "cases.json",
            {"schema_version": SCHEMA_VERSION, "passed": True, "cases": results},
        )
    finally:
        cleanup_runtime(root, evidence)
    secret_scan(evidence)
    print(f"admitted {candidate.version} {candidate.archive_digest}")
    print(evidence)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--archive", required=True)
    parser.add_argument("--source-repo", required=True)
    parser.add_argument("--expected-source", required=True)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--owner-inventory", required=True)
    parser.add_argument("--queue-dir", required=True)
    parser.add_argument("--cases", required=True)
    parser.add_argument("--output-root", default="/build/frodex-admission")
    parser.add_argument("--platform")
    parser.add_argument("--auth-source")
    parser.add_argument(
        "--bwrap-profile",
        choices=("auto", *BWRAP_PROFILES),
        default="auto",
    )
    parser.add_argument("--no-bwrap", action="store_true", help=argparse.SUPPRESS)
    return parser


def main() -> int:
    try:
        return admit(build_parser().parse_args())
    except (AdmissionError, OSError, subprocess.SubprocessError) as error:
        print(f"frodex admission failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
