#!/usr/bin/env python3
"""Resume a persisted checkpoint fixture before and after app-server restart."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys


THREAD_ID = "00000000-0000-0000-0000-0000000007fa"
ROLLOUT_NAME = f"rollout-2025-01-03T13-42-00-{THREAD_ID}.jsonl"
SENTINEL = "FRODEX_DECIMAL_CHECKPOINT_SENTINEL"


def load_app_server_client(source: Path):
    module_path = source / "scripts/mcp_conformance/run_codex_compliance.py"
    spec = importlib.util.spec_from_file_location(
        "frodex_resume_compliance", module_path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load app-server client from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.AppServerClient


def response_result(response: dict[str, object], method: str) -> dict[str, object]:
    error = response.get("error")
    if error is not None:
        raise RuntimeError(f"{method} failed: {json.dumps(error, sort_keys=True)}")
    result = response.get("result")
    if not isinstance(result, dict):
        raise RuntimeError(f"{method} returned no object result")
    return result


def resume_once(client_type, codex: Path, source: Path) -> None:
    with client_type(
        codex,
        env=dict(os.environ),
        cwd=source,
        timeout_seconds=60,
    ) as client:
        response_result(
            client.request(
                "initialize",
                {
                    "clientInfo": {"name": "frodex-resume-smoke", "version": "1"},
                    "capabilities": {"experimentalApi": True},
                },
            ),
            "initialize",
        )
        client.notify("initialized")
        resumed = response_result(
            client.request("thread/resume", {"threadId": THREAD_ID}),
            "thread/resume",
        )
        thread = resumed.get("thread")
        if not isinstance(thread, dict) or thread.get("id") != THREAD_ID:
            raise RuntimeError("thread/resume returned the wrong thread")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--codex", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()

    home = Path(os.environ["CODEX_HOME"])
    fixture = (
        args.source
        / "scripts/frodex_admission/fixtures/thread_resume_decimal_rate_limit.jsonl"
    )
    fixture_payload = fixture.read_bytes()
    if SENTINEL.encode() not in fixture_payload:
        raise RuntimeError("model-context sentinel is absent from the fixture")
    fixture_digest = hashlib.sha256(fixture_payload).hexdigest()

    session_dir = home / "sessions/2025/01/03"
    session_dir.mkdir(parents=True, mode=0o700)
    shutil.copyfile(fixture, session_dir / ROLLOUT_NAME)
    (home / "config.toml").write_text(
        'model = "test-model"\n'
        'model_provider = "test-provider"\n'
        '[model_providers."test-provider"]\n'
        'name = "Test provider"\n'
        'base_url = "http://127.0.0.1:9"\n'
        'wire_api = "responses"\n',
        encoding="utf-8",
    )

    client_type = load_app_server_client(args.source)
    resume_once(client_type, args.codex, args.source)
    resume_once(client_type, args.codex, args.source)

    args.report.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "passed": True,
                "thread_resume": True,
                "restart_thread_resume": True,
                "model_context_sentinel": SENTINEL,
                "fixture_sha256": fixture_digest,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
