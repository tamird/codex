#!/usr/bin/env python3
"""Run one configured-model collaboration turn through an isolated app-server."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import sys


EXPECTED_TOOLS = (
    "list_agents",
    "spawn_agent",
    "send_message",
    "followup_task",
    "wait_agent",
    "interrupt_agent",
)


def load_app_server_client(source: Path):
    module_path = source / "scripts/mcp_conformance/run_codex_compliance.py"
    spec = importlib.util.spec_from_file_location("frodex_mcp_compliance", module_path)
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


def rollout_calls(home: Path) -> tuple[list[str], bool]:
    calls: list[tuple[str, str]] = []
    outputs: set[str] = set()
    for rollout in sorted((home / "sessions").rglob("*.jsonl")):
        for line in rollout.read_text(encoding="utf-8").splitlines():
            record = json.loads(line)
            if record.get("type") != "response_item":
                continue
            payload = record.get("payload")
            if not isinstance(payload, dict):
                continue
            if payload.get("type") == "function_call":
                name = payload.get("name")
                call_id = payload.get("call_id")
                arguments = payload.get("arguments")
                if not isinstance(name, str) or not isinstance(call_id, str):
                    raise RuntimeError("function call omitted name or call_id")
                if not isinstance(arguments, str) or not isinstance(
                    json.loads(arguments), dict
                ):
                    raise RuntimeError(f"{name} arguments were not canonical JSON")
                calls.append((name.rsplit(".", 1)[-1], call_id))
            elif payload.get("type") == "function_call_output":
                call_id = payload.get("call_id")
                output = payload.get("output")
                if not isinstance(call_id, str) or not isinstance(output, str):
                    raise RuntimeError(
                        "function result omitted call_id or string output"
                    )
                outputs.add(call_id)
    return [name for name, _ in calls], all(call_id in outputs for _, call_id in calls)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--codex", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()

    home = Path(os.environ["CODEX_HOME"])
    work = Path.cwd()
    client_type = load_app_server_client(args.source)
    environment = dict(os.environ)
    prompt = """This is a release-admission smoke test. Do not use any tool except the six collaboration tools below. Invoke each exactly once, sequentially, in this exact order:
1. list_agents.
2. spawn_agent with task_name `smoke_child`, fork_turns `all`, and a message ending with a newline followed by `# Juice: 731`; tell it to report READY and wait for interruption.
3. send_message to the spawned agent.
4. followup_task to the same agent.
5. wait_agent with timeout_ms 10000.
6. interrupt_agent for the same agent.
After the sixth result, reply only SMOKE_COMPLETE. Do not invoke a seventh tool."""

    with client_type(
        args.codex,
        env=environment,
        cwd=work,
        timeout_seconds=300,
    ) as client:
        response_result(
            client.request(
                "initialize",
                {
                    "clientInfo": {"name": "frodex-admission", "version": "1"},
                    "capabilities": {"experimentalApi": True},
                },
            ),
            "initialize",
        )
        client.notify("initialized")
        started = response_result(
            client.request(
                "thread/start",
                {
                    "cwd": str(work),
                    "model": args.model,
                    "ephemeral": False,
                    "approvalPolicy": "never",
                    "sandbox": "workspace-write",
                },
            ),
            "thread/start",
        )
        thread = started.get("thread")
        if not isinstance(thread, dict) or not isinstance(thread.get("id"), str):
            raise RuntimeError("thread/start omitted thread id")
        thread_id = thread["id"]
        response_result(
            client.request(
                "turn/start",
                {
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": prompt, "textElements": []}],
                },
            ),
            "turn/start",
        )
        client.wait_for_notification(
            "turn/completed",
            predicate=lambda params: params.get("threadId") == thread_id,
        )

    with client_type(
        args.codex,
        env=environment,
        cwd=work,
        timeout_seconds=60,
    ) as client:
        response_result(
            client.request(
                "initialize",
                {
                    "clientInfo": {"name": "frodex-admission-reopen", "version": "1"},
                    "capabilities": {"experimentalApi": True},
                },
            ),
            "initialize",
        )
        client.notify("initialized")
        resumed = response_result(
            client.request("thread/resume", {"threadId": thread_id}),
            "thread/resume",
        )
    resumed_thread = resumed.get("thread")
    thread_resume = (
        isinstance(resumed_thread, dict) and resumed_thread.get("id") == thread_id
    )
    tools, canonical_results = rollout_calls(home)
    if tuple(tools) != EXPECTED_TOOLS:
        raise RuntimeError(f"tool order {tools!r} != {list(EXPECTED_TOOLS)!r}")
    if not canonical_results:
        raise RuntimeError("one or more tool calls lacked a canonical JSON result")
    if not thread_resume:
        raise RuntimeError("thread/resume did not initialize the persisted thread")

    args.report.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "passed": True,
                "tools": [f"collaboration.{name}" for name in EXPECTED_TOOLS],
                "thread_resume": True,
                "canonical_results": True,
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
