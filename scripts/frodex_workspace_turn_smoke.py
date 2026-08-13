#!/usr/bin/env python3
"""Require an ordinary user turn after workspace.set_cwd in the packaged app-server."""

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import threading

from frodex_thread_resume_smoke import load_app_server_client, response_result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--codex", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    home = Path(os.environ["CODEX_HOME"])
    primary = Path.cwd() / "primary"
    linked = Path.cwd() / "linked"
    primary.mkdir()
    for command in (
        ["init", "-q"],
        [
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "--allow-empty",
            "-qm",
            "fixture",
        ],
        ["worktree", "add", "-q", "-b", "linked", str(linked)],
    ):
        subprocess.run(
            ["git", "-C", str(primary), *command],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    primary, linked = primary.resolve(), linked.resolve()
    instructions = "FRODEX_LINKED_WORKTREE_INSTRUCTIONS"
    (linked / "AGENTS.md").write_text(instructions + "\n")
    requests = []

    class ModelHandler(BaseHTTPRequestHandler):
        """Mock inference separately from the provider's turn-cost endpoint."""

        def log_message(self, *_args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if self.path == "/v1/analytics/codex/turn-costs":
                response = b'{"turns":[]}'
                content_type = "application/json"
            elif self.path == "/v1/responses":
                requests.append(body)
                number = len(requests)
                item = (
                    {
                        "type": "function_call",
                        "call_id": "switch-cwd",
                        "namespace": "workspace",
                        "name": "set_cwd",
                        "arguments": json.dumps({"path": str(linked)}),
                    }
                    if number == 1
                    else {
                        "type": "message",
                        "id": f"msg-{number}",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "continued"}],
                    }
                )
                events = [
                    {"type": "response.created", "response": {"id": f"resp-{number}"}},
                    {"type": "response.output_item.done", "item": item},
                    {
                        "type": "response.completed",
                        "response": {"id": f"resp-{number}"},
                    },
                ]
                response = "".join(
                    f"event: {e['type']}\ndata: {json.dumps(e)}\n\n" for e in events
                ).encode()
                content_type = "text/event-stream"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

    server = ThreadingHTTPServer(("127.0.0.1", 0), ModelHandler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    port = server.server_port
    (home / "config.toml").write_text(f"""model = "mock-model"
model_provider = "mock_provider"
approval_policy = "never"
sandbox_mode = "read-only"
[features]
workspace_cwd_tool = true
[model_providers.mock_provider]
name = "Workspace admission mock"
base_url = "http://127.0.0.1:{port}/v1"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
""")
    try:
        client_type = load_app_server_client(args.source)
        with client_type(
            args.codex, env=dict(os.environ), cwd=primary, timeout_seconds=30
        ) as client:
            response_result(
                client.request(
                    "initialize",
                    {
                        "clientInfo": {
                            "name": "frodex-workspace-admission",
                            "version": "1",
                        },
                        "capabilities": {"experimentalApi": True},
                    },
                ),
                "initialize",
            )
            client.notify("initialized")
            thread = response_result(
                client.request(
                    "thread/start",
                    {
                        "cwd": str(primary),
                        "historyMode": "paginated",
                    },
                ),
                "thread/start",
            )["thread"]["id"]
            for text, environments in (
                ("switch to the linked worktree", None),
                (
                    "continue in the linked worktree",
                    [
                        {
                            "environmentId": "local",
                            "cwd": str(linked),
                            "runtimeWorkspaceRoots": [str(linked)],
                        }
                    ],
                ),
            ):
                started = response_result(
                    client.request(
                        "turn/start",
                        {
                            "threadId": thread,
                            "input": [
                                {"type": "text", "text": text, "textElements": []}
                            ],
                            "environments": environments,
                        },
                    ),
                    "turn/start",
                )
                completed = client.wait_for_notification(
                    "turn/completed",
                    predicate=lambda params: (
                        params.get("threadId") == thread
                        and params.get("turn", {}).get("id") == started["turn"]["id"]
                    ),
                )
                assert completed["params"]["turn"]["status"] == "completed", completed
            read = response_result(
                client.request(
                    "thread/read",
                    {
                        "threadId": thread,
                        "includeTurns": True,
                    },
                ),
                "thread/read",
            )
        assert len(requests) == 3, len(requests)
        switch = next(
            item
            for item in requests[1]["input"]
            if item.get("type") == "function_call_output"
            and item.get("call_id") == "switch-cwd"
        )
        result = json.loads(switch["output"])
        assert result["changed"] is True and Path(result["cwd"]) == linked, result
        assert instructions in json.dumps(requests[2]["input"])
        assert "continue in the linked worktree" in json.dumps(requests[2]["input"])
        assert Path(read["thread"]["cwd"]) == linked
        assert len(read["thread"]["turns"]) == 2
        args.report.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "passed": True,
                    "assertions": [
                        "workspace_switch_completed",
                        "next_client_turn_completed",
                        "linked_instructions_preserved",
                        "linked_cwd_persisted",
                        "both_user_turns_visible",
                    ],
                },
                indent=2,
            )
            + "\n"
        )
    finally:
        server.shutdown()
        server.server_close()
        worker.join()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
