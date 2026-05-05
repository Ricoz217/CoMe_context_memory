from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import requests


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _python_bin() -> str:
    venv_python = _repo_root() / ".venv" / "Scripts" / "python.exe"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def _build_env() -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(_repo_root() / "src")
    return env


def run_rpc_smoke(*, base_dir: Path, real_llm: bool, port: int) -> dict[str, object]:
    base_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        _python_bin(),
        "-m",
        "come_context_memory.rpc_server",
        "--host",
        "127.0.0.1",
        "--port",
        str(int(port)),
        "--base-dir",
        str(base_dir),
        "--no-debug-mode",
    ]
    if not real_llm:
        cmd.append("--mock")

    proc = subprocess.Popen(
        cmd,
        cwd=str(_repo_root()),
        env=_build_env(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        health_url = f"http://127.0.0.1:{int(port)}/healthz"
        ok = False
        for _ in range(80):
            try:
                resp = requests.get(health_url, timeout=1.0)
                if resp.status_code == 200:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(0.25)
        if not ok:
            return {
                "success": False,
                "real_llm": bool(real_llm),
                "base_dir": str(base_dir),
                "port": int(port),
                "error": "rpc server failed to start",
            }

        rpc_url = f"http://127.0.0.1:{int(port)}/jsonrpc"

        def call(req_id: int, method: str, params: dict[str, object]) -> dict[str, object]:
            payload = {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}
            r = requests.post(rpc_url, json=payload, timeout=180)
            r.raise_for_status()
            return r.json()

        ping = call(1, "ping", {})
        list_before = call(2, "list_memories", {"include_gray": False})
        add = call(3, "add_memory", {"raw_text": "rpc smoke memory for cache write", "topic": "rpc_smoke"})
        query = call(4, "query", {"query_text": "cache write", "top_k": 1, "mode": "auto"})
        optimize = call(5, "optimize", {"reason": "script_rpc_smoke"})

        payload = {
            "success": True,
            "real_llm": bool(real_llm),
            "base_dir": str(base_dir),
            "port": int(port),
            "responses": {
                "ping": ping,
                "list_before": list_before,
                "add": add,
                "query": query,
                "optimize": optimize,
            },
        }
        for key in ("ping", "list_before", "add", "query", "optimize"):
            if "error" in payload["responses"][key]:
                payload["success"] = False
                break
        return payload
    except Exception as exc:
        return {
            "success": False,
            "real_llm": bool(real_llm),
            "base_dir": str(base_dir),
            "port": int(port),
            "error": str(exc),
        }
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def main() -> None:
    parser = argparse.ArgumentParser(description="CoMe JSON-RPC smoke script")
    parser.add_argument(
        "--base-dir",
        default=str(_repo_root() / "tests_data" / "script_rpc_smoke"),
        help="runtime storage directory",
    )
    parser.add_argument("--real-llm", action="store_true", help="use real llm instead of --mock")
    parser.add_argument("--port", type=int, default=19012)
    args = parser.parse_args()

    result = run_rpc_smoke(
        base_dir=Path(args.base_dir),
        real_llm=bool(args.real_llm),
        port=int(args.port),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if not result["success"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
