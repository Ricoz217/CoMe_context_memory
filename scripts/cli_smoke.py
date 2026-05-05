from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


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


def run_cli_smoke(*, base_dir: Path, real_llm: bool, timeout_sec: int) -> dict[str, object]:
    base_dir.mkdir(parents=True, exist_ok=True)
    commands = [
        "add cli smoke memory for cache write",
        "query cache write --top-k 1 --mode auto",
        "optimize",
        "stats",
        "exit",
    ]
    cmd = [
        _python_bin(),
        "-m",
        "come_context_memory.cli",
        "--base-dir",
        str(base_dir),
        "--no-debug-mode",
    ]
    if not real_llm:
        cmd.append("--mock")

    proc = subprocess.run(
        cmd,
        cwd=str(_repo_root()),
        env=_build_env(),
        input=("\n".join(commands) + "\n").encode("utf-8"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=max(30, int(timeout_sec)),
    )
    stdout = proc.stdout.decode("utf-8", errors="ignore")
    stderr = proc.stderr.decode("utf-8", errors="ignore")
    ok = proc.returncode == 0 and '"success": true' in stdout.lower() and '"bucket_total"' in stdout
    return {
        "success": bool(ok),
        "returncode": int(proc.returncode),
        "real_llm": bool(real_llm),
        "base_dir": str(base_dir),
        "stdout": stdout,
        "stderr": stderr,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="CoMe CLI smoke script")
    parser.add_argument(
        "--base-dir",
        default=str(_repo_root() / "tests_data" / "script_cli_smoke"),
        help="runtime storage directory",
    )
    parser.add_argument("--real-llm", action="store_true", help="use real llm instead of --mock")
    parser.add_argument("--timeout-sec", type=int, default=600)
    args = parser.parse_args()

    result = run_cli_smoke(
        base_dir=Path(args.base_dir),
        real_llm=bool(args.real_llm),
        timeout_sec=int(args.timeout_sec),
    )
    payload = json.dumps(result, ensure_ascii=False, indent=2)
    try:
        print(payload)
    except UnicodeEncodeError:
        safe = payload.encode(sys.stdout.encoding or "utf-8", errors="replace").decode(
            sys.stdout.encoding or "utf-8",
            errors="replace",
        )
        print(safe)
    if not result["success"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
