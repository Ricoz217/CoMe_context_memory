from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from come_context_memory import ContextMemoryConfig, ContextMemoryEngineV3
from come_context_memory.config import get_llm


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _runtime_root() -> Path:
    root = _repo_root() / "tests_data" / "release_smoke_runtime"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _env_true(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _use_real_llm() -> bool:
    return _env_true("COME_RELEASE_SMOKE_REAL_LLM", False)


def _ensure_real_llm_ready() -> None:
    preset = get_llm("CONTEXT_MEMORY")
    token = str(getattr(preset, "token", "")).strip()
    endpoint = str(getattr(preset, "endpoint", "")).strip()
    if not token or not endpoint:
        pytest.skip("real llm smoke requested but CONTEXT_MEMORY preset is not ready")


def _python_bin() -> str:
    venv_python = _repo_root() / ".venv" / "Scripts" / "python.exe"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def _base_env() -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(_repo_root() / "src")
    return env


@pytest.fixture(scope="session")
def smoke_runtime_dir() -> Path:
    run_dir = _runtime_root() / f"run_{uuid.uuid4().hex[:10]}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


@pytest.fixture(scope="session")
def smoke_text_file(smoke_runtime_dir: Path) -> Path:
    file_path = smoke_runtime_dir / "smoke_input.txt"
    file_path.write_text(
        "cache write function stores bytes and updates metadata for retrieval",
        encoding="utf-8",
    )
    return file_path


@pytest.mark.asyncio
async def test_release_smoke_python_api(smoke_runtime_dir: Path, smoke_text_file: Path) -> None:
    if _use_real_llm():
        _ensure_real_llm_ready()

    api_store = smoke_runtime_dir / "api_store"
    if api_store.exists():
        shutil.rmtree(api_store, ignore_errors=True)

    cfg = ContextMemoryConfig(
        base_dir=api_store,
        llm_preset="CONTEXT_MEMORY",
        image_llm_preset="KIMI2.6",
        ask_timeout=300.0,
        use_mock_llm=not _use_real_llm(),
        enable_cleaning=True,
        init_config=False,
        auto_manage=False,
        max_context_window=1_000_000,
    )
    engine = ContextMemoryEngineV3(config=cfg)

    root = await engine.set_bucket("RELEASE_SMOKE_API")
    assert root.bucket_id

    add_text = await root.add_memory("api smoke memory about cache write and metadata", topic="api")
    assert add_text.success is True
    assert add_text.added_keys

    add_file = await root.add_memory_from_file(str(smoke_text_file), topic="api_file", force_split=True)
    assert add_file.success is True
    assert add_file.added_keys

    listed = await root.list_memories(include_gray=False, include_content=False)
    assert int(listed.get("total_memory_count", 0)) >= 1

    query = await root.query("cache write metadata", top_k=3, mode="auto")
    assert query.success is True
    assert isinstance(query.matches, list)

    optimize = await root.optimize(reason="release_smoke_api")
    assert hasattr(optimize, "success")

    compress = await root.force_compress(reason="release_smoke_api")
    assert hasattr(compress, "success")

    stats = await root.stats()
    assert int(getattr(stats, "bucket_total", 0)) >= 1


def test_release_smoke_cli(smoke_runtime_dir: Path) -> None:
    cli_store = smoke_runtime_dir / "cli_store"
    cli_store.mkdir(parents=True, exist_ok=True)

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
        str(cli_store),
        "--no-debug-mode",
    ]
    if not _use_real_llm():
        cmd.append("--mock")
    else:
        _ensure_real_llm_ready()

    payload = ("\n".join(commands) + "\n").encode("utf-8")
    proc = subprocess.run(
        cmd,
        cwd=str(_repo_root()),
        env=_base_env(),
        input=payload,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=600,
    )
    if proc.returncode != 0:
        raise AssertionError(
            "cli smoke failed\n"
            f"returncode={proc.returncode}\n"
            f"stdout={proc.stdout.decode('utf-8', errors='ignore')}\n"
            f"stderr={proc.stderr.decode('utf-8', errors='ignore')}"
        )

    out = proc.stdout.decode("utf-8", errors="ignore")
    assert '"success": true' in out.lower()
    assert '"bucket_total"' in out


def test_release_smoke_jsonrpc(smoke_runtime_dir: Path) -> None:
    rpc_store = smoke_runtime_dir / "rpc_store"
    rpc_store.mkdir(parents=True, exist_ok=True)

    port = 19010
    cmd = [
        _python_bin(),
        "-m",
        "come_context_memory.rpc_server",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--base-dir",
        str(rpc_store),
        "--no-debug-mode",
    ]
    if not _use_real_llm():
        cmd.append("--mock")
    else:
        _ensure_real_llm_ready()

    proc = subprocess.Popen(
        cmd,
        cwd=str(_repo_root()),
        env=_base_env(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        health_url = f"http://127.0.0.1:{port}/healthz"
        ok = False
        for _ in range(60):
            try:
                r = requests.get(health_url, timeout=1.0)
                if r.status_code == 200:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(0.25)
        assert ok, "rpc server failed to start"

        rpc_url = f"http://127.0.0.1:{port}/jsonrpc"

        def call(req_id: int, method: str, params: dict[str, object]) -> dict[str, object]:
            payload = {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}
            resp = requests.post(rpc_url, json=payload, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            assert "error" not in data, json.dumps(data, ensure_ascii=False)
            return data

        ping = call(1, "ping", {})
        assert bool(ping.get("result", {}).get("pong")) is True

        add = call(2, "add_memory", {"raw_text": "rpc smoke memory for cache write", "topic": "rpc"})
        add_res = add.get("result", {})
        assert bool(add_res.get("success")) is True

        query = call(3, "query", {"query_text": "cache write", "top_k": 1, "mode": "auto"})
        query_res = query.get("result", {})
        assert bool(query_res.get("success")) is True
        assert isinstance(query_res.get("matches", []), list)

        optimize = call(4, "optimize", {"reason": "release_smoke_rpc"})
        assert "result" in optimize

        stats = call(5, "stats", {})
        stats_res = stats.get("result", {})
        assert int(stats_res.get("bucket_total", 0)) >= 1
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

