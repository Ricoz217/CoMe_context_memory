from __future__ import annotations

import os
import shutil
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from context_memory import ContextMemoryConfig, ContextMemoryEngineV3
from context_memory.config import get_llm


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _env_true(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _ensure_real_llm_ready() -> None:
    if not _env_true("COME_RUN_REAL_LLM_SMOKE", True):
        pytest.skip("real llm smoke disabled by COME_RUN_REAL_LLM_SMOKE")
    preset = get_llm("CONTEXT_MEMORY")
    token = str(getattr(preset, "token", "")).strip()
    endpoint = str(getattr(preset, "endpoint", "")).strip()
    if not token:
        pytest.skip("llm preset CONTEXT_MEMORY has empty token")
    if not endpoint:
        pytest.skip("llm preset CONTEXT_MEMORY has empty endpoint")


@pytest.fixture(scope="session")
def smoke_runtime_dir() -> Path:
    runtime_root = _repo_root() / "tests_data" / "real_llm_ci_runtime"
    runtime_dir = runtime_root / f"run_{uuid.uuid4().hex[:10]}"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    return runtime_dir


@pytest.fixture(scope="session")
def smoke_input_dir(smoke_runtime_dir: Path) -> Path:
    input_dir = smoke_runtime_dir / "inputs"
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "sample_file_cache.py").write_text(
        (
            "from pathlib import Path\n"
            "import json\n"
            "\n"
            "def write_cache(name: str, data: bytes, root: Path) -> Path:\n"
            "    target = root / name\n"
            "    target.parent.mkdir(parents=True, exist_ok=True)\n"
            "    target.write_bytes(data)\n"
            "    return target\n"
            "\n"
            "def write_meta(meta_path: Path, payload: dict) -> None:\n"
            "    meta_path.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')\n"
        ),
        encoding="utf-8",
    )
    batch_flat = input_dir / "batch_flat"
    batch_flat.mkdir(parents=True, exist_ok=True)
    (batch_flat / "a.txt").write_text("cache write path and metadata update", encoding="utf-8")
    (batch_flat / "b.txt").write_text("renew and get file path for cache entry", encoding="utf-8")
    batch_tree = input_dir / "batch_tree"
    (batch_tree / "module_a").mkdir(parents=True, exist_ok=True)
    (batch_tree / "module_b").mkdir(parents=True, exist_ok=True)
    (batch_tree / "root.txt").write_text("root level cache notes", encoding="utf-8")
    (batch_tree / "module_a" / "alpha.txt").write_text("alpha module cache add_file", encoding="utf-8")
    (batch_tree / "module_b" / "beta.txt").write_text(
        "beta module metadata and remove flow",
        encoding="utf-8",
    )
    return input_dir


@pytest.fixture(scope="session")
async def engine(smoke_runtime_dir: Path) -> ContextMemoryEngineV3:
    _ensure_real_llm_ready()
    store_dir = smoke_runtime_dir / "store"
    if store_dir.exists():
        shutil.rmtree(store_dir, ignore_errors=True)
    cfg = ContextMemoryConfig(
        base_dir=store_dir,
        llm_preset="CONTEXT_MEMORY",
        image_llm_preset="KIMI2.6",
        ask_timeout=300.0,
        use_mock_llm=False,
        enable_cleaning=True,
        auto_manage=False,
    )
    return ContextMemoryEngineV3(config=cfg)


@pytest.mark.asyncio
async def test_real_llm_ci_full_interface_smoke(
    engine: ContextMemoryEngineV3,
    smoke_input_dir: Path,
) -> None:
    root = await engine.set_bucket("CI_REAL_LLM_SMOKE")
    assert root.bucket_id

    switched = await engine.set_active_bucket(root.bucket_id)
    assert switched.get("success") is True
    switched_alias = await engine.switch_active_bucket(root.bucket_id)
    assert switched_alias.get("success") is True

    base_add = await root.add_memory(
        "file cache writes content to disk and updates metadata atomically",
        topic="core",
    )
    assert base_add.success is True
    assert base_add.added_keys
    primary_key = base_add.added_keys[0]

    extra_add = await engine.add_memory(
        "renew API updates use_time for a cached file record",
        topic="renew",
        bucket_id=root.bucket_id,
    )
    assert extra_add.success is True

    child = await root.create_child_bucket(
        title="Public API Bucket",
        summary="cache public operations",
        content="add_file renew remove",
    )
    assert child.bucket_id
    assert child.bucket_id in root

    moved = await root.move_item(primary_key, target_bucket_id=child.bucket_id, reason="smoke_move")
    assert moved.success is True

    query_res = await root.query("how does cache write happen", top_k=5, mode="auto")
    assert query_res.success is True
    assert isinstance(query_res.matches, list)

    child_handle = engine.get_bucket(child.bucket_id)
    child_query = await child_handle.query("where is add file logic", top_k=3)
    assert child_query.success is True

    updated = await engine.update_memory(
        primary_key,
        "add_file writes bytes and then updates metadata with file info",
    )
    assert updated.success is True

    gray_on = await engine.set_gray(primary_key, gray=True, reason="smoke_gray_on")
    assert gray_on.success is True
    gray_off = await engine.set_gray(primary_key, gray=False, reason="smoke_gray_off")
    assert gray_off.success is True

    add_file_result = await root.add_memory_from_file(
        str(smoke_input_dir / "sample_file_cache.py"),
        topic="file_cache_source",
        force_split=True,
        dedup_in_bucket=True,
    )
    assert add_file_result.success is True
    assert add_file_result.added_keys

    add_dir_flat = await root.add_memory_from_dir(
        str(smoke_input_dir / "batch_flat"),
        auto_create_sub_buckets=False,
        force_split=True,
        dedup_in_bucket=True,
    )
    assert add_dir_flat.get("success") is True
    assert int(add_dir_flat.get("success_count", 0)) >= 1
    assert isinstance(add_dir_flat.get("added_keys", []), list)

    add_dir_tree = await root.add_memory_from_dir(
        str(smoke_input_dir / "batch_tree"),
        auto_create_sub_buckets=True,
        force_split=True,
        dedup_in_bucket=True,
    )
    assert add_dir_tree.get("success") is True
    assert int(add_dir_tree.get("success_count", 0)) >= 1

    listed = await root.list_memories(include_gray=False, include_content=False)
    assert listed.get("bucket_id")
    assert int(listed.get("memory_count", 0)) >= 1
    assert int(listed.get("total_memory_count", 0)) >= int(listed.get("memory_count", 0))

    iter_records = [rec async for rec in root]
    assert len(iter_records) >= 1
    assert primary_key in child_handle

    bucket_usage = await root.get_bucket_context_usage()
    assert "estimated_tokens" in bucket_usage

    summary_refresh = await root.refresh_bucket_summary(force=True)
    assert isinstance(summary_refresh, dict)

    split_result = await root.split_bucket(reason="smoke_split")
    assert bool(split_result.get("success", True)) is True

    optimize_result = await root.optimize(reason="smoke_optimize")
    assert hasattr(optimize_result, "reason_code")

    compress_result = await root.force_compress(reason="smoke_compress")
    assert hasattr(compress_result, "success")

    latest_root = await root.latest_bucket_id()
    assert latest_root
    latest_engine = await engine.latest_bucket_id(root.bucket_id)
    assert latest_engine

    switched_from_handle = await root.set_active_bucket()
    assert switched_from_handle.get("success") is True

    mem_record = await root.get_memory(add_file_result.added_keys[0], with_evidence=True)
    assert mem_record is not None
    exported = await root.export_memory_to_markdown(mem_record.key)
    assert bool(exported.get("success")) is True
    md_path = Path(str(exported.get("path", "")))
    assert md_path.is_file()

    evidence_text = await root.get_evidence_content(mem_record.key)
    assert isinstance(evidence_text, str)

    deleted = await root.delete_memory(mem_record, reason="smoke_delete_obj")
    assert deleted.success is True

    dry_gc = await root.gc_storage(dry_run=True, reason="smoke_gc")
    assert dry_gc.success is True

    cleanup = await root.cleanup_expired()
    assert cleanup.success is True

    migrated = await root.migrate_storage_paths_to_relative()
    assert isinstance(migrated, dict)

    stats = await root.stats()
    assert int(getattr(stats, "bucket_total", 0)) >= 1
    assert int(getattr(stats, "total_keys", 0)) >= 1

    buckets = root.list_buckets()
    assert len(buckets) >= 1
