from __future__ import annotations

import ast
import asyncio
import threading
import time
from pathlib import Path
from unittest.mock import AsyncMock, patch

from context_memory.memory import aliasing as aliasing_module
from context_memory.memory import engine as memory_engine
from context_memory.memory.models import MemoryRecord, normalize_relations


_BLOCKING_ALIAS_METHODS = {
    "assert_safe",
    "decode_tree",
    "encode_many",
    "encode_tree",
    "freeze",
    "map_version",
    "prepare",
    "resolve_many",
    "restore",
    "snapshot_hash",
    "to_alias",
    "to_real",
    "to_real_many",
}


def _create_engine(base_dir: Path):
    with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096):
        return memory_engine.ContextMemoryEngineV3(
            base_dir=base_dir,
            use_mock_llm=True,
            init_config=False,
            auto_manage=False,
            auto_resume_pending_jobs=False,
        )


def _record(engine, bucket_id: str, index: int) -> MemoryRecord:
    label = f"alias-performance-{index}"
    return MemoryRecord(
        key=engine.storage.generate_key(),
        revision_id=engine.storage.generate_revision_id(),
        kind="memory",
        bucket_id=bucket_id,
        title=label,
        summary=label,
        content=f"content:{label}",
        weight=0.5,
        event="ADD",
        gray=False,
        relations=normalize_relations({}),
        confidence_type="common",
    )


def test_transactional_batch_resolve_loads_one_alias_snapshot(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    try:
        root = engine.root_bucket_id()
        table = engine._alias_table(root)
        real_ids = [engine.storage.generate_key() for _ in range(12)]
        aliases = [table.to_alias(real_id) for real_id in real_ids]
        original_load = engine.storage.load_alias_map

        with patch.object(engine.storage, "load_alias_map", wraps=original_load) as load_map:
            resolved = table.to_real_many(aliases, strict=True)

        assert resolved == dict(zip(aliases, real_ids, strict=True))
        assert load_map.call_count == 1
    finally:
        engine.shutdown(wait=True)


def test_alias_write_transaction_uses_delta_without_deepcopy(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    try:
        root = engine.root_bucket_id()
        table = engine._alias_table(root)
        real_ids = [engine.storage.generate_key() for _ in range(8)]
        original_commit = engine.storage.commit_alias_map

        with (
            patch.object(
                aliasing_module.copy,
                "deepcopy",
                side_effect=AssertionError("runtime alias transaction deep-copied the full map"),
            ),
            patch.object(engine.storage, "commit_alias_map", wraps=original_commit) as commit_map,
        ):
            encoded = table.encode_many(real_ids, allow_create=True, strict=True)
            read_only = table.encode_many(real_ids, allow_create=False, strict=True)

        assert [value for success, value in encoded if success] == [
            f"memory_{index}" for index in range(1, 9)
        ]
        assert read_only == encoded
        assert commit_map.call_count == 1
        assert table.map_version() == 9
    finally:
        engine.shutdown(wait=True)


def test_query_alias_work_is_batched_and_never_blocks_event_loop(tmp_path: Path) -> None:
    asyncio.run(_test_query_alias_work_is_batched_and_never_blocks_event_loop(tmp_path))


async def _test_query_alias_work_is_batched_and_never_blocks_event_loop(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    try:
        root = engine.root_bucket_id()
        records = [_record(engine, root, index) for index in range(16)]
        for record in records:
            await engine._run_storage_task(engine.storage.write_memory_record, record)

        prepared = await engine.prepare_alias_payload(
            root,
            {"records": [record.to_dict() for record in records]},
        )
        first_alias = prepared["records"][0]["key"]
        event_loop_thread = threading.get_ident()
        load_threads: list[int] = []
        original_load = engine.storage.load_alias_map

        def slow_load(bucket_id: str):
            load_threads.append(threading.get_ident())
            time.sleep(0.02)
            return original_load(bucket_id)

        loop_gaps: list[float] = []

        async def heartbeat() -> None:
            last = time.perf_counter()
            for _ in range(30):
                await asyncio.sleep(0.005)
                now = time.perf_counter()
                loop_gaps.append(now - last)
                last = now

        llm_result = {
            "answer": "ok",
            "matches": [
                {
                    "key": first_alias,
                    "score": 1.0,
                    "reason": "match",
                    "summary": "match",
                }
            ],
        }
        with (
            patch.object(engine.storage, "load_alias_map", side_effect=slow_load) as load_map,
            patch.object(engine.pipeline, "query", new=AsyncMock(return_value=llm_result)),
        ):
            result, _ = await asyncio.gather(
                engine.query(
                    "alias performance",
                    bucket_id=root,
                    top_k=1,
                    use_cache=False,
                    mode="semantic",
                    global_recall_top_n=1,
                ),
                heartbeat(),
            )

        assert result.success is True
        assert result.matches and result.matches[0].key == records[0].key
        assert load_map.call_count <= 4
        assert load_threads and all(thread_id != event_loop_thread for thread_id in load_threads)
        assert loop_gaps and max(loop_gaps) < 0.06
    finally:
        await engine.close(wait=True)


def test_async_business_methods_never_call_blocking_alias_methods_directly() -> None:
    memory_root = Path(memory_engine.__file__).resolve().parent
    business_files = [
        memory_root / "engine.py",
        *sorted((memory_root / "services").glob("*.py")),
    ]
    violations: list[str] = []
    for path in business_files:
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        for function in ast.walk(tree):
            if not isinstance(function, ast.AsyncFunctionDef):
                continue
            for node in ast.walk(function):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in _BLOCKING_ALIAS_METHODS
                ):
                    violations.append(
                        f"{path.name}:{node.lineno}:{function.name}:{node.func.attr}"
                    )

    assert violations == []
