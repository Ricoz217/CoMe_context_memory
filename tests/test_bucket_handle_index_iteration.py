from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from unittest.mock import patch

from context_memory.memory import engine as memory_engine


def _create_engine(base_dir: Path):
    with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096):
        return memory_engine.ContextMemoryEngineV3(
            base_dir=base_dir,
            use_mock_llm=True,
            init_config=False,
            auto_manage=False,
            auto_resume_pending_jobs=False,
        )


def test_bucket_handle_iterator_loads_records_lazily_off_event_loop(tmp_path: Path) -> None:
    asyncio.run(_test_bucket_handle_iterator_loads_records_lazily_off_event_loop(tmp_path))


async def _test_bucket_handle_iterator_loads_records_lazily_off_event_loop(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    handle = await engine.set_bucket("iterator")
    first_added = await handle.add_memory("first memory")
    await handle.add_memory("second memory")
    await handle.create_bucket(title="child")

    event_loop_thread = threading.get_ident()
    load_threads: list[int] = []
    original_loader = engine.storage._json_to_memory_record

    def tracked_loader(path: Path):
        load_threads.append(threading.get_ident())
        return original_loader(path)

    with patch.object(engine.storage, "_json_to_memory_record", side_effect=tracked_loader):
        iterator = handle.__aiter__()
        assert load_threads == []

        first = await anext(iterator)
        assert first.key == first_added.key
        assert len(load_threads) == 1
        assert load_threads[0] != event_loop_thread

        await iterator.aclose()
        assert len(load_threads) == 1


def test_bucket_handle_contains_uses_index_without_loading_record_files(tmp_path: Path) -> None:
    asyncio.run(_test_bucket_handle_contains_uses_index_without_loading_record_files(tmp_path))


async def _test_bucket_handle_contains_uses_index_without_loading_record_files(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    handle = await engine.set_bucket("contains")
    added = await handle.add_memory("indexed memory")
    child = await handle.create_bucket(title="child")

    with patch.object(
        engine.storage,
        "_json_to_memory_record",
        side_effect=AssertionError("membership must not load record files"),
    ):
        assert added.key in handle
        assert child.bucket_id in handle
        assert "missing-key" not in handle


def test_bucket_handle_iterator_preserves_memory_then_bucket_order(tmp_path: Path) -> None:
    asyncio.run(_test_bucket_handle_iterator_preserves_memory_then_bucket_order(tmp_path))


async def _test_bucket_handle_iterator_preserves_memory_then_bucket_order(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    handle = await engine.set_bucket("order")
    first = await handle.add_memory("first")
    second = await handle.add_memory("second")
    child = await handle.create_bucket(title="child")

    records = [record async for record in handle]

    assert [record.key for record in records[:2]] == [first.key, second.key]
    assert [record.kind for record in records] == ["memory", "memory", "bucket"]
    assert records[-1].child_bucket_id == child.bucket_id
