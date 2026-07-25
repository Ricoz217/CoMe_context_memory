from __future__ import annotations

import asyncio
import threading
import time
from pathlib import Path
from unittest.mock import patch

from context_memory.LLM_connect import Prompts, TextPrompt
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


async def _build_tree(engine):
    root = engine.root_bucket_id()
    target = await engine.create_bucket(root, title="target", summary="target")
    nested = await engine.create_bucket(target.bucket_id, title="nested", summary="nested")
    unrelated = await engine.create_bucket(root, title="unrelated", summary="unrelated")

    target_handle = engine._bucket_handle_cls(engine, target.bucket_id)
    nested_handle = engine._bucket_handle_cls(engine, nested.bucket_id)
    unrelated_handle = engine._bucket_handle_cls(engine, unrelated.bucket_id)
    await target_handle.add_memory("target memory")
    await nested_handle.add_memory("nested memory")
    await unrelated_handle.add_memory("unrelated memory")
    return target.bucket_id


def test_advance_snapshot_reads_indexes_once_and_only_target_revisions(tmp_path: Path) -> None:
    asyncio.run(_test_advance_snapshot_reads_indexes_once_and_only_target_revisions(tmp_path))


async def _test_advance_snapshot_reads_indexes_once_and_only_target_revisions(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    target_bucket = await _build_tree(engine)
    service = engine._advance_query_service
    event_loop_thread = threading.get_ident()
    worker_threads: list[int] = []

    original_topology = engine.storage.topology_snapshot
    original_nodes = engine.storage.runtime_index_nodes_for_bucket
    original_record = engine.storage.load_record_from_index_node

    def tracked(callable_):
        def wrapped(*args, **kwargs):
            worker_threads.append(threading.get_ident())
            return callable_(*args, **kwargs)

        return wrapped

    with (
        patch.object(engine.storage, "topology_snapshot", side_effect=tracked(original_topology)) as topology_loader,
        patch.object(
            engine.storage,
            "runtime_index_nodes_for_bucket",
            side_effect=tracked(original_nodes),
        ) as node_loader,
        patch.object(engine.storage, "load_record_from_index_node", side_effect=tracked(original_record)) as record_loader,
        patch.object(
            engine.storage,
            "load_runtime_index_snapshot",
            side_effect=AssertionError("advance_query must not materialize the full locator index"),
        ),
    ):
        resolved, node, payload = await service._advance_collect_bucket_snapshot(
            bucket_id=target_bucket,
            include_gray=False,
            max_expand_depth=None,
        )

    assert resolved == target_bucket
    assert node["bucket_id"] == target_bucket
    assert payload == service._advance_render_top_payload(node)
    assert topology_loader.call_count == 1
    assert node_loader.call_count == 2
    assert record_loader.call_count == 2
    assert worker_threads
    assert all(thread_id != event_loop_thread for thread_id in worker_threads)
    await engine.close(wait=True)


def test_advance_snapshot_slow_io_does_not_block_event_loop(tmp_path: Path) -> None:
    asyncio.run(_test_advance_snapshot_slow_io_does_not_block_event_loop(tmp_path))


async def _test_advance_snapshot_slow_io_does_not_block_event_loop(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    target_bucket = await _build_tree(engine)
    service = engine._advance_query_service
    original_record = engine.storage.load_record_from_index_node

    def slow_record(node):
        time.sleep(0.06)
        return original_record(node)

    ticks = 0

    async def heartbeat():
        nonlocal ticks
        for _ in range(5):
            await asyncio.sleep(0.01)
            ticks += 1

    with patch.object(engine.storage, "load_record_from_index_node", side_effect=slow_record):
        await asyncio.gather(
            service._advance_collect_bucket_snapshot(
                bucket_id=target_bucket,
                include_gray=False,
                max_expand_depth=None,
            ),
            heartbeat(),
        )

    assert ticks == 5
    await engine.close(wait=True)


def test_advance_query_entrypoint_never_uses_global_record_scan(tmp_path: Path) -> None:
    asyncio.run(_test_advance_query_entrypoint_never_uses_global_record_scan(tmp_path))


async def _test_advance_query_entrypoint_never_uses_global_record_scan(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    target_bucket = await _build_tree(engine)
    service = engine._advance_query_service

    async def fake_request(**_kwargs):
        return Prompts(TextPrompt("assistant", "ok"))

    with (
        patch.object(
            engine.storage,
            "list_bucket_records",
            side_effect=AssertionError("advance_query must not scan all latest records"),
        ),
        patch.object(service, "_advance_count_tokens_exact", return_value=1),
        patch.object(service, "_advance_llm_request", side_effect=fake_request),
    ):
        result = await engine.advance_query(
            command="inspect",
            mode="single_shot",
            bucket_id=target_bucket,
            enable_aliasing=False,
        )

    assert isinstance(result, Prompts)
    await engine.close(wait=True)
