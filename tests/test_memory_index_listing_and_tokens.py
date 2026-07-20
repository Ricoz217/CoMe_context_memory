from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import fields
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
import tiktoken

from context_memory.memory import engine as memory_engine
from context_memory.memory.models import BucketContextUsage, ListMemoriesResult, MemoryIndexItem
from context_memory.memory.token_counter import TokenCountError, TokenCounter
from context_memory.rpc_server import RpcError, _call


def _create_engine(base_dir: Path):
    with patch.object(memory_engine, "_resolve_effective_max_context_window", return_value=4096):
        return memory_engine.ContextMemoryEngineV3(
            base_dir=base_dir,
            use_mock_llm=True,
            init_config=False,
            auto_manage=False,
            auto_resume_pending_jobs=False,
        )


def test_list_memories_returns_index_result_without_loading_revisions(tmp_path: Path) -> None:
    asyncio.run(_test_list_memories_returns_index_result_without_loading_revisions(tmp_path))


async def _test_list_memories_returns_index_result_without_loading_revisions(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    handle = await engine.set_bucket("index-list")
    direct = await handle.add_memory("direct memory")
    child = await handle.create_bucket(title="child")
    child_handle = engine._bucket_handle_cls(engine, child.bucket_id)
    nested = await child_handle.add_memory("nested memory")

    with patch.object(
        engine.storage,
        "_json_to_memory_record",
        side_effect=AssertionError("list_memories must not load revision files"),
    ):
        result = await handle.list_memories()

    assert isinstance(result, ListMemoriesResult)
    assert all(isinstance(item, MemoryIndexItem) for item in result.memories + result.buckets)
    assert [item.key for item in result.memories] == [direct.key]
    assert [item.child_bucket_id for item in result.buckets] == [child.bucket_id]
    assert nested.key not in {item.key for item in result.memories}
    assert result.memory_count == 1
    assert result.total_memory_count == 2
    assert result.bucket_count == 1
    assert result.context_tokens >= 1
    assert result.token_count_method == "tiktoken"


def test_large_index_listing_does_not_require_revision_files(tmp_path: Path) -> None:
    asyncio.run(_test_large_index_listing_does_not_require_revision_files(tmp_path))


async def _test_large_index_listing_does_not_require_revision_files(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    handle = await engine.set_bucket("large-index")
    state = engine.storage.load_state()
    keys = state.setdefault("keys", {})
    for index in range(2000):
        key = f"mem_20990101000000_{index:032x}"
        keys[key] = {
            "latest_revision": f"rev_20990101000000_{index:032x}",
            "latest_path": f"memories/{key}/missing.json",
            "bucket_id": handle.bucket_id,
            "kind": "memory",
            "child_bucket_id": "",
            "gray": False,
            "updated_at": "2099-01-01T00:00:00+00:00",
        }
    engine.storage.save_state(state)

    with patch.object(
        engine.storage,
        "_json_to_memory_record",
        side_effect=AssertionError("large list must remain index-only"),
    ):
        result = await handle.list_memories()
    assert result.memory_count == 2000
    assert result.total_memory_count == 2000


def test_list_memories_offloads_index_context_and_tokenizer_work(tmp_path: Path) -> None:
    asyncio.run(_test_list_memories_offloads_index_context_and_tokenizer_work(tmp_path))


async def _test_list_memories_offloads_index_context_and_tokenizer_work(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    handle = await engine.set_bucket("offload")
    await handle.add_memory("threaded work")
    event_loop_thread = threading.get_ident()
    worker_threads: list[int] = []

    original_state = engine.storage.load_state
    original_tree = engine.storage.load_bucket_tree
    original_context = engine.storage.load_bucket_context
    original_count = engine.token_counter.count_text

    def tracked(callable_):
        def _wrapped(*args, **kwargs):
            worker_threads.append(threading.get_ident())
            return callable_(*args, **kwargs)

        return _wrapped

    with (
        patch.object(engine.storage, "load_state", side_effect=tracked(original_state)) as state_loader,
        patch.object(engine.storage, "load_bucket_tree", side_effect=tracked(original_tree)) as tree_loader,
        patch.object(engine.storage, "load_bucket_context", side_effect=tracked(original_context)) as context_loader,
        patch.object(engine.token_counter, "count_text", side_effect=tracked(original_count)) as token_count,
    ):
        await handle.list_memories()

    assert worker_threads
    assert all(thread_id != event_loop_thread for thread_id in worker_threads)
    assert state_loader.call_count == 1
    assert tree_loader.call_count == 1
    assert context_loader.call_count == 1
    assert token_count.call_count == 1

    with (
        patch.object(engine.storage, "load_bucket_context", wraps=original_context) as warm_context_loader,
        patch.object(engine.token_counter, "count_text", wraps=original_count) as warm_token_count,
    ):
        await handle.list_memories()
    warm_context_loader.assert_not_called()
    warm_token_count.assert_not_called()


def test_subtree_count_respects_gray_nodes_successors_and_cycles(tmp_path: Path) -> None:
    asyncio.run(_test_subtree_count_respects_gray_nodes_successors_and_cycles(tmp_path))


async def _test_subtree_count_respects_gray_nodes_successors_and_cycles(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    root = await engine.set_bucket("subtree")
    direct = await root.add_memory("direct")
    direct_gray = await root.add_memory("direct gray")
    await root.set_gray(direct_gray.key, gray=True)

    old_child = await root.create_bucket(title="old child")
    old_handle = engine._bucket_handle_cls(engine, old_child.bucket_id)
    old_memory = await old_handle.add_memory("old child memory")
    child_node = next(item for item in (await root.list_memories()).buckets if item.child_bucket_id == old_child.bucket_id)

    hidden = await root.list_memories()
    visible = await root.list_memories(include_gray=True)
    assert [item.key for item in hidden.memories] == [direct.key]
    assert hidden.total_memory_count == 2
    assert visible.total_memory_count == 3

    await root.set_gray(child_node.key, gray=True)
    assert (await root.list_memories()).total_memory_count == 1
    assert (await root.list_memories(include_gray=True)).total_memory_count == 3
    await root.set_gray(child_node.key, gray=False)

    successor = await root.create_bucket(title="successor")
    successor_handle = engine._bucket_handle_cls(engine, successor.bucket_id)
    successor_memory = await successor_handle.add_memory("successor memory")
    engine.storage.seal_bucket_successor(
        source_bucket_id=old_child.bucket_id,
        successor_bucket_id=successor.bucket_id,
    )
    redirected = await root.list_memories()
    assert redirected.total_memory_count == 2
    assert old_memory.key not in {item.key for item in redirected.memories}
    assert successor_memory.key not in {item.key for item in redirected.memories}

    tree = engine.storage.load_bucket_tree()
    tree["buckets"][successor.bucket_id]["sealed"] = True
    tree["buckets"][successor.bucket_id]["sealed_to"] = old_child.bucket_id
    engine.storage.save_bucket_tree(tree)
    cycled = await asyncio.wait_for(root.list_memories(), timeout=1.0)
    assert cycled.total_memory_count == 3


def test_context_usage_uses_tokenizer_and_marks_display_fallback(tmp_path: Path) -> None:
    asyncio.run(_test_context_usage_uses_tokenizer_and_marks_display_fallback(tmp_path))


async def _test_context_usage_uses_tokenizer_and_marks_display_fallback(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    handle = await engine.set_bucket("usage")
    await handle.add_memory("真实 context tokenizer 统计")

    context = engine.storage.load_bucket_context(handle.bucket_id)
    raw_text = "\n".join(
        prompt.text
        for prompt in context.to_prompts()
        if isinstance(getattr(prompt, "text", None), str) and prompt.text
    )
    expected = max(1, len(tiktoken.get_encoding("o200k_base").encode(raw_text)))

    usage = await handle.get_bucket_context_usage()
    assert isinstance(usage, BucketContextUsage)
    assert usage.context_tokens == expected
    assert usage.token_count_method == "tiktoken"

    engine.memory_manager.remove(f"ctx_tokens:{handle.bucket_id}")
    with patch.object(engine.token_counter, "count_text", side_effect=TokenCountError("broken tokenizer")):
        fallback = await handle.get_bucket_context_usage()
    assert fallback.context_tokens >= 1
    assert fallback.token_count_method == "char_estimate"


def test_removed_include_content_and_engine_stats_token_field(tmp_path: Path) -> None:
    asyncio.run(_test_removed_include_content_and_engine_stats_token_field(tmp_path))


async def _test_removed_include_content_and_engine_stats_token_field(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    handle = await engine.set_bucket("contract")

    with pytest.raises(TypeError):
        await handle.list_memories(include_content=True)  # type: ignore[call-arg]

    stats = await engine.stats()
    assert "estimated_tokens" not in {field.name for field in fields(stats)}

    with pytest.raises(RpcError) as exc_info:
        await _call(engine, "list_memories", {"include_content": False})
    assert exc_info.value.code == -32602


def test_token_counter_strict_mode_never_uses_character_fallback() -> None:
    counter = TokenCounter()
    with patch("context_memory.memory.token_counter.tiktoken.get_encoding", side_effect=RuntimeError("broken")):
        with pytest.raises(TokenCountError):
            counter.count_text("payload")

    payload = {"reason": "test", "max_context_window": 4096, "records": [{"key": "memory_1"}]}
    payload_tokens = counter.count_json_with_token_field(payload)
    final_payload = {**payload, "payload_tokens": payload_tokens}
    assert counter.count_json(final_payload) == payload_tokens


def test_context_usage_cache_and_version_invalidation(tmp_path: Path) -> None:
    asyncio.run(_test_context_usage_cache_and_version_invalidation(tmp_path))


async def _test_context_usage_cache_and_version_invalidation(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    handle = await engine.set_bucket("cache")
    await handle.add_memory("first")
    await handle.get_bucket_context_usage()

    with patch.object(
        engine.storage,
        "load_bucket_context",
        side_effect=AssertionError("warm cache must avoid context I/O"),
    ):
        await handle.get_bucket_context_usage()

    await handle.add_memory("second")
    with patch.object(engine.storage, "load_bucket_context", wraps=engine.storage.load_bucket_context) as loader:
        await handle.get_bucket_context_usage()
    assert loader.call_count == 1


def test_slow_list_storage_does_not_block_event_loop(tmp_path: Path) -> None:
    asyncio.run(_test_slow_list_storage_does_not_block_event_loop(tmp_path))


async def _test_slow_list_storage_does_not_block_event_loop(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    handle = await engine.set_bucket("heartbeat")
    await handle.add_memory("heartbeat memory")
    engine.memory_manager.remove(f"ctx_tokens:{handle.bucket_id}")
    original_state = engine.storage.load_state

    def slow_state():
        time.sleep(0.05)
        return original_state()

    ticks = 0

    async def heartbeat():
        nonlocal ticks
        for _ in range(5):
            await asyncio.sleep(0.01)
            ticks += 1

    with patch.object(engine.storage, "load_state", side_effect=slow_state):
        await asyncio.gather(handle.list_memories(), heartbeat())
    assert ticks == 5


def test_compress_token_failure_is_fail_closed(tmp_path: Path) -> None:
    asyncio.run(_test_compress_token_failure_is_fail_closed(tmp_path))


async def _test_compress_token_failure_is_fail_closed(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    handle = await engine.set_bucket("compress-token")
    await handle.add_memory("compress me")
    llm_call = AsyncMock()

    with (
        patch.object(engine.token_counter, "count_json", side_effect=TokenCountError("broken tokenizer")),
        patch.object(engine.pipeline, "compress", llm_call),
        pytest.raises(TokenCountError),
    ):
        await handle.force_compress()

    llm_call.assert_not_awaited()


def test_optimize_token_failure_is_fail_closed(tmp_path: Path) -> None:
    asyncio.run(_test_optimize_token_failure_is_fail_closed(tmp_path))


async def _test_optimize_token_failure_is_fail_closed(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    handle = await engine.set_bucket("optimize-token")
    await handle.add_memory("optimize me")
    llm_call = AsyncMock()

    with (
        patch.object(engine.token_counter, "count_json", side_effect=TokenCountError("broken tokenizer")),
        patch.object(engine.pipeline, "optimize", llm_call),
        pytest.raises(TokenCountError),
    ):
        await handle.optimize()

    llm_call.assert_not_awaited()


def test_compress_payload_tokens_match_actual_outbound_json(tmp_path: Path) -> None:
    asyncio.run(_test_compress_payload_tokens_match_actual_outbound_json(tmp_path))


async def _test_compress_payload_tokens_match_actual_outbound_json(tmp_path: Path) -> None:
    engine = _create_engine(tmp_path / "store")
    handle = await engine.set_bucket("compress-token-exact")
    await handle.add_memory("compress exact payload")
    original_compress = engine.pipeline.compress
    checked = False

    async def capture(**kwargs):
        nonlocal checked
        actual_payload = {
            "reason": kwargs["reason"],
            "payload_tokens": kwargs["payload_tokens"],
            "max_context_window": kwargs["max_context_window"],
            "records": kwargs["records"],
        }
        assert engine.token_counter.count_json(actual_payload) == kwargs["payload_tokens"]
        checked = True
        return await original_compress(**kwargs)

    with patch.object(engine.pipeline, "compress", side_effect=capture):
        await handle.force_compress()
    assert checked is True
