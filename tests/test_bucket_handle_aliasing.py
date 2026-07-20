from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from context_memory import ContextMemoryConfig, ContextMemoryEngineV3


def _engine(tmp_path: Path) -> ContextMemoryEngineV3:
    return ContextMemoryEngineV3(
        config=ContextMemoryConfig(
            base_dir=tmp_path,
            use_mock_llm=True,
            init_config=False,
            auto_manage=False,
            auto_resume_pending_jobs=False,
        )
    )


@pytest.mark.asyncio
async def test_bucket_handle_resolve_alias_handles_memory_bucket_and_bucket_node(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    handle = await engine.set_bucket("ALIAS_RESOLVE")
    added = await handle.add_memory("ordinary memory")
    child = await handle.create_bucket(title="child")

    listed = await handle.list_memories()
    bucket_node = next(record for record in listed.buckets if record.child_bucket_id == child.bucket_id)

    memory_alias = engine.get_or_create_alias(handle.bucket_id, added.key, "memory")
    bucket_alias = engine.get_or_create_alias(handle.bucket_id, child.bucket_id, "bucket")
    bucket_node_alias = engine.get_or_create_alias(handle.bucket_id, bucket_node.key, "memory")

    assert await handle.resolve_alias(memory_alias) == added.key
    assert await handle.resolve_alias(bucket_alias) == child.bucket_id
    assert await handle.resolve_alias(bucket_node_alias) == bucket_node.key

    resolved_node = await handle.get_memory(await handle.resolve_alias(bucket_node_alias))
    assert resolved_node is not None
    assert resolved_node.kind == "bucket"
    assert resolved_node.child_bucket_id == child.bucket_id


@pytest.mark.asyncio
async def test_bucket_handle_resolve_alias_preserves_validation_errors(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    handle = await engine.set_bucket("ALIAS_ERRORS")
    child = await handle.create_bucket(title="child")
    bucket_alias = engine.get_or_create_alias(handle.bucket_id, child.bucket_id, "bucket")

    with pytest.raises(TypeError, match="alias type mismatch"):
        await handle.resolve_alias(bucket_alias, expected_type="memory")

    with pytest.raises(KeyError, match="unknown alias"):
        await handle.resolve_alias("memory_999")


@pytest.mark.asyncio
async def test_bucket_handle_resolve_aliases_batches_successes_with_one_refresh(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    handle = await engine.set_bucket("ALIAS_BATCH")
    added = await handle.add_memory("batch memory")
    child = await handle.create_bucket(title="batch child")
    memory_alias = engine.get_or_create_alias(handle.bucket_id, added.key, "memory")
    bucket_alias = engine.get_or_create_alias(handle.bucket_id, child.bucket_id, "bucket")

    with patch.object(engine, "_resolve_bucket_id", wraps=engine._resolve_bucket_id) as resolve_bucket:
        resolved = await handle.resolve_aliases(
            [memory_alias, bucket_alias, "memory_999", "not-an-alias", memory_alias]
        )

    assert resolved == {memory_alias: added.key, bucket_alias: child.bucket_id}
    assert resolve_bucket.call_count == 1

    memory_only = await handle.resolve_aliases(
        [memory_alias, bucket_alias],
        expected_type="memory",
    )
    assert memory_only == {memory_alias: added.key}

    with pytest.raises(KeyError, match="unknown alias"):
        await handle.resolve_aliases([memory_alias, "memory_999"], strict=True)
    with pytest.raises(TypeError, match="alias type mismatch"):
        await handle.resolve_aliases([bucket_alias], expected_type="memory", strict=True)
