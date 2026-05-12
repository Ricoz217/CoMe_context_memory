from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from context_memory import ContextMemoryConfig, ContextMemoryEngineV3


@pytest.mark.asyncio
async def test_active_child_rebuild_does_not_move_root(tmp_path: Path) -> None:
    cfg = ContextMemoryConfig(
        base_dir=tmp_path / "store_active_child",
        llm_preset="CONTEXT_MEMORY",
        image_llm_preset="KIMI2.6",
        use_mock_llm=True,
        init_config=False,
        auto_manage=False,
    )
    engine = ContextMemoryEngineV3(config=cfg)

    parent = await engine.set_bucket("ROOT_POINTER_GUARD")
    root_before = engine.root_bucket_id()
    active_before = engine.active_bucket_id()
    assert root_before == active_before

    child = await parent.create_child_bucket(
        title="child",
        summary="child summary",
        content="child content",
    )
    await engine.set_active_bucket(child.bucket_id)
    assert engine.active_bucket_id() == child.bucket_id

    child_handle = engine.get_bucket(child.bucket_id)
    added = await child_handle.add_memory("child memory for pointer stability", topic="child")
    assert added.success is True

    compressed = await child_handle.force_compress(reason="pointer_guard_active_child")
    assert compressed.success is True

    root_after = engine.root_bucket_id()
    active_after = engine.active_bucket_id()
    child_latest = await engine.latest_bucket_id(child.bucket_id)

    assert root_after == root_before
    assert child_latest != child.bucket_id
    assert active_after == child_latest


@pytest.mark.asyncio
async def test_root_rebuild_updates_root_and_active(tmp_path: Path) -> None:
    cfg = ContextMemoryConfig(
        base_dir=tmp_path / "store_root_rebuild",
        llm_preset="CONTEXT_MEMORY",
        image_llm_preset="KIMI2.6",
        use_mock_llm=True,
        init_config=False,
        auto_manage=False,
    )
    engine = ContextMemoryEngineV3(config=cfg)

    await engine.set_bucket("ROOT_REBUILD_GUARD")
    root_before = engine.root_bucket_id()
    root_handle = engine.get_bucket(root_before)
    await engine.set_active_bucket(root_before)
    assert engine.active_bucket_id() == root_before

    added = await root_handle.add_memory("root memory for pointer stability", topic="root")
    assert added.success is True

    compressed = await root_handle.force_compress(reason="pointer_guard_root")
    assert compressed.success is True

    root_after = engine.root_bucket_id()
    active_after = engine.active_bucket_id()
    root_latest = await engine.latest_bucket_id(root_before)

    assert root_after == root_latest
    assert active_after == root_after
    assert root_after != root_before
