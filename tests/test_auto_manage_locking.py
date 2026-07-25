from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from context_memory import ContextMemoryConfig, ContextMemoryEngineV3


def _engine(tmp_path: Path) -> ContextMemoryEngineV3:
    engine = ContextMemoryEngineV3(
        config=ContextMemoryConfig(
            base_dir=tmp_path,
            use_mock_llm=True,
            init_config=False,
            auto_manage=True,
            enable_forgetting=True,
            auto_resume_pending_jobs=False,
        )
    )
    engine._runtime._negative_delete_threshold = 1.1
    return engine


@pytest.mark.asyncio
async def test_add_memory_auto_forget_does_not_reenter_bucket_lock(tmp_path: Path) -> None:
    engine = _engine(tmp_path / "add")

    result = await asyncio.wait_for(engine.add_memory("auto forget add"), timeout=2.0)

    assert result.success is True
    record = engine.storage.get_record(result.key)
    assert record is not None
    assert record.gray is True


@pytest.mark.asyncio
async def test_update_memory_auto_forget_does_not_reenter_bucket_lock(tmp_path: Path) -> None:
    engine = _engine(tmp_path / "update")
    engine.auto_manage = False
    added = await engine.add_memory("before update")
    assert added.success is True

    engine.auto_manage = True
    result = await asyncio.wait_for(engine.update_memory(added.key, "after update"), timeout=2.0)

    assert result.success is True


@pytest.mark.asyncio
async def test_force_split_auto_forget_does_not_reenter_bucket_lock(tmp_path: Path) -> None:
    engine = _engine(tmp_path / "force_split")

    result = await asyncio.wait_for(
        engine.add_memory("auto forget force split", force_split=True),
        timeout=2.0,
    )

    assert result.success is True
    assert result.split_performed is True
