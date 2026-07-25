from __future__ import annotations

import asyncio
from pathlib import Path

from context_memory.memory import ContextMemoryConfig, ContextMemoryEngineV3


def test_export_memory_to_markdown_after_service_split(tmp_path: Path) -> None:
    async def run() -> None:
        engine = ContextMemoryEngineV3(
            config=ContextMemoryConfig(
                base_dir=tmp_path,
                use_mock_llm=True,
                init_config=False,
                auto_manage=False,
                auto_resume_pending_jobs=False,
            )
        )
        try:
            added = await engine.add_memory("record service export smoke")
            exported = await engine.export_memory_to_markdown(added.key)
            assert exported["success"] is True
            assert Path(exported["path"]).is_file()
        finally:
            await engine.close(wait=True)

    asyncio.run(run())
