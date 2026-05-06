from __future__ import annotations

import argparse
import asyncio
import importlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    mod = importlib.import_module(args.engine_module)
    Config = getattr(mod, "ContextMemoryConfig")
    Engine = getattr(mod, "ContextMemoryEngineV3")

    with TemporaryDirectory(prefix="come_mem_query_") as td:
        temp_root = Path(td)
        sample = temp_root / "sample.txt"
        sample.write_text("query benchmark sample", encoding="utf-8")

        cfg = Config(
            base_dir=temp_root / "store",
            use_mock_llm=args.use_mock_llm,
            auto_manage=False,
        )
        engine = Engine(config=cfg)

        await engine.add_memory_from_file(str(sample), topic="bench", force_split=True)

        async def one_query(i: int) -> dict[str, Any]:
            q = await engine.query(f"smoke query {i}", top_k=5, use_cache=True)
            return {
                "id": i,
                "degraded": bool(getattr(q, "degraded", False)),
                "reason_code": str(getattr(q, "reason_code", "")),
                "matches": len(getattr(q, "matches", []) or []),
            }

        tasks = [one_query(i) for i in range(args.concurrency)]
        results = await asyncio.gather(*tasks)

        if hasattr(engine, "close"):
            close_fn = getattr(engine, "close")
            if asyncio.iscoroutinefunction(close_fn):
                await close_fn()

        degraded = sum(1 for r in results if r["degraded"])
        return {
            "engine_module": args.engine_module,
            "concurrency": args.concurrency,
            "use_mock_llm": args.use_mock_llm,
            "degraded_count": degraded,
            "results": results,
        }


def main() -> None:
    p = argparse.ArgumentParser(description="ContextMemory query concurrency smoke")
    p.add_argument("--engine-module", default="context_memory.memory.engine")
    p.add_argument("--concurrency", type=int, default=20)
    p.add_argument("--use-mock-llm", action="store_true", default=True)
    p.add_argument("--out", default="")
    args = p.parse_args()

    report = asyncio.run(_run(args))
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
    else:
        print(text)


if __name__ == "__main__":
    main()
