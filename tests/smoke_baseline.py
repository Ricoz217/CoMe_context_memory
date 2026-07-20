from __future__ import annotations

import argparse
import asyncio
import importlib
import json
from dataclasses import asdict, is_dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any


def _normalize_result(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return {k: _normalize_result(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_result(v) for v in value]
    return value


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    mod = importlib.import_module(args.engine_module)
    Config = getattr(mod, "ContextMemoryConfig")
    Engine = getattr(mod, "ContextMemoryEngineV3")

    with TemporaryDirectory(prefix="come_mem_smoke_") as td:
        temp_root = Path(td)
        sample = temp_root / "sample.txt"
        sample.write_text(args.sample_text, encoding="utf-8")

        cfg = Config(
            base_dir=temp_root / "store",
            use_mock_llm=args.use_mock_llm,
            enable_cleaning=True,
            auto_manage=False,
        )
        engine = Engine(config=cfg)

        pre = await engine.list_memories(include_gray=False)
        add_result = await engine.add_memory_from_file(str(sample), topic="smoke", force_split=True)

        rounds: list[dict[str, Any]] = []
        for i in range(args.optimize_rounds):
            before = await engine.list_memories(include_gray=False)
            opt = await engine.optimize(reason=f"smoke_round_{i+1}")
            after = await engine.list_memories(include_gray=False)
            rounds.append(
                {
                    "round": i + 1,
                    "before": {
                        "memory_count": before.memory_count,
                        "bucket_count": before.bucket_count,
                        "total_memory_count": before.total_memory_count,
                        "bucket_id": before.bucket_id,
                    },
                    "optimize": {
                        "success": bool(getattr(opt, "success", False)),
                        "reason_code": getattr(opt, "reason_code", ""),
                        "coverage_ratio": float(getattr(opt, "coverage_ratio", 0.0) or 0.0),
                        "moved_items": int(getattr(opt, "moved_items", 0) or 0),
                        "created_buckets": list(getattr(opt, "created_buckets", []) or []),
                    },
                    "after": {
                        "memory_count": after.memory_count,
                        "bucket_count": after.bucket_count,
                        "total_memory_count": after.total_memory_count,
                        "bucket_id": after.bucket_id,
                    },
                }
            )

        post = await engine.list_memories(include_gray=False)

        report = {
            "engine_module": args.engine_module,
            "use_mock_llm": args.use_mock_llm,
            "optimize_rounds": args.optimize_rounds,
            "pre": {
                "memory_count": pre.memory_count,
                "bucket_count": pre.bucket_count,
                "total_memory_count": pre.total_memory_count,
                "bucket_id": pre.bucket_id,
            },
            "add_result": _normalize_result(add_result),
            "rounds": rounds,
            "post": {
                "memory_count": post.memory_count,
                "bucket_count": post.bucket_count,
                "total_memory_count": post.total_memory_count,
                "bucket_id": post.bucket_id,
            },
        }

        if hasattr(engine, "close"):
            close_fn = getattr(engine, "close")
            if asyncio.iscoroutinefunction(close_fn):
                await close_fn()

        return report


def main() -> None:
    p = argparse.ArgumentParser(description="ContextMemory decouple smoke baseline runner")
    p.add_argument("--engine-module", default="context_memory.memory.engine", help="import path of engine module")
    p.add_argument("--optimize-rounds", type=int, default=3)
    p.add_argument("--use-mock-llm", action="store_true", default=True)
    p.add_argument("--sample-text", default="ContextMemory smoke sample text.")
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
