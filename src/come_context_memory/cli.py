from __future__ import annotations

import argparse
import asyncio
import json
import shlex
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from come_context_memory import ContextMemoryConfig, ContextMemoryEngineV3


HELP_TEXT = """
Commands:
  help
  add <text> [--bucket <bucket_id>] [--force-split] [--create-new-bucket] [--chunk-max N] [--chunk-overlap N]
  add_file <path> [topic] [--bucket <bucket_id>] [--force-split] [--create-new-bucket] [--chunk-max N] [--chunk-overlap N]
  add_dir <dir> [--bucket <bucket_id>] [--auto-sub-buckets] [--force-split] [--create-new-bucket]
  get <key> [--evidence]
  evidence <key>
  export <memory_id>
  update <key> <patch_text>
  gray <key> <set|clear> [reason]
  delete <key> [reason]
  query <text> [--top-k N] [--gray] [--bucket <bucket_id>] [--mode auto|semantic|hybrid]
  list [--gray] [--bucket <bucket_id>] [--with-content]
  buckets
  create_bucket <parent_bucket_id> <title> [summary] [--lock-summary]
  create_child_bucket <parent_bucket_id> <title> [summary] [--lock-summary]
  switch_bucket <bucket_id>
  latest_bucket [bucket_id]
  refresh_summary <bucket_id> [--force]
  split <bucket_id>
  optimize [bucket_id]
  move <key> <target_bucket_id> [reason]
  compress [bucket_id]
  gc [--apply]
  cleanup
  stats
  exit
"""


def _jsonable(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return value.to_dict()
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [_jsonable(v) for v in value]
    return value


def _print_json(data: Any) -> None:
    print(json.dumps(_jsonable(data), ensure_ascii=False, indent=2))


async def _read_input(prompt: str = "come-memory> ") -> str:
    return await asyncio.to_thread(input, prompt)


def _remove_flag_tokens(tokens: list[str], raw: str, flag: str, takes_value: bool) -> str:
    out = raw
    idx = 0
    while idx < len(tokens):
        if tokens[idx] == flag:
            fragment = flag
            if takes_value and idx + 1 < len(tokens):
                fragment = f"{flag} {tokens[idx + 1]}"
                idx += 1
            out = out.replace(fragment, "", 1)
        idx += 1
    return " ".join(out.split())


def _parse_common_split_flags(parts: list[str]) -> tuple[bool, bool, int | None, int | None, str | None]:
    force_split = "--force-split" in parts
    create_new_bucket = "--create-new-bucket" in parts
    chunk_max_chars = None
    chunk_overlap_chars = None
    bucket_id = None

    if "--chunk-max" in parts:
        idx = parts.index("--chunk-max")
        if idx + 1 < len(parts):
            try:
                chunk_max_chars = int(parts[idx + 1])
            except ValueError:
                chunk_max_chars = None
    if "--chunk-overlap" in parts:
        idx = parts.index("--chunk-overlap")
        if idx + 1 < len(parts):
            try:
                chunk_overlap_chars = int(parts[idx + 1])
            except ValueError:
                chunk_overlap_chars = None
    if "--bucket" in parts:
        idx = parts.index("--bucket")
        if idx + 1 < len(parts):
            bucket_id = parts[idx + 1]
    return force_split, create_new_bucket, chunk_max_chars, chunk_overlap_chars, bucket_id


def _make_config(args: argparse.Namespace) -> ContextMemoryConfig:
    return ContextMemoryConfig(
        base_dir=args.base_dir,
        llm_preset=args.preset,
        image_llm_preset=args.image_preset,
        ask_timeout=args.timeout,
        use_mock_llm=args.mock,
        enable_cleaning=not args.no_clean,
        enable_forgetting=not args.no_forgetting,
        init_config=not args.no_debug_mode,
        auto_manage=not args.no_auto_manage,
        max_bucket_depth=args.max_bucket_depth,
        max_context_window=args.max_context_window,
        max_memory_bytes=args.max_memory_bytes,
        evidence_versions=args.evidence_versions,
    )


async def run_cli(args: argparse.Namespace) -> None:
    engine = ContextMemoryEngineV3(config=_make_config(args))

    print("CoMe ContextMemory CLI")
    print(f"base_dir={Path(args.base_dir).resolve()}")
    print(f"preset={args.preset} image_preset={args.image_preset} mock={args.mock}")
    print(HELP_TEXT.strip())

    while True:
        raw = (await _read_input()).strip()
        if not raw:
            continue
        try:
            parts = shlex.split(raw)
        except ValueError as exc:
            print(f"parse error: {exc}")
            continue
        if not parts:
            continue

        cmd = parts[0].lower()
        if cmd in {"exit", "quit"}:
            print("bye")
            return
        if cmd == "help":
            print(HELP_TEXT.strip())
            continue

        try:
            if cmd == "add":
                force_split, create_new_bucket, chunk_max, chunk_overlap, bucket_id = _parse_common_split_flags(parts)
                text = raw[len(parts[0]):].strip()
                text = _remove_flag_tokens(parts, text, "--bucket", takes_value=True)
                text = _remove_flag_tokens(parts, text, "--force-split", takes_value=False)
                text = _remove_flag_tokens(parts, text, "--create-new-bucket", takes_value=False)
                text = _remove_flag_tokens(parts, text, "--chunk-max", takes_value=True)
                text = _remove_flag_tokens(parts, text, "--chunk-overlap", takes_value=True)
                if not text:
                    print("usage: add <text> [--bucket <bucket_id>] [--force-split] [--create-new-bucket]")
                    continue
                result = await engine.add_memory(
                    text,
                    bucket_id=bucket_id,
                    force_split=force_split,
                    create_new_bucket=create_new_bucket,
                    chunk_max_chars=chunk_max,
                    chunk_overlap_chars=chunk_overlap,
                )
                _print_json(result)

            elif cmd == "add_file":
                if len(parts) < 2:
                    print("usage: add_file <path> [topic] [--bucket <bucket_id>] [--force-split] [--create-new-bucket]")
                    continue
                force_split, create_new_bucket, chunk_max, chunk_overlap, bucket_id = _parse_common_split_flags(parts)
                file_path = parts[1]
                topic = ""
                if len(parts) >= 3 and not parts[2].startswith("--"):
                    topic = parts[2]
                result = await engine.add_memory_from_file(
                    file_path,
                    topic=topic,
                    bucket_id=bucket_id,
                    force_split=force_split,
                    create_new_bucket=create_new_bucket,
                    chunk_max_chars=chunk_max,
                    chunk_overlap_chars=chunk_overlap,
                )
                _print_json(result)

            elif cmd == "add_dir":
                if len(parts) < 2:
                    print("usage: add_dir <dir> [--bucket <bucket_id>] [--auto-sub-buckets]")
                    continue
                force_split, create_new_bucket, chunk_max, chunk_overlap, bucket_id = _parse_common_split_flags(parts)
                auto_sub = "--auto-sub-buckets" in parts
                result = await engine.add_memory_from_dir(
                    parts[1],
                    bucket_id=bucket_id,
                    auto_create_sub_buckets=auto_sub,
                    force_split=force_split,
                    create_new_bucket=create_new_bucket,
                    chunk_max_chars=chunk_max,
                    chunk_overlap_chars=chunk_overlap,
                )
                _print_json(result)

            elif cmd == "get":
                if len(parts) < 2:
                    print("usage: get <key> [--evidence]")
                    continue
                record = await engine.get_memory(parts[1], with_evidence=("--evidence" in parts[2:]))
                _print_json(record or {"success": False, "message": "not found"})

            elif cmd == "evidence":
                if len(parts) < 2:
                    print("usage: evidence <key>")
                    continue
                text = await engine.get_evidence_content(parts[1])
                _print_json({"key": parts[1], "evidence_content": text})

            elif cmd == "export":
                if len(parts) < 2:
                    print("usage: export <memory_id>")
                    continue
                _print_json(await engine.export_memory_to_markdown(parts[1]))

            elif cmd == "update":
                if len(parts) < 3:
                    print("usage: update <key> <patch_text>")
                    continue
                key = parts[1]
                patch = raw.split(key, 1)[1].strip()
                _print_json(await engine.update_memory(key, patch))

            elif cmd == "gray":
                if len(parts) < 3:
                    print("usage: gray <key> <set|clear> [reason]")
                    continue
                key = parts[1]
                op = parts[2].lower()
                reason = raw.split(parts[2], 1)[1].strip() if len(parts) > 3 else ""
                if op not in {"set", "clear"}:
                    print("gray action must be set|clear")
                    continue
                _print_json(await engine.set_gray(key, gray=(op == "set"), reason=reason))

            elif cmd == "delete":
                if len(parts) < 2:
                    print("usage: delete <key> [reason]")
                    continue
                key = parts[1]
                reason = raw.split(key, 1)[1].strip() if len(parts) > 2 else ""
                _print_json(await engine.delete_memory(key, reason=reason))

            elif cmd == "query":
                if len(parts) < 2:
                    print("usage: query <text> [--top-k N] [--gray] [--bucket <bucket_id>] [--mode auto|semantic|hybrid]")
                    continue
                include_gray = "--gray" in parts
                top_k = 5
                bucket_id = None
                mode = "auto"
                if "--top-k" in parts:
                    idx = parts.index("--top-k")
                    if idx + 1 < len(parts):
                        try:
                            top_k = int(parts[idx + 1])
                        except ValueError:
                            top_k = 5
                if "--bucket" in parts:
                    idx = parts.index("--bucket")
                    if idx + 1 < len(parts):
                        bucket_id = parts[idx + 1]
                if "--mode" in parts:
                    idx = parts.index("--mode")
                    if idx + 1 < len(parts):
                        mode = parts[idx + 1]
                q = raw[len(parts[0]):].strip()
                q = _remove_flag_tokens(parts, q, "--gray", takes_value=False)
                q = _remove_flag_tokens(parts, q, "--top-k", takes_value=True)
                q = _remove_flag_tokens(parts, q, "--bucket", takes_value=True)
                q = _remove_flag_tokens(parts, q, "--mode", takes_value=True)
                _print_json(await engine.query(q, top_k=top_k, include_gray=include_gray, bucket_id=bucket_id, mode=mode))

            elif cmd == "list":
                include_gray = "--gray" in parts[1:]
                include_content = "--with-content" in parts[1:]
                bucket_id = None
                if "--bucket" in parts:
                    idx = parts.index("--bucket")
                    if idx + 1 < len(parts):
                        bucket_id = parts[idx + 1]
                _print_json(await engine.list_memories(include_gray=include_gray, include_content=include_content, bucket_id=bucket_id))

            elif cmd == "buckets":
                _print_json(engine.list_buckets())

            elif cmd == "create_bucket":
                if len(parts) < 3:
                    print("usage: create_bucket <parent_bucket_id> <title> [summary] [--lock-summary]")
                    continue
                parent = parts[1]
                title = parts[2]
                summary_locked = "--lock-summary" in parts
                summary = ""
                if len(parts) >= 4:
                    summary = raw.split(title, 1)[1].strip()
                    summary = _remove_flag_tokens(parts, summary, "--lock-summary", takes_value=False)
                _print_json(
                    await engine.create_bucket(
                        parent,
                        title=title,
                        summary=summary,
                        summary_locked=summary_locked,
                    )
                )

            elif cmd == "create_child_bucket":
                if len(parts) < 3:
                    print("usage: create_child_bucket <parent_bucket_id> <title> [summary] [--lock-summary]")
                    continue
                parent = parts[1]
                title = parts[2]
                summary_locked = "--lock-summary" in parts
                summary = ""
                if len(parts) >= 4:
                    summary = raw.split(title, 1)[1].strip()
                    summary = _remove_flag_tokens(parts, summary, "--lock-summary", takes_value=False)
                _print_json(
                    await engine.create_child_bucket(
                        parent,
                        title=title,
                        summary=summary,
                        summary_locked=summary_locked,
                    )
                )

            elif cmd == "switch_bucket":
                if len(parts) < 2:
                    print("usage: switch_bucket <bucket_id>")
                    continue
                _print_json(await engine.set_active_bucket(parts[1]))

            elif cmd == "latest_bucket":
                bucket_id = parts[1] if len(parts) > 1 else None
                _print_json({"bucket_id": await engine.latest_bucket_id(bucket_id)})

            elif cmd == "refresh_summary":
                if len(parts) < 2:
                    print("usage: refresh_summary <bucket_id> [--force]")
                    continue
                _print_json(await engine.refresh_bucket_summary(parts[1], force=("--force" in parts)))

            elif cmd == "split":
                if len(parts) < 2:
                    print("usage: split <bucket_id>")
                    continue
                _print_json(await engine.split_bucket(parts[1], reason="manual"))

            elif cmd == "optimize":
                target = parts[1] if len(parts) > 1 else None
                _print_json(await engine.optimize(bucket_id=target, reason="manual_optimize"))

            elif cmd == "move":
                if len(parts) < 3:
                    print("usage: move <key> <target_bucket_id> [reason]")
                    continue
                key = parts[1]
                target = parts[2]
                reason = raw.split(target, 1)[1].strip() if len(parts) > 3 else "manual_move"
                _print_json(await engine.move_item(key, target_bucket_id=target, reason=reason))

            elif cmd == "compress":
                target = parts[1] if len(parts) > 1 else None
                _print_json(await engine.force_compress(reason="manual", bucket_id=target))

            elif cmd == "gc":
                apply = "--apply" in parts
                _print_json(await engine.gc_storage(dry_run=not apply, reason="manual_gc"))

            elif cmd == "cleanup":
                _print_json(await engine.cleanup_expired())

            elif cmd == "stats":
                _print_json(await engine.stats())

            else:
                print(f"unknown command: {cmd}")
        except Exception as exc:
            print(f"error: {exc}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CoMe ContextMemory CLI")
    parser.add_argument(
        "--base-dir",
        default=str(Path(__file__).resolve().parents[2] / "data" / "cli_runtime"),
        help="Storage base directory",
    )
    parser.add_argument("--preset", default="CONTEXT_MEMORY", help="LLM preset name")
    parser.add_argument("--image-preset", default="KIMI2.6", help="Image extract LLM preset name")
    parser.add_argument("--timeout", type=float, default=180.0, help="LLM ask timeout in seconds")
    parser.add_argument("--mock", action="store_true", help="Use mock LLM only")
    parser.add_argument("--no-clean", action="store_true", help="Disable cleaning stage")
    parser.add_argument("--no-forgetting", action="store_true", help="Disable negative-weight forgetting logic")
    parser.add_argument("--no-debug-mode", action="store_true", help="Skip debug_mode precheck init")
    parser.add_argument("--no-auto-manage", action="store_true", help="Disable auto compress/split/forget")
    parser.add_argument("--max-context-window", type=int, default=1_000_000, help="Approx context window tokens")
    parser.add_argument("--max-memory-bytes", type=int, default=1_000_000_000, help="In-memory cache budget")
    parser.add_argument("--evidence-versions", type=int, default=5, help="Keep latest N evidence versions per key")
    parser.add_argument("--max-bucket-depth", type=int, default=3, help="Max bucket depth")
    return parser


async def _main() -> None:
    args = build_parser().parse_args()
    await run_cli(args)


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
