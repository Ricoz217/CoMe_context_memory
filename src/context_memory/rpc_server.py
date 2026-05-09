from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

from context_memory import ContextMemoryConfig, ContextMemoryEngineV3

try:
    from fastapi import FastAPI, HTTPException, Request
    import uvicorn
except Exception:
    FastAPI = None  # type: ignore[assignment]
    HTTPException = Exception  # type: ignore[assignment]
    Request = object  # type: ignore[assignment]
    uvicorn = None  # type: ignore[assignment]


ENGINE: ContextMemoryEngineV3 | None = None


def _default_enable_forgetting_from_config() -> bool:
    try:
        from context_memory.config import SETTING_CFG
    except Exception:
        return True
    memory_cfg = getattr(SETTING_CFG, "Memory", None)
    if memory_cfg is None:
        return True
    try:
        return bool(getattr(memory_cfg, "enable_forgetting", True))
    except Exception:
        return True


class RpcError(Exception):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = int(code)
        self.message = str(message)


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


def _make_config(args: argparse.Namespace) -> ContextMemoryConfig:
    enable_forgetting = False if bool(args.no_forgetting) else _default_enable_forgetting_from_config()
    return ContextMemoryConfig(
        base_dir=args.base_dir,
        llm_preset=args.preset,
        image_llm_preset=args.image_preset,
        ask_timeout=args.timeout,
        use_mock_llm=args.mock,
        enable_cleaning=not args.no_clean,
        enable_forgetting=enable_forgetting,
        init_config=not args.no_debug_mode,
        auto_manage=not args.no_auto_manage,
        max_bucket_depth=args.max_bucket_depth,
        max_memory_bytes=args.max_memory_bytes,
        evidence_versions=args.evidence_versions,
        query_top_k_default=args.query_top_k_default,
        query_max_depth_default=args.query_max_depth_default,
    )


def _ok(result: Any, req_id: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": _jsonable(result)}


def _err(code: int, msg: str, req_id: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": msg}}


def _parse_query_mode(mode_raw: Any) -> str:
    mode = str(mode_raw if mode_raw is not None else "auto").strip().lower() or "auto"
    if mode == "literal":
        raise RpcError(-32602, "invalid params: mode 'literal' is not supported; use auto|semantic|hybrid")
    if mode not in {"auto", "semantic", "hybrid"}:
        raise RpcError(-32602, "invalid params: mode must be one of auto|semantic|hybrid")
    return mode


def _handlers(engine: ContextMemoryEngineV3) -> dict[str, Callable[[dict[str, Any]], Any]]:
    return {
        "ping": lambda _p: {"pong": True},
        "stats": lambda _p: engine.stats(),
        "list_buckets": lambda _p: engine.list_buckets(),
        "list_memories": lambda p: engine.list_memories(
            include_gray=bool(p.get("include_gray", True)),
            include_content=bool(p.get("include_content", False)),
            bucket_id=p.get("bucket_id"),
        ),
        "set_active_bucket": lambda p: engine.set_active_bucket(str(p.get("bucket_id", ""))),
        "latest_bucket_id": lambda p: engine.latest_bucket_id(p.get("bucket_id")),
        "add_memory": lambda p: engine.add_memory(
            str(p.get("raw_text", "")),
            evidence_path=p.get("evidence_path"),
            key=p.get("key"),
            topic=str(p.get("topic", "")),
            bucket_id=p.get("bucket_id"),
            force_split=bool(p.get("force_split", False)),
            create_new_bucket=bool(p.get("create_new_bucket", False)),
            chunk_max_chars=p.get("chunk_max_chars"),
            chunk_overlap_chars=p.get("chunk_overlap_chars"),
            dedup_in_bucket=bool(p.get("dedup_in_bucket", False)),
        ),
        "add_memory_from_file": lambda p: engine.add_memory_from_file(
            str(p.get("file_path", "")),
            topic=str(p.get("topic", "")),
            bucket_id=p.get("bucket_id"),
            image_extract_hint=str(p.get("image_extract_hint", "")),
            query_hint=str(p.get("query_hint", "")),
            force_split=bool(p.get("force_split", False)),
            create_new_bucket=bool(p.get("create_new_bucket", False)),
            chunk_max_chars=p.get("chunk_max_chars"),
            chunk_overlap_chars=p.get("chunk_overlap_chars"),
            dedup_in_bucket=bool(p.get("dedup_in_bucket", True)),
            auto_optimize_after_split=bool(p.get("auto_optimize_after_split", True)),
        ),
        "add_memory_from_dir": lambda p: engine.add_memory_from_dir(
            str(p.get("dir_path", "")),
            bucket_id=p.get("bucket_id"),
            auto_create_sub_buckets=bool(p.get("auto_create_sub_buckets", False)),
            image_extract_hint=str(p.get("image_extract_hint", "")),
            force_split=bool(p.get("force_split", True)),
            create_new_bucket=bool(p.get("create_new_bucket", False)),
            chunk_max_chars=p.get("chunk_max_chars"),
            chunk_overlap_chars=p.get("chunk_overlap_chars"),
            dedup_in_bucket=bool(p.get("dedup_in_bucket", True)),
            collect_token_usage=bool(p.get("collect_token_usage", False)),
        ),
        "get_memory": lambda p: engine.get_memory(
            str(p.get("key", "")),
            with_evidence=bool(p.get("with_evidence", False)),
            revision=p.get("revision"),
        ),
        "get_evidence_content": lambda p: engine.get_evidence_content(
            str(p.get("key", "")),
            revision=p.get("revision"),
        ),
        "export_memory_to_markdown": lambda p: engine.export_memory_to_markdown(str(p.get("memory_id", ""))),
        "update_memory": lambda p: engine.update_memory(
            str(p.get("key", "")),
            str(p.get("patch_text", "")),
            evidence_path=p.get("evidence_path"),
        ),
        "set_gray": lambda p: engine.set_gray(
            str(p.get("key", "")),
            gray=bool(p.get("gray", True)),
            reason=str(p.get("reason", "manual")),
        ),
        "delete_memory": lambda p: engine.delete_memory(
            str(p.get("key", "")),
            reason=str(p.get("reason", "")),
        ),
        "query": lambda p: engine.query(
            str(p.get("query_text", "")),
            top_k=(int(p["top_k"]) if "top_k" in p and p.get("top_k") is not None else None),
            include_gray=bool(p.get("include_gray", False)),
            with_evidence=bool(p.get("with_evidence", False)),
            use_cache=bool(p.get("use_cache", True)),
            bucket_id=p.get("bucket_id"),
            max_depth=p.get("max_depth"),
            mode=_parse_query_mode(p.get("mode", "auto")),
            global_recall_top_n=p.get("global_recall_top_n"),
            global_recall_top_m=p.get("global_recall_top_m"),
            global_recall_depth_limit=p.get("global_recall_depth_limit"),
            global_recall_time_budget_ms=p.get("global_recall_time_budget_ms"),
        ),
        "force_compress": lambda p: engine.force_compress(
            reason=str(p.get("reason", "manual")),
            bucket_id=p.get("bucket_id"),
        ),
        "cleanup_expired": lambda _p: engine.cleanup_expired(),
        "create_bucket": lambda p: engine.create_bucket(
            str(p.get("parent_bucket_id", "")),
            title=str(p.get("title", "")),
            summary=str(p.get("summary", "")),
            content=str(p.get("content", "")),
            summary_locked=bool(p.get("summary_locked", False)),
        ),
        "create_child_bucket": lambda p: engine.create_child_bucket(
            p.get("parent_bucket_id"),
            title=str(p.get("title", "")),
            summary=str(p.get("summary", "")),
            content=str(p.get("content", "")),
            summary_locked=bool(p.get("summary_locked", False)),
        ),
        "refresh_bucket_summary": lambda p: engine.refresh_bucket_summary(
            str(p.get("bucket_id", "")),
            force=bool(p.get("force", False)),
        ),
        "split_bucket": lambda p: engine.split_bucket(
            str(p.get("bucket_id", "")),
            reason=str(p.get("reason", "manual")),
            target_groups_min=int(p.get("target_groups_min", 2)),
            target_groups_max=int(p.get("target_groups_max", 10)),
        ),
        "optimize": lambda p: engine.optimize(
            bucket_id=p.get("bucket_id"),
            reason=str(p.get("reason", "manual_optimize")),
        ),
        "move_item": lambda p: engine.move_item(
            str(p.get("key", "")),
            target_bucket_id=str(p.get("target_bucket_id", "")),
            reason=str(p.get("reason", "manual_move")),
        ),
        "gc_storage": lambda p: engine.gc_storage(
            dry_run=bool(p.get("dry_run", True)),
            reason=str(p.get("reason", "manual_gc")),
        ),
        "get_bucket_context_usage": lambda p: engine.get_bucket_context_usage(bucket_id=p.get("bucket_id")),
        "migrate_storage_paths_to_relative": lambda _p: engine.migrate_storage_paths_to_relative(),
    }


async def _maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value) or isinstance(value, Awaitable):
        return await value
    return value


async def _call(engine: ContextMemoryEngineV3, method: str, params: dict[str, Any]) -> Any:
    handlers = _handlers(engine)
    handler = handlers.get(method)
    if handler is None:
        raise RpcError(-32601, f"method not found: {method}")

    timeout_ms = params.get("timeout_ms")
    timeout_sec = None
    if timeout_ms is not None:
        try:
            timeout_sec = max(0.001, float(timeout_ms) / 1000.0)
        except (TypeError, ValueError):
            raise RpcError(-32602, "timeout_ms must be number")

    out = handler(params)
    if timeout_sec is not None:
        try:
            out = await asyncio.wait_for(_maybe_await(out), timeout=timeout_sec)
        except asyncio.TimeoutError as exc:
            raise RpcError(-32001, f"method timeout: {method}") from exc
    else:
        out = await _maybe_await(out)
    return _jsonable(out)


async def _handle_single(engine: ContextMemoryEngineV3, body: dict[str, Any]) -> dict[str, Any]:
    req_id = body.get("id")
    if body.get("jsonrpc") != "2.0":
        return _err(-32600, "invalid request", req_id)
    method = str(body.get("method", "")).strip()
    params = body.get("params", {})
    if not isinstance(params, dict):
        return _err(-32602, "params must be object", req_id)
    try:
        result = await _call(engine, method, params)
    except RpcError as exc:
        return _err(exc.code, exc.message, req_id)
    except Exception as exc:
        msg = str(exc)
        if "context_overflow" in msg.lower() or "overflow" in msg.lower():
            return _err(-32010, msg, req_id)
        return _err(-32000, msg, req_id)
    return _ok(result, req_id)


def make_app(engine: ContextMemoryEngineV3) -> Any:
    if FastAPI is None:
        raise RuntimeError("fastapi/uvicorn is not available. Install with: pip install fastapi uvicorn")

    app = FastAPI(title="CoMe ContextMemory JSON-RPC", version="1.0.0")

    @app.post("/jsonrpc")
    async def jsonrpc(request: Request) -> Any:
        body = await request.json()
        if isinstance(body, list):
            if not body:
                return _err(-32600, "invalid request", None)
            out = []
            for item in body:
                if not isinstance(item, dict):
                    out.append(_err(-32600, "invalid request", None))
                    continue
                out.append(await _handle_single(engine, item))
            return out
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="invalid json-rpc payload")
        return await _handle_single(engine, body)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CoMe ContextMemory JSON-RPC 2.0 server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9010)
    parser.add_argument(
        "--base-dir",
        default=str(Path(__file__).resolve().parents[2] / "data" / "rpc_runtime"),
        help="runtime storage path",
    )
    parser.add_argument("--preset", default="CONTEXT_MEMORY")
    parser.add_argument("--image-preset", default="KIMI2.6")
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--mock", action="store_true")
    parser.add_argument("--no-clean", action="store_true")
    parser.add_argument("--no-forgetting", action="store_true")
    parser.add_argument("--no-debug-mode", action="store_true")
    parser.add_argument("--no-auto-manage", action="store_true")
    parser.add_argument("--max-memory-bytes", type=int, default=1_000_000_000)
    parser.add_argument("--evidence-versions", type=int, default=5)
    parser.add_argument("--max-bucket-depth", type=int, default=4)
    parser.add_argument("--query-top-k-default", type=int, default=5)
    parser.add_argument(
        "--query-max-depth-default",
        type=int,
        default=None,
        help="Global default recursive query max depth when request omits max_depth; default follows max_bucket_depth",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if uvicorn is None:
        raise RuntimeError("fastapi/uvicorn is not available. Install with: pip install fastapi uvicorn")
    global ENGINE
    ENGINE = ContextMemoryEngineV3(config=_make_config(args))
    app = make_app(ENGINE)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
