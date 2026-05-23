import asyncio
import json
import os
import shlex
import sys
import traceback
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.input_manager import read_command_async


class JsonRpcClient:
    def __init__(self, url: str, *, timeout: float = 120.0) -> None:
        self.url = url
        self.timeout = timeout
        self._req_id = 0
        self._client = httpx.AsyncClient(timeout=self.timeout)

    async def close(self) -> None:
        await self._client.aclose()

    async def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        self._req_id += 1
        payload = {
            "jsonrpc": "2.0",
            "id": self._req_id,
            "method": method,
            "params": params or {},
        }
        resp = await self._client.post(self.url, json=payload)
        resp.raise_for_status()
        body = resp.json()
        if "error" in body:
            err = body["error"]
            raise RuntimeError(f"RPC error {err.get('code')}: {err.get('message')}")
        return body.get("result")


def _json_print(obj: Any) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False)


async def _ensure_test_bucket(client: JsonRpcClient, title: str = "TEST") -> dict[str, Any]:
    buckets = await client.call("list_buckets", {})
    target = None
    if isinstance(buckets, list):
        for b in buckets:
            if isinstance(b, dict) and str(b.get("title", "")).strip() == title:
                target = b
                break
    if target is None:
        target = await client.call(
            "create_bucket",
            {
                "parent_bucket_id": "ROOT",
                "title": title,
            },
        )
    bid = str(target.get("bucket_id", "")).strip() if isinstance(target, dict) else ""
    if bid:
        await client.call("set_active_bucket", {"bucket_id": bid})
    return {"bucket": target, "active_set": bool(bid)}


async def main() -> None:
    rpc_url = os.getenv("COME_RPC_URL", "http://127.0.0.1:9010/jsonrpc").strip()
    client = JsonRpcClient(rpc_url)

    async def add_file(_file: str) -> None:
        _path = Path(_file.strip('"')).expanduser()
        result = await client.call(
            "add_memory_from_file",
            {
                "file_path": str(_path),
                "force_split": True,
            },
        )
        print(_json_print(result))

    async def add_memory(_text: str) -> None:
        print(_json_print(await client.call("add_memory", {"raw_text": _text})))

    async def get_memory(key: str) -> None:
        print(_json_print(await client.call("get_memory", {"key": key})))

    async def list_memory() -> None:
        print(_json_print(await client.call("list_memories", {"include_gray": False})))

    async def query(_text: str) -> None:
        print(_json_print(await client.call("query", {"query_text": _text})))

    async def stats() -> None:
        print(_json_print(await client.call("stats", {})))

    async def optimize() -> None:
        print(_json_print(await client.call("optimize", {})))

    async def export(mem_id: str) -> None:
        print(_json_print(await client.call("export_memory_to_markdown", {"memory_id": mem_id})))

    async def compress() -> None:
        print(_json_print(await client.call("force_compress", {})))

    async def delete(hash_id: str) -> None:
        print(_json_print(await client.call("delete_memory", {"key": hash_id})))

    async def add_dir(_dir: str) -> None:
        _dir = Path(_dir.strip('"')).expanduser()
        print(
            _json_print(
                await client.call(
                    "add_memory_from_dir",
                    {
                        "dir_path": str(_dir),
                        "force_split": True,
                    },
                )
            )
        )

    async def switch_bucket(bucket_id: str) -> None:
        print(_json_print(await client.call("set_active_bucket", {"bucket_id": bucket_id})))

    async def latest_bucket(bucket_id: str = "") -> None:
        params = {"bucket_id": bucket_id} if bucket_id else {}
        print(_json_print(await client.call("latest_bucket_id", params)))

    async def raw_rpc(payload_text: str) -> None:
        # usage: rpc <method> {"k":"v"}
        pieces = payload_text.strip().split(" ", 1)
        method = pieces[0].strip() if pieces else ""
        if not method:
            print('usage: rpc <method> {"k":"v"}')
            return
        params: dict[str, Any] = {}
        if len(pieces) > 1 and pieces[1].strip():
            params = json.loads(pieces[1].strip())
            if not isinstance(params, dict):
                raise ValueError("rpc params must be a JSON object")
        print(_json_print(await client.call(method, params)))

    exec_mapping = {
        "add_file": add_file,
        "add": add_memory,
        "get": get_memory,
        "list": list_memory,
        "query": query,
        "stats": stats,
        "optimize": optimize,
        "export": export,
        "compress": compress,
        "delete": delete,
        "add_dir": add_dir,
        "switch_bucket": switch_bucket,
        "latest_bucket": latest_bucket,
        "rpc": raw_rpc,
    }
    async def _run():
        if params:
            await exec_mapping[cmd](params)
        else:
            await exec_mapping[cmd]()

    try:
        init_res = await _ensure_test_bucket(client, title="TEST")
        print(f"RPC: {rpc_url}")
        print(_json_print({"init": init_res}))
        print("commands: add/add_file/add_dir/list/query/stats/optimize/export/compress/delete/switch_bucket/latest_bucket/rpc")

        while True:
            try:
                prompt: str = await read_command_async("jsonrpc> ")
                stripped = prompt.strip()
                if not stripped:
                    continue
                if stripped.lower() in {"exit", "quit"}:
                    return
                parts = shlex.split(stripped)
                cmd = parts[0]
                params = " ".join(parts[1:]) if len(parts) > 1 else ""
                if cmd not in exec_mapping:
                    print(f"unknown cmd: {cmd}")
                    continue
                asyncio.create_task(_run())

            except asyncio.CancelledError:
                return
            except KeyboardInterrupt:
                return
            except Exception as exc:
                print(exc)
                print(traceback.format_exc())
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
