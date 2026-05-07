# JSON-RPC 2.0 Interface Guide

This guide is for calling `CoMe_ContextMemory` over HTTP. It covers startup, protocol rules, method parameters, and return structures.

## 1. Service Endpoints

1. JSON-RPC endpoint: `POST /jsonrpc`
2. Health check: `GET /healthz`

Default bind:
1. `host=127.0.0.1`
2. `port=9010`

Examples:
1. `http://127.0.0.1:9010/jsonrpc`
2. `http://127.0.0.1:9010/healthz`

## 2. Start Server

Example:

```bash
python -m context_memory.rpc_server \
  --host 127.0.0.1 \
  --port 9010 \
  --base-dir ./data/rpc_runtime \
  --preset CONTEXT_MEMORY \
  --image-preset KIMI2.6
```

Common startup flags:
1. `--base-dir`: runtime storage path
2. `--preset`: main LLM preset
3. `--image-preset`: image extraction preset
4. `--timeout`: LLM timeout in seconds
5. `--no-clean`: disable cleaning stage
6. `--no-forgetting`: disable forgetting logic
7. `--no-auto-manage`: disable automatic maintenance
8. `--max-memory-bytes`: memory budget
9. `--max-bucket-depth`: max bucket depth

Single-writer rule:
1. One memory store (`same BASE_DIR`) should have only one writer process.
2. If CLI/Python/RPC are all needed, use one RPC process as the write entry instead of writing from multiple processes directly.

## 3. JSON-RPC Protocol Rules

### 3.1 Request Shape

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "query",
  "params": {
    "query_text": "How is cache written?",
    "top_k": 5,
    "mode": "auto"
  }
}
```

Fields:
1. `jsonrpc`: must be `"2.0"`
2. `id`: any traceable request id (number/string)
3. `method`: method name
4. `params`: object

### 3.2 Response Shape

Success:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {}
}
```

Error:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": -32602,
    "message": "invalid params"
  }
}
```

### 3.3 Batch Requests

`POST /jsonrpc` accepts request arrays (batch) and returns response arrays.

### 3.4 Per-call Timeout

All methods can include:
1. `timeout_ms: number`

Timeout returns:
1. `code = -32001`
2. `message = "method timeout: <method>"`

## 4. Query Mode Constraints

`query.mode` supports only:
1. `auto`
2. `semantic`
3. `hybrid`

Rules:
1. `literal` returns `-32602` invalid params.
2. `auto` routes to `semantic` or `hybrid` automatically.

## 5. Method Overview

### 5.1 Basics and Status

1. `ping`
2. `stats`
3. `list_buckets`
4. `list_memories`
5. `set_active_bucket`
6. `latest_bucket_id`
7. `get_bucket_context_usage`

### 5.2 Memory Write and Mutation

1. `add_memory`
2. `add_memory_from_file`
3. `add_memory_from_dir`
4. `update_memory`
5. `set_gray`
6. `delete_memory`

### 5.3 Query and Read

1. `query`
2. `get_memory`
3. `get_evidence_content`
4. `export_memory_to_markdown`

### 5.4 Bucket Ops and Maintenance

1. `create_bucket`
2. `create_child_bucket`
3. `refresh_bucket_summary`
4. `split_bucket`
5. `optimize`
6. `force_compress`
7. `move_item`
8. `cleanup_expired`
9. `gc_storage`
10. `migrate_storage_paths_to_relative`

Notes:
1. `create_bucket` requires `parent_bucket_id`; you may pass `ROOT` to target root bucket explicitly.
2. `create_child_bucket` uses current active bucket by default; `parent_bucket_id` is optional.

## 6. Key Methods: Parameters and Returns

### 6.1 list_memories

Params:
1. `bucket_id?: str`
2. `include_gray?: bool` (default `true`)
3. `include_content?: bool` (default `false`)

Returns:
1. `memories`: direct memory list
2. `buckets`: direct child bucket list
3. stats fields like `memory_count` / `bucket_count` / `total_memory_count`

### 6.2 add_memory

Params:
1. `raw_text: str`
2. `bucket_id?: str`
3. `topic?: str`
4. `key?: str`
5. `evidence_path?: str`
6. `force_split?: bool`
7. `create_new_bucket?: bool`
8. `chunk_max_chars?: int`
9. `chunk_overlap_chars?: int`
10. `dedup_in_bucket?: bool` (default `false`)

Returns (`AddResult`):
1. `success: bool`
2. `bucket_id: str`
3. `memory_count: int`
4. `added_keys: list[str]`
5. `split_performed: bool`
6. `split_rebuild_detected: bool`
7. `message: str`

### 6.3 add_memory_from_file

Params:
1. `file_path: str`
2. `bucket_id?: str`
3. `topic?: str`
4. `image_extract_hint?: str`
5. `query_hint?: str` (compatibility field; prefer `image_extract_hint`)
6. `force_split?: bool`
7. `create_new_bucket?: bool`
8. `chunk_max_chars?: int`
9. `chunk_overlap_chars?: int`
10. `dedup_in_bucket?: bool` (default `true`)
11. `auto_optimize_after_split?: bool` (default `true`)

Returns: `AddResult`.

### 6.4 add_memory_from_dir

Params:
1. `dir_path: str`
2. `bucket_id?: str`
3. `auto_create_sub_buckets?: bool` (default `false`)
4. `image_extract_hint?: str`
5. `force_split?: bool` (default `true`)
6. `create_new_bucket?: bool`
7. `chunk_max_chars?: int`
8. `chunk_overlap_chars?: int`
9. `dedup_in_bucket?: bool` (default `true`)
10. `collect_token_usage?: bool` (default `false`)

Returns (batch stats):
1. `success_count: int`
2. `fail_count: int`
3. `skip_duplicate_count: int`
4. `added_keys: list[str]`
5. `per_file_added_keys: dict[str, list[str]]`
6. `failures: list[dict]`

### 6.5 query

Params:
1. `query_text: str`
2. `bucket_id?: str`
3. `top_k?: int` (default `5`)
4. `include_gray?: bool` (default `false`)
5. `with_evidence?: bool` (default `false`)
6. `use_cache?: bool` (default `true`)
7. `max_depth?: int`
8. `mode?: str` (`auto|semantic|hybrid`)
9. `global_recall_top_n?: int`
10. `global_recall_top_m?: int`
11. `global_recall_depth_limit?: int`
12. `global_recall_time_budget_ms?: int`

Returns (`QueryResult`):
1. `success: bool`
2. `answer: str`
3. `matches: list[QueryMatch]`
4. `sub_answer: str`
5. `sub_answer_from: str` (source key of recursive replacement)
6. `result_source: str`
7. `degraded: bool`
8. `degraded_reason: str`
9. `failure_stage: str`
10. `message: str`

### 6.6 delete_memory

Params:
1. `key: str`
2. `reason?: str`

Note:
1. JSON-RPC accepts string keys only (not Python objects).

### 6.7 optimize

Params:
1. `bucket_id?: str`
2. `reason?: str` (default `manual_optimize`)

Returns:
1. `OptimizeResult` (`success`, `reason_code`, `created_buckets`, `moved_items`, `coverage_ratio`, etc.)

### 6.8 force_compress

Params:
1. `bucket_id?: str`
2. `reason?: str` (default `manual`)

Returns:
1. `CompressResult` (`success`, `message`, `dropped_count`, `kept_count`, etc.)

## 7. Error Codes

1. `-32600`: invalid request
2. `-32601`: method not found
3. `-32602`: invalid params
4. `-32001`: method timeout
5. `-32010`: context-overflow-related runtime error
6. `-32000`: generic internal error

## 8. Python Examples

### 8.1 Minimal RPC Helper

```python
import requests

RPC_URL = "http://127.0.0.1:9010/jsonrpc"


def rpc_call(method: str, params: dict, req_id: int = 1):
    payload = {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": method,
        "params": params,
    }
    resp = requests.post(RPC_URL, json=payload, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    if "error" in body:
        raise RuntimeError(f"RPC error {body['error']['code']}: {body['error']['message']}")
    return body["result"]
```

### 8.2 Create Bucket, Ingest, Query

```python
root = rpc_call("set_active_bucket", {"bucket_id": "bucket_20260501_xxx"})
print("active:", root)

add_res = rpc_call("add_memory_from_file", {
    "file_path": "D:/work/file_cache.py",
    "dedup_in_bucket": True,
    "auto_optimize_after_split": True,
})
print("added_keys:", add_res.get("added_keys", []))

query_res = rpc_call("query", {
    "query_text": "How is cache written?",
    "top_k": 5,
    "mode": "hybrid",
    "global_recall_top_n": 120,
    "global_recall_top_m": 8,
})
print("answer:", query_res["answer"])
for m in query_res.get("matches", []):
    print(m["key"], m.get("score"), m.get("summary"))
```

### 8.3 Batch Request

```python
import requests

payload = [
    {"jsonrpc": "2.0", "id": 1, "method": "ping", "params": {}},
    {"jsonrpc": "2.0", "id": 2, "method": "stats", "params": {}},
]
resp = requests.post("http://127.0.0.1:9010/jsonrpc", json=payload, timeout=30)
resp.raise_for_status()
print(resp.json())
```

### 8.4 Single Call with `timeout_ms`

```python
res = rpc_call("query", {
    "query_text": "Where is cache input logic",
    "top_k": 5,
    "mode": "auto",
    "timeout_ms": 2500
})
print(res["answer"])
```
