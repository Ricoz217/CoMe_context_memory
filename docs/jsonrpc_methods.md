# JSON-RPC 2.0 接口指南

本文档面向通过 HTTP 调用 `CoMe_ContextMemory` 的场景，详细说明服务启动方式、协议约定、方法参数与返回结构。

## 1. 服务地址与健康检查

1. JSON-RPC 端点：`POST /jsonrpc`
2. 健康检查：`GET /healthz`

默认监听地址：
1. `host=127.0.0.1`
2. `port=9010`

示例：
1. `http://127.0.0.1:9010/jsonrpc`
2. `http://127.0.0.1:9010/healthz`

## 2. 启动服务

示例命令：

```bash
python -m come_context_memory.rpc_server \
  --host 127.0.0.1 \
  --port 9010 \
  --base-dir ./data/rpc_runtime \
  --preset CONTEXT_MEMORY \
  --image-preset KIMI2.6
```

常用启动参数：
1. `--base-dir`: 运行时记忆库目录
2. `--preset`: 主 LLM preset
3. `--image-preset`: 图片抽取 preset
4. `--timeout`: LLM 请求超时秒数
5. `--no-clean`: 关闭清洗链路
6. `--no-forgetting`: 关闭遗忘逻辑
7. `--no-auto-manage`: 关闭自动维护
8. `--max-context-window`: 上下文窗口上限
9. `--max-memory-bytes`: 内存预算
10. `--max-bucket-depth`: 桶层级上限

## 3. JSON-RPC 协议规则

### 3.1 请求结构

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "query",
  "params": {
    "query_text": "如何写入缓存",
    "top_k": 5,
    "mode": "auto"
  }
}
```

字段说明：
1. `jsonrpc`: 固定 `"2.0"`
2. `id`: 任意可追踪请求标识（数字/字符串均可）
3. `method`: 方法名
4. `params`: 对象

### 3.2 响应结构

成功：

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {}
}
```

失败：

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

### 3.3 批量请求

`POST /jsonrpc` 支持数组请求（batch），返回数组响应，顺序与处理结果对应。

### 3.4 单次调用超时

所有方法都支持在 `params` 中附带：
1. `timeout_ms: number`

超时会返回：
1. `code = -32001`
2. `message = "method timeout: <method>"`

## 4. Query 模式约束

`query.mode` 仅支持：
1. `auto`
2. `semantic`
3. `hybrid`

约束：
1. 传入 `literal` 会返回 `-32602` 参数错误。
2. `auto` 会自动分流到 `semantic` 或 `hybrid`。

## 5. 方法总览

### 5.1 基础与状态

1. `ping`
2. `stats`
3. `list_buckets`
4. `list_memories`
5. `set_active_bucket`
6. `latest_bucket_id`
7. `get_bucket_context_usage`

### 5.2 记忆写入与修改

1. `add_memory`
2. `add_memory_from_file`
3. `add_memory_from_dir`
4. `update_memory`
5. `set_gray`
6. `delete_memory`

### 5.3 查询与读取

1. `query`
2. `get_memory`
3. `get_evidence_content`
4. `export_memory_to_markdown`

### 5.4 桶操作与维护

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

## 6. 关键方法参数与返回

### 6.1 list_memories

参数：
1. `bucket_id?: str`
2. `include_gray?: bool`（默认 `true`）
3. `include_content?: bool`（默认 `false`）

返回：
1. `memories`: 直属记忆列表
2. `buckets`: 直属子桶列表
3. `memory_count` / `bucket_count` / `total_memory_count` 等统计字段

### 6.2 add_memory

参数：
1. `raw_text: str`
2. `bucket_id?: str`
3. `topic?: str`
4. `key?: str`
5. `evidence_path?: str`
6. `force_split?: bool`
7. `create_new_bucket?: bool`
8. `chunk_max_chars?: int`
9. `chunk_overlap_chars?: int`
10. `dedup_in_bucket?: bool`（默认 `false`）

返回（`AddResult`）：
1. `success: bool`
2. `bucket_id: str`
3. `memory_count: int`
4. `added_keys: list[str]`
5. `split_performed: bool`
6. `split_rebuild_detected: bool`
7. `message: str`

### 6.3 add_memory_from_file

参数：
1. `file_path: str`
2. `bucket_id?: str`
3. `topic?: str`
4. `image_extract_hint?: str`
5. `query_hint?: str`（兼容字段，建议改用 `image_extract_hint`）
6. `force_split?: bool`
7. `create_new_bucket?: bool`
8. `chunk_max_chars?: int`
9. `chunk_overlap_chars?: int`
10. `dedup_in_bucket?: bool`（默认 `true`）
11. `auto_optimize_after_split?: bool`（默认 `true`）

返回：`AddResult`（结构同上）。

### 6.4 add_memory_from_dir

参数：
1. `dir_path: str`
2. `bucket_id?: str`
3. `auto_create_sub_buckets?: bool`（默认 `false`）
4. `image_extract_hint?: str`
5. `force_split?: bool`（默认 `true`）
6. `create_new_bucket?: bool`
7. `chunk_max_chars?: int`
8. `chunk_overlap_chars?: int`
9. `dedup_in_bucket?: bool`（默认 `true`）
10. `collect_token_usage?: bool`（默认 `false`）

返回（批处理统计）：
1. `success_count: int`
2. `fail_count: int`
3. `skip_duplicate_count: int`
4. `added_keys: list[str]`
5. `per_file_added_keys: dict[str, list[str]]`
6. `failures: list[dict]`

### 6.5 query

参数：
1. `query_text: str`
2. `bucket_id?: str`
3. `top_k?: int`（默认 `5`）
4. `include_gray?: bool`（默认 `false`）
5. `with_evidence?: bool`（默认 `false`）
6. `use_cache?: bool`（默认 `true`）
7. `max_depth?: int`
8. `mode?: str`（`auto|semantic|hybrid`）
9. `global_recall_top_n?: int`
10. `global_recall_top_m?: int`
11. `global_recall_depth_limit?: int`
12. `global_recall_time_budget_ms?: int`

返回（`QueryResult`）：
1. `success: bool`
2. `answer: str`
3. `matches: list[QueryMatch]`
4. `sub_answer: str`
5. `sub_answer_from: str`（递归替换来源 key）
6. `result_source: str`
7. `degraded: bool`
8. `degraded_reason: str`
9. `failure_stage: str`
10. `message: str`

### 6.6 delete_memory

参数：
1. `key: str`
2. `reason?: str`

说明：
1. JSON-RPC 只接收字符串 key，不接收 Python 对象。

### 6.7 optimize

参数：
1. `bucket_id?: str`
2. `reason?: str`（默认 `manual_optimize`）

返回：
1. `OptimizeResult`（含 `success`、`reason_code`、`created_buckets`、`moved_items`、`coverage_ratio` 等字段）

### 6.8 force_compress

参数：
1. `bucket_id?: str`
2. `reason?: str`（默认 `manual`）

返回：
1. `CompressResult`（含 `success`、`message`、`dropped_count`、`kept_count` 等字段）

## 7. 错误码

1. `-32600`: invalid request
2. `-32601`: method not found
3. `-32602`: invalid params
4. `-32001`: method timeout
5. `-32010`: context overflow 相关运行时错误
6. `-32000`: 通用内部错误

## 8. Python 调用示例

### 8.1 最小请求封装

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

### 8.2 创建桶、入库、查询

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
    "query_text": "如何写入缓存",
    "top_k": 5,
    "mode": "hybrid",
    "global_recall_top_n": 120,
    "global_recall_top_m": 8,
})
print("answer:", query_res["answer"])
for m in query_res.get("matches", []):
    print(m["key"], m.get("score"), m.get("summary"))
```

### 8.3 批量请求（batch）

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

### 8.4 带 timeout_ms 的单次调用

```python
res = rpc_call("query", {
    "query_text": "输入缓存逻辑在哪",
    "top_k": 5,
    "mode": "auto",
    "timeout_ms": 2500
})
print(res["answer"])
```
