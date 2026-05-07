# JSON-RPC 2.0 API Specification

This document provides a strict API-style reference for the CoMe JSON-RPC service:
1. Every method is listed.
2. Every method has a parameter table.
3. Required/optional flags, types, defaults, and behavior are explicit.

## 1. Endpoint

1. JSON-RPC: `POST /jsonrpc`
2. Health check: `GET /healthz`

Default bind:
1. `host=127.0.0.1`
2. `port=9010`

## 2. Server Startup Parameters

| Arg | Required | Type | Default | Description |
|---|---|---|---|---|
| `--host` | No | `str` | `127.0.0.1` | Bind host |
| `--port` | No | `int` | `9010` | Bind port |
| `--base-dir` | No | `str` | `./data/rpc_runtime` | Storage directory |
| `--preset` | No | `str` | `CONTEXT_MEMORY` | Main LLM preset |
| `--image-preset` | No | `str` | `KIMI2.6` | Image extraction preset |
| `--timeout` | No | `float` | `300.0` | LLM request timeout (seconds) |
| `--mock` | No | `flag` | `false` | Use mock LLM |
| `--no-clean` | No | `flag` | `false` | Disable clean stage |
| `--no-forgetting` | No | `flag` | `false` | Disable forgetting |
| `--no-debug-mode` | No | `flag` | `false` | Skip debug init |
| `--no-auto-manage` | No | `flag` | `false` | Disable auto maintenance |
| `--max-memory-bytes` | No | `int` | `1000000000` | Memory budget |
| `--evidence-versions` | No | `int` | `5` | Evidence retention versions |
| `--max-bucket-depth` | No | `int` | `4` | Maximum bucket depth |
| `--query-top-k-default` | No | `int` | `5` | Global query default `top_k` when request omits `top_k` |

## 3. Protocol Rules

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

`POST /jsonrpc` supports array payloads (batch).

### 3.4 Universal Optional Param

All methods support:

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `timeout_ms` | No | `number` | `null` | Per-request timeout. If exceeded: `-32001 method timeout` |

## 4. Query Mode Rules

Allowed modes:
1. `auto`
2. `semantic`
3. `hybrid`

Rules:
1. `literal` is rejected with `-32602`.
2. If `top_k` is omitted in `query`, engine uses global `query_top_k_default` (default `5`).
3. If `top_k` is provided, request value overrides global default.

## 5. Method List

1. `ping`
2. `stats`
3. `list_buckets`
4. `list_memories`
5. `set_active_bucket`
6. `latest_bucket_id`
7. `add_memory`
8. `add_memory_from_file`
9. `add_memory_from_dir`
10. `get_memory`
11. `get_evidence_content`
12. `export_memory_to_markdown`
13. `update_memory`
14. `set_gray`
15. `delete_memory`
16. `query`
17. `force_compress`
18. `cleanup_expired`
19. `create_bucket`
20. `create_child_bucket`
21. `refresh_bucket_summary`
22. `split_bucket`
23. `optimize`
24. `move_item`
25. `gc_storage`
26. `get_bucket_context_usage`
27. `migrate_storage_paths_to_relative`

## 6. Method Parameters (Standard Tables)

### 6.1 `ping`

No business params.

### 6.2 `stats`

No business params.

### 6.3 `list_buckets`

No business params.

### 6.4 `list_memories`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `bucket_id` | No | `str` | `null` | Target bucket; active bucket when omitted |
| `include_gray` | No | `bool` | `true` | Include gray records |
| `include_content` | No | `bool` | `false` | Include content text in list output |

### 6.5 `set_active_bucket`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `bucket_id` | Yes | `str` | - | Bucket id to activate |

### 6.6 `latest_bucket_id`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `bucket_id` | No | `str` | `null` | Resolve redirected/latest id; active bucket when omitted |

### 6.7 `add_memory`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `raw_text` | Yes | `str` | - | Raw text to ingest |
| `evidence_path` | No | `str` | `null` | Evidence file path |
| `key` | No | `str` | `null` | Force memory key (advanced/internal usage) |
| `topic` | No | `str` | `""` | Topic hint |
| `bucket_id` | No | `str` | `null` | Target bucket; active bucket when omitted |
| `force_split` | No | `bool` | `false` | Force split flow |
| `create_new_bucket` | No | `bool` | `false` | Allow creating bucket in flow |
| `chunk_max_chars` | No | `int` | `null` | Split chunk max size |
| `chunk_overlap_chars` | No | `int` | `null` | Split chunk overlap |
| `dedup_in_bucket` | No | `bool` | `false` | Dedup direct memory chunks in target bucket |

### 6.8 `add_memory_from_file`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `file_path` | Yes | `str` | - | File path |
| `topic` | No | `str` | `""` | Topic hint |
| `bucket_id` | No | `str` | `null` | Target bucket |
| `image_extract_hint` | No | `str` | `""` | Image extraction hint |
| `query_hint` | No | `str` | `""` | Compatibility hint field |
| `force_split` | No | `bool` | `false` | Force split |
| `create_new_bucket` | No | `bool` | `false` | Allow creating bucket in flow |
| `chunk_max_chars` | No | `int` | `null` | Split chunk max size |
| `chunk_overlap_chars` | No | `int` | `null` | Split chunk overlap |
| `dedup_in_bucket` | No | `bool` | `true` | Dedup direct memory chunks in target bucket |
| `auto_optimize_after_split` | No | `bool` | `true` | Auto optimize once when split/rebuild detected |

### 6.9 `add_memory_from_dir`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `dir_path` | Yes | `str` | - | Directory path |
| `bucket_id` | No | `str` | `null` | Target bucket |
| `auto_create_sub_buckets` | No | `bool` | `false` | Auto-create child buckets by subdir |
| `image_extract_hint` | No | `str` | `""` | Image extraction hint |
| `force_split` | No | `bool` | `true` | Force split |
| `create_new_bucket` | No | `bool` | `false` | Allow creating bucket in flow |
| `chunk_max_chars` | No | `int` | `null` | Split chunk max size |
| `chunk_overlap_chars` | No | `int` | `null` | Split chunk overlap |
| `dedup_in_bucket` | No | `bool` | `true` | Dedup direct memory chunks in target bucket |
| `collect_token_usage` | No | `bool` | `false` | Return token usage statistics for this batch |

### 6.10 `get_memory`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `key` | Yes | `str` | - | Memory key |
| `with_evidence` | No | `bool` | `false` | Include evidence content/path data |
| `revision` | No | `str` | `null` | Query specific revision |

### 6.11 `get_evidence_content`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `key` | Yes | `str` | - | Memory key |
| `revision` | No | `str` | `null` | Specific revision |

### 6.12 `export_memory_to_markdown`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `memory_id` | Yes | `str` | - | Memory id to export |

### 6.13 `update_memory`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `key` | Yes | `str` | - | Memory key |
| `patch_text` | Yes | `str` | - | Patch content |
| `evidence_path` | No | `str` | `null` | Evidence file path |

### 6.14 `set_gray`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `key` | Yes | `str` | - | Memory key |
| `gray` | No | `bool` | `true` | Gray state to set |
| `reason` | No | `str` | `"manual"` | Reason text |

### 6.15 `delete_memory`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `key` | Yes | `str` | - | Memory key |
| `reason` | No | `str` | `""` | Delete reason |

### 6.16 `query`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `query_text` | Yes | `str` | - | Query text |
| `top_k` | No | `int` | `null` | When omitted, use global `query_top_k_default` |
| `include_gray` | No | `bool` | `false` | Include gray records |
| `with_evidence` | No | `bool` | `false` | Include evidence on matched records |
| `use_cache` | No | `bool` | `true` | Enable query cache |
| `bucket_id` | No | `str` | `null` | Query starting bucket |
| `max_depth` | No | `int` | `null` | Recursive query depth cap |
| `mode` | No | `str` | `"auto"` | `auto|semantic|hybrid` |
| `global_recall_top_n` | No | `int` | `null` | Override global recall top N |
| `global_recall_top_m` | No | `int` | `null` | Override global recall top M |
| `global_recall_depth_limit` | No | `int` | `null` | Override recall traversal depth |
| `global_recall_time_budget_ms` | No | `int` | `null` | Override recall time budget |

### 6.17 `force_compress`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `reason` | No | `str` | `"manual"` | Compress reason |
| `bucket_id` | No | `str` | `null` | Target bucket |

### 6.18 `cleanup_expired`

No business params.

### 6.19 `create_bucket`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `parent_bucket_id` | Yes | `str` | - | Parent bucket id (`ROOT` allowed) |
| `title` | Yes | `str` | - | Bucket title |
| `summary` | No | `str` | `""` | Bucket summary |
| `content` | No | `str` | `""` | Bucket content |
| `summary_locked` | No | `bool` | `false` | Lock summary edits |

### 6.20 `create_child_bucket`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `title` | Yes | `str` | - | Bucket title |
| `parent_bucket_id` | No | `str` | `null` | Parent bucket (active bucket when omitted) |
| `summary` | No | `str` | `""` | Bucket summary |
| `content` | No | `str` | `""` | Bucket content |
| `summary_locked` | No | `bool` | `false` | Lock summary edits |

### 6.21 `refresh_bucket_summary`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `bucket_id` | Yes | `str` | - | Target bucket |
| `force` | No | `bool` | `false` | Force summary refresh |

### 6.22 `split_bucket`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `bucket_id` | Yes | `str` | - | Target bucket |
| `reason` | No | `str` | `"manual"` | Split reason |
| `target_groups_min` | No | `int` | `2` | Minimum split groups |
| `target_groups_max` | No | `int` | `10` | Maximum split groups |

### 6.23 `optimize`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `bucket_id` | No | `str` | `null` | Target bucket (active bucket when omitted) |
| `reason` | No | `str` | `"manual_optimize"` | Optimize reason |

### 6.24 `move_item`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `key` | Yes | `str` | - | Memory key to move |
| `target_bucket_id` | Yes | `str` | - | Destination bucket id |
| `reason` | No | `str` | `"manual_move"` | Move reason |

### 6.25 `gc_storage`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `dry_run` | No | `bool` | `true` | Dry-run only when true |
| `reason` | No | `str` | `"manual_gc"` | GC reason |

### 6.26 `get_bucket_context_usage`

| Param | Required | Type | Default | Description |
|---|---|---|---|---|
| `bucket_id` | No | `str` | `null` | Target bucket (active bucket when omitted) |

### 6.27 `migrate_storage_paths_to_relative`

No business params.

## 7. Error Codes

1. `-32600`: invalid request
2. `-32601`: method not found
3. `-32602`: invalid params
4. `-32001`: method timeout
5. `-32010`: context-overflow-related runtime error
6. `-32000`: generic internal error
