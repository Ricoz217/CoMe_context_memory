# JSON-RPC 2.0 接口规范

本文档按标准 API 规格给出 CoMe 的 JSON-RPC 接口：
1. 方法完整清单
2. 每个方法的参数表
3. 必填/可选、类型、默认值、行为说明

## 1. 服务端点

1. JSON-RPC：`POST /jsonrpc`
2. 健康检查：`GET /healthz`

默认监听：
1. `host=127.0.0.1`
2. `port=9010`

## 2. 服务启动参数

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `--host` | 否 | `str` | `127.0.0.1` | 监听地址 |
| `--port` | 否 | `int` | `9010` | 监听端口 |
| `--base-dir` | 否 | `str` | `./data/rpc_runtime` | 存储目录 |
| `--preset` | 否 | `str` | `CONTEXT_MEMORY` | 主 LLM 预设 |
| `--image-preset` | 否 | `str` | `KIMI2.6` | 图像提取预设 |
| `--timeout` | 否 | `float` | `300.0` | LLM 请求超时（秒） |
| `--mock` | 否 | `flag` | `false` | 是否使用 mock LLM |
| `--no-clean` | 否 | `flag` | `false` | 关闭 clean 阶段 |
| `--no-forgetting` | 否 | `flag` | `false` | 关闭遗忘逻辑 |
| `--no-debug-mode` | 否 | `flag` | `false` | 跳过 debug 初始化 |
| `--no-auto-manage` | 否 | `flag` | `false` | 关闭自动维护 |
| `--max-memory-bytes` | 否 | `int` | `1000000000` | 内存预算 |
| `--evidence-versions` | 否 | `int` | `5` | 证据版本保留数 |
| `--max-bucket-depth` | 否 | `int` | `4` | 最大桶层级 |
| `--query-top-k-default` | 否 | `int` | `5` | query 未传 `top_k` 时的全局默认值 |

## 3. 协议规则

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

`POST /jsonrpc` 支持数组批量请求。

### 3.4 全方法通用参数

所有方法都支持：

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `timeout_ms` | 否 | `number` | `null` | 本次调用超时毫秒数。超时返回 `-32001` |

## 4. Query 模式规则

`mode` 仅支持：
1. `auto`
2. `semantic`
3. `hybrid`

规则：
1. `literal` 会被拒绝（`-32602`）。
2. `query` 未传 `top_k` 时，使用全局 `query_top_k_default`（默认 `5`）。
3. `query` 显式传入 `top_k` 时，以请求参数为准。

## 5. 方法总览

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

## 6. 各方法参数表

### 6.1 `ping`

无业务参数。

### 6.2 `stats`

无业务参数。

### 6.3 `list_buckets`

无业务参数。

### 6.4 `list_memories`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `bucket_id` | 否 | `str` | `null` | 目标桶，不传走 active bucket |
| `include_gray` | 否 | `bool` | `true` | 是否包含灰记录 |
| `include_content` | 否 | `bool` | `false` | 是否返回 content 正文 |

### 6.5 `set_active_bucket`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `bucket_id` | 是 | `str` | - | 要切换的桶 id |

### 6.6 `latest_bucket_id`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `bucket_id` | 否 | `str` | `null` | 要追溯的桶 id，不传走 active bucket |

### 6.7 `add_memory`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `raw_text` | 是 | `str` | - | 原始文本 |
| `evidence_path` | 否 | `str` | `null` | 证据文件路径 |
| `key` | 否 | `str` | `null` | 指定 key（高级/内部用） |
| `topic` | 否 | `str` | `""` | 主题提示 |
| `bucket_id` | 否 | `str` | `null` | 目标桶 |
| `force_split` | 否 | `bool` | `false` | 强制切分 |
| `create_new_bucket` | 否 | `bool` | `false` | 允许流程中创建新桶 |
| `chunk_max_chars` | 否 | `int` | `null` | 切分最大长度 |
| `chunk_overlap_chars` | 否 | `int` | `null` | 切分重叠长度 |
| `dedup_in_bucket` | 否 | `bool` | `false` | 桶内直属分片去重 |

### 6.8 `add_memory_from_file`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `file_path` | 是 | `str` | - | 文件路径 |
| `topic` | 否 | `str` | `""` | 主题提示 |
| `bucket_id` | 否 | `str` | `null` | 目标桶 |
| `image_extract_hint` | 否 | `str` | `""` | 图像提取提示词 |
| `query_hint` | 否 | `str` | `""` | 兼容字段 |
| `force_split` | 否 | `bool` | `false` | 强制切分 |
| `create_new_bucket` | 否 | `bool` | `false` | 允许流程中创建新桶 |
| `chunk_max_chars` | 否 | `int` | `null` | 切分最大长度 |
| `chunk_overlap_chars` | 否 | `int` | `null` | 切分重叠长度 |
| `dedup_in_bucket` | 否 | `bool` | `true` | 桶内直属分片去重 |
| `auto_optimize_after_split` | 否 | `bool` | `true` | 触发 split/rebuild 后自动 optimize 一次 |

### 6.9 `add_memory_from_dir`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `dir_path` | 是 | `str` | - | 目录路径 |
| `bucket_id` | 否 | `str` | `null` | 目标桶 |
| `auto_create_sub_buckets` | 否 | `bool` | `false` | 是否按子目录自动建桶 |
| `image_extract_hint` | 否 | `str` | `""` | 图像提取提示词 |
| `force_split` | 否 | `bool` | `true` | 强制切分 |
| `create_new_bucket` | 否 | `bool` | `false` | 允许流程中创建新桶 |
| `chunk_max_chars` | 否 | `int` | `null` | 切分最大长度 |
| `chunk_overlap_chars` | 否 | `int` | `null` | 切分重叠长度 |
| `dedup_in_bucket` | 否 | `bool` | `true` | 桶内直属分片去重 |
| `collect_token_usage` | 否 | `bool` | `false` | 是否统计本批 token 消耗 |

### 6.10 `get_memory`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `key` | 是 | `str` | - | 记忆 key |
| `with_evidence` | 否 | `bool` | `false` | 是否包含证据信息 |
| `revision` | 否 | `str` | `null` | 指定 revision 查询 |

### 6.11 `get_evidence_content`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `key` | 是 | `str` | - | 记忆 key |
| `revision` | 否 | `str` | `null` | 指定 revision |

### 6.12 `export_memory_to_markdown`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `memory_id` | 是 | `str` | - | 要导出的记忆 id |

### 6.13 `update_memory`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `key` | 是 | `str` | - | 记忆 key |
| `patch_text` | 是 | `str` | - | 修改文本 |
| `evidence_path` | 否 | `str` | `null` | 证据文件路径 |

### 6.14 `set_gray`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `key` | 是 | `str` | - | 记忆 key |
| `gray` | 否 | `bool` | `true` | 目标灰状态 |
| `reason` | 否 | `str` | `"manual"` | 原因 |

### 6.15 `delete_memory`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `key` | 是 | `str` | - | 记忆 key |
| `reason` | 否 | `str` | `""` | 原因 |

### 6.16 `query`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `query_text` | 是 | `str` | - | 查询文本 |
| `top_k` | 否 | `int` | `null` | 不传时走 `query_top_k_default` |
| `include_gray` | 否 | `bool` | `false` | 是否包含灰记录 |
| `with_evidence` | 否 | `bool` | `false` | 是否返回证据信息 |
| `use_cache` | 否 | `bool` | `true` | 是否启用缓存 |
| `bucket_id` | 否 | `str` | `null` | 起始查询桶 |
| `max_depth` | 否 | `int` | `null` | 递归深度上限 |
| `mode` | 否 | `str` | `"auto"` | `auto|semantic|hybrid` |
| `global_recall_top_n` | 否 | `int` | `null` | 覆盖全局召回 top_n |
| `global_recall_top_m` | 否 | `int` | `null` | 覆盖全局召回 top_m |
| `global_recall_depth_limit` | 否 | `int` | `null` | 覆盖召回深度限制 |
| `global_recall_time_budget_ms` | 否 | `int` | `null` | 覆盖召回时间预算 |

### 6.17 `force_compress`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `reason` | 否 | `str` | `"manual"` | 压缩原因 |
| `bucket_id` | 否 | `str` | `null` | 目标桶 |

### 6.18 `cleanup_expired`

无业务参数。

### 6.19 `create_bucket`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `parent_bucket_id` | 是 | `str` | - | 父桶 id（支持 `ROOT`） |
| `title` | 是 | `str` | - | 桶标题 |
| `summary` | 否 | `str` | `""` | 桶摘要 |
| `content` | 否 | `str` | `""` | 桶内容 |
| `summary_locked` | 否 | `bool` | `false` | 是否锁定摘要 |

### 6.20 `create_child_bucket`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `title` | 是 | `str` | - | 桶标题 |
| `parent_bucket_id` | 否 | `str` | `null` | 父桶 id，不传走 active bucket |
| `summary` | 否 | `str` | `""` | 桶摘要 |
| `content` | 否 | `str` | `""` | 桶内容 |
| `summary_locked` | 否 | `bool` | `false` | 是否锁定摘要 |

### 6.21 `refresh_bucket_summary`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `bucket_id` | 是 | `str` | - | 目标桶 |
| `force` | 否 | `bool` | `false` | 是否强制刷新 |

### 6.22 `split_bucket`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `bucket_id` | 是 | `str` | - | 目标桶 |
| `reason` | 否 | `str` | `"manual"` | 原因 |
| `target_groups_min` | 否 | `int` | `2` | 最小分组数 |
| `target_groups_max` | 否 | `int` | `10` | 最大分组数 |

### 6.23 `optimize`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `bucket_id` | 否 | `str` | `null` | 目标桶，不传走 active bucket |
| `reason` | 否 | `str` | `"manual_optimize"` | 原因 |

### 6.24 `move_item`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `key` | 是 | `str` | - | 要移动的 key |
| `target_bucket_id` | 是 | `str` | - | 目标桶 id |
| `reason` | 否 | `str` | `"manual_move"` | 原因 |

### 6.25 `gc_storage`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `dry_run` | 否 | `bool` | `true` | 是否仅预演 |
| `reason` | 否 | `str` | `"manual_gc"` | 原因 |

### 6.26 `get_bucket_context_usage`

| 参数 | 必填 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| `bucket_id` | 否 | `str` | `null` | 目标桶，不传走 active bucket |

### 6.27 `migrate_storage_paths_to_relative`

无业务参数。

## 7. 错误码

1. `-32600`：invalid request
2. `-32601`：method not found
3. `-32602`：invalid params
4. `-32001`：method timeout
5. `-32010`：上下文超窗类错误
6. `-32000`：通用内部错误
