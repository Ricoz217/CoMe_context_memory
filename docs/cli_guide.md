# CLI 使用指南

本文档说明 `context_memory.cli` 的常用启动参数与命令。

## 1. 启动方式

```powershell
python -m context_memory.cli --base-dir <Your Memory Base Dir>
```

## 2. 启动参数

1. 运行与模型
   - `--base-dir <path>`: 记忆库存储目录
   - `--config <path>`: 配置文件路径 (等同于设置 `COME_CONTEXT_MEMORY_CONFIG`)，不填会用默认路径
   - `--preset <name>`: 主 LLM preset（默认 `CONTEXT_MEMORY`）
   - `--image-preset <name>`: 图片抽取 preset（默认 `KIMI2.6`）
   - `--timeout <sec>`: LLM 超时
   - `--mock`: 使用 mock LLM
   - `--query-top-k-default <int>` : 设置默认的 `query` top-k，默认为5

2. 功能开关
   - `--no-clean`: 关闭 clean 阶段
   - `--no-forgetting`: 关闭负权重忘却
   - `--no-auto-manage`: 关闭自动压缩/分桶/维护
   - `--no-debug-mode`: 跳过调试初始化流程

3. 资源与限制
   - `--max-memory-bytes <int>`
   - `--evidence-versions <int>`
    - `--max-bucket-depth <int>`

## 3. 命令总览

1. 基础命令
   - `help`
   - `exit`

2. 入库命令
   - `add <text> [--bucket ...] [--force-split] [--create-new-bucket] [--chunk-max N] [--chunk-overlap N]`
   - `add_file <path> [topic] [--bucket ...] [--no-force-split] [--create-new-bucket] [--chunk-max N] [--chunk-overlap N]`
   - `add_dir <dir> [--bucket ...] [--auto-sub-buckets] [--no-force-split] [--create-new-bucket]`

3. 查询与读取
   - `query <text> [--top-k N] [--branch-expand-k N] [--gray] [--bucket <bucket_id>] [--mode auto|semantic|hybrid]`
   - `list [--gray] [--bucket <bucket_id>] [--with-content]`
   - `get <key> [--evidence]`
   - `evidence <key>`
   - `export <memory_id>`

4. 修改与维护
   - `update <key> <patch_text>`
   - `gray <key> <set|clear> [reason]`
   - `delete <key> [reason]`
   - `optimize [bucket_id]`
   - `compress [bucket_id]`
   - `split <bucket_id>`
   - `move <key> <target_bucket_id> [reason]`

5. 桶与系统
   - `buckets`
   - `create_bucket <parent_bucket_id> <title> [summary] [--lock-summary]`
   - `create_child_bucket <title> [summary] [--lock-summary]`
   - `switch_bucket <bucket_id>`
   - `latest_bucket [bucket_id]`
   - `refresh_summary <bucket_id> [--force]`
   - `gc [--apply]`
   - `cleanup`
    - `stats`

## 4. Query 模式说明

CLI 仅支持：
1. `auto`
2. `semantic`
3. `hybrid`

说明：
1. `literal` 已移除，传入会报参数错误。
2. `auto` 会根据查询文本自动分流到 `semantic/hybrid`。

## 5. 示例流程

```text
add_file D:\code\file_cache.py file_cache
query 如何写入缓存 --top-k 3 --mode auto
optimize
list --with-content
```

## 6. 注意事项

1. 不传 `--bucket` 时，使用当前 active bucket。
2. `gc` 默认 dry-run，仅 `gc --apply` 执行实际清理。
3. `add_file` 当前不支持 `pdf/docx`。

## 多接口并用约束

1. 同一个记忆库（同一 `BASE_DIR`）采用单写者模型。
2. 不要让 CLI 与其他写入接口同时写同一 `BASE_DIR`。
3. 如需多入口并用，建议统一通过一个服务进程进行写入（推荐 JSON-RPC）。

## 配置文件路径（新增）

CLI 现在支持：
1. `--config <path>`：显式指定配置文件路径
2. 其行为等价于设置环境变量 `COME_CONTEXT_MEMORY_CONFIG`

## create_bucket / create_child_bucket 语义补充

1. `create_bucket` 需要父桶 id，且支持传入 `ROOT` 表示根桶。
2. `create_child_bucket` 默认以当前 active bucket 作为父桶。

## Query 参数语义补充（Agent 视角）

这部分用于澄清一个常见误解：`mode` 不是遍历算法开关，`mode` 只影响打分融合；遍历始终包含 BFS 召回阶段。

### 1. 两层机制（必须区分）

1. 遍历层（Traversal）
   - 全局召回阶段会按预算做 BFS 扫桶。
   - 相关参数：`global_recall_top_n`、`global_recall_depth_limit`、`global_recall_time_budget_ms`、`max_depth`、`branch_expand_k`。

2. 打分层（Scoring）
   - `mode` 只控制桶内候选打分融合策略，不控制是否 BFS。
   - `semantic`：更偏向词项匹配（适合代码符号、路径、术语）。
   - `hybrid`：提高模糊语义权重（适合自然语言、描述式问题）。
   - `auto`：按查询文本特征在 `semantic/hybrid` 间自动分流。

### 2. 参数如何影响 Agent 自由度

1. 想“少漏召回、允许探索更多方向”
   - 增大：`global_recall_top_n`、`global_recall_depth_limit`、`global_recall_time_budget_ms`、`branch_expand_k`。
   - 代价：延迟与噪声上升。

2. 想“更快、更聚焦、可控”
   - 减小：`global_recall_top_n`、`global_recall_depth_limit`、`branch_expand_k`，并设置较小 `max_depth`。
   - 代价：可能漏掉远层相关信息。

3. 想“精确查代码事实”
   - 优先：`mode="semantic"`，并适度降低 `branch_expand_k`。

4. 想“容忍表述变化/模糊问题”
   - 优先：`mode="hybrid"`，并适度提高 BFS 预算参数。

### 3. 推荐的多轮查询策略（给 Agent）

1. 第一轮：广召回
   - `mode="auto"`，中高 `global_recall_top_n`，中等 `branch_expand_k`。

2. 第二轮：定向收敛
   - 根据第一轮证据 key，降低 BFS 预算并收紧 `max_depth`。

3. 第三轮：精确验证
   - `mode="semantic"`，小 `top_k`，对关键 bucket 或 key 做复查。
