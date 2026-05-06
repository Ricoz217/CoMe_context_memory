# CLI 使用指南

本文档说明 `come_context_memory.cli` 的常用启动参数与命令。

## 1. 启动方式

```powershell
$env:PYTHONPATH='D:\Python\CoMe_ContextMemory\src'
python -m come_context_memory.cli --base-dir D:\Python\CoMe_ContextMemory\data\cli_runtime
```

## 2. 启动参数

1. 运行与模型
   - `--base-dir <path>`: 记忆库存储目录
   - `--preset <name>`: 主 LLM preset（默认 `CONTEXT_MEMORY`）
   - `--image-preset <name>`: 图片抽取 preset（默认 `KIMI2.6`）
   - `--timeout <sec>`: LLM 超时
   - `--mock`: 使用 mock LLM

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
   - `add_file <path> [topic] [--bucket ...] [--force-split] [--create-new-bucket] [--chunk-max N] [--chunk-overlap N]`
   - `add_dir <dir> [--bucket ...] [--auto-sub-buckets] [--force-split] [--create-new-bucket]`

3. 查询与读取
   - `query <text> [--top-k N] [--gray] [--bucket <bucket_id>] [--mode auto|semantic|hybrid]`
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
   - `create_child_bucket <parent_bucket_id> <title> [summary] [--lock-summary]`
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
