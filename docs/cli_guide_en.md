# CLI Guide

This document describes common startup parameters and commands for `come_context_memory.cli`.

## 1. Startup

```powershell
python -m context_memory.cli --base-dir <Your Memory Base Dir> --config <Your 'context_memory.yaml' path>
```

## 2. Startup Arguments

1. Runtime and model
   - `--base-dir <path>`: memory storage directory
   - `--config <path>`: explicit config file path (equivalent to setting `COME_CONTEXT_MEMORY_CONFIG`)
   - `--preset <name>`: main LLM preset (default `CONTEXT_MEMORY`)
   - `--image-preset <name>`: image extraction preset (default `KIMI2.6`)
   - `--timeout <sec>`: LLM timeout
   - `--mock`: use mock LLM

2. Feature switches
   - `--no-clean`: disable clean stage
   - `--no-forgetting`: disable negative-weight forgetting
   - `--no-auto-manage`: disable automatic compress/split/maintenance
   - `--no-debug-mode`: skip debug initialization flow

3. Resource and limits
   - `--max-memory-bytes <int>`
   - `--evidence-versions <int>`
   - `--max-bucket-depth <int>`

## 3. Command Overview

1. Basic commands
   - `help`
   - `exit`

2. Ingest commands
   - `add <text> [--bucket ...] [--force-split] [--create-new-bucket] [--chunk-max N] [--chunk-overlap N]`
   - `add_file <path> [topic] [--bucket ...] [--force-split] [--create-new-bucket] [--chunk-max N] [--chunk-overlap N]`
   - `add_dir <dir> [--bucket ...] [--auto-sub-buckets] [--force-split] [--create-new-bucket]`

3. Query and read
   - `query <text> [--top-k N] [--gray] [--bucket <bucket_id>] [--mode auto|semantic|hybrid]`
   - `list [--gray] [--bucket <bucket_id>] [--with-content]`
   - `get <key> [--evidence]`
   - `evidence <key>`
   - `export <memory_id>`

4. Mutation and maintenance
   - `update <key> <patch_text>`
   - `gray <key> <set|clear> [reason]`
   - `delete <key> [reason]`
   - `optimize [bucket_id]`
   - `compress [bucket_id]`
   - `split <bucket_id>`
   - `move <key> <target_bucket_id> [reason]`

5. Buckets and system
   - `buckets`
   - `create_bucket <parent_bucket_id> <title> [summary] [--lock-summary]`
   - `create_child_bucket <title> [summary] [--lock-summary]`
   - `switch_bucket <bucket_id>`
   - `latest_bucket [bucket_id]`
   - `refresh_summary <bucket_id> [--force]`
   - `gc [--apply]`
   - `cleanup`
   - `stats`

## 4. Query Modes

CLI supports only:
1. `auto`
2. `semantic`
3. `hybrid`

Notes:
1. `literal` has been removed and will raise a parameter error.
2. `auto` routes query text to `semantic` or `hybrid` automatically.

## 5. Example Flow

```text
add_file D:\codeile_cache.py file_cache
query How is cache written --top-k 3 --mode auto
optimize
list --with-content
```

## 6. Notes

1. If `--bucket` is omitted, current active bucket is used.
2. `gc` is dry-run by default; use `gc --apply` for actual cleanup.
3. `add_file` currently does not support `pdf/docx`.
4. Single-writer rule: do not run CLI and other writer interfaces against the same `BASE_DIR` at the same time.
5. If multiple interfaces are required, route writes through one service process (recommended: JSON-RPC).
6. `create_bucket` accepts a real parent bucket id; use `ROOT` to explicitly target root bucket.
7. `create_child_bucket` always creates under current active bucket.
