# Python API Guide

This guide is for using `CoMe_ContextMemory` directly as a Python library.

## 1. Minimal Example

```python
import asyncio
from context_memory import ContextMemoryConfig, ContextMemoryEngineV3


async def main():
    cfg = ContextMemoryConfig(
        base_dir="data/my_memory",
        llm_preset="CONTEXT_MEMORY",
        image_llm_preset="KIMI2.6",
        use_mock_llm=False,
    )
    engine = ContextMemoryEngineV3(config=cfg)

    root = await engine.set_bucket("Demo")
    await root.add_memory("The file cache module contains add_file / get_file_path / remove_fire")
    result = await root.query("How is cache written?", top_k=3, mode="auto")
    print(result.answer)


asyncio.run(main())
```

## 2. Core Objects

1. `ContextMemoryConfig`
   - Engine config object (depth, window, auto-maintenance, default query mode, etc.)

2. `ContextMemoryEngineV3`
   - Main engine object exposing all capabilities

3. `BucketHandle`
   - Bucket-scoped handle (`add/query/list/optimize/...`)

## 3. Common Engine APIs

1. Ingest and mutate
   - `add_memory(raw_text, ...)`
   - `add_memory_from_file(file_path, ...)`
   - `add_memory_from_dir(dir_path, ...)`
   - `update_memory(key, patch_text, ...)`
   - `set_gray(key, gray=True/False, ...)`
   - `delete_memory(key_or_obj, ...)`

2. Query and read
   - `query(query_text, top_k=None, mode="auto", ...)`
   - `list_memories(include_gray=False, include_content=False, ...)`
   - `get_memory(key, with_evidence=False, revision=None)`
   - `get_evidence_content(key, revision=None)`
   - `export_memory_to_markdown(memory_id)`

3. Bucket operations
   - `set_bucket(title, ...)`
   - `set_active_bucket(bucket_id)` / `switch_active_bucket(bucket_id)`
   - `create_bucket(parent_bucket_id, ...)`
   - `create_child_bucket(parent_bucket_id=None, ...)`
   - `split_bucket(bucket_id, ...)`
   - `optimize(bucket_id=None, ...)`
   - `force_compress(bucket_id=None, ...)`
   - `move_item(key, target_bucket_id, ...)`

4. Maintenance and stats
   - `stats()`
   - `cleanup_expired()`
   - `gc_storage(dry_run=True, ...)`
   - `migrate_storage_paths_to_relative()`

## 4. Query Modes

Public modes:
1. `auto`
2. `semantic`
3. `hybrid`

Rule:
1. `auto` routes literal-heavy queries to `hybrid`, and regular natural-language queries to `semantic`.

Top-k default behavior:
1. If `top_k` is omitted (`None`), engine uses global config `query_top_k_default` (default `5`).
2. If `top_k` is explicitly provided, call value takes precedence.

## 5. Batch Ingest Return Values

1. `add_memory_from_file(...)` returns `AddResult`
   - `added_keys`: keys newly added in this call
   - `split_performed`: whether chunk split happened
   - `split_rebuild_detected`: whether split/rebuild was detected

2. `add_memory_from_dir(...)` returns `dict`
   - `success_count` / `fail_count` / `skip_duplicate_count`
   - `added_keys` (aggregated)
   - `per_file_added_keys` (per file)

Notes:
1. You can use `added_keys` for manual rollback via `delete_memory`.
2. Duplicates and failed items are excluded from `added_keys`.

## 6. Bucket Routing and Active Bucket

**Object-based calls are strongly recommended. They pass `bucket_id` automatically, so you can usually ignore manual routing details.**

1. If `bucket_id` is omitted, calls use current `active_bucket_id`.
2. It is recommended to call `set_active_bucket(...)` at session start.
3. `latest_bucket_id(...)` can resolve to the latest bucket after optimize/split.
4. `create_bucket(parent_bucket_id=...)` accepts `ROOT` as an explicit parent shortcut.
5. `create_child_bucket(...)` defaults to the current active bucket when `parent_bucket_id` is omitted.

## 7. File Ingest Notes

1. `add_memory_from_file` currently supports:
   - text files (including source code)
   - image files (via image extraction chain)

2. Not supported yet:
   - `pdf`
   - `docx`

3. Prompt hint parameters:
   - `image_extract_hint` is recommended
   - `query_hint` is retained for compatibility, but should be avoided in new code

## 8. Resource Cleanup

When the process is ending or the engine is no longer used, close it:

```python
await engine.close()
# or
engine.shutdown(wait=False)
```

This releases internal resources such as query CPU thread pools.

## 9. Multi-Interface Concurrency Constraint

1. One memory store (`same BASE_DIR`) must follow a single-writer model.
2. Running Python API, CLI, and JSON-RPC as separate processes on the same `BASE_DIR` can cause multi-writer risk.
3. If you need multiple interfaces at the same time, use one service process as the write gateway (recommended: JSON-RPC).

## 10. create_bucket / create_child_bucket

1. `create_bucket(parent_bucket_id=...)` support parse "ROOT" for root_bucket.
2. `create_child_bucket(...)` when not parse in `parent_bucket_id`, use active bucket by default.