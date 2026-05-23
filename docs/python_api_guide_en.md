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
## 11. Schema Migration (Brief)

Release build now includes a built-in schema migration system (current `__data_version__ = 2`) to upgrade old stores before runtime operations.

1. Auto trigger point
   - After storage binding, `ContextMemoryEngineV3` runs migration checks before exposing normal operations.
   - If store schema is older than code schema, migration runs first.

2. Version rules
   - Missing `index/schema_version.json` is treated as legacy v1.
   - Upgrade direction is forward-only (old -> new).
   - If `data_version > code_version`, engine refuses to run and asks for newer code.

3. Runtime files (under `BASE_DIR/index/`)
   - `schema_version.json`: current data schema version.
   - `migration_journal.json`: migration progress and failure details.
   - `migration.lock`: migration mutex lock.
   - `migration_tmp/`: migration workspace and checkpoints.
   - `migration_backups/pre_upgrade_latest/`: the only long-term retained pre-upgrade backup.

4. Python APIs
   - `await engine.migration_status()`: inspect schema gap, planned steps, lock state, journal, and paths.
   - `await engine.migrate_schema(dry_run=True)`: preview only.
   - `await engine.migrate_schema(dry_run=False)`: execute migration.
   - `BucketHandle` provides the same passthrough methods.

## 12. advance_query (Detailed)

`advance_query` is a panoramic query interface fully separated from regular `query`, designed for full-bucket/subtree summarization and custom prompt-driven tasks.

1. Positioning
   - Does not use the regular `query` BFS/rerank chain.
   - Builds a temporary payload per request (no persistence).
   - Returns the final raw LLM response object (typically `Prompts` in current pipeline).

2. Engine signature

```python
await engine.advance_query(
    command="",
    system_prompt=None,
    mode="best_effort_full_view",  # or "single_shot"
    bucket_id=None,
    max_expand_depth=None,
    include_gray=False,
    llm_preset=None,
    tool_input=None,
    enable_aliasing=True,
    audit=False,
    max_parallel_chunks=None,
)
```

3. Key parameters
   - `mode`
     - `single_shot`: one request only; overflow raises an error.
     - `best_effort_full_view`: overflow triggers automatic chunking and final aggregation.
   - `max_expand_depth`: subtree expansion depth limit; `None` means unlimited.
   - `include_gray`: include gray memories or not.
   - `tool_input`: tools are allowed only on the final top-level request; all chunk passes disable tools.
   - `audit`: whether to emit `ADVANCE_QUERY_*` events.
   - `max_parallel_chunks`: chunk concurrency cap; defaults to `split_ingest_parallelism`.

4. Payload layout (important)
   - Markdown shell + JSON (`indent=2`) memory payload.
   - Fixed order: memory first, command last (for stable KV-cache behavior).

```markdown
# System Prompt

<system_prompt>

---

# 记忆库

<RESTRUCTURE_MEMORY_JSON>

---

# 指令

<command>
```

5. RESTRUCTURE_MEMORY ordering
   - Starts from target `bucket_id` (or active bucket) and expands subtree.
   - Inside each bucket `content`:
     - memories first, sorted by memory key.
     - child buckets after memories, sorted by `(last_event_at ASC, bucket_id ASC)`.
   - Only necessary metadata fields are included to reduce token overhead.

6. Overflow and chunking
   - Exact token estimation uses tiktoken on the final full markdown string.
   - Threshold is fixed at `0.8 * max_context_window`.
   - `best_effort_full_view` uses stable first-fit chunking (not FFD).
   - Leaf chunks run in parallel; parent chunks wait for dependencies; result aggregation may enter `result_chunk` splitting if needed.
   - Each subrequest retries once; if still failing, a missing placeholder is kept and aggregation continues.

7. Aliasing
   - With `enable_aliasing=True`, the expanded subtree uses the top target bucket alias map.
   - Child bucket alias maps are not written back.

8. BucketHandle passthrough
   - `await bucket.advance_query(...)` matches engine behavior.
   - On `BucketHandle`, target bucket defaults to the handle bucket; no manual `bucket_id` is required in normal use.
