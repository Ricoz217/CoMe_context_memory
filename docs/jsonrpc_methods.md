# CoMe ContextMemory JSON-RPC 2.0

Endpoint: `POST /jsonrpc`  
Health: `GET /healthz`

Server runtime options include `--no-forgetting` to disable negative-weight forgetting logic.

## Core Methods

- `ping`
- `stats`
- `list_buckets`
- `list_memories`
- `set_active_bucket`
- `latest_bucket_id`
- `add_memory`
- `add_memory_from_file`
- `add_memory_from_dir`
- `get_memory`
- `get_evidence_content`
- `export_memory_to_markdown`
- `update_memory`
- `set_gray`
- `delete_memory`
- `query`
- `force_compress`
- `cleanup_expired`
- `create_bucket`
- `create_child_bucket`
- `refresh_bucket_summary`
- `split_bucket`
- `optimize`
- `move_item`
- `gc_storage`
- `get_bucket_context_usage`
- `migrate_storage_paths_to_relative`

## Request Shape

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "query",
  "params": {
    "query_text": "how to write cache",
    "top_k": 5
  }
}
```

## Optional Per-call Timeout

Any method supports:

- `timeout_ms: number`

If exceeded, server returns:

- code `-32001` (method timeout)

## Error Codes

- `-32600`: invalid request
- `-32601`: method not found
- `-32602`: invalid params
- `-32001`: method timeout
- `-32010`: context overflow related runtime error
- `-32000`: generic internal error
