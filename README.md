# CoMe ContextMemory (V3 decoupled)

## Quick Start

1. Install deps
```powershell
pip install -r requirements.txt
```

2. Configure
- Edit `config/memory.yaml`.
- Or set env:
  - `COME_CONTEXT_MEMORY_ROOT`
  - `COME_CONTEXT_MEMORY_CONFIG`

3. Import engine
```python
from come_context_memory.memory import ContextMemoryConfig, ContextMemoryEngineV3
```

## Smoke Commands

### Baseline smoke
```powershell
$env:PYTHONPATH='D:\Python\CoMe_ContextMemory\src'
python tests\smoke_baseline.py --engine-module come_context_memory.memory.engine --optimize-rounds 2 --out docs\smoke_baseline_report.json
```

### Query concurrency smoke
```powershell
$env:PYTHONPATH='D:\Python\CoMe_ContextMemory\src'
python tests\query_concurrency_smoke.py --engine-module come_context_memory.memory.engine --concurrency 20 --out docs\query_concurrency_report.json
```

### Release Smoke (API + CLI + JSON-RPC)
```powershell
$env:PYTHONPATH='D:\Python\CoMe_ContextMemory\src'
python -m pytest tests\test_release_smoke_three_interfaces.py -q
```

Real-LLM mode:
```powershell
$env:PYTHONPATH='D:\Python\CoMe_ContextMemory\src'
$env:COME_RELEASE_SMOKE_REAL_LLM='1'
python -m pytest tests\test_release_smoke_three_interfaces.py -q
```

Standalone script smoke:
```powershell
$env:PYTHONPATH='D:\Python\CoMe_ContextMemory\src'
python scripts\cli_smoke.py
python scripts\rpc_smoke.py
```

Standalone script smoke (real LLM):
```powershell
$env:PYTHONPATH='D:\Python\CoMe_ContextMemory\src'
python scripts\cli_smoke.py --real-llm
python scripts\rpc_smoke.py --real-llm --port 19013
```

## Interfaces

### CLI
```powershell
$env:PYTHONPATH='D:\Python\CoMe_ContextMemory\src'
python -m come_context_memory.cli --base-dir D:\Python\CoMe_ContextMemory\data\cli_runtime
```

### JSON-RPC 2.0 Server
```powershell
$env:PYTHONPATH='D:\Python\CoMe_ContextMemory\src'
python -m come_context_memory.rpc_server --host 127.0.0.1 --port 9010 --base-dir D:\Python\CoMe_ContextMemory\data\rpc_runtime
```

Endpoints:
- `POST /jsonrpc`
- `GET /healthz`

Method list:
- [JSON-RPC Methods](D:/Python/CoMe_ContextMemory/docs/jsonrpc_methods.md)

## Notes
- Tool-call components are preserved in `LLM_connect.py`.
- Lightweight logger and YAML config are used instead of TIYA config/logger.

## API Notes
- `add_memory_from_file(...)` and `add_memory_from_dir(...)` now use `image_extract_hint` for image OCR/extraction guidance.
- `query_hint` is still accepted as a backward-compatible alias, but new code should use `image_extract_hint`.
- For text files, this hint is ignored; it only affects image ingestion (`detect_file_kind == "image"`).
- `add_memory_from_file(...)` returns `AddResult.added_keys`; split/shard ingest returns all newly created keys in this call.
- `add_memory_from_dir(...)` returns aggregated `added_keys` and `per_file_added_keys`.
- Use `set_active_bucket(bucket_id)` (alias: `switch_active_bucket`) to switch the active bucket explicitly.
- When an API call does not provide `bucket_id`, routing defaults to the current `active_bucket_id`.
- Forgetting switch:
  - `ContextMemoryConfig(enable_forgetting=False)` disables negative-weight forgetting and auto-forget graying.
  - CLI supports `--no-forgetting`.
  - JSON-RPC server supports `--no-forgetting`.
- Query mode:
  - Supported: `auto`, `semantic`, `hybrid`.
  - `auto`: literal-like query routes to `hybrid`; normal natural-language query routes to `semantic`.
  - `literal` has been removed from public mode options and now returns an invalid-parameter error.
