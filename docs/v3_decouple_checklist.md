# CoMe ContextMemory V3 Decouple Checklist

## Scope
- Decouple ContextMemory V3 runtime from TIYA runtime dependencies.
- Keep V3 behavior baseline for list/query/optimize/split/compress.
- Keep tool-call components in LLM_connect.

## Implemented
- Copied modules into `src/come_context_memory/`:
  - `memory/`
  - `LLM_connect.py`
  - `file_cache.py`
  - `utils.py`
  - `time_id.py`
  - `LLM_usage.py`
- Replaced runtime imports from `TIYA.*` to `come_context_memory.*`.
- Added lightweight config module: `come_context_memory/config.py`.
- Added lightweight logger module: `come_context_memory/logger.py`.
- Added default config file: `config/memory.yaml`.
- Added smoke baseline runner: `tests/smoke_baseline.py`.
- Added query concurrency smoke runner: `tests/query_concurrency_smoke.py`.
- Added baseline template: `docs/baseline_report_template.md`.

## Validation Results
- `python -m compileall src/come_context_memory` -> PASS.
- TIYA import scan in runtime package -> 0 matches.
- Smoke report generated:
  - `docs/smoke_baseline_report.json`
- Query concurrency report generated:
  - `docs/query_concurrency_report.json`

## Known Notes
- Smoke currently uses mock-LLM mode in test runner.
- Query smoke in mock mode returns degraded fallback by design.

## Next Suggested Step
- Run same smoke runner against old TIYA engine module for baseline comparison:
  - `--engine-module TIYA.memory.engine`

## TODO (Post-Decouple Stability)
- [ ] Make bucket max depth configurable (`max_bucket_depth`) via `ContextMemoryConfig` and YAML.
- [ ] Replace hardcoded depth limit (`3`) in engine checks/messages with configured value.
- [ ] Keep optimize payload fixed at 2 levels (no optimize prompt/payload shape change).
- [ ] Verify query default depth behavior remains `eng._max_depth + 2` after config migration.
- [ ] Add regression check: deep bucket create/move respects configured depth, optimize behavior unchanged.
