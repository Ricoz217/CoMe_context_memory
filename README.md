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

## Notes
- Tool-call components are preserved in `LLM_connect.py`.
- Lightweight logger and YAML config are used instead of TIYA config/logger.
