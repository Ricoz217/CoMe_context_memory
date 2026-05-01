# Baseline Report Template

## Metadata
- Date:
- Runner:
- Engine Module (`TIYA.memory.engine` / `come_context_memory.memory.engine`):
- Use Mock LLM:

## Smoke Pipeline
- Flow: `add_memory_from_file -> list -> optimize -> list` (multi-round)
- Rounds:

## Metrics (each round)
- `reason_code`
- `success`
- `coverage_ratio`
- `memory_count`
- `bucket_count`
- `total_memory_count`
- `moved_items`

## Validation Focus
- Optimize only reorders/restructures; no unexpected memory growth/loss
- No `duplicate_leaf_keys` unless LLM plan truly duplicates leaves
- No false-positive `leaf_bucket_missing`

## Query Concurrency
- Concurrency level:
- Degraded count:
- Avg matches per query:

## Conclusion
- Baseline stable: Yes/No
- Delta vs previous stage:
- Next phase go/no-go:
