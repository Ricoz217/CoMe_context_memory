# Memory API Notes

## Query Result Source

`QueryResult` now includes `result_source` with fixed enum values:

- `LLM`: LLM participated and returned enough matches up to requested `top_k`.
- `MIX`: LLM participated but returned insufficient matches; local rerank supplemented.
- `LOCAL`: LLM degraded/failed and local path produced the final result.

This is a top-level summary signal and does not replace per-match `source` (`llm`, `bm25`, `recursive`).

## Optimize API

Manual structure optimization entrypoints:

- `ContextMemoryEngineV3.optimize(bucket_id: str | None = None, *, reason: str = "manual_optimize")`
- `BucketHandle.optimize(*, reason: str = "manual_optimize")`

Behavior:

- Manual-only, best-effort.
- Reorders bucket structure without rewriting memory content.
- Returns `OptimizeResult` with `reason_code`, coverage, created buckets, moved items, sealed redirects, and post actions.
- Model can explicitly skip optimization (`reason_code=skip_by_model`) when structure is already reasonable.
- Leaf retention guard:
  - Any missing leaf bucket causes failure.
  - Leaf-node loss ratio above threshold causes failure.
  - Threshold is configurable via `ContextMemoryConfig.optimize_leaf_loss_threshold` (default `0.03`).
