# CoMe ContextMemory Project Introduction

This document describes the core design, capability boundaries, and practical usage recommendations for `CoMe_ContextMemory`.

---

## 1. Fundamentals

`ContextMemory` treats each context container as a "memory bucket" and maintains synchronized local storage, cache, and retrieval indexes.

Each LLM interaction can be abstracted into three parts:
1. `system_prompt`: stable chain-level instruction
2. `memory`: append-only memory event stream
3. `command`: task instruction and payload for the current round

The core principle is `Event Append-Only`:
1. historical events are never modified in place
2. new states are formed by appending new events
3. rebuild/compress pipelines generate a readable "latest view"

---

## 2. Compared with RAG

### 2.1 Advantages
1. Full context can be carried directly within a bucket, without strong dependency on chunk-only retrieval.
2. Retrieval works without a separate vector database.
3. Bucket structure and memory relations are explicit and controllable for manual intervention and debugging.

### 2.2 Limitations
1. Constrained by LLM context windows, deep subtree routing is inherently harder.
2. Tree-recursive query increases latency and call cost in deeper hierarchies.
3. Compared with classic "one retrieval + one answer" RAG, latency variance is higher.

---

## 3. Ingestion and Structure Maintenance

### 3.1 Memory Ingestion
1. Supports `add_memory`, `add_memory_from_file`, and `add_memory_from_dir`.
2. `add_memory_from_file` supports text and image files. Images can be guided by `image_extract_hint`.
3. `pdf/docx` are not supported yet.

### 3.2 Automatic Maintenance
1. When context pressure exceeds thresholds, `compress/split_bucket` may be triggered.
2. You can manually call `optimize` for structural reordering.
3. Batch interfaces include automatic slicing/compression, split, and optimize behavior by default.

### 3.3 Depth Limit
1. Bucket depth is controlled by `max_bucket_depth`.
2. Auto child-bucket creation performs depth validation to prevent invalid nesting.

---

## 4. Query Main Flow

A query is composed of "local retrieval + LLM + local merge": two local compute stages plus one LLM call.

### 4.1 Flow Overview
1. Local routing enhancement: run subtree scan (BFS) under budget and compute local recall auxiliary scores.
2. LLM query: generate `answer + matches` using key hints and bucket context.
3. Local rerank: merge LLM scores and local scores into final ranking.
4. If bucket nodes are matched, recursively query child buckets and return results to top-level output.
5. In one line: BFS -> LLM -> Rerank, with automatic recursive chaining.

### 4.2 Local Retrieval Stack
1. Main: `BM25`
2. Enhancement: `char 3-gram` (Dice similarity)
3. Usage: subtree routing boost + candidate rerank correction

### 4.3 Query Modes
Public modes:
1. `auto`
2. `semantic`
3. `hybrid`

`auto` routing rule:
1. strong literal features (code symbols/paths/etc.) route to `hybrid`
2. natural-language queries route to `semantic`

---

## 5. Cost and Latency

### 5.1 Cost Sources
Main cost comes from LLM calls; the project applies `cache_hit` optimization.
Local stages use cache and batch processing to reduce repeated requests.
For cost and latency control, `DeepSeek V4-flash` via official API is strongly recommended.
It offers low cost, good latency, 1M context, and strong retrieval quality.
Other models and local LLMs are also supported via two standard API styles:

- OpenAI Completion API
- Anthropic API

`OpenAI Response API` is not supported.

### 5.2 Cost Details
Cost is affected by different LLM chains and bucket cold/hot starts:
1. Different LLM chains have different `system_prompt`, so cache hits are not shared.
2. First request per bucket per chain (or provider cache expiry) is a cold start.
3. Repeated calls on the same chain before split/compress are hot-start behavior.
4. Hot-start cost is very low because `system_prompt` and prior memory can hit cache.
5. Practical hot-start cost is mostly new memory and current command payload.

> In short: before bucket rebuild (compress/split), querying at a proper frequency is extremely cheap.

### 5.3 Latency Sources
1. Ingestion path: chunking, cleaning, ingest calls
2. Query path: recursion depth, subtree scan budget, cache hit rate

### 5.4 Latency Details
- Ingestion can be slow, especially file/dir ingestion, because:
  1. single-writer model, and same-bucket writes are serialized
  2. directory ingestion recursively calls file ingestion
  3. file ingestion usually chunks first, then runs batch ingest tasks
  4. each memory ingest needs LLM `ingest` for relation generation
  5. batch ingestion has concurrency optimization but still incurs latency
  6. LLM calls dominate ingest latency
- Query latency is mainly determined by subtree depth and cold/hot start:
  1. query recursively queries child buckets
  2. hot-start cache significantly speeds LLM calls
  3. query latency is cumulative time of all LLM calls
  4. child-bucket recursive queries are concurrent
  5. query supports concurrency; bucket locks are write locks only

### 5.5 Practical Recommendations
1. Organize child buckets by module/function.
2. Prefer querying target sub-buckets to avoid blind full-tree scans.

---

## 6. I/O and Concurrency Constraints

### 6.1 Runtime Model
1. Designed as a single-writer-safe model.
2. The same `BASE_DIR` must not be written by multiple processes/instances at the same time.
3. There is no Redis lock, distributed lock, or process-level write coordination.
4. Multi-writer sharing is not blocked by code, but strongly discouraged.
5. Use `get_context_memory_engine` for singleton access, or instantiate multiple independent engines with different `BASE_DIR`.

### 6.2 Lock Semantics

Write operations:
`add/update/move/split/compress/create_bucket/optimize` and any operation that changes bucket content or creates buckets.

Read operations:
`query/list/get` and other non-mutating operations.

1. Same-bucket writes are serialized (not strict FIFO).
2. Cross-bucket writes can run concurrently (locks only involved buckets).
3. Read operations are concurrent (`query/list/get`).
4. Batch ingestion (`add_memory_from_file/add_memory_from_dir`) treats chunks from one file as one batch and commits after processing.

### 6.3 Risks
If multiple writers share one `BASE_DIR`, risks include:
1. inconsistent indexes
2. cache mapping corruption
3. storage corruption

---

## 7. Interface Forms

The project provides three usage forms:
1. Python API (recommended): import as a Python package with object-centric operations.
2. CLI: standalone process wrapping Python APIs.
3. JSON-RPC 2.0 (local service): standalone process for non-Python integration.

RPC has no built-in auth by default. If exposed externally, add auth and rate limits at gateway level.

---

## 8. Current Boundaries and TODO

Current boundaries:
1. no GUI
2. no distributed lock / multi-writer coordination
3. local filesystem storage only (no DB backend yet)
4. no automated storage cleanup scheduler (manual `gc_storage` provided)
5. memory and bucket revisions exist, but storage-level version governance is still limited
6. no time relation, only partial llm pipeline will get memory create time

Future directions:
1. web visualization UI
2. LLM Wiki-style upgrade
3. incremental storage and history management optimization
4. more complete automated cleanup and operations support
5. more comprehensive memory management: explicit weights, expiration, locking