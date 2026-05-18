# code_detail.md

> 记录代码实现细节、设计原因、风险点与排障经验。代码原文以仓库为准，这里只做“高价值导读”。

## 模块设计原因

1. 采用“桶树 + 事件上下文 + 治理工具（compress/split/optimize）”而非纯向量检索，核心目标是可解释、可追溯、可维护。
2. query 采用“LLM + 本地信号（BM25/ngram/rerank）”混合，保证在 LLM 弱可用时仍可降级。
3. 大文本场景是常态压力源，治理链路必须可 fallback，不能依赖“LLM 必定成功”。

## Dirty Hack

1. fallback 分数封顶（如 local_rerank 上限）属于工程折中，优先保证来源解释性和安全性。
2. 部分超窗场景下会优先“保可用”再“保最优”，后续需要在 TODO 中继续收敛策略。

## 风险点

1. ROOT/active 指针更新逻辑，任何条件合并都可能引发拓扑漂移。
2. split fallback 若只保留不拆分，会形成“执行成功但结构不变”的死桶风险。
3. query 来源标记与评分融合若不同步，会误导上层 Agent 决策。
4. 批量分片入库涉及 generation/恢复，边界处理不当会造成重复写入或任务悬挂。

## 重点代码

### 1) query：BFS、rerank、递归 query、结果替换、评分规则

代码锚点：
- `src/context_memory/memory/services/query_service.py`
- `src/context_memory/memory/rerank.py`

关键片段（BFS 全局召回，用于 boost）：
```python
queue: deque[tuple[str, int]] = deque([(root_bucket_id, 1)])
while queue and len(scanned) < max(1, int(top_n)):
    bucket_id, depth = queue.popleft()
    ...
    if rec.kind == BUCKET_KIND_BUCKET and rec.child_bucket_id:
        queue.append((child, depth + 1))
```

关键片段（本地 rerank 融合）：
```python
ranked = rank_records_with_index(query_text, list(records), top_k=..., index=bm25_index)
bm25_conf = [QueryService._abs_confidence_from_raw(score) for _, score in ranked]
ngram_conf = [_clamp_score(score) for score in ngram_raw]
fused = [_clamp_score(bm25_weight * bm25_conf[i] + ngram_weight * ngram_conf[i]) ...]
```

关键片段（递归替换父桶候选）：
```python
if rec.kind == BUCKET_KIND_BUCKET and rec.child_bucket_id and depth < depth_limit:
    bucket_candidates.append((match, child_bucket_id))
...
child = await self.query_bucket_recursive(bucket_id=child_bucket_id, top_k=branch_expand_k, ...)
merged_score = _clamp_score(_BRANCH_PARENT_WEIGHT * parent_match.score + _BRANCH_CHILD_WEIGHT * child_match.score)
```

关键片段（评分规则与来源）：
```python
if normalized_source == "local_rerank":
    llm_score = 0.0
    local_fused = _clamp_score(0.7 * bm_score + 0.3 * local_score)
    final = min(0.6, _clamp_score(local_fused))
else:
    llm_score = raw_item_score
    final = _clamp_score(0.85 * llm_score + 0.15 * bm_score)
```

说明：
- `mode` 是评分融合策略，不是 BFS 开关。
- 桶节点命中后会递归子桶并进行结果替换。
- fallback 路径必须标记真实 `source`。

### 2) compress / split_bucket 触发阈值控制

代码锚点：`src/context_memory/memory/engine.py::_auto_manage_bucket`

```python
pressure, count = self._bucket_pressure(bucket_id)
if pressure > self._auto_compress_trigger_ratio or count > 1000:
    await self._force_compress_unlocked(...)
...
if pressure > self._auto_split_trigger_ratio or count > 1000:
    result = await self._split_bucket_unlocked(bucket_id=bucket_id, reason="auto_post_compress")
```

说明：
- 自动治理以“压力比例 + 记录数”双阈值触发。
- 先压缩后拆桶，并有冷却/轮次保护。

### 3) 配置入口及影响范围

代码锚点：
- `src/context_memory/cli.py::_make_config`
- `src/context_memory/rpc_server.py::_make_config`
- `src/context_memory/memory/engine.py::__init__/apply_config`
- `src/context_memory/memory/llm_pipeline.py::_resolve_preset_name`

关键片段：
```python
if isinstance(config, ContextMemoryConfig):
    cfg_obj = config
elif isinstance(config, dict):
    cfg_obj = ContextMemoryConfig.from_dict(config)
...
self.pipeline.tool_presets = dict(normalized_tool_presets)
self.auto_manage = bool(cfg_obj.auto_manage)
self._query_branch_expand_k = max(1, int(cfg_obj.query_branch_expand_k))
```

说明：
- 配置影响范围很广：LLM preset、query 默认行为、自动治理阈值、内存上限、分片并发等。

### 4) batch_ingest 逻辑

代码锚点：
- `src/context_memory/memory/engine.py::add_memory_from_dir`
- `src/context_memory/memory/engine.py`（分片批处理主循环）
- `src/context_memory/memory/services/split_ingest_job_service.py`

关键片段：
```python
batch_id = f"batch_{...}_{uuid4().hex}"
self.storage.save_job_journal({... "generation": generation, "status": "running" ...})
...
out, overflow_seen, _ = await self._ingest_with_overflow_retry_detail(...)
if overflow_seen:
    pause_event.clear(); rebuild_event.set()
```

说明：
- add_dir 支持目录递归和子桶路由（`auto_create_sub_buckets=True`）。
- 分片入库支持并发、暂停重建、任务恢复（running/paused）。

### 5) optimize payload 构造逻辑

代码锚点：
- `src/context_memory/memory/services/optimize_service.py::_build_optimize_payload`
- `src/context_memory/memory/services/optimize_service.py::optimize`
- `src/context_memory/memory/llm_pipeline.py::optimize`

关键片段：
```python
payload = self._build_optimize_payload(...)
est_tokens = max(1, len(json.dumps(payload, ensure_ascii=False)) // 3)
if est_tokens > int(eng.max_context_window * 0.70):
    return OptimizeResult(success=False, reason_code="payload_over_70pct")
...
llm_alias = await eng.pipeline.optimize(bucket_context=None, reason=reason, payload=alias_payload)
```

```python
result = await self._ask_json(..., include_context=False, ...)
```

说明：
- optimize 使用“树结构 + 约束”payload，不直接喂全量原始上下文。
- 当前明确关闭 context 注入（`include_context=False`）。

### 6) 动态内存管理逻辑

代码锚点：
- `src/context_memory/memory/memory_manager.py`
- `src/context_memory/memory/engine.py::_run_memory_gc`
- `src/context_memory/memory/engine.py::_apply_forgetting`

关键片段：
```python
def cleanup(self, *, force_aggressive: bool = False):
    # idle eviction + pressure eviction
    ...
```

```python
evicted = self.memory_manager.cleanup()
if not evicted:
    self.bm25_cache.prune_to_limit(...)
elif self.memory_manager.aggressive_mode:
    self.bm25_cache.prune_to_limit(...)
```

说明：
- 运行时缓存与本地索引缓存联动回收。
- forgetting 通过负权重衰减 + gray 化实现“软删除治理”。

### 7) 本地记忆索引逻辑

代码锚点：`src/context_memory/memory/rerank.py`

关键片段：
```python
class BM25IndexCache:
    def get_or_build(self, *, bucket_id, bucket_version, records):
        key = f"{bucket_id}:{bucket_version}"
```

```python
score = index.bm25.score(query_terms, pos)
if query_lower in title_lower: score += 2.0
if query_lower in summary_lower: score += 1.2
if query_lower in content_lower: score += 1.0
score += max(0.0, min(1.0, float(rec.weight))) * 0.2
```

说明：
- 索引按 `bucket_id:bucket_version` 缓存，版本变化即重建。
- 本地评分由 BM25 + 命中加权 + memory weight 组成。

## Debug 经验

1. query 异常优先看：`source` 是否真实、`branch_expand_k` 是否透传、递归替换是否生效。
2. 治理异常优先看：`_bucket_pressure` 是否真实、冷却/轮次保护是否触发、fallback 是否真正拆分。
3. 批处理异常优先看：job journal（running/paused/completed）、generation 是否越界、恢复逻辑是否重复提交。
4. optimize 异常优先看：payload 体积、leaf 校验、include_context 是否误打开。