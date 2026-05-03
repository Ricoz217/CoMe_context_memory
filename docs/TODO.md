# TODO

## add_memory_from_file / add_memory_from_dir 去重策略（设计项）

1. 增加去重开关：
   - `add_memory()` 默认不查重。
   - `add_memory_from_file()` 增加参数 `dedup_in_bucket: bool = True`（默认开启）。
   - `add_memory_from_dir()` 复用同一参数，默认开启（主要防止重复导入同一文件/目录）。

2. 去重范围限定：
   - 仅查目标桶直属 `memory` 记录。
   - 不查 `bucket` 记录。
   - 不递归子桶。

3. 去重时机：
   - 在 `split_text` 之后、`clean/ingest` 之前执行。
   - 对“原始分片文本”计算哈希进行比对（不使用 clean 后文本）。

4. 命中处理策略：
   - `add_memory_from_file`：命中重复直接 `skip`。
   - `add_memory_from_dir`：命中重复记录批处理错误/跳过信息（`duplicate_in_bucket`），继续处理后续文件。
   - 明确不做 `merge_relations`。

5. 批处理统计输出（目录导入）：
   - 输出 `success_count / fail_count / skip_duplicate_count`。
   - 输出失败或跳过明细（至少含文件路径与原因）。

6. 约束补充（与已确认规则一致）：
   - 非自动建子桶模式下，`optimize` 只对目标桶执行一次，不对子桶执行。
   - 若 `success_count == 0`，不触发 `optimize`。
   - `add_memory_from_dir` 实施排期放在“可配置层级上线”之后。

## Query 深层召回缺陷与改进（设计项，暂不施工）

### 背景缺陷

1. 当前 query 的路由依赖“父层命中桶节点 -> 递归子桶”。
2. 若目标代码片段位于深层子桶，且上层桶摘要未覆盖该信息，路由可能失败，LLM 可视范围受限。
3. 这类场景相较传统 RAG（向量库直接全局召回）存在召回风险。

### 已确认方向

1. 引入“全局召回”作为前置/辅助信号（优先 BM25 倒排，后续可扩展向量）。
2. 不替换现有递归 query 主流程；以低侵入方式增强路由。
3. 将全局召回结果聚合为子树/桶级 boost 分，注入现有 rerank，提升正确子桶触发概率。
4. 递归策略保持现有框架，优先做“候选子树递归”，避免全树递归带来的延迟和噪声。

### 模式策略（代码片段查询）

1. 不使用额外 LLM 判断检索模式（避免增加 query 延迟）。
2. 模式采用“本地规则自动判断 + 用户显式覆盖”：
   - `mode=auto|semantic|literal|hybrid`
3. `auto` 基于规则（符号密度、多行代码、路径形态等）选择；低置信度回退 `hybrid`。

### 缓存与内存策略

1. 全局索引采用增量维护（基于事件流更新），避免每次全量重建。
2. 查询缓存 key 纳入 `global_index_version`，版本变更自动失效。
3. 内存回收分层：
   - 先淘汰查询结果缓存
   - 再按 LRU 淘汰冷索引分片
   - 保留磁盘索引快照以便快速恢复

### 风险与边界

1. 全局召回会增加计算量，需设置预算：`topN_memory / topM_buckets / depth_limit / time_budget_ms`。
2. 需防止 boost 过强导致噪声路由；建议加入上限与归一化。
3. 该方案目标是“提升深层召回率”，不改变现有 API 语义与结果结构。
