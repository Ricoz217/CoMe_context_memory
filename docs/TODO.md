# TODO (Unified)

## P0 施工主线

- [x] 锁模型改造：不同桶并发写、同桶串行
  - 目标语义：
    - 同一 `bucket_id` 的写操作严格串行。
    - 不同 `bucket_id` 的写操作可并发执行。
    - 跨桶操作（如 `move/optimize/split`）按稳定顺序获取多把桶锁，避免死锁。
  - 现状问题：
    - 当前大量写路径使用全局 `self._lock`，导致跨桶完全串行。
  - 新锁结构（建议）：
    - `BucketLockManager`：`bucket_id -> asyncio.Lock`（惰性创建）。
    - `GlobalMetaLock`：仅用于全局元数据/全局结构（如 root/active 指针、bucket_mapping、全局树维护）。
    - `QuerySideEffectLock`：将 query 异步副作用与主写锁解耦，避免无关阻塞。
  - 锁获取规则（必须落文档并在代码统一复用）：
    - 单桶写：仅获取该桶锁。
    - 双桶/多桶写：按 `sorted(bucket_ids)` 顺序获取。
    - 需要同时改全局结构时：先拿桶锁，再拿 `GlobalMetaLock`；释放反序执行。
    - 禁止“先全局锁再桶锁”的反向路径（防止循环等待）。
  - 写路径分级改造（分阶段）：
    - Phase A（低风险）：`add_memory / update_memory / set_gray / delete_memory` 改为单桶锁。
    - Phase B（中风险）：`move_item` 改为双桶锁（source+target）+ 必要元数据锁。
    - Phase C（高风险）：`split_bucket / optimize / force_compress` 改为“源桶锁 + 新桶锁 + 元数据锁”组合。
    - Phase D（全局维护）：`gc_storage / cleanup_expired / migrate_paths` 保持全局串行（单独维护锁），不与业务写锁混用。
  - 关键风险点：
    - 死锁风险：多桶获取顺序不一致。
    - 竞态风险：`_resolve_bucket_id` 与 sealed redirect 链更新并发。
    - 原子性风险：跨桶事件写入与桶树更新顺序不一致导致中间态可见。
    - 兼容风险：alias session / alias map version 在并发下的一致性。
  - 验收与压测：
    - 并发压测：N 个协程分别写不同桶，吞吐显著高于全局锁版本。
    - 串行保证：同桶并发写无乱序/重复 revision。
    - 死锁测试：高并发 `move(A->B)` 与 `move(B->A)` 不死锁。
    - 一致性测试：事件流、bucket_tree、state、alias 审计可自洽。

- [x] 使最多三层桶树可配置
  - 新增配置项：`max_bucket_depth`（YAML + `ContextMemoryConfig`）。
  - 替换引擎中硬编码 `3` 的层级判断与提示。
  - 保持 `optimize` payload 仍固定两层（不改 prompt/payload 形状）。
  - 回归：`create/move/split/optimize/query` 在不同深度配置下行为正确。

- [x] 增加 `add_memory_from_dir`（`add_dir`）
  - 递归扫描目录，逐文件调用现有入库链路（不改核心 ingest 逻辑）。
  - 参数：目标桶、是否按子目录自动建子桶、是否查重、是否统计 token。
  - 非自动建子桶模式：全部入目标桶，末尾仅对目标桶执行一次 `optimize`。
  - 自动建子桶模式：不触发 `optimize`。
  - 若检测到自动建子桶会超出最大层级：拒绝整个任务。
  - 输出批处理进度与最终统计：`success_count / fail_count / skip_duplicate_count`。

- [x] 增加已有记忆检测（去重）
  - 开关策略：
    - `add_memory()` 默认不查重。
    - `add_memory_from_file()` 默认查重（可关闭）。
    - `add_memory_from_dir()` 默认查重（可关闭）。
  - 范围：仅查目标桶直属 `memory`，不查 `bucket`，不递归子桶。
  - 时机：`split_text` 后、`clean/ingest` 前；按原始分片文本比对。
  - 命中处理：
    - `add_memory_from_file`：直接 `skip`。
    - `add_memory_from_dir`：记录 `duplicate_in_bucket` 并继续后续文件。
  - 明确不做 `merge_relations`。

- [x] 压缩事件数据量
  - 仅压缩 `GRAY_SET` 事件。
  - `GRAY_SET` 事件默认不保留完整 `content/title/summary/relations`，仅保留最小审计字段。
  - 对以下事件保持完整信息（含桶事件）：
    - `ADD`
    - `UPDATE`
    - `MOVE_IN`
    - `OPTIMIZE_REBUILD`
    - `COMPRESS_REBUILD`
  - 回归检查：查询召回与审计可追溯能力不退化。

- [x] 提升深层桶召回率和优化路由策略
  - 保持现有递归 query 主流程不变。
  - 增加“全局召回”辅助分（优先 BM25），注入现有 rerank 作为 bucket/subtree boost。
  - 模式策略：`mode=auto|semantic|literal|hybrid`，优先本地规则，不引入额外 LLM 判定。
  - 缓存策略：引入 `global_index_version` 以支持增量索引和自动失效。
  - 约束：增加预算阈值，避免全局召回导致延迟失控与噪声路由。

## P1 依赖与顺序

- [x] 先完成 `max_bucket_depth` 配置化，再实施 `add_memory_from_dir`。
- [x] `add_memory_from_dir` 上线前，先落地去重能力与批处理统计结构。
- [x] 深层召回增强放在 V3 稳定后单独分支施工，避免影响当前主链路。

## P2 Prompt/Schema Simplification

- [ ] compress prompt/schema remove `keep_keys`, keep only `drop_keys`.
  - Rationale: for LLM output, long `keep_keys` lists are high-risk (miss/oversight), increase output tokens, and can cause misunderstanding of downstream keep/drop merge semantics.
  - Target behavior after refactor: default keep-all, apply `drop_keys` only; preserve existing safety validation in post-processing.
- [ ] add_memory_from_file: when file ingestion triggers bucket split, run one optimize after the whole file shards are ingested.
  - Keep same principle for add_memory_from_dir without adding separate branch logic.
  - add_memory_from_dir remains automation wrapper; behavior should derive from add_memory_from_file.
- [ ] add API to switch active bucket explicitly (set_active_bucket / switch_bucket).
  - ensure default routing rule is explicit: when `bucket_id` is not provided, operations resolve to `active_bucket_id`.
  - include docs note for default bucket resolution behavior.
- [ ] add_memory_from_file / add_memory_from_dir should return newly added memory keys for user-side manual rollback.
  - add_memory_from_file: return `added_keys` for this file ingest batch.
  - add_memory_from_dir: aggregate and return `added_keys` (and optionally `per_file_added_keys`).
  - dedup-skipped chunks/files must not appear in added key lists.
  - users can call delete on returned keys; no built-in rollback API.
- [ ] runtime persistence instance-scoped: bind `LLM_connect` image mapping and `LLM_usage` usage store to each engine `BASE_DIR`.
  - Remove hard dependency on module-level global files under package `DATA_DIR` for library mode.
  - Inject instance stores along `Engine -> LLMPipeline -> Chat` path (fallback to globals only for backward compatibility).
  - Ensure multi-engine same-process isolation when engines use different `BASE_DIR`.
