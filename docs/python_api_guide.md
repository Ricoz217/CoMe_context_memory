# Python API 使用指南

本文档面向直接以 Python 库方式使用 `CoMe_ContextMemory` 的场景。

## 1. 最小示例

```python
import asyncio
from context_memory import ContextMemoryConfig, ContextMemoryEngineV3


async def main():
    cfg = ContextMemoryConfig(
        base_dir="data/my_memory",
        llm_preset="CONTEXT_MEMORY",
        image_llm_preset="KIMI2.6",
        use_mock_llm=False,
    )
    engine = ContextMemoryEngineV3(config=cfg)

    root = await engine.set_bucket("Demo")
    await root.add_memory("文件缓存模块包含 add_file / get_file_path / remove_fire")
    result = await root.query("如何写入缓存", top_k=3, mode="auto")
    print(result.answer)


asyncio.run(main())
```

## 2. 核心对象

1. `ContextMemoryConfig`
   - 引擎配置对象（深度、窗口、自动管理、query 默认模式等）。

2. `ContextMemoryEngineV3`
   - 主引擎对象，提供所有能力。

3. `BucketHandle`
   - 桶句柄，支持以桶为中心调用接口（`add/query/list/optimize/...`）。

## 3. 常用接口（Engine）

1. 入库与修改
   - `add_memory(raw_text, ...)`
   - `add_memory_from_file(file_path, ...)`
   - `add_memory_from_dir(dir_path, ...)`
   - `update_memory(key, patch_text, ...)`
   - `set_gray(key, gray=True/False, ...)`
   - `delete_memory(key_or_obj, ...)`

2. 查询与读取
   - `query(query_text, top_k=5, mode="auto", ...)`
   - `list_memories(include_gray=False, ...)`：返回 `ListMemoriesResult` 索引快照，不读取记忆正文
   - `get_bucket_context_usage(...)`：返回 `BucketContextUsage`，按真实 `context.json` prompts 统计 token
   - `get_memory(key, with_evidence=False, revision=None)`
   - `get_evidence_content(key, revision=None)`
   - `export_memory_to_markdown(memory_id)`

3. 桶操作
   - `set_bucket(title, ...)`
   - `set_active_bucket(bucket_id)` / `switch_active_bucket(bucket_id)`
   - `create_bucket(parent_bucket_id, ...)`
   - `create_child_bucket(parent_bucket_id=None, ...)`
   - `split_bucket(bucket_id, ...)`
   - `optimize(bucket_id=None, ...)`
   - `force_compress(bucket_id=None, ...)`
   - `move_item(key, target_bucket_id, ...)`

4. 运维与统计
   - `stats()`
   - `cleanup_expired()`
   - `gc_storage(dry_run=True, ...)`
   - `migrate_storage_paths_to_relative()`

## 4. Query 模式说明

公开模式仅支持：
1. `auto`
2. `semantic`
3. `hybrid`

规则：
1. `auto` 会自动分流：字面特征强走 `hybrid`，普通自然语言走 `semantic`。

## 5. 批量入库返回值

1. `add_memory_from_file(...)` 返回 `AddResult`
   - `added_keys`: 本次新增记忆 key 列表
   - `split_performed`: 是否发生切分
   - `split_rebuild_detected`: 是否检测到分桶/重建过程

2. `add_memory_from_dir(...)` 返回 `dict`
   - `success_count` / `fail_count` / `skip_duplicate_count`
   - `added_keys`（聚合）
   - `per_file_added_keys`（按文件）

说明：
1. 可用 `added_keys` 做手动回滚（调用 `delete_memory`）。
2. 去重跳过和失败项不会进入 `added_keys`。

## 6. 桶路由与 active bucket

**强烈建议使用对象操作，对象操作会自动传递 `bucket_id` ，可以无视下述说明**  

1. 不传 `bucket_id` 时，默认使用当前 `active_bucket_id`。
2. 建议在会话开始时显式调用 `set_active_bucket(...)`。
3. `latest_bucket_id(...)` 可用于追溯优化后新桶 id。

## 7. 文件入库说明

1. `add_memory_from_file` 当前支持：
   - 文本文件（含代码）
   - 图片文件（走图像抽取链路）

2. 暂不支持：
   - `pdf`
   - `docx`

3. 提示词参数：
   - 推荐使用 `image_extract_hint`，仅影响图片解析

## 8. 资源回收

当进程结束或不再使用引擎时，建议关闭：

```python
await engine.close()
# 或
engine.shutdown(wait=False)
```

这样会释放 query CPU 线程池等内部资源。

## 多接口并用约束

1. 同一个记忆库（同一 `BASE_DIR`）只能有一个写入进程。
2. 若 Python/CLI/JSON-RPC 作为不同进程同时写入同一 `BASE_DIR`，会有多写者风险。
3. 若需要多种接口同时使用，建议统一通过一个服务进程作为写入入口（推荐 JSON-RPC）。

## create_bucket / create_child_bucket 语义补充

1. `create_bucket(parent_bucket_id=...)` 支持传入 `ROOT` 作为根桶快捷写法。
2. `create_child_bucket(...)` 在未传 `parent_bucket_id` 时，默认使用当前 active bucket。

## Query 参数语义补充（Agent 视角）

这部分用于澄清一个常见误解：`mode` 不是遍历算法开关，`mode` 只影响打分融合；遍历始终包含 BFS 召回阶段。

### 1. 两层机制（必须区分）

1. 遍历层（Traversal）
   - 全局召回阶段会按预算做 BFS 扫桶。
   - 相关参数：`global_recall_top_n`、`global_recall_depth_limit`、`global_recall_time_budget_ms`、`max_depth`、`branch_expand_k`。

2. 打分层（Scoring）
   - `mode` 只控制桶内候选打分融合策略，不控制是否 BFS。
   - `semantic`：更偏向词项匹配（适合代码符号、路径、术语）。
   - `hybrid`：提高模糊语义权重（适合自然语言、描述式问题）。
   - `auto`：按查询文本特征在 `semantic/hybrid` 间自动分流。

### 2. 参数如何影响 Agent 自由度

1. 想“少漏召回、允许探索更多方向”
   - 增大：`global_recall_top_n`、`global_recall_depth_limit`、`global_recall_time_budget_ms`、`branch_expand_k`。
   - 代价：延迟与噪声上升。

2. 想“更快、更聚焦、可控”
   - 减小：`global_recall_top_n`、`global_recall_depth_limit`、`branch_expand_k`，并设置较小 `max_depth`。
   - 代价：可能漏掉远层相关信息。

3. 想“精确查代码事实”
   - 优先：`mode="semantic"`，并适度降低 `branch_expand_k`。

4. 想“容忍表述变化/模糊问题”
   - 优先：`mode="hybrid"`，并适度提高 BFS 预算参数。

### 3. 推荐的多轮查询策略（给 Agent）

1. 第一轮：广召回
   - `mode="auto"`，中高 `global_recall_top_n`，中等 `branch_expand_k`。

2. 第二轮：定向收敛
   - 根据第一轮证据 key，降低 BFS 预算并收紧 `max_depth`。

3. 第三轮：精确验证
   - `mode="semantic"`，小 `top_k`，对关键 bucket 或 key 做复查。
## 11. Schema Migration（简要）

发布版已内置 schema 迁移系统（当前 `__data_version__ = 3`），用于在启动阶段自动将旧数据升级到当前代码可用版本。

1. 自动触发时机
   - `ContextMemoryEngineV3` 完成存储绑定后会先执行迁移检查。
   - 若数据版本低于代码版本，会先迁移再继续对外提供能力。

2. 版本规则
   - 缺失 `index/schema_version.json` 时按历史初版（v1）处理。
   - 仅允许旧 -> 新。
   - 若检测到 `data_version > code_version`，会直接拒绝运行并提示升级代码。

3. 关键运行文件（位于 `BASE_DIR/index/`）
   - `schema_version.json`：当前数据 schema 版本。
   - `migration_journal.json`：迁移过程与失败信息。
   - `migration.lock`：迁移互斥锁。
   - `migration_tmp/`：迁移工作区与中间断点。
   - `migration_backups/pre_upgrade_latest/`：唯一长期保留的升级前备份。

4. 对外 API（Python）
   - `await engine.migration_status()`：查询迁移状态、版本差异、迁移计划、锁与路径信息。
   - `await engine.migrate_schema(dry_run=True)`：仅预览，不执行。
   - `await engine.migrate_schema(dry_run=False)`：执行真实迁移。
   - `BucketHandle` 也提供同名透传方法。

## 12. advance_query（详细）

`advance_query` 是与常规 `query` 完全分离的“全景查询”接口，面向整桶/子树总结、全量审阅、定制提示词任务。

1. 接口定位
   - 不走 BFS/Rerank 的 `query` 逻辑。
   - 临时构造请求 payload，不落持久化。
   - 返回最终 LLM 原始响应对象（当前链路通常为 `Prompts`）。

2. 入口与签名（Engine）

```python
await engine.advance_query(
    command="",
    system_prompt=None,
    mode="best_effort_full_view",  # 或 "single_shot"
    bucket_id=None,
    max_expand_depth=None,
    include_gray=False,
    llm_preset=None,
    tool_input=None,
    enable_aliasing=True,
    audit=False,
    max_parallel_chunks=None,
)
```

3. 参数说明（关键）
   - `mode`
     - `single_shot`：只允许单次请求；若超窗直接报错。
     - `best_effort_full_view`：超窗时自动分片并最终汇总。
   - `max_expand_depth`：限制子树展开深度；`None` 表示不限。
   - `include_gray`：是否把灰化记忆纳入全景内容。
   - `tool_input`：仅最终顶层请求可携带 tool；chunk 阶段强制禁用 tool。
   - `audit`：是否写入 `ADVANCE_QUERY_*` 事件。
   - `max_parallel_chunks`：分片并发上限；为空时使用 `split_ingest_parallelism`。

4. Payload 结构（重要）
   - 采用 Markdown 外壳 + JSON（`indent=2`）记忆体。
   - 顺序固定为：先记忆库，再指令（用于提升 KV cache 稳定命中）。

```markdown
# System Prompt

<system_prompt>

---

# 记忆库

<RESTRUCTURE_MEMORY_JSON>

---

# 指令

<command>
```

5. RESTRUCTURE_MEMORY 组织规则
   - 顶层从 `bucket_id`（或 active bucket）开始，按子树展开。
   - 每个桶的 `content` 内：
     - 记忆单片在前，按 key 字典序。
     - 子桶在后，按 `(last_event_at 升序, bucket_id 升序)`；越活跃的桶越靠后。
   - 默认只保留必要 metadata 字段，减少 token 占用。

6. 超窗与分片规则
   - 真实 token 估算：对“最终完整 Markdown 字符串”做 tiktoken 计数。
   - 阈值固定：`0.8 * max_context_window`。
   - `best_effort_full_view` 在超窗时使用稳定 first-fit 分片（非 FFD）。
   - 先叶子 chunk 并发执行，再按依赖向上汇总；必要时进入 `result_chunk` 二次分片。
   - 每个子请求失败自动重试 1 次；仍失败会保留缺失占位并继续汇总，尽量保证最终有结果。

7. Aliasing 规则
   - `enable_aliasing=True` 时，子树统一使用目标桶（顶层桶）的 alias map。
   - 不回写子桶 alias map，避免多映射源混乱。
   - `await bucket.resolve_alias(alias)` 可使用当前句柄桶的 alias map 手动取得真实 ID。
   - `await bucket.resolve_aliases(aliases)` 可在一次 successor 刷新和一次映射锁内批量反解，返回 `alias -> real_id` 的成功映射。
   - `memory_xx` 返回真实 `mem_...`；`bucket_xx` 返回真实 `bucket_...`；`revision_xx` 与 `ref_xx` 同理。
   - 桶节点本身也是一条 memory 记录：其 `memory_xx` 可交给 `get_memory()`，记录中的 `child_bucket_id` 指向桶实体。

```python
response = await bucket.advance_query(command="找出相关记忆")

real_mem_id = await bucket.resolve_alias("memory_12")
memory_or_bucket_node = await bucket.get_memory(real_mem_id)

real_bucket_id = await bucket.resolve_alias("bucket_3")
child_bucket = bucket.get_bucket(real_bucket_id)

resolved = await bucket.resolve_aliases(["memory_12", "bucket_3", "memory_999"])
# 未知 alias 默认跳过：{"memory_12": "mem_...", "bucket_3": "bucket_..."}
```

必须使用生成该 alias 的目标桶句柄反解；alias map 按桶隔离。需要严格限制类型时可传 `expected_type="memory"` 等参数。批量接口默认跳过未知 alias 和类型不匹配项；传 `strict=True` 时恢复单条接口的 fail-fast `KeyError`/`TypeError` 行为。映射损坏不会被跳过。

8. BucketHandle 透传
   - `await bucket.advance_query(...)` 与 engine 行为一致。
   - `BucketHandle` 场景下默认作用于当前句柄桶，不需要额外传 `bucket_id`。

## 13. 父桶级 Title Mapping（Schema v3）

- `set_bucket(title)` 只在调用父桶自己的 title 映射表中查找；不同父桶可安全使用相同 title。
- `BucketHandle.set_bucket(title)` 始终返回该句柄当前 canonical 桶的直接子桶。
- `create_bucket()` / `create_child_bucket()` 仍表示明确新建，不登记 setdefault title 映射，因此同一父桶下仍允许显式创建同名桶。
- 删除父桶后再次按同名创建，会生成新的父桶及子树，不会从旧父桶的历史映射中恢复成员子桶。
- schema v3 会把旧 `bucket_mapping.json` 迁移到 `bucket_tree.json.child_title_maps`；迁移成功后 live 旧文件删除，升级前备份保留。
- `alias_map.json` 格式不变。合法映射不重编号；仅允许补齐安全元数据，映射冲突、损坏或拓扑异常会中止并回滚迁移。
