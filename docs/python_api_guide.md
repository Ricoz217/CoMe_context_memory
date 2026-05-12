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
   - `list_memories(include_gray=False, include_content=False, ...)`
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