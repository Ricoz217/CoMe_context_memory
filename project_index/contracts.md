# contracts.md

## 1. 契约范围
本文件记录“不能随意改”的稳定接口与行为契约，供发布版开发与双仓同步使用。

## 2. API 契约（Python）

### 2.1 稳定入口
- `get_context_memory_engine(config=...)`
- `ContextMemoryEngineV3`
- `BucketHandle`

### 2.2 常用稳定能力（不应随意改名/改语义）
- 入库：`add_memory`, `add_memory_from_file`, `add_memory_from_dir`
- 查询：`query`
- 桶管理：`set_bucket`, `set_active_bucket`, `create_bucket`, `create_child_bucket`
- 结构治理：`optimize`, `split_bucket`, `force_compress`
- 维护：`stats`, `cleanup_expired`, `gc_storage`

### 2.3 参数语义契约（关键）
- `query.mode` 只允许：`auto|semantic|hybrid`。
- `query.branch_expand_k`：控制分支扩展宽度，传空时走引擎默认。
- `bucket_id` 为空时：默认作用于当前 active bucket。
- `add_file` / `add_dir` 的 `force_split` 与 `create_new_bucket` 必须保持既有语义。

## 3. API 契约（CLI）

### 3.1 稳定命令面
- 查询类：`query`, `list`, `get`
- 入库类：`add`, `add_file`, `add_dir`
- 治理类：`optimize`, `compress`, `split`
- 桶类：`create_bucket`, `create_child_bucket`, `switch_bucket`, `latest_bucket`

### 3.2 参数契约
- 关闭 `add_file` 自动强切分：`--no-force-split`。
- `query` 强制指定评分模式：`--mode auto|semantic|hybrid`。
- `--bucket` 用于覆盖 active bucket 路由。

## 4. API 契约（JSON-RPC）

### 4.1 稳定方法名（核心）
- `add_memory`, `add_memory_from_file`, `add_memory_from_dir`
- `query`, `list_memories`, `get_memory`
- `optimize`, `split_bucket`, `force_compress`
- `set_active_bucket`, `latest_bucket_id`, `stats`

### 4.2 返回结构契约
- JSON-RPC 2.0 包装固定：`{jsonrpc,id,result|error}`。
- 业务结果内的字段允许增量扩展，不应无通知删除核心字段。

## 5. 数据契约

### 5.1 记忆检索结果最小字段
- `key`
- `score`
- `summary`
- `source`
- （可选）`llm_score`, `bm25_score`, `final_score`

### 5.2 评分与来源契约
- `final_score` 区间：`[0, 1]`。
- `source` 必须反映真实路径：
  - 主链路：`llm`/`bm25`/`mix`（按实现）
  - 降级链路：`local_rerank`（已定）
- 不允许“执行路径是 fallback，但 source 仍标为 llm”。

## 6. Service 契约（内部稳定行为）
- `optimize/compress/split` 必须具备可用 fallback，不能因 LLM 失败让系统失去治理能力。
- `split` fallback 必须可产生结构变化，不允许“看似执行、结构不变”。
- 对 sealed/successor 的处理必须保证读写路径可追溯。

## 7. 隐式契约（高风险）
- ROOT 指针不是 active 指针，更新规则必须解耦。
- `mode` 不是 BFS 开关；BFS 召回由 recall 参数控制。
- 发布版同步必须保留对外兼容，不可把内部试验语义直接外放。

## 8. 变更流程契约
- 任何接口行为变化都必须同时更新：
  - 对应代码
  - 回归测试
  - 用户文档（python/cli/jsonrpc）
  - 本索引（contracts/work_history/TODO）