# map.md

## 1. 文件结构地图（发布版主视角）

### 1.1 发布版仓库结构（D:\Python\CoMe_ContextMemory）
- `src/context_memory/`
  - `memory/`：核心记忆引擎
  - `cli.py`：命令行入口
  - `rpc_server.py`：JSON-RPC 服务入口
  - `config.py`：配置解析
- `docs/`：对外文档（API/CLI/JSON-RPC/配置）
- `tests/`：发布版回归与烟测
- `project_index/`：本索引（接力开发知识库）
- `releases/`：发布相关资产

### 1.2 内部版仓库结构（D:\Python\TIYA_ThenIAskYou_2026）
- `src/TIYA/memory/`：memory 试验主阵地（与发布版同源核心）
- `src/TIYA/*.py`：内部业务模块（bot/dialog/任务调度等）
- `review_guides/`：阶段性评审、补丁记录、试验结论
- `tests/`：内部版混合测试（memory + 业务）

## 2. 模块关系（双仓）

### 2.1 同源核心模块（高度重叠）
- `memory/engine.py`
- `memory/storage.py`
- `memory/llm_pipeline.py`
- `memory/aliasing.py`
- `memory/rerank.py`
- `memory/services/*`

### 2.2 发布版保留模块（对外交付）
- `src/context_memory/cli.py`
- `src/context_memory/rpc_server.py`
- `docs/*`（对外说明）
- 打包与版本元数据（`pyproject.toml`, `README.md`）

### 2.3 内部版保留模块（试验与业务）
- `src/TIYA/` 下业务耦合模块（非发布版目标）
- 先行实验逻辑与临时验证脚本

## 3. 功能映射（按能力归类）

### 3.1 记忆写入与清洗
- 入口：`add_memory` / `add_memory_from_file` / `add_memory_from_dir`
- 关键链路：engine -> ingest_service -> llm_pipeline(clean/ingest) -> storage
- 风险：超窗、自动切分、fallback 语义一致性

### 3.2 查询与召回
- 入口：`query`
- 关键链路：query_service + rerank + llm_pipeline
- 重点：
  - `mode` 影响评分融合，不等于是否 BFS
  - BFS/全局召回由 recall 参数控制
  - fallback 来源必须真实（`source`）

### 3.3 结构治理（compress/split/optimize）
- 入口：`force_compress` / `split_bucket` / `optimize`
- 关键链路：engine + optimize_service + llm_pipeline
- 风险：
  - ROOT/active 指针
  - successor 切换与 reparent
  - fallback 必须可生效，防死桶

### 3.4 存储与元数据
- 关键：`storage.py`
- 关注点：
  - bucket_version/context_version
  - alias map 持久化与冻结
  - 事件日志与上下文落盘一致性

## 4. 入口点

### 4.1 Python API
- 常用入口：`get_context_memory_engine`, `ContextMemoryEngineV3`, `BucketHandle`
- 最常被调用：`set_bucket/add_memory/query/list_memories/optimize/compress/split`

### 4.2 CLI
- 启动：`python -m context_memory.cli`
- 常用命令：`add/add_file/add_dir/query/list/optimize/compress/split/switch_bucket`
- 注意：`add_file` 关闭自动强切分用 `--no-force-split`

### 4.3 JSON-RPC
- 启动：`python -m context_memory.rpc_server`
- 路径：`POST /jsonrpc`
- 常用方法：`add_memory`, `add_memory_from_file`, `add_memory_from_dir`, `query`, `optimize`, `split_bucket`, `force_compress`

## 5. 测试入口（发布版）
- 三接口烟测：`tests/test_release_smoke_three_interfaces.py`
- 真实 LLM 烟测：`tests/test_real_llm_ci_smoke.py`
- 指针稳定性回归：`tests/test_root_active_pointer_stability.py`
- add_dir 严格回归：`tests/test_add_dir_subbucket_strict.py`
- 查询模式语义：`tests/test_query_mode_semantics.py`

## 6. 热路径与高风险区
- `engine.py`：生命周期、指针更新、重建切换、auto-manage
- `services/optimize_service.py`：计划校验与应用
- `llm_pipeline.py`：降级与来源标签
- `storage.py`：结构一致性与版本变更

## 7. 修改前阅读建议
- 改 query：先读 `query_service.py` + `llm_pipeline.py` + `invariants.md`
- 改 split/optimize/compress：先读 `engine.py` + `optimize_service.py` + `troubles.md`
- 改持久化：先读 `storage.py` + `contracts.md`