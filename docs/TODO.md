# TODO

## Alias 全链路收敛重构（下个小版本）

- 背景：当前 alias 映射逻辑分散在多条链路（query/optimize/split/compress/ingest/context_event/fallback），容易出现“桶被映射为 memory alias”这类漏修问题。
- 目标：alias 的映射与反映射统一下沉到 `aliasing` 模块，业务链路不再自行处理 alias 细节。

### 设计约束

- 各链路仅提供：
  - `key`
  - `bucket_id`
  - （可选）字段语义信息（用于判断 memory/bucket/ref）
- `aliasing` 模块统一负责：
  - 映射表读取与写入
  - alias 分配
  - 反解与类型校验
  - alias-only 载荷校验

### 覆盖范围

- `query`
- `optimize`
- `split`
- `compress`
- `ingest`
- `context_event`
- `fallback/degraded path`

### 验收标准

- 不再出现“桶节点被映射成 `memory_x`”的链路级错误。
- payload 构造与 LLM 输出解析都走统一入口。
- 删除链路内零散 alias 特判与补丁逻辑。
- 增加端到端 alias 一致性测试（正常/降级/递归场景）。

