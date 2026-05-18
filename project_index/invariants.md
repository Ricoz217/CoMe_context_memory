# invariants.md

## 1. 系统不变式
- 引擎必须在 LLM 不稳定时仍可用（可降级，不可瘫痪）。
- 任何治理动作都应有可追踪事件或状态变化。
- 读路径必须可追溯到当前可用桶（包含 sealed_to 路径）。

## 2. 桶结构不变式
- ROOT 是整棵树入口，不随 active 子桶重建而变化。
- active bucket 是会话工作焦点，可变化但必须有效。
- sealed 桶不可写，且应能追踪 successor。
- 父子关系变更后，parent/children 必须同步一致。

## 3. 数据不变式
- 同一 key 的“最新版本”引用必须指向可读取记录。
- bucket_version/context_version 增量必须与写操作对应。
- alias map 的生成、冻结、追踪要前后一致。

## 4. 查询不变式
- `final_score` 必须在 `[0,1]`。
- `source` 必须与真实执行路径一致。
- fallback 结果不得伪装主链路来源。

## 5. 治理不变式（compress/split/optimize）
- 即使 LLM 返回空或失败，也必须有兜底执行路径。
- split fallback 必须能真正拆分，不能“执行后无变化”。
- optimize 不应注入与目标无关的大体量上下文，避免自锁。

## 6. 兼容不变式
- 发布版接口默认保持向后兼容。
- 高风险变更必须同步测试与文档。
- 不允许将内部业务耦合逻辑直接并入发布版。