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
