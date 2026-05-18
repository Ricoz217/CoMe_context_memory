# context_snapshot.md

> 快照日期：2026-05-18
> 用途：给新线程快速恢复“当前上下文中的重要信息”。

## A. 双仓协作现状
- 发布版仓库：`D:\Python\CoMe_ContextMemory`
- 内部版仓库：`D:\Python\TIYA_ThenIAskYou_2026`
- 规则：内部版先修复验证，发布版再同步收敛。

## B. 已确认的关键技术结论
1. `mode=auto|semantic|hybrid` 不是 BFS 开关，而是评分融合策略。
2. fallback 路径必须写真实来源，不能标记成 llm。
3. ROOT 与 active 是两个独立指针，更新条件必须拆开。
4. 大文本场景下治理链路会超窗，必须保证本地兜底可执行。

## C. 当前风险优先级
- P0：大文本压力治理（真实 token 统计 + 防死桶 fallback）。
- P0：Schema Migration 系统（已发布后兼容升级）。
- P1：RTK/HEADROOM 分类能力（先数据侧，后行为侧）。
- P1：桶事件 `last_event_at` 字段。

## D. 已补的关键回归点（发布版）
- ROOT/active 指针稳定性测试。
- add_dir 子目录递归与子桶创建测试。
- 三接口烟测与真实 LLM 烟测可作为发布前基线。

## E. 协作硬约束
- 用户优先中文。
- 编码必须 UTF-8，写后复读检查。
- 每次关键修复都要补历史与 TODO，防止长线程遗失。