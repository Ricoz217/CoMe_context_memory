本文档为 `CoMe_ContextMemory` 的项目介绍

---

# 基本原理

- 将一个 `Context` 视为一个记忆桶，本地构建同步的缓存和索引，即是 `ContextMemory`
- 每个 `Context` 分为三部分内容: 
  1. `system_prompt`: 不变，每个LLM链路不同
  2. `memory`: **只增不改** 采用 `Event Append_Only` 方式增加
  3. `command`: 具体的任务指令和payload
- 解释 `Event Append_Only`

# 对比RAG的优缺点

## 优点

- RAG由向量数据库筛选合适chunk发送给LLM作答，有切片和无全局观问题。  
`ContextMemory` 一个桶内LLM能看见所有信息，包括完整文档/记忆内容，无需分片，能全局观作答。
- RAG需要向量数据库。`ContextMemory` 不需要，因为 `Context` 本身就是一种向量数据库。

## 缺点

- `ContextMemory` 有LLM最大视野限制，造成子桶路由问题。RAG是全量向量数据库检索，没有这种问题。
- `ContextMemory` 需要树状递归 `query`，子桶嵌套过深时延迟和成本都会增加。RAG只需要一次LLM回答，延迟和成本基本固定。

# 入库

- `ContextMemory` 采用 `Event Append_Only` 模式，`add_memory/update_memory/move_in` 等操作会注入完整的记忆内容进上下文。
- 触发超窗检测后，会自动 `compress/split_bucket`，创建新桶/新子桶。
- 提供批量入库方式，支持 `add_memory_from_file`, `add_memory_from_dir`; 从目录入库支持对子目录自动创建子桶，需要注意子桶嵌套深度限制。
- 提供手动触发的 `optimize`，批量入库接口有自动触发逻辑。
- `add_memory_from_file` 支持文本文件（包括代码），也支持图片文件，可显式传入解析图片的提示词。暂不支持 `pdf/docx`。

# Query

为了优化子桶路由问题，一次 `query` 一般会进行两次本地运算 + 一次LLM运算:   

1. 得到 `query` 内容时，会先根据内容，自动判断是否需要 `BFS全量子树路由增强`，这里会使用 `BM25 + n-gram` 扫描全量记忆，提升子树筛选正确性。
2. 一般自然语言内容不会走BFS以减少延迟和计算量，若要强制走BFS可在 `query` 接口显式传 `mode` 参数。
3. 发送到LLM，获取 `answer` 和 `matches`。
4. 对 `matches` 执行本地Rerank，也使用 `BM25 + n-gram`。
5. `BFS`, `LLM`, `Rerank` 三者分数会加权计算，以LLM分数为主，返回最终分数。
6. 若 `matches` 内有桶，会自动递归 `query` top1，直到抵达 `query` 深度限制或召回记忆单片。
7. 递归 `qeury` 的 `answer` 会自动传递到顶层 `query`，作为 `sub_answer` 字段，由用户自行处理，一般可将 `sub_answer` 作为最终答复。
8. 递归 `query` 会自动替换顶层 `matches` 的内容，可直接使用。  

子桶路由问题只是优化，本质无法解决，因此强烈建议不要按RAG的使用方式，而是先按照结构分若干子桶，`query` 时先选择指定子桶再执行。  
能显著降低延迟和提高召回率。

# 成本与延迟

`ContextMemory` 的成本与延迟都出在LLM请求上，但整个项目都做了 `cache_hit` 优化，不如说就是基于这点设计的。  
为了成本和延迟控制，强烈推荐唯一指定使用 `DeepSeek V4-flash`，并且走官方API，能获得极低的成本和较好的延迟表现，且拥有1M上下文和优异的召回率。  
若要用其他模型或本地LLM也支持，核心LLM请求链路走的是两种标准LLM API，在配置文件注明API格式即可。即:  

- Openai Completion Api
- Anthropic Api   

不支持 `Openai Response Api`

## 成本

成本由不同LLM链路、不同桶的冷热启动影响，冷热启动区别较大，原因如下: 
1. 不同LLM链路的 `system_prompt` 不同，无法命中 `cache_hit`。
2. 每个桶每个链路第一次请求/API供应商缓存过期，属于冷启动，无法命中 `cache_hit`。
3. 在不触发压缩、分桶（即桶一个桶），重复执行某个LLM链路，属于热启动，同时供应商的 `cache` 也会保留更长时间。
4. 热启动成本极低，即: `system_prompt` 和已缓存的老 `memory` 会命中 `cache_hit`，DSV4的缓存命中价格仅为 **0.02元/M token**。
5. 热启动实际消耗的成本，为新增部分的 `memory` 和本次操作的 `command`，这些实际上都很少，且下次请求新增的 `memory` 也会纳入缓存。  

> 一句话: 在桶重建(压缩/分桶)前，以合适的频率 `query` 基本不花钱

## 延迟

- 入库较慢，特别是文件入库、目录入库，这些都属于批量入库。原因: 
  1. `ContextMemory` 是单写者模式，且同一个桶不支持并发写，将在下方 `IO限制` 中细讲。
  2. 目录入库是自动遍历调用文件入库。文件入库默认会自动切片，然后创建批量入库任务。
  3. 批量入库任务实际上也是调用单个记忆入库接口(`add_memory`)，只是清洗逻辑不同，且在入库后会自动 `optimize` (注意：会创建一个新桶)。
  4. 每个记忆入库都需要LLM进行 `ingest`，主要是生成关系信息 `relations`。
  5. 批量入库任务有并发优化，但延迟依旧不低。
  6. 入库主要延迟就在LLM请求。
- Query 延迟主要在子树深度，同时与是否冷启动有关。原因: 
  1. `query` 会自动递归 `query` 子桶(子树)。
  2. 热启动(供应商有缓存)LLM请求会显著加快。
  3. `query` 延迟是所有LLM请求的总耗时，多个子桶递归 `query` 会并发请求。
  4. `query` 本身支持并发，桶只有写锁，无读锁。

# IO限制

- `ContextMemory` 为单写者设计，没做Redis、分布式锁、多进程锁，全异步设计，原子写，但多写者不安全，同时注意多线程的 `event_loop` 处理。  
项目以 `BASE_DIR(记忆库目录)` 为基础，不支持但没限制多进程、多实例对同一个记忆库写，请千万不要这么做。  
一定确保只有一个实例持有一个 `BASE_DIR`，否则将导致数据不安全、缓存索引错乱、甚至损坏记忆库。  

- 项目默认为单例模式，提供了 `get_context_memory_engine` 方法获取单例，但也可以通过直接创建 `ContextMemoryEngineV3` 对象获取多个不同实例。
- 对于桶分为写入和读取两种操作，写入每个桶一把锁，串行排队，不保证 `FIFO`，不同桶可并行写入。ROOT本身也是一个桶，但不建议直接写入ROOT。
  - 写入: `add/update/move/split/compress/create_bucket/optimize` 等会改变桶内容、创建新桶的都属于写入操作，同一个桶会串行等待锁。
  - 读取: `qeury/list/get` 等不改变桶内容的，属于读取操作。无锁，可并发。
  - 批量入库任务: `add_memory_from_file/add_memory_from_dir` 等，会将一个文件的记忆chunk视为一个批次，等待全部处理完成后一次性入库。  
  做了批量 `cache_hit` 优化，并发入库优化，同时会自动触发压缩/分桶/优化。

# 接口/API

提供三种接口:  
- python库: 全对象设计，可直接对象操作。
- CLI: 单独进程，通过CLI转换至python接口。
- JSON-RPC2.0: 与CLI一样，是转换接口。为其他语言且不想用CLI提供一种使用方式。无鉴权，API仅本机暴露，需要开放的可自行写网关。  

具体使用方式详接口文档和示例  

# 项目缺陷/TODO

- 无GUI，无分布式锁
- 记忆库仅为本地文件系统，暂未提供数据库接口
- 记忆库暂无自动化清理逻辑，仅提供了手动清理接口 `gc_storage`，且本地文件清理周期长，有空间膨胀风险。
- 虽然记忆和桶有哈希版本管理，但记忆库本身没有

---  

- [ ] 实现网页GUI，可视化使用记忆库
- [ ] 升级为LLM WIKI
- [ ] 仿照git的思路，重构记忆库文件逻辑，改为全量 + 增量存储，节省存储空间
- [ ] 引擎分布式