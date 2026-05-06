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

- 为了优化子桶路由问题，一次 `query` 一般会进行两次本地运算 + 一次LLM运算: 
    1. 得到 `query` 内容时，会先根据内容，自动判断是否需要做 