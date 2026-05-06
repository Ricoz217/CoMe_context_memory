# 配置填写指南

本文档说明 `CoMe_ContextMemory` 的配置来源、优先级与字段含义。

## 1. 配置加载规则

1. 根路径（`ROOT_DIR`）：
   - 若设置 `COME_CONTEXT_MEMORY_ROOT`，使用该路径
   - 否则使用项目目录上级推导路径

2. 配置文件路径：
   - 若设置 `COME_CONTEXT_MEMORY_CONFIG`，使用该文件
   - 否则默认读取/生成 `./config/context_memory.yaml`（当前工作目录）

3. 自动生成：
   - 配置不存在时会自动生成初始 YAML

## 2. 两层配置体系

项目有两类配置：

1. 运行时引擎配置（`ContextMemoryConfig`）
   - 控制桶深度、自动维护、query 策略等
   - 在 Python/CLI/RPC 启动时传入

2. LLM 与代理配置（YAML）
   - 由 `config/context_memory.yaml` 提供
   - 主要用于 `llm_presets` 和 `proxies`  

运行时配置非空项会覆盖 `config` 文件的配置

上下文窗口规则（重要）：
1. 引擎不再接受手动 `max_context_window` 配置。
2. 窗口上限统一由 `llm_presets.<preset>.max_context` 决定。
3. 若目标 preset 缺少 `max_context`，程序会直接抛异常并中止启动。

## 3. YAML 结构示例

```yaml
Common:
  FileCacheExpire: 30

LLM:
  ChatRequestTimeout: 300

llm_presets:
  CONTEXT_MEMORY:
    endpoint: "https://api.deepseek.com/chat/completions"
    token: "YOUR_API_KEY"
    model: "deepseek-v4-flash"
    api_type: "openai"   # openai | anthropic
    max_context: 1000000
    auto_compress_gate: 0.7
    extra_parameter: {"thinking": {"type": "disabled"}, "temperature": 0.7, "max_token": 1000000}
    proxy_mode: ""
    price: {}

  KIMI2.6:
    endpoint: "https://api.siliconflow.cn/v1/chat/completions"
    token: "YOUR_API_KEY"
    model: "Pro/moonshotai/Kimi-K2.6"
    api_type: "openai"
    max_context: 256000
    auto_compress_gate: 0.7
    extra_parameter: {}
    proxy_mode: ""
    price: {}

proxies:
  LOCAL_7890:
    http: "http://127.0.0.1:7890"
    https: "http://127.0.0.1:7890"

Logging:
  stdout_enabled: true
  write_error_file: true
  error_log_file: "logs/error.log"
```

## 4. llm_presets 字段说明

必填字段：
1. `endpoint`
2. `token`
3. `model`
4. `api_type`
5. `max_context`
6. `auto_compress_gate`
7. `extra_parameter`
8. `proxy_mode`
9. `price`

说明：
1. `api_type` 当前支持 `openai` 与 `anthropic` 风格接口，即 OpenAI Completion API 和 Anthropic API。
2. 不支持 OpenAI Response API。
3. `proxy_mode` 可填代理名称（来自 `proxies`）或直接填 URL。
4. `extra_parameter` 为输入模型的额外参数。其中 `thinking` 是DS官方的关闭思维模型参数，其他模型不一定通用。  
`temperature` 不建议高于0.7，`max_token` 保持与 `max_context` 一致即可。
5. `auto_compress_gate` 会影响自动压缩/自动拆桶，若不清楚具体作用就不要改，保持0.7。
6. `price` 字段为历史遗留问题，不要删除也不需要填写值，保持空字典即可。
7. LLM模板可以复制以创建你自己的模板。

## 5. ContextMemoryConfig 关键字段

常用字段：
1. `base_dir`: 记忆库目录（强烈建议显式传入）
2. `llm_preset` / `image_llm_preset`
3. `use_mock_llm`
4. `enable_cleaning`
5. `enable_forgetting`
6. `auto_manage`
7. `max_bucket_depth`
8. `max_memory_bytes`
9. `query_mode_default`（仅支持 `auto|semantic|hybrid`）

全局召回相关：
1. `global_recall_top_n`
2. `global_recall_top_m`
3. `global_recall_depth_limit`
4. `global_recall_time_budget_ms`
5. `global_recall_boost_weight`

## 6. 模式与合法值

1. `query_mode_default` 支持：
   - `auto`
   - `semantic`
   - `hybrid`

## 7. 优先级建议

1. 业务项目建议：
   - 显式传 `ContextMemoryConfig(base_dir=...)`
   - YAML 主要维护 LLM preset 与代理

2. 多实例场景建议：
   - 每个实例使用不同 `base_dir`
   - 不要多写者共享同一个 `base_dir`

## 8. 运行前检查清单

1. `llm_presets.CONTEXT_MEMORY.token` 已填写
2. `llm_presets.CONTEXT_MEMORY.endpoint` 与 `api_type` 匹配
3. 若启用代理，`proxy_mode` 对应项存在
4. `query_mode_default` 不是 `literal`
5. `base_dir` 路径可写
