# Configuration Guide

This document explains configuration sources, precedence, and key fields for `CoMe_ContextMemory`.

## 1. Configuration Loading Rules

1. Root path (`ROOT_DIR`)
   - If `COME_CONTEXT_MEMORY_ROOT` is set, that path is used.
   - Otherwise, a default path is derived from the project context.

2. Config file path
   - If `COME_CONTEXT_MEMORY_CONFIG` is set, that file is used.
   - Otherwise, default path is `./config/context_memory.yaml` under current working directory.

3. Auto generation
   - If config file does not exist, an initial YAML template is generated automatically.

## 2. Two-layer Configuration System

The project has two config layers:

1. Runtime engine config (`ContextMemoryConfig`)
   - Controls bucket depth, auto-maintenance, query strategy, etc.
   - Passed at Python/CLI/RPC startup.

2. LLM and proxy config (YAML)
   - Provided by `config/context_memory.yaml`.
   - Mainly for `llm_presets` and `proxies`.

Non-empty runtime config values override YAML config values.

Context window rule (important):
1. The engine no longer accepts manual `max_context_window`.
2. The effective window limit is always read from `llm_presets.<preset>.max_context`.
3. If the target preset misses `max_context`, the program raises an exception and aborts startup.

## 3. YAML Example

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

## 4. `llm_presets` Field Notes

Required fields:
1. `endpoint`
2. `token`
3. `model`
4. `api_type`
5. `max_context`
6. `auto_compress_gate`
7. `extra_parameter`
8. `proxy_mode`
9. `price`

Notes:
1. `api_type` currently supports `openai` and `anthropic` style APIs.
2. OpenAI Response API is not supported.
3. `proxy_mode` can be either a proxy name (from `proxies`) or a direct proxy URL.
4. `extra_parameter` contains model-specific extra options.
5. `auto_compress_gate` affects auto-compress and auto-split behavior.
6. `price` is a compatibility field and can stay as an empty dict.
7. You can copy preset templates to create custom presets.

## 5. Key `ContextMemoryConfig` Fields

Common fields:
1. `base_dir`: storage directory (strongly recommended to set explicitly)
2. `llm_preset` / `image_llm_preset`
3. `use_mock_llm`
4. `enable_cleaning`
5. `enable_forgetting`
6. `auto_manage`
7. `max_bucket_depth`
8. `max_memory_bytes`
9. `query_mode_default` (only `auto|semantic|hybrid`)

Global recall fields:
1. `global_recall_top_n`
2. `global_recall_top_m`
3. `global_recall_depth_limit`
4. `global_recall_time_budget_ms`
5. `global_recall_boost_weight`

## 6. Modes and Valid Values

1. `query_mode_default` supports:
   - `auto`
   - `semantic`
   - `hybrid`

## 7. Recommended Usage

1. For business projects:
   - pass `ContextMemoryConfig(base_dir=...)` explicitly
   - use YAML mainly for LLM presets and proxies

2. For multi-instance setups:
   - use different `base_dir` per instance
   - do not use multiple writers on the same `base_dir`

## 8. Pre-run Checklist

1. `llm_presets.CONTEXT_MEMORY.token` is set.
2. `llm_presets.CONTEXT_MEMORY.endpoint` matches `api_type`.
3. If proxy is used, `proxy_mode` exists in config.
4. `query_mode_default` is not `literal`.
5. `base_dir` is writable.
