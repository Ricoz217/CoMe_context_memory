# CoMe ContextMemory 文档总览

## 入门
- 项目介绍: [project_introduction.md](project_introduction.md)
- 根 README: [../README.md](../README.md)

## API 与接口
- Python API 使用指南: [python_api_guide.md](python_api_guide.md)
- CLI 使用指南: [cli_guide.md](cli_guide.md)
- 配置填写指南: [config_guide.md](config_guide.md)
- JSON-RPC 2.0 方法清单: [jsonrpc_methods.md](jsonrpc_methods.md)

## English
- Docs Index (EN): [README_en.md](README_en.md)
- Project Introduction (EN): [project_introduction_en.md](project_introduction_en.md)
- Python API Guide (EN): [python_api_guide_en.md](python_api_guide_en.md)
- CLI Guide (EN): [cli_guide_en.md](cli_guide_en.md)
- Configuration Guide (EN): [config_guide_en.md](config_guide_en.md)
- JSON-RPC 2.0 Guide (EN): [jsonrpc_methods_en.md](jsonrpc_methods_en.md)

## 单写者约束（重要）
- 同一个记忆库（同一 `BASE_DIR`）采用**单写者模型**。
- 不要让多个写入进程同时操作同一 `BASE_DIR`（例如 Python + CLI + RPC 同时写）。
- 如需多入口并用，建议统一通过一个服务进程进行写入（推荐 JSON-RPC 服务）。
