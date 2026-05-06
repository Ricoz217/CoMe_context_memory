# CoMe ContextMemory Docs (English)

## Getting Started
- Project Introduction: [project_introduction_en.md](project_introduction_en.md)
- Root README: [../README.md](../README.md)

## APIs and Interfaces
- Python API Guide: [python_api_guide_en.md](python_api_guide_en.md)
- CLI Guide: [cli_guide_en.md](cli_guide_en.md)
- Configuration Guide: [config_guide_en.md](config_guide_en.md)
- JSON-RPC 2.0 Guide: [jsonrpc_methods_en.md](jsonrpc_methods_en.md)

## Single-Writer Rule (Important)
- A single memory store (`same BASE_DIR`) is **single-writer**.
- Do not run multiple writer processes against the same `BASE_DIR` at the same time (for example Python + CLI + RPC concurrently).
- If you need multiple entry interfaces together, route all writes through one service process (recommended: one JSON-RPC server).
