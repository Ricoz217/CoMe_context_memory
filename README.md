# CoMe ContextMemory

`CoMe_ContextMemory` 是一个面向 LLM 上下文的本地记忆引擎。  
它不依赖向量数据库，直接把记忆组织为可递归的桶结构，通过 `query`、`optimize`、`compress`、`split` 等流程在本地维护可检索上下文。

## 文档入口
- 项目介绍: [docs/project_introduction.md](docs/project_introduction.md)
- 文档总览: [docs/README.md](docs/README.md)
- JSON-RPC 方法: [docs/jsonrpc_methods.md](docs/jsonrpc_methods.md)

## 快速开始
1. 安装依赖
```powershell
pip install -r requirements.txt
```

2. 准备配置（自动生成）
- 默认会在当前工作目录读取或生成 `config/context_memory.yaml`
- 也可通过环境变量指定：
  - `COME_CONTEXT_MEMORY_ROOT`
  - `COME_CONTEXT_MEMORY_CONFIG`

3. Python 调用
```python
from come_context_memory import ContextMemoryConfig, ContextMemoryEngineV3

cfg = ContextMemoryConfig(
    base_dir="data/my_memory",
    llm_preset="CONTEXT_MEMORY",
    use_mock_llm=False,
)
engine = ContextMemoryEngineV3(config=cfg)
```

## 三种接口
1. Python API（主接口）
2. CLI
3. JSON-RPC 2.0（FastAPI）

### CLI 启动
```powershell
$env:PYTHONPATH='D:\Python\CoMe_ContextMemory\src'
python -m come_context_memory.cli --base-dir D:\Python\CoMe_ContextMemory\data\cli_runtime
```

### JSON-RPC 启动
```powershell
$env:PYTHONPATH='D:\Python\CoMe_ContextMemory\src'
python -m come_context_memory.rpc_server --host 127.0.0.1 --port 9010 --base-dir D:\Python\CoMe_ContextMemory\data\rpc_runtime
```

服务端点：
- `POST /jsonrpc`
- `GET /healthz`

## 核心行为说明
1. `add_memory_from_file` 支持文本与图片（图片可用 `image_extract_hint` 指导解析），暂不支持 `pdf/docx`。
2. `add_memory_from_dir` 为自动化批处理入口，返回聚合 `added_keys` 便于手动回滚。
3. 未显式传 `bucket_id` 时，默认路由到当前 `active_bucket_id`。
4. `query` 模式仅支持 `auto | semantic | hybrid`：
   - `auto`: 字面特征强时走 `hybrid`，普通自然语言走 `semantic`
   - `literal` 已从公开模式移除，传入会报参数错误
5. 忘却机制可关闭：`ContextMemoryConfig(enable_forgetting=False)`，CLI/RPC 也提供 `--no-forgetting`。

## 烟测命令
```powershell
$env:PYTHONPATH='D:\Python\CoMe_ContextMemory\src'
python -m pytest tests\test_release_smoke_three_interfaces.py -q
python tests\query_concurrency_smoke.py --engine-module come_context_memory.memory.engine --concurrency 20 --use-mock-llm
```

真实 LLM 烟测：
```powershell
$env:PYTHONPATH='D:\Python\CoMe_ContextMemory\src'
$env:COME_RELEASE_SMOKE_REAL_LLM='1'
python -m pytest tests\test_release_smoke_three_interfaces.py -q
```
