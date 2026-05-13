# CoMe ContextMemory

English version: [README_en.md](README_en.md)

`CoMe_ContextMemory` 是一个基于 LLM 上下文的记忆库，通过另一种方式实现RAG。  
它不依赖向量数据库，而是通过桶树结构组织记忆，并提供 `query / optimize / compress / split` 等流程进行维护与检索。

## 文档入口
- 项目介绍: [docs/project_introduction.md](docs/project_introduction.md)
- 文档总览: [docs/README.md](docs/README.md)
- Python API: [docs/python_api_guide.md](docs/python_api_guide.md)
- CLI 指南: [docs/cli_guide.md](docs/cli_guide.md)
- JSON-RPC 指南: [docs/jsonrpc_methods.md](docs/jsonrpc_methods.md)
- 配置指南: [docs/config_guide.md](docs/config_guide.md)

---  

## 快速开始

### 1. 通过 pip 安装（推荐）
```powershell
pip install come-context-memory
```

安装后可直接使用：
- Python（`import context_memory`）
- CLI 模式
- JSON-RPC 服务器

### 2. 使用 Release 的 Embedding 包（仅 Windows）
适用于不想单独安装 Python 环境的场景。

1. 下载并解压 release 中的 embedding 压缩包  
2. 进入解压目录  
3. 运行：
```bat
run_cli.bat
```
或
```bat
run_jsonrpc.bat --host 127.0.0.1 --port 8000
```

说明：
- embedding 方案目前仅支持 Windows
- 依赖和解释器都封装在目录内，不依赖系统 Python

### 3. 手动安装
```powershell
git clone https://github.com/Ricoz217/CoMe_context_memory.git
cd CoMe_context_memory
pip install -r requirements.txt
pip install -e ./
```

可直接启动：
```powershell
python -m context_memory.cli
python -m context_memory.rpc_server
```

> 首次运行会在工作目录生成config文件夹，可配置你的 `LLM ApiKey` 后重启。详细说明: [docs/config_guide.md](docs/config_guide.md)

## 三种接口
1. Python 调用（主接口）
2. CLI
3. JSON-RPC 2.0（FastAPI）

---  

## 使用示例

### Python 调用

```python
import os
import asyncio
from pathlib import Path
from context_memory import get_context_memory_engine, ContextMemoryConfig  # 单例入口、配置


"""
初始化配置，设置记忆库目录(BASE_DIR)、主LLM预设、识图LLM预设
"""
_BASE_DIR = Path(os.getcwd()) / "MemoryLibrary"
_init_config = ContextMemoryConfig(
    base_dir=_BASE_DIR,  # 记忆库目录
    llm_preset="CONTEXT_MEMORY",  # 主LLM预设，强烈建议使用DSV4-flash
    image_llm_preset="KIMI2.6"  # 识图VLM预设
)

# Engine内部使用的是异步锁，因此需要自行管理多线程调用/事件循环。
# CoMe为线程不安全，尽量把Engine实例放在一个单独的线程/唯一event_loop，跨线程调用可将任务提交到主线程。
memory_engine = get_context_memory_engine(config=_init_config)  # 获取单例并配置


"""
全异步对象化操作
"""
async def main():
    # 创建一个桶，ROOT本身也是一个桶，但不推荐直接把记忆放入ROOT
    # 标题映射表为Engine全局，因此不要依赖标题，使用 `get_bucket()` 唯一桶ID更安全
    # 旧桶ID在物理删除前一直可用，会自动追溯到最新可用桶id，`get_bucket()` 会返回该桶最新id
    test_bucket = await memory_engine.set_bucket("TEST")  # 通过标题创建/获取桶。

    # 添加一条记忆
    print(await test_bucket.add_memory("今天是3月25日，地下室好冷，没什么人理我"))

    # 从文件添加记忆
    print(await test_bucket.add_memory_from_file("./母猪的产后护理.txt"))

    # 从目录添加记忆
    print(await test_bucket.add_memory_from_dir("./TypeScript Best Language of the World/"))

    # 列出当前桶内容
    print(await test_bucket.list_memories())

    # Query
    print(await test_bucket.query("谁是蒙古上单？"))
    
if __name__ == '__main__':
    asyncio.run(main())
```

详细说明: [docs/python_api_guide.md](docs/python_api_guide.md)

### CLI

- pip 安装: 直接在当前环境输入 `python -m context_memory.cli` 即可启动命令行模式，输入 `help` 获取命令列表
- release 包: 运行 run_cli.bat，可附加参数。
- 手动安装: 安装生产环境后运行 `python -m context_memory.cli`，  
也可以直接运行以下文件 `./src/context_memory/cli.py` 
- 详细说明: [docs/cli_guide.md](docs/cli_guide.md)

### JSON-RPC (FastApi)

- pip 安装: 直接在当前环境输入 `python -m context_memory.rpc_server` 即可开启本机服务器，其他程序通过调用API的方式使用
- release 包: 运行 run_jsonrpc.bat，可附加参数。
- 手动安装: 安装生产环境后运行 `python -m context_memory.rpc_server`，  
也可以直接运行以下文件 `./src/context_memory/rpc_server.py`  
- 详细说明: [docs/jsonrpc_methods.md](docs/jsonrpc_methods.md)

---  

## 注意事项

- 不支持对同一记忆库使用多个进程(多个引擎实例)并发使用，会造成数据不安全
- CLI/JSON-RPC 都会开启一个独立进程，不要同时使用不同的接口方式
- 可通过以下代码手动创建不同的引擎实例，同时管理多个记忆库:  
```python
from context_memory import ContextMemoryEngineV3

new_engine = ContextMemoryEngineV3()
```
- 不提供回退接口，批量入库需谨慎。记忆系统为了数据安全起见，即使中断了重启后也会继续未完成的任务。只能通过批量任务结果手动逐个删除新添加的记忆。
- JSON-RPC 无鉴权，要自行编写网关。
- 当前记忆未作时间管理，若要添加带时间要素的记忆(例如日程安排、重要事件)，需要在记忆中显式注明。  
同时 `query` 也不会自动传入时间，若有需要请显式传入。 

## 提问的智慧

为了更快定位问题、减少来回沟通，建议提问时尽量包含以下信息：

1. 明确目标
   - 你想实现什么，而不只是“哪里报错了”。  

2. 提供最小复现
   - 最好给出最短代码片段、CLI 命令或 JSON-RPC 请求体。  

3. 说明环境
   - 包版本、安装方式（pip/embedding/source）、操作系统、Python 版本。  

4. 提供完整报错
   - 贴完整 traceback 或关键日志，不要只截最后一行。  

5. 说明预期与实际差异
   - 预期结果是什么，实际结果是什么。  

6. 说明已尝试的排查
   - 你已经做过哪些检查，能避免重复建议。  

这样的问题通常能更快拿到准确答复。

## TODO
- [ ] 显式记忆权重管理
- [ ] 显式设置记忆过期时间
- [ ] 显式锁定记忆
- [ ] 加入时间管理功能
- [ ] 加入RTK和HEADROOM
- [ ] 记忆库升级系统
