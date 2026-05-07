# CoMe ContextMemory

`CoMe_ContextMemory` is a local memory engine for LLM context management.  
It does not rely on vector databases. Instead, it organizes memory in a bucket-tree structure and provides workflows such as `query / optimize / compress / split` for maintenance and retrieval.

## Documentation
- Project Introduction: [docs/project_introduction.md](docs/project_introduction.md)
- Docs Index: [docs/README.md](docs/README.md)
- Python API: [docs/python_api_guide.md](docs/python_api_guide.md)
- CLI Guide: [docs/cli_guide.md](docs/cli_guide.md)
- JSON-RPC Guide: [docs/jsonrpc_methods.md](docs/jsonrpc_methods.md)
- Config Guide: [docs/config_guide.md](docs/config_guide.md)

---

## Quick Start

### 1. Install via pip (recommended)
```powershell
pip install -i https://test.pypi.org/simple/ come-context-memory
```

After installation, you can directly use:
- Python (`import context_memory`)
- CLI mode
- JSON-RPC server

### 2. Use the Release Embedding package (Windows only)
This is suitable if you do not want to install Python separately.

1. Download and extract the embedding archive from Release.
2. Enter the extracted directory.
3. Run:
```bat
run_cli.bat
```
or
```bat
run_jsonrpc.bat --host 127.0.0.1 --port 8000
```

Notes:
- The embedding solution currently supports Windows only.
- Dependencies and interpreter are bundled in the package directory (no system Python required).

### 3. Manual installation
```powershell
git clone https://github.com/Ricoz217/CoMe_context_memory.git
cd CoMe_context_memory
pip install -r requirements.txt
pip install -e ./
```

Then start directly:
```powershell
python -m context_memory.cli
python -m context_memory.rpc_server
```

> On first run, a `config` directory is created in your working directory. Configure your `LLM ApiKey` and restart. See [docs/config_guide.md](docs/config_guide.md).

## Three Interfaces
1. Python API (primary interface)
2. CLI
3. JSON-RPC 2.0 (FastAPI)

---

## Usage Example

### Python API

```python
import os
import asyncio
from pathlib import Path
from context_memory import get_context_memory_engine, ContextMemoryConfig

"""
Initialize config: BASE_DIR, main LLM preset, image LLM preset
"""
_BASE_DIR = Path(os.getcwd()) / "MemoryLibrary"
_init_config = ContextMemoryConfig(
    base_dir=_BASE_DIR,
    llm_preset="CONTEXT_MEMORY",
    image_llm_preset="KIMI2.6"
)

# Engine uses async locks internally.
# Treat CoMe as not thread-safe: keep one engine in one dedicated thread/event loop.
# For cross-thread calls, submit tasks back to the engine's main loop.
memory_engine = get_context_memory_engine(config=_init_config)

"""
Fully async object-style operations
"""
async def main():
    # ROOT is also a bucket, but storing directly in ROOT is not recommended.
    # Title mapping is engine-global; bucket ID access is safer than relying on titles.
    # Historical bucket IDs are auto-resolved to latest available IDs before hard deletion.
    test_bucket = await memory_engine.set_bucket("TEST")

    # Add one memory
    print(await test_bucket.add_memory("2021 Stockholm Major Nuke 3rd floor deagle triple kill"))

    # Add memory from a file
    print(await test_bucket.add_memory_from_file("./postpartum_care_of_sows.txt"))

    # Add memory from a directory
    print(await test_bucket.add_memory_from_dir("./TypeScript Best Language of the World/"))

    # List bucket content
    print(await test_bucket.list_memories())

    # Query
    print(await test_bucket.query("Who is the Mongolian top laner?"))

if __name__ == "__main__":
    asyncio.run(main())
```

Detailed guide: [docs/python_api_guide.md](docs/python_api_guide.md)

### CLI

- pip install: run `python -m context_memory.cli` in your current environment, then use `help` to list commands.
- release package: run `run_cli.bat` (supports extra args).
- manual install: after environment setup, run `python -m context_memory.cli`; you can also run `./src/context_memory/cli.py` directly.
- details: [docs/cli_guide.md](docs/cli_guide.md)

### JSON-RPC (FastAPI)

- pip install: run `python -m context_memory.rpc_server` to start local server; call APIs from other programs.
- release package: run `run_jsonrpc.bat` (supports extra args).
- manual install: after environment setup, run `python -m context_memory.rpc_server`; you can also run `./src/context_memory/rpc_server.py` directly.
- details: [docs/jsonrpc_methods.md](docs/jsonrpc_methods.md)

---

## Notes

- Do not use multiple processes (multiple engine instances) on the same memory library concurrently; this is unsafe.
- CLI and JSON-RPC each start a separate process; do not use different interface modes simultaneously on the same library.
- You can manually create different engine instances for different memory libraries:
```python
from context_memory import ContextMemoryEngineV3

new_engine = ContextMemoryEngineV3()
```
- No rollback API is provided. Use batch ingestion carefully. For safety, interrupted tasks are resumed after restart, and added items must be manually deleted via returned keys if needed.
- JSON-RPC has no built-in authentication; place it behind your own gateway/auth layer.

## TODO

- [ ] TBD

