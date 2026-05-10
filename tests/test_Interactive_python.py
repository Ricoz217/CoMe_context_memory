import asyncio
import sys
import traceback
import json
from pathlib import Path
from typing import Any
from dataclasses import is_dataclass, asdict

from context_memory import get_context_memory_engine, ContextMemoryConfig

sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.input_manager import read_command_async

_MEMORY_DIR = Path(__file__).parent.parent / "data" / "memory"
_MEMORY_DIR.mkdir(parents=True, exist_ok=True)
memory_config = ContextMemoryConfig(
    base_dir=_MEMORY_DIR,
    llm_preset="CONTEXT_MEMORY",
    image_llm_preset="KIMI2.6"
)

def _json_print(obj: Any) -> str:
    def _default_json(_obj):

        if hasattr(_obj, "to_dict") and callable(_obj.to_dict):
            return _obj.to_dict()

        if is_dataclass(_obj):
            return asdict(_obj)

        raise TypeError(f"{type(_obj)} 不可序列化")

    return json.dumps(obj, indent=2, ensure_ascii=False, default=_default_json)

async def main():
    async def add_file(_file: str):
        _path = Path(_file.strip('"'))
        result = await memory.add_memory_from_file(_file, force_split=True)
        print(_json_print(result))

    async def add_memory(_text: str):
        result = await memory.add_memory(_text)
        print(_json_print(result))

    async def list_memory():
        result = await memory.list_memories(include_gray=False)
        print(_json_print(result))

    async def query(_text: str):
        result = await memory.query(_text)
        print(_json_print(result))

    async def stats():
        result = await memory.stats()
        print(_json_print(result))

    async def optimize():
        result = await memory.optimize()
        print(_json_print(result))

    async def export(mem_id: str):
        result = await memory.export_memory_to_markdown(mem_id)
        print(_json_print(result))

    async def compress():
        result = await memory.force_compress()
        print(_json_print(result))

    async def delete(hash_id: str):
        result = await memory.delete_memory(hash_id)
        print(_json_print(result))

    async def add_dir(_dir: str):
        _dir = Path(_dir.strip('"'))
        result = await memory.add_memory_from_dir(str(_dir))
        print(_json_print(result))

    async def switch(bucket_id: str):
        result = await memory.set_active_bucket(bucket_id)
        print(_json_print(result))

    async def latest_bucket():
        result = await memory.latest_bucket_id()
        print(_json_print(result))

    async def get_memory(key: str):
        result = await memory.get_memory(key)
        print(_json_print(result))

    async def _run():
        if cmd in exec_mapping:
            if params:
                await exec_mapping[cmd](params)

            else:
                await exec_mapping[cmd]()

    exec_mapping = {
        "add_file": add_file,
        "add": add_memory,
        "get": get_memory,
        "list": list_memory,
        "query": query,
        "stats": stats,
        "optimize": optimize,
        "export": export,
        "compress": compress,
        "delete": delete,
        "add_dir": add_dir,
        "switch": switch,
        "lastest": latest_bucket
    }

    root = get_context_memory_engine(config=memory_config)
    memory = await root.set_bucket("TEST_FROMDIR")
    while True:
        try:
            prompt: str = await read_command_async()
            splited = prompt.strip().split(' ')
            if not splited:
                continue

            cmd = splited[0]
            params = ' '.join(splited[1:]) if len(splited) > 1 else ""
            if cmd not in exec_mapping:
                print(f"unknown cmd: {cmd}")
                continue

            asyncio.create_task(_run())

        except asyncio.CancelledError:
            exit(0)

        except KeyboardInterrupt:
            exit(0)

        except Exception as E:
            print(E)
            print(traceback.format_exc())

if __name__ == '__main__':
    asyncio.run(main())