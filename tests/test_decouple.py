import asyncio
import sys
import traceback
from come_context_memory import get_context_memory_engine, ContextMemoryConfig
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.input_manager import read_command_async

_MEMORY_DIR = Path(__file__).parent.parent / "data" / "memory"
_MEMORY_DIR.mkdir(parents=True, exist_ok=True)
memory_config = ContextMemoryConfig(
    base_dir=_MEMORY_DIR,
    llm_preset="CONTEXT_MEMORY",
    image_llm_preset="KIMI2.6"
)

async def main():
    async def add_file(_file: str):
        _path = Path(_file.strip('"'))
        print(await memory.add_memory_from_file(_path, force_split=True))

    async def add_memory(_text: str):
        print(await memory.add_memory(_text))

    async def list_memory():
        print(await memory.list_memories(include_gray=False))

    async def query(_text: str):
        print(await memory.query(_text))

    async def stats():
        print(await memory.stats())

    async def optimize():
        print(await memory.optimize())

    async def export(mem_id: str):
        print(await memory.export_memory_to_markdown(mem_id))

    async def compress():
        print(await memory.force_compress())

    async def delete(hash_id: str):
        print(await memory.delete_memory(hash_id))

    async def add_dir(_dir: str):
        _dir = Path(_dir.strip('"'))
        print(await memory.add_memory_from_dir(_dir))

    async def _run():
        if cmd in exec_mapping:
            if params:
                await exec_mapping[cmd](params)

            else:
                await exec_mapping[cmd]()

    exec_mapping = {
        "add_file": add_file,
        "add": add_memory,
        "list": list_memory,
        "query": query,
        "stats": stats,
        "optimize": optimize,
        "export": export,
        "compress": compress,
        "delete": delete,
        "add_dir": add_dir
    }

    memory = get_context_memory_engine(config=memory_config)
    memory = await memory.set_bucket("TEST")
    while True:
        try:
            prompt: str = await read_command_async()
            splited = prompt.strip().split(' ')
            if not splited:
                continue

            cmd = splited[0]
            params = ' '.join(splited[1:]) if len(splited) > 1 else ""
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