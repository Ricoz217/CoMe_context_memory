__data_version__ = 4

from .engine import (
    BucketHandle,
    ContextMemoryConfig,
    ContextMemoryEngineV3,
    ContextMemorySystem,
    __data_version__ as engine_data_version,
    get_context_memory_engine,
)
from .llm_pipeline import LLMPresetConfigError
from .models import BucketContextUsage, ListMemoriesResult, MemoryIndexItem

if int(engine_data_version) != int(__data_version__):
    raise RuntimeError(
        f"memory __data_version__ mismatch: package={__data_version__}, engine={engine_data_version}"
    )

__all__ = [
    "__data_version__",
    "ContextMemoryEngineV3",
    "ContextMemorySystem",
    "ContextMemoryConfig",
    "BucketHandle",
    "get_context_memory_engine",
    "LLMPresetConfigError",
    "MemoryIndexItem",
    "ListMemoriesResult",
    "BucketContextUsage",
]
