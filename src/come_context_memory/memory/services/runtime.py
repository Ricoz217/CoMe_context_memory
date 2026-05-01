from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..engine import ContextMemoryEngineV3


@dataclass(slots=True)
class ServiceRuntime:
    engine: "ContextMemoryEngineV3"

    def __getattr__(self, name: str) -> Any:
        return getattr(self.engine, name)
