from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class MigrationContext:
    run_id: str
    from_version: int
    to_version: int
    workspace_root: Path


class DataMigrationStep(Protocol):
    id: str
    from_version: int
    to_version: int

    def apply(self, *, storage: Any, context: MigrationContext) -> dict[str, Any]:
        ...

    def validate(self, *, storage: Any, context: MigrationContext) -> dict[str, Any]:
        ...

