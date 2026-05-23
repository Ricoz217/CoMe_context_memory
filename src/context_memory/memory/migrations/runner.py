from __future__ import annotations

from typing import Any

from .registry import resolve_chain
from .types import MigrationContext


def build_migration_plan(*, from_version: int, to_version: int) -> list[dict[str, Any]]:
    chain = resolve_chain(from_version=from_version, to_version=to_version)
    return [
        {
            "id": str(step.id),
            "from_version": int(step.from_version),
            "to_version": int(step.to_version),
        }
        for step in chain
    ]


def run_migration_chain(
    *,
    storage: Any,
    from_version: int,
    to_version: int,
    run_id: str,
    workspace_root: Any,
) -> list[dict[str, Any]]:
    chain = resolve_chain(from_version=from_version, to_version=to_version)
    out: list[dict[str, Any]] = []
    for step in chain:
        context = MigrationContext(
            run_id=run_id,
            from_version=int(step.from_version),
            to_version=int(step.to_version),
            workspace_root=workspace_root,
        )
        apply_info = step.apply(storage=storage, context=context) or {}
        validate_info = step.validate(storage=storage, context=context) or {}
        out.append(
            {
                "id": str(step.id),
                "from_version": int(step.from_version),
                "to_version": int(step.to_version),
                "apply": dict(apply_info) if isinstance(apply_info, dict) else {"result": apply_info},
                "validate": dict(validate_info) if isinstance(validate_info, dict) else {"result": validate_info},
            }
        )
    return out

