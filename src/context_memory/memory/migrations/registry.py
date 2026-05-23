from __future__ import annotations

from typing import Sequence

from .types import DataMigrationStep

_STEPS: list[DataMigrationStep] = []


def register_step(step: DataMigrationStep) -> None:
    if int(step.to_version) <= int(step.from_version):
        raise ValueError(f"invalid migration step (to<=from): {step.id}")
    for existing in _STEPS:
        if int(existing.from_version) == int(step.from_version):
            raise ValueError(
                f"duplicate migration from_version={step.from_version}: "
                f"{existing.id} vs {step.id}"
            )
        if int(existing.to_version) == int(step.to_version):
            raise ValueError(
                f"duplicate migration to_version={step.to_version}: "
                f"{existing.id} vs {step.id}"
            )
    _STEPS.append(step)
    _STEPS.sort(key=lambda s: int(s.from_version))


def list_steps() -> Sequence[DataMigrationStep]:
    return tuple(_STEPS)


def latest_registered_version(default: int) -> int:
    if not _STEPS:
        return int(default)
    out = int(default)
    for step in _STEPS:
        out = max(out, int(step.to_version))
    return out


def resolve_chain(from_version: int, to_version: int) -> list[DataMigrationStep]:
    source = int(from_version)
    target = int(to_version)
    if source == target:
        return []
    if source > target:
        raise RuntimeError(f"downgrade is not supported: {source} -> {target}")
    by_from: dict[int, DataMigrationStep] = {int(s.from_version): s for s in _STEPS}
    chain: list[DataMigrationStep] = []
    cur = source
    while cur < target:
        step = by_from.get(cur)
        if step is None:
            raise RuntimeError(f"missing migration step from schema_version={cur} to reach {target}")
        nxt = int(step.to_version)
        if nxt <= cur:
            raise RuntimeError(f"invalid non-forward migration step: {step.id}")
        chain.append(step)
        cur = nxt
    if cur != target:
        raise RuntimeError(f"migration chain mismatch: reached {cur}, expected {target}")
    return chain

