from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable

from ..models import BUCKET_KIND_MEMORY, MemoryRecord, parse_iso_or_none

if TYPE_CHECKING:
    from ..engine_runtime import EngineRuntime


def _clamp_score(value: float) -> float:
    return max(0.0, min(1.0, float(value)))

class ForgettingService:
    def __init__(self, runtime: "EngineRuntime", *, primitives: Callable[[], Any]) -> None:
        self.runtime = runtime
        self._primitives_provider = primitives

    @property
    def primitives(self) -> Any:
        return self._primitives_provider()

    async def _apply_forgetting(self, bucket_id: str, *, from_compress: bool) -> None:
        if not self.runtime._enable_forgetting:
            return
        now = datetime.now(timezone.utc)
        records = await self.runtime.run_storage_task(self.runtime.storage.load_bucket_snapshot, bucket_id, include_gray=False)
        for rec in records:
            if rec.kind != BUCKET_KIND_MEMORY:
                continue
            node = await self.runtime.run_storage_task(self.runtime.storage.get_key_node, rec.key) or {}
            if from_compress:
                last_penalty = parse_iso_or_none(str(node.get('last_compress_penalty_at', '')))
                if last_penalty is not None and last_penalty.tzinfo is None:
                    last_penalty = last_penalty.replace(tzinfo=timezone.utc)
                if last_penalty is not None and now - last_penalty < timedelta(days=1):
                    continue
                await self.runtime.run_storage_task(self.runtime.storage.set_last_compress_penalty, rec.key)
            negative = self._calc_negative_weight(rec, node=node)
            await self.runtime.run_storage_task(self.runtime.storage.apply_negative_penalty, rec.key, negative)
            if rec.weight + negative < self.runtime._negative_delete_threshold:
                current = await self.runtime.run_storage_task(self.runtime.storage.get_record, rec.key)
                if current is not None and not current.gray:
                    await self.primitives.set_gray_unlocked(
                        current,
                        gray=True,
                        reason='auto_forget',
                    )

    def _calc_negative_weight(self, rec: MemoryRecord, *, node: dict[str, Any]) -> float:
        if not self.runtime._enable_forgetting:
            return 0.0
        now = datetime.now(timezone.utc)
        created = parse_iso_or_none(rec.created_at)
        if created is None:
            created = now
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        last_recalled = parse_iso_or_none(str(node.get('last_recalled_at', '')))
        if last_recalled is None:
            last_recalled = created
        if last_recalled.tzinfo is None:
            last_recalled = last_recalled.replace(tzinfo=timezone.utc)
        age_days = max(0.0, (now - created).total_seconds() / 86400.0)
        idle_days = max(0.0, (now - last_recalled).total_seconds() / 86400.0)
        query_hits = max(0, int(node.get('query_hits', 0)))
        penalty = 0.02 * age_days + 0.03 * idle_days - 0.01 * min(query_hits, 30)
        penalty = max(0.0, min(0.9, penalty))
        return -penalty

    def _apply_negative_weight_adjust(self, key: str, score: float, *, negative_weight: float | None=None) -> float:
        if not self.runtime._enable_forgetting:
            return _clamp_score(score)
        if negative_weight is None:
            node = self.runtime.storage.get_key_node(key) or {}
            negative_weight = float(node.get('last_negative_weight', 0.0))
        neg = float(negative_weight)
        adjusted = score + neg * 0.35
        return _clamp_score(adjusted)
