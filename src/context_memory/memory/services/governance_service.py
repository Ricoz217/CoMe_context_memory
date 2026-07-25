from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Callable


if TYPE_CHECKING:
    from ..engine_runtime import EngineRuntime


def _clamp_score(value: float) -> float:
    return max(0.0, min(1.0, float(value)))

class GovernanceService:
    def __init__(self, runtime: "EngineRuntime", *, compression: Callable[[], Any], forgetting: Callable[[], Any], read: Callable[[], Any], split: Callable[[], Any], topology: Callable[[], Any]) -> None:
        self.runtime = runtime
        self._compression_provider = compression
        self._forgetting_provider = forgetting
        self._read_provider = read
        self._split_provider = split
        self._topology_provider = topology

    @property
    def compression(self) -> Any:
        return self._compression_provider()

    @property
    def forgetting(self) -> Any:
        return self._forgetting_provider()

    @property
    def read(self) -> Any:
        return self._read_provider()

    @property
    def split(self) -> Any:
        return self._split_provider()

    @property
    def topology(self) -> Any:
        return self._topology_provider()

    async def _auto_manage_bucket(self, bucket_id: str) -> None:
        if not self.runtime.auto_manage:
            return
        info = self.runtime.storage.get_bucket_info(bucket_id)
        if info is None or info.sealed:
            return
        await self.forgetting._apply_forgetting(bucket_id, from_compress=False)
        async with self.topology._bucket_write_lock(bucket_id) as locked_bucket:
            info = self.runtime.storage.get_bucket_info(locked_bucket)
            if info is None or info.sealed:
                return
            pressure, count = await self._bucket_pressure(locked_bucket)
            did_compress = False
            did_split = False
            split_round = 0
            if pressure > self.runtime._auto_compress_trigger_ratio or count > 1000:
                did_compress = True
                try:
                    comp = await self.compression._force_compress_unlocked(bucket_id=locked_bucket, reason='auto_threshold')
                    if not bool(getattr(comp, 'success', False)):
                        await self.runtime.run_storage_task(self.runtime.storage.append_event, event_type='AUTO_COMPRESS_FAIL', bucket_id=locked_bucket, payload={'reason': 'auto_threshold', 'message': str(getattr(comp, 'message', ''))})
                except Exception as exc:
                    await self.runtime.run_storage_task(self.runtime.storage.append_event, event_type='AUTO_COMPRESS_FAIL', bucket_id=locked_bucket, payload={'reason': 'auto_threshold', 'error': repr(exc)})
                pressure, count = await self._bucket_pressure(locked_bucket)
            if did_compress and (pressure > self.runtime._auto_split_trigger_ratio or count > 1000):
                if did_split:
                    await self.runtime.run_storage_task(self.runtime.storage.record_auto_split_guard_hit)
                    return
                if split_round >= self.runtime._auto_split_max_round_per_manage:
                    await self.runtime.run_storage_task(self.runtime.storage.record_auto_split_guard_hit)
                    return
                if not await self.topology._can_auto_split_now(bucket_id=locked_bucket):
                    await self.runtime.run_storage_task(self.runtime.storage.record_auto_split_cooldown_skip)
                    return
                result = await self.split._split_bucket_unlocked(bucket_id=locked_bucket, reason='auto_post_compress')
                split_round += 1
                did_split = bool(result.get('success', False))
                if not did_split:
                    await self.runtime.run_storage_task(self.runtime.storage.record_auto_split_guard_hit)
                    return
            if did_compress and did_split:
                return

    async def _bucket_pressure(self, bucket_id: str) -> tuple[float, int]:
        usage, count = await asyncio.gather(
            self.read.get_context_usage(
                bucket_id,
                allow_fallback=False,
                resolve_bucket=False,
            ),
            self.read.count_direct_records(bucket_id, include_gray=False),
        )
        est = usage.context_tokens
        return (est / max(1, self.runtime.max_context_window), count)
