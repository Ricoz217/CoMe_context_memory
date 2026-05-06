from __future__ import annotations

from ..models import CompressResult
from .runtime import ServiceRuntime


class CompressSplitService:
    def __init__(self, runtime: ServiceRuntime) -> None:
        self.runtime = runtime

    async def force_compress(self, *, reason: str = "manual", bucket_id: str | None = None) -> CompressResult:
        eng = self.runtime.engine
        bucket = eng._resolve_bucket_id(bucket_id)
        result = await eng._force_compress_unlocked(bucket_id=bucket, reason=reason)
        await eng._run_memory_gc()
        return result

    async def split_bucket(
        self,
        bucket_id: str,
        *,
        reason: str = "manual_split",
        target_groups_min: int = 2,
        target_groups_max: int = 10,
    ) -> dict[str, object]:
        eng = self.runtime.engine
        result = await eng._split_bucket_unlocked(
            bucket_id=eng._resolve_bucket_id(bucket_id),
            reason=reason,
            target_groups_min=target_groups_min,
            target_groups_max=target_groups_max,
        )
        await eng._run_memory_gc()
        return result
