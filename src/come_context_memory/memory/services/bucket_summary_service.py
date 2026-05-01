from __future__ import annotations

from .runtime import ServiceRuntime


class BucketSummaryService:
    def __init__(self, runtime: ServiceRuntime) -> None:
        self.runtime = runtime

    async def refresh_bucket_summary(self, bucket_id: str, *, force: bool = False) -> dict[str, object]:
        eng = self.runtime.engine
        result = await eng._refresh_bucket_summary_unlocked(bucket_id=bucket_id, force=force, reason="manual")
        await eng._run_memory_gc()
        return result
