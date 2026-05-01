from __future__ import annotations

from ..models import utc_now_iso
from .runtime import ServiceRuntime


class SplitIngestJobService:
    def __init__(self, runtime: ServiceRuntime) -> None:
        self.runtime = runtime

    async def resume_pending_jobs(self) -> dict[str, object]:
        eng = self.runtime.engine
        jobs = eng.storage.list_job_journals(statuses={"running", "paused"})
        results: list[dict[str, object]] = []
        for job in jobs:
            try:
                result = await eng._resume_split_job_unlocked(job)
            except Exception as exc:
                batch_id = str(job.get("batch_id", "")).strip()
                eng.storage.save_job_journal(
                    {
                        **job,
                        "batch_id": batch_id,
                        "status": "paused",
                        "message": f"resume exception: {exc}",
                        "updated_at": utc_now_iso(),
                    }
                )
                result = {"batch_id": batch_id, "success": False, "message": f"resume exception: {exc}"}
            results.append(result)

        completed = sum(1 for x in results if bool(x.get("success", False)))
        failed = len(results) - completed
        return {
            "success": failed == 0,
            "total_jobs": len(results),
            "completed_jobs": completed,
            "failed_jobs": failed,
            "jobs": results,
        }
