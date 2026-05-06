from __future__ import annotations

from datetime import datetime, timezone

from ..models import CleanupResult, EngineStats, parse_iso_or_none
from .runtime import ServiceRuntime


class MaintenanceService:
    def __init__(self, runtime: ServiceRuntime) -> None:
        self.runtime = runtime

    async def cleanup_expired(self) -> CleanupResult:
        eng = self.runtime.engine
        changed = 0
        records = eng.storage.list_latest_records(include_gray=True)
        now = datetime.now(timezone.utc)
        for rec in records:
            if rec.gray:
                continue
            expiry = parse_iso_or_none(rec.expires_at)
            if expiry is None:
                continue
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)
            if expiry > now:
                continue
            result = await eng.set_gray(rec.key, gray=True, reason="expire")
            if result.success:
                changed += 1
        return CleanupResult(success=True, expired_marked=changed, message="cleanup done")

    async def stats(self) -> EngineStats:
        eng = self.runtime.engine
        raw = eng.storage.get_stats()
        root_bucket = eng.root_bucket_id()
        estimated = eng.storage.estimate_bucket_tokens(root_bucket, include_gray=False)
        llm_input = int(raw.get("llm_input_tokens_total", 0))
        llm_cached_input = int(raw.get("llm_cached_input_tokens_total", 0))
        llm_hit_rate = (llm_cached_input / llm_input) if llm_input > 0 else 0.0

        memory_cache_bytes = int(eng.memory_manager.total_bytes() + eng.bm25_cache.estimate_memory_bytes())
        mem_diag = eng.memory_manager.diagnostics()
        return EngineStats(
            total_keys=int(raw.get("total_keys", 0)),
            active_keys=int(raw.get("active_keys", 0)),
            gray_keys=int(raw.get("gray_keys", 0)),
            revision_total=int(raw.get("revision_total", 0)),
            event_total=int(raw.get("event_total", 0)),
            cache_entries=int(raw.get("cache_entries", 0)),
            dirty=bool(raw.get("dirty", False)),
            context_version=int(raw.get("context_version", 0)),
            estimated_tokens=estimated,
            latest_snapshot=str(raw.get("latest_snapshot", "")),
            llm_calls_total=int(raw.get("llm_calls_total", 0)),
            llm_input_tokens_total=llm_input,
            llm_output_tokens_total=int(raw.get("llm_output_tokens_total", 0)),
            llm_cached_input_tokens_total=llm_cached_input,
            llm_cache_hit_rate_global=llm_hit_rate,
            degraded_query_total=int(raw.get("degraded_query_total", 0)),
            llm_parse_fail_total=int(raw.get("llm_parse_fail_total", 0)),
            llm_precheck_fail_total=int(raw.get("llm_precheck_fail_total", 0)),
            clean_reject_total=int(raw.get("clean_reject_total", 0)),
            clean_fallback_total=int(raw.get("clean_fallback_total", 0)),
            ingest_blocked_by_clean_total=int(raw.get("ingest_blocked_by_clean_total", 0)),
            root_bucket_id=str(raw.get("root_bucket_id", "")),
            active_bucket_id=str(raw.get("active_bucket_id", "")),
            bucket_total=int(raw.get("bucket_total", 0)),
            memory_cache_bytes=memory_cache_bytes,
            aggressive_memory_mode=bool(eng.memory_manager.aggressive_mode),
            memory_idle_evictions_total=int(mem_diag.get("idle_evictions_total", 0)),
            memory_pressure_evictions_total=int(mem_diag.get("pressure_evictions_total", 0)),
            memory_cleanup_runs_total=int(mem_diag.get("cleanup_runs_total", 0)),
            memory_aggressive_enters_total=int(mem_diag.get("aggressive_enters_total", 0)),
            memory_aggressive_seconds_total=float(mem_diag.get("aggressive_seconds_total", 0.0)),
            context_overflow_total=int(raw.get("context_overflow_total", 0)),
            overflow_query_total=int(raw.get("overflow_query_total", 0)),
            overflow_ingest_total=int(raw.get("overflow_ingest_total", 0)),
            overflow_compress_total=int(raw.get("overflow_compress_total", 0)),
            file_import_reject_total=int(raw.get("file_import_reject_total", 0)),
            auto_split_guard_hit_total=int(raw.get("auto_split_guard_hit_total", 0)),
            auto_split_cooldown_skip_total=int(raw.get("auto_split_cooldown_skip_total", 0)),
            auto_split_no_progress_total=int(raw.get("auto_split_no_progress_total", 0)),
            split_plan_warn_total=int(raw.get("split_plan_warn_total", 0)),
            query_alias_miss_build_total=int(raw.get("query_alias_miss_build_total", 0)),
            query_alias_miss_resolve_total=int(raw.get("query_alias_miss_resolve_total", 0)),
            query_side_effect_drop_total=int(raw.get("query_side_effect_drop_total", 0)),
        )
