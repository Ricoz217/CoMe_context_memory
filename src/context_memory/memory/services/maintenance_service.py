from __future__ import annotations
import json
import shutil
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable
from uuid import uuid4
from ..models import (
    BUCKET_KIND_BUCKET,
    BucketInfo,
    CleanupResult,
    EngineStats,
    GCResult,
    normalize_relations,
    parse_iso_or_none,
    utc_now_iso,
)
if TYPE_CHECKING:
    from ..engine_runtime import EngineRuntime

def _clamp_score(value: float) -> float:
    return max(0.0, min(1.0, float(value)))

class MaintenanceService:

    def __init__(
        self,
        runtime: 'EngineRuntime',
        *,
        record: Callable[[], Any],
        topology: Callable[[], Any],
    ) -> None:
        self.runtime = runtime
        self._record_provider = record
        self._topology_provider = topology

    @property
    def record(self) -> Any:
        return self._record_provider()

    @property
    def topology(self) -> Any:
        return self._topology_provider()

    def _create_gc_snapshot_unlocked(self, *, reason: str) -> str:
        stamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        snap_dir = self.runtime.storage.snapshots_dir / f'gc_snapshot_{stamp}_{uuid4().hex[:8]}'
        snap_dir.mkdir(parents=True, exist_ok=True)
        for src in (self.runtime.storage.state_file, self.runtime.storage.meta_file, self.runtime.storage.bucket_tree_file, self.runtime.storage.events_file, self.runtime.storage.alias_audit_file):
            if src.exists():
                shutil.copy2(src, snap_dir / src.name)
        marker = {'reason': reason, 'created_at': utc_now_iso()}
        (snap_dir / 'marker.json').write_text(json.dumps(marker, ensure_ascii=False, indent=2), encoding='utf-8')
        return str(snap_dir)

    def _gc_storage_unlocked(self, *, dry_run: bool, reason: str) -> GCResult:
        now = datetime.now(timezone.utc)
        rev_retention = timedelta(days=int(self.runtime._gc_revision_retention_days))
        gray_retention = timedelta(days=int(self.runtime._gc_gray_key_retention_days))
        bucket_retention = timedelta(days=int(self.runtime._gc_archived_bucket_retention_days))
        counts = {'revision': 0, 'key': 0, 'bucket': 0, 'evidence': 0}
        skipped = {'protected': 0, 'referenced': 0}
        errors: list[str] = []
        state = self.runtime.storage.state_snapshot_for_maintenance()
        keys = state.get('keys', {})
        if not isinstance(keys, dict):
            keys = {}
        active_records = [record for record in self.runtime.storage.load_all_records_snapshot(include_gray=True) if not record.gray]
        active_targets: set[str] = set()
        active_child_buckets: set[str] = set()
        for rec in active_records:
            if rec.kind == BUCKET_KIND_BUCKET and str(rec.child_bucket_id or '').strip():
                active_child_buckets.add(str(rec.child_bucket_id).strip())
            rels = normalize_relations(rec.relations)
            for rel_items in rels.values():
                for item in rel_items:
                    tgt = str(item.get('target', '')).strip()
                    if tgt:
                        active_targets.add(tgt)
        snapshot_path = ''
        if not dry_run:
            snapshot_path = self._create_gc_snapshot_unlocked(reason=reason)
        for key, node in list(keys.items()):
            if not isinstance(node, dict):
                continue
            key_dir = self.runtime.storage.memories_dir / str(key)
            latest_rev = str(node.get('latest_revision', '')).strip()
            revision_files = sorted(key_dir.glob('*.json')) if key_dir.exists() else []
            for rf in revision_files:
                rev_id = rf.stem
                if rev_id == latest_rev:
                    continue
                rec = self.runtime.storage._json_to_memory_record(rf)
                created = parse_iso_or_none(rec.created_at) if rec is not None else None
                if created is None:
                    created = datetime.fromtimestamp(rf.stat().st_mtime, tz=timezone.utc)
                if created.tzinfo is None:
                    created = created.replace(tzinfo=timezone.utc)
                if now - created < rev_retention:
                    continue
                counts['revision'] += 1
                if not dry_run:
                    try:
                        rf.unlink(missing_ok=True)
                    except Exception as exc:
                        errors.append(f'revision_delete_failed:{rf}:{exc}')
            if not isinstance(node.get('gray', False), bool):
                node['gray'] = bool(node.get('gray', False))
            if not bool(node.get('gray', False)):
                continue
            if str(key) in active_targets:
                skipped['referenced'] += 1
                continue
            updated = parse_iso_or_none(str(node.get('updated_at', ''))) or parse_iso_or_none(str(node.get('created_at', '')))
            if updated is None:
                continue
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=timezone.utc)
            if now - updated < gray_retention:
                continue
            counts['key'] += 1
            if not dry_run:
                try:
                    if key_dir.exists():
                        shutil.rmtree(key_dir, ignore_errors=True)
                    self.runtime.storage.purge_evidence_for_key(str(key))
                    keys.pop(str(key), None)
                except Exception as exc:
                    errors.append(f'key_delete_failed:{key}:{exc}')
        tree = self.runtime.storage.topology_snapshot()
        buckets_raw = tree.get('buckets', {})
        if not isinstance(buckets_raw, dict):
            buckets_raw = {}
        root_bucket_id = str(tree.get('root_bucket_id', '')).strip()
        active_bucket_id = str(tree.get('active_bucket_id', '')).strip()
        title_maps = tree.get('child_title_maps', {})
        if not isinstance(title_maps, dict):
            title_maps = {}
        protected_buckets = {root_bucket_id, active_bucket_id}
        sealed_successors: set[str] = set()
        for raw in buckets_raw.values():
            if not isinstance(raw, dict):
                continue
            if bool(raw.get('sealed', False)):
                dst = str(raw.get('sealed_to', '')).strip()
                if dst:
                    sealed_successors.add(dst)
        for bucket_id, raw in list(buckets_raw.items()):
            if not isinstance(raw, dict):
                continue
            if bucket_id in protected_buckets or bucket_id in sealed_successors:
                skipped['protected'] += 1
                continue
            info = BucketInfo.from_dict(raw)
            if not (info.sealed and info.archived):
                continue
            if info.children:
                skipped['referenced'] += 1
                continue
            if bucket_id in active_child_buckets:
                skipped['referenced'] += 1
                continue
            updated = parse_iso_or_none(info.updated_at) or parse_iso_or_none(info.created_at)
            if updated is None:
                continue
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=timezone.utc)
            if now - updated < bucket_retention:
                continue
            counts['bucket'] += 1
            if not dry_run:
                try:
                    bdir = self.runtime.storage.buckets_dir / bucket_id
                    if bdir.exists():
                        shutil.rmtree(bdir, ignore_errors=True)
                    buckets_raw.pop(bucket_id, None)
                    title_maps.pop(bucket_id, None)
                    for parent_id, parent_map in list(title_maps.items()):
                        if not isinstance(parent_map, dict):
                            title_maps.pop(parent_id, None)
                            continue
                        cleaned = {title: target for title, target in parent_map.items() if str(target) != bucket_id}
                        if cleaned:
                            title_maps[parent_id] = cleaned
                        else:
                            title_maps.pop(parent_id, None)
                    for p_raw in buckets_raw.values():
                        if isinstance(p_raw, dict):
                            children = p_raw.get('children', [])
                            if isinstance(children, list):
                                p_raw['children'] = [c for c in children if str(c) != bucket_id]
                except Exception as exc:
                    errors.append(f'bucket_delete_failed:{bucket_id}:{exc}')
        referenced_evidence: set[str] = set()
        for node in keys.values():
            if not isinstance(node, dict):
                continue
            latest_ref = str(node.get('latest_evidence_ref', '')).strip()
            if latest_ref:
                referenced_evidence.add(latest_ref)
            hist = node.get('evidence_history', [])
            if isinstance(hist, list):
                for item in hist:
                    ref = str(item).strip()
                    if ref:
                        referenced_evidence.add(ref)
        for p in self.runtime.storage.evidence_dir.rglob('*'):
            if not p.is_file():
                continue
            rel = str(p.relative_to(self.runtime.storage.evidence_dir)).replace('\\', '/')
            if rel in referenced_evidence:
                continue
            counts['evidence'] += 1
            if not dry_run:
                try:
                    p.unlink(missing_ok=True)
                except Exception as exc:
                    errors.append(f'evidence_delete_failed:{rel}:{exc}')
        if not dry_run:
            state['keys'] = keys
            tree['buckets'] = buckets_raw
            tree['child_title_maps'] = title_maps
            self.runtime.storage.commit_maintenance_snapshots(state=state, tree=tree)
        self.runtime.storage.append_event(event_type='GC_STORAGE', bucket_id=self.topology.active_bucket_id(), payload={'dry_run': bool(dry_run), 'reason': reason, 'snapshot_path': snapshot_path, 'counts': counts, 'skipped': skipped, 'errors': errors[:20]})
        if dry_run:
            return GCResult(success=True, dry_run=True, message='gc dry-run done', would_delete=counts, skipped=skipped, errors=errors)
        return GCResult(success=len(errors) == 0, dry_run=False, message='gc done', deleted=counts, skipped=skipped, errors=errors)

    async def _run_memory_gc(self) -> None:
        evicted = self.runtime.memory_manager.cleanup()
        if not evicted:
            self.runtime.bm25_cache.prune_to_limit(approx_limit_bytes=max(64 * 1024 * 1024, self.runtime.memory_manager.max_bytes // 3))
            return
        if self.runtime.memory_manager.aggressive_mode:
            self.runtime.bm25_cache.prune_to_limit(approx_limit_bytes=max(32 * 1024 * 1024, self.runtime.memory_manager.max_bytes // 5))

    async def gc_storage(
        self,
        *,
        dry_run: bool = True,
        reason: str = 'manual_gc',
    ) -> GCResult:
        async with self.runtime._global_meta_lock:
            return await self.runtime.run_storage_task(
                self._gc_storage_unlocked,
                dry_run=dry_run,
                reason=reason,
            )

    async def cleanup_expired(self) -> CleanupResult:
        changed = 0
        now = datetime.now(timezone.utc)
        expired_keys = await self.runtime.run_storage_task(self.runtime.storage.list_expired_active_keys, now)
        for key in expired_keys:
            result = await self.record.set_gray(key, gray=True, reason='expire')
            if result.success:
                changed += 1
        return CleanupResult(success=True, expired_marked=changed, message='cleanup done')

    async def stats(self) -> EngineStats:
        raw = await self.runtime.run_storage_task(self.runtime.storage.get_stats)
        llm_input = int(raw.get('llm_input_tokens_total', 0))
        llm_cached_input = int(raw.get('llm_cached_input_tokens_total', 0))
        llm_hit_rate = llm_cached_input / llm_input if llm_input > 0 else 0.0
        memory_cache_bytes = int(self.runtime.memory_manager.total_bytes() + self.runtime.bm25_cache.estimate_memory_bytes())
        mem_diag = self.runtime.memory_manager.diagnostics()
        return EngineStats(total_keys=int(raw.get('total_keys', 0)), active_keys=int(raw.get('active_keys', 0)), gray_keys=int(raw.get('gray_keys', 0)), revision_total=int(raw.get('revision_total', 0)), event_total=int(raw.get('event_total', 0)), cache_entries=int(raw.get('cache_entries', 0)), dirty=bool(raw.get('dirty', False)), context_version=int(raw.get('context_version', 0)), latest_snapshot=str(raw.get('latest_snapshot', '')), llm_calls_total=int(raw.get('llm_calls_total', 0)), llm_input_tokens_total=llm_input, llm_output_tokens_total=int(raw.get('llm_output_tokens_total', 0)), llm_cached_input_tokens_total=llm_cached_input, llm_cache_hit_rate_global=llm_hit_rate, degraded_query_total=int(raw.get('degraded_query_total', 0)), llm_parse_fail_total=int(raw.get('llm_parse_fail_total', 0)), llm_precheck_fail_total=int(raw.get('llm_precheck_fail_total', 0)), clean_reject_total=int(raw.get('clean_reject_total', 0)), clean_fallback_total=int(raw.get('clean_fallback_total', 0)), ingest_blocked_by_clean_total=int(raw.get('ingest_blocked_by_clean_total', 0)), root_bucket_id=str(raw.get('root_bucket_id', '')), active_bucket_id=str(raw.get('active_bucket_id', '')), bucket_total=int(raw.get('bucket_total', 0)), memory_cache_bytes=memory_cache_bytes, aggressive_memory_mode=bool(self.runtime.memory_manager.aggressive_mode), memory_idle_evictions_total=int(mem_diag.get('idle_evictions_total', 0)), memory_pressure_evictions_total=int(mem_diag.get('pressure_evictions_total', 0)), memory_cleanup_runs_total=int(mem_diag.get('cleanup_runs_total', 0)), memory_aggressive_enters_total=int(mem_diag.get('aggressive_enters_total', 0)), memory_aggressive_seconds_total=float(mem_diag.get('aggressive_seconds_total', 0.0)), context_overflow_total=int(raw.get('context_overflow_total', 0)), overflow_query_total=int(raw.get('overflow_query_total', 0)), overflow_ingest_total=int(raw.get('overflow_ingest_total', 0)), overflow_compress_total=int(raw.get('overflow_compress_total', 0)), file_import_reject_total=int(raw.get('file_import_reject_total', 0)), auto_split_guard_hit_total=int(raw.get('auto_split_guard_hit_total', 0)), auto_split_cooldown_skip_total=int(raw.get('auto_split_cooldown_skip_total', 0)), auto_split_no_progress_total=int(raw.get('auto_split_no_progress_total', 0)), split_plan_warn_total=int(raw.get('split_plan_warn_total', 0)), query_alias_miss_build_total=int(raw.get('query_alias_miss_build_total', 0)), query_alias_miss_resolve_total=int(raw.get('query_alias_miss_resolve_total', 0)), query_side_effect_drop_total=int(raw.get('query_side_effect_drop_total', 0)))
