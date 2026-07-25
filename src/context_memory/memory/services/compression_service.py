from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Callable

from ..models import CompressResult, MemoryRecord, normalize_relations

if TYPE_CHECKING:
    from ..engine_runtime import EngineRuntime


def _clamp_score(value: float) -> float:
    return max(0.0, min(1.0, float(value)))

class CompressionService:
    def __init__(self, runtime: "EngineRuntime", *, alias: Callable[[], Any], forgetting: Callable[[], Any], maintenance: Callable[[], Any], primitives: Callable[[], Any], split: Callable[[], Any], summary: Callable[[], Any], topology: Callable[[], Any]) -> None:
        self.runtime = runtime
        self._alias_provider = alias
        self._forgetting_provider = forgetting
        self._maintenance_provider = maintenance
        self._primitives_provider = primitives
        self._split_provider = split
        self._summary_provider = summary
        self._topology_provider = topology

    @property
    def alias(self) -> Any:
        return self._alias_provider()

    @property
    def forgetting(self) -> Any:
        return self._forgetting_provider()

    @property
    def maintenance(self) -> Any:
        return self._maintenance_provider()

    @property
    def primitives(self) -> Any:
        return self._primitives_provider()

    @property
    def split(self) -> Any:
        return self._split_provider()

    @property
    def summary(self) -> Any:
        return self._summary_provider()

    @property
    def topology(self) -> Any:
        return self._topology_provider()

    async def force_compress(
        self,
        *,
        reason: str = 'manual',
        bucket_id: str | None = None,
    ) -> CompressResult:
        self.alias.begin_session()
        try:
            async with self.topology._bucket_write_lock(bucket_id) as resolved:
                result = await self._force_compress_unlocked(
                    bucket_id=resolved,
                    reason=reason,
                )
                await self.maintenance._run_memory_gc()
            return result
        finally:
            self.alias.end_session(flush=True)

    async def _force_compress_unlocked(self, *, bucket_id: str, reason: str) -> CompressResult:
        source = self.runtime.storage.get_bucket_info(bucket_id)
        if source is None:
            return CompressResult(success=False, message=f'bucket not found: {bucket_id}')
        if source.sealed:
            return CompressResult(success=False, message='sealed bucket is read-only')
        latest_all = await self.runtime.run_storage_task(self.runtime.storage.load_bucket_snapshot, bucket_id, include_gray=True)
        latest = [r for r in latest_all if not r.gray]
        if not latest:
            return CompressResult(success=True, message='bucket is empty')
        records = [r.to_dict() for r in latest_all]
        prepared_alias, map_ver = await self.alias._prepare_alias_payload_with_version(bucket_id, {'records': records})
        alias_records = prepared_alias.get('records', [])
        payload_base = {'reason': reason, 'max_context_window': self.runtime.max_context_window, 'records': alias_records}
        payload_tokens = await asyncio.to_thread(self.runtime.token_counter.count_json_with_token_field, payload_base)
        compress_alias_payload = {'reason': reason, 'payload_tokens': payload_tokens, 'max_context_window': self.runtime.max_context_window, 'records': alias_records}
        await self.alias._assert_alias_payload_safe(bucket_id, compress_alias_payload)
        plan_alias = await self.runtime.pipeline.compress(bucket_context=await self.runtime.bucket_context(bucket_id), records=alias_records, reason=reason, payload_tokens=payload_tokens, max_context_window=self.runtime.max_context_window)
        await self.alias.audit_llm_call(tool='compress', bucket_id=bucket_id, map_version=map_ver, alias_input=compress_alias_payload, alias_output=plan_alias)
        plan = await self.alias.restore_alias_payload(bucket_id, plan_alias, map_version=map_ver)
        await self.runtime.record_llm_usage()
        await self.runtime.record_llm_diag()
        if self.runtime.is_context_overflow_diag(self.runtime.pipeline.last_diagnostics):
            await self.runtime.record_overflow(stage='compress')
        drop_keys = [str(k) for k in plan.get('drop_keys', []) if str(k).strip()]
        drop_set = set(drop_keys)
        key_to_record = {r.key: r for r in latest}
        all_keys = set(key_to_record.keys())
        keep_set = set(all_keys)
        keep_set -= drop_set
        survivors: dict[str, MemoryRecord] = {}
        for k in keep_set:
            rec = key_to_record.get(k)
            if rec is not None:
                survivors[k] = rec
        reweighted = plan.get('reweighted', [])
        content_updates = plan.get('content_updates', [])
        reweighted_count = 0
        rewritten_count = 0
        changed = 0
        dropped = 0
        if isinstance(reweighted, list):
            for item in reweighted:
                if not isinstance(item, dict):
                    continue
                key = str(item.get('key', '')).strip()
                rec = survivors.get(key)
                if rec is None:
                    continue
                try:
                    new_weight = float(item.get('weight', rec.weight))
                except (TypeError, ValueError):
                    continue
                new_weight = _clamp_score(new_weight)
                if abs(new_weight - float(rec.weight)) < 1e-06:
                    continue
                survivors[key] = replace(rec, weight=new_weight)
                changed += 1
                reweighted_count += 1
        allowed_rewrite_reasons = {'conflict', 'outdated', 'duplicate_merge'}
        if isinstance(content_updates, list):
            for item in content_updates:
                if not isinstance(item, dict):
                    continue
                key = str(item.get('key', '')).strip()
                new_content = str(item.get('content', '')).strip()
                rewrite_reason = str(item.get('reason', '')).strip().lower()
                if not key or not new_content or rewrite_reason not in allowed_rewrite_reasons:
                    continue
                rec = survivors.get(key)
                if rec is None:
                    continue
                new_hash = hashlib.sha1(new_content.encode('utf-8')).hexdigest()
                survivors[key] = replace(rec, content=new_content, source_hash=new_hash)
                changed += 1
                rewritten_count += 1
        evidence_status = await self.runtime.run_storage_task(self.runtime.storage.evidence_exists_many, [rec.evidence_ref for rec in survivors.values() if rec.evidence_ref])
        for key, rec in list(survivors.items()):
            if rec.evidence_ref and (not evidence_status.get(rec.evidence_ref, False)):
                survivors.pop(key, None)
                drop_set.add(key)
        for key in all_keys:
            if key not in survivors:
                dropped += 1
        snapshot_path = await self.runtime.run_storage_task(self.runtime.storage.create_snapshot, summary=str(plan.get('merged_summary', '')), bucket_id=bucket_id, reason=reason, keep_keys=sorted(survivors.keys()), drop_keys=sorted(set(all_keys) - set(survivors.keys())))
        est_after_tokens = await asyncio.to_thread(self.runtime.token_counter.count_json, [rec.to_dict() for rec in survivors.values()])
        if est_after_tokens / max(1, self.runtime.max_context_window) > self.runtime._auto_split_trigger_ratio:
            split_res = await self.split._split_bucket_unlocked(bucket_id=bucket_id, reason='compress_over_threshold_split')
            return CompressResult(success=bool(split_res.get('success', False)), changed=changed, dropped=dropped, reweighted=reweighted_count, rewritten=rewritten_count, message='compress estimated overflow; split executed')
        successor = await self.topology._create_successor_bucket_shallow_unlocked(source_bucket_id=bucket_id, title=f'{source.title}_compress', summary=str(plan.get('merged_summary', '')).strip() or source.summary or 'compressed successor')
        for rec in survivors.values():
            await self.primitives._write_rebuilt_record_unlocked(source_record=rec, dst_bucket_id=successor.bucket_id, event='COMPRESS_REBUILD', reason=reason)
        successor_info = self.runtime.storage.get_bucket_info(successor.bucket_id)
        if successor_info is not None:
            merged_summary = str(plan.get('merged_summary', '')).strip()
            if merged_summary:
                successor_info.summary = merged_summary[:140]
                successor_info.summary_status = 'ready'
                await self.runtime.run_storage_task(self.runtime.storage.update_bucket_info, successor_info)
                await self.summary._append_bucket_summary_update_event_unlocked(info=successor_info, summary=successor_info.summary, content=merged_summary[:1000], reason=f'compress:{reason}')
        await self.topology._seal_and_switch_bucket_unlocked(source_bucket_id=bucket_id, successor_bucket_id=successor.bucket_id, reason=reason)
        for key in set(all_keys) - set(survivors.keys()):
            await self.runtime.run_storage_task(self.runtime.storage.purge_evidence_for_key, key)
        await self.runtime.run_storage_task(self.runtime.storage.append_event, event_type='COMPRESS_DONE', bucket_id=bucket_id, payload={'reason': reason, 'snapshot_path': snapshot_path, 'drop_count': dropped, 'keep_count': len(survivors), 'changed': changed, 'successor_bucket_id': successor.bucket_id, 'rebuild_mode': True})
        await self.forgetting._apply_forgetting(successor.bucket_id, from_compress=True)
        if not self.summary._should_skip_auto_summary(successor_info):
            await self.summary._refresh_bucket_summary_unlocked(bucket_id=successor.bucket_id, force=False, reason='auto_after_compress')
        return CompressResult(success=True, changed=changed, dropped=dropped, reweighted=reweighted_count, rewritten=rewritten_count, message='compressed via successor rebuild')

    async def _compress_remove_missing_evidence(self, bucket_id: str) -> int:
        changed = 0
        latest = await self.runtime.run_storage_task(self.runtime.storage.load_bucket_snapshot, bucket_id, include_gray=False)
        evidence_status = await self.runtime.run_storage_task(self.runtime.storage.evidence_exists_many, [rec.evidence_ref for rec in latest if rec.evidence_ref])
        for rec in latest:
            if rec.gray:
                continue
            if not rec.evidence_ref:
                continue
            if evidence_status.get(rec.evidence_ref, False):
                continue
            relations = normalize_relations(rec.relations)
            relations['lifecycle_links'].append({'target': rec.revision_id, 'type': 'tombstones', 'score': 1.0, 'note': 'missing_evidence'})
            tomb = MemoryRecord(key=rec.key, revision_id=self.runtime.storage.generate_revision_id(), kind=rec.kind, bucket_id=rec.bucket_id, title=rec.title, summary=f'{rec.summary[:220]} [GRAY_SET:MISSING_EVIDENCE]', content=rec.content, weight=rec.weight, event='GRAY_SET', gray=True, relations=relations, evidence_ref=rec.evidence_ref, expires_at=rec.expires_at, source_hash=rec.source_hash, child_bucket_id=rec.child_bucket_id, confidence_type=rec.confidence_type)
            await self.runtime.run_storage_task(self.runtime.storage.write_memory_record, tomb)
            await self.primitives._append_context_event(bucket_id=bucket_id, event_type='GRAY_SET', record=tomb, payload={'reason': 'missing_evidence_after_compress', 'from_revision': rec.revision_id})
            changed += 1
        return changed
