from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any, Callable

from ..models import BucketInfo, MemoryRecord, normalize_relations

if TYPE_CHECKING:
    from ..engine_runtime import EngineRuntime


def _clamp_score(value: float) -> float:
    return max(0.0, min(1.0, float(value)))

class BucketSummaryService:
    def __init__(self, runtime: "EngineRuntime", *, alias: Callable[[], Any], maintenance: Callable[[], Any], primitives: Callable[[], Any], topology: Callable[[], Any]) -> None:
        self.runtime = runtime
        self._alias_provider = alias
        self._maintenance_provider = maintenance
        self._primitives_provider = primitives
        self._topology_provider = topology

    @property
    def alias(self) -> Any:
        return self._alias_provider()

    @property
    def maintenance(self) -> Any:
        return self._maintenance_provider()

    @property
    def primitives(self) -> Any:
        return self._primitives_provider()

    @property
    def topology(self) -> Any:
        return self._topology_provider()

    async def refresh_bucket_summary(
        self,
        bucket_id: str,
        *,
        force: bool = False,
    ) -> dict[str, Any]:
        async with self.topology._bucket_write_lock(bucket_id) as resolved:
            result = await self._refresh_bucket_summary_unlocked(
                bucket_id=resolved,
                force=force,
                reason='manual',
            )
            await self.maintenance._run_memory_gc()
        return result

    def _should_skip_auto_summary(self, info: BucketInfo | None) -> bool:
        if info is None:
            return True
        return bool(getattr(info, 'summary_locked', False))

    async def _refresh_bucket_summary_unlocked(self, *, bucket_id: str, force: bool, reason: str) -> dict[str, Any]:
        info = self.runtime.storage.get_bucket_info(bucket_id)
        if info is None:
            return {'success': False, 'bucket_id': bucket_id, 'updated': False, 'message': 'bucket not found'}
        if info.sealed:
            return {'success': False, 'bucket_id': bucket_id, 'updated': False, 'message': 'sealed bucket is read-only'}
        if info.summary_locked and (not force):
            return {'success': True, 'bucket_id': bucket_id, 'updated': False, 'message': 'summary locked', 'summary_status': info.summary_status}
        records = await self.runtime.run_storage_task(self.runtime.storage.load_bucket_snapshot, bucket_id, include_gray=False)
        if not records:
            if info.summary != self.runtime._pending_bucket_summary or info.summary_status != 'pending':
                info.summary = self.runtime._pending_bucket_summary
                info.summary_status = 'pending'
                await self.runtime.run_storage_task(self.runtime.storage.update_bucket_info, info)
                await self._append_bucket_summary_update_event_unlocked(info=info, summary=info.summary, content=info.summary, reason=f'{reason}:pending')
            return {'success': True, 'bucket_id': bucket_id, 'updated': False, 'message': 'bucket has no active memories', 'summary_status': info.summary_status}
        prepared_alias, map_ver = await self.alias._prepare_alias_payload_with_version(bucket_id, {'records': [r.to_dict() for r in records]})
        alias_records = prepared_alias.get('records', [])
        summary_alias_payload = {'records': alias_records, 'reason': reason}
        await self.alias._assert_alias_payload_safe(bucket_id, summary_alias_payload)
        summary_out_alias = await self.runtime.pipeline.summarize_bucket(records=alias_records, reason=reason)
        await self.alias.audit_llm_call(tool='bucket_summary', bucket_id=bucket_id, map_version=map_ver, alias_input=summary_alias_payload, alias_output=summary_out_alias)
        summary_out = await self.alias.restore_alias_payload(bucket_id, summary_out_alias, map_version=map_ver)
        await self.runtime.record_llm_usage()
        await self.runtime.record_llm_diag()
        if self.runtime.is_context_overflow_diag(self.runtime.pipeline.last_diagnostics):
            await self.runtime.record_overflow(stage='compress')
        new_summary = str(summary_out.get('summary', '')).strip()[:140] or info.summary
        new_content = str(summary_out.get('content', '')).strip()[:1000] or new_summary
        info.summary = new_summary
        info.summary_status = 'ready'
        await self.runtime.run_storage_task(self.runtime.storage.update_bucket_info, info)
        await self._append_bucket_summary_update_event_unlocked(info=info, summary=new_summary, content=new_content, reason=reason)
        return {'success': True, 'bucket_id': bucket_id, 'updated': True, 'message': 'bucket summary refreshed', 'summary_status': info.summary_status}

    async def _append_bucket_summary_update_event_unlocked(self, *, info: BucketInfo, summary: str, content: str, reason: str) -> None:
        if not info.node_key or not info.parent_bucket_id:
            return
        current = await self.runtime.run_storage_task(self.runtime.storage.get_record, info.node_key)
        if current is None or current.gray:
            return
        target_bucket_id = current.bucket_id
        bucket_info = self.runtime.storage.get_bucket_info(target_bucket_id)
        if bucket_info is not None and bucket_info.sealed:
            try:
                resolved_bucket = self.topology._resolve_bucket_id(target_bucket_id)
            except Exception:
                return
            if not resolved_bucket:
                return
            target_bucket_id = resolved_bucket
        relations = normalize_relations(current.relations)
        relations['lifecycle_links'].append({'target': current.revision_id, 'type': 'revises', 'score': 1.0, 'note': f'bucket_summary:{reason}'})
        updated = MemoryRecord(key=current.key, revision_id=self.runtime.storage.generate_revision_id(), kind=current.kind, bucket_id=target_bucket_id, title=info.title or current.title, summary=summary[:300], content=content, weight=current.weight, event='UPDATE', gray=False, relations=relations, evidence_ref=current.evidence_ref, expires_at=current.expires_at, source_hash=hashlib.sha1(content.encode('utf-8')).hexdigest(), child_bucket_id=current.child_bucket_id, confidence_type=current.confidence_type)
        await self.runtime.run_storage_task(self.runtime.storage.write_memory_record, updated)
        await self.primitives._append_context_event(bucket_id=target_bucket_id, event_type='UPDATE', record=updated, payload={'from_revision': current.revision_id, 'reason': f'bucket_summary:{reason}', 'kind': 'bucket'})
