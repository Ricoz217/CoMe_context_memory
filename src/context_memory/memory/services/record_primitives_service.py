from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable

from ..models import (
    BUCKET_KIND_BUCKET,
    BUCKET_KIND_MEMORY,
    MemoryRecord,
    UpdateResult,
    normalize_relations,
)

if TYPE_CHECKING:
    from ..engine_runtime import EngineRuntime


def _clamp_score(value: float) -> float:
    return max(0.0, min(1.0, float(value)))

class RecordPrimitivesService:
    def __init__(self, runtime: "EngineRuntime", *, alias: Callable[[], Any]) -> None:
        self.runtime = runtime
        self._alias_provider = alias

    @property
    def alias(self) -> Any:
        return self._alias_provider()

    @staticmethod
    def _append_relation_once(rel_list: list[dict[str, Any]], *, target: str, rel_type: str, score: float, note: str) -> None:
        for item in rel_list:
            if str(item.get('target', '')) == target and str(item.get('type', '')) == rel_type:
                return
        rel_list.append({'target': target, 'type': rel_type, 'score': max(0.0, min(1.0, float(score))), 'note': note})

    async def _append_context_event(self, *, bucket_id: str, event_type: str, record: MemoryRecord, payload: dict[str, Any] | None=None) -> None:
        await self.runtime.run_storage_task(self._append_context_event_sync, bucket_id=bucket_id, event_type=event_type, record=record, payload=payload)
        self.runtime.invalidate_bucket_context_cache(bucket_id)

    def _append_context_event_sync(self, *, bucket_id: str, event_type: str, record: MemoryRecord, payload: dict[str, Any] | None=None) -> None:
        if payload is None:
            payload = {}
        event_key = str(record.key or '').strip()
        if record.kind == BUCKET_KIND_BUCKET:
            child_node_key = str(record.child_bucket_id or '').strip()
            if child_node_key:
                event_key = child_node_key
        if str(event_type).upper() == 'GRAY_SET':
            min_payload: dict[str, Any] = {}
            reason = payload.get('reason')
            from_revision = payload.get('from_revision')
            if reason is not None:
                min_payload['reason'] = reason
            if from_revision is not None:
                min_payload['from_revision'] = from_revision
            payload = min_payload
            event = {'event_type': event_type, 'bucket_id': bucket_id, 'key': event_key, 'revision_id': record.revision_id, 'kind': record.kind, 'event': record.event, 'gray': record.gray, 'confidence_type': str(record.confidence_type or 'common'), 'created_at': record.created_at, 'payload': payload}
        else:
            event = {'event_type': event_type, 'bucket_id': bucket_id, 'key': event_key, 'revision_id': record.revision_id, 'kind': record.kind, 'title': record.title, 'summary': record.summary, 'content': record.content, 'weight': record.weight, 'gray': record.gray, 'confidence_type': str(record.confidence_type or 'common'), 'relations': record.relations, 'evidence_ref': record.evidence_ref, 'expires_at': record.expires_at, 'created_at': record.created_at, 'child_bucket_id': record.child_bucket_id, 'payload': payload}
        alias_table = self.alias._alias_table(bucket_id)
        alias_event = alias_table.encode_tree(event)
        alias_table.assert_safe(alias_event)
        self.runtime.storage.append_bucket_event(bucket_id, alias_event)
        self.runtime.storage.append_event(event_type=event_type, bucket_id=bucket_id, key=record.key, revision_id=record.revision_id, payload=payload)
        try:
            self.runtime.storage.touch_bucket_last_event_at(bucket_id=bucket_id, event_ts=datetime.now(timezone.utc).timestamp())
        except Exception:
            pass

    def _build_record(self, *, key: str, event: str, ingested: dict[str, Any], bucket_id: str, evidence_ref: str, kind: str=BUCKET_KIND_MEMORY, child_bucket_id: str='') -> MemoryRecord:
        content = str(ingested.get('content', ''))
        relations = normalize_relations(ingested.get('relations', {}))
        source_hash = hashlib.sha1(content.encode('utf-8')).hexdigest()
        return MemoryRecord(key=key, revision_id=self.runtime.storage.generate_revision_id(), kind=kind, bucket_id=bucket_id, title=str(ingested.get('title', '')).strip() or key, summary=str(ingested.get('summary', '')).strip()[:300], content=content, weight=max(0.0, min(1.0, float(ingested.get('weight', 0.5)))), event=str(ingested.get('event', event)), gray=bool(ingested.get('gray', False)), relations=relations, evidence_ref=evidence_ref, expires_at=ingested.get('expires_at'), source_hash=source_hash, child_bucket_id=child_bucket_id, confidence_type=str(ingested.get('confidence_type', 'common') or 'common'))

    async def set_gray_unlocked(
        self,
        current: MemoryRecord,
        *,
        gray: bool,
        reason: str,
    ) -> UpdateResult:
        if current.gray == gray:
            return UpdateResult(
                success=True,
                key=current.key,
                revision_id=current.revision_id,
                message='gray already set',
            )
        event = 'GRAY_SET' if gray else 'GRAY_CLEAR'
        relations = normalize_relations(current.relations)
        relations['lifecycle_links'].append(
            {
                'target': current.revision_id,
                'type': 'revises',
                'score': 1.0,
                'note': 'manual gray set' if gray else 'manual gray clear',
            }
        )
        record = MemoryRecord(
            key=current.key,
            revision_id=self.runtime.storage.generate_revision_id(),
            kind=current.kind,
            bucket_id=current.bucket_id,
            title=current.title,
            summary=current.summary,
            content=current.content,
            weight=current.weight,
            event=event,
            gray=gray,
            relations=relations,
            evidence_ref=current.evidence_ref,
            expires_at=current.expires_at,
            source_hash=current.source_hash,
            child_bucket_id=current.child_bucket_id,
            confidence_type=current.confidence_type,
        )
        await self.runtime.run_storage_task(self.runtime.storage.write_memory_record, record)
        await self._append_context_event(
            bucket_id=current.bucket_id,
            event_type=event,
            record=record,
            payload={'from_revision': current.revision_id, 'reason': reason},
        )
        return UpdateResult(
            success=True,
            key=current.key,
            revision_id=record.revision_id,
            message='gray state updated',
        )

    async def _write_rebuilt_record_unlocked(self, *, source_record: MemoryRecord, dst_bucket_id: str, event: str, reason: str) -> MemoryRecord:
        if source_record.kind == BUCKET_KIND_BUCKET and str(source_record.child_bucket_id or '').strip():
            await self.runtime.run_storage_task(self.runtime.storage.reparent_bucket, bucket_id=str(source_record.child_bucket_id).strip(), new_parent_bucket_id=dst_bucket_id, preserve_old_title_map=True)
        rel = normalize_relations(source_record.relations)
        self._append_relation_once(rel['lifecycle_links'], target=source_record.revision_id, rel_type='supersedes', score=1.0, note=event.lower())
        in_rec = MemoryRecord(key=source_record.key, revision_id=self.runtime.storage.generate_revision_id(), kind=source_record.kind, bucket_id=dst_bucket_id, title=source_record.title, summary=source_record.summary, content=source_record.content, weight=source_record.weight, event=event, gray=False, relations=rel, evidence_ref=source_record.evidence_ref, expires_at=source_record.expires_at, source_hash=source_record.source_hash, child_bucket_id=source_record.child_bucket_id, confidence_type=source_record.confidence_type)
        await self.runtime.run_storage_task(self.runtime.storage.write_memory_record, in_rec)
        await self._append_context_event(bucket_id=dst_bucket_id, event_type=event, record=in_rec, payload={'from_bucket': source_record.bucket_id, 'from_revision': source_record.revision_id, 'reason': reason})
        return in_rec
