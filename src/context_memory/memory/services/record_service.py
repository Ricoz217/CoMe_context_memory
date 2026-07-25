from __future__ import annotations

import yaml
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Callable

from ..models import (
    BUCKET_KIND_BUCKET,
    BUCKET_KIND_MEMORY,
    BucketInfo,
    DeleteResult,
    MemoryRecord,
    MoveResult,
    UpdateResult,
    normalize_relations,
)

if TYPE_CHECKING:
    from ..engine_runtime import EngineRuntime


def _clamp_score(value: float) -> float:
    return max(0.0, min(1.0, float(value)))

class RecordService:
    def __init__(self, runtime: "EngineRuntime", *, alias: Callable[[], Any], governance: Callable[[], Any], ingest: Callable[[], Any], maintenance: Callable[[], Any], primitives: Callable[[], Any], topology: Callable[[], Any]) -> None:
        self.runtime = runtime
        self._alias_provider = alias
        self._governance_provider = governance
        self._ingest_provider = ingest
        self._maintenance_provider = maintenance
        self._primitives_provider = primitives
        self._topology_provider = topology

    @property
    def alias(self) -> Any:
        return self._alias_provider()

    @property
    def governance(self) -> Any:
        return self._governance_provider()

    @property
    def ingest(self) -> Any:
        return self._ingest_provider()

    @property
    def maintenance(self) -> Any:
        return self._maintenance_provider()

    @property
    def primitives(self) -> Any:
        return self._primitives_provider()

    @property
    def topology(self) -> Any:
        return self._topology_provider()

    async def get_memory(self, key: str, *, with_evidence: bool=False, revision: str | None=None) -> MemoryRecord | None:
        rec = await self.runtime.run_storage_task(self.runtime.storage.get_record, key, revision)
        if rec is None:
            return None
        if with_evidence and rec.evidence_ref:
            evidence_content = await self.runtime.run_storage_task(self.runtime.storage.read_evidence, rec.evidence_ref)
            return replace(rec, evidence_content=evidence_content)
        return rec

    async def export_memory_to_markdown(self, memory_id: str) -> dict[str, Any]:
        key = str(memory_id or '').strip()
        if not key:
            return {'success': False, 'memory_id': key, 'path': '', 'message': 'memory id is required'}
        if key in {'.', '..'} or '/' in key or '\\' in key:
            return {'success': False, 'memory_id': key, 'path': '', 'message': 'invalid memory id'}
        async with self.runtime._global_meta_lock:
            if self.runtime.storage.get_bucket_info(key) is not None:
                return {'success': False, 'memory_id': key, 'path': '', 'message': 'bucket id is not allowed'}
            rec = await self.runtime.run_storage_task(self.runtime.storage.get_record, key)
            if rec is None:
                return {'success': False, 'memory_id': key, 'path': '', 'message': 'memory id not found'}
            out_path = self.runtime.base_dir / 'exports' / 'memory_md' / f'{key}.md'
            metadata = rec.to_dict()
            body = str(metadata.pop('content', '') or '')
            frontmatter = yaml.safe_dump(metadata, allow_unicode=True, sort_keys=False).strip()
            markdown_text = f'---\n{frontmatter}\n---\n\n{body}'
            await self.runtime.run_storage_task(self.runtime.write_text_file, out_path, markdown_text)
        final_path = str(out_path.resolve())
        print(final_path)
        return {'success': True, 'memory_id': key, 'path': final_path, 'message': 'markdown exported'}

    async def get_evidence_content(self, key: str, *, revision: str | None=None) -> str:
        return await self.runtime.run_storage_task(self.runtime.storage.get_evidence_content_by_key, key, revision)

    async def update_memory(self, key: str, patch_text: str, *, evidence_path: str | None=None) -> UpdateResult:
        self.alias.begin_session()
        try:
            post_manage_bucket: str | None = None
            result: UpdateResult | None = None
            current0 = await self.runtime.run_storage_task(self.runtime.storage.get_record, key)
            if current0 is None:
                return UpdateResult(success=False, key=key, message='memory key not found')
            async with self.topology._bucket_write_lock(current0.bucket_id):
                current = await self.runtime.run_storage_task(self.runtime.storage.get_record, key)
                if current is None:
                    return UpdateResult(success=False, key=key, message='memory key not found')
                if current.kind != BUCKET_KIND_MEMORY:
                    return UpdateResult(success=False, key=key, message='bucket node cannot be updated as memory')
                evidence_ref = current.evidence_ref
                evidence_text = ''
                if evidence_path:
                    evidence_ref = await self.runtime.run_storage_task(self.runtime.storage.copy_evidence, evidence_path, key=key)
                    evidence_text = await self.runtime.run_storage_task(self.runtime.storage.read_evidence, evidence_ref)
                elif evidence_ref:
                    evidence_text = await self.runtime.run_storage_task(self.runtime.storage.read_evidence, evidence_ref)
                clean_result = await self.runtime.pipeline.clean(raw_text=patch_text, evidence_text=evidence_text)
                await self.runtime.record_llm_usage()
                await self.runtime.record_llm_diag()
                diag = self.runtime.pipeline.last_diagnostics
                if str(diag.get('degraded_reason', '')) == 'clean_fallback':
                    await self.runtime.run_storage_task(self.runtime.storage.record_clean_fallback)
                if not bool(clean_result.get('accept', True)):
                    await self.runtime.run_storage_task(self.runtime.storage.record_clean_reject)
                    await self.runtime.run_storage_task(self.runtime.storage.record_ingest_blocked_by_clean)
                    reason = str(clean_result.get('reject_reason', '')).strip() or 'clean rejected input'
                    return UpdateResult(success=False, key=key, message=f'memory update rejected: {reason}')
                clean_type = str(clean_result.get('input_type', '')).strip().lower()
                preserve_literal = bool(clean_result.get('preserve_literal', False)) or clean_type == 'source_code'
                skip_clean = bool(clean_result.get('skip_clean', False)) or preserve_literal
                ingest_input = patch_text if skip_clean else str(clean_result.get('clean_text', '')).strip() or patch_text
                ingested = await self.ingest._ingest_with_overflow_retry(pipeline=self.runtime.pipeline, bucket_id=current.bucket_id, ingest_kwargs={'bucket_context': await self.runtime.bucket_context(current.bucket_id), 'key': key, 'event': 'UPDATE', 'raw_text': ingest_input, 'evidence_text': evidence_text, 'topic': '', 'input_type': clean_type, 'skip_clean': skip_clean, 'preserve_literal': preserve_literal, 'previous_record': current.to_dict()})
                relations = normalize_relations(ingested.get('relations', {}))
                relations['lifecycle_links'].append({'target': current.revision_id, 'type': 'supersedes', 'score': 1.0, 'note': 'auto lifecycle relation'})
                ingested['relations'] = relations
                record = self.primitives._build_record(key=key, event='UPDATE', ingested=ingested, bucket_id=current.bucket_id, evidence_ref=evidence_ref, kind=current.kind, child_bucket_id=current.child_bucket_id)
                await self.runtime.run_storage_task(self.runtime.storage.write_memory_record, record)
                await self.primitives._append_context_event(bucket_id=current.bucket_id, event_type='UPDATE', record=record, payload={'from_revision': current.revision_id})
                post_manage_bucket = current.bucket_id
                result = UpdateResult(success=True, key=key, revision_id=record.revision_id, message='memory updated')
            if post_manage_bucket:
                await self.governance._auto_manage_bucket(post_manage_bucket)
            await self.maintenance._run_memory_gc()
            return result if result is not None else UpdateResult(success=False, key=key, message='memory update produced no result')
        finally:
            self.alias.end_session(flush=True)

    async def set_gray(self, key: str, *, gray: bool, reason: str='manual') -> UpdateResult:
        current0 = await self.runtime.run_storage_task(self.runtime.storage.get_record, key)
        if current0 is None:
            return UpdateResult(success=False, key=key, message='memory key not found')
        async with self.topology._bucket_write_lock(current0.bucket_id):
            current = await self.runtime.run_storage_task(self.runtime.storage.get_record, key)
            if current is None:
                return UpdateResult(success=False, key=key, message='memory key not found')
            result = await self.primitives.set_gray_unlocked(
                current,
                gray=gray,
                reason=reason,
            )
            await self.maintenance._run_memory_gc()
            return result

    def _resolve_delete_target_key(self, target: Any) -> str:
        if isinstance(target, MemoryRecord):
            return str(target.key).strip()
        if isinstance(target, BucketInfo):
            node_key = str(target.node_key or '').strip()
            if node_key:
                return node_key
            bid = str(target.bucket_id or '').strip()
            if bid:
                info = self.runtime.storage.get_bucket_info(bid)
                if info is not None and str(info.node_key or '').strip():
                    return str(info.node_key).strip()
                return bid
            return ''
        if isinstance(target, dict):
            key_token = str(target.get('key', '')).strip()
            if key_token:
                return key_token
            node_token = str(target.get('node_key', '')).strip()
            if node_token:
                return node_token
            bucket_token = str(target.get('bucket_id', '')).strip()
            if bucket_token:
                info = self.runtime.storage.get_bucket_info(bucket_token)
                if info is not None and str(info.node_key or '').strip():
                    return str(info.node_key).strip()
                return bucket_token
            return ''
        if isinstance(target, str):
            token = target.strip()
            if not token:
                return ''
            info = self.runtime.storage.get_bucket_info(token)
            if info is not None and str(info.node_key or '').strip():
                return str(info.node_key).strip()
            return token
        key_attr = str(getattr(target, 'key', '') or '').strip()
        if key_attr:
            return key_attr
        node_attr = str(getattr(target, 'node_key', '') or '').strip()
        if node_attr:
            return node_attr
        bucket_attr = str(getattr(target, 'bucket_id', '') or '').strip()
        if bucket_attr:
            info = self.runtime.storage.get_bucket_info(bucket_attr)
            if info is not None and str(info.node_key or '').strip():
                return str(info.node_key).strip()
            return bucket_attr
        return ''

    async def delete_memory(self, key: Any, *, reason: str='') -> DeleteResult:
        target_key = self._resolve_delete_target_key(key)
        if not target_key:
            return DeleteResult(success=False, key='', message='invalid delete target')
        current = await self.runtime.run_storage_task(self.runtime.storage.get_record, target_key)
        info: BucketInfo | None = None
        if current is not None and current.kind == BUCKET_KIND_BUCKET:
            child_id = str(current.child_bucket_id or '').strip()
            if child_id:
                info = self.runtime.storage.get_bucket_info(child_id)
        if info is None:
            info = self.runtime.storage.get_bucket_info(target_key)
        res = await self.set_gray(target_key, gray=True, reason=reason or 'delete')
        if res.success and info is not None and info.parent_bucket_id:
            await self.runtime.run_storage_task(self.runtime.storage.remove_child_title_refs, parent_bucket_id=info.parent_bucket_id, child_bucket_id=info.bucket_id)
        return DeleteResult(success=res.success, key=res.key, revision_id=res.revision_id, message='memory marked gray' if res.success else res.message)

    async def _move_item_unlocked(self, *, key: str, target_bucket_id: str, reason: str) -> MoveResult:
        key = str(key or '').strip()
        if not key:
            return MoveResult(success=False, message='key is required')
        current = await self.runtime.run_storage_task(self.runtime.storage.get_record, key)
        if current is None:
            return MoveResult(success=False, key=key, message='key not found')
        if current.gray:
            return MoveResult(success=False, key=key, message='gray item cannot be moved')
        source_info = self.runtime.storage.get_bucket_info(current.bucket_id)
        if source_info is None or source_info.sealed:
            return MoveResult(success=False, key=key, message='source bucket is not writable')
        target_bucket = self.topology._resolve_bucket_id(target_bucket_id)
        target_info = self.runtime.storage.get_bucket_info(target_bucket)
        if target_info is None:
            return MoveResult(success=False, key=key, message='target bucket not found')
        if target_info.sealed:
            return MoveResult(success=False, key=key, message='target bucket is sealed')
        if current.kind == BUCKET_KIND_BUCKET:
            child_raw = str(current.child_bucket_id or '').strip()
            if not child_raw:
                return MoveResult(success=False, key=key, message='invalid bucket node: missing child_bucket_id')
            child_bucket_id = self.topology._resolve_bucket_id(child_raw)
            if child_bucket_id == self.topology.root_bucket_id():
                return MoveResult(success=False, key=key, message='ROOT bucket cannot be moved')
            if child_bucket_id == target_bucket:
                return MoveResult(success=False, key=key, message='bucket cannot move to itself')
            if self.topology._is_bucket_descendant_unlocked(ancestor_bucket_id=child_bucket_id, candidate_bucket_id=target_bucket):
                return MoveResult(success=False, key=key, message='bucket cannot move to its descendant')
            child_info = self.runtime.storage.get_bucket_info(child_bucket_id)
            if child_info is None:
                return MoveResult(success=False, key=key, message='child bucket not found')
            subtree_max = self.topology._bucket_subtree_max_level_unlocked(child_bucket_id)
            depth_span = max(0, int(subtree_max) - int(child_info.level))
            new_max_level = int(target_info.level) + 1 + depth_span
            if new_max_level > self.runtime._max_depth:
                return MoveResult(success=False, key=key, message='move would exceed max depth (3)')
            await self.runtime.run_storage_task(self.runtime.storage.reparent_bucket, bucket_id=child_bucket_id, new_parent_bucket_id=target_bucket)
        else:
            child_bucket_id = ''
        if current.bucket_id == target_bucket:
            return MoveResult(success=True, key=key, from_bucket=current.bucket_id, to_bucket=target_bucket, revision_id=current.revision_id, moved_kind=current.kind, message='already in target bucket')
        out_rel = normalize_relations(current.relations)
        self.primitives._append_relation_once(out_rel['lifecycle_links'], target=current.revision_id, rel_type='tombstones', score=1.0, note='move_out')
        out_rec = MemoryRecord(key=current.key, revision_id=self.runtime.storage.generate_revision_id(), kind=current.kind, bucket_id=current.bucket_id, title=current.title, summary=current.summary, content=current.content, weight=current.weight, event='GRAY_SET', gray=True, relations=out_rel, evidence_ref=current.evidence_ref, expires_at=current.expires_at, source_hash=current.source_hash, child_bucket_id=child_bucket_id or current.child_bucket_id, confidence_type=current.confidence_type)
        await self.runtime.run_storage_task(self.runtime.storage.write_memory_record, out_rec)
        await self.primitives._append_context_event(bucket_id=current.bucket_id, event_type='GRAY_SET', record=out_rec, payload={'from_revision': current.revision_id, 'reason': reason, 'to_bucket': target_bucket})
        in_rel = normalize_relations(current.relations)
        self.primitives._append_relation_once(in_rel['lifecycle_links'], target=out_rec.revision_id, rel_type='supersedes', score=1.0, note='move_in')
        in_rec = MemoryRecord(key=current.key, revision_id=self.runtime.storage.generate_revision_id(), kind=current.kind, bucket_id=target_bucket, title=current.title, summary=current.summary, content=current.content, weight=current.weight, event='MOVE_IN', gray=False, relations=in_rel, evidence_ref=current.evidence_ref, expires_at=current.expires_at, source_hash=current.source_hash, child_bucket_id=child_bucket_id or current.child_bucket_id, confidence_type=current.confidence_type)
        await self.runtime.run_storage_task(self.runtime.storage.write_memory_record, in_rec)
        await self.primitives._append_context_event(bucket_id=target_bucket, event_type='MOVE_IN', record=in_rec, payload={'from_bucket': current.bucket_id, 'from_revision': out_rec.revision_id, 'reason': reason})
        return MoveResult(success=True, key=key, from_bucket=current.bucket_id, to_bucket=target_bucket, revision_id=in_rec.revision_id, moved_kind=current.kind, message='moved')

    async def move_item(
        self,
        key: str,
        *,
        target_bucket_id: str,
        reason: str = 'manual_move',
    ) -> MoveResult:
        self.alias.begin_session()
        try:
            current = await self.runtime.run_storage_task(self.runtime.storage.get_record, key)
            source_bucket_id = current.bucket_id if current is not None else ''
            target_resolved = self.topology._resolve_bucket_id_soft(target_bucket_id)
            async with self.topology._multi_bucket_write_lock(
                [source_bucket_id, target_resolved]
            ):
                return await self._move_item_unlocked(
                    key=key,
                    target_bucket_id=target_bucket_id,
                    reason=reason,
                )
        finally:
            self.alias.end_session(flush=True)
