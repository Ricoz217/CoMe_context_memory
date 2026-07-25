from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

from ..models import BUCKET_KIND_BUCKET, BUCKET_KIND_MEMORY, MemoryRecord, normalize_relations
from ..rerank import louvain_split_groups

if TYPE_CHECKING:
    from ..engine_runtime import EngineRuntime


def _clamp_score(value: float) -> float:
    return max(0.0, min(1.0, float(value)))

class BucketSplitService:
    def __init__(self, runtime: "EngineRuntime", *, alias: Callable[[], Any], governance: Callable[[], Any], maintenance: Callable[[], Any], primitives: Callable[[], Any], summary: Callable[[], Any], topology: Callable[[], Any]) -> None:
        self.runtime = runtime
        self._alias_provider = alias
        self._governance_provider = governance
        self._maintenance_provider = maintenance
        self._primitives_provider = primitives
        self._summary_provider = summary
        self._topology_provider = topology

    @property
    def alias(self) -> Any:
        return self._alias_provider()

    @property
    def governance(self) -> Any:
        return self._governance_provider()

    @property
    def maintenance(self) -> Any:
        return self._maintenance_provider()

    @property
    def primitives(self) -> Any:
        return self._primitives_provider()

    @property
    def summary(self) -> Any:
        return self._summary_provider()

    @property
    def topology(self) -> Any:
        return self._topology_provider()

    async def split_bucket(
        self,
        bucket_id: str,
        *,
        reason: str = 'manual_split',
        target_groups_min: int = 2,
        target_groups_max: int = 10,
    ) -> dict[str, Any]:
        self.alias.begin_session()
        try:
            async with self.topology._bucket_write_lock(bucket_id) as resolved:
                result = await self._split_bucket_unlocked(
                    bucket_id=resolved,
                    reason=reason,
                    target_groups_min=target_groups_min,
                    target_groups_max=target_groups_max,
                )
                await self.maintenance._run_memory_gc()
                return result
        finally:
            self.alias.end_session(flush=True)

    async def _split_bucket_unlocked(self, *, bucket_id: str, reason: str, target_groups_min: int=2, target_groups_max: int=10) -> dict[str, Any]:
        source = self.runtime.storage.get_bucket_info(bucket_id)
        if source is None:
            return {'success': False, 'message': f'bucket not found: {bucket_id}'}
        if self.topology._is_auto_split_reason(reason):
            if not await self.topology._can_auto_split_now(bucket_id=bucket_id):
                await self.runtime.run_storage_task(self.runtime.storage.record_auto_split_cooldown_skip)
                return {'success': False, 'created_buckets': 0, 'moved_memories': 0, 'message': 'split skipped by cooldown'}
        records = await self.runtime.run_storage_task(self.runtime.storage.load_bucket_snapshot, bucket_id, include_gray=False)
        if len(records) < 2:
            return {'success': True, 'created_buckets': 0, 'moved_memories': 0, 'message': 'not enough records to split'}
        pressure_before, _ = await self.governance._bucket_pressure(bucket_id)
        prepared_alias, map_ver = await self.alias._prepare_alias_payload_with_version(bucket_id, {'records': [r.to_dict() for r in records]})
        alias_records = prepared_alias.get('records', [])
        split_alias_payload = {'reason': reason, 'split_plan_target_items': self.runtime._split_plan_target_items, 'split_plan_hard_cap': self.runtime._split_plan_hard_cap, 'target_groups_min': target_groups_min, 'target_groups_max': target_groups_max, 'records': alias_records}
        await self.alias._assert_alias_payload_safe(bucket_id, split_alias_payload)
        split_plan_alias = await self.runtime.pipeline.bucket_split(bucket_context=await self.runtime.bucket_context(bucket_id), records=alias_records, split_plan_target_items=self.runtime._split_plan_target_items, split_plan_hard_cap=self.runtime._split_plan_hard_cap, target_groups_min=target_groups_min, target_groups_max=target_groups_max, reason=reason)
        await self.alias.audit_llm_call(tool='split_bucket', bucket_id=bucket_id, map_version=map_ver, alias_input=split_alias_payload, alias_output=split_plan_alias)
        split_plan = await self.alias.restore_alias_payload(bucket_id, split_plan_alias, map_version=map_ver)
        await self.runtime.record_llm_usage()
        await self.runtime.record_llm_diag()
        merge_groups_raw = split_plan.get('merge_groups', [])
        keep_items_raw = split_plan.get('keep_items', [])
        if not isinstance(merge_groups_raw, list):
            merge_groups_raw = []
        if not isinstance(keep_items_raw, list):
            keep_items_raw = []
        key_to_rec = {r.key: r for r in records}
        key_set = set(key_to_rec.keys())
        bucket_id_to_node_key: dict[str, str] = {}
        for rec in records:
            if rec.kind != BUCKET_KIND_BUCKET:
                continue
            child_raw = str(rec.child_bucket_id or '').strip()
            if not child_raw:
                continue
            bucket_id_to_node_key[child_raw] = rec.key
            try:
                resolved_child = self.topology._resolve_bucket_id(child_raw)
            except Exception:
                resolved_child = child_raw
            if resolved_child:
                bucket_id_to_node_key[str(resolved_child).strip()] = rec.key
        merge_groups: list[dict[str, Any]] = []
        keep_keys_set: set[str] = set()
        for g in merge_groups_raw:
            if not isinstance(g, dict):
                continue
            keys_raw = g.get('keys', [])
            if not isinstance(keys_raw, list):
                continue
            keys: list[str] = []
            for raw_key in keys_raw:
                token = str(raw_key).strip()
                if not token:
                    continue
                normalized = token
                if token not in key_set:
                    mapped = str(bucket_id_to_node_key.get(token, '')).strip()
                    if mapped:
                        normalized = mapped
                if normalized in key_set and normalized not in keys:
                    keys.append(normalized)
            if not keys:
                continue
            merge_groups.append({'title': str(g.get('title', '')).strip() or 'split_group', 'summary': str(g.get('summary', '')).strip()[:140] or 'split group', 'content': str(g.get('content', '')).strip()[:1000], 'keys': keys})
        for item in keep_items_raw:
            if not isinstance(item, dict):
                continue
            keys_raw = item.get('keys', [])
            if not isinstance(keys_raw, list):
                continue
            for k in keys_raw:
                ks = str(k).strip()
                if ks in key_set:
                    keep_keys_set.add(ks)
        merge_item_count = len(merge_groups) + len(keep_items_raw)
        if merge_item_count > self.runtime._split_plan_hard_cap:
            await self.runtime.run_storage_task(self.runtime.storage.record_split_plan_warn)
            merge_groups = []
            keep_keys_set.clear()
        elif merge_item_count > self.runtime._split_plan_target_items:
            await self.runtime.run_storage_task(self.runtime.storage.record_split_plan_warn)
        if not merge_groups:
            mem_records = [r for r in records if r.kind == BUCKET_KIND_MEMORY]
            louvain_groups = louvain_split_groups(mem_records, target_groups_min=max(2, int(target_groups_min)), target_groups_max=max(2, int(target_groups_max)))
            for idx, g in enumerate(louvain_groups):
                if not g:
                    continue
                keys = [r.key for r in g]
                prepared_alias, map_ver = await self.alias._prepare_alias_payload_with_version(bucket_id, {'records': [x.to_dict() for x in g]})
                alias_records = prepared_alias.get('records', [])
                summary_alias_payload = {'records': alias_records, 'reason': 'louvain_split'}
                await self.alias._assert_alias_payload_safe(bucket_id, summary_alias_payload)
                summary_alias = await self.runtime.pipeline.summarize_bucket(records=alias_records, reason='louvain_split')
                await self.alias.audit_llm_call(tool='bucket_summary', bucket_id=bucket_id, map_version=map_ver, alias_input=summary_alias_payload, alias_output=summary_alias)
                summary = await self.alias.restore_alias_payload(bucket_id, summary_alias, map_version=map_ver)
                await self.runtime.record_llm_usage()
                await self.runtime.record_llm_diag()
                merge_groups.append({'title': f'cluster_{idx + 1}', 'summary': summary.get('summary', f'cluster {idx + 1}'), 'content': summary.get('content', f'cluster {idx + 1} detail')[:1000], 'keys': keys})
            if not merge_groups:
                return {'success': False, 'created_buckets': 0, 'moved_memories': 0, 'message': 'split fallback failed'}
        for rec in records:
            if rec.kind == BUCKET_KIND_BUCKET:
                keep_keys_set.add(rec.key)
        created = 0
        moved = 0
        target_map: dict[str, str] = {}
        created_bucket_ids: list[str] = []
        for g in merge_groups:
            if source.level < self.runtime._max_depth:
                new_bucket = await self.topology._create_bucket_unlocked(source.bucket_id, title=g['title'], summary=g['summary'], content=g['content'])
            else:
                new_bucket = await self.topology._create_sibling_bucket(source.bucket_id, title=g['title'], summary=g['summary'], content=g['content'])
            created += 1
            created_bucket_ids.append(new_bucket.bucket_id)
            for k in g['keys']:
                target_map[k] = new_bucket.bucket_id
        for key, dst_bucket in target_map.items():
            rec = await self.runtime.run_storage_task(self.runtime.storage.get_record, key)
            if rec is None or rec.gray:
                continue
            if rec.bucket_id != source.bucket_id:
                continue
            if rec.kind == BUCKET_KIND_BUCKET and str(rec.child_bucket_id or '').strip():
                await self.runtime.run_storage_task(self.runtime.storage.reparent_bucket, bucket_id=str(rec.child_bucket_id).strip(), new_parent_bucket_id=dst_bucket)
            rel_old = normalize_relations(rec.relations)
            rel_old['lifecycle_links'].append({'target': rec.revision_id, 'type': 'tombstones', 'score': 1.0, 'note': 'split_move_out'})
            out_rec = MemoryRecord(key=rec.key, revision_id=self.runtime.storage.generate_revision_id(), kind=rec.kind, bucket_id=source.bucket_id, title=rec.title, summary=rec.summary, content=rec.content, weight=rec.weight, event='GRAY_SET', gray=True, relations=rel_old, evidence_ref=rec.evidence_ref, expires_at=rec.expires_at, source_hash=rec.source_hash, child_bucket_id=rec.child_bucket_id, confidence_type=rec.confidence_type)
            await self.runtime.run_storage_task(self.runtime.storage.write_memory_record, out_rec)
            await self.primitives._append_context_event(bucket_id=source.bucket_id, event_type='GRAY_SET', record=out_rec, payload={'from_revision': rec.revision_id, 'reason': 'split_move_out'})
            rel_new = normalize_relations(rec.relations)
            rel_new['lifecycle_links'].append({'target': out_rec.revision_id, 'type': 'supersedes', 'score': 1.0, 'note': 'split_move_in'})
            in_rec = MemoryRecord(key=rec.key, revision_id=self.runtime.storage.generate_revision_id(), kind=rec.kind, bucket_id=dst_bucket, title=rec.title, summary=rec.summary, content=rec.content, weight=rec.weight, event='MOVE_IN', gray=False, relations=rel_new, evidence_ref=rec.evidence_ref, expires_at=rec.expires_at, source_hash=rec.source_hash, child_bucket_id=rec.child_bucket_id, confidence_type=rec.confidence_type)
            await self.runtime.run_storage_task(self.runtime.storage.write_memory_record, in_rec)
            await self.primitives._append_context_event(bucket_id=dst_bucket, event_type='MOVE_IN', record=in_rec, payload={'from_bucket': source.bucket_id, 'from_revision': out_rec.revision_id})
            moved += 1
        keep_keys = [k for k in keep_keys_set if k not in target_map]
        for bid in created_bucket_ids:
            binfo = self.runtime.storage.get_bucket_info(bid)
            if binfo is not None and binfo.node_key:
                keep_keys.append(binfo.node_key)
        assigned = set(target_map.keys()) | set(keep_keys)
        for k in key_set:
            if k not in assigned:
                keep_keys.append(k)
        successor_bucket_id = await self.topology._rebuild_source_successor_unlocked(source_bucket_id=source.bucket_id, keep_keys=keep_keys, created_bucket_ids=created_bucket_ids, reason=reason)
        await self.runtime.run_storage_task(self.runtime.storage.append_event, event_type='SPLIT_DONE', bucket_id=source.bucket_id, payload={'reason': reason, 'created_buckets': created, 'moved_memories': moved, 'successor_bucket_id': successor_bucket_id})
        source_info = self.runtime.storage.get_bucket_info(source.bucket_id)
        if not self.summary._should_skip_auto_summary(source_info):
            await self.summary._refresh_bucket_summary_unlocked(bucket_id=source.bucket_id, force=False, reason='auto_after_split_source')
        for bid in created_bucket_ids:
            info = self.runtime.storage.get_bucket_info(bid)
            if self.summary._should_skip_auto_summary(info):
                continue
            await self.summary._refresh_bucket_summary_unlocked(bucket_id=bid, force=False, reason='auto_after_split_target')
        successor_info = self.runtime.storage.get_bucket_info(successor_bucket_id)
        if successor_info is not None and (not self.summary._should_skip_auto_summary(successor_info)):
            await self.summary._refresh_bucket_summary_unlocked(bucket_id=successor_bucket_id, force=False, reason='auto_after_split_successor')
        pressure_after, _ = await self.governance._bucket_pressure(successor_bucket_id)
        drop_abs = pressure_before - pressure_after
        if self.topology._is_auto_split_reason(reason) and drop_abs < self.runtime._auto_split_min_drop_abs:
            await self.runtime.run_storage_task(self.runtime.storage.record_auto_split_no_progress)
            return {'success': False, 'created_buckets': created, 'moved_memories': moved, 'message': f'split no progress: before={pressure_before:.4f} after={pressure_after:.4f}', 'successor_bucket_id': successor_bucket_id}
        return {'success': True, 'created_buckets': created, 'moved_memories': moved, 'message': 'split done', 'successor_bucket_id': successor_bucket_id, 'pressure_before': pressure_before, 'pressure_after': pressure_after}
