from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable

from ..models import (
    BUCKET_KIND_BUCKET,
    BucketInfo,
    MemoryRecord,
    normalize_relations,
    parse_iso_or_none,
    utc_now_iso,
)

if TYPE_CHECKING:
    from ..bucket_handle import BucketHandle
    from ..engine_runtime import EngineRuntime


def _clamp_score(value: float) -> float:
    return max(0.0, min(1.0, float(value)))

class BucketTopologyService:
    def __init__(self, runtime: "EngineRuntime", *, alias: Callable[[], Any], handle_factory: Callable[[str], Any], maintenance: Callable[[], Any], primitives: Callable[[], Any]) -> None:
        self.runtime = runtime
        self._alias_provider = alias
        self._handle_factory = handle_factory
        self._maintenance_provider = maintenance
        self._primitives_provider = primitives

    @property
    def alias(self) -> Any:
        return self._alias_provider()

    @property
    def maintenance(self) -> Any:
        return self._maintenance_provider()

    @property
    def primitives(self) -> Any:
        return self._primitives_provider()

    def root_bucket_id(self) -> str:
        return self.runtime.storage.get_root_bucket_id()

    def active_bucket_id(self) -> str:
        return self.runtime.storage.get_active_bucket_id()

    @property
    def bucket_id(self):
        return self.active_bucket_id()

    @asynccontextmanager
    async def _bucket_write_lock(self, bucket_id: str | None):
        resolved = self._resolve_bucket_id_soft(bucket_id) or self.active_bucket_id()
        async with self.runtime._bucket_lock_manager.acquire_many([resolved]):
            yield resolved

    @asynccontextmanager
    async def _multi_bucket_write_lock(
        self,
        bucket_ids: list[str] | tuple[str, ...] | set[str],
    ):
        resolved = [self._resolve_bucket_id_soft(bucket_id) for bucket_id in bucket_ids]
        async with self.runtime._bucket_lock_manager.acquire_many(resolved):
            yield resolved

    async def resolve_bucket_handle_id(self, bucket_id: str) -> str:
        return await asyncio.to_thread(self._resolve_bucket_id, bucket_id)

    def list_buckets(self) -> list[BucketInfo]:
        infos = self.runtime.storage.list_buckets()
        infos.sort(key=lambda item: (item.level, item.bucket_id))
        return infos

    async def set_active_bucket(self, bucket_id: str) -> dict[str, Any]:
        target = str(bucket_id or '').strip()
        if not target:
            return {'success': False, 'bucket_id': '', 'message': 'bucket_id is required'}
        async with self.runtime._global_meta_lock:
            resolved = self._resolve_bucket_id_soft(target) or target
            info = self.runtime.storage.get_bucket_info(resolved)
            if info is None:
                return {'success': False, 'bucket_id': resolved, 'message': f'bucket not found: {resolved}'}
            await self.runtime.run_storage_task(self.runtime.storage.set_active_bucket_id, resolved)
            return {'success': True, 'bucket_id': resolved, 'message': 'active bucket updated'}

    async def switch_active_bucket(self, bucket_id: str) -> dict[str, Any]:
        return await self.set_active_bucket(bucket_id)

    def _resolve_bucket_redirect_chain(self, bucket_id: str) -> tuple[str, list[str]]:
        current = str(bucket_id or '').strip()
        if not current:
            return ('', [])
        lineage: list[str] = [current]
        visited: set[str] = {current}
        while True:
            info = self.runtime.storage.get_bucket_info(current)
            if info is None:
                break
            next_id = str(info.sealed_to or '').strip() if info.sealed else ''
            if not next_id or next_id in visited:
                break
            next_info = self.runtime.storage.get_bucket_info(next_id)
            if next_info is None:
                break
            current = next_id
            lineage.append(current)
            visited.add(current)
        return (current, lineage)

    def _resolve_bucket_id_soft(self, bucket_id: str | None) -> str:
        raw = str(bucket_id or '').strip()
        if not raw:
            return raw
        try:
            return self._resolve_bucket_id(raw)
        except Exception:
            return raw

    async def latest_bucket_id(self, bucket_id: str | None=None) -> str:
        """Resolve a historical bucket id to its latest canonical id."""
        async with self.runtime._global_meta_lock:
            return self._resolve_bucket_id(bucket_id)

    def _repair_sealed_child_links_unlocked(self) -> int:
        changed = 0
        records = self.runtime.storage.load_all_records_snapshot(include_gray=True)
        for rec in records:
            if rec.kind != BUCKET_KIND_BUCKET:
                continue
            child_id = str(rec.child_bucket_id or '').strip()
            if not child_id:
                continue
            child_info = self.runtime.storage.get_bucket_info(child_id)
            if child_info is None or not child_info.sealed:
                continue
            successor_id = str(child_info.sealed_to or '').strip()
            if not successor_id or successor_id == child_id:
                continue
            successor_info = self.runtime.storage.get_bucket_info(successor_id)
            if successor_info is None:
                continue
            relations = normalize_relations(rec.relations)
            relations['lifecycle_links'].append({'target': rec.revision_id, 'type': 'revises', 'score': 1.0, 'note': 'repair_sealed_child_redirect'})
            patched = MemoryRecord(key=rec.key, revision_id=self.runtime.storage.generate_revision_id(), kind=rec.kind, bucket_id=rec.bucket_id, title=rec.title, summary=rec.summary, content=rec.content, weight=rec.weight, event='UPDATE', gray=rec.gray, relations=relations, evidence_ref=rec.evidence_ref, expires_at=rec.expires_at, source_hash=rec.source_hash, child_bucket_id=successor_id, confidence_type=rec.confidence_type)
            self.runtime.storage.write_memory_record(patched)
            self.primitives._append_context_event_sync(bucket_id=rec.bucket_id, event_type='UPDATE', record=patched, payload={'from_revision': rec.revision_id, 'reason': 'repair_sealed_child_redirect', 'old_child_bucket_id': child_id, 'new_child_bucket_id': successor_id})
            changed += 1
        return changed

    def _maybe_repair_sealed_child_links_unlocked(self, *, force: bool=False) -> int:
        meta = self.runtime.storage.metadata_snapshot()
        try:
            version = int(meta.get('context_version', 0))
        except Exception:
            version = 0
        if not force and version == self.runtime._last_sealed_link_repair_version:
            return 0
        changed = self._repair_sealed_child_links_unlocked()
        meta_after = self.runtime.storage.metadata_snapshot()
        try:
            self.runtime._last_sealed_link_repair_version = int(meta_after.get('context_version', version))
        except Exception:
            self.runtime._last_sealed_link_repair_version = version
        return changed

    def _resolve_bucket_id(self, bucket_id: str | None) -> str:
        raw = str(bucket_id or '').strip()
        if raw.upper() == 'ROOT':
            resolved = self.root_bucket_id()
        else:
            resolved = raw or self.active_bucket_id()
        final_id, _ = self._resolve_bucket_redirect_chain(resolved)
        resolved = final_id or resolved
        info = self.runtime.storage.get_bucket_info(resolved)
        if info is None:
            raise ValueError(f'bucket not found: {resolved}')
        return resolved

    async def _create_bucket_unlocked(self, parent_bucket_id: str, *, title: str, summary: str='', content: str='', summary_locked: bool=False, mapping_title: str | None=None) -> BucketInfo:
        parent_id = self._resolve_bucket_id(parent_bucket_id)
        parent = self.runtime.storage.get_bucket_info(parent_id)
        if parent is None:
            raise ValueError(f'bucket not found: {parent_id}')
        if parent.level >= self.runtime._max_depth:
            raise ValueError(f'bucket level exceeds limit: max depth is {self.runtime._max_depth} (root included)')
        node_key = self.runtime.storage.generate_key()
        summary_text = summary.strip()
        summary_status = 'ready' if summary_text else 'pending'
        child_summary = summary_text or self.runtime._pending_bucket_summary
        child = await self.runtime.run_storage_task(self.runtime.storage.create_bucket, parent_bucket_id=parent.bucket_id, level=parent.level + 1, title=title.strip() or 'child bucket', summary=child_summary, node_key=node_key, summary_status=summary_status, summary_locked=bool(summary_locked), mapping_title=mapping_title)
        node_ingested = {'title': child.title, 'summary': child.summary[:140], 'content': (content or child.summary)[:1000], 'weight': 0.75, 'event': 'ADD', 'gray': False, 'relations': normalize_relations({})}
        node_record = self.primitives._build_record(key=node_key, event='ADD', ingested=node_ingested, bucket_id=parent.bucket_id, evidence_ref='', kind=BUCKET_KIND_BUCKET, child_bucket_id=child.bucket_id)
        await self.runtime.run_storage_task(self.runtime.storage.write_memory_record, node_record)
        await self.primitives._append_context_event(bucket_id=parent.bucket_id, event_type='ADD', record=node_record, payload={'kind': 'bucket', 'child_bucket_id': child.bucket_id})
        return child

    async def set_bucket_with_id(self, title: str, parent_bucket_id: str, *, summary: str='', content: str='', summary_locked: bool=False) -> BucketHandle:
        """
        Get or create a same-title child with setdefault concurrency semantics.

        A depth-limit failure propagates ``ValueError`` from the create path.
        """
        async with self._bucket_write_lock(parent_bucket_id) as resolved_parent:
            async with self.runtime._global_meta_lock:
                exist_bucket_id = self.runtime.storage.get_child_title_target(resolved_parent, title)
                if exist_bucket_id:
                    try:
                        resolved_existing = self._resolve_bucket_id(exist_bucket_id)
                    except ValueError:
                        resolved_existing = ''
                    existing_info = self.runtime.storage.get_bucket_info(resolved_existing) if resolved_existing else None
                    parent_info = self.runtime.storage.get_bucket_info(resolved_parent)
                    if existing_info is not None and existing_info.parent_bucket_id == resolved_parent and (parent_info is not None) and (resolved_existing in parent_info.children):
                        if exist_bucket_id != resolved_existing:
                            await self.runtime.run_storage_task(self.runtime.storage.set_child_title_target, parent_bucket_id=resolved_parent, title=title, child_bucket_id=resolved_existing)
                        await self.maintenance._run_memory_gc()
                        return self._handle_factory(resolved_existing)
                    await self.runtime.run_storage_task(self.runtime.storage.remove_child_title_refs, parent_bucket_id=resolved_parent, child_bucket_id=exist_bucket_id)
                child = await self._create_bucket_unlocked(resolved_parent, title=title, summary=summary, content=content, summary_locked=summary_locked, mapping_title=title)
                await self.maintenance._run_memory_gc()
                return self._handle_factory(child.bucket_id)

    async def set_bucket(self, title: str, *, summary: str='', content: str='', summary_locked: bool=False) -> BucketHandle:
        return await self.set_bucket_with_id(title, self.root_bucket_id(), summary=summary, content=content, summary_locked=summary_locked)

    async def create_bucket(self, parent_bucket_id: str, *, title: str, summary: str='', content: str='', summary_locked: bool=False) -> BucketInfo:
        """Create a child bucket under the given parent bucket."""
        async with self._bucket_write_lock(parent_bucket_id) as resolved_parent:
            async with self.runtime._global_meta_lock:
                child = await self._create_bucket_unlocked(resolved_parent, title=title, summary=summary, content=content, summary_locked=summary_locked)
                await self.maintenance._run_memory_gc()
                return child

    async def create_child_bucket(self, parent_bucket_id: str | None=None, *, title: str, summary: str='', content: str='', summary_locked: bool=False) -> BucketInfo:
        target_parent = str(parent_bucket_id or '').strip() or self.active_bucket_id()
        return await self.create_bucket(target_parent, title=title, summary=summary, content=content, summary_locked=summary_locked)

    async def _create_sibling_bucket(self, source_bucket_id: str, *, title: str, summary: str, content: str='') -> BucketInfo:
        source = self.runtime.storage.get_bucket_info(source_bucket_id)
        if source is None:
            raise ValueError(f'bucket not found: {source_bucket_id}')
        if source.parent_bucket_id is None:
            raise ValueError('root bucket cannot create same-level sibling')
        parent = self.runtime.storage.get_bucket_info(source.parent_bucket_id)
        if parent is None:
            raise ValueError('source parent bucket missing')
        node_key = self.runtime.storage.generate_key()
        sibling = await self.runtime.run_storage_task(self.runtime.storage.create_bucket, parent_bucket_id=parent.bucket_id, level=source.level, title=title.strip() or 'sibling bucket', summary=summary.strip() or 'sibling summary', node_key=node_key, summary_status='ready', summary_locked=False)
        node_ingested = {'title': sibling.title, 'summary': sibling.summary[:140], 'content': (content or sibling.summary)[:1000], 'weight': 0.75, 'event': 'ADD', 'gray': False, 'relations': normalize_relations({})}
        node_record = self.primitives._build_record(key=node_key, event='ADD', ingested=node_ingested, bucket_id=parent.bucket_id, evidence_ref='', kind=BUCKET_KIND_BUCKET, child_bucket_id=sibling.bucket_id)
        await self.runtime.run_storage_task(self.runtime.storage.write_memory_record, node_record)
        await self.primitives._append_context_event(bucket_id=parent.bucket_id, event_type='ADD', record=node_record, payload={'kind': 'bucket', 'child_bucket_id': sibling.bucket_id})
        return sibling

    async def _create_bucket_auto(self, *, target_bucket_id: str, title: str, summary: str, content: str='') -> BucketInfo:
        source = self.runtime.storage.get_bucket_info(target_bucket_id)
        if source is None:
            raise ValueError(f'bucket not found: {target_bucket_id}')
        if source.level < self.runtime._max_depth:
            return await self._create_bucket_unlocked(source.bucket_id, title=title, summary=summary, content=content)
        return await self._create_sibling_bucket(source.bucket_id, title=title, summary=summary, content=content)

    @staticmethod
    def _is_auto_split_reason(reason: str) -> bool:
        r = str(reason or '').strip().lower()
        return r.startswith('auto_') or 'post_compress_split' in r or 'context_overflow' in r

    async def _can_auto_split_now(self, *, bucket_id: str) -> bool:
        if self.runtime._auto_split_cooldown_sec <= 0:
            return True
        last_at_raw = await self.runtime.run_storage_task(self.runtime.storage.get_last_auto_split_at, bucket_id)
        last_at = parse_iso_or_none(last_at_raw)
        if last_at is None:
            return True
        if last_at.tzinfo is None:
            last_at = last_at.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return (now - last_at).total_seconds() >= self.runtime._auto_split_cooldown_sec

    async def _seal_bucket_unlocked(self, *, source_bucket_id: str, successor_bucket_id: str) -> None:
        source_alias_table = self.alias._alias_table(source_bucket_id, resolve_successor=False)
        old_map_hash = await self.runtime.run_storage_task(source_alias_table.snapshot_hash)
        source = self.runtime.storage.get_bucket_info(source_bucket_id)
        if source is None:
            return
        await self.runtime.run_storage_task(self.runtime.storage.seal_bucket_successor, source_bucket_id=source_bucket_id, successor_bucket_id=successor_bucket_id)
        await self.runtime.run_storage_task(source_alias_table.freeze)
        new_map_hash = await self.runtime.run_storage_task(self.alias._alias_table(successor_bucket_id).snapshot_hash)
        await self.runtime.run_storage_task(self.runtime.storage.append_alias_audit, {'request_id': self.alias.next_request_id('seal_switch'), 'tool': 'seal_switch', 'source_bucket_id': source_bucket_id, 'successor_bucket_id': successor_bucket_id, 'old_map_hash': old_map_hash, 'new_map_hash': new_map_hash, 'switched_at': utc_now_iso()})

    async def _rebuild_source_successor_unlocked(self, *, source_bucket_id: str, keep_keys: list[str], created_bucket_ids: list[str], reason: str) -> str:
        source = self.runtime.storage.get_bucket_info(source_bucket_id)
        if source is None:
            raise ValueError(f'bucket not found: {source_bucket_id}')
        successor = await self.runtime.run_storage_task(self.runtime.storage.create_bucket, parent_bucket_id=source.parent_bucket_id, level=source.level, title=f'{source.title}_successor', summary=source.summary or 'successor bucket', node_key=self.runtime.storage.generate_key(), summary_status='ready' if source.summary.strip() else 'pending', summary_locked=False)
        for bid in created_bucket_ids:
            binfo = self.runtime.storage.get_bucket_info(bid)
            if binfo is None:
                continue
            await self.runtime.run_storage_task(self.runtime.storage.reparent_bucket, bucket_id=bid, new_parent_bucket_id=successor.bucket_id, preserve_old_title_map=True)
        dedup_keep = []
        seen_keep: set[str] = set()
        for k in keep_keys:
            ks = str(k).strip()
            if not ks or ks in seen_keep:
                continue
            seen_keep.add(ks)
            dedup_keep.append(ks)
        for key in dedup_keep:
            rec = await self.runtime.run_storage_task(self.runtime.storage.get_record, key)
            if rec is None or rec.gray:
                continue
            if rec.bucket_id != source_bucket_id:
                continue
            if rec.kind == BUCKET_KIND_BUCKET and rec.child_bucket_id:
                await self.runtime.run_storage_task(self.runtime.storage.reparent_bucket, bucket_id=rec.child_bucket_id, new_parent_bucket_id=successor.bucket_id, preserve_old_title_map=True)
            rel_old = normalize_relations(rec.relations)
            self.primitives._append_relation_once(rel_old['lifecycle_links'], target=rec.revision_id, rel_type='tombstones', score=1.0, note='successor_rebuild_out')
            out_rec = MemoryRecord(key=rec.key, revision_id=self.runtime.storage.generate_revision_id(), kind=rec.kind, bucket_id=source_bucket_id, title=rec.title, summary=rec.summary, content=rec.content, weight=rec.weight, event='GRAY_SET', gray=True, relations=rel_old, evidence_ref=rec.evidence_ref, expires_at=rec.expires_at, source_hash=rec.source_hash, child_bucket_id=rec.child_bucket_id, confidence_type=rec.confidence_type)
            await self.runtime.run_storage_task(self.runtime.storage.write_memory_record, out_rec)
            await self.primitives._append_context_event(bucket_id=source_bucket_id, event_type='GRAY_SET', record=out_rec, payload={'from_revision': rec.revision_id, 'reason': 'successor_rebuild_out'})
            rel_new = normalize_relations(rec.relations)
            self.primitives._append_relation_once(rel_new['lifecycle_links'], target=out_rec.revision_id, rel_type='supersedes', score=1.0, note='successor_rebuild_in')
            in_rec = MemoryRecord(key=rec.key, revision_id=self.runtime.storage.generate_revision_id(), kind=rec.kind, bucket_id=successor.bucket_id, title=rec.title, summary=rec.summary, content=rec.content, weight=rec.weight, event='MOVE_IN', gray=False, relations=rel_new, evidence_ref=rec.evidence_ref, expires_at=rec.expires_at, source_hash=rec.source_hash, child_bucket_id=rec.child_bucket_id, confidence_type=rec.confidence_type)
            await self.runtime.run_storage_task(self.runtime.storage.write_memory_record, in_rec)
            await self.primitives._append_context_event(bucket_id=successor.bucket_id, event_type='MOVE_IN', record=in_rec, payload={'from_bucket': source_bucket_id, 'from_revision': out_rec.revision_id, 'reason': reason})
        await self._seal_bucket_unlocked(source_bucket_id=source_bucket_id, successor_bucket_id=successor.bucket_id)
        root_id = self.root_bucket_id()
        active_id = self.active_bucket_id()
        if source_bucket_id == root_id:
            await self.runtime.run_storage_task(self.runtime.storage.set_root_bucket_id, successor.bucket_id)
        if source_bucket_id == active_id:
            await self.runtime.run_storage_task(self.runtime.storage.set_active_bucket_id, successor.bucket_id)
        if self._is_auto_split_reason(reason):
            await self.runtime.run_storage_task(self.runtime.storage.mark_auto_split, source_bucket_id=source_bucket_id, successor_bucket_id=successor.bucket_id)
        return successor.bucket_id

    def _is_bucket_descendant_unlocked(self, *, ancestor_bucket_id: str, candidate_bucket_id: str) -> bool:
        ancestor = str(ancestor_bucket_id or '').strip()
        current = str(candidate_bucket_id or '').strip()
        if not ancestor or not current:
            return False
        if ancestor == current:
            return True
        seen: set[str] = set()
        while current and current not in seen:
            seen.add(current)
            info = self.runtime.storage.get_bucket_info(current)
            if info is None or not info.parent_bucket_id:
                return False
            current = str(info.parent_bucket_id).strip()
            if current == ancestor:
                return True
        return False

    def _bucket_subtree_max_level_unlocked(self, root_bucket_id: str) -> int:
        root = str(root_bucket_id or '').strip()
        info = self.runtime.storage.get_bucket_info(root)
        if info is None:
            return 0
        max_level = int(info.level)
        for item in self.runtime.storage.list_buckets():
            bid = str(item.bucket_id or '').strip()
            if not bid:
                continue
            if self._is_bucket_descendant_unlocked(ancestor_bucket_id=root, candidate_bucket_id=bid):
                max_level = max(max_level, int(item.level))
        return max_level

    async def _create_successor_bucket_shallow_unlocked(self, *, source_bucket_id: str, title: str='', summary: str='') -> BucketInfo:
        source = self.runtime.storage.get_bucket_info(source_bucket_id)
        if source is None:
            raise ValueError(f'bucket not found: {source_bucket_id}')
        successor = await self.runtime.run_storage_task(self.runtime.storage.create_bucket, parent_bucket_id=source.parent_bucket_id, level=source.level, title=title.strip() or f'{source.title}_successor', summary=summary.strip() or source.summary or 'successor bucket', node_key=self.runtime.storage.generate_key(), summary_status='ready', summary_locked=False)
        return successor

    async def _seal_and_switch_bucket_unlocked(self, *, source_bucket_id: str, successor_bucket_id: str, reason: str) -> None:
        await self._seal_bucket_unlocked(source_bucket_id=source_bucket_id, successor_bucket_id=successor_bucket_id)
        root_id = self.root_bucket_id()
        active_id = self.active_bucket_id()
        if source_bucket_id == root_id:
            await self.runtime.run_storage_task(self.runtime.storage.set_root_bucket_id, successor_bucket_id)
        if source_bucket_id == active_id:
            await self.runtime.run_storage_task(self.runtime.storage.set_active_bucket_id, successor_bucket_id)
        if self._is_auto_split_reason(reason):
            await self.runtime.run_storage_task(self.runtime.storage.mark_auto_split, source_bucket_id=source_bucket_id, successor_bucket_id=successor_bucket_id)
