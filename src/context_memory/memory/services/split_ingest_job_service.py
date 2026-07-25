from __future__ import annotations
import asyncio
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable
from ..models import BUCKET_KIND_MEMORY, normalize_relations, utc_now_iso
from ..storage import MemoryStorageV3
if TYPE_CHECKING:
    from ..engine_runtime import EngineRuntime

def _clamp_score(value: float) -> float:
    return max(0.0, min(1.0, float(value)))

class SplitIngestJobService:

    def __init__(self, runtime: 'EngineRuntime', *, alias: Callable[[], Any], compression: Callable[[], Any], governance: Callable[[], Any], ingest: Callable[[], Any], maintenance: Callable[[], Any], primitives: Callable[[], Any], topology: Callable[[], Any]) -> None:
        self.runtime = runtime
        self._alias_provider = alias
        self._compression_provider = compression
        self._governance_provider = governance
        self._ingest_provider = ingest
        self._maintenance_provider = maintenance
        self._primitives_provider = primitives
        self._topology_provider = topology

    @property
    def alias(self) -> Any:
        return self._alias_provider()

    @property
    def compression(self) -> Any:
        return self._compression_provider()

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

    async def _auto_resume_pending_jobs_runner(self) -> None:
        try:
            self.runtime._auto_resume_last_result = await self.resume_pending_jobs()
        except Exception as exc:
            self.runtime._auto_resume_last_result = {'success': False, 'total_jobs': 0, 'completed_jobs': 0, 'failed_jobs': 0, 'jobs': [], 'message': f'auto_resume_exception: {exc}'}
        finally:
            self.runtime._auto_resume_task = None

    def _trigger_auto_resume_pending_jobs(self) -> None:
        if not self.runtime._auto_resume_pending_jobs:
            return
        if not isinstance(self.runtime.storage, MemoryStorageV3):
            return
        task = getattr(self, '_auto_resume_task', None)
        if task is not None and (not task.done()):
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                self.runtime._auto_resume_last_result = asyncio.run(self.resume_pending_jobs())
            except Exception as exc:
                self.runtime._auto_resume_last_result = {'success': False, 'total_jobs': 0, 'completed_jobs': 0, 'failed_jobs': 0, 'jobs': [], 'message': f'auto_resume_exception: {exc}'}
            return
        self.runtime._auto_resume_task = loop.create_task(self._auto_resume_pending_jobs_runner())

    async def _resume_split_job_unlocked(self, job: dict[str, Any]) -> dict[str, Any]:
        batch_id = str(job.get('batch_id', '')).strip()
        chunk_keys_raw = job.get('chunk_keys', [])
        chunk_texts_raw = job.get('chunk_texts', [])
        if not batch_id or not isinstance(chunk_keys_raw, list) or (not isinstance(chunk_texts_raw, list)):
            return {'batch_id': batch_id, 'success': False, 'message': 'invalid job payload'}
        chunk_keys = [str(x).strip() for x in chunk_keys_raw]
        chunk_texts = [str(x) for x in chunk_texts_raw]
        if not chunk_keys or len(chunk_keys) != len(chunk_texts):
            return {'batch_id': batch_id, 'success': False, 'message': 'invalid chunk sequence'}
        topic = str(job.get('topic', '')).strip()
        split_reason = str(job.get('split_reason', 'resume')).strip() or 'resume'
        input_type = str(job.get('input_type', 'plain')).strip().lower() or 'plain'
        skip_clean = bool(job.get('skip_clean', False))
        preserve_literal = bool(job.get('preserve_literal', False))
        evidence_path = str(job.get('evidence_path', '')).strip()
        chunk_total = len(chunk_keys)
        done_indices: set[int] = {int(i) for i in job.get('done_indices', []) if isinstance(i, int) and 0 <= int(i) < chunk_total}
        done_keys: set[str] = {chunk_keys[i] for i in done_indices}
        generation = int(job.get('generation', 0))
        rebuilt_once = bool(job.get('rebuilt_once', False))
        current_bucket_id = self.topology._resolve_bucket_id(str(job.get('current_bucket_id', '')).strip())
        if not current_bucket_id:
            current_bucket_id = self.topology._resolve_bucket_id(str(job.get('target_bucket_id', '')).strip())
        if not current_bucket_id:
            return {'batch_id': batch_id, 'success': False, 'message': 'missing target bucket'}

        async def _save(status: str, message: str='') -> None:
            payload = dict(job)
            payload['current_bucket_id'] = current_bucket_id
            payload['generation'] = generation
            payload['rebuilt_once'] = rebuilt_once
            payload['done_indices'] = sorted(done_indices)
            payload['done_keys'] = sorted(done_keys)
            payload['status'] = status
            payload['message'] = message
            payload['updated_at'] = utc_now_iso()
            await self.runtime.run_storage_task(self.runtime.storage.save_job_journal, payload)

        async def _build_split_chunks_payload() -> list[dict[str, Any]]:
            statuses = await self.runtime.run_storage_task(self.runtime.storage.record_statuses, chunk_keys)
            out: list[dict[str, Any]] = []
            for idx in range(chunk_total):
                status = statuses.get(chunk_keys[idx], {})
                out.append({'index': idx + 1, 'key': chunk_keys[idx], 'content': chunk_texts[idx], 'stored': bool(status.get('stored', False)), 'bucket_id': str(status.get('bucket_id', '')), 'revision_id': str(status.get('revision_id', ''))})
            return out
        seed_evidence_ref = str(job.get('evidence_ref_seed', '')).strip()
        evidence_text = ''
        if seed_evidence_ref:
            evidence_text = await self.runtime.run_storage_task(self.runtime.storage.read_evidence, seed_evidence_ref)
        pending_indices = [idx for idx in range(chunk_total) if idx not in done_indices]
        if not pending_indices:
            await _save('completed', 'already completed')
            return {'batch_id': batch_id, 'success': True, 'completed': chunk_total, 'pending': 0}
        for idx in pending_indices:
            attempts = 0
            ingested: dict[str, Any] | None = None
            while attempts < 2:
                attempts += 1
                context_snapshot = await self.runtime.bucket_context(current_bucket_id)
                split_chunks_payload = await _build_split_chunks_payload()
                out, overflow_seen, _ = await self.ingest._ingest_with_overflow_retry_detail(pipeline=self.runtime.pipeline, bucket_id=current_bucket_id, allow_retry=False, ingest_kwargs={'bucket_context': context_snapshot, 'key': chunk_keys[idx], 'event': 'ADD', 'raw_text': chunk_texts[idx], 'evidence_text': evidence_text, 'topic': f'{topic} [chunk {idx + 1}/{chunk_total}]'.strip(), 'input_type': input_type, 'skip_clean': skip_clean, 'preserve_literal': preserve_literal, 'split_chunks': split_chunks_payload, 'split_keys': chunk_keys, 'split_index': idx + 1, 'split_total': chunk_total, 'default_weight': 0.75})
                if not overflow_seen:
                    ingested = out
                    break
                if generation == 0 and (not rebuilt_once):
                    try:
                        await self.compression._force_compress_unlocked(bucket_id=current_bucket_id, reason='resume_split_ingest_overflow_switch')
                    except Exception:
                        pass
                    try:
                        await self.governance._auto_manage_bucket(current_bucket_id)
                    except Exception:
                        pass
                    current_bucket_id = self.topology._resolve_bucket_id(current_bucket_id) or current_bucket_id
                    generation = 1
                    rebuilt_once = True
                    await _save('running', 'payload rebuilt once during resume')
                    continue
                await _save('paused', f'recoverable_split_ingest_overflow_after_rebuild; batch_id={batch_id}; chunk_index={idx + 1}')
                return {'batch_id': batch_id, 'success': False, 'completed': len(done_indices), 'pending': chunk_total - len(done_indices), 'message': 'recoverable overflow after rebuild'}
            if ingested is None:
                await _save('paused', f'resume failed without ingest result; chunk_index={idx + 1}')
                return {'batch_id': batch_id, 'success': False, 'completed': len(done_indices), 'pending': chunk_total - len(done_indices), 'message': 'missing ingest result'}
            evidence_ref = ''
            if evidence_path:
                path_obj = Path(evidence_path)
                if path_obj.exists() and path_obj.is_file():
                    if idx == 0 and seed_evidence_ref:
                        evidence_ref = seed_evidence_ref
                    else:
                        evidence_ref = await self.runtime.run_storage_task(self.runtime.storage.copy_evidence, path_obj, key=chunk_keys[idx])
            rec = self.primitives._build_record(key=chunk_keys[idx], event='ADD', ingested=ingested, bucket_id=self.topology._resolve_bucket_id(current_bucket_id) or current_bucket_id, evidence_ref=evidence_ref, kind=BUCKET_KIND_MEMORY)
            rel = normalize_relations(rec.relations)
            if idx > 0:
                self.primitives._append_relation_once(rel['memory_links'], target=chunk_keys[idx - 1], rel_type='references', score=1.0, note='split_prev')
                self.primitives._append_relation_once(rel['dependency_links'], target=chunk_keys[idx - 1], rel_type='depends_on', score=0.9, note='split_sequence_prev')
            if idx + 1 < chunk_total:
                self.primitives._append_relation_once(rel['memory_links'], target=chunk_keys[idx + 1], rel_type='references', score=1.0, note='split_next')
                self.primitives._append_relation_once(rel['memory_links'], target=chunk_keys[idx + 1], rel_type='extends', score=0.85, note='split_sequence_next')
            rec = replace(rec, relations=rel)
            await self.runtime.run_storage_task(self.runtime.storage.write_memory_record, rec)
            await self.primitives._append_context_event(bucket_id=rec.bucket_id, event_type='ADD', record=rec, payload={'topic': topic, 'split_chunk_index': idx + 1, 'split_chunk_total': chunk_total, 'split_key_prev': chunk_keys[idx - 1] if idx > 0 else '', 'split_key_next': chunk_keys[idx + 1] if idx + 1 < chunk_total else '', 'split_reason': split_reason, 'resume_batch_id': batch_id})
            done_indices.add(idx)
            done_keys.add(chunk_keys[idx])
            await _save('running', f'resumed chunk {idx + 1}/{chunk_total}')
        await _save('completed', 'ok')
        await self.governance._auto_manage_bucket(current_bucket_id)
        await self.maintenance._run_memory_gc()
        return {'batch_id': batch_id, 'success': True, 'completed': len(done_indices), 'pending': 0}

    async def _resume_pending_jobs_unlocked(self) -> dict[str, object]:
        jobs = await self.runtime.run_storage_task(self.runtime.storage.list_job_journals, statuses={'running', 'paused'})
        results: list[dict[str, object]] = []
        for job in jobs:
            try:
                result = await self._resume_split_job_unlocked(job)
            except Exception as exc:
                batch_id = str(job.get('batch_id', '')).strip()
                await self.runtime.run_storage_task(self.runtime.storage.save_job_journal, {**job, 'batch_id': batch_id, 'status': 'paused', 'message': f'resume exception: {exc}', 'updated_at': utc_now_iso()})
                result = {'batch_id': batch_id, 'success': False, 'message': f'resume exception: {exc}'}
            results.append(result)
        completed = sum((1 for x in results if bool(x.get('success', False))))
        failed = len(results) - completed
        return {'success': failed == 0, 'total_jobs': len(results), 'completed_jobs': completed, 'failed_jobs': failed, 'jobs': results}

    async def resume_pending_jobs(self) -> dict[str, object]:
        self.alias.begin_session()
        try:
            async with self.runtime._global_meta_lock:
                return await self._resume_pending_jobs_unlocked()
        finally:
            self.alias.end_session(flush=True)
