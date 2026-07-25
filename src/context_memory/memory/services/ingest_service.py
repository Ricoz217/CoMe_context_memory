from __future__ import annotations
import asyncio
import hashlib
import random
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable
from uuid import uuid4
from ..llm_pipeline import LLMPipelineV3
from ..models import BUCKET_KIND_MEMORY, AddResult, normalize_relations, utc_now_iso
from ..multimodal import detect_file_kind, read_text_file
if TYPE_CHECKING:
    from ..engine_runtime import EngineRuntime

def _clamp_score(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


@dataclass(slots=True)
class _SplitIngestPreparation:
    target_bucket: str
    source_text: str
    input_type: str
    skip_clean: bool
    preserve_literal: bool
    chunk_texts: list[str]
    chunk_keys: list[str]
    seed_evidence_ref: str
    evidence_text: str
    source_hash: str
    batch_id: str
    base_split_chunks: list[dict[str, Any]]


class IngestService:

    def __init__(self, runtime: 'EngineRuntime', *, alias: Callable[[], Any], compression: Callable[[], Any], governance: Callable[[], Any], maintenance: Callable[[], Any], optimize: Callable[[], Any], primitives: Callable[[], Any], read: Callable[[], Any], summary: Callable[[], Any], topology: Callable[[], Any]) -> None:
        self.runtime = runtime
        self._alias_provider = alias
        self._compression_provider = compression
        self._governance_provider = governance
        self._maintenance_provider = maintenance
        self._optimize_provider = optimize
        self._primitives_provider = primitives
        self._read_provider = read
        self._summary_provider = summary
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
    def maintenance(self) -> Any:
        return self._maintenance_provider()

    @property
    def optimize(self) -> Any:
        return self._optimize_provider()

    @property
    def primitives(self) -> Any:
        return self._primitives_provider()

    @property
    def read(self) -> Any:
        return self._read_provider()

    @property
    def summary(self) -> Any:
        return self._summary_provider()

    @property
    def topology(self) -> Any:
        return self._topology_provider()

    def _new_split_ingest_pipeline(self) -> LLMPipelineV3:
        return LLMPipelineV3(self.runtime.pipeline.prompt_dir, llm_preset=self.runtime.pipeline.default_llm_preset, tool_presets=self.runtime.pipeline.tool_presets, ask_timeout=self.runtime.pipeline.ask_timeout, max_retries=self.runtime.pipeline.max_retries, use_mock_llm=self.runtime.pipeline.use_mock_llm, enable_cleaning=self.runtime.pipeline.enable_cleaning, usage_store=self.runtime.pipeline.usage_store, image_name_mapping=self.runtime.pipeline.image_name_mapping, init_config=False)

    async def _ingest_with_overflow_retry_detail(self, *, pipeline: LLMPipelineV3, bucket_id: str, ingest_kwargs: dict[str, Any], allow_retry: bool=True) -> tuple[dict[str, Any], bool, bool]:

        async def _aliasize_ingest_call() -> tuple[dict[str, Any], dict[str, Any], int]:
            raw_payload = {'event': ingest_kwargs.get('event', 'ADD'), 'input_type': ingest_kwargs.get('input_type', ''), 'skip_clean': bool(ingest_kwargs.get('skip_clean', False)), 'preserve_literal': bool(ingest_kwargs.get('preserve_literal', False)), 'split_total': ingest_kwargs.get('split_total'), 'split_chunks': ingest_kwargs.get('split_chunks', []) or [], 'split_keys': ingest_kwargs.get('split_keys', []) or [], 'default_weight': ingest_kwargs.get('default_weight'), 'evidence_text': ingest_kwargs.get('evidence_text', ''), 'previous_record': ingest_kwargs.get('previous_record', {}) or {}, 'topic': ingest_kwargs.get('topic', ''), 'key': ingest_kwargs.get('key', ''), 'split_index': ingest_kwargs.get('split_index'), 'raw_text': ingest_kwargs.get('raw_text', '')}
            alias_payload, map_ver = await self.alias._prepare_alias_payload_with_version(bucket_id, raw_payload)
            kwargs = dict(ingest_kwargs)
            for name in ('event', 'input_type', 'skip_clean', 'preserve_literal', 'split_total', 'split_chunks', 'split_keys', 'default_weight', 'evidence_text', 'previous_record', 'topic', 'key', 'split_index', 'raw_text'):
                kwargs[name] = alias_payload.get(name)
            return (kwargs, alias_payload, map_ver)
        alias_kwargs, alias_input, map_ver = await _aliasize_ingest_call()
        result_alias = await pipeline.ingest(**alias_kwargs)
        await self.alias.audit_llm_call(tool='ingest', bucket_id=bucket_id, map_version=map_ver, alias_input=alias_input, alias_output=result_alias)
        result = await self.alias.restore_alias_payload(bucket_id, result_alias, map_version=map_ver)
        await self.runtime.run_storage_task(self.runtime.record_llm_usage_values, pipeline.last_usage)
        await self.runtime.run_storage_task(self.runtime.record_llm_diag_values, pipeline.last_diagnostics)
        overflow_seen = self.runtime.is_context_overflow_diag(pipeline.last_diagnostics)
        if not overflow_seen:
            return (result, False, False)
        await self.runtime.record_overflow(stage='ingest')
        if not allow_retry:
            return (result, True, True)
        try:
            await self.compression._force_compress_unlocked(bucket_id=bucket_id, reason='context_overflow_ingest_retry')
        except Exception:
            pass
        alias_kwargs_retry, alias_input_retry, map_ver_retry = await _aliasize_ingest_call()
        retry_alias = await pipeline.ingest(**alias_kwargs_retry)
        await self.alias.audit_llm_call(tool='ingest', bucket_id=bucket_id, map_version=map_ver_retry, alias_input=alias_input_retry, alias_output=retry_alias)
        retry = await self.alias.restore_alias_payload(bucket_id, retry_alias, map_version=map_ver_retry)
        await self.runtime.run_storage_task(self.runtime.record_llm_usage_values, pipeline.last_usage)
        await self.runtime.run_storage_task(self.runtime.record_llm_diag_values, pipeline.last_diagnostics)
        overflow_still = self.runtime.is_context_overflow_diag(pipeline.last_diagnostics)
        if overflow_still:
            await self.runtime.record_overflow(stage='ingest')
        return (retry, True, overflow_still)

    async def _ingest_with_overflow_retry(self, *, pipeline: LLMPipelineV3, bucket_id: str, ingest_kwargs: dict[str, Any]) -> dict[str, Any]:
        result, _, _ = await self._ingest_with_overflow_retry_detail(pipeline=pipeline, bucket_id=bucket_id, ingest_kwargs=ingest_kwargs, allow_retry=True)
        return result

    async def _has_duplicate_memory_in_bucket(self, bucket_id: str, raw_text: str) -> bool:
        target = str(raw_text or '')
        if not target:
            return False
        records = await self.runtime.run_storage_task(self.runtime.storage.load_bucket_snapshot, bucket_id, include_gray=False)
        for rec in records:
            if rec.kind != BUCKET_KIND_MEMORY:
                continue
            if str(rec.content or '') == target:
                return True
        return False

    async def _filter_duplicate_chunks_in_bucket(self, bucket_id: str, chunks: list[str]) -> list[str]:
        if not chunks:
            return []
        existing_contents: set[str] = set()
        records = await self.runtime.run_storage_task(self.runtime.storage.load_bucket_snapshot, bucket_id, include_gray=False)
        for rec in records:
            if rec.kind != BUCKET_KIND_MEMORY:
                continue
            existing_contents.add(str(rec.content or ''))
        out: list[str] = []
        for chunk in chunks:
            token = str(chunk or '')
            if not token:
                continue
            if token in existing_contents:
                continue
            existing_contents.add(token)
            out.append(token)
        return out

    async def add_memory(self, raw_text: str, *, evidence_path: str | None=None, key: str | None=None, topic: str='', bucket_id: str | None=None, force_split: bool=False, create_new_bucket: bool=False, chunk_max_chars: int | None=None, chunk_overlap_chars: int | None=None, dedup_in_bucket: bool=False) -> AddResult:
        self.alias.begin_session()
        try:
            post_manage_buckets: list[str] = []
            result: AddResult | None = None
            async with self.topology._bucket_write_lock(bucket_id) as bucket:
                memory_count_before = await self.read.bucket_memory_count(bucket)
                text = str(raw_text or '')
                effective_force_split = bool(force_split)
                effective_create_new_bucket = bool(create_new_bucket) if effective_force_split else False
                max_chars = self.runtime._default_chunk_max_chars if chunk_max_chars is None else max(100, int(chunk_max_chars))
                overlap_chars = self.runtime._default_chunk_overlap_chars if chunk_overlap_chars is None else max(0, int(chunk_overlap_chars))
                if overlap_chars >= max_chars:
                    overlap_chars = max_chars // 4
                if effective_force_split or len(text) > self.runtime._max_memory_chars:
                    split_reason = 'force_split' if effective_force_split else 'oversize_auto_split'
                    result = await self._add_memory_with_split(raw_text=text, topic=topic, key=key, evidence_path=evidence_path, target_bucket_id=bucket, create_new_bucket=effective_create_new_bucket or len(text) > self.runtime._max_memory_chars, chunk_max_chars=max_chars, chunk_overlap_chars=overlap_chars, apply_clean_gate=False, split_reason=split_reason, dedup_in_bucket=dedup_in_bucket, deferred_auto_manage=post_manage_buckets)
                else:
                    memory_key = key.strip() if isinstance(key, str) and key.strip() else self.runtime.storage.generate_key()
                    evidence_ref = ''
                    evidence_text = ''
                    if evidence_path:
                        evidence_ref = await self.runtime.run_storage_task(self.runtime.storage.copy_evidence, evidence_path, key=memory_key)
                        evidence_text = await self.runtime.run_storage_task(self.runtime.storage.read_evidence, evidence_ref)
                    clean_result = await self.runtime.pipeline.clean(raw_text=text, evidence_text=evidence_text)
                    await self.runtime.record_llm_usage()
                    await self.runtime.record_llm_diag()
                    diag = self.runtime.pipeline.last_diagnostics
                    if str(diag.get('degraded_reason', '')) == 'clean_fallback':
                        await self.runtime.run_storage_task(self.runtime.storage.record_clean_fallback)
                    if not bool(clean_result.get('accept', True)):
                        await self.runtime.run_storage_task(self.runtime.storage.record_clean_reject)
                        await self.runtime.run_storage_task(self.runtime.storage.record_ingest_blocked_by_clean)
                        reason = str(clean_result.get('reject_reason', '')).strip() or 'clean rejected input'
                        return AddResult(success=False, key=memory_key, message=f'memory rejected: {reason}')
                    if dedup_in_bucket and await self._has_duplicate_memory_in_bucket(bucket, text):
                        return AddResult(success=False, key=memory_key, message='duplicate_in_bucket')
                    clean_type = str(clean_result.get('input_type', '')).strip().lower()
                    preserve_literal = bool(clean_result.get('preserve_literal', False)) or clean_type == 'source_code'
                    skip_clean = bool(clean_result.get('skip_clean', False)) or preserve_literal
                    ingest_input = text if skip_clean else str(clean_result.get('clean_text', '')).strip() or text
                    ingested = await self._ingest_with_overflow_retry(pipeline=self.runtime.pipeline, bucket_id=bucket, ingest_kwargs={'bucket_context': await self.runtime.bucket_context(bucket), 'key': memory_key, 'event': 'ADD', 'raw_text': ingest_input, 'evidence_text': evidence_text, 'topic': topic, 'input_type': clean_type, 'skip_clean': skip_clean, 'preserve_literal': preserve_literal})
                    record = self.primitives._build_record(key=memory_key, event='ADD', ingested=ingested, bucket_id=bucket, evidence_ref=evidence_ref, kind=BUCKET_KIND_MEMORY, child_bucket_id='')
                    await self.runtime.run_storage_task(self.runtime.storage.write_memory_record, record)
                    await self.primitives._append_context_event(bucket_id=bucket, event_type='ADD', record=record, payload={'topic': topic})
                    if memory_count_before == 0:
                        info = self.runtime.storage.get_bucket_info(bucket)
                        if not self.summary._should_skip_auto_summary(info):
                            await self.summary._refresh_bucket_summary_unlocked(bucket_id=bucket, force=False, reason='auto_first_memory')
                    post_manage_buckets.append(bucket)
                    result = AddResult(success=True, key=record.key, revision_id=record.revision_id, message='memory added', added_keys=[record.key], split_performed=False)
            for post_bucket in dict.fromkeys(post_manage_buckets):
                await self.governance._auto_manage_bucket(post_bucket)
            if result is not None and result.success:
                await self.maintenance._run_memory_gc()
            return result if result is not None else AddResult(success=False, message='memory add produced no result')
        finally:
            self.alias.end_session(flush=True)

    async def _prepare_split_ingest(
        self,
        *,
        raw_text: str,
        topic: str,
        key: str | None,
        evidence_path: str | None,
        target_bucket_id: str,
        create_new_bucket: bool,
        chunk_max_chars: int,
        chunk_overlap_chars: int,
        apply_clean_gate: bool,
        split_reason: str,
        dedup_in_bucket: bool,
    ) -> _SplitIngestPreparation | AddResult:
        if self.runtime.storage.get_bucket_info(target_bucket_id) is None:
            return AddResult(success=False, message=f'bucket not found: {target_bucket_id}')
        source_text = str(raw_text or '')
        input_type = 'plain'
        skip_clean = False
        preserve_literal = False
        if apply_clean_gate:
            clean_result = await self.runtime.pipeline.clean(
                raw_text=source_text,
                evidence_text='',
            )
            await self.runtime.record_llm_usage()
            await self.runtime.record_llm_diag()
            diag = self.runtime.pipeline.last_diagnostics
            if str(diag.get('degraded_reason', '')) == 'clean_fallback':
                await self.runtime.run_storage_task(
                    self.runtime.storage.record_clean_fallback
                )
            if not bool(clean_result.get('accept', True)):
                await self.runtime.run_storage_task(
                    self.runtime.storage.record_clean_reject
                )
                await self.runtime.run_storage_task(
                    self.runtime.storage.record_ingest_blocked_by_clean
                )
                reason = (
                    str(clean_result.get('reject_reason', '')).strip()
                    or 'clean rejected input'
                )
                return AddResult(
                    success=False,
                    key='',
                    message=f'memory rejected: {reason}',
                )
            input_type = (
                str(clean_result.get('input_type', '')).strip().lower() or 'plain'
            )
            preserve_literal = (
                bool(clean_result.get('preserve_literal', False))
                or input_type == 'source_code'
            )
            skip_clean = bool(clean_result.get('skip_clean', False)) or preserve_literal
            source_text = (
                source_text
                if skip_clean
                else str(clean_result.get('clean_text', '')).strip() or source_text
            )
        target_bucket = target_bucket_id
        if create_new_bucket:
            sample_record = {
                'title': topic or 'split bucket',
                'summary': source_text[:200],
                'content': source_text[:2000],
            }
            bucket_summary = await self.runtime.pipeline.summarize_bucket(
                records=[sample_record],
                reason='text_chunk_target_bucket',
            )
            await self.runtime.record_llm_usage()
            await self.runtime.record_llm_diag()
            new_bucket = await self.topology._create_bucket_auto(
                target_bucket_id=target_bucket_id,
                title=(topic or 'split_bucket')[:80],
                summary=bucket_summary.get('summary', 'split bucket'),
                content=bucket_summary.get('content', '')[:1000],
            )
            target_bucket = new_bucket.bucket_id
        chunk_plan = await self.runtime.pipeline.text_chunk(
            raw_text=source_text,
            topic=topic,
            chunk_max_chars=chunk_max_chars,
            chunk_overlap_chars=chunk_overlap_chars,
            reason=split_reason,
        )
        await self.runtime.record_llm_usage()
        await self.runtime.record_llm_diag()
        chunks = chunk_plan.get('chunks', [])
        if not isinstance(chunks, list):
            chunks = []
        chunk_texts = [str(chunk).strip() for chunk in chunks if str(chunk).strip()]
        if not chunk_texts:
            return AddResult(success=False, message='split produced empty chunks')
        if dedup_in_bucket:
            bucket_for_dedup = self.topology._resolve_bucket_id_soft(target_bucket_id)
            chunk_texts = await self._filter_duplicate_chunks_in_bucket(
                bucket_for_dedup,
                chunk_texts,
            )
            if not chunk_texts:
                return AddResult(success=False, message='duplicate_in_bucket')
        chunk_keys = [
            key.strip()
            if index == 0 and isinstance(key, str) and key.strip()
            else self.runtime.storage.generate_key()
            for index in range(len(chunk_texts))
        ]
        seed_evidence_ref = ''
        evidence_text = ''
        if evidence_path:
            seed_evidence_ref = await self.runtime.run_storage_task(
                self.runtime.storage.copy_evidence,
                evidence_path,
                key=chunk_keys[0],
            )
            evidence_text = await self.runtime.run_storage_task(
                self.runtime.storage.read_evidence,
                seed_evidence_ref,
            )
        batch_id = (
            f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_"
            f"{uuid4().hex}"
        )
        base_split_chunks = [
            {'index': index + 1, 'key': chunk_keys[index], 'content': chunk_text}
            for index, chunk_text in enumerate(chunk_texts)
        ]
        return _SplitIngestPreparation(
            target_bucket=target_bucket,
            source_text=source_text,
            input_type=input_type,
            skip_clean=skip_clean,
            preserve_literal=preserve_literal,
            chunk_texts=chunk_texts,
            chunk_keys=chunk_keys,
            seed_evidence_ref=seed_evidence_ref,
            evidence_text=evidence_text,
            source_hash=hashlib.sha1(source_text.encode('utf-8')).hexdigest(),
            batch_id=batch_id,
            base_split_chunks=base_split_chunks,
        )

    async def _add_memory_with_split(self, *, raw_text: str, topic: str, key: str | None, evidence_path: str | None, target_bucket_id: str, create_new_bucket: bool, chunk_max_chars: int, chunk_overlap_chars: int, apply_clean_gate: bool, split_reason: str, dedup_in_bucket: bool, deferred_auto_manage: list[str] | None=None) -> AddResult:
        prepared = await self._prepare_split_ingest(
            raw_text=raw_text,
            topic=topic,
            key=key,
            evidence_path=evidence_path,
            target_bucket_id=target_bucket_id,
            create_new_bucket=create_new_bucket,
            chunk_max_chars=chunk_max_chars,
            chunk_overlap_chars=chunk_overlap_chars,
            apply_clean_gate=apply_clean_gate,
            split_reason=split_reason,
            dedup_in_bucket=dedup_in_bucket,
        )
        if isinstance(prepared, AddResult):
            return prepared
        target_bucket = prepared.target_bucket
        input_type = prepared.input_type
        skip_clean = prepared.skip_clean
        preserve_literal = prepared.preserve_literal
        chunk_texts = prepared.chunk_texts
        chunk_keys = prepared.chunk_keys
        seed_evidence_ref = prepared.seed_evidence_ref
        evidence_text = prepared.evidence_text
        source_hash = prepared.source_hash
        batch_id = prepared.batch_id
        base_split_chunks = prepared.base_split_chunks
        chunk_total = len(chunk_texts)
        committed_indices: set[int] = set()
        committed_keys: set[str] = set()
        generation = 0
        rebuilt_once = False
        current_bucket_id = target_bucket

        async def _build_split_chunks_payload() -> list[dict[str, Any]]:
            statuses = await self.runtime.run_storage_task(self.runtime.storage.record_statuses, chunk_keys)
            payload: list[dict[str, Any]] = []
            for idx in range(chunk_total):
                key_i = chunk_keys[idx]
                status = statuses.get(key_i, {})
                payload.append({'index': idx + 1, 'key': key_i, 'content': chunk_texts[idx], 'stored': bool(status.get('stored', False)), 'bucket_id': str(status.get('bucket_id', '')), 'revision_id': str(status.get('revision_id', ''))})
            return payload

        async def _save_job(status: str, *, message: str='') -> None:
            await self.runtime.run_storage_task(self.runtime.storage.save_job_journal, {'batch_id': batch_id, 'target_bucket_id': target_bucket, 'current_bucket_id': current_bucket_id, 'topic': topic, 'split_reason': split_reason, 'chunk_total': chunk_total, 'chunk_keys': list(chunk_keys), 'chunk_texts': list(chunk_texts), 'done_indices': sorted(committed_indices), 'done_keys': sorted(committed_keys), 'generation': int(generation), 'rebuilt_once': bool(rebuilt_once), 'status': status, 'source_hash': source_hash, 'input_type': input_type, 'skip_clean': bool(skip_clean), 'preserve_literal': bool(preserve_literal), 'evidence_ref_seed': seed_evidence_ref, 'evidence_path': str(evidence_path or ''), 'message': message, 'created_at': utc_now_iso()})
        await _save_job('running')
        results: list[dict[str, Any] | None] = [None for _ in range(chunk_total)]
        result_bucket_ids: list[str] = ['' for _ in range(chunk_total)]
        errors: list[str] = []
        done_indices: set[int] = set()
        state_lock = asyncio.Lock()
        queue: asyncio.Queue[int] = asyncio.Queue()
        for idx in range(chunk_total):
            queue.put_nowait(idx)
        pause_event = asyncio.Event()
        pause_event.set()
        rebuild_event = asyncio.Event()
        drain_event = asyncio.Event()
        drain_event.set()
        inflight = 0
        fatal_recoverable_error = ''
        generation_context = await self.runtime.bucket_context(current_bucket_id)
        current_split_chunks = list(base_split_chunks)
        parallelism = max(1, min(self.runtime._split_ingest_parallelism, chunk_total))
        workers = [self._new_split_ingest_pipeline() for _ in range(parallelism)]
        loop = asyncio.get_running_loop()
        launch_lock = asyncio.Lock()
        next_launch_at = loop.time()
        delay_min = max(0.0, float(self.runtime._split_ingest_delay_min))
        delay_max = max(delay_min, float(self.runtime._split_ingest_delay_max))

        async def _wait_launch_slot() -> None:
            nonlocal next_launch_at
            if delay_max <= 0.0:
                return
            async with launch_lock:
                now = loop.time()
                if now < next_launch_at:
                    await asyncio.sleep(next_launch_at - now)
                gap = random.uniform(delay_min, delay_max)
                next_launch_at = loop.time() + gap

        async def _ingest_worker(pipe: LLMPipelineV3) -> None:
            nonlocal inflight, fatal_recoverable_error
            while True:
                await pause_event.wait()
                work_queue = queue
                try:
                    idx = work_queue.get_nowait()
                except asyncio.QueueEmpty:
                    async with state_lock:
                        done_all = len(done_indices) >= chunk_total
                        no_inflight = inflight == 0
                        fatal = bool(fatal_recoverable_error)
                    if done_all or (fatal and no_inflight):
                        return
                    await asyncio.sleep(0.02)
                    continue
                async with state_lock:
                    if idx in done_indices:
                        work_queue.task_done()
                        continue
                    inflight += 1
                    drain_event.clear()
                    local_generation = generation
                    local_bucket = current_bucket_id
                    local_context = generation_context
                    local_split_chunks = current_split_chunks
                try:
                    await _wait_launch_slot()
                    out, overflow_seen, _ = await self._ingest_with_overflow_retry_detail(pipeline=pipe, bucket_id=local_bucket, allow_retry=False, ingest_kwargs={'bucket_context': local_context, 'key': chunk_keys[idx], 'event': 'ADD', 'raw_text': chunk_texts[idx], 'evidence_text': evidence_text, 'topic': f'{topic} [chunk {idx + 1}/{chunk_total}]'.strip(), 'input_type': input_type, 'skip_clean': skip_clean, 'preserve_literal': preserve_literal, 'split_chunks': local_split_chunks, 'split_keys': chunk_keys, 'split_index': idx + 1, 'split_total': chunk_total, 'default_weight': 0.75})
                    if overflow_seen:
                        async with state_lock:
                            work_queue.put_nowait(idx)
                            if local_generation == 0 and (not rebuilt_once):
                                pause_event.clear()
                                rebuild_event.set()
                            elif not fatal_recoverable_error:
                                fatal_recoverable_error = f'recoverable_split_ingest_overflow_after_rebuild; batch_id={batch_id}'
                                pause_event.clear()
                        continue
                    async with state_lock:
                        results[idx] = out
                        result_bucket_ids[idx] = local_bucket
                        done_indices.add(idx)
                except Exception as exc:
                    errors.append(f'chunk {idx + 1}: {exc}')
                    async with state_lock:
                        results[idx] = {'kind': BUCKET_KIND_MEMORY, 'title': f"{topic or 'chunk'} #{idx + 1}", 'summary': chunk_texts[idx][:120], 'content': chunk_texts[idx], 'weight': 0.75, 'event': 'ADD', 'gray': False, 'relations': normalize_relations({}), 'expires_at': None}
                        result_bucket_ids[idx] = local_bucket
                        done_indices.add(idx)
                finally:
                    async with state_lock:
                        inflight -= 1
                        if inflight <= 0:
                            inflight = 0
                            drain_event.set()
                    work_queue.task_done()
        worker_tasks = [asyncio.create_task(_ingest_worker(pipe)) for pipe in workers]
        try:
            while True:
                async with state_lock:
                    done_all = len(done_indices) >= chunk_total
                    fatal_now = bool(fatal_recoverable_error)
                    needs_rebuild = rebuild_event.is_set()
                if done_all or fatal_now:
                    break
                if not needs_rebuild:
                    await asyncio.sleep(0.02)
                    continue
                rebuild_event.clear()
                await drain_event.wait()
                async with state_lock:
                    if rebuilt_once:
                        if not fatal_recoverable_error:
                            fatal_recoverable_error = f'recoverable_split_ingest_generation_limit_reached; batch_id={batch_id}'
                        break
                    old_bucket_id = current_bucket_id
                new_bucket_id = await self._prepare_rebuilt_split_bucket(
                    old_bucket_id,
                    deferred_auto_manage,
                )
                async with state_lock:
                    generation = 1
                    rebuilt_once = True
                    current_bucket_id = new_bucket_id
                    generation_context = await self.runtime.bucket_context(current_bucket_id)
                    current_split_chunks = await _build_split_chunks_payload()
                    pending = [i for i in range(chunk_total) if i not in done_indices]
                    queue = asyncio.Queue()
                    for i in pending:
                        queue.put_nowait(i)
                    pause_event.set()
                await _save_job('running', message='payload rebuilt once after overflow')
        finally:
            for t in worker_tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
        return await self._commit_split_ingest(
            topic=topic,
            split_reason=split_reason,
            evidence_path=evidence_path,
            seed_evidence_ref=seed_evidence_ref,
            chunk_texts=chunk_texts,
            chunk_keys=chunk_keys,
            results=results,
            result_bucket_ids=result_bucket_ids,
            current_bucket_id=current_bucket_id,
            committed_indices=committed_indices,
            committed_keys=committed_keys,
            fatal_recoverable_error=fatal_recoverable_error,
            deferred_auto_manage=deferred_auto_manage,
            parallelism=parallelism,
            errors=errors,
            save_job=_save_job,
        )

    async def _prepare_rebuilt_split_bucket(
        self,
        old_bucket_id: str,
        deferred_auto_manage: list[str] | None,
    ) -> str:
        try:
            await self.compression._force_compress_unlocked(
                bucket_id=old_bucket_id,
                reason='split_ingest_overflow_pause_switch',
            )
        except Exception:
            pass
        try:
            if deferred_auto_manage is not None:
                deferred_auto_manage.append(old_bucket_id)
            else:
                await self.governance._auto_manage_bucket(old_bucket_id)
        except Exception:
            pass
        return self.topology._resolve_bucket_id(old_bucket_id) or old_bucket_id

    async def _commit_split_ingest(
        self,
        *,
        topic: str,
        split_reason: str,
        evidence_path: str | None,
        seed_evidence_ref: str,
        chunk_texts: list[str],
        chunk_keys: list[str],
        results: list[dict[str, Any] | None],
        result_bucket_ids: list[str],
        current_bucket_id: str,
        committed_indices: set[int],
        committed_keys: set[str],
        fatal_recoverable_error: str,
        deferred_auto_manage: list[str] | None,
        parallelism: int,
        errors: list[str],
        save_job: Callable[..., Any],
    ) -> AddResult:
        chunk_total = len(chunk_texts)
        first_key = ''
        first_revision = ''
        for idx, _chunk in enumerate(chunk_texts):
            if results[idx] is None:
                continue
            memory_key = chunk_keys[idx]
            if memory_key in committed_keys:
                continue
            evidence_ref = ''
            if evidence_path:
                if idx == 0 and seed_evidence_ref:
                    evidence_ref = seed_evidence_ref
                else:
                    evidence_ref = await self.runtime.run_storage_task(
                        self.runtime.storage.copy_evidence,
                        evidence_path,
                        key=memory_key,
                    )
            resolved_bucket = self.topology._resolve_bucket_id(
                result_bucket_ids[idx] or current_bucket_id
            ) or current_bucket_id
            rec = self.primitives._build_record(
                key=memory_key,
                event='ADD',
                ingested=results[idx],
                bucket_id=resolved_bucket,
                evidence_ref=evidence_ref,
                kind=BUCKET_KIND_MEMORY,
            )
            relations = normalize_relations(rec.relations)
            if idx > 0:
                self.primitives._append_relation_once(
                    relations['memory_links'],
                    target=chunk_keys[idx - 1],
                    rel_type='references',
                    score=1.0,
                    note='split_prev',
                )
                self.primitives._append_relation_once(
                    relations['dependency_links'],
                    target=chunk_keys[idx - 1],
                    rel_type='depends_on',
                    score=0.9,
                    note='split_sequence_prev',
                )
            if idx + 1 < chunk_total:
                self.primitives._append_relation_once(
                    relations['memory_links'],
                    target=chunk_keys[idx + 1],
                    rel_type='references',
                    score=1.0,
                    note='split_next',
                )
                self.primitives._append_relation_once(
                    relations['memory_links'],
                    target=chunk_keys[idx + 1],
                    rel_type='extends',
                    score=0.85,
                    note='split_sequence_next',
                )
            rec = replace(rec, relations=relations)
            await self.runtime.run_storage_task(
                self.runtime.storage.write_memory_record,
                rec,
            )
            await self.primitives._append_context_event(
                bucket_id=resolved_bucket,
                event_type='ADD',
                record=rec,
                payload={
                    'topic': topic,
                    'split_chunk_index': idx + 1,
                    'split_chunk_total': chunk_total,
                    'split_key_prev': chunk_keys[idx - 1] if idx > 0 else '',
                    'split_key_next': (
                        chunk_keys[idx + 1] if idx + 1 < chunk_total else ''
                    ),
                    'split_reason': split_reason,
                },
            )
            if not first_key:
                first_key = rec.key
                first_revision = rec.revision_id
            committed_indices.add(idx)
            committed_keys.add(memory_key)
            await save_job('running')
        pending_after = [
            index for index in range(chunk_total) if index not in committed_indices
        ]
        if fatal_recoverable_error:
            await save_job('paused', message=fatal_recoverable_error)
            if deferred_auto_manage is None:
                await self.maintenance._run_memory_gc()
            return AddResult(
                success=False,
                key=first_key,
                revision_id=first_revision,
                message=(
                    f'{fatal_recoverable_error}; '
                    f'committed={len(committed_indices)}/{chunk_total}; '
                    f'current_bucket={current_bucket_id}'
                ),
                added_keys=[key for key in chunk_keys if key in committed_keys],
                split_performed=True,
            )
        if deferred_auto_manage is not None:
            deferred_auto_manage.append(current_bucket_id)
        else:
            await self.governance._auto_manage_bucket(current_bucket_id)
        target_info = self.runtime.storage.get_bucket_info(current_bucket_id)
        if not self.summary._should_skip_auto_summary(target_info):
            await self.summary._refresh_bucket_summary_unlocked(
                bucket_id=current_bucket_id,
                force=False,
                reason=f'auto_split_batch:{split_reason}',
            )
        await save_job('completed', message='ok')
        if deferred_auto_manage is None:
            await self.maintenance._run_memory_gc()
        return AddResult(
            success=True,
            key=first_key,
            revision_id=first_revision,
            message=(
                f'memory split into {chunk_total} chunks, '
                f'target_bucket={current_bucket_id}, parallel={parallelism}, '
                f'errors={len(errors)}, pending={len(pending_after)}'
            ),
            added_keys=[key for key in chunk_keys if key in committed_keys],
            split_performed=True,
        )

    async def add_memory_from_dir(self, dir_path: str, *, bucket_id: str | None=None, auto_create_sub_buckets: bool=False, image_extract_hint: str='', force_split: bool=True, create_new_bucket: bool=False, chunk_max_chars: int | None=None, chunk_overlap_chars: int | None=None, dedup_in_bucket: bool=True, collect_token_usage: bool=False) -> dict[str, Any]:
        effective_image_hint = str(image_extract_hint or '').strip()
        root_dir = Path(dir_path).expanduser()
        directory_exists, files, max_rel_depth = await self.runtime.run_storage_task(self.runtime.scan_directory_files, root_dir)
        if not directory_exists:
            return {'success': False, 'message': f'directory not found: {dir_path}', 'success_count': 0, 'fail_count': 0, 'skip_duplicate_count': 0, 'added_keys': [], 'per_file_added_keys': {}}
        target_bucket_id = self.topology._resolve_bucket_id(bucket_id)
        root_info = self.runtime.storage.get_bucket_info(target_bucket_id)
        if root_info is None:
            return {'success': False, 'message': f'bucket not found: {target_bucket_id}', 'success_count': 0, 'fail_count': 0, 'skip_duplicate_count': 0, 'added_keys': [], 'per_file_added_keys': {}}
        if not files:
            return {'success': True, 'message': 'empty directory', 'success_count': 0, 'fail_count': 0, 'skip_duplicate_count': 0, 'bucket_id': target_bucket_id, 'processed_files': 0, 'added_keys': [], 'per_file_added_keys': {}}
        if auto_create_sub_buckets:
            if int(root_info.level) + int(max_rel_depth) > int(self.runtime._max_depth):
                return {'success': False, 'message': f'max bucket depth exceeded: root_level={root_info.level}, required={root_info.level + max_rel_depth}, limit={self.runtime._max_depth}', 'success_count': 0, 'fail_count': 0, 'skip_duplicate_count': 0, 'added_keys': [], 'per_file_added_keys': {}}
        llm_before = await self.runtime.run_storage_task(self.runtime.storage.metadata_snapshot) if collect_token_usage else {}
        usage_before = {'llm_calls_total': int(llm_before.get('llm_calls_total', 0)), 'llm_input_tokens_total': int(llm_before.get('llm_input_tokens_total', 0)), 'llm_output_tokens_total': int(llm_before.get('llm_output_tokens_total', 0)), 'llm_cached_input_tokens_total': int(llm_before.get('llm_cached_input_tokens_total', 0))}
        success_count = 0
        fail_count = 0
        skip_duplicate_count = 0
        details: list[dict[str, str]] = []
        added_keys: list[str] = []
        per_file_added_keys: dict[str, list[str]] = {}
        dir_bucket_cache: dict[tuple[str, ...], str] = {(): target_bucket_id}
        total = len(files)
        for index, file_path in enumerate(files, start=1):
            rel_parent = file_path.parent.relative_to(root_dir)
            rel_parts = () if str(rel_parent) in {'.', ''} else tuple((str(x) for x in rel_parent.parts))
            current_bucket = target_bucket_id
            if auto_create_sub_buckets and rel_parts:
                path_acc: list[str] = []
                for part in rel_parts:
                    path_acc.append(part)
                    path_key = tuple(path_acc)
                    cached_bucket = dir_bucket_cache.get(path_key, '')
                    if cached_bucket:
                        current_bucket = cached_bucket
                        continue
                    child = await self.topology.set_bucket_with_id(part, current_bucket, summary='', content='', summary_locked=False)
                    current_bucket = child.bucket_id
                    dir_bucket_cache[path_key] = current_bucket
            result = await self.add_memory_from_file(str(file_path), topic=file_path.name, bucket_id=current_bucket, image_extract_hint=effective_image_hint, force_split=force_split, create_new_bucket=create_new_bucket, chunk_max_chars=chunk_max_chars, chunk_overlap_chars=chunk_overlap_chars, dedup_in_bucket=dedup_in_bucket, auto_optimize_after_split=False)
            file_added = [str(k).strip() for k in result.added_keys if str(k).strip()]
            if file_added:
                per_file_added_keys[str(file_path)] = file_added
                added_keys.extend(file_added)
            if result.success:
                success_count += 1
                print(f'[add_dir] {index}/{total} OK: {file_path}')
                continue
            msg = str(result.message or 'failed')
            if msg == 'duplicate_in_bucket':
                skip_duplicate_count += 1
            else:
                fail_count += 1
            details.append({'file': str(file_path), 'message': msg})
            print(f'[add_dir] {index}/{total} FAIL: {file_path} | {msg}')
        optimize_result: dict[str, Any] | None = None
        if not auto_create_sub_buckets and success_count > 0:
            opt = await self.optimize.optimize(bucket_id=target_bucket_id, reason='batch_dir_ingest')
            optimize_result = opt.to_dict()
        out: dict[str, Any] = {'success': True, 'message': 'batch completed', 'bucket_id': target_bucket_id, 'processed_files': total, 'success_count': success_count, 'fail_count': fail_count, 'skip_duplicate_count': skip_duplicate_count, 'details': details, 'optimize_result': optimize_result, 'added_keys': added_keys, 'per_file_added_keys': per_file_added_keys}
        if collect_token_usage:
            llm_after = await self.runtime.run_storage_task(self.runtime.storage.metadata_snapshot)
            out['token_usage_delta'] = {'llm_calls_total': int(llm_after.get('llm_calls_total', 0)) - usage_before['llm_calls_total'], 'llm_input_tokens_total': int(llm_after.get('llm_input_tokens_total', 0)) - usage_before['llm_input_tokens_total'], 'llm_output_tokens_total': int(llm_after.get('llm_output_tokens_total', 0)) - usage_before['llm_output_tokens_total'], 'llm_cached_input_tokens_total': int(llm_after.get('llm_cached_input_tokens_total', 0)) - usage_before['llm_cached_input_tokens_total']}
        return out

    async def add_memory_from_file(self, file_path: str, *, topic: str='', bucket_id: str | None=None, image_extract_hint: str='', query_hint: str | None=None, force_split: bool=True, create_new_bucket: bool=False, chunk_max_chars: int | None=None, chunk_overlap_chars: int | None=None, dedup_in_bucket: bool=True, auto_optimize_after_split: bool=True) -> AddResult:
        effective_image_hint = str(image_extract_hint or '').strip() or str(query_hint or '').strip()
        path = Path(file_path)
        exists, kind, text = await self.runtime.run_storage_task(self._inspect_file, path, self.runtime._max_memory_chars * 2)
        if not exists:
            return AddResult(success=False, message=f'file not found: {file_path}')
        target_bucket = self.topology._resolve_bucket_id(bucket_id)
        before_bucket_count = len(self.runtime.storage.list_buckets())
        result: AddResult
        if kind == 'text':
            if not text.strip():
                return AddResult(success=False, message='text file is empty or unreadable')
            result = await self.add_memory(text, evidence_path=str(path), topic=topic or path.name, bucket_id=target_bucket, force_split=force_split, create_new_bucket=create_new_bucket, chunk_max_chars=chunk_max_chars, chunk_overlap_chars=chunk_overlap_chars, dedup_in_bucket=dedup_in_bucket)
        elif kind == 'image':
            extracted = await self.runtime.image_extractor.extract(path, query=effective_image_hint)
            if not extracted.strip():
                await self.runtime.run_storage_task(self.runtime.storage.record_file_import_reject)
                return AddResult(success=False, message='image extraction returned empty text')
            result = await self.add_memory(extracted, evidence_path=str(path), topic=topic or path.name, bucket_id=target_bucket, force_split=force_split, create_new_bucket=create_new_bucket, chunk_max_chars=chunk_max_chars, chunk_overlap_chars=chunk_overlap_chars, dedup_in_bucket=dedup_in_bucket)
        else:
            await self.runtime.run_storage_task(self.runtime.storage.record_file_import_reject)
            suffix = path.suffix or '(no suffix)'
            return AddResult(success=False, message=f'unsupported file kind: {suffix}; detect_file_kind=unknown')
        if not result.success:
            return result
        after_bucket = self.topology._resolve_bucket_id(target_bucket)
        after_bucket_count = len(self.runtime.storage.list_buckets())
        split_rebuild_detected = bool(result.split_performed or after_bucket_count > before_bucket_count or after_bucket != target_bucket)
        result.split_rebuild_detected = split_rebuild_detected
        if auto_optimize_after_split and split_rebuild_detected and result.added_keys:
            await self.optimize.optimize(bucket_id=after_bucket, reason='auto_post_file_split')
        return result

    @staticmethod
    def _inspect_file(path: Path, max_chars: int) -> tuple[bool, str, str]:
        if not path.exists() or not path.is_file():
            return False, 'unknown', ''
        kind = detect_file_kind(path)
        text = read_text_file(path, max_chars=max_chars) if kind == 'text' else ''
        return True, kind, text
