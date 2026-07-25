from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path
from typing import Any, Callable, TypeVar

from .aliasing import AliasCodec
from .config import ContextMemoryConfig, _normalize_tool_presets, _resolve_effective_max_context_window
from .llm_pipeline import LLMPipelineV3
from .memory_manager import MemoryManager
from .multimodal import ImageTextExtractor
from .rerank import BM25IndexCache
from .storage import MemoryStorageV3
from .token_counter import TokenCounter
from context_memory.LLM_usage import LLMUsage
from context_memory.file_cache import configure_global_file_cache_dir
from context_memory.time_id import configure_global_time_id_state_file
from context_memory.utils import AutoMapping

TCPU = TypeVar("TCPU")


class BucketLockManager:
    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}
        self._guard = asyncio.Lock()

    async def get_lock(self, bucket_id: str) -> asyncio.Lock:
        token = str(bucket_id or "").strip() or "__empty_bucket__"
        async with self._guard:
            lock = self._locks.get(token)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[token] = lock
            return lock

    @asynccontextmanager
    async def acquire_many(self, bucket_ids):
        ordered = sorted({str(x or "").strip() for x in bucket_ids if str(x or "").strip()})
        locks = [await self.get_lock(bucket_id) for bucket_id in ordered]
        for lock in locks:
            await lock.acquire()
        try:
            yield
        finally:
            for lock in reversed(locks):
                lock.release()


class EngineRuntime:

    def __init__(
        self,
        base_dir: str | Path | None = None,
        *,
        config: ContextMemoryConfig | dict[str, Any] | None = None,
        llm_preset: str = "",
        image_llm_preset: str = "",
        tool_presets: dict[str, str] | None = None,
        ask_timeout: float = 300.0,
        auto_resume_pending_jobs: bool = True,
        use_mock_llm: bool = False,
        enable_cleaning: bool = True,
        init_config: bool = True,
        evidence_versions: int = 5,
        auto_manage: bool = True,
        enable_forgetting: bool | None = None,
        max_bucket_depth: int = 4,
        max_memory_bytes: int = 1_000_000_000,
        auto_compress_trigger_ratio: float = 0.70,
        auto_split_trigger_ratio: float = 0.50,
        split_plan_target_items: int = 180,
        split_plan_hard_cap: int = 250,
        auto_split_cooldown_sec: int = 600,
        auto_split_min_drop_abs: float = 0.03,
        auto_split_max_round_per_manage: int = 1,
        split_ingest_parallelism: int = 16,
        split_ingest_delay_min: float = 1.0,
        split_ingest_delay_max: float = 3.0,
        optimize_leaf_loss_threshold: float = 0.03,
        gc_revision_retention_days: int = 14,
        gc_gray_key_retention_days: int = 45,
        gc_archived_bucket_retention_days: int = 45,
        query_top_k_default: int = 5,
        query_branch_expand_k: int = 5,
        query_branch_expand_bind_top_k: bool = False,
        query_max_depth_default: int | None = None,
        query_mode_default: str = "auto",
        global_recall_top_n: int = 120,
        global_recall_top_m: int = 8,
        global_recall_depth_limit: int = 8,
        global_recall_time_budget_ms: int = 80,
        global_recall_boost_weight: float = 0.20,
        data_version: int = 4,
    ) -> None:
        cfg_obj: ContextMemoryConfig | None = None
        if isinstance(config, ContextMemoryConfig):
            cfg_obj = config
        elif isinstance(config, dict):
            cfg_obj = ContextMemoryConfig.from_dict(config)
        if cfg_obj is not None:
            base_dir = cfg_obj.base_dir if cfg_obj.base_dir is not None else base_dir
            llm_preset = cfg_obj.llm_preset
            image_llm_preset = cfg_obj.image_llm_preset
            ask_timeout = cfg_obj.ask_timeout
            auto_resume_pending_jobs = cfg_obj.auto_resume_pending_jobs
            use_mock_llm = cfg_obj.use_mock_llm
            enable_cleaning = cfg_obj.enable_cleaning
            init_config = cfg_obj.init_config
            evidence_versions = cfg_obj.evidence_versions
            auto_manage = cfg_obj.auto_manage
            enable_forgetting = cfg_obj.enable_forgetting
            max_bucket_depth = cfg_obj.max_bucket_depth
            max_memory_bytes = cfg_obj.max_memory_bytes
            auto_compress_trigger_ratio = cfg_obj.auto_compress_trigger_ratio
            auto_split_trigger_ratio = cfg_obj.auto_split_trigger_ratio
            split_plan_target_items = cfg_obj.split_plan_target_items
            split_plan_hard_cap = cfg_obj.split_plan_hard_cap
            auto_split_cooldown_sec = cfg_obj.auto_split_cooldown_sec
            auto_split_min_drop_abs = cfg_obj.auto_split_min_drop_abs
            auto_split_max_round_per_manage = cfg_obj.auto_split_max_round_per_manage
            split_ingest_parallelism = cfg_obj.split_ingest_parallelism
            split_ingest_delay_min = cfg_obj.split_ingest_delay_min
            split_ingest_delay_max = cfg_obj.split_ingest_delay_max
            optimize_leaf_loss_threshold = cfg_obj.optimize_leaf_loss_threshold
            gc_revision_retention_days = cfg_obj.gc_revision_retention_days
            gc_gray_key_retention_days = cfg_obj.gc_gray_key_retention_days
            gc_archived_bucket_retention_days = cfg_obj.gc_archived_bucket_retention_days
            query_top_k_default = cfg_obj.query_top_k_default
            query_branch_expand_k = cfg_obj.query_branch_expand_k
            query_branch_expand_bind_top_k = cfg_obj.query_branch_expand_bind_top_k
            query_max_depth_default = cfg_obj.query_max_depth_default
            query_mode_default = cfg_obj.query_mode_default
            global_recall_top_n = cfg_obj.global_recall_top_n
            global_recall_top_m = cfg_obj.global_recall_top_m
            global_recall_depth_limit = cfg_obj.global_recall_depth_limit
            global_recall_time_budget_ms = cfg_obj.global_recall_time_budget_ms
            global_recall_boost_weight = cfg_obj.global_recall_boost_weight
            tool_presets = dict(cfg_obj.tool_presets)
        normalized_tool_presets = _normalize_tool_presets(tool_presets)
        self.tool_presets = dict(normalized_tool_presets)
        self.llm_preset = llm_preset
        self.image_llm_preset = normalized_tool_presets.get('image_extract', '').strip() or image_llm_preset
        self._evidence_versions = max(1, int(evidence_versions))
        self.data_version = int(data_version)
        self.base_dir: Path | None = None
        self._storage: MemoryStorageV3 | None = None
        self._llm_usage_store: LLMUsage | None = None
        self._image_name_mapping_store: AutoMapping[list[str]] | None = None
        self.alias_codec = AliasCodec(None)
        self._alias_request_seq = 0
        if base_dir is not None:
            self.bind_storage(base_dir, evidence_versions=self._evidence_versions)
        prompt_dir = Path(__file__).resolve().parent / 'prompts'
        self.pipeline = LLMPipelineV3(prompt_dir, llm_preset=llm_preset, tool_presets=normalized_tool_presets, ask_timeout=ask_timeout, use_mock_llm=use_mock_llm, enable_cleaning=enable_cleaning, init_config=init_config, usage_store=self._llm_usage_store, image_name_mapping=self._image_name_mapping_store)
        self.auto_manage = auto_manage
        self.max_context_window = _resolve_effective_max_context_window(self.llm_preset)
        self.bm25_cache = BM25IndexCache(max_buckets=64)
        self.memory_manager = MemoryManager(max_bytes=max_memory_bytes)
        self.token_counter = TokenCounter()
        self.image_extractor = ImageTextExtractor(llm_preset=self.image_llm_preset, init_config=init_config, usage_store=self._llm_usage_store, image_name_mapping=self._image_name_mapping_store)
        self._global_meta_lock = asyncio.Lock()
        self._bucket_lock_manager = BucketLockManager()
        self._lock = self._global_meta_lock
        self._cpu_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix='come-query-cpu')
        self._storage_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='context-memory-storage')
        self._negative_delete_threshold = 0.1
        if enable_forgetting is None:
            from .config import _default_enable_forgetting

            enable_forgetting = _default_enable_forgetting()
        self._enable_forgetting = bool(enable_forgetting)
        self._max_depth = max(1, int(max_bucket_depth))
        self._max_memory_chars = 100000
        self._default_chunk_max_chars = 4000
        self._default_chunk_overlap_chars = 200
        self._pending_bucket_summary = 'pending_summary'
        self._split_ingest_parallelism = max(1, int(split_ingest_parallelism))
        self._auto_compress_trigger_ratio = max(0.0, min(float(auto_compress_trigger_ratio), 0.7))
        self._auto_split_trigger_ratio = max(0.0, min(float(auto_split_trigger_ratio), 0.5))
        self._split_plan_target_items = max(1, int(split_plan_target_items))
        self._split_plan_hard_cap = max(self._split_plan_target_items, int(split_plan_hard_cap))
        self._auto_split_cooldown_sec = max(0, int(auto_split_cooldown_sec))
        self._auto_split_min_drop_abs = max(0.0, float(auto_split_min_drop_abs))
        self._auto_split_max_round_per_manage = max(1, int(auto_split_max_round_per_manage))
        self._last_sealed_link_repair_version = -1
        if use_mock_llm:
            self._split_ingest_delay_min = 0.0
            self._split_ingest_delay_max = 0.0
        else:
            self._split_ingest_delay_min = max(0.0, float(split_ingest_delay_min))
            self._split_ingest_delay_max = max(self._split_ingest_delay_min, float(split_ingest_delay_max))
        self._optimize_leaf_loss_threshold = max(0.0, min(1.0, float(optimize_leaf_loss_threshold)))
        self._gc_revision_retention_days = max(1, int(gc_revision_retention_days))
        self._gc_gray_key_retention_days = max(1, int(gc_gray_key_retention_days))
        self._gc_archived_bucket_retention_days = max(1, int(gc_archived_bucket_retention_days))
        self._query_top_k_default = max(1, int(query_top_k_default))
        self._query_branch_expand_k = max(1, int(query_branch_expand_k))
        self._query_branch_expand_bind_top_k = bool(query_branch_expand_bind_top_k)
        self._query_max_depth_default = max(
            1,
            int(
                query_max_depth_default
                if query_max_depth_default is not None
                else self._max_depth
            ),
        )
        self._query_mode_default = self.normalize_query_mode_value(query_mode_default, field_name='query_mode_default')
        self._global_recall_top_n = max(10, int(global_recall_top_n))
        self._global_recall_top_m = max(1, int(global_recall_top_m))
        self._global_recall_depth_limit = max(1, int(global_recall_depth_limit))
        self._global_recall_time_budget_ms = max(10, int(global_recall_time_budget_ms))
        self._global_recall_boost_weight = max(0.0, min(1.0, float(global_recall_boost_weight)))
        self._auto_resume_pending_jobs = bool(auto_resume_pending_jobs)
        self._auto_resume_task: asyncio.Task | None = None
        self._auto_resume_last_result: dict[str, Any] | None = None

    @property
    def storage(self) -> MemoryStorageV3:
        storage = self._storage
        if storage is None:
            raise RuntimeError("memory base_dir is not configured")
        return storage

    def bind_storage(self, base_dir: str | Path, *, evidence_versions: int) -> None:
        self._evidence_versions = max(1, int(evidence_versions))
        self.base_dir = Path(base_dir)
        self._storage = MemoryStorageV3(self.base_dir, evidence_versions=self._evidence_versions, prefer_v4=self.data_version >= 4)
        self.alias_codec = AliasCodec(self.storage)
        self._last_sealed_link_repair_version = -1
        runtime_dir = self.base_dir / "runtime"
        usage_file = runtime_dir / "token_usage" / "usage.json"
        usage_file.parent.mkdir(parents=True, exist_ok=True)
        usage_store = LLMUsage(Path())
        usage_store.data_file = usage_file
        self._llm_usage_store = usage_store
        mapping_file = runtime_dir / "llm_connect" / "image_name_mapping.json"
        mapping_file.parent.mkdir(parents=True, exist_ok=True)
        self._image_name_mapping_store = AutoMapping(mapping_file, expire_day=14)
        configure_global_time_id_state_file(runtime_dir / "time_id_state.json")
        configure_global_file_cache_dir(runtime_dir / "file_cache")
        if hasattr(self, "pipeline"):
            self.pipeline.usage_store = self._llm_usage_store
            self.pipeline.image_name_mapping = self._image_name_mapping_store
        if hasattr(self, "image_extractor"):
            self.image_extractor.usage_store = self._llm_usage_store
            self.image_extractor.image_name_mapping = self._image_name_mapping_store

    def apply_config(self, config: ContextMemoryConfig | dict[str, Any]) -> None:
        cfg_obj = config if isinstance(config, ContextMemoryConfig) else ContextMemoryConfig.from_dict(config)
        normalized_tool_presets = _normalize_tool_presets(cfg_obj.tool_presets)
        new_base = cfg_obj.base_dir
        if new_base is not None:
            new_base_path = Path(new_base)
            if self.base_dir is None:
                self.bind_storage(new_base_path, evidence_versions=cfg_obj.evidence_versions)
            elif new_base_path != self.base_dir:
                raise RuntimeError(f'memory base_dir cannot be changed after initialization: {self.base_dir} -> {new_base_path}')
            elif int(cfg_obj.evidence_versions) != int(self._evidence_versions):
                raise RuntimeError('memory evidence_versions cannot be changed after storage initialization')
        self.llm_preset = cfg_obj.llm_preset
        self.tool_presets = dict(normalized_tool_presets)
        self.image_llm_preset = normalized_tool_presets.get('image_extract', '').strip() or cfg_obj.image_llm_preset
        self.pipeline.llm_preset = self.llm_preset
        self.pipeline.default_llm_preset = self.llm_preset
        self.pipeline.tool_presets = dict(normalized_tool_presets)
        self.pipeline.ask_timeout = float(cfg_obj.ask_timeout)
        self.pipeline.use_mock_llm = bool(cfg_obj.use_mock_llm)
        self.pipeline.enable_cleaning = bool(cfg_obj.enable_cleaning)
        self.pipeline.init_config = bool(cfg_obj.init_config)
        self.image_extractor.llm_preset = self.image_llm_preset
        self.image_extractor.init_config = bool(cfg_obj.init_config)
        self.auto_manage = bool(cfg_obj.auto_manage)
        self._auto_resume_pending_jobs = bool(cfg_obj.auto_resume_pending_jobs)
        self._enable_forgetting = bool(cfg_obj.enable_forgetting)
        self._max_depth = max(1, int(cfg_obj.max_bucket_depth))
        self.max_context_window = _resolve_effective_max_context_window(self.llm_preset)
        self.memory_manager.max_bytes = max(128 * 1024 * 1024, int(cfg_obj.max_memory_bytes))
        self._auto_compress_trigger_ratio = max(0.0, min(float(cfg_obj.auto_compress_trigger_ratio), 0.7))
        self._auto_split_trigger_ratio = max(0.0, min(float(cfg_obj.auto_split_trigger_ratio), 0.5))
        self._split_plan_target_items = max(1, int(cfg_obj.split_plan_target_items))
        self._split_plan_hard_cap = max(self._split_plan_target_items, int(cfg_obj.split_plan_hard_cap))
        self._auto_split_cooldown_sec = max(0, int(cfg_obj.auto_split_cooldown_sec))
        self._auto_split_min_drop_abs = max(0.0, float(cfg_obj.auto_split_min_drop_abs))
        self._auto_split_max_round_per_manage = max(1, int(cfg_obj.auto_split_max_round_per_manage))
        self._split_ingest_parallelism = max(1, int(cfg_obj.split_ingest_parallelism))
        if cfg_obj.use_mock_llm:
            self._split_ingest_delay_min = 0.0
            self._split_ingest_delay_max = 0.0
        else:
            self._split_ingest_delay_min = max(0.0, float(cfg_obj.split_ingest_delay_min))
            self._split_ingest_delay_max = max(self._split_ingest_delay_min, float(cfg_obj.split_ingest_delay_max))
        self._optimize_leaf_loss_threshold = max(0.0, min(1.0, float(cfg_obj.optimize_leaf_loss_threshold)))
        self._gc_revision_retention_days = max(1, int(cfg_obj.gc_revision_retention_days))
        self._gc_gray_key_retention_days = max(1, int(cfg_obj.gc_gray_key_retention_days))
        self._gc_archived_bucket_retention_days = max(1, int(cfg_obj.gc_archived_bucket_retention_days))
        self._query_top_k_default = max(1, int(cfg_obj.query_top_k_default))
        self._query_branch_expand_k = max(1, int(cfg_obj.query_branch_expand_k))
        self._query_branch_expand_bind_top_k = bool(cfg_obj.query_branch_expand_bind_top_k)
        self._query_max_depth_default = max(
            1,
            int(
                cfg_obj.query_max_depth_default
                if cfg_obj.query_max_depth_default is not None
                else self._max_depth
            ),
        )
        self._query_mode_default = self.normalize_query_mode_value(cfg_obj.query_mode_default, field_name='query_mode_default')
        self._global_recall_top_n = max(10, int(cfg_obj.global_recall_top_n))
        self._global_recall_top_m = max(1, int(cfg_obj.global_recall_top_m))
        self._global_recall_depth_limit = max(1, int(cfg_obj.global_recall_depth_limit))
        self._global_recall_time_budget_ms = max(10, int(cfg_obj.global_recall_time_budget_ms))
        self._global_recall_boost_weight = max(0.0, min(1.0, float(cfg_obj.global_recall_boost_weight)))

    @staticmethod
    def normalize_query_mode_value(mode: str, *, field_name: str) -> str:
        token = str(mode or 'auto').strip().lower() or 'auto'
        if token == 'literal':
            raise ValueError(f"{field_name}='literal' is not supported; use one of: auto, semantic, hybrid")
        if token not in {'auto', 'semantic', 'hybrid'}:
            raise ValueError(f'invalid {field_name}: {mode!r}; expected one of: auto, semantic, hybrid')
        return token

    async def bucket_context(self, bucket_id: str):
        cache_key = f'ctx:{bucket_id}'
        cached = self.memory_manager.get(cache_key)
        if cached is not None:
            return cached
        ctx = await self.run_storage_task(self.storage.load_bucket_context, bucket_id)
        if ctx is not None:
            try:
                size = len(str(ctx.to_dict())) * 2
            except Exception:
                size = 256 * 1024
            self.memory_manager.set(cache_key, ctx, bytes_estimate=size, dirty=False)
        return ctx

    def invalidate_bucket_context_cache(self, bucket_id: str) -> None:
        self.memory_manager.remove(f'ctx:{bucket_id}')
        self.memory_manager.remove(f'ctx_tokens:{bucket_id}')
        keep_version = self.storage.get_bucket_version(bucket_id)
        self.bm25_cache.clear_old_versions(bucket_id=bucket_id, keep_version=keep_version)

    async def record_llm_usage(self) -> None:
        await self.run_storage_task(self.record_llm_usage_values, self.pipeline.last_usage)

    def record_llm_usage_values(self, usage: dict[str, Any]) -> None:
        self.storage.record_llm_usage(input_tokens=usage.get('input_tokens', 0), output_tokens=usage.get('output_tokens', 0), cached_input_tokens=usage.get('cached_input_tokens', 0), calls=usage.get('calls', 0))

    async def run_cpu_task(self, fn: Callable[..., TCPU], /, *args: Any, **kwargs: Any) -> TCPU:
        loop = asyncio.get_running_loop()
        call = partial(fn, *args, **kwargs)
        return await loop.run_in_executor(self._cpu_executor, call)

    async def run_storage_task(self, fn: Callable[..., TCPU], /, *args: Any, **kwargs: Any) -> TCPU:
        loop = asyncio.get_running_loop()
        call = partial(fn, *args, **kwargs)
        return await loop.run_in_executor(self._storage_executor, call)

    @staticmethod
    def write_text_file(path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding='utf-8')

    @staticmethod
    def scan_directory_files(root_dir: Path) -> tuple[bool, list[Path], int]:
        if not root_dir.exists() or not root_dir.is_dir():
            return (False, [], 0)
        files = sorted((path for path in root_dir.rglob('*') if path.is_file()))
        max_depth = 0
        for file_path in files:
            relative_parent = file_path.parent.relative_to(root_dir)
            depth = 0 if str(relative_parent) in {'.', ''} else len(relative_parent.parts)
            max_depth = max(max_depth, depth)
        return (True, files, max_depth)

    def shutdown(self, *, wait: bool=False) -> None:
        storage_executor = getattr(self, '_storage_executor', None)
        if storage_executor is not None:
            self._storage_executor = None
            try:
                storage_executor.shutdown(wait=wait, cancel_futures=True)
            except TypeError:
                storage_executor.shutdown(wait=wait)
        storage = getattr(self, 'storage', None)
        if storage is not None:
            storage.close()
        executor = getattr(self, '_cpu_executor', None)
        if executor is None:
            return
        self._cpu_executor = None
        try:
            executor.shutdown(wait=wait, cancel_futures=True)
        except TypeError:
            executor.shutdown(wait=wait)

    async def close(self, *, wait: bool=False) -> None:
        self.shutdown(wait=wait)

    async def record_llm_diag(self) -> None:
        await self.run_storage_task(self.record_llm_diag_values, self.pipeline.last_diagnostics)

    def record_llm_diag_values(self, diag: dict[str, Any]) -> None:
        if diag.get('parse_failed', False):
            self.storage.record_llm_parse_fail()
        if diag.get('precheck_failed', False):
            self.storage.record_llm_precheck_fail()

    def diag_failure_stage(self, diag: dict[str, Any]) -> str:
        return str(diag.get('failure_stage', '')).strip().lower()

    def is_context_overflow_diag(self, diag: dict[str, Any]) -> bool:
        return self.diag_failure_stage(diag) == 'context_overflow'

    async def record_overflow(self, *, stage: str) -> None:
        await self.run_storage_task(self.storage.record_context_overflow, stage)
